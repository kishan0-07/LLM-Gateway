import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.application.ports.budget_store import DatabaseUnavailable
from app.application.services.budget_authorizer import CandidateExposure
from app.application.services.response_validator import ResponseValidator
from app.application.services.routing_engine import RouteCandidate
from app.application.use_cases.stream_completion import (
    PreparedStream,
    StreamCompletion,
    StreamRequest,
)
from app.domain.budget import ReservationResult
from app.domain.provider import ProviderStreamEvent


class FixedTokenEstimator:
    def output_cap(self, messages, model, requested_max_tokens):
        return 128

    def estimate_input_tokens(self, messages, model):
        return 7

    def estimate_output_tokens_for_text(self, *, text, model):
        return len(text)

    def count_output_tokens(self, *, text, model):
        return len(text)

    def truncate_output_text(self, *, text, model, max_tokens):
        if max_tokens < 1:
            raise ValueError("max_tokens must be positive")
        if len(text) <= max_tokens:
            return text, len(text), False
        return text[:max_tokens], max_tokens, True


class RecordingBudgetAuthorizer:
    def __init__(self, reservation=ReservationResult(True, "reservation-1")):
        self.reservation = reservation
        self.capacity_available = True
        self.attempt_usage: list[dict] = []
        self.finalizations: list[dict] = []
        self.reconciliation_marks: list[dict] = []

    def estimate_candidate_exposure(self, **kwargs):
        return CandidateExposure(input_tokens=7, output_cap=128, max_cost_micros=100)

    async def authorize(self, **kwargs):
        if isinstance(self.reservation, Exception):
            raise self.reservation
        return self.reservation

    async def ensure_attempt_capacity(self, **kwargs):
        return self.capacity_available

    async def record_attempt_usage(self, **kwargs):
        self.attempt_usage.append(kwargs)
        return 10

    async def record_conservative_attempt(self, **kwargs):
        self.attempt_usage.append({**kwargs, "usage_source": "conservative"})
        self.reconciliation_marks.append(
            {
                "reservation_id": kwargs["reservation_id"],
                "reason": "provider_usage_unavailable",
            }
        )
        return kwargs["exposure"].max_cost_micros

    async def finalize_reservation(self, **kwargs):
        self.finalizations.append(kwargs)
        return "none"

    async def mark_needs_reconciliation(self, **kwargs):
        self.reconciliation_marks.append(kwargs)


class RecordingCircuit:
    def __init__(self, available=True):
        self.available = available
        self.successes = []
        self.failures = []

    async def is_available(self, provider, model):
        return self.available

    async def record_success(self, provider, model):
        self.successes.append((provider, model))

    async def record_failure(self, provider, model):
        self.failures.append((provider, model))


class AllowAllRateLimiter:
    async def check(self, tenant_id, api_key_id):
        return None


class CapturingEventSink:
    def __init__(self):
        self.events = []

    async def emit(self, event):
        self.events.append(event)


class FailingEventSink:
    async def emit(self, event):
        raise RuntimeError("observability unavailable")


class ScriptedStreamProvider:
    def __init__(self, items, *, name="mock"):
        self.metadata = SimpleNamespace(name=name)
        self._items = list(items)
        self.calls = []
        self.closed = False

    async def stream(self, model, messages, *, max_tokens):
        self.calls.append((model, messages, max_tokens))
        try:
            for item in self._items:
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            self.closed = True


def build_stream_use_case(
    *,
    budget,
    circuit,
    events,
    timeout=30.0,
    idle_timeout=20.0,
):
    use_case = StreamCompletion(
        budget_authorizer=budget,
        routing_engine=SimpleNamespace(plan=lambda model: []),
        circuit_breaker=circuit,
        response_validator=ResponseValidator(),
        rate_limiter=AllowAllRateLimiter(),
        event_sink=events,
        token_estimator=FixedTokenEstimator(),
        stream_timeout_seconds=timeout,
        stream_idle_timeout_seconds=idle_timeout,
    )
    use_case._create_gateway_request = AsyncMock(return_value=101)
    use_case._update_gateway_request_status = AsyncMock()
    use_case._start_provider_attempt = AsyncMock(return_value=201)
    return use_case


def prepared(provider, *, model="gpt-5.4-mini"):
    request = StreamRequest(
        tenant_id=1,
        api_key_id=10,
        trace_id="stream-unit-test",
        model=model,
        messages=[{"role": "user", "content": "email jane@example.com"}],
    )
    return PreparedStream(
        request=request,
        gateway_request_id=101,
        reservation_id="reservation-1",
        output_cap=128,
        candidates=[RouteCandidate(provider, model, 0)] if provider else [],
    )


@pytest.mark.asyncio
async def test_preflight_fails_closed_on_database_unavailable():
    budget = RecordingBudgetAuthorizer(DatabaseUnavailable())
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    with pytest.raises(DatabaseUnavailable):
        await use_case.prepare(
            StreamRequest(
                tenant_id=1,
                api_key_id=10,
                trace_id="trace",
                model="mock-model",
                messages=[],
            )
        )

    assert use_case._start_provider_attempt.call_count == 0


@pytest.mark.asyncio
async def test_success_accounts_provider_usage_before_durable_completion():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="safe answer"),
            ProviderStreamEvent(type="usage", input_tokens=11, output_tokens=9),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    events = CapturingEventSink()
    use_case = build_stream_use_case(budget=budget, circuit=circuit, events=events)

    stream_events = [event async for event in use_case.stream(prepared(provider))]

    assert [event.type for event in stream_events] == ["delta", "done"]
    assert budget.attempt_usage == [
        {
            "reservation_id": "reservation-1",
            "provider_attempt_id": 201,
            "provider": "mock",
            "model": "gpt-5.4-mini",
            "input_tokens": 11,
            "output_tokens": 9,
            "usage_source": "actual",
            "attempt_status": "success",
            "latency_ms": pytest.approx(0, abs=100),
        }
    ]
    assert budget.finalizations[0]["final_status"] == "completed"
    assert circuit.successes == [("mock", "gpt-5.4-mini")]
    assert events.events[0]["reconciliation_state"] == "none"
    assert "[EMAIL]" in events.events[0]["prompt_excerpt"]
    assert "jane@example.com" not in events.events[0]["prompt_excerpt"]


@pytest.mark.asyncio
async def test_missing_usage_is_estimated_from_stream_text():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="hello "),
            ProviderStreamEvent(type="delta", content="world"),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    _ = [event async for event in use_case.stream(prepared(provider))]

    assert budget.attempt_usage[0]["input_tokens"] == 7
    assert budget.attempt_usage[0]["output_tokens"] == 11
    assert budget.attempt_usage[0]["usage_source"] == "estimated"


@pytest.mark.asyncio
async def test_provider_error_after_visible_delta_is_billed_and_cannot_fallback():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="start"),
            ProviderStreamEvent(type="error", content="timeout"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=circuit,
        events=CapturingEventSink(),
    )

    stream_events = [event async for event in use_case.stream(prepared(provider))]

    assert [event.type for event in stream_events] == ["delta", "error"]
    assert stream_events[-1].content == "provider_stream_failed"
    assert budget.attempt_usage[0]["attempt_status"] == "provider_stream_failed"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "failed"
    assert circuit.failures == [("mock", "gpt-5.4-mini")]


@pytest.mark.asyncio
async def test_attempt_capacity_rejection_happens_before_provider_call():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="must not be emitted"),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    budget.capacity_available = False
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    stream_events = [event async for event in use_case.stream(prepared(provider))]

    assert stream_events[-1].content == "budget_exceeded_for_fallback"
    assert provider.calls == []
    assert budget.attempt_usage == []
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_timeout_after_visible_delta_uses_conservative_accounting():
    class BlockingProvider:
        metadata = SimpleNamespace(name="mock")

        async def stream(self, *args, **kwargs):
            yield ProviderStreamEvent(type="delta", content="1")
            await asyncio.sleep(1)

    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
        timeout=0.001,
    )

    stream_events = [
        event async for event in use_case.stream(prepared(BlockingProvider()))
    ]

    assert stream_events[-1].content == "stream_timeout"
    assert budget.attempt_usage[0]["attempt_status"] == "stream_timeout"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_caller_cancellation_is_durably_finalized():
    provider_started = asyncio.Event()

    class WaitingProvider:
        metadata = SimpleNamespace(name="mock")

        async def stream(self, *args, **kwargs):
            yield ProviderStreamEvent(type="delta", content="1")
            provider_started.set()
            await asyncio.sleep(10)

    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    async def consume():
        async for _ in use_case.stream(prepared(WaitingProvider())):
            pass

    task = asyncio.create_task(consume())
    await provider_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await use_case.drain_finalizers()

    assert budget.attempt_usage[0]["attempt_status"] == "cancelled"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "cancelled"


@pytest.mark.asyncio
async def test_every_candidate_unavailable_releases_reservation():
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    stream_events = [event async for event in use_case.stream(prepared(None))]

    assert stream_events[0].content == "all_providers_unavailable"
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_cancellation_during_finalization_still_completes():
    finalize_started = asyncio.Event()
    finalize_completed = asyncio.Event()

    class SlowFinalizeBudgetAuthorizer(RecordingBudgetAuthorizer):
        async def finalize_reservation(self, **kwargs):
            finalize_started.set()
            await asyncio.sleep(0.1)
            self.finalizations.append(kwargs)
            finalize_completed.set()

    budget = SlowFinalizeBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="1"),
            ProviderStreamEvent(type="done"),
        ]
    )

    async def consume():
        async for _ in use_case.stream(prepared(provider)):
            pass

    task = asyncio.create_task(consume())
    await finalize_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.wait_for(finalize_completed.wait(), timeout=2.0)
    assert len(budget.finalizations) == 1


@pytest.mark.asyncio
async def test_stream_error_never_contains_provider_text():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(
                type="error",
                content="timeout: Connection to api.groq.com:443 timed out",
            ),
        ]
    )
    sink = CapturingEventSink()
    use_case = build_stream_use_case(
        budget=RecordingBudgetAuthorizer(),
        circuit=RecordingCircuit(),
        events=sink,
    )

    stream_events = [event async for event in use_case.stream(prepared(provider))]
    public_errors = [event.content for event in stream_events if event.type == "error"]

    assert public_errors == ["all_providers_unavailable"]
    assert all("groq.com" not in (error or "") for error in public_errors)
    terminal_event = sink.events[-1]
    assert terminal_event["event"] == "stream_failed"
    assert terminal_event["provider"] == "mock"
    assert terminal_event["model"] == "gpt-5.4-mini"
    assert terminal_event["final_provider_attempt_id"] == 201
    assert terminal_event["reconciliation_state"] == "none"


@pytest.mark.asyncio
async def test_empty_iterator_is_failed_and_never_emits_done():
    provider = ScriptedStreamProvider([])
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    assert [event.type for event in events] == ["error"]
    assert events[0].content == "all_providers_unavailable"
    assert budget.attempt_usage[0]["attempt_status"] == "stream_terminated_unexpectedly"
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_delta_then_exhaustion_is_truncated_not_successful():
    provider = ScriptedStreamProvider(
        [ProviderStreamEvent(type="delta", content="partial")]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    assert [event.type for event in events] == ["delta", "error"]
    assert events[-1].content == "stream_terminated_unexpectedly"
    assert all(event.type != "done" for event in events)
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_failure_before_first_delta_can_fallback_without_splicing():
    first = ScriptedStreamProvider(
        [ProviderStreamEvent(type="error", content="timeout")],
        name="groq",
    )
    second = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="fallback"),
            ProviderStreamEvent(type="usage", input_tokens=3, output_tokens=2),
            ProviderStreamEvent(type="done"),
        ],
        name="openai",
    )
    stream = prepared(first)
    stream = PreparedStream(
        request=stream.request,
        gateway_request_id=stream.gateway_request_id,
        reservation_id=stream.reservation_id,
        output_cap=stream.output_cap,
        candidates=[
            RouteCandidate(first, "openai/gpt-oss-20b", 0),
            RouteCandidate(second, "gpt-5.4-mini", 1),
        ],
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    use_case._start_provider_attempt = AsyncMock(side_effect=[201, 202])

    events = [event async for event in use_case.stream(stream)]

    assert [event.type for event in events] == ["delta", "done"]
    assert events[0].content == "fallback"
    assert [row["attempt_status"] for row in budget.attempt_usage] == [
        "provider_stream_failed",
        "success",
    ]
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_idle_timeout_is_separate_from_healthy_total_duration():
    class SteadyProvider:
        metadata = SimpleNamespace(name="mock")

        async def stream(self, *args, **kwargs):
            for _ in range(8):
                await asyncio.sleep(0.005)
                yield ProviderStreamEvent(type="delta", content="ok")
            yield ProviderStreamEvent(type="done")

    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
        timeout=1.0,
        idle_timeout=0.1,
    )

    events = [event async for event in use_case.stream(prepared(SteadyProvider()))]

    assert events[-1].type == "done"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_idle_timeout_before_first_token_is_accounted():
    class SilentProvider:
        metadata = SimpleNamespace(name="mock")

        async def stream(self, *args, **kwargs):
            await asyncio.sleep(1)
            yield ProviderStreamEvent(type="done")

    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
        timeout=1.5,
        idle_timeout=0.001,
    )

    events = [event async for event in use_case.stream(prepared(SilentProvider()))]

    assert events[-1].content == "all_providers_unavailable"
    assert budget.attempt_usage[0]["attempt_status"] == "stream_timeout"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"


@pytest.mark.asyncio
async def test_database_failure_during_completion_marks_reconciliation_and_no_done():
    class FinalizationFailureBudget(RecordingBudgetAuthorizer):
        async def finalize_reservation(self, **kwargs):
            raise DatabaseUnavailable("finalization failed")

    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="valid"),
            ProviderStreamEvent(type="usage", input_tokens=3, output_tokens=2),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = FinalizationFailureBudget()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    events = []

    with pytest.raises(DatabaseUnavailable):
        async for event in use_case.stream(prepared(provider)):
            events.append(event)

    assert [event.type for event in events] == ["delta"]
    assert budget.attempt_usage[0]["attempt_status"] == "success"
    assert budget.reconciliation_marks == [
        {
            "reservation_id": "reservation-1",
            "reason": "stream_durable_finalization_failed",
        }
    ]


@pytest.mark.asyncio
async def test_stream_event_sink_failure_cannot_remove_done():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="valid"),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=FailingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    assert events[-1].type == "done"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_visible_text_stops_at_authorized_output_cap():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="x" * 200),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    sink = CapturingEventSink()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=circuit,
        events=sink,
    )

    stream_events = [event async for event in use_case.stream(prepared(provider))]
    await use_case.drain_finalizers()

    visible_text = "".join(
        event.content
        for event in stream_events
        if event.type == "delta" and event.content
    )
    assert visible_text == "x" * 128
    assert stream_events[-1] == ProviderStreamEvent(
        type="error",
        content="output_limit_exceeded",
    )
    assert all(event.type != "done" for event in stream_events)
    assert budget.attempt_usage[0]["attempt_status"] == "output_limit_exceeded"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.reconciliation_marks == [
        {
            "reservation_id": "reservation-1",
            "reason": "provider_usage_unavailable",
        }
    ]
    assert budget.finalizations == [
        {
            "reservation_id": "reservation-1",
            "final_status": "failed",
        }
    ]
    assert circuit.failures == []
    assert circuit.successes == [("mock", "gpt-5.4-mini")]
    assert provider.closed is True
    assert sink.events[-1]["event"] == "stream_output_limit_exceeded"
    assert sink.events[-1]["reconciliation_state"] == "none"
    assert sink.events[-1]["response_excerpt"] == ""


@pytest.mark.asyncio
async def test_provider_reported_usage_above_cap_is_stored_unchanged():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="short"),
            ProviderStreamEvent(
                type="usage",
                input_tokens=10,
                output_tokens=500,
            ),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=circuit,
        events=CapturingEventSink(),
    )

    stream_events = [event async for event in use_case.stream(prepared(provider))]

    assert stream_events[-1].content == "output_limit_exceeded"
    assert all(event.type != "done" for event in stream_events)
    assert budget.attempt_usage == [
        {
            "reservation_id": "reservation-1",
            "provider_attempt_id": 201,
            "provider": "mock",
            "model": "gpt-5.4-mini",
            "input_tokens": 10,
            "output_tokens": 500,
            "usage_source": "actual",
            "attempt_status": "output_limit_exceeded",
            "latency_ms": budget.attempt_usage[0]["latency_ms"],
        }
    ]
    assert budget.reconciliation_marks == []
    assert budget.finalizations[0]["final_status"] == "failed"
    assert circuit.failures == []
    assert provider.closed is True


@pytest.mark.asyncio
async def test_gradual_deltas_stop_at_cap_boundary():
    provider = ScriptedStreamProvider(
        [
            *[ProviderStreamEvent(type="delta", content="a") for _ in range(130)],
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    visible_text = "".join(
        event.content for event in events if event.type == "delta" and event.content
    )
    assert len(visible_text) == 128
    assert events[-1].content == "output_limit_exceeded"
    assert budget.finalizations[0]["final_status"] == "failed"


@pytest.mark.asyncio
async def test_text_exactly_at_cap_completes_normally():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="a" * 128),
            ProviderStreamEvent(type="usage", input_tokens=7, output_tokens=128),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    assert events[-1].type == "done"
    assert budget.attempt_usage[0]["attempt_status"] == "success"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_malformed_usage_is_terminal_without_circuit_failure():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="usage", input_tokens=7),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=circuit,
        events=CapturingEventSink(),
    )

    events = [event async for event in use_case.stream(prepared(provider))]

    assert events[-1].content == "invalid_provider_usage"
    assert budget.attempt_usage[0]["attempt_status"] == "invalid_provider_usage"
    assert budget.finalizations[0]["final_status"] == "failed"
    assert circuit.failures == []


@pytest.mark.asyncio
async def test_provider_iterator_close_error_does_not_escape():
    class CloseFailureIterator:
        async def aclose(self):
            raise RuntimeError("close failed")

    use_case = build_stream_use_case(
        budget=RecordingBudgetAuthorizer(),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    await use_case._close_provider_iterator(
        CloseFailureIterator(),
        trace_id="close-test",
        provider="mock",
        model="gpt-5.4-mini",
    )


@pytest.mark.asyncio
async def test_provider_iterator_close_timeout_does_not_escape():
    class SlowCloseIterator:
        async def aclose(self):
            await asyncio.sleep(5)

    use_case = build_stream_use_case(
        budget=RecordingBudgetAuthorizer(),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    started_at = asyncio.get_running_loop().time()

    await use_case._close_provider_iterator(
        SlowCloseIterator(),
        trace_id="close-timeout-test",
        provider="mock",
        model="gpt-5.4-mini",
    )

    assert asyncio.get_running_loop().time() - started_at < 1.5


@pytest.mark.asyncio
async def test_disconnect_after_final_safe_delta_keeps_output_limit_outcome():
    provider = ScriptedStreamProvider(
        [
            ProviderStreamEvent(type="delta", content="x" * 200),
            ProviderStreamEvent(type="done"),
        ]
    )
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    stream = use_case.stream(prepared(provider))

    first = await anext(stream)
    assert first == ProviderStreamEvent(type="delta", content="x" * 128)
    await stream.aclose()
    await use_case.drain_finalizers()

    assert budget.attempt_usage[0]["attempt_status"] == "output_limit_exceeded"
    assert budget.finalizations == [
        {
            "reservation_id": "reservation-1",
            "final_status": "failed",
        }
    ]
    assert provider.closed is True


@pytest.mark.asyncio
async def test_output_limit_fallback_event_keeps_prior_cost_and_attempt_count():
    first = ScriptedStreamProvider(
        [ProviderStreamEvent(type="error", content="timeout")],
        name="groq",
    )
    second = ScriptedStreamProvider(
        [ProviderStreamEvent(type="delta", content="x" * 200)],
        name="openai",
    )
    original = prepared(first)
    stream = PreparedStream(
        request=original.request,
        gateway_request_id=original.gateway_request_id,
        reservation_id=original.reservation_id,
        output_cap=original.output_cap,
        candidates=[
            RouteCandidate(first, "openai/gpt-oss-20b", 0),
            RouteCandidate(second, "gpt-5.4-mini", 1),
        ],
    )
    budget = RecordingBudgetAuthorizer()
    sink = CapturingEventSink()
    use_case = build_stream_use_case(
        budget=budget,
        circuit=RecordingCircuit(),
        events=sink,
    )
    use_case._start_provider_attempt = AsyncMock(side_effect=[201, 202])

    events = [event async for event in use_case.stream(stream)]

    assert events[-1].content == "output_limit_exceeded"
    output_event = sink.events[-1]
    assert output_event["event"] == "stream_output_limit_exceeded"
    assert output_event["attempt_count"] == 2
    assert output_event["failover_count"] == 1
    assert output_event["cost_usd"] == "0.000200"
    assert output_event["reconciliation_state"] == "none"
    assert [row["attempt_status"] for row in budget.attempt_usage] == [
        "provider_stream_failed",
        "output_limit_exceeded",
    ]


def test_get_completion_use_cases_is_cached():
    from app.api.deps import get_completion_use_cases

    assert get_completion_use_cases() is get_completion_use_cases()
