import pytest
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from app.application.services.routing_engine import RouteCandidate
from app.application.use_cases.stream_completion import (
    PreparedStream,
    StreamCompletion,
    StreamRequest,
)
from app.domain.budget import ReservationResult
from app.domain.provider import ProviderStreamEvent
from app.application.ports.budget_store import BudgetBackendUnavailable


class TestEncoder:
    def encode(self, value: str) -> list[int]:
        return list(range(len(value)))


class FixedTokenEstimator:
    def output_cap(self, messages, model, requested_max_tokens):
        return 128

    def estimate_input_tokens(self, messages, model):
        return 7

    def _get_encoder(self, tokenizer_hint):
        return TestEncoder()


class RecordingBudgetAuthorizer:
    def __init__(self, reservation=ReservationResult(True, "reservation-1")):
        self.reservation = reservation
        self.settlements: list[dict] = []
        self.remaining = 999.0

    async def authorize(self, **kwargs):
        if isinstance(self.reservation, Exception):
            raise self.reservation
        return self.reservation

    async def settle(self, **kwargs):
        self.settlements.append(kwargs)

    async def remaining_usd(self, tenant_id):
        return self.remaining


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


class ScriptedStreamProvider:
    def __init__(self, items, *, name="mock"):
        self.metadata = SimpleNamespace(name=name)
        self._items = list(items)
        self.calls = []

    async def stream(self, model, messages, *, max_tokens):
        self.calls.append((model, messages, max_tokens))
        for item in self._items:
            if isinstance(item, BaseException):
                raise item
            yield item


def build_stream_use_case(*, budget, circuit, events, timeout=30.0):
    use_case = StreamCompletion(
        budget_authorizer=budget,
        routing_engine=SimpleNamespace(plan=lambda model: []),
        circuit_breaker=circuit,
        response_validator=SimpleNamespace(),
        rate_limiter=AllowAllRateLimiter(),
        event_sink=events,
        token_estimator=FixedTokenEstimator(),
        stream_timeout_seconds=timeout,
    )
    use_case._create_gateway_request = AsyncMock(return_value=101)
    use_case._update_gateway_request_status = AsyncMock()
    use_case._start_provider_attempt = AsyncMock(return_value=201)
    use_case._finish_provider_attempt = AsyncMock()
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
async def test_preflight_fails_closed_on_budget_backend_unavailable():
    budget = RecordingBudgetAuthorizer(BudgetBackendUnavailable())
    events = CapturingEventSink()
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=events)
    
    with pytest.raises(BudgetBackendUnavailable):
        await use_case.prepare(StreamRequest(
            tenant_id=1, api_key_id=10, trace_id="trace", model="mock-model", messages=[]
        ))
    
    use_case._update_gateway_request_status.assert_awaited_with(101, "budget_backend_unavailable")
    assert use_case._start_provider_attempt.call_count == 0


@pytest.mark.asyncio
async def test_success_uses_provider_usage_and_finalizes_once():
    provider = ScriptedStreamProvider([
        ProviderStreamEvent(type="delta", content="safe answer"),
        ProviderStreamEvent(type="usage", input_tokens=11, output_tokens=9),
        ProviderStreamEvent(type="done"),
    ])
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    events = CapturingEventSink()
    use_case = build_stream_use_case(budget=budget, circuit=circuit, events=events)
    
    stream_events = [event async for event in use_case.stream(prepared(provider))]
    
    assert [event.type for event in stream_events] == ["delta"]
    assert budget.settlements == [{
        "reservation_id": "reservation-1",
        "provider": "mock",
        "model": "gpt-5.4-mini",
        "input_tokens": 11,
        "output_tokens": 9,
        "status": "success",
    }]
    use_case._finish_provider_attempt.assert_awaited_once()
    use_case._update_gateway_request_status.assert_awaited_once_with(101, "completed")
    assert circuit.successes == [("mock", "gpt-5.4-mini")]
    
    assert "[EMAIL]" in events.events[0]["prompt_excerpt"]
    assert "jane@example.com" not in events.events[0]["prompt_excerpt"]


@pytest.mark.asyncio
async def test_missing_usage_falls_back_to_estimate():
    provider = ScriptedStreamProvider([
        ProviderStreamEvent(type="delta", content="hello "),
        ProviderStreamEvent(type="delta", content="world"),
        ProviderStreamEvent(type="done"),
    ])
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=CapturingEventSink())
    
    stream_events = [event async for event in use_case.stream(prepared(provider))]
    
    assert budget.settlements[0]["input_tokens"] == 7
    assert budget.settlements[0]["output_tokens"] == 11  # len("hello world")


@pytest.mark.asyncio
async def test_provider_error_emits_error_and_settles():
    provider = ScriptedStreamProvider([
        ProviderStreamEvent(type="delta", content="start"),
        ProviderStreamEvent(type="error", content="timeout: provider stopped"),
    ])
    budget = RecordingBudgetAuthorizer()
    circuit = RecordingCircuit()
    use_case = build_stream_use_case(budget=budget, circuit=circuit, events=CapturingEventSink())
    
    stream_events = [event async for event in use_case.stream(prepared(provider))]
    assert [event.type for event in stream_events] == ["delta", "error"]
    assert circuit.failures == [("mock", "gpt-5.4-mini")]
    
    use_case._finish_provider_attempt.assert_awaited_once_with(201, status="provider_error", latency_ms=pytest.approx(0, abs=100))
    assert budget.settlements[0]["status"] == "error"
    use_case._update_gateway_request_status.assert_awaited_with(101, "failed")


@pytest.mark.asyncio
async def test_mid_stream_budget_cutoff():
    provider = ScriptedStreamProvider([
        ProviderStreamEvent(type="delta", content="x" * 100),
    ])
    budget = RecordingBudgetAuthorizer()
    budget.remaining = 0.0
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=CapturingEventSink())
    
    stream_events = [event async for event in use_case.stream(prepared(provider))]
    
    assert stream_events[-1].content == "budget_exceeded_mid_stream"
    use_case._finish_provider_attempt.assert_awaited_with(201, status="budget_exceeded", latency_ms=pytest.approx(0, abs=100))
    assert len(budget.settlements) == 1


@pytest.mark.asyncio
async def test_timeout_uses_finalizer():
    class BlockingProvider:
        metadata = SimpleNamespace(name="mock")
        async def stream(self, *args, **kwargs):
            yield ProviderStreamEvent(type="delta", content="1")
            await asyncio.sleep(1)

    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=CapturingEventSink(), timeout=0.001)
    
    stream_events = [event async for event in use_case.stream(prepared(BlockingProvider()))]
    
    assert stream_events[-1].type == "error"
    assert stream_events[-1].content == "stream_timeout"
    assert budget.settlements[0]["status"] == "error"
    use_case._update_gateway_request_status.assert_awaited_with(101, "failed")


@pytest.mark.asyncio
async def test_caller_cancellation_finalizes():
    flag = asyncio.Event()
    class WaitingProvider:
        metadata = SimpleNamespace(name="mock")
        async def stream(self, *args, **kwargs):
            yield ProviderStreamEvent(type="delta", content="1")
            flag.set()
            await asyncio.sleep(10)
            
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=CapturingEventSink())
    
    async def consume():
        async for event in use_case.stream(prepared(WaitingProvider())):
            pass
            
    task = asyncio.create_task(consume())
    await flag.wait()
    task.cancel()
    
    with pytest.raises(asyncio.CancelledError):
        await task

    use_case._finish_provider_attempt.assert_awaited_with(201, status="cancelled", latency_ms=pytest.approx(0, abs=100))
    assert budget.settlements[0]["status"] == "error"
    use_case._update_gateway_request_status.assert_awaited_with(101, "failed")


@pytest.mark.asyncio
async def test_every_candidate_unavailable_releases_reservation():
    budget = RecordingBudgetAuthorizer()
    use_case = build_stream_use_case(budget=budget, circuit=RecordingCircuit(), events=CapturingEventSink())
    
    stream_events = [event async for event in use_case.stream(prepared(None))]
    
    assert stream_events[0].type == "error"
    assert stream_events[0].content == "all_providers_unavailable"
    assert budget.settlements[0]["provider"] == "none"
    assert budget.settlements[0]["status"] == "error"
    use_case._update_gateway_request_status.assert_awaited_with(101, "failed")