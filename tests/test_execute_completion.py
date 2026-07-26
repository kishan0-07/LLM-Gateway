import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.application.services.budget_authorizer import CandidateExposure
from app.application.services.response_validator import ResponseValidator
from app.application.services.routing_engine import RouteCandidate
from app.application.use_cases.execute_completion import (
    AllProvidersFailedError,
    CompletionRequest,
    ExecuteCompletion,
)
from app.domain.budget import ReservationResult
from app.domain.provider import ProviderError, ProviderResult
from app.infrastructure.redis.circuit_breaker import CircuitBreaker


class FixedTokenEstimator:
    def output_cap(self, messages, model, requested_max_tokens):
        return 64

    def estimate_input_tokens(self, messages, model):
        return 8

    def estimate_output_tokens_for_text(self, *, text, model):
        return len(text)


class RecordingBudgetAuthorizer:
    def __init__(self, reservation: ReservationResult):
        self._token_estimator = FixedTokenEstimator()
        self.reservation = reservation
        self.attempt_usage: list[dict] = []
        self.finalizations: list[dict] = []
        self.reconciliation_marks: list[dict] = []

    def estimate_candidate_exposure(self, **kwargs):
        return CandidateExposure(input_tokens=8, output_cap=64, max_cost_micros=100)

    async def authorize(self, **kwargs):
        return self.reservation

    async def ensure_attempt_capacity(self, **kwargs):
        return True

    async def record_attempt_usage(self, **kwargs):
        self.attempt_usage.append(kwargs)
        return 10

    async def record_conservative_attempt(self, **kwargs):
        self.attempt_usage.append({**kwargs, "usage_source": "conservative"})
        return kwargs["exposure"].max_cost_micros

    async def finalize_reservation(self, **kwargs):
        self.finalizations.append(kwargs)

    async def mark_needs_reconciliation(self, **kwargs):
        self.reconciliation_marks.append(kwargs)


class StaticRouting:
    def __init__(self, candidates):
        self.candidates = candidates
        self.calls = []

    def plan(self, model):
        self.calls.append(model)
        return list(self.candidates)


class RecordingCircuit:
    def __init__(self):
        self.failures = []
        self.successes = []

    async def is_available(self, provider, model):
        return True

    async def record_failure(self, provider, model):
        self.failures.append((provider, model))

    async def record_success(self, provider, model):
        self.successes.append((provider, model))


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


class ScriptedProvider:
    def __init__(self, name, outcomes):
        self.metadata = SimpleNamespace(name=name)
        self._outcomes = iter(outcomes)
        self.calls = []

    async def complete(self, model, messages, *, max_tokens):
        self.calls.append((model, messages, max_tokens))
        outcome = next(self._outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def build_use_case(*, budget, routing, circuit, events):
    use_case = ExecuteCompletion(
        budget,
        routing,
        circuit,
        ResponseValidator(),
        AllowAllRateLimiter(),
        events,
        FixedTokenEstimator(),
    )
    use_case._create_gateway_request = AsyncMock(return_value=101)
    use_case._update_gateway_request_status = AsyncMock()
    use_case._start_provider_attempt = AsyncMock(side_effect=[201, 202, 203])
    return use_case


@pytest.mark.asyncio
async def test_unknown_model_rejects_before_request_creation():
    use_case = build_use_case(
        budget=RecordingBudgetAuthorizer(ReservationResult(True, "res-1")),
        routing=StaticRouting([]),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    with pytest.raises(ProviderError) as exc_info:
        await use_case.execute(
            CompletionRequest(
                tenant_id=1,
                api_key_id=10,
                trace_id="trace-1",
                model="invalid/model",
                messages=[],
            )
        )

    assert exc_info.value.category == "invalid_request"
    assert use_case._create_gateway_request.call_count == 0


@pytest.mark.asyncio
async def test_budget_rejection_never_calls_provider():
    budget = RecordingBudgetAuthorizer(ReservationResult(False, None, "over_budget"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting([]),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    with pytest.raises(ProviderError) as exc_info:
        await use_case.execute(
            CompletionRequest(
                tenant_id=1,
                api_key_id=10,
                trace_id="trace-1",
                model="openai/gpt-oss-20b",
                messages=[],
            )
        )

    assert exc_info.value.category == "invalid_request"
    assert exc_info.value.message == "over_budget"
    use_case._update_gateway_request_status.assert_called_with(101, "budget_rejected")
    assert use_case._start_provider_attempt.call_count == 0


@pytest.mark.asyncio
async def test_retryable_provider_failure_falls_back_and_settles_success():
    first = ScriptedProvider(
        "groq", [ProviderError("groq", "timeout", "forced timeout", True)]
    )
    second = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai", "gpt-5.4-mini", "fallback answer", 12, 8, "actual", 7
            )
        ],
    )
    routing = StaticRouting(
        [
            RouteCandidate(first, "openai/gpt-oss-20b", 0),
            RouteCandidate(second, "gpt-5.4-mini", 1),
        ]
    )
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    circuit = RecordingCircuit()
    events = CapturingEventSink()
    use_case = build_use_case(
        budget=budget, routing=routing, circuit=circuit, events=events
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-fallback",
            model="openai/gpt-oss-20b",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert response.provider == "openai"
    assert circuit.failures == [("groq", "openai/gpt-oss-20b")]
    assert circuit.successes == [("openai", "gpt-5.4-mini")]
    assert [row["attempt_status"] for row in budget.attempt_usage] == [
        "timeout",
        "success",
    ]
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_invalid_output_falls_back_without_circuit_failure():
    first = ScriptedProvider(
        "groq",
        [ProviderResult("groq", "openai/gpt-oss-20b", "   ", 10, 0, "actual", 5)],
    )
    second = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai", "gpt-5.4-mini", "valid fallback", 10, 5, "actual", 6
            )
        ],
    )
    routing = StaticRouting(
        [
            RouteCandidate(first, "openai/gpt-oss-20b", 0),
            RouteCandidate(second, "gpt-5.4-mini", 1),
        ]
    )
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    circuit = RecordingCircuit()
    use_case = build_use_case(
        budget=budget, routing=routing, circuit=circuit, events=CapturingEventSink()
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-invalid",
            model="openai/gpt-oss-20b",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert response.provider == "openai"
    assert circuit.failures == []  # No circuit failure!
    assert circuit.successes == [
        ("groq", "openai/gpt-oss-20b"),
        ("openai", "gpt-5.4-mini"),
    ]
    assert [row["attempt_status"] for row in budget.attempt_usage] == [
        "invalid_output",
        "success",
    ]
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_all_candidates_failed_settles_error_and_marks_request_failed():
    first = ScriptedProvider(
        "groq", [ProviderError("groq", "server_error", "fail", True)]
    )
    routing = StaticRouting([RouteCandidate(first, "openai/gpt-oss-20b", 0)])
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    events = CapturingEventSink()
    use_case = build_use_case(
        budget=budget, routing=routing, circuit=RecordingCircuit(), events=events
    )

    with pytest.raises(AllProvidersFailedError):
        await use_case.execute(
            CompletionRequest(
                tenant_id=1,
                api_key_id=10,
                trace_id="unit-all-fail",
                model="openai/gpt-oss-20b",
                messages=[],
            )
        )

    assert budget.attempt_usage[0]["provider"] == "groq"
    assert budget.attempt_usage[0]["attempt_status"] == "server_error"
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "failed"
    assert events.events[0]["event"] == "request_failed"


@pytest.mark.asyncio
async def test_completion_event_sanitizes_prompt_and_response():
    provider = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai",
                "gpt-5.4-mini",
                "Contact jane@example.com at 555-123-4567; SSN 123-45-6789",
                10,
                10,
                "actual",
                5,
            )
        ],
    )
    routing = StaticRouting([RouteCandidate(provider, "gpt-5.4-mini", 0)])
    events = CapturingEventSink()
    use_case = build_use_case(
        budget=RecordingBudgetAuthorizer(ReservationResult(True, "res")),
        routing=routing,
        circuit=RecordingCircuit(),
        events=events,
    )

    await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-pii",
            model="gpt-5.4-mini",
            messages=[
                {
                    "role": "user",
                    "content": "Email jane@example.com, phone 555-123-4567, SSN 123-45-6789",
                }
            ],
        )
    )

    event = events.events[0]
    assert "jane@example.com" not in event["prompt_excerpt"]
    assert "555-123-4567" not in event["prompt_excerpt"]
    assert "123-45-6789" not in event["response_excerpt"]

    assert "[EMAIL]" in event["prompt_excerpt"]
    assert "[PHONE]" in event["prompt_excerpt"]
    assert "[SSN]" in event["response_excerpt"]


@pytest.mark.asyncio
async def test_missing_provider_usage_is_estimated_instead_of_zero():
    provider = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai",
                "gpt-5.4-mini",
                "estimated answer",
                0,
                0,
                "estimated",
                5,
            )
        ],
    )
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting([RouteCandidate(provider, "gpt-5.4-mini", 0)]),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-estimated-usage",
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert (response.input_tokens, response.output_tokens) == (8, 16)
    assert budget.attempt_usage[0]["usage_source"] == "estimated"
    assert budget.attempt_usage[0]["input_tokens"] == 8
    assert budget.attempt_usage[0]["output_tokens"] == 16


@pytest.mark.asyncio
async def test_event_sink_failure_cannot_undo_financial_finalization():
    provider = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai",
                "gpt-5.4-mini",
                "successful answer",
                10,
                5,
                "actual",
                5,
            )
        ],
    )
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting([RouteCandidate(provider, "gpt-5.4-mini", 0)]),
        circuit=RecordingCircuit(),
        events=FailingEventSink(),
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-event-failure",
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert response.content == "successful answer"
    assert budget.attempt_usage[0]["attempt_status"] == "success"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_redis_circuit_failure_cannot_block_completion():
    class BrokenRedis:
        async def get(self, *args, **kwargs):
            raise ConnectionError("redis unavailable")

        def pipeline(self):
            raise ConnectionError("redis unavailable")

    provider = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai",
                "gpt-5.4-mini",
                "successful answer",
                10,
                5,
                "actual",
                5,
            )
        ],
    )
    circuit = CircuitBreaker()
    circuit._redis = BrokenRedis()
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting([RouteCandidate(provider, "gpt-5.4-mini", 0)]),
        circuit=circuit,
        events=CapturingEventSink(),
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-circuit-redis-failure",
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert response.content == "successful answer"
    assert budget.attempt_usage[0]["attempt_status"] == "success"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_unexpected_adapter_failure_is_conservatively_billed_and_falls_back():
    first = ScriptedProvider("groq", [RuntimeError("unexpected SDK shape")])
    second = ScriptedProvider(
        "openai",
        [
            ProviderResult(
                "openai",
                "gpt-5.4-mini",
                "fallback answer",
                10,
                5,
                "actual",
                5,
            )
        ],
    )
    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting(
            [
                RouteCandidate(first, "openai/gpt-oss-20b", 0),
                RouteCandidate(second, "gpt-5.4-mini", 1),
            ]
        ),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )

    response = await use_case.execute(
        CompletionRequest(
            tenant_id=1,
            api_key_id=10,
            trace_id="unit-unexpected-adapter-error",
            model="openai/gpt-oss-20b",
            messages=[{"role": "user", "content": "hello"}],
        )
    )

    assert response.provider == "openai"
    assert [row["attempt_status"] for row in budget.attempt_usage] == [
        "server_error",
        "success",
    ]
    assert budget.attempt_usage[0]["usage_source"] == "conservative"
    assert budget.finalizations[0]["final_status"] == "completed"


@pytest.mark.asyncio
async def test_nonstream_cancellation_marks_attempt_for_reconciliation():
    started = asyncio.Event()

    class WaitingProvider:
        metadata = SimpleNamespace(name="openai")

        async def complete(self, model, messages, *, max_tokens):
            started.set()
            await asyncio.sleep(10)

    budget = RecordingBudgetAuthorizer(ReservationResult(True, "reservation-1"))
    use_case = build_use_case(
        budget=budget,
        routing=StaticRouting([RouteCandidate(WaitingProvider(), "gpt-5.4-mini", 0)]),
        circuit=RecordingCircuit(),
        events=CapturingEventSink(),
    )
    task = asyncio.create_task(
        use_case.execute(
            CompletionRequest(
                tenant_id=1,
                api_key_id=10,
                trace_id="unit-nonstream-cancel",
                model="gpt-5.4-mini",
                messages=[{"role": "user", "content": "hello"}],
            )
        )
    )
    await started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert budget.attempt_usage == []
    assert budget.reconciliation_marks == [
        {
            "reservation_id": "reservation-1",
            "reason": "nonstream_provider_call_cancelled",
        }
    ]
