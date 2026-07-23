import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from app.application.services.response_validator import ResponseValidator
from app.application.services.routing_engine import RouteCandidate
from app.application.use_cases.execute_completion import (
    AllProvidersFailedError,
    CompletionRequest,
    ExecuteCompletion,
)
from app.domain.budget import ReservationResult
from app.domain.provider import ProviderError, ProviderResult


class FixedTokenEstimator:
    def output_cap(self, messages, model, requested_max_tokens):
        return 64


class RecordingBudgetAuthorizer:
    def __init__(self, reservation: ReservationResult):
        self._token_estimator = FixedTokenEstimator()
        self.reservation = reservation
        self.settlements: list[dict] = []

    async def authorize(self, **kwargs):
        return self.reservation

    async def settle(self, **kwargs):
        self.settlements.append(kwargs)


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
    use_case._finish_provider_attempt = AsyncMock()
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
    assert budget.settlements[0]["status"] == "success"
    assert [
        call.kwargs["status"]
        for call in use_case._finish_provider_attempt.await_args_list
    ] == ["timeout", "success"]


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
    assert circuit.successes == [("openai", "gpt-5.4-mini")]
    assert [
        call.kwargs["status"]
        for call in use_case._finish_provider_attempt.await_args_list
    ] == ["invalid_output", "success"]


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

    assert budget.settlements[0]["provider"] == "none"
    assert budget.settlements[0]["status"] == "error"
    use_case._update_gateway_request_status.assert_called_with(101, "failed")
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
