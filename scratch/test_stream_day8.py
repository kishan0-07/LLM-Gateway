import asyncio
from app.infrastructure.providers.mock import MockProvider
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.infrastructure.redis.rate_limiter import PermissiveRateLimiter
from app.infrastructure.observability.event_logger import LogEventSink
from app.application.services.budget_authorizer import BudgetAuthorizer
from app.application.services.token_estimator import TokenEstimator
from app.application.services.routing_engine import RoutingEngine
from app.application.services.response_validator import ResponseValidator
from app.application.use_cases.stream_completion import StreamCompletion, StreamRequest


async def test_mock_stream():
    mock = MockProvider(mode="stream_delta")
    budget_store = RedisBudgetStore()

    use_case = StreamCompletion(
        budget_authorizer=BudgetAuthorizer(
            budget_store=budget_store,
            usage_ledger=budget_store,
            token_estimator=TokenEstimator(),
        ),
        routing_engine=RoutingEngine(providers={"mock": mock}),
        circuit_breaker=CircuitBreaker(),
        response_validator=ResponseValidator(),
        rate_limiter=PermissiveRateLimiter(),
        event_sink=LogEventSink(),
        token_estimator=TokenEstimator(),
    )

    request = StreamRequest(
        tenant_id=1,
        trace_id="test-stream-1",
        model="mock-model",
        messages=[{"role": "user", "content": "test"}],
    )

    events = []
    async for event in use_case.stream(request):
        events.append(event)
        print(
            f"  Event: type={event.type}, content={event.content}, "
            f"input_tokens={event.input_tokens}, output_tokens={event.output_tokens}"
        )

    types = [e.type for e in events]
    assert "delta" in types, "Expected at least one delta event"
    print("Test 1 (mock stream success) passed")
    print(f" Events: {types}")
    print()


async def test_provider_error_raised():
    mock = MockProvider(mode="timeout")
    budget_store = RedisBudgetStore()

    use_case = StreamCompletion(
        budget_authorizer=BudgetAuthorizer(
            budget_store=budget_store,
            usage_ledger=budget_store,
            token_estimator=TokenEstimator(),
        ),
        routing_engine=RoutingEngine(providers={"mock": mock}),
        circuit_breaker=CircuitBreaker(),
        response_validator=ResponseValidator(),
        rate_limiter=PermissiveRateLimiter(),
        event_sink=LogEventSink(),
        token_estimator=TokenEstimator(),
    )

    request = StreamRequest(
        tenant_id=1,
        trace_id="test-stream-2",
        model="mock-model",
        messages=[{"role": "user", "content": "test"}],
    )

    events = []
    async for event in use_case.stream(request):
        events.append(event)
        print(f"  Event: type={event.type}, content={event.content}")

    assert any(e.type == "error" for e in events), (
        "Expected an error event from except path"
    )
    print(" Test 2 (provider raises exception) passed")
    print("  This tested the 'except Exception' path, NOT the 'yield error event' path")
    print()


async def test_invalid_model():
    mock = MockProvider(mode="stream_delta")
    budget_store = RedisBudgetStore()

    use_case = StreamCompletion(
        budget_authorizer=BudgetAuthorizer(
            budget_store=budget_store,
            usage_ledger=budget_store,
            token_estimator=TokenEstimator(),
        ),
        routing_engine=RoutingEngine(providers={"mock": mock}),
        circuit_breaker=CircuitBreaker(),
        response_validator=ResponseValidator(),
        rate_limiter=PermissiveRateLimiter(),
        event_sink=LogEventSink(),
        token_estimator=TokenEstimator(),
    )

    request = StreamRequest(
        tenant_id=1,
        trace_id="test-stream-3",
        model="nonexistent-model",  # not in catalog
        messages=[{"role": "user", "content": "test"}],
    )

    events = []
    async for event in use_case.stream(request):
        events.append(event)
        print(f"  Event: type={event.type}, content={event.content}")

    assert any(e.type == "error" for e in events), (
        "Expected an error event for invalid model"
    )
    print(" Test 3 (invalid model) passed — no crash, clean error event")
    print()


async def main():
    print("=== StreamCompletion Finalizer Tests ===\n")
    await test_mock_stream()
    await test_provider_error_raised()
    await test_invalid_model()
    print("All stream tests passed!")


if __name__ == "__main__":
    asyncio.run(main())
