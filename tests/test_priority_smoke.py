import pytest
import asyncio
from dataclasses import replace as dataclass_replace
from decimal import Decimal
from unittest.mock import patch
from httpx import AsyncClient, ASGITransport
from sqlalchemy import select, update
from app.main import app
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import GatewayRequest, ProviderAttempt, BudgetAccount
from app.domain.provider import ProviderStreamEvent


# --- Reusable assertion helpers ---

async def latest_request_for_trace(trace_id: str) -> GatewayRequest:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(GatewayRequest).where(GatewayRequest.trace_id == trace_id)
        )
        return result.scalars().first()


async def attempts_for_request(gateway_request_id: int) -> list[ProviderAttempt]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ProviderAttempt)
            .where(ProviderAttempt.gateway_request_id == gateway_request_id)
            .order_by(ProviderAttempt.attempt_number)
        )
        return list(result.scalars().all())


async def reservation_status_for_request(gateway_request_id: int) -> str:
    from app.infrastructure.db.models import BudgetReservation
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BudgetReservation).where(BudgetReservation.gateway_request_id == gateway_request_id)
        )
        res = result.scalars().first()
        return res.status if res else None


def _make_use_cases(mock_provider, *, rate_limiter=None, budget_store=None):

    from app.api.deps import CompletionUseCases
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.redis.circuit_breaker import CircuitBreaker
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.infrastructure.observability.event_logger import LogEventSink
    from app.application.services.budget_authorizer import BudgetAuthorizer
    from app.application.services.token_estimator import TokenEstimator
    from app.application.services.routing_engine import RoutingEngine
    from app.application.services.response_validator import ResponseValidator
    from app.application.use_cases.execute_completion import ExecuteCompletion
    from app.application.use_cases.stream_completion import StreamCompletion

    store = budget_store or RedisBudgetStore()
    token_estimator = TokenEstimator()
    budget_authorizer = BudgetAuthorizer(store, store, token_estimator)
    routing = RoutingEngine(providers={
        "mock": mock_provider,
        "groq": mock_provider,
        "openai": mock_provider,
    })
    circuit = CircuitBreaker()
    rl = rate_limiter or RedisRateLimiter()
    event_sink = LogEventSink()
    validator = ResponseValidator()

    return CompletionUseCases(
        execute=ExecuteCompletion(budget_authorizer, routing, circuit, validator, rl, event_sink),
        stream=StreamCompletion(budget_authorizer, routing, circuit, validator, rl, event_sink, token_estimator),
    )

# Test 1: Happy path — non-streaming

@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mock_gateway", ["success"], indirect=True)
async def test_1_happy_non_stream(test_env, mock_gateway):
    trace_id = "smoke-non-stream"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    assert response.status_code == 200
    req = await latest_request_for_trace(trace_id)
    assert req is not None
    assert req.status == "completed"

    attempts = await attempts_for_request(req.id)
    assert len(attempts) == 1
    assert attempts[0].status == "success"

    res_status = await reservation_status_for_request(req.id)
    assert res_status == "settled"


# Test 2: Happy path — streaming

@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mock_gateway", ["stream_delta"], indirect=True)
async def test_2_happy_stream(test_env, mock_gateway):
    trace_id = "smoke-stream"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]

    lines = response.text.split("\n\n")
    assert any("mock " in line for line in lines)
    assert any("[DONE]" in line for line in lines)

    req = await latest_request_for_trace(trace_id)
    assert req.status == "completed"

    res_status = await reservation_status_for_request(req.id)
    assert res_status == "settled"


# Test 3: Budget exhausted before provider call

@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mock_gateway", ["success"], indirect=True)
async def test_3_budget_exhausted_before_provider(test_env, mock_gateway):
    # Set budget to zero
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(BudgetAccount)
            .where(BudgetAccount.tenant_id == test_env["tenant_id"])
            .values(monthly_limit_usd=Decimal("0.000000"))
        )
        await session.commit()

    # Also zero out the Redis budget counter so the Lua script sees limit=0
    from app.infrastructure.redis.client import get_redis
    r = get_redis()
    await r.delete(f"budget:{test_env['tenant_id']}:used")

    trace_id = "smoke-budget-fail"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    assert response.status_code == 429
    assert response.json()["error"]["code"] == "budget_exceeded"

    req = await latest_request_for_trace(trace_id)
    assert req.status == "budget_rejected"

    attempts = await attempts_for_request(req.id)
    assert len(attempts) == 0


# Test 4: Budget exhausted mid-stream

@pytest.mark.integration
@pytest.mark.asyncio
async def test_4_budget_exhausted_mid_stream(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class BudgetDrainMockProvider(MockProvider):
        async def stream(self, model: str, messages: list[dict], *, max_tokens: int):
            # Yield enough content to cross the 100-token threshold and exceed max_tokens
            for _ in range(15):
                yield ProviderStreamEvent(type="delta", content="token " * 10)
                await asyncio.sleep(0.01)

            yield ProviderStreamEvent(type="done")

    mock_provider = BudgetDrainMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-budget-mid-stream"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
            "max_tokens": 50,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    app.dependency_overrides.clear()

    assert response.status_code == 200
    lines = response.text.split("\n\n")
    # Ensure budget_exceeded error event was yielded before DONE
    assert any("budget_exceeded_mid_stream" in line for line in lines)

    req = await latest_request_for_trace(trace_id)
    # Finalizer should settle the reservation even on mid-stream abort
    res_status = await reservation_status_for_request(req.id)
    assert res_status == "settled"


# Test 5: Provider failure with fallback to another provider

@pytest.mark.integration
@pytest.mark.asyncio
async def test_5_provider_failure_with_fallback(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class FallbackMockProvider(MockProvider):
        async def complete(self, model: str, messages: list[dict], *, max_tokens: int):
            if model == "gpt-5.4-mini":
                raise self._wrap_error("timeout", "openai forced timeout", retryable=True)
            return await super().complete(model, messages, max_tokens=max_tokens)

    mock_provider = FallbackMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-fallback"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    app.dependency_overrides.clear()

    assert response.status_code == 200
    req = await latest_request_for_trace(trace_id)

    attempts = await attempts_for_request(req.id)
    # First attempt: timeout on gpt-5.4-mini, second: success on fallback model
    assert len(attempts) == 2
    assert attempts[0].status == "timeout"
    assert attempts[1].status == "success"


# Test 6: Empty output triggers validator failure and fallback

@pytest.mark.integration
@pytest.mark.asyncio
async def test_6_empty_output_fallback(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class EmptyOutputMockProvider(MockProvider):
        async def complete(self, model: str, messages: list[dict], *, max_tokens: int):
            if model == "gpt-5.4-mini":
                # gpt-5.4-mini succeeds normally (this is the fallback target)
                return await super().complete(model, messages, max_tokens=max_tokens)
            # All other models return empty content → triggers ResponseValidator failure
            res = await super().complete(model, messages, max_tokens=max_tokens)
            return dataclass_replace(res, content="")

    mock_provider = EmptyOutputMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-empty-output"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "openai/gpt-oss-20b",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    app.dependency_overrides.clear()

    assert response.status_code == 200
    req = await latest_request_for_trace(trace_id)
    attempts = await attempts_for_request(req.id)

    assert len(attempts) == 3
    assert [(attempt.model, attempt.status) for attempt in attempts] == [
        ("openai/gpt-oss-20b", "invalid_output"),
        ("mock-model", "invalid_output"),
        ("gpt-5.4-mini", "success"),
    ]


# Test 7: All providers unavailable

@pytest.mark.integration
@pytest.mark.asyncio
async def test_7_all_providers_unavailable(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class AllFailedMockProvider(MockProvider):
        async def complete(self, model: str, messages: list[dict], *, max_tokens: int):
            raise self._wrap_error("server_error", "forced failure", retryable=True)

    mock_provider = AllFailedMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-all-failed"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "provider_unavailable"
    assert response.json()["error"]["trace_id"] == trace_id

    req = await latest_request_for_trace(trace_id)
    res_status = await reservation_status_for_request(req.id)
    assert res_status == "settled"

# Test 8: Rate limit exceeded

@pytest.mark.integration
@pytest.mark.asyncio
async def test_8_rate_limit_exceeded(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.api.deps import get_completion_use_cases

    mock_provider = MockProvider(mode="success")
    rate_limiter = RedisRateLimiter(window_seconds=60, tenant_limit=1, api_key_limit=1)
    use_cases = _make_use_cases(mock_provider, rate_limiter=rate_limiter)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id_1 = "smoke-rate-limit-1"
    trace_id_2 = "smoke-rate-limit-2"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # First request — allowed
        res1 = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id_1})
        assert res1.status_code == 200

        # Second request — immediately throttled
        res2 = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id_2})

    app.dependency_overrides.clear()

    assert res2.status_code == 429
    assert res2.json()["error"]["code"] == "rate_limited"
    assert "Retry-After" in res2.headers

    req = await latest_request_for_trace(trace_id_2)
    assert req.status == "rate_limited"



# Test 9: Missing or invalid API key

@pytest.mark.integration
@pytest.mark.asyncio
async def test_9_missing_or_invalid_api_key(test_env):
    trace_id_1 = "smoke-no-key"
    trace_id_2 = "smoke-bad-key"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Missing key
        res1 = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-Trace-ID": trace_id_1})

        # Invalid key
        res2 = await ac.post("/v1/chat/completions", json={
            "model": "gpt-5.4-mini",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": False,
        }, headers={"X-API-Key": "bad-key-value", "X-Trace-ID": trace_id_2})

    assert res1.status_code == 401
    assert res1.json()["error"]["code"] == "authentication_failed"
    assert res1.json()["error"]["trace_id"] == trace_id_1

    assert res2.status_code == 401
    assert res2.json()["error"]["code"] == "authentication_failed"
    assert res2.json()["error"]["trace_id"] == trace_id_2


# Test 10: Database unavailable after auth but before provider call

@pytest.mark.integration
@pytest.mark.asyncio
async def test_10_database_unavailable_before_provider(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    mock_provider = MockProvider(mode="success")
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-db-down"

    # Patch AsyncSessionLocal only in execute_completion module so auth still works
    with patch(
        "app.application.use_cases.execute_completion.AsyncSessionLocal",
        side_effect=Exception("Simulated DB connection failure"),
    ):
        with patch.object(mock_provider, "complete", wraps=mock_provider.complete) as spy:
            transport = ASGITransport(app=app, raise_app_exceptions=False)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                response = await ac.post("/v1/chat/completions", json={
                    "model": "gpt-5.4-mini",
                    "messages": [{"role": "user", "content": "hello"}],
                    "stream": False,
                }, headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id})

            # Provider was never called because DB failed first
            assert spy.call_count == 0

    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"

@pytest.mark.integration
@pytest.mark.asyncio
async def test_budget_backend_unavailable_before_provider(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases
    from app.application.ports.budget_store import BudgetBackendUnavailable
    
    class UnavailableBudgetStore:
        async def try_reserve(self, request):
            raise BudgetBackendUnavailable()
        async def settle(self, *args, **kwargs):
            raise AssertionError("settle must not run without a reservation")
        async def remaining_usd(self, tenant_id: int) -> float:
            raise AssertionError("streaming was not entered")
        async def expire_stale_once(self) -> int:
            return 0

    mock_provider = MockProvider(mode="success")
    use_cases = _make_use_cases(
        mock_provider,
        budget_store=UnavailableBudgetStore(),
    )
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    with patch.object(mock_provider, "complete", wraps=mock_provider.complete) as spy:
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url="http://test") as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "gpt-5.4-mini",
                        "messages": [{"role": "user", "content": "hello"}],
                    },
                    headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": "test-budget-fail-closed"},
                )

            assert response.status_code == 503
            assert response.json()["error"]["code"] == "budget_backend_unavailable"
            
            # Assert no provider calls
            assert spy.call_count == 0
            
            req = await latest_request_for_trace("test-budget-fail-closed")
            assert req.status == "budget_backend_unavailable"
        finally:
            app.dependency_overrides.clear()