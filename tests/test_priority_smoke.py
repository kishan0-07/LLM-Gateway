import pytest
from dataclasses import replace as dataclass_replace
from decimal import Decimal
from unittest.mock import patch
from httpx import AsyncClient, ASGITransport
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, update
from app.main import app
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import (
    BudgetAccount,
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.domain.budget import to_micros
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


async def reservation_for_request(gateway_request_id: int) -> BudgetReservation | None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(BudgetReservation).where(
                BudgetReservation.gateway_request_id == gateway_request_id
            )
        )
        return result.scalars().first()


async def ledger_for_request(gateway_request_id: int) -> list[UsageLedger]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(UsageLedger)
            .where(UsageLedger.gateway_request_id == gateway_request_id)
            .order_by(UsageLedger.provider_attempt_id)
        )
        return list(result.scalars().all())


def _make_use_cases(mock_provider, *, rate_limiter=None, budget_store=None):

    from app.api.deps import CompletionUseCases
    from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
    from app.infrastructure.redis.circuit_breaker import CircuitBreaker
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.infrastructure.observability.event_logger import LogEventSink
    from app.application.services.budget_authorizer import BudgetAuthorizer
    from app.application.services.token_estimator import TokenEstimator
    from app.application.services.routing_engine import RoutingEngine
    from app.application.services.response_validator import ResponseValidator
    from app.application.use_cases.execute_completion import ExecuteCompletion
    from app.application.use_cases.stream_completion import StreamCompletion

    store = budget_store or PostgreSQLBudgetStore()
    token_estimator = TokenEstimator()
    budget_authorizer = BudgetAuthorizer(store, store, token_estimator)
    routing = RoutingEngine(
        providers={
            "mock": mock_provider,
            "groq": mock_provider,
            "openai": mock_provider,
        }
    )
    circuit = CircuitBreaker()
    rl = rate_limiter or RedisRateLimiter()
    event_sink = LogEventSink()
    validator = ResponseValidator()

    return CompletionUseCases(
        execute=ExecuteCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rl,
            event_sink,
            token_estimator,
        ),
        stream=StreamCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rl,
            event_sink,
            token_estimator,
        ),
    )


# Test 1: Happy path — non-streaming


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mock_gateway", ["success"], indirect=True)
async def test_1_happy_non_stream(test_env, mock_gateway):
    trace_id = "smoke-non-stream"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    assert response.status_code == 200
    req = await latest_request_for_trace(trace_id)
    assert req is not None
    assert req.status == "completed"

    attempts = await attempts_for_request(req.id)
    assert len(attempts) == 1
    assert attempts[0].status == "success"

    reservation = await reservation_for_request(req.id)
    assert reservation is not None
    assert (reservation.status, reservation.final_status) == ("settled", "completed")
    assert len(await ledger_for_request(req.id)) == 1


# Test 2: Happy path — streaming


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("mock_gateway", ["stream_delta"], indirect=True)
async def test_2_happy_stream(test_env, mock_gateway):
    trace_id = "smoke-stream"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": True,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    assert response.headers.get_list("x-trace-id") == [trace_id]

    lines = response.text.split("\n\n")
    assert any("mock " in line for line in lines)
    assert any("[DONE]" in line for line in lines)

    req = await latest_request_for_trace(trace_id)
    assert req.status == "completed"

    reservation = await reservation_for_request(req.id)
    assert reservation is not None
    assert (reservation.status, reservation.final_status) == ("settled", "completed")
    assert len(await ledger_for_request(req.id)) == 1


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

    trace_id = "smoke-budget-fail"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    assert response.status_code == 429
    assert response.json()["error"]["code"] == "budget_exceeded"

    req = await latest_request_for_trace(trace_id)
    assert req.status == "budget_rejected"

    attempts = await attempts_for_request(req.id)
    assert len(attempts) == 0


# Test 4: Provider-reported overrun is preserved for reconciliation


@pytest.mark.integration
@pytest.mark.asyncio
async def test_4_stream_usage_overrun_is_not_lost(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class UsageOverrunMockProvider(MockProvider):
        async def stream(self, model: str, messages: list[dict], *, max_tokens: int):
            assert max_tokens == 1
            yield ProviderStreamEvent(type="delta", content="ok")
            yield ProviderStreamEvent(
                type="usage",
                input_tokens=10,
                output_tokens=1_000_000,
            )
            yield ProviderStreamEvent(type="done")

    mock_provider = UsageOverrunMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-stream-overrun"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": True,
                "max_tokens": 1,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "[DONE]" in response.text

    req = await latest_request_for_trace(trace_id)
    reservation = await reservation_for_request(req.id)
    ledger = await ledger_for_request(req.id)
    assert reservation is not None
    assert reservation.status == "settled"
    assert reservation.reconciliation_state == "needs_reconciliation"
    assert len(ledger) == 1
    assert ledger[0].output_tokens == 1_000_000


# Test 5: Provider failure with fallback to another provider


@pytest.mark.integration
@pytest.mark.asyncio
async def test_5_provider_failure_with_fallback(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases

    class FallbackMockProvider(MockProvider):
        async def complete(self, model: str, messages: list[dict], *, max_tokens: int):
            if model == "gpt-5.4-mini":
                raise self._wrap_error(
                    "timeout", "openai forced timeout", retryable=True
                )
            return await super().complete(model, messages, max_tokens=max_tokens)

    mock_provider = FallbackMockProvider()
    use_cases = _make_use_cases(mock_provider)
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    trace_id = "smoke-fallback"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    req = await latest_request_for_trace(trace_id)

    attempts = await attempts_for_request(req.id)
    # First attempt: timeout on gpt-5.4-mini, second: success on fallback model
    assert len(attempts) == 2
    assert attempts[0].status == "timeout"
    assert attempts[1].status == "success"
    ledger = await ledger_for_request(req.id)
    assert len(ledger) == 2
    assert [row.usage_source for row in ledger] == ["conservative", "actual"]
    assert to_micros(response.json()["usage"]["cost_usd"]) == sum(
        row.cost_micros for row in ledger
    )


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
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "openai/gpt-oss-20b",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

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
    ledger = await ledger_for_request(req.id)
    reservation = await reservation_for_request(req.id)

    assert reservation is not None
    assert (reservation.status, reservation.final_status) == (
        "settled",
        "completed",
    )
    assert reservation.held_micros == 0
    assert len(ledger) == len(attempts) == 3
    assert {row.provider_attempt_id for row in ledger} == {
        attempt.id for attempt in attempts
    }
    assert reservation.consumed_micros == sum(row.cost_micros for row in ledger)
    assert to_micros(response.json()["usage"]["cost_usd"]) == sum(
        row.cost_micros for row in ledger
    )


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
        response = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
        )

    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "provider_unavailable"
    assert response.json()["error"]["trace_id"] == trace_id

    req = await latest_request_for_trace(trace_id)
    reservation = await reservation_for_request(req.id)
    assert reservation is not None
    assert (reservation.status, reservation.final_status) == ("settled", "failed")
    attempts = await attempts_for_request(req.id)
    ledger = await ledger_for_request(req.id)
    assert len(attempts) == 4
    assert len(ledger) == len(attempts)


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
        res1 = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id_1},
        )
        assert res1.status_code == 200

        # Second request — immediately throttled
        res2 = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id_2},
        )

    app.dependency_overrides.clear()

    assert res2.status_code == 429
    assert res2.json()["error"]["code"] == "rate_limited"
    assert "Retry-After" in res2.headers

    req = await latest_request_for_trace(trace_id_2)
    assert req.status == "rate_limited"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rate_limiter_backend_unavailable_fails_closed_before_provider(
    test_env,
):
    from app.api.deps import get_completion_use_cases
    from app.application.ports.rate_limiter import RateLimitBackendUnavailable
    from app.infrastructure.providers.mock import MockProvider

    class UnavailableRateLimiter:
        async def check(self, tenant_id: int, api_key_id: int) -> None:
            raise RateLimitBackendUnavailable()

    provider = MockProvider(mode="success")
    use_cases = _make_use_cases(
        provider,
        rate_limiter=UnavailableRateLimiter(),
    )
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases
    trace_id = "smoke-rate-limiter-unavailable"

    try:
        with patch.object(provider, "complete", wraps=provider.complete) as spy:
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport,
                base_url="http://test",
            ) as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "gpt-5.4-mini",
                        "messages": [{"role": "user", "content": "hello"}],
                    },
                    headers={
                        "X-API-Key": test_env["api_key"],
                        "X-Trace-ID": trace_id,
                    },
                )

            assert spy.call_count == 0
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "rate_limiter_unavailable"
    assert response.headers["Retry-After"] == "1"

    request_row = await latest_request_for_trace(trace_id)
    assert request_row.status == "rate_limit_unavailable"
    assert await attempts_for_request(request_row.id) == []
    assert await ledger_for_request(request_row.id) == []
    assert await reservation_for_request(request_row.id) is None


# Test 9: Missing or invalid API key


@pytest.mark.integration
@pytest.mark.asyncio
async def test_9_missing_or_invalid_api_key(test_env):
    trace_id_1 = "smoke-no-key"
    trace_id_2 = "smoke-bad-key"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Missing key
        res1 = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-Trace-ID": trace_id_1},
        )

        # Invalid key
        res2 = await ac.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": False,
            },
            headers={"X-API-Key": "bad-key-value", "X-Trace-ID": trace_id_2},
        )

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
        side_effect=SQLAlchemyError("Simulated DB connection failure"),
    ):
        with patch.object(
            mock_provider, "complete", wraps=mock_provider.complete
        ) as spy:
            transport = ASGITransport(app=app, raise_app_exceptions=False)
            async with AsyncClient(transport=transport, base_url="http://test") as ac:
                response = await ac.post(
                    "/v1/chat/completions",
                    json={
                        "model": "gpt-5.4-mini",
                        "messages": [{"role": "user", "content": "hello"}],
                        "stream": False,
                    },
                    headers={"X-API-Key": test_env["api_key"], "X-Trace-ID": trace_id},
                )

            # Provider was never called because DB failed first
            assert spy.call_count == 0

    app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "database_unavailable"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_budget_database_unavailable_before_provider(test_env):
    from app.infrastructure.providers.mock import MockProvider
    from app.api.deps import get_completion_use_cases
    from app.application.ports.budget_store import DatabaseUnavailable

    class UnavailableBudgetStore:
        async def try_reserve(self, request):
            raise DatabaseUnavailable()

    mock_provider = MockProvider(mode="success")
    use_cases = _make_use_cases(
        mock_provider,
        budget_store=UnavailableBudgetStore(),
    )
    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases

    with patch.object(mock_provider, "complete", wraps=mock_provider.complete) as spy:
        try:
            transport = ASGITransport(app=app)
            async with AsyncClient(
                transport=transport, base_url="http://test"
            ) as client:
                response = await client.post(
                    "/v1/chat/completions",
                    json={
                        "model": "gpt-5.4-mini",
                        "messages": [{"role": "user", "content": "hello"}],
                    },
                    headers={
                        "X-API-Key": test_env["api_key"],
                        "X-Trace-ID": "test-budget-fail-closed",
                    },
                )

            assert response.status_code == 503
            assert response.json()["error"]["code"] == "database_unavailable"

            # Assert no provider calls
            assert spy.call_count == 0

        finally:
            app.dependency_overrides.clear()
