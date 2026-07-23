import pytest
import pytest_asyncio
import hashlib
import time
from decimal import Decimal
from sqlalchemy import text, delete, select
from app.main import app
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import (
    Tenant,
    ApiKey,
    BudgetAccount,
    GatewayRequest,
    ProviderAttempt,
    BudgetReservation,
    UsageLedger,
)
from app.infrastructure.redis.client import get_redis
from app.api.deps import get_completion_use_cases, CompletionUseCases


@pytest_asyncio.fixture(scope="session")
async def _check_infra():
    """Ensure Postgres and Redis are reachable before running any tests."""
    from app.infrastructure.db.session import engine

    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"Postgres not available: {exc}")

    try:
        r = get_redis()
        await r.ping()
    except Exception as exc:
        pytest.skip(f"Redis not available: {exc}")


@pytest_asyncio.fixture
async def test_env(_check_infra):
    """Yields a unique test tenant, active key, and budget account, cleaning up after."""
    tenant_name = f"test-tenant-{int(time.time() * 1000)}"
    api_key_str = f"test-key-{int(time.time() * 1000)}"

    async with AsyncSessionLocal() as session:
        tenant = Tenant(name=tenant_name)
        session.add(tenant)
        await session.flush()

        key_hash = hashlib.sha256(api_key_str.encode()).hexdigest()
        api_key = ApiKey(
            tenant_id=tenant.id,
            prefix="test",
            key_hash=key_hash,
            status="active",
        )
        session.add(api_key)

        budget = BudgetAccount(
            tenant_id=tenant.id,
            monthly_limit_usd=Decimal("100.00"),
        )
        session.add(budget)
        await session.commit()

        tenant_id = tenant.id
        api_key_id = api_key.id

    yield {
        "tenant_id": tenant_id,
        "api_key_id": api_key_id,
        "api_key": api_key_str,
    }

    # --- Cleanup (order matters due to FK constraints) ---
    async with AsyncSessionLocal() as session:
        # 1. Find all gateway_request IDs for this tenant
        gw_ids_result = await session.execute(
            select(GatewayRequest.id).where(GatewayRequest.tenant_id == tenant_id)
        )
        gw_ids = [row[0] for row in gw_ids_result.all()]

        if gw_ids:
            # 2. Delete usage_ledger rows first (RESTRICT FK on reservation_id)
            await session.execute(
                delete(UsageLedger).where(UsageLedger.gateway_request_id.in_(gw_ids))
            )
            # 3. Delete budget_reservations
            await session.execute(
                delete(BudgetReservation).where(
                    BudgetReservation.gateway_request_id.in_(gw_ids)
                )
            )
            # 4. Delete provider_attempts
            await session.execute(
                delete(ProviderAttempt).where(
                    ProviderAttempt.gateway_request_id.in_(gw_ids)
                )
            )
            # 5. Delete gateway_requests
            await session.execute(
                delete(GatewayRequest).where(GatewayRequest.tenant_id == tenant_id)
            )

        # 6. Delete budget_account, api_key, tenant (CASCADE handles api_key & budget)
        await session.execute(
            delete(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
        )
        await session.execute(delete(ApiKey).where(ApiKey.tenant_id == tenant_id))
        await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
        await session.commit()

    # 7. Clean up Redis keys
    r = get_redis()
    await r.delete(f"budget:{tenant_id}:used")
    await r.delete(f"rate_limit:tenant:{tenant_id}")
    await r.delete(f"rate_limit:api_key:{api_key_id}")


@pytest.fixture
def mock_gateway(request):
    """Configures dependency overrides to use MockProvider instead of real providers."""
    mode = getattr(request, "param", "success")

    from app.infrastructure.providers.mock import MockProvider
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

    mock_provider = MockProvider(mode=mode)
    budget_store = RedisBudgetStore()
    token_estimator = TokenEstimator()
    budget_authorizer = BudgetAuthorizer(
        budget_store=budget_store,
        usage_ledger=budget_store,
        token_estimator=token_estimator,
    )

    routing = RoutingEngine(
        providers={
            "mock": mock_provider,
            "groq": mock_provider,
            "openai": mock_provider,
        }
    )
    circuit = CircuitBreaker()
    rate_limiter = RedisRateLimiter()
    event_sink = LogEventSink()
    validator = ResponseValidator()

    use_cases = CompletionUseCases(
        execute=ExecuteCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rate_limiter,
            event_sink,
            token_estimator,
        ),
        stream=StreamCompletion(
            budget_authorizer,
            routing,
            circuit,
            validator,
            rate_limiter,
            event_sink,
            token_estimator,
        ),
    )

    app.dependency_overrides[get_completion_use_cases] = lambda: use_cases
    yield mock_provider
    app.dependency_overrides.clear()
