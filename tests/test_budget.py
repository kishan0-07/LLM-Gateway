import pytest
import asyncio
from decimal import Decimal
from sqlalchemy import select, update
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.redis.client import get_redis
from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import BudgetReservation, BudgetAccount, UsageLedger


async def create_gateway_request_for_test(test_env, trace_id: str) -> int:
    from app.infrastructure.db.models import GatewayRequest
    from app.infrastructure.db.session import AsyncSessionLocal

    async with AsyncSessionLocal() as session:
        req = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env.get("api_key_id"),
            trace_id=trace_id,
        )
        session.add(req)
        await session.commit()
        return req.id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reservation_creates_postgres_row(test_env):

    store = RedisBudgetStore()
    # Create a fake gateway_request_id (we need one because of FK)
    from app.infrastructure.db.models import GatewayRequest

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="test-budget-reserve",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )
    )
    assert result.approved is True
    assert result.reservation_id is not None

    # Verify Postgres row exists
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        assert row.status == "reserved"
        assert row.tenant_id == test_env["tenant_id"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reservation_rejected_when_over_budget(test_env):
    from app.infrastructure.redis.budget_store import budget_counter_key

    # Set budget to $0
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(BudgetAccount)
            .where(BudgetAccount.tenant_id == test_env["tenant_id"])
            .values(monthly_limit_usd=Decimal("0.0000"))
        )
        await session.commit()

    r = get_redis()
    await r.delete(budget_counter_key(test_env["tenant_id"]))

    store = RedisBudgetStore()
    from app.infrastructure.db.models import GatewayRequest

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="test-budget-reject",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )
    )
    assert result.approved is False
    assert result.reason == "over_budget"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_settlement_is_idempotent(test_env):
    """Calling settle() twice with the same reservation_id must not create duplicate ledger rows."""
    store = RedisBudgetStore()
    from app.infrastructure.db.models import GatewayRequest

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="test-settlement-idempotent",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )
    )
    assert result.approved

    # Settle once
    await store.settle(
        reservation_id=result.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.0005,
        status="success",
        usage_source="actual",
    )

    # Settle again -
    await store.settle(
        reservation_id=result.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.0005,
        status="success",
        usage_source="actual",
    )

    # Verify only 1 ledger row
    async with AsyncSessionLocal() as session:
        rows = (
            (
                await session.execute(
                    select(UsageLedger).where(
                        UsageLedger.reservation_id == result.reservation_id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_settlement_corrects_redis_counter(test_env):
    """Settlement must true-up the Redis counter when actual cost differs from estimated."""
    from app.infrastructure.redis.budget_store import budget_counter_key

    store = RedisBudgetStore()
    r = get_redis()
    await r.delete(budget_counter_key(test_env["tenant_id"]))

    from app.infrastructure.db.models import GatewayRequest

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="test-settlement-trueup",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    estimated_cost = 0.010  # overestimate
    actual_cost = 0.002  # actual is much less

    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=500,
            estimated_cost_usd=estimated_cost,
        )
    )
    assert result.approved

    used_after_reserve = int(
        await r.get(budget_counter_key(test_env["tenant_id"])) or 0
    )

    await store.settle(
        reservation_id=result.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=actual_cost,
        status="success",
        usage_source="actual",
    )

    used_after_settle = int(await r.get(budget_counter_key(test_env["tenant_id"])) or 0)

    # After settlement, the Redis counter should have decreased (true-up refund)
    assert used_after_settle < used_after_reserve


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_reservation_expiry(test_env):

    import datetime
    from app.infrastructure.redis.budget_store import budget_counter_key

    store = RedisBudgetStore()
    r = get_redis()
    await r.delete(budget_counter_key(test_env["tenant_id"]))

    from app.infrastructure.db.models import GatewayRequest

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="test-stale-expiry",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            gateway_request_id=gw_id,
            estimated_tokens=100,
            estimated_cost_usd=0.005,
        )
    )
    assert result.approved

    # Manually backdate the reservation to 2 hours ago
    async with AsyncSessionLocal() as session:
        res = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        res.created_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(hours=2)
        await session.commit()

    used_before = int(await r.get(budget_counter_key(test_env["tenant_id"])) or 0)

    # Run reconciler
    expired_count = await store.expire_stale_once()
    assert expired_count >= 1

    # Verify reservation is expired
    async with AsyncSessionLocal() as session:
        res = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        assert res.status == "expired"

    used_after = int(await r.get(budget_counter_key(test_env["tenant_id"])) or 0)
    assert used_after < used_before


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_reservations_no_double_spend(test_env):
    """100 concurrent reservations against a $0.10 budget must not overspend."""
    from app.infrastructure.redis.budget_store import budget_counter_key

    store = RedisBudgetStore()
    r = get_redis()
    await r.delete(budget_counter_key(test_env["tenant_id"]))

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(BudgetAccount)
            .where(BudgetAccount.tenant_id == test_env["tenant_id"])
            .values(monthly_limit_usd=Decimal("0.1000"))
        )
        await session.commit()

    from app.infrastructure.db.models import GatewayRequest

    async def make_reservation(i: int):
        async with AsyncSessionLocal() as session:
            gw = GatewayRequest(
                tenant_id=test_env["tenant_id"],
                api_key_id=test_env["api_key_id"],
                trace_id=f"concurrent-{i}",
                status="pending",
                is_stream=False,
            )
            session.add(gw)
            await session.commit()
            gw_id = gw.id

        return await store.try_reserve(
            ReservationRequest(
                tenant_id=test_env["tenant_id"],
                gateway_request_id=gw_id,
                requested_model="mock-model",
                estimated_input_tokens=50,
                estimated_output_tokens=50,
                estimated_tokens=100,
                estimated_cost_usd=0.01,
            )
        )

    results = await asyncio.gather(*[make_reservation(i) for i in range(100)])
    approved_count = sum(1 for r in results if r.approved)

    assert approved_count == 10, f"Expected 10 approvals, got {approved_count}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_settlement_increases_counter_when_actual_cost_exceeds_estimate(test_env):
    from app.infrastructure.redis.budget_store import (
        RedisBudgetStore,
        budget_counter_key,
    )
    from app.domain.budget import ReservationRequest
    from app.infrastructure.redis.client import get_redis

    store = RedisBudgetStore()
    redis_client = get_redis()
    key = budget_counter_key(test_env["tenant_id"])
    await redis_client.delete(key)

    gateway_request_id = await create_gateway_request_for_test(
        test_env,
        trace_id="under-estimate-true-up",
    )
    reservation = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gateway_request_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=10,
            estimated_cost_usd=0.001,
        )
    )
    assert reservation.approved

    used_after_reserve = int(await redis_client.get(key) or 0)

    await store.settle(
        reservation_id=reservation.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=100,
        output_tokens=100,
        actual_cost_usd=0.003,
        status="success",
        usage_source="actual",
    )

    used_after_settle = int(await redis_client.get(key) or 0)
    assert used_after_settle > used_after_reserve


@pytest.mark.integration
@pytest.mark.asyncio
async def test_settlement_with_missing_month_key_flags_cache_sync(test_env):
    """When Redis month key is deleted before settlement, cache_sync_required must be True."""
    from app.infrastructure.redis.budget_store import (
        RedisBudgetStore,
        budget_counter_key,
    )

    store = RedisBudgetStore()
    r = get_redis()

    gw_id = await create_gateway_request_for_test(test_env, "test-cache-miss")
    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )
    )
    assert result.approved

    # Simulate Redis cache loss
    await r.delete(budget_counter_key(test_env["tenant_id"]))

    # Settle — should detect missing key and flag for repair
    await store.settle(
        reservation_id=result.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.0005,
        status="success",
        usage_source="actual",
    )

    # Verify the flag is set
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        assert row.status == "settled"
        assert row.cache_sync_required is True


@pytest.mark.integration
@pytest.mark.asyncio
async def test_repair_rebuilds_counter_with_positive_ttl(test_env):
    """After cache-miss settlement, repair must rebuild counter with a finite TTL."""
    from app.infrastructure.redis.budget_store import (
        RedisBudgetStore,
        budget_counter_key,
    )

    store = RedisBudgetStore()
    r = get_redis()
    key = budget_counter_key(test_env["tenant_id"])

    # Force a cache_sync_required flag (reuse the settlement test logic)
    gw_id = await create_gateway_request_for_test(test_env, "test-repair-ttl")
    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.001,
        )
    )
    assert result.approved
    await r.delete(key)
    await store.settle(
        reservation_id=result.reservation_id,
        provider="mock",
        model="mock-model",
        input_tokens=10,
        output_tokens=5,
        actual_cost_usd=0.0005,
        status="success",
        usage_source="actual",
    )

    # Run repair
    repaired = await store.repair_out_of_sync_caches_once()
    assert repaired >= 1

    # Verify counter exists with a finite TTL
    ttl = await r.ttl(key)
    assert ttl > 0, f"Rebuilt counter must have a positive TTL, got {ttl}"

    # Verify flag is cleared
    async with AsyncSessionLocal() as session:
        row = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        assert row.cache_sync_required is False


@pytest.mark.integration
@pytest.mark.asyncio
async def test_needs_reconciliation_gets_conservative_settlement(test_env):
    """A stale reservation with a provider attempt must be conservatively settled."""
    import datetime
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.db.models import (
        ProviderAttempt,
        BudgetReservation,
        UsageLedger,
    )

    store = RedisBudgetStore()

    gw_id = await create_gateway_request_for_test(test_env, "test-reconcile")
    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=50,
            estimated_output_tokens=50,
            estimated_tokens=100,
            estimated_cost_usd=0.005,
        )
    )
    assert result.approved

    # Create a provider attempt (simulating a stream that started but didn't finish)
    async with AsyncSessionLocal() as session:
        attempt = ProviderAttempt(
            gateway_request_id=gw_id,
            provider="mock",
            model="mock-model",
            attempt_number=1,
            status="in_progress",
        )
        session.add(attempt)

        # Backdate reservation past grace period
        res = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        res.created_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(hours=2)
        res.reconciliation_state = "needs_reconciliation"
        res.reconciliation_reason = "settlement_failed_after_provider_attempt"
        await session.commit()

    # Run reconciler
    count = await store.reconcile_needs_reconciliation_once()
    assert count == 1

    # Verify: reservation settled with estimated usage
    async with AsyncSessionLocal() as session:
        res = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        assert res.status == "settled"
        assert res.reconciliation_state == "reconciled"

        ledger = (
            await session.execute(
                select(UsageLedger).where(
                    UsageLedger.reservation_id == result.reservation_id
                )
            )
        ).scalar_one()
        assert ledger.usage_source == "estimated_reconciliation"
        assert ledger.input_tokens == 50
        assert ledger.output_tokens == 50


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconciliation_idempotent(test_env):
    """Re-running the reconciler must not create a second ledger row."""
    import datetime
    from app.infrastructure.redis.budget_store import RedisBudgetStore
    from app.infrastructure.db.models import (
        ProviderAttempt,
        BudgetReservation,
        UsageLedger,
    )

    store = RedisBudgetStore()

    gw_id = await create_gateway_request_for_test(test_env, "test-reconcile-idempotent")
    result = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=gw_id,
            requested_model="mock-model",
            estimated_input_tokens=30,
            estimated_output_tokens=30,
            estimated_tokens=60,
            estimated_cost_usd=0.003,
        )
    )
    assert result.approved

    async with AsyncSessionLocal() as session:
        session.add(
            ProviderAttempt(
                gateway_request_id=gw_id,
                provider="mock",
                model="mock-model",
                attempt_number=1,
                status="in_progress",
            )
        )
        res = (
            await session.execute(
                select(BudgetReservation).where(
                    BudgetReservation.id == result.reservation_id
                )
            )
        ).scalar_one()
        res.created_at = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.timedelta(hours=2)
        res.reconciliation_state = "needs_reconciliation"
        await session.commit()

    # Run twice
    count1 = await store.reconcile_needs_reconciliation_once()
    count2 = await store.reconcile_needs_reconciliation_once()
    assert count1 == 1
    assert count2 == 0  # Already reconciled — must not repeat

    # Verify exactly one ledger row
    async with AsyncSessionLocal() as session:
        rows = (
            (
                await session.execute(
                    select(UsageLedger).where(
                        UsageLedger.reservation_id == result.reservation_id
                    )
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
