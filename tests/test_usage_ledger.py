import pytest
from sqlalchemy import select
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import UsageLedger, GatewayRequest
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.redis.client import get_redis
from app.domain.budget import ReservationRequest


@pytest.mark.integration
@pytest.mark.asyncio
async def test_successful_settlement_writes_ledger_row(test_env):
    """A settled reservation must produce exactly one usage_ledger row with actual usage."""
    store = RedisBudgetStore()
    r = get_redis()
    await r.delete(f"budget:{test_env['tenant_id']}:used")

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="ledger-success",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(ReservationRequest(
        tenant_id=test_env["tenant_id"],
        gateway_request_id=gw_id,
        estimated_tokens=200,
        estimated_cost_usd=0.005,
    ))
    assert result.approved

    await store.settle(
        reservation_id=result.reservation_id,
        provider="openai", model="gpt-5.4-mini",
        input_tokens=50, output_tokens=30,
        actual_cost_usd=0.002, status="success",
    )

    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(UsageLedger).where(UsageLedger.reservation_id == result.reservation_id)
        )).scalar_one()

    assert row.provider == "openai"
    assert row.model == "gpt-5.4-mini"
    assert row.input_tokens == 50
    assert row.output_tokens == 30
    assert float(row.cost_usd) == pytest.approx(0.002, abs=1e-6)
    assert row.usage_source == "actual"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_error_settlement_writes_estimated_usage(test_env):
    """A settlement with status='error' must write usage_source='estimated'."""
    store = RedisBudgetStore()
    r = get_redis()
    await r.delete(f"budget:{test_env['tenant_id']}:used")

    async with AsyncSessionLocal() as session:
        gw = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="ledger-error",
            status="pending",
            is_stream=False,
        )
        session.add(gw)
        await session.commit()
        gw_id = gw.id

    result = await store.try_reserve(ReservationRequest(
        tenant_id=test_env["tenant_id"],
        gateway_request_id=gw_id,
        estimated_tokens=200,
        estimated_cost_usd=0.005,
    ))
    assert result.approved

    await store.settle(
        reservation_id=result.reservation_id,
        provider="none", model="gpt-5.4-mini",
        input_tokens=0, output_tokens=0,
        actual_cost_usd=0.0, status="error",
    )

    async with AsyncSessionLocal() as session:
        row = (await session.execute(
            select(UsageLedger).where(UsageLedger.reservation_id == result.reservation_id)
        )).scalar_one()

    assert row.usage_source == "estimated"
    assert row.input_tokens == 0
    assert row.output_tokens == 0