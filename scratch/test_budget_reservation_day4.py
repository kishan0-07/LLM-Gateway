import asyncio
from app.domain.budget import ReservationRequest
from app.infrastructure.redis.budget_store import RedisBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import Tenant, BudgetAccount, UsageLedger
from app.infrastructure.redis.client import get_redis
from sqlalchemy import select


async def setup_tenant(limit_usd: float) -> int:
    async with AsyncSessionLocal() as session:
        tenant = Tenant(name="day4-load-test")
        session.add(tenant)
        await session.flush()
        session.add(BudgetAccount(tenant_id=tenant.id, monthly_limit_usd=limit_usd))
        await session.commit()
        return tenant.id


async def test_concurrent_reserve():
    tenant_id = await setup_tenant(limit_usd=0.01)
    store = RedisBudgetStore()
    per_call_cost = 0.0005
    reqs = [ReservationRequest(tenant_id, 1, 100, per_call_cost) for _ in range(100)]

    results = await asyncio.gather(*(store.try_reserve(r) for r in reqs))
    approved = sum(1 for r in results if r.approved)
    print(f"approved={approved} rejected={100 - approved}")

    used_micros = int(await get_redis().get(f"budget:{tenant_id}:used") or 0)
    expected = round(approved * per_call_cost * 1_000_000)
    assert used_micros == expected, (
        f"drift: redis={used_micros} expected={expected} — double-spend or lost increment"
    )
    assert used_micros <= round(0.01 * 1_000_000), "counter exceeded the actual limit"
    print("NO DOUBLE-SPEND CONFIRMED")
    return results


async def test_idempotent_settlement(store: RedisBudgetStore, reservation_id: str):
    await asyncio.gather(
        store.settle(
            reservation_id, "groq", "openai/gpt-oss-20b", 50, 20, 0.0001, "success"
        ),
        store.settle(
            reservation_id, "groq", "openai/gpt-oss-20b", 50, 20, 0.0001, "success"
        ),
    )
    async with AsyncSessionLocal() as session:
        rows = (
            (
                await session.execute(
                    select(UsageLedger).where(
                        UsageLedger.reservation_id == reservation_id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1, (
        f"expected 1 ledger row, got {len(rows)} — settlement is NOT idempotent"
    )
    print("IDEMPOTENT SETTLEMENT CONFIRMED")


async def main():
    results = await test_concurrent_reserve()
    approved_id = next(r.reservation_id for r in results if r.approved)
    await test_idempotent_settlement(RedisBudgetStore(), approved_id)


if __name__ == "__main__":
    asyncio.run(main())
