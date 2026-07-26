import datetime

import pytest
from sqlalchemy import func, select, update

from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import (
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal


async def _reserved_request(test_env: dict, *, held_micros: int = 500):
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id="ledger-reconciliation",
            status="pending",
        )
        session.add(request)
        await session.commit()

    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        ReservationRequest(
            tenant_id=test_env["tenant_id"],
            gateway_request_id=request.id,
            requested_model="gpt-5.4-mini",
            estimated_input_tokens=10,
            estimated_output_tokens=20,
            estimated_tokens=30,
            estimated_cost_micros=held_micros,
        )
    )
    assert reservation.reservation_id is not None
    return store, request.id, reservation.reservation_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_attempt_is_not_released_as_free_usage(test_env):
    store, request_id, reservation_id = await _reserved_request(test_env)
    async with AsyncSessionLocal() as session:
        session.add(
            ProviderAttempt(
                gateway_request_id=request_id,
                provider="mock",
                model="gpt-5.4-mini",
                attempt_number=1,
                status="started",
            )
        )
        await session.execute(
            update(BudgetReservation)
            .where(BudgetReservation.id == reservation_id)
            .values(
                created_at=datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(hours=2)
            )
        )
        await session.commit()

    assert await store.expire_stale_once() == 0

    async with AsyncSessionLocal() as session:
        reservation = await session.get(BudgetReservation, reservation_id)
    assert reservation is not None
    assert reservation.status == "reserved"
    assert reservation.reconciliation_state == "needs_reconciliation"
    assert reservation.reconciliation_reason == "stale_with_provider_attempt"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reconciler_creates_one_conservative_attempt_ledger(test_env):
    store, request_id, reservation_id = await _reserved_request(
        test_env, held_micros=500
    )
    async with AsyncSessionLocal() as session:
        attempt = ProviderAttempt(
            gateway_request_id=request_id,
            provider="mock",
            model="gpt-5.4-mini",
            attempt_number=1,
            status="started",
            authorized_cost_micros=300,
            estimated_input_tokens=4,
            estimated_output_tokens=5,
        )
        session.add(attempt)
        await session.flush()
        attempt_id = attempt.id
        await session.execute(
            update(BudgetReservation)
            .where(BudgetReservation.id == reservation_id)
            .values(
                reconciliation_state="needs_reconciliation",
                reconciliation_reason="test_interruption",
                reconciliation_requested_at=(
                    datetime.datetime.now(datetime.timezone.utc)
                    - datetime.timedelta(minutes=5)
                ),
            )
        )
        await session.commit()

    assert await store.reconcile_needs_reconciliation_once() == 1
    assert await store.reconcile_needs_reconciliation_once() == 0

    async with AsyncSessionLocal() as session:
        reservation = await session.get(BudgetReservation, reservation_id)
        attempt = await session.get(ProviderAttempt, attempt_id)
        rows = list(
            (
                await session.execute(
                    select(UsageLedger).where(
                        UsageLedger.provider_attempt_id == attempt_id
                    )
                )
            )
            .scalars()
            .all()
        )
        ledger_count = await session.scalar(
            select(func.count(UsageLedger.id)).where(
                UsageLedger.provider_attempt_id == attempt_id
            )
        )

    assert reservation is not None
    assert reservation.status == "settled"
    assert reservation.final_status == "failed"
    assert reservation.reconciliation_state == "reconciled"
    assert reservation.held_micros == 0
    assert attempt is not None and attempt.status == "reconciled_estimate"
    assert ledger_count == 1
    assert rows[0].usage_source == "conservative"
    assert rows[0].billing_status == "estimated"
    assert rows[0].cost_micros == 300
    assert (rows[0].input_tokens, rows[0].output_tokens) == (4, 5)
