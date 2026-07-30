import asyncio
import datetime
import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, update

from app.domain.budget import ReservationRequest
from app.infrastructure.db.models import (
    BudgetAccount,
    BudgetPeriod,
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.postgres_budget_store import PostgreSQLBudgetStore
from app.infrastructure.db.session import AsyncSessionLocal


async def _create_request(test_env: dict, *, suffix: str | None = None) -> int:
    async with AsyncSessionLocal() as session:
        request = GatewayRequest(
            tenant_id=test_env["tenant_id"],
            api_key_id=test_env["api_key_id"],
            trace_id=f"budget-{suffix or uuid.uuid4().hex}",
            status="pending",
        )
        session.add(request)
        await session.commit()
        return request.id


async def _create_attempt(request_id: int, attempt_number: int) -> int:
    async with AsyncSessionLocal() as session:
        attempt = ProviderAttempt(
            gateway_request_id=request_id,
            provider="mock",
            model="gpt-5.4-mini",
            attempt_number=attempt_number,
            status="started",
        )
        session.add(attempt)
        await session.commit()
        return attempt.id


def _reservation_request(
    test_env: dict,
    request_id: int,
    *,
    cost_micros: int,
) -> ReservationRequest:
    return ReservationRequest(
        tenant_id=test_env["tenant_id"],
        gateway_request_id=request_id,
        requested_model="gpt-5.4-mini",
        estimated_input_tokens=10,
        estimated_output_tokens=20,
        estimated_tokens=30,
        estimated_cost_micros=cost_micros,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reservation_is_authoritative_in_postgres(test_env):
    request_id = await _create_request(test_env)
    store = PostgreSQLBudgetStore()

    result = await store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=2_500)
    )

    assert result.approved is True
    assert result.reservation_id is not None

    async with AsyncSessionLocal() as session:
        reservation = await session.get(BudgetReservation, result.reservation_id)
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )

    assert reservation is not None
    assert reservation.held_micros == 2_500
    assert reservation.consumed_micros == 0
    assert period is not None
    assert period.reserved_micros == 2_500
    assert period.spent_micros == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_outage_cannot_reject_postgres_budget_reservation(test_env):
    request_id = await _create_request(test_env)
    store = PostgreSQLBudgetStore()

    with patch(
        "app.infrastructure.redis.client.get_redis",
        side_effect=ConnectionError("redis unavailable"),
    ):
        result = await store.try_reserve(
            _reservation_request(test_env, request_id, cost_micros=500)
        )

    assert result.approved is True
    assert result.reservation_id is not None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_reservations_cannot_overspend(test_env):
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(BudgetAccount)
            .where(BudgetAccount.tenant_id == test_env["tenant_id"])
            .values(monthly_limit_usd="0.0010")
        )
        await session.commit()

    request_ids = [
        await _create_request(test_env, suffix=f"concurrent-{index}")
        for index in range(25)
    ]
    store = PostgreSQLBudgetStore()

    results = await asyncio.gather(
        *[
            store.try_reserve(
                _reservation_request(test_env, request_id, cost_micros=100)
            )
            for request_id in request_ids
        ]
    )

    assert sum(result.approved for result in results) == 10
    approved = [
        (request_id, result.reservation_id)
        for request_id, result in zip(request_ids, results, strict=True)
        if result.approved and result.reservation_id is not None
    ]
    attempt_ids = await asyncio.gather(
        *[_create_attempt(request_id, 1) for request_id, _reservation_id in approved]
    )
    await asyncio.gather(
        *[
            store.record_attempt_usage(
                reservation_id=reservation_id,
                provider_attempt_id=attempt_id,
                provider="mock",
                model="gpt-5.4-mini",
                input_tokens=1,
                output_tokens=1,
                cost_micros=100,
                usage_source="actual",
                attempt_status="success",
                latency_ms=1,
            )
            for (_request_id, reservation_id), attempt_id in zip(
                approved, attempt_ids, strict=True
            )
        ]
    )
    await asyncio.gather(
        *[
            store.finalize_reservation(
                reservation_id=reservation_id,
                final_status="completed",
            )
            for _request_id, reservation_id in approved
        ]
    )

    async with AsyncSessionLocal() as session:
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        ledger_total = await session.scalar(
            select(func.coalesce(func.sum(UsageLedger.cost_micros), 0)).where(
                UsageLedger.tenant_id == test_env["tenant_id"]
            )
        )
        active_hold_total = await session.scalar(
            select(func.coalesce(func.sum(BudgetReservation.held_micros), 0)).where(
                BudgetReservation.tenant_id == test_env["tenant_id"],
                BudgetReservation.status == "reserved",
            )
        )
    assert period is not None
    assert period.reserved_micros == 0
    assert period.spent_micros == 1_000
    assert period.reserved_micros + period.spent_micros <= period.limit_micros
    assert period.spent_micros == ledger_total
    assert period.reserved_micros == active_hold_total


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reservation_retry_is_idempotent(test_env):
    request_id = await _create_request(test_env)
    store = PostgreSQLBudgetStore()
    request = _reservation_request(test_env, request_id, cost_micros=900)

    first, second = await asyncio.gather(
        store.try_reserve(request),
        store.try_reserve(request),
    )

    assert first.approved is True
    assert second.approved is True
    assert first.reservation_id == second.reservation_id

    async with AsyncSessionLocal() as session:
        reservation_count = await session.scalar(
            select(func.count(BudgetReservation.id)).where(
                BudgetReservation.gateway_request_id == request_id
            )
        )
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
    assert reservation_count == 1
    assert period is not None
    assert period.reserved_micros == 900


@pytest.mark.integration
@pytest.mark.asyncio
async def test_attempt_accounting_is_idempotent_and_updates_attempt(test_env):
    request_id = await _create_request(test_env)
    attempt_id = await _create_attempt(request_id, 1)
    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=1_000)
    )
    assert reservation.reservation_id is not None

    usage = dict(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=attempt_id,
        provider="mock",
        model="gpt-5.4-mini",
        input_tokens=10,
        output_tokens=20,
        cost_micros=400,
        usage_source="actual",
        attempt_status="success",
        latency_ms=12,
    )
    await store.record_attempt_usage(**usage)
    await store.record_attempt_usage(**usage)

    async with AsyncSessionLocal() as session:
        ledger_count = await session.scalar(
            select(func.count(UsageLedger.id)).where(
                UsageLedger.provider_attempt_id == attempt_id
            )
        )
        row = await session.scalar(
            select(UsageLedger).where(UsageLedger.provider_attempt_id == attempt_id)
        )
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        stored_reservation = await session.get(
            BudgetReservation, reservation.reservation_id
        )
        attempt = await session.get(ProviderAttempt, attempt_id)

    assert ledger_count == 1
    assert row is not None and row.billing_status == "known"
    assert period is not None
    assert period.reserved_micros == 600
    assert period.spent_micros == 400
    assert stored_reservation is not None
    assert stored_reservation.held_micros == 600
    assert stored_reservation.consumed_micros == 400
    assert attempt is not None
    assert (attempt.status, attempt.latency_ms) == ("success", 12)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_fallback_attempts_are_both_billed_and_finalization_releases_hold(
    test_env,
):
    request_id = await _create_request(test_env)
    first_attempt = await _create_attempt(request_id, 1)
    second_attempt = await _create_attempt(request_id, 2)
    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=100)
    )
    assert reservation.reservation_id is not None

    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=first_attempt,
        provider="groq",
        model="openai/gpt-oss-20b",
        input_tokens=5,
        output_tokens=5,
        cost_micros=40,
        usage_source="conservative",
        attempt_status="timeout",
        latency_ms=100,
    )
    assert await store.ensure_attempt_capacity(
        reservation_id=reservation.reservation_id,
        required_micros=100,
    )
    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=second_attempt,
        provider="openai",
        model="gpt-5.4-mini",
        input_tokens=5,
        output_tokens=5,
        cost_micros=30,
        usage_source="actual",
        attempt_status="success",
        latency_ms=20,
    )
    await store.finalize_reservation(
        reservation_id=reservation.reservation_id,
        final_status="completed",
        gateway_overhead_ms=7,
    )

    async with AsyncSessionLocal() as session:
        ledger_rows = list(
            (
                await session.execute(
                    select(UsageLedger)
                    .where(UsageLedger.gateway_request_id == request_id)
                    .order_by(UsageLedger.provider_attempt_id)
                )
            )
            .scalars()
            .all()
        )
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        stored_reservation = await session.get(
            BudgetReservation, reservation.reservation_id
        )
        request = await session.get(GatewayRequest, request_id)

    assert [row.cost_micros for row in ledger_rows] == [40, 30]
    assert period is not None
    assert (period.reserved_micros, period.spent_micros) == (0, 70)
    assert stored_reservation is not None
    assert stored_reservation.final_status == "completed"
    assert stored_reservation.held_micros == 0
    assert request is not None
    assert (request.status, request.gateway_overhead_ms) == ("completed", 7)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_actual_cost_overrun_is_recorded_and_marked_for_reconciliation(test_env):
    request_id = await _create_request(test_env)
    attempt_id = await _create_attempt(request_id, 1)
    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=100)
    )
    assert reservation.reservation_id is not None

    await store.record_attempt_usage(
        reservation_id=reservation.reservation_id,
        provider_attempt_id=attempt_id,
        provider="mock",
        model="gpt-5.4-mini",
        input_tokens=10,
        output_tokens=100,
        cost_micros=150,
        usage_source="actual",
        attempt_status="success",
        latency_ms=10,
    )
    reconciliation_state = await store.finalize_reservation(
        reservation_id=reservation.reservation_id,
        final_status="completed",
    )

    async with AsyncSessionLocal() as session:
        stored_reservation = await session.get(
            BudgetReservation, reservation.reservation_id
        )
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
    assert stored_reservation is not None
    assert reconciliation_state == "needs_reconciliation"
    assert stored_reservation.reconciliation_state == "needs_reconciliation"
    assert stored_reservation.reconciliation_reason == "actual_cost_exceeded_hold"
    assert period is not None
    assert (period.reserved_micros, period.spent_micros) == (0, 150)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_unattempted_reservation_expires_and_releases_hold(test_env):
    request_id = await _create_request(test_env)
    store = PostgreSQLBudgetStore()
    reservation = await store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=700)
    )
    assert reservation.reservation_id is not None

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(BudgetReservation)
            .where(BudgetReservation.id == reservation.reservation_id)
            .values(
                created_at=datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(hours=2)
            )
        )
        await session.commit()

    assert await store.expire_stale_once() == 1

    async with AsyncSessionLocal() as session:
        stored_reservation = await session.get(
            BudgetReservation, reservation.reservation_id
        )
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        request = await session.get(GatewayRequest, request_id)
    assert stored_reservation is not None
    assert (stored_reservation.status, stored_reservation.held_micros) == (
        "expired",
        0,
    )
    assert period is not None and period.reserved_micros == 0
    assert request is not None and request.status == "failed"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reservation_commit_failure_rolls_back_period_and_row(test_env):
    request_id = await _create_request(test_env)
    healthy_store = PostgreSQLBudgetStore()
    assert await healthy_store.remaining_micros(test_env["tenant_id"]) == 100_000_000
    failing_store = PostgreSQLBudgetStore(_test_failpoint="before_reservation_commit")

    with pytest.raises(RuntimeError, match="reservation commit"):
        await failing_store.try_reserve(
            _reservation_request(test_env, request_id, cost_micros=500)
        )

    async with AsyncSessionLocal() as session:
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        reservation = await session.scalar(
            select(BudgetReservation).where(
                BudgetReservation.gateway_request_id == request_id
            )
        )
    assert period is not None
    assert (period.reserved_micros, period.spent_micros) == (0, 0)
    assert reservation is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_attempt_usage_commit_failure_rolls_back_ledger_and_counters(test_env):
    request_id = await _create_request(test_env)
    attempt_id = await _create_attempt(request_id, 1)
    healthy_store = PostgreSQLBudgetStore()
    reservation = await healthy_store.try_reserve(
        _reservation_request(test_env, request_id, cost_micros=500)
    )
    assert reservation.reservation_id is not None
    failing_store = PostgreSQLBudgetStore(_test_failpoint="before_attempt_usage_commit")

    with pytest.raises(RuntimeError, match="attempt usage commit"):
        await failing_store.record_attempt_usage(
            reservation_id=reservation.reservation_id,
            provider_attempt_id=attempt_id,
            provider="mock",
            model="gpt-5.4-mini",
            input_tokens=10,
            output_tokens=20,
            cost_micros=300,
            usage_source="actual",
            attempt_status="success",
            latency_ms=9,
        )

    async with AsyncSessionLocal() as session:
        period = await session.scalar(
            select(BudgetPeriod).where(BudgetPeriod.tenant_id == test_env["tenant_id"])
        )
        stored_reservation = await session.get(
            BudgetReservation, reservation.reservation_id
        )
        attempt = await session.get(ProviderAttempt, attempt_id)
        ledger_count = await session.scalar(
            select(func.count(UsageLedger.id)).where(
                UsageLedger.provider_attempt_id == attempt_id
            )
        )
    assert period is not None
    assert (period.reserved_micros, period.spent_micros) == (500, 0)
    assert stored_reservation is not None
    assert (stored_reservation.held_micros, stored_reservation.consumed_micros) == (
        500,
        0,
    )
    assert attempt is not None
    assert (attempt.status, attempt.latency_ms) == ("started", None)
    assert ledger_count == 0
