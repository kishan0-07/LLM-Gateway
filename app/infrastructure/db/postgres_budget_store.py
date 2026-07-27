import datetime
import uuid
from typing import Literal

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.ports.budget_store import DatabaseUnavailable
from app.core.logging import logger
from app.domain.budget import (
    ReservationRequest,
    ReservationResult,
    micros_to_decimal,
    to_micros,
)
from app.infrastructure.db.models import (
    BudgetAccount,
    BudgetPeriod,
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal


RESERVATION_TTL_SECONDS = 3600
RECONCILIATION_GRACE_SECONDS = 120
RECONCILIATION_BATCH_SIZE = 100

UsageSource = Literal["actual", "estimated", "conservative"]
FinalStatus = Literal["completed", "failed", "cancelled"]


def period_bounds(
    now: datetime.datetime,
) -> tuple[datetime.datetime, datetime.datetime]:
    utc = now.astimezone(datetime.timezone.utc)
    start = datetime.datetime(utc.year, utc.month, 1, tzinfo=datetime.timezone.utc)
    if start.month == 12:
        end = datetime.datetime(start.year + 1, 1, 1, tzinfo=datetime.timezone.utc)
    else:
        end = datetime.datetime(
            start.year,
            start.month + 1,
            1,
            tzinfo=datetime.timezone.utc,
        )
    return start, end


class PostgreSQLBudgetStore:
    """PostgreSQL is the sole authority for budget admission and settlement."""

    def __init__(self, *, _test_failpoint: str | None = None) -> None:
        # Deliberately constructor-only: tests can exercise transaction crash
        # windows without exposing a failpoint through settings or HTTP.
        self._test_failpoint = _test_failpoint

    async def _ensure_period(
        self,
        session: AsyncSession,
        tenant_id: int,
        now: datetime.datetime,
    ) -> tuple[datetime.datetime, datetime.datetime] | None:
        period_start, period_end = period_bounds(now)
        monthly_limit = await session.scalar(
            select(BudgetAccount.monthly_limit_usd).where(
                BudgetAccount.tenant_id == tenant_id
            )
        )
        if monthly_limit is None:
            return None

        await session.execute(
            pg_insert(BudgetPeriod)
            .values(
                tenant_id=tenant_id,
                period_start=period_start,
                period_end=period_end,
                limit_micros=to_micros(monthly_limit),
                reserved_micros=0,
                spent_micros=0,
            )
            .on_conflict_do_nothing(
                index_elements=["tenant_id", "period_start"],
            )
        )
        return period_start, period_end

    async def try_reserve(self, request: ReservationRequest) -> ReservationResult:
        if request.estimated_cost_micros < 0:
            raise ValueError("estimated_cost_micros must be nonnegative")

        reservation_id = str(uuid.uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    existing = await session.scalar(
                        select(BudgetReservation).where(
                            BudgetReservation.gateway_request_id
                            == request.gateway_request_id
                        )
                    )
                    if existing is not None:
                        return ReservationResult(
                            approved=existing.status == "reserved",
                            reservation_id=existing.id,
                            reason=(
                                None
                                if existing.status == "reserved"
                                else "request_already_finalized"
                            ),
                        )

                    period = await self._ensure_period(
                        session,
                        request.tenant_id,
                        now,
                    )
                    if period is None:
                        return ReservationResult(
                            approved=False,
                            reservation_id=None,
                            reason="account_not_found",
                        )
                    period_start, _ = period

                    admitted = await session.scalar(
                        update(BudgetPeriod)
                        .where(
                            BudgetPeriod.tenant_id == request.tenant_id,
                            BudgetPeriod.period_start == period_start,
                            (
                                BudgetPeriod.spent_micros
                                + BudgetPeriod.reserved_micros
                                + request.estimated_cost_micros
                            )
                            <= BudgetPeriod.limit_micros,
                        )
                        .values(
                            reserved_micros=(
                                BudgetPeriod.reserved_micros
                                + request.estimated_cost_micros
                            ),
                            updated_at=func.now(),
                        )
                        .returning(BudgetPeriod.reserved_micros)
                    )
                    if admitted is None:
                        return ReservationResult(
                            approved=False,
                            reservation_id=None,
                            reason="over_budget",
                        )

                    session.add(
                        BudgetReservation(
                            id=reservation_id,
                            tenant_id=request.tenant_id,
                            gateway_request_id=request.gateway_request_id,
                            requested_model=request.requested_model,
                            estimated_input_tokens=request.estimated_input_tokens,
                            estimated_output_tokens=request.estimated_output_tokens,
                            estimated_tokens=request.estimated_tokens,
                            estimated_cost_usd=micros_to_decimal(
                                request.estimated_cost_micros
                            ),
                            period_start=period_start,
                            held_micros=request.estimated_cost_micros,
                            consumed_micros=0,
                            status="reserved",
                            reconciliation_state="none",
                        )
                    )
                    if self._test_failpoint == "before_reservation_commit":
                        await session.flush()
                        raise RuntimeError("injected reservation commit failure")

            return ReservationResult(
                approved=True,
                reservation_id=reservation_id,
            )
        except IntegrityError:
            # A concurrent retry with the same gateway_request_id may have won.
            async with AsyncSessionLocal() as session:
                existing = await session.scalar(
                    select(BudgetReservation).where(
                        BudgetReservation.gateway_request_id
                        == request.gateway_request_id
                    )
                )
                if existing is not None:
                    return ReservationResult(
                        approved=existing.status == "reserved",
                        reservation_id=existing.id,
                        reason=(
                            None
                            if existing.status == "reserved"
                            else "request_already_finalized"
                        ),
                    )
            raise DatabaseUnavailable("budget reservation conflict could not recover")
        except SQLAlchemyError as exc:
            logger.error(
                "postgres_budget_reservation_failed",
                tenant_id=request.tenant_id,
                error_type=type(exc).__name__,
            )
            raise DatabaseUnavailable() from exc

    async def ensure_attempt_capacity(
        self,
        *,
        reservation_id: str,
        required_micros: int,
    ) -> bool:
        if required_micros < 0:
            raise ValueError("required_micros must be nonnegative")

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    reservation = await session.scalar(
                        select(BudgetReservation)
                        .where(BudgetReservation.id == reservation_id)
                        .with_for_update()
                    )
                    if reservation is None or reservation.status != "reserved":
                        return False

                    additional = max(0, required_micros - reservation.held_micros)
                    if additional == 0:
                        return True

                    admitted = await session.scalar(
                        update(BudgetPeriod)
                        .where(
                            BudgetPeriod.tenant_id == reservation.tenant_id,
                            BudgetPeriod.period_start == reservation.period_start,
                            (
                                BudgetPeriod.spent_micros
                                + BudgetPeriod.reserved_micros
                                + additional
                            )
                            <= BudgetPeriod.limit_micros,
                        )
                        .values(
                            reserved_micros=BudgetPeriod.reserved_micros + additional,
                            updated_at=func.now(),
                        )
                        .returning(BudgetPeriod.reserved_micros)
                    )
                    if admitted is None:
                        return False

                    reservation.held_micros += additional
                    return True
        except SQLAlchemyError as exc:
            logger.error(
                "postgres_attempt_capacity_failed",
                reservation_id=reservation_id,
                error_type=type(exc).__name__,
            )
            raise DatabaseUnavailable() from exc

    async def record_attempt_usage(
        self,
        *,
        reservation_id: str,
        provider_attempt_id: int,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        cost_micros: int,
        usage_source: UsageSource,
        attempt_status: str,
        latency_ms: int,
    ) -> None:
        if min(input_tokens, output_tokens, cost_micros, latency_ms) < 0:
            raise ValueError("attempt usage values must be nonnegative")

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    reservation = await session.scalar(
                        select(BudgetReservation)
                        .where(BudgetReservation.id == reservation_id)
                        .with_for_update()
                    )
                    if reservation is None:
                        raise DatabaseUnavailable("reservation not found")

                    attempt = await session.scalar(
                        select(ProviderAttempt)
                        .where(ProviderAttempt.id == provider_attempt_id)
                        .with_for_update()
                    )
                    if attempt is None:
                        raise DatabaseUnavailable("provider attempt not found")
                    if attempt.gateway_request_id != reservation.gateway_request_id:
                        raise DatabaseUnavailable(
                            "provider attempt does not belong to reservation"
                        )

                    existing = await session.scalar(
                        select(UsageLedger.id).where(
                            UsageLedger.provider_attempt_id == provider_attempt_id
                        )
                    )
                    if existing is not None:
                        return

                    released_hold = min(reservation.held_micros, cost_micros)
                    overrun_micros = max(0, cost_micros - reservation.held_micros)

                    period_updated = await session.scalar(
                        update(BudgetPeriod)
                        .where(
                            BudgetPeriod.tenant_id == reservation.tenant_id,
                            BudgetPeriod.period_start == reservation.period_start,
                        )
                        .values(
                            reserved_micros=(
                                BudgetPeriod.reserved_micros - released_hold
                            ),
                            spent_micros=BudgetPeriod.spent_micros + cost_micros,
                            updated_at=func.now(),
                        )
                        .returning(BudgetPeriod.spent_micros)
                    )
                    if period_updated is None:
                        raise DatabaseUnavailable("budget period not found")

                    reservation.held_micros -= released_hold
                    reservation.consumed_micros += cost_micros
                    if overrun_micros:
                        reservation.reconciliation_state = "needs_reconciliation"
                        reservation.reconciliation_reason = "actual_cost_exceeded_hold"
                        reservation.reconciliation_requested_at = datetime.datetime.now(
                            datetime.timezone.utc
                        )

                    session.add(
                        UsageLedger(
                            tenant_id=reservation.tenant_id,
                            gateway_request_id=reservation.gateway_request_id,
                            reservation_id=reservation.id,
                            provider_attempt_id=provider_attempt_id,
                            provider=provider,
                            model=model,
                            input_tokens=input_tokens,
                            output_tokens=output_tokens,
                            cost_micros=cost_micros,
                            cost_usd=micros_to_decimal(cost_micros),
                            usage_source=usage_source,
                            billing_status=(
                                "known" if usage_source == "actual" else "estimated"
                            ),
                        )
                    )
                    attempt.status = attempt_status
                    attempt.latency_ms = latency_ms
                    if self._test_failpoint == "before_attempt_usage_commit":
                        await session.flush()
                        raise RuntimeError("injected attempt usage commit failure")
        except SQLAlchemyError as exc:
            logger.error(
                "postgres_attempt_accounting_failed",
                reservation_id=reservation_id,
                provider_attempt_id=provider_attempt_id,
                error_type=type(exc).__name__,
            )
            raise DatabaseUnavailable() from exc

    async def finalize_reservation(
        self,
        *,
        reservation_id: str,
        final_status: FinalStatus,
        gateway_overhead_ms: int | None = None,
    ) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        request_status = {
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
        }[final_status]

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    reservation = await session.scalar(
                        select(BudgetReservation)
                        .where(BudgetReservation.id == reservation_id)
                        .with_for_update()
                    )
                    if reservation is None:
                        raise DatabaseUnavailable("reservation not found")
                    if reservation.status != "reserved":
                        return

                    remaining_hold = reservation.held_micros
                    if remaining_hold:
                        released = await session.scalar(
                            update(BudgetPeriod)
                            .where(
                                BudgetPeriod.tenant_id == reservation.tenant_id,
                                BudgetPeriod.period_start == reservation.period_start,
                            )
                            .values(
                                reserved_micros=(
                                    BudgetPeriod.reserved_micros - remaining_hold
                                ),
                                updated_at=func.now(),
                            )
                            .returning(BudgetPeriod.reserved_micros)
                        )
                        if released is None:
                            raise DatabaseUnavailable("budget period not found")

                    reservation.held_micros = 0
                    reservation.status = "settled"
                    reservation.final_status = final_status
                    reservation.finalized_at = now
                    reservation.settled_at = now

                    request_values: dict[str, object] = {"status": request_status}
                    if gateway_overhead_ms is not None:
                        request_values["gateway_overhead_ms"] = gateway_overhead_ms
                    await session.execute(
                        update(GatewayRequest)
                        .where(GatewayRequest.id == reservation.gateway_request_id)
                        .values(**request_values)
                    )
        except SQLAlchemyError as exc:
            logger.error(
                "postgres_reservation_finalization_failed",
                reservation_id=reservation_id,
                error_type=type(exc).__name__,
            )
            raise DatabaseUnavailable() from exc

    async def mark_needs_reconciliation(
        self,
        *,
        reservation_id: str,
        reason: str,
    ) -> None:
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    await session.execute(
                        update(BudgetReservation)
                        .where(
                            BudgetReservation.id == reservation_id,
                            BudgetReservation.status == "reserved",
                        )
                        .values(
                            reconciliation_state="needs_reconciliation",
                            reconciliation_reason=reason,
                            reconciliation_requested_at=datetime.datetime.now(
                                datetime.timezone.utc
                            ),
                        )
                    )
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc

    async def remaining_micros(self, tenant_id: int) -> int:
        now = datetime.datetime.now(datetime.timezone.utc)
        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    period = await self._ensure_period(session, tenant_id, now)
                    if period is None:
                        raise DatabaseUnavailable("budget account not found")
                    period_start, _ = period
                    row = await session.execute(
                        select(
                            BudgetPeriod.limit_micros,
                            BudgetPeriod.reserved_micros,
                            BudgetPeriod.spent_micros,
                        ).where(
                            BudgetPeriod.tenant_id == tenant_id,
                            BudgetPeriod.period_start == period_start,
                        )
                    )
                    limit_micros, reserved_micros, spent_micros = row.one()
                    return max(
                        0,
                        int(limit_micros) - int(reserved_micros) - int(spent_micros),
                    )
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc

    async def expire_stale_once(self) -> int:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=RESERVATION_TTL_SECONDS
        )
        expired_count = 0

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    reservations = list(
                        (
                            await session.execute(
                                select(BudgetReservation)
                                .where(
                                    BudgetReservation.status == "reserved",
                                    BudgetReservation.created_at < cutoff,
                                )
                                .order_by(BudgetReservation.created_at)
                                .limit(RECONCILIATION_BATCH_SIZE)
                                .with_for_update(skip_locked=True)
                            )
                        )
                        .scalars()
                        .all()
                    )
                    if not reservations:
                        return 0

                    request_ids = [
                        reservation.gateway_request_id for reservation in reservations
                    ]
                    attempted_request_ids = set(
                        (
                            await session.execute(
                                select(ProviderAttempt.gateway_request_id)
                                .where(
                                    ProviderAttempt.gateway_request_id.in_(request_ids)
                                )
                                .distinct()
                            )
                        )
                        .scalars()
                        .all()
                    )

                    now = datetime.datetime.now(datetime.timezone.utc)
                    for reservation in reservations:
                        if (
                            reservation.gateway_request_id in attempted_request_ids
                            or reservation.reconciliation_state
                            == "needs_reconciliation"
                        ):
                            reservation.reconciliation_state = "needs_reconciliation"
                            reservation.reconciliation_reason = (
                                reservation.reconciliation_reason
                                or "stale_with_provider_attempt"
                            )
                            reservation.reconciliation_requested_at = (
                                reservation.reconciliation_requested_at or now
                            )
                            continue

                        if reservation.held_micros:
                            await session.execute(
                                update(BudgetPeriod)
                                .where(
                                    BudgetPeriod.tenant_id == reservation.tenant_id,
                                    BudgetPeriod.period_start
                                    == reservation.period_start,
                                )
                                .values(
                                    reserved_micros=(
                                        BudgetPeriod.reserved_micros
                                        - reservation.held_micros
                                    ),
                                    updated_at=func.now(),
                                )
                            )

                        reservation.held_micros = 0
                        reservation.status = "expired"
                        reservation.final_status = "failed"
                        reservation.finalized_at = now
                        reservation.settled_at = now
                        await session.execute(
                            update(GatewayRequest)
                            .where(GatewayRequest.id == reservation.gateway_request_id)
                            .values(status="failed")
                        )
                        expired_count += 1
            return expired_count
        except SQLAlchemyError as exc:
            raise DatabaseUnavailable() from exc

    async def reconcile_needs_reconciliation_once(self) -> int:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            seconds=RECONCILIATION_GRACE_SECONDS
        )
        reconciled_count = 0

        try:
            async with AsyncSessionLocal() as session:
                async with session.begin():
                    reservations = list(
                        (
                            await session.execute(
                                select(BudgetReservation)
                                .where(
                                    BudgetReservation.status == "reserved",
                                    BudgetReservation.reconciliation_state
                                    == "needs_reconciliation",
                                    BudgetReservation.reconciliation_requested_at
                                    < cutoff,
                                )
                                .order_by(BudgetReservation.reconciliation_requested_at)
                                .limit(RECONCILIATION_BATCH_SIZE)
                                .with_for_update(skip_locked=True)
                            )
                        )
                        .scalars()
                        .all()
                    )

                    now = datetime.datetime.now(datetime.timezone.utc)
                    for reservation in reservations:
                        latest_attempt = await session.scalar(
                            select(ProviderAttempt)
                            .where(
                                ProviderAttempt.gateway_request_id
                                == reservation.gateway_request_id
                            )
                            .order_by(ProviderAttempt.attempt_number.desc())
                            .limit(1)
                            .with_for_update()
                        )

                        if latest_attempt is not None:
                            existing_ledger = await session.scalar(
                                select(UsageLedger.id).where(
                                    UsageLedger.provider_attempt_id == latest_attempt.id
                                )
                            )
                            if existing_ledger is None:
                                authorized_cost_micros = (
                                    latest_attempt.authorized_cost_micros
                                    if latest_attempt.authorized_cost_micros is not None
                                    else to_micros(reservation.estimated_cost_usd)
                                )
                                estimated_cost_micros = min(
                                    reservation.held_micros,
                                    authorized_cost_micros,
                                )
                                released_hold = min(
                                    reservation.held_micros,
                                    estimated_cost_micros,
                                )
                                await session.execute(
                                    update(BudgetPeriod)
                                    .where(
                                        BudgetPeriod.tenant_id == reservation.tenant_id,
                                        BudgetPeriod.period_start
                                        == reservation.period_start,
                                    )
                                    .values(
                                        reserved_micros=(
                                            BudgetPeriod.reserved_micros - released_hold
                                        ),
                                        spent_micros=(
                                            BudgetPeriod.spent_micros
                                            + estimated_cost_micros
                                        ),
                                        updated_at=func.now(),
                                    )
                                )
                                reservation.held_micros -= released_hold
                                reservation.consumed_micros += estimated_cost_micros
                                session.add(
                                    UsageLedger(
                                        tenant_id=reservation.tenant_id,
                                        gateway_request_id=(
                                            reservation.gateway_request_id
                                        ),
                                        reservation_id=reservation.id,
                                        provider_attempt_id=latest_attempt.id,
                                        provider=latest_attempt.provider,
                                        model=latest_attempt.model,
                                        input_tokens=(
                                            latest_attempt.estimated_input_tokens
                                            if latest_attempt.estimated_input_tokens
                                            is not None
                                            else (
                                                reservation.estimated_input_tokens or 0
                                            )
                                        ),
                                        output_tokens=(
                                            latest_attempt.estimated_output_tokens
                                            if latest_attempt.estimated_output_tokens
                                            is not None
                                            else (
                                                reservation.estimated_output_tokens or 0
                                            )
                                        ),
                                        cost_micros=estimated_cost_micros,
                                        cost_usd=micros_to_decimal(
                                            estimated_cost_micros
                                        ),
                                        usage_source="conservative",
                                        billing_status="estimated",
                                    )
                                )
                                latest_attempt.status = "reconciled_estimate"

                        remaining_hold = reservation.held_micros
                        if remaining_hold:
                            await session.execute(
                                update(BudgetPeriod)
                                .where(
                                    BudgetPeriod.tenant_id == reservation.tenant_id,
                                    BudgetPeriod.period_start
                                    == reservation.period_start,
                                )
                                .values(
                                    reserved_micros=(
                                        BudgetPeriod.reserved_micros - remaining_hold
                                    ),
                                    updated_at=func.now(),
                                )
                            )

                        reservation.held_micros = 0
                        reservation.status = "settled"
                        reservation.final_status = "failed"
                        reservation.finalized_at = now
                        reservation.settled_at = now
                        reservation.reconciliation_state = "reconciled"
                        await session.execute(
                            update(GatewayRequest)
                            .where(GatewayRequest.id == reservation.gateway_request_id)
                            .values(status="failed")
                        )
                        reconciled_count += 1

            return reconciled_count
        except SQLAlchemyError as exc:
            logger.error(
                "postgres_budget_reconciliation_failed",
                error_type=type(exc).__name__,
            )
            raise DatabaseUnavailable() from exc
