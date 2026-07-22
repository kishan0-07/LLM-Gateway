import uuid
import datetime
from decimal import Decimal, ROUND_HALF_UP
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession
from redis.exceptions import RedisError

from app.domain.budget import ReservationRequest, ReservationResult
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import BudgetReservation, BudgetAccount, UsageLedger, ProviderAttempt
from app.infrastructure.redis.client import get_redis
from app.application.ports.budget_store import BudgetBackendUnavailable, DatabaseUnavailable
from app.core.logging import logger

MICROS_PER_DOLLAR = Decimal("1000000")
RESERVATION_TTL_SECONDS = 3600

def to_micros(value: Decimal | float) -> int:
    dec = Decimal(str(value)) if not isinstance(value, Decimal) else value
    return int((dec * MICROS_PER_DOLLAR).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def budget_counter_key(tenant_id: int, now: datetime.datetime | None = None) -> str:
    utc_now = (now or datetime.datetime.now(datetime.timezone.utc)).astimezone(datetime.timezone.utc)
    return f"budget:{tenant_id}:used:{utc_now:%Y-%m}"

def reservation_marker_key(reservation_id: str) -> str:
    return f"budget:reservation:{reservation_id}"

# Lua Script 1: Atomic reservation admission with durable seed rehydration
RESERVE_WITH_SEED_LUA = """
local month_key = KEYS[1]
local marker_key = KEYS[2]

local requested = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local seed = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])

local exists = redis.call('EXISTS', month_key)
local used = 0

if exists == 0 then
    used = seed
    redis.call('SET', month_key, used, 'EX', ttl_seconds)
else
    used = tonumber(redis.call('GET', month_key) or "0")
    if used < seed then
        used = seed
        redis.call('SET', month_key, used, 'KEEPTTL')
    end
end

if (limit - used) >= requested then
    redis.call('INCRBY', month_key, requested)
    redis.call('SET', marker_key, requested, 'EX', ttl_seconds)
    return 1
else
    return 0
end
"""

# Lua Script 2: Compensate reservation if PostgreSQL commit fails after Lua approved
COMPENSATE_LUA = """
local month_key = KEYS[1]
local marker_key = KEYS[2]

local amount_str = redis.call('GET', marker_key)
if amount_str then
    local amount = tonumber(amount_str)
    if redis.call('EXISTS', month_key) == 1 then
        local current = tonumber(redis.call('GET', month_key) or "0")
        local new_val = math.max(0, current - amount)
        redis.call('SET', month_key, new_val, 'KEEPTTL')
    end
    redis.call('DEL', marker_key)
    return 1
end
return 0
"""

# Lua Script 3: Update month key on settlement safely
SETTLE_TRUE_UP_LUA = """
local month_key = KEYS[1]
local marker_key = KEYS[2]
local delta = tonumber(ARGV[1])

if redis.call('EXISTS', month_key) == 1 then
    if delta > 0 then
        redis.call('INCRBY', month_key, delta)
    elseif delta < 0 then
        local current = tonumber(redis.call('GET', month_key) or "0")
        local new_val = math.max(0, current + delta)
        redis.call('SET', month_key, new_val, 'KEEPTTL')
    end
end
redis.call('DEL', marker_key)
return 1
"""


class RedisBudgetStore:
    def __init__(self):
        self._redis = get_redis()
        self._reserve_script = self._redis.register_script(RESERVE_WITH_SEED_LUA)
        self._compensate_script = self._redis.register_script(COMPENSATE_LUA)
        self._settle_script = self._redis.register_script(SETTLE_TRUE_UP_LUA)

    async def _durable_period_micros(
        self, session: AsyncSession, tenant_id: int, period_start: datetime.datetime, period_end: datetime.datetime
    ) -> int:
        # 1. Sum settled usage linked to reservations created in this UTC month
        settled_stmt = (
            select(func.coalesce(func.sum(UsageLedger.cost_usd), Decimal("0")))
            .join(BudgetReservation, UsageLedger.reservation_id == BudgetReservation.id)
            .where(
                BudgetReservation.tenant_id == tenant_id,
                BudgetReservation.created_at >= period_start,
                BudgetReservation.created_at < period_end,
            )
        )
        settled_sum: Decimal = (await session.execute(settled_stmt)).scalar_one()

        # 2. Sum active reservations created in this UTC month
        active_stmt = (
            select(func.coalesce(func.sum(BudgetReservation.estimated_cost_usd), Decimal("0")))
            .where(
                BudgetReservation.tenant_id == tenant_id,
                BudgetReservation.created_at >= period_start,
                BudgetReservation.created_at < period_end,
                BudgetReservation.status == "reserved",
                BudgetReservation.reconciliation_state.in_(["none", "needs_reconciliation"]),
            )
        )
        active_sum: Decimal = (await session.execute(active_stmt)).scalar_one()

        return to_micros(settled_sum + active_sum)

    async def try_reserve(self, request: ReservationRequest) -> ReservationResult:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        period_start = datetime.datetime(now_utc.year, now_utc.month, 1, tzinfo=datetime.timezone.utc)
        if now_utc.month == 12:
            period_end = datetime.datetime(now_utc.year + 1, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            period_end = datetime.datetime(now_utc.year, now_utc.month + 1, 1, tzinfo=datetime.timezone.utc)

        seconds_until_month_end = int((period_end - now_utc).total_seconds()) + 86400

        reservation_id = str(uuid.uuid4())
        requested_micros = to_micros(request.estimated_cost_usd)

        # Step 1: PostgreSQL Lock & Authoritative Calculation
        try:
            async with AsyncSessionLocal() as session:
                account_result = await session.execute(
                    select(BudgetAccount)
                    .where(BudgetAccount.tenant_id == request.tenant_id)
                    .with_for_update()
                )
                account = account_result.scalar_one_or_none()
                if not account:
                    return ReservationResult(approved=False, reservation_id=None, reason="account_not_found")

                limit_micros = to_micros(account.monthly_limit_usd)
                durable_used_micros = await self._durable_period_micros(session, request.tenant_id, period_start, period_end)

                if (limit_micros - durable_used_micros) < requested_micros:
                    return ReservationResult(approved=False, reservation_id=None, reason="over_budget")

                # Step 2: Redis Admission Check & Marker Write
                m_key = budget_counter_key(request.tenant_id, now_utc)
                res_key = reservation_marker_key(reservation_id)

                try:
                    approved = await self._reserve_script(
                        keys=[m_key, res_key],
                        args=[requested_micros, limit_micros, durable_used_micros, seconds_until_month_end],
                    )
                except RedisError as exc:
                    logger.error("budget_backend_unavailable", tenant_id=request.tenant_id, error=str(exc))
                    raise BudgetBackendUnavailable() from exc

                if not approved:
                    return ReservationResult(approved=False, reservation_id=None, reason="over_budget")

                # Step 3: Write BudgetReservation in PostgreSQL
                reservation = BudgetReservation(
                    id=reservation_id,
                    tenant_id=request.tenant_id,
                    gateway_request_id=request.gateway_request_id,
                    estimated_tokens=request.estimated_tokens,
                    estimated_cost_usd=request.estimated_cost_usd,
                    status="reserved",
                    reconciliation_state="none",
                    cache_sync_required=False,
                )
                session.add(reservation)
                
                try:
                    await session.commit()
                except Exception as db_exc:
                    logger.error("postgres_commit_failed_compensating_redis", reservation_id=reservation_id, error=str(db_exc))
                    # Step 4: Compensate Redis on Postgres Commit Failure
                    try:
                        await self._compensate_script(keys=[m_key, res_key])
                    except Exception as comp_exc:
                        logger.critical("redis_compensation_failed", reservation_id=reservation_id, error=str(comp_exc))
                    raise DatabaseUnavailable() from db_exc

                return ReservationResult(approved=True, reservation_id=reservation_id)
        except (DatabaseUnavailable, BudgetBackendUnavailable):
            raise
        except Exception as exc:
            logger.error("try_reserve_failed", error=str(exc))
            raise DatabaseUnavailable() from exc

    async def settle(
        self, reservation_id: str, provider: str, model: str,
        input_tokens: int, output_tokens: int, actual_cost_usd: float, status: str,
    ) -> None:
        actual_dec = Decimal(str(actual_cost_usd))
        
        # Step 1: PostgreSQL Durable Settlement First
        try:
            async with AsyncSessionLocal() as session:
                res_result = await session.execute(
                    select(BudgetReservation)
                    .where(BudgetReservation.id == reservation_id)
                    .with_for_update()
                )
                reservation = res_result.scalar_one_or_none()
                if not reservation or reservation.status != "reserved":
                    return

                tenant_id = reservation.tenant_id
                created_at = reservation.created_at
                estimated_cost_usd = reservation.estimated_cost_usd

                session.add(UsageLedger(
                    tenant_id=tenant_id,
                    gateway_request_id=reservation.gateway_request_id,
                    reservation_id=reservation.id,
                    provider=provider, model=model,
                    input_tokens=input_tokens, output_tokens=output_tokens,
                    cost_usd=actual_dec,
                    usage_source="actual" if status == "success" else "estimated",
                ))
                reservation.status = "settled"
                reservation.settled_at = datetime.datetime.now(datetime.timezone.utc)
                await session.commit()
        except Exception as exc:
            logger.error("durable_settlement_failed", reservation_id=reservation_id, error=str(exc))
            raise DatabaseUnavailable() from exc

        # Step 2: Redis True-Up Follow-up
        delta_micros = to_micros(actual_dec - estimated_cost_usd)
        m_key = budget_counter_key(tenant_id, created_at)
        res_key = reservation_marker_key(reservation_id)

        try:
            await self._settle_script(keys=[m_key, res_key], args=[delta_micros])
        except RedisError as exc:
            logger.warning("redis_settle_trueup_failed_flagging_cache_sync", reservation_id=reservation_id, error=str(exc))
            try:
                async with AsyncSessionLocal() as session:
                    await session.execute(
                        update(BudgetReservation)
                        .where(BudgetReservation.id == reservation_id)
                        .values(cache_sync_required=True)
                    )
                    await session.commit()
            except Exception:
                pass

    async def reservation_estimated_cost_usd(self, reservation_id: str) -> Decimal:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BudgetReservation.estimated_cost_usd).where(BudgetReservation.id == reservation_id)
            )
            val = result.scalar_one_or_none()
            if val is None:
                raise DatabaseUnavailable(f"Reservation {reservation_id} not found")
            return val

    async def mark_needs_reconciliation(self, *, reservation_id: str, reason: str) -> None:
        async with AsyncSessionLocal() as session:
            await session.execute(
                update(BudgetReservation)
                .where(BudgetReservation.id == reservation_id)
                .values(
                    reconciliation_state="needs_reconciliation",
                    reconciliation_reason=reason,
                )
            )
            await session.commit()

    async def expire_stale_once(self) -> int:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=RESERVATION_TTL_SECONDS)
        expired_count = 0

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BudgetReservation)
                .where(
                    BudgetReservation.status == "reserved",
                    BudgetReservation.created_at < cutoff,
                )
                .with_for_update(skip_locked=True)
            )
            stale_reservations = list(result.scalars().all())

            for res in stale_reservations:
                attempts_result = await session.execute(
                    select(func.count(ProviderAttempt.id)).where(ProviderAttempt.gateway_request_id == res.gateway_request_id)
                )
                attempt_count = attempts_result.scalar_one()

                if attempt_count > 0 or res.reconciliation_state != "none":
                    # Provider attempt exists! Hold reservation for reconciliation
                    res.reconciliation_state = "needs_reconciliation"
                    res.reconciliation_reason = "stale_with_provider_attempt"
                    logger.warning("stale_reservation_held_for_reconciliation", reservation_id=res.id, gateway_request_id=res.gateway_request_id)
                else:
                    # No provider execution — safe to expire
                    res.status = "expired"
                    res.settled_at = datetime.datetime.now(datetime.timezone.utc)
                    expired_count += 1
                    
                    m_key = budget_counter_key(res.tenant_id, res.created_at)
                    res_key = reservation_marker_key(res.id)
                    micros = to_micros(res.estimated_cost_usd)
                    try:
                        await self._settle_script(keys=[m_key, res_key], args=[-micros])
                    except RedisError:
                        res.cache_sync_required = True

            await session.commit()
        return expired_count

    async def repair_out_of_sync_caches_once(self) -> int:
        repaired_count = 0
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BudgetReservation.tenant_id, BudgetReservation.created_at)
                .where(BudgetReservation.cache_sync_required)
                .distinct()
            )
            distinct_periods = list(result.all())

            for tenant_id, created_at in distinct_periods:
                now_utc = created_at.astimezone(datetime.timezone.utc)
                period_start = datetime.datetime(now_utc.year, now_utc.month, 1, tzinfo=datetime.timezone.utc)
                if now_utc.month == 12:
                    period_end = datetime.datetime(now_utc.year + 1, 1, 1, tzinfo=datetime.timezone.utc)
                else:
                    period_end = datetime.datetime(now_utc.year, now_utc.month + 1, 1, tzinfo=datetime.timezone.utc)

                durable_micros = await self._durable_period_micros(session, tenant_id, period_start, period_end)
                m_key = budget_counter_key(tenant_id, now_utc)

                try:
                    await self._redis.set(m_key, durable_micros, keepttl=True)
                    await session.execute(
                        update(BudgetReservation)
                        .where(
                            BudgetReservation.tenant_id == tenant_id,
                            BudgetReservation.created_at >= period_start,
                            BudgetReservation.created_at < period_end,
                            BudgetReservation.cache_sync_required,
                        )
                        .values(cache_sync_required=False)
                    )
                    repaired_count += 1
                except RedisError as exc:
                    logger.warning("cache_repair_redis_failed", tenant_id=tenant_id, error=str(exc))

            await session.commit()
        return repaired_count

    async def remaining_usd(self, tenant_id: int) -> float:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        period_start = datetime.datetime(now_utc.year, now_utc.month, 1, tzinfo=datetime.timezone.utc)
        if now_utc.month == 12:
            period_end = datetime.datetime(now_utc.year + 1, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            period_end = datetime.datetime(now_utc.year, now_utc.month + 1, 1, tzinfo=datetime.timezone.utc)

        try:
            async with AsyncSessionLocal() as session:
                account_result = await session.execute(
                    select(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
                )
                account = account_result.scalar_one_or_none()
                if not account:
                    return 0.0

                limit_micros = to_micros(account.monthly_limit_usd)
                durable_micros = await self._durable_period_micros(session, tenant_id, period_start, period_end)
                remaining_micros = max(0, limit_micros - durable_micros)
                return remaining_micros / float(MICROS_PER_DOLLAR)
        except Exception as exc:
            logger.error("remaining_usd_failed", tenant_id=tenant_id, error=str(exc))
            raise DatabaseUnavailable() from exc