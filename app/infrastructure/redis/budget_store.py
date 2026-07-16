import uuid
import datetime
from sqlalchemy import select
from app.domain.budget import ReservationRequest, ReservationResult
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import BudgetReservation, BudgetAccount, UsageLedger
from app.infrastructure.redis.client import get_redis
from redis.exceptions import RedisError
from app.application.ports.budget_store import BudgetBackendUnavailable
from app.core.logging import logger

MICROS_PER_DOLLAR = 1_000_000
RESERVATION_TTL_SECONDS = 3600  # safety-net self-heal window, NOT the billing period — Postgres created_at is the real boundary

RESERVE_LUA = """
local used = tonumber(redis.call('GET', KEYS[1]) or "0")
local limit = tonumber(ARGV[2])
local requested = tonumber(ARGV[1])
if (limit - used) >= requested then
    redis.call('INCRBY', KEYS[1], requested)
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    return 1
else return 0 end
"""


class RedisBudgetStore:
    def __init__(self):
        self._redis = get_redis()
        self._reserve_script = self._redis.register_script(RESERVE_LUA)

    async def try_reserve(self, request: ReservationRequest) -> ReservationResult:
        async with AsyncSessionLocal() as session:
            account = (await session.execute(
                select(BudgetAccount).where(BudgetAccount.tenant_id == request.tenant_id)
            )).scalar_one()

            key = f"budget:{request.tenant_id}:used"
            requested_micros = round(request.estimated_cost_usd * MICROS_PER_DOLLAR)
            limit_micros = round(float(account.monthly_limit_usd) * MICROS_PER_DOLLAR)

            try:
                approved = await self._reserve_script(
                    keys=[key],
                    args=[requested_micros, limit_micros, RESERVATION_TTL_SECONDS],
                )
            except RedisError as exc:
                logger.error(
                    "budget_backend_unavailable",
                    tenant_id=request.tenant_id,
                    gateway_request_id=request.gateway_request_id,
                    error_type=type(exc).__name__,
                )
                raise BudgetBackendUnavailable() from exc
            if not approved:
                return ReservationResult(approved=False, reservation_id=None, reason="over_budget")

            reservation = BudgetReservation(
                id=str(uuid.uuid4()),
                tenant_id=request.tenant_id,
                gateway_request_id=request.gateway_request_id,
                estimated_tokens=request.estimated_tokens,
                estimated_cost_usd=request.estimated_cost_usd,
                status="reserved",
            )
            session.add(reservation)
            await session.commit()
            return ReservationResult(approved=True, reservation_id=reservation.id)

    async def settle(
        self, reservation_id: str, provider: str, model: str,
        input_tokens: int, output_tokens: int, actual_cost_usd: float, status: str,
    ) -> None:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BudgetReservation).where(BudgetReservation.id == reservation_id).with_for_update()
            )
            reservation = result.scalar_one()

            if reservation.status != "reserved":
                return  # lock held since the SELECT above — a concurrent settle() blocks here until this transaction commits, then sees "settled" and no-ops

            tenant_id = reservation.tenant_id
            estimated_cost_usd = reservation.estimated_cost_usd

            session.add(UsageLedger(
                tenant_id=tenant_id,
                gateway_request_id=reservation.gateway_request_id,
                reservation_id=reservation.id,
                provider=provider, model=model,
                input_tokens=input_tokens, output_tokens=output_tokens,
                cost_usd=actual_cost_usd,
                usage_source="actual" if status == "success" else "estimated",
            ))
            reservation.status = "settled"
            reservation.settled_at = datetime.datetime.now(datetime.timezone.utc)
            await session.commit()

        # true-up: reserved was an upper-bound estimate, this corrects the fast-path counter to actual
        delta_micros = round((float(actual_cost_usd) - float(estimated_cost_usd)) * MICROS_PER_DOLLAR)
        if delta_micros != 0:
            await self._redis.incrby(f"budget:{tenant_id}:used", delta_micros)

    async def expire_stale_once(self) -> int:
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=RESERVATION_TTL_SECONDS)
        releases: list[tuple[int, int]] = []

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BudgetReservation)
                .where(
                    BudgetReservation.status == "reserved",
                    BudgetReservation.created_at < cutoff,
                )
                .with_for_update(skip_locked=True)
            )
            for reservation in result.scalars():
                reservation.status = "expired"
                reservation.settled_at = datetime.datetime.now(datetime.timezone.utc)
                releases.append((
                    reservation.tenant_id,
                    round(float(reservation.estimated_cost_usd) * MICROS_PER_DOLLAR),
                ))
            await session.commit()

        for tenant_id, estimated_micros in releases:
            await self._redis.decrby(f"budget:{tenant_id}:used", estimated_micros)
        return len(releases)

    async def remaining_usd(self, tenant_id: int) -> float:
        async with AsyncSessionLocal() as session:
            account = (
                await session.execute(
                    select(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
                )
            ).scalar_one()

        limit_micros = round(float(account.monthly_limit_usd) * MICROS_PER_DOLLAR)
        used_raw = await self._redis.get(f"budget:{tenant_id}:used")
        used_micros = int(used_raw) if used_raw is not None else 0
        remaining_micros = max(0, limit_micros - used_micros)
        return remaining_micros / MICROS_PER_DOLLAR