from typing import Literal, Protocol
from app.domain.budget import ReservationRequest, ReservationResult


class DatabaseUnavailable(Exception):
    """A required PostgreSQL operation failed before safe request completion."""


class BudgetStore(Protocol):
    async def try_reserve(self, request: ReservationRequest) -> ReservationResult: ...

    async def ensure_attempt_capacity(
        self,
        *,
        reservation_id: str,
        required_micros: int,
    ) -> bool: ...

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
        usage_source: Literal["actual", "estimated", "conservative"],
        attempt_status: str,
        latency_ms: int,
    ) -> None: ...

    async def finalize_reservation(
        self,
        *,
        reservation_id: str,
        final_status: Literal["completed", "failed", "cancelled"],
        gateway_overhead_ms: int | None = None,
    ) -> None: ...

    async def expire_stale_once(self) -> int: ...
    async def reconcile_needs_reconciliation_once(self) -> int: ...
    async def remaining_micros(self, tenant_id: int) -> int: ...

    async def mark_needs_reconciliation(
        self, *, reservation_id: str, reason: str
    ) -> None: ...
