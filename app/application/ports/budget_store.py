from typing import Protocol
from decimal import Decimal
from app.domain.budget import ReservationRequest, ReservationResult


class BudgetBackendUnavailable(Exception):
    """Raised when budget authorization cannot safely use its configured backend."""


class DatabaseUnavailable(Exception):
    """A required PostgreSQL operation failed before safe request completion."""


class BudgetExceededMidStream(Exception):
    """The stream's provisional cost exceeded its approved reservation."""


class BudgetStore(Protocol):
    async def try_reserve(self, request: ReservationRequest) -> ReservationResult: ...
    async def settle(
        self,
        reservation_id: str,
        provider: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
        actual_cost_usd: float,
        status: str,
        usage_source: str,
    ) -> None: ...
    async def expire_stale_once(self) -> int: ...
    async def remaining_usd(self, tenant_id: int) -> float: ...
    async def reservation_estimated_cost_usd(self, reservation_id: str) -> Decimal: ...
    async def mark_needs_reconciliation(
        self, *, reservation_id: str, reason: str
    ) -> None: ...
    async def repair_out_of_sync_caches_once(self) -> int: ...
    async def reconcile_needs_reconciliation_once(self) -> int: ...
