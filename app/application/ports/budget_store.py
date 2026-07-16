from typing import Protocol
from app.domain.budget import ReservationRequest, ReservationResult

class BudgetBackendUnavailable(Exception):
    """Raised when budget authorization cannot safely use its configured backend."""

class BudgetStore(Protocol):
    async def try_reserve(self, request: ReservationRequest) -> ReservationResult: ...

    async def expire_stale_once(self) -> int: ...

    async def remaining_usd(self, tenant_id: int) -> float: ...

