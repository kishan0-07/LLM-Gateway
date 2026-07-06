from typing import Protocol
from app.domain.budget import ReservationRequest, ReservationResult


class BudgetStore(Protocol):
    async def try_reserve(self, request: ReservationRequest) -> ReservationResult: ...

    async def settle(
        self, reservation_id: str, provider: str, model: str,
        input_tokens: int, output_tokens: int, actual_cost_usd: float, status: str,
    ) -> None: ...

    async def expire_stale_once(self) -> int: ...