from typing import Protocol


class UsageLedger(Protocol):
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
