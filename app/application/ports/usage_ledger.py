from typing import Literal, Protocol


class UsageLedger(Protocol):
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
