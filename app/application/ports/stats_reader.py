from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class GatewayOverheadSummary:
    average_ms: float | None
    samples: int


@dataclass(frozen=True)
class UsageStatsSummary:
    total_requests: int
    settled_requests: int
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: Decimal
    failover_count: int
    gateway_overhead: GatewayOverheadSummary


class StatsReader(Protocol):
    async def read(
        self,
        *,
        tenant_id: int,
        api_key_id: int | None,
    ) -> UsageStatsSummary:
        ...