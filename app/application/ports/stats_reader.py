from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class GatewayOverheadSummary:
    average_ms: float | None
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    samples: int


@dataclass(frozen=True)
class UsageStatsSummary:
    total_requests: int
    settled_requests: int  # Deprecated alias for settled_reservations.
    settled_reservations: int
    provider_attempts: int
    usage_ledger_entries: int
    active_reservations: int
    active_held_micros: int
    reconciliation_needed_reservations: int
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
    ) -> UsageStatsSummary: ...
