from pydantic import BaseModel


class GatewayOverheadResponse(BaseModel):
    average_ms: float | None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    samples: int


class UsageStatsResponse(BaseModel):
    scope: str
    total_requests: int
    settled_requests: int
    settled_reservations: int
    provider_attempts: int
    usage_ledger_entries: int
    active_reservations: int
    active_held_micros: int
    reconciliation_needed_reservations: int
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: str
    failover_count: int
    gateway_overhead_ms: GatewayOverheadResponse
