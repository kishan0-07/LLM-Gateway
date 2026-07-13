from pydantic import BaseModel


class GatewayOverheadResponse(BaseModel):
    average_ms: float | None
    samples: int


class UsageStatsResponse(BaseModel):
    scope: str
    total_requests: int
    settled_requests: int
    total_input_tokens: int
    total_output_tokens: int
    total_cost_usd: str
    failover_count: int
    gateway_overhead_ms: GatewayOverheadResponse