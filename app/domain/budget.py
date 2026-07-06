from dataclasses import dataclass


@dataclass(frozen=True)
class ReservationRequest:
    tenant_id: int
    gateway_request_id: int
    estimated_tokens: int
    estimated_cost_usd: float


@dataclass(frozen=True)
class ReservationResult:
    approved: bool
    reservation_id: str | None
    reason: str | None = None