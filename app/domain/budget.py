from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


MICROS_PER_DOLLAR = Decimal("1000000")


def to_micros(value: Decimal | float | str) -> int:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    return int(
        (decimal_value * MICROS_PER_DOLLAR).quantize(
            Decimal("1"),
            rounding=ROUND_HALF_UP,
        )
    )


def micros_to_decimal(value: int) -> Decimal:
    return (Decimal(value) / MICROS_PER_DOLLAR).quantize(Decimal("0.000001"))


@dataclass(frozen=True)
class ReservationRequest:
    tenant_id: int
    gateway_request_id: int
    requested_model: str
    estimated_input_tokens: int
    estimated_output_tokens: int
    estimated_tokens: int
    estimated_cost_micros: int


@dataclass(frozen=True)
class ReservationResult:
    approved: bool
    reservation_id: str | None
    reason: str | None = None
