import argparse
import asyncio
import json
import time
from typing import Any

from sqlalchemy import select

from app.infrastructure.db.models import (
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal


async def snapshot(trace_id: str) -> dict[str, Any]:
    async with AsyncSessionLocal() as session:
        requests = list(
            await session.scalars(
                select(GatewayRequest)
                .where(GatewayRequest.trace_id == trace_id)
                .order_by(GatewayRequest.id)
            )
        )
        if not requests:
            return {"request": None}
        if len(requests) != 1:
            raise RuntimeError(f"trace_id matched {len(requests)} gateway requests")
        request = requests[0]

        reservation = await session.scalar(
            select(BudgetReservation).where(
                BudgetReservation.gateway_request_id == request.id
            )
        )
        attempts = list(
            await session.scalars(
                select(ProviderAttempt)
                .where(ProviderAttempt.gateway_request_id == request.id)
                .order_by(ProviderAttempt.attempt_number)
            )
        )
        ledger = list(
            await session.scalars(
                select(UsageLedger)
                .where(UsageLedger.gateway_request_id == request.id)
                .order_by(UsageLedger.provider_attempt_id)
            )
        )

        return {
            "request": request,
            "reservation": reservation,
            "attempts": attempts,
            "ledger": ledger,
        }


def assert_mode(state: dict[str, Any], mode: str) -> dict[str, Any]:
    request = state["request"]

    if mode == "database-preflight":
        assert request is None, "request row exists even though auth database was down"
        return {"request": "absent", "provider_attempts": 0, "ledger_rows": 0}

    assert request is not None, "gateway request row was not found"
    reservation = state["reservation"]
    attempts = state["attempts"]
    ledger = state["ledger"]

    if mode == "rate-limit":
        assert request.status == "rate_limit_unavailable", request.status
        assert reservation is None
        assert attempts == []
        assert ledger == []
        return {
            "request_status": request.status,
            "provider_attempts": 0,
            "ledger_rows": 0,
        }

    if mode == "inspect":
        # Read-only, allowlisted inspection. Never add prompts, responses,
        # credentials, connection values, or raw provider errors.
        attempt_ids = {attempt.id for attempt in attempts}
        ledger_attempt_ids = {
            row.provider_attempt_id
            for row in ledger
            if row.provider_attempt_id is not None
        }
        ledger_cost_micros = sum(row.cost_micros for row in ledger)
        return {
            "request_id": request.id,
            "request_status": request.status,
            "is_stream": request.is_stream,
            "gateway_overhead_ms": request.gateway_overhead_ms,
            "reservation_status": (
                reservation.status if reservation is not None else None
            ),
            "reservation_final_status": (
                reservation.final_status if reservation is not None else None
            ),
            "reservation_held_micros": (
                reservation.held_micros if reservation is not None else None
            ),
            "reservation_consumed_micros": (
                reservation.consumed_micros if reservation is not None else None
            ),
            "reconciliation_state": (
                reservation.reconciliation_state if reservation is not None else None
            ),
            "reconciliation_reason": (
                reservation.reconciliation_reason if reservation is not None else None
            ),
            "attempts": [
                {
                    "attempt_id": attempt.id,
                    "attempt_number": attempt.attempt_number,
                    "provider": attempt.provider,
                    "model": attempt.model,
                    "status": attempt.status,
                    "latency_ms": attempt.latency_ms,
                    "authorized_cost_micros": attempt.authorized_cost_micros,
                }
                for attempt in attempts
            ],
            "ledger": [
                {
                    "ledger_id": row.id,
                    "attempt_id": row.provider_attempt_id,
                    "provider": row.provider,
                    "model": row.model,
                    "input_tokens": row.input_tokens,
                    "output_tokens": row.output_tokens,
                    "usage_source": row.usage_source,
                    "billing_status": row.billing_status,
                    "cost_micros": row.cost_micros,
                }
                for row in ledger
            ],
            "accounting_complete": (
                len(ledger) == len(attempts) and ledger_attempt_ids == attempt_ids
            ),
            "cost_matches_reservation": (
                reservation is not None
                and reservation.consumed_micros == ledger_cost_micros
            ),
        }

    assert mode == "disconnect"
    assert reservation is not None
    assert request.status == "cancelled", f"request_status={request.status}"
    assert reservation.status == "settled", f"reservation_status={reservation.status}"
    assert reservation.final_status == "cancelled", (
        f"reservation_final_status={reservation.final_status}"
    )
    assert reservation.held_micros == 0, (
        f"reservation_held_micros={reservation.held_micros}"
    )
    assert attempts, "disconnect did not start a provider attempt"
    assert all(attempt.status != "started" for attempt in attempts), (
        f"attempt_statuses={[attempt.status for attempt in attempts]}"
    )
    assert {row.provider_attempt_id for row in ledger} == {
        attempt.id for attempt in attempts
    }
    assert reservation.consumed_micros == sum(row.cost_micros for row in ledger)

    if reservation.reconciliation_state == "needs_reconciliation":
        assert reservation.reconciliation_reason == "provider_usage_unavailable", (
            f"reconciliation_reason={reservation.reconciliation_reason}"
        )
        assert any(row.usage_source == "conservative" for row in ledger)
        assert any(row.billing_status == "estimated" for row in ledger)
    else:
        assert reservation.reconciliation_state in {"none", "reconciled"}, (
            f"reconciliation_state={reservation.reconciliation_state}"
        )

    return {
        "request_status": request.status,
        "reservation_status": reservation.status,
        "final_status": reservation.final_status,
        "held_micros": reservation.held_micros,
        "reconciliation_state": reservation.reconciliation_state,
        "reconciliation_reason": reservation.reconciliation_reason,
        "provider_attempts": len(attempts),
        "ledger_rows": len(ledger),
        "ledger_usage_sources": [row.usage_source for row in ledger],
        "ledger_billing_statuses": [row.billing_status for row in ledger],
        "consumed_micros": reservation.consumed_micros,
    }


def summarize_state(state: dict[str, Any]) -> dict[str, Any]:
    request = state["request"]
    if request is None:
        return {"request": None}

    reservation = state["reservation"]
    attempts = state["attempts"]
    ledger = state["ledger"]
    return {
        "request_status": request.status,
        "reservation_status": (reservation.status if reservation is not None else None),
        "reservation_final_status": (
            reservation.final_status if reservation is not None else None
        ),
        "reservation_held_micros": (
            reservation.held_micros if reservation is not None else None
        ),
        "reconciliation_state": (
            reservation.reconciliation_state if reservation is not None else None
        ),
        "reconciliation_reason": (
            reservation.reconciliation_reason if reservation is not None else None
        ),
        "attempt_statuses": [attempt.status for attempt in attempts],
        "ledger_rows": len(ledger),
        "ledger_usage_sources": [row.usage_source for row in ledger],
        "ledger_billing_statuses": [row.billing_status for row in ledger],
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--trace-id", required=True)
    parser.add_argument(
        "--mode",
        choices=["rate-limit", "database-preflight", "disconnect", "inspect"],
        required=True,
    )
    parser.add_argument("--wait-seconds", type=float, default=20.0)
    args = parser.parse_args()

    deadline = time.monotonic() + args.wait_seconds
    last_error: AssertionError | None = None
    while time.monotonic() < deadline:
        state = await snapshot(args.trace_id)
        try:
            result = assert_mode(state, args.mode)
        except AssertionError as exc:
            last_error = exc
            await asyncio.sleep(0.5)
            continue

        print(json.dumps({"trace_id": args.trace_id, **result}))
        return

    raise AssertionError(
        "durable state did not converge for "
        f"{args.trace_id}: {last_error}; state={summarize_state(state)}"
    )


if __name__ == "__main__":
    asyncio.run(main())
