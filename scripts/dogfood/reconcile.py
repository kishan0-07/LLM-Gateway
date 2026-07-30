"""Reconcile dogfood trace IDs against PostgreSQL without content fields."""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.budget import micros_to_decimal
from app.infrastructure.db.models import (
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal

SAFE_CLIENT_FIELDS = {
    "application_sha",
    "case_id",
    "category",
    "stream",
    "trace_id",
    "http_status",
    "terminal_event",
    "gateway_request_id",
    "provider",
    "model",
    "input_tokens",
    "output_tokens",
    "cost_usd",
    "client_e2e_ms",
    "stream_ttft_ms",
    "response_chars",
    "error_code",
}


def safe_client_fields(client_row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in client_row.items() if key in SAFE_CLIENT_FIELDS
    }


async def reconcile_trace(
    session: AsyncSession,
    client_row: dict[str, Any],
) -> dict[str, Any]:
    safe_client_row = safe_client_fields(client_row)
    trace_id = safe_client_row.get("trace_id")
    if not isinstance(trace_id, str) or not trace_id:
        raise RuntimeError("client row is missing a valid trace ID")

    requests = list(
        await session.scalars(
            select(GatewayRequest)
            .where(GatewayRequest.trace_id == trace_id)
            .order_by(GatewayRequest.id)
        )
    )
    if not requests:
        return {**safe_client_row, "found": False}
    if len(requests) != 1:
        raise RuntimeError(f"ambiguous duplicate trace id: {trace_id}")
    request = requests[0]

    reported_request_id = safe_client_row.get("gateway_request_id")
    if reported_request_id is not None and reported_request_id != request.id:
        raise RuntimeError(f"gateway request id mismatch for trace: {trace_id}")

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

    final_attempt = attempts[-1] if attempts else None
    final_ledger = ledger[-1] if ledger else None
    total_input_tokens = sum(row.input_tokens for row in ledger)
    total_output_tokens = sum(row.output_tokens for row in ledger)
    total_cost_micros = sum(row.cost_micros for row in ledger)
    attempt_ids = {attempt.id for attempt in attempts}
    ledger_attempt_ids = [
        row.provider_attempt_id for row in ledger if row.provider_attempt_id is not None
    ]
    accounting_complete = (
        len(ledger) == len(attempts)
        and len(ledger_attempt_ids) == len(set(ledger_attempt_ids))
        and attempt_ids == set(ledger_attempt_ids)
    )
    reservation_consumed_micros = reservation.consumed_micros if reservation else None

    return {
        **safe_client_row,
        "found": True,
        "gateway_request_id": request.id,
        "request_id": request.id,
        "request_status": request.status,
        "is_stream": request.is_stream,
        "gateway_overhead_ms": request.gateway_overhead_ms,
        "reservation_status": reservation.status if reservation else None,
        "reservation_final_status": (reservation.final_status if reservation else None),
        "reservation_held_micros": (reservation.held_micros if reservation else None),
        "reservation_consumed_micros": reservation_consumed_micros,
        "reconciliation_state": (
            reservation.reconciliation_state if reservation else None
        ),
        "reconciliation_reason": (
            reservation.reconciliation_reason if reservation else None
        ),
        "provider": (
            final_ledger.provider
            if final_ledger is not None
            else (final_attempt.provider if final_attempt is not None else None)
        ),
        "model": (
            final_ledger.model
            if final_ledger is not None
            else (final_attempt.model if final_attempt is not None else None)
        ),
        "input_tokens": total_input_tokens,
        "output_tokens": total_output_tokens,
        "cost_micros": total_cost_micros,
        "cost_usd": f"{micros_to_decimal(total_cost_micros):.6f}",
        "accounting_complete": accounting_complete,
        "cost_matches_reservation": (
            reservation_consumed_micros == total_cost_micros
            if reservation_consumed_micros is not None
            else False
        ),
        "attempt_count": len(attempts),
        "attempts": [
            {
                "attempt_id": attempt.id,
                "attempt_number": attempt.attempt_number,
                "provider": attempt.provider,
                "model": attempt.model,
                "status": attempt.status,
                "latency_ms": attempt.latency_ms,
            }
            for attempt in attempts
        ],
        "ledger_count": len(ledger),
        "ledger": [
            {
                "ledger_id": row.id,
                "attempt_id": row.provider_attempt_id,
                "provider": row.provider,
                "model": row.model,
                "input_tokens": row.input_tokens,
                "output_tokens": row.output_tokens,
                "cost_micros": row.cost_micros,
                "usage_source": row.usage_source,
                "billing_status": row.billing_status,
            }
            for row in ledger
        ],
    }


def load_result_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_trace_ids: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                row = json.loads(stripped)
            except json.JSONDecodeError:
                raise SystemExit(
                    f"invalid JSON in result file at line {line_number}"
                ) from None
            if not isinstance(row, dict):
                raise SystemExit(
                    f"result file line {line_number} must be a JSON object"
                )
            trace_id = row.get("trace_id")
            if not isinstance(trace_id, str) or not trace_id:
                raise SystemExit("result row is missing trace_id")
            if trace_id in seen_trace_ids:
                raise SystemExit(f"duplicate trace id in result file: {trace_id}")
            seen_trace_ids.add(trace_id)
            rows.append(row)
    return rows


async def reconcile_rows(
    rows: list[dict[str, Any]],
    output_path: Path,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        raise SystemExit(f"refusing to append to existing output file: {output_path}")

    missing = 0
    async with AsyncSessionLocal() as session:
        with output_path.open("x", encoding="utf-8") as out:
            for client_row in rows:
                record = await reconcile_trace(session, client_row)
                if not record.get("found"):
                    missing += 1
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
    return missing


async def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile dogfood trace IDs")
    parser.add_argument("--results", required=True, help="Results JSONL path")
    parser.add_argument("--output", required=True, help="Reconciled output JSONL")
    args = parser.parse_args()

    rows = load_result_rows(Path(args.results))
    print(f"Reconciling {len(rows)} trace IDs...")
    missing = await reconcile_rows(rows, Path(args.output))
    print(f"Reconciled output: {args.output}")
    if missing:
        raise SystemExit(f"{missing} dogfood trace(s) were not found in PostgreSQL")


if __name__ == "__main__":
    asyncio.run(main())
