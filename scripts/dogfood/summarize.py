"""Generate a redacted Markdown summary from reconciled dogfood results."""

import argparse
import json
from decimal import Decimal
from pathlib import Path
from typing import Any


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    position = (len(sorted_values) - 1) * p
    floor = int(position)
    ceiling = floor + 1
    if ceiling >= len(sorted_values):
        return sorted_values[-1]
    return sorted_values[floor] + (position - floor) * (
        sorted_values[ceiling] - sorted_values[floor]
    )


def metric(value: float | None, suffix: str = "") -> str:
    return "not measured" if value is None else f"{value:.1f}{suffix}"


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                raise SystemExit(
                    f"invalid JSON in reconciled file at line {line_number}"
                ) from None
            if not isinstance(row, dict):
                raise SystemExit(f"reconciled line {line_number} must be a JSON object")
            rows.append(row)
    return rows


def build_summary(rows: list[dict[str, Any]], *, application_sha: str) -> str:
    row_shas = {row.get("application_sha") for row in rows}
    if row_shas != {application_sha}:
        raise ValueError(
            "every reconciled row must identify the requested application SHA"
        )
    total = len(rows)
    found = [row for row in rows if row.get("found")]
    not_found = total - len(found)
    successes = [
        row
        for row in rows
        if row.get("http_status") == 200
        and row.get("terminal_event") in {"success", "done"}
    ]
    failed = total - len(successes)
    streaming = [row for row in rows if row.get("stream") is True]
    settled = [row for row in found if row.get("reservation_status") == "settled"]
    active = [row for row in found if row.get("reservation_status") == "reserved"]
    active_held_micros = sum(
        int(row.get("reservation_held_micros") or 0) for row in active
    )
    reconciliation_needed = [
        row
        for row in found
        if row.get("reconciliation_state") == "needs_reconciliation"
    ]
    reconciliation_active = [
        row
        for row in reconciliation_needed
        if row.get("reservation_status") == "reserved"
    ]
    reconciliation_settled = [
        row
        for row in reconciliation_needed
        if row.get("reservation_status") == "settled"
    ]
    accounting_mismatches = [
        row
        for row in found
        if not row.get("accounting_complete") or not row.get("cost_matches_reservation")
    ]

    e2e = [
        float(row["client_e2e_ms"])
        for row in rows
        if row.get("client_e2e_ms") is not None
    ]
    stream_e2e = [
        float(row["client_e2e_ms"])
        for row in streaming
        if row.get("client_e2e_ms") is not None
    ]
    ttft = [
        float(row["stream_ttft_ms"])
        for row in streaming
        if row.get("stream_ttft_ms") is not None
    ]
    gateway_overhead = [
        float(row["gateway_overhead_ms"])
        for row in found
        if row.get("is_stream") is False and row.get("gateway_overhead_ms") is not None
    ]

    attempt_count = sum(int(row.get("attempt_count") or 0) for row in found)
    ledger_count = sum(int(row.get("ledger_count") or 0) for row in found)
    input_tokens = sum(int(row.get("input_tokens") or 0) for row in found)
    output_tokens = sum(int(row.get("output_tokens") or 0) for row in found)
    total_cost = sum(
        (Decimal(str(row.get("cost_usd") or "0")) for row in found),
        start=Decimal("0"),
    )
    failovers = sum(1 for row in found if int(row.get("attempt_count") or 0) > 1)
    pii_sentinels = sum(1 for row in rows if row.get("category") == "pii_sentinel")

    return f"""# Dogfood Summary — GatewayLLM

- Application SHA: `{application_sha}`

## Overview

- Total cases: {total}
- Successful / failed: {len(successes)} / {failed}
- Streaming cases: {len(streaming)}
- Reconciled: {len(found)}
- Not found: {not_found}
- Settled reservations: {len(settled)}
- Active reservations after grace: {len(active)}
- Active held micros after grace: {active_held_micros}
- Reconciliation-needed: {len(reconciliation_needed)}
  - active recovery work: {len(reconciliation_active)}
  - settled provider-bill review: {len(reconciliation_settled)}
- Provider attempts: {attempt_count}
- Usage-ledger entries: {ledger_count}
- Attempt/ledger/cost mismatches: {len(accounting_mismatches)}
- Failover requests: {failovers}
- Tokens: {input_tokens} input / {output_tokens} output
- Accounted cost: ${total_cost:.6f}
- Fake-PII sentinel cases executed: {pii_sentinels}

## Non-Stream Gateway Overhead

| Metric | Value |
|---|---:|
| p50 | {metric(percentile(gateway_overhead, 0.50), " ms")} |
| p95 | {metric(percentile(gateway_overhead, 0.95), " ms")} |
| p99 | {metric(percentile(gateway_overhead, 0.99), " ms")} |

## Client Latency

| Metric | Value |
|---|---:|
| p50 | {metric(percentile(e2e, 0.50), " ms")} |
| p95 | {metric(percentile(e2e, 0.95), " ms")} |
| p99 | {metric(percentile(e2e, 0.99), " ms")} |

## Stream TTFT

| Metric | Value |
|---|---:|
| p50 | {metric(percentile(ttft, 0.50), " ms")} |
| p95 | {metric(percentile(ttft, 0.95), " ms")} |
| p99 | {metric(percentile(ttft, 0.99), " ms")} |

## Stream End-to-End

| Metric | Value |
|---|---:|
| p50 | {metric(percentile(stream_e2e, 0.50), " ms")} |
| p95 | {metric(percentile(stream_e2e, 0.95), " ms")} |
| p99 | {metric(percentile(stream_e2e, 0.99), " ms")} |

> Dataset note: prompts and responses are not published. This summary contains
> only aggregate metrics. PII redaction must still be confirmed separately in
> logs and Langfuse.
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Reconciled JSONL path")
    parser.add_argument("--markdown", required=True, help="Output Markdown path")
    parser.add_argument(
        "--application-sha",
        required=True,
        help="Exact tested application commit",
    )
    args = parser.parse_args()

    markdown_path = Path(args.markdown)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(
        build_summary(
            load_rows(Path(args.input)),
            application_sha=args.application_sha,
        ),
        encoding="utf-8",
    )
    print(f"Summary: {markdown_path}")


if __name__ == "__main__":
    main()
