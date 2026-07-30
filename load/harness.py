"""Pure validation and SSE measurement helpers for the Locust harness."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

import httpx


class StreamResponseMetadata(Protocol):
    request_meta: dict[str, Any]


@dataclass(frozen=True)
class StreamObservation:
    success: bool
    error: str | None
    ttft_ms: float | None
    e2e_ms: float
    response_bytes: int


def validated_base_url(value: str | None = None) -> str:
    """Return a credential-free HTTP(S) root origin."""
    configured = value if value is not None else os.environ.get("GATEWAY_BASE_URL")
    if not configured:
        raise RuntimeError("GATEWAY_BASE_URL is required")
    try:
        parsed = httpx.URL(configured)
    except Exception:
        raise RuntimeError("GATEWAY_BASE_URL is invalid") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.host
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(
            "GATEWAY_BASE_URL must be a credential-free HTTP(S) root origin"
        )
    return configured.rstrip("/")


def configured_host() -> str | None:
    """Validate a configured host while keeping test imports side-effect free."""
    value = os.environ.get("GATEWAY_BASE_URL")
    return validated_base_url(value) if value else None


def valid_nonstream_response(value: object) -> bool:
    """Validate the public normalized response without retaining its content."""
    if not isinstance(value, dict):
        return False
    usage = value.get("usage")
    request_id = value.get("gateway_request_id")
    provider = value.get("provider")
    model = value.get("model")
    if (
        not isinstance(request_id, int)
        or isinstance(request_id, bool)
        or request_id < 1
        or not isinstance(value.get("content"), str)
        or not isinstance(provider, str)
        or not provider
        or not isinstance(model, str)
        or not model
        or not isinstance(usage, dict)
    ):
        return False

    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    cost_usd = usage.get("cost_usd")
    if (
        not isinstance(input_tokens, int)
        or isinstance(input_tokens, bool)
        or input_tokens < 0
        or not isinstance(output_tokens, int)
        or isinstance(output_tokens, bool)
        or output_tokens < 0
        or not isinstance(cost_usd, str)
    ):
        return False

    try:
        cost = Decimal(cost_usd)
    except InvalidOperation:
        return False
    return cost.is_finite() and cost >= 0


def public_stream_code(value: object) -> str:
    """Allow only a public snake-case code into Locust failure summaries."""
    if not isinstance(value, str):
        return "stream_error"
    if value and all(
        character.islower() or character.isdigit() or character == "_"
        for character in value
    ):
        return value
    return "stream_error"


def new_trace_id(*, stream: bool) -> str:
    """Return a unique full-UUID trace ID."""
    prefix = "locust-stream" if stream else "locust"
    return f"{prefix}-{uuid.uuid4().hex}"


def observe_stream(
    lines: Iterable[str | bytes],
    *,
    started_at: float,
    clock=time.perf_counter,
) -> StreamObservation:
    """Consume SSE through its terminal event and compute TTFT and E2E."""
    saw_done = False
    error: str | None = None
    ttft_ms: float | None = None
    response_bytes = 0

    for raw_line in lines:
        line = (
            raw_line.decode("utf-8", errors="replace")
            if isinstance(raw_line, bytes)
            else raw_line
        )
        response_bytes += len(line.encode("utf-8"))
        if not line.startswith("data:"):
            continue

        payload = line.removeprefix("data:").strip()
        if payload == "[DONE]":
            saw_done = True
            break
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            error = "malformed_sse_json"
            break
        if not isinstance(event, dict):
            error = "malformed_sse_json"
            break
        if (
            event.get("type") == "delta"
            and isinstance(event.get("content"), str)
            and event["content"]
            and ttft_ms is None
        ):
            ttft_ms = (clock() - started_at) * 1000
        if event.get("type") == "error":
            error = public_stream_code(event.get("content"))
            break

    e2e_ms = (clock() - started_at) * 1000
    if error is None and not saw_done:
        error = "missing_terminal_done"

    return StreamObservation(
        success=error is None and saw_done,
        error=error,
        ttft_ms=ttft_ms,
        e2e_ms=e2e_ms,
        response_bytes=response_bytes,
    )


def apply_stream_timing(
    response: StreamResponseMetadata,
    observation: StreamObservation,
) -> None:
    """Override Locust's header-only stream timing with true body E2E."""
    response.request_meta["response_time"] = observation.e2e_ms
    response.request_meta["response_length"] = observation.response_bytes
