"""Send five concurrent, traceable requests through GatewayLLM.

The script reads ``GATEWAY_BASE_URL``, ``GATEWAY_API_KEY``, and the optional
``GATEWAY_MODEL`` environment variable. It deliberately prints only safe
operational metadata: status, trace ID, provider, model, token counts, cost,
and latency. Prompts, model output, and credentials are never printed.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation

import httpx

REQUEST_COUNT = 5
DEFAULT_MODEL = "openai/gpt-oss-20b"
PROMPTS = (
    "What is a hash table?",
    "Explain TCP versus UDP briefly.",
    "What is Big O notation?",
    "Define the ACID properties.",
    "What is a mutex?",
)


def require_env(name: str) -> str:
    """Return a required environment variable without exposing its value."""
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def validate_base_url(value: str) -> str:
    """Accept only a credential-free HTTP(S) root origin."""
    try:
        parsed = httpx.URL(value)
    except Exception:
        raise ValueError("GATEWAY_BASE_URL is invalid") from None

    if parsed.username or parsed.password:
        raise ValueError("GATEWAY_BASE_URL must not contain credentials")
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("GATEWAY_BASE_URL must use http or https")
    if not parsed.host:
        raise ValueError("GATEWAY_BASE_URL must include a host")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("GATEWAY_BASE_URL must be a root origin without query data")
    return value.rstrip("/")


def safe_success_fields(value: object) -> dict[str, object] | None:
    """Validate the normalized response and return only allowlisted metadata."""
    if not isinstance(value, dict):
        return None

    usage = value.get("usage")
    content = value.get("content")
    request_id = value.get("gateway_request_id")
    provider = value.get("provider")
    model = value.get("model")
    if (
        not isinstance(request_id, int)
        or isinstance(request_id, bool)
        or request_id < 1
        or not isinstance(content, str)
        or not isinstance(provider, str)
        or not provider
        or not isinstance(model, str)
        or not model
        or not isinstance(usage, dict)
    ):
        return None

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
        return None

    try:
        cost = Decimal(cost_usd)
    except InvalidOperation:
        return None
    if not cost.is_finite() or cost < 0:
        return None

    return {
        "gateway_request_id": request_id,
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
    }


async def one_request(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    model: str,
    index: int,
) -> dict[str, object]:
    """Execute one request and return only safe diagnostic fields."""
    trace_id = f"demo-concurrent-{index}-{uuid.uuid4().hex}"
    headers = {"X-API-Key": api_key, "X-Trace-ID": trace_id}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": PROMPTS[index]}],
        "max_tokens": 120,
        "stream": False,
    }

    started = time.perf_counter()
    try:
        response = await client.post(
            f"{base_url}/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=60.0,
        )
        result: dict[str, object] = {
            "index": index,
            "trace_id": trace_id,
            "status": response.status_code,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }
        if response.status_code != 200:
            result["error"] = "http_error"
            return result

        try:
            safe_fields = safe_success_fields(response.json())
        except ValueError:
            safe_fields = None
        if safe_fields is None:
            result["error"] = "invalid_gateway_response"
            return result

        result.update(safe_fields)
        return result
    except (httpx.HTTPError, ValueError) as exc:
        return {
            "index": index,
            "trace_id": trace_id,
            "status": 0,
            "error": type(exc).__name__,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }


async def run_requests(
    client: httpx.AsyncClient,
    base_url: str,
    api_key: str,
    model: str,
) -> list[dict[str, object]]:
    """Run the fixed five-request demo concurrently."""
    return list(
        await asyncio.gather(
            *(
                one_request(client, base_url, api_key, model, index)
                for index in range(REQUEST_COUNT)
            )
        )
    )


def successful_run(results: list[dict[str, object]]) -> bool:
    """Require five valid successes with five unique trace IDs."""
    traces = {result.get("trace_id") for result in results}
    return (
        len(results) == REQUEST_COUNT
        and len(traces) == REQUEST_COUNT
        and all(
            result.get("status") == 200 and "error" not in result for result in results
        )
    )


def print_results(results: list[dict[str, object]]) -> None:
    """Print allowlisted results and a compact pass/fail summary."""
    print(json.dumps(results, indent=2))
    succeeded = sum(
        result.get("status") == 200 and "error" not in result for result in results
    )
    unique_traces = len({result.get("trace_id") for result in results})
    marker = "PASS" if successful_run(results) else "FAIL"
    print(f"\n[{marker}] {succeeded}/{REQUEST_COUNT} succeeded")
    print(f"Unique trace IDs: {unique_traces}/{REQUEST_COUNT}")


async def main() -> int:
    """Load configuration, execute the demo, and return a process exit code."""
    try:
        base_url = validate_base_url(require_env("GATEWAY_BASE_URL"))
        api_key = require_env("GATEWAY_API_KEY")
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    model = os.getenv("GATEWAY_MODEL", DEFAULT_MODEL)
    print(f"Sending {REQUEST_COUNT} concurrent requests to {base_url}...")

    async with httpx.AsyncClient(timeout=60.0) as client:
        results = await run_requests(client, base_url, api_key, model)

    print_results(results)
    return 0 if successful_run(results) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
