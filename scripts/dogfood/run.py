"""Run redacted study requests through GatewayLLM.

Secrets come only from environment variables. Output rows never contain prompt
text, response content, headers, or credentials.
"""

import argparse
import asyncio
import json
import os
import re
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation
from pathlib import Path

import httpx

SUCCESS_TERMINALS = {"success", "done"}
APPLICATION_SHA_RE = re.compile(r"[0-9a-f]{40}")


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"ERROR: {name} environment variable is required", file=sys.stderr)
        raise SystemExit(1)
    return value


def validate_base_url(value: str) -> str:
    try:
        parsed = httpx.URL(value)
    except Exception:
        raise SystemExit("GATEWAY_BASE_URL is invalid") from None
    if parsed.username or parsed.password:
        raise SystemExit("GATEWAY_BASE_URL must not contain credentials")
    if parsed.scheme not in {"http", "https"}:
        raise SystemExit("GATEWAY_BASE_URL must use http or https")
    if not parsed.host:
        raise SystemExit("GATEWAY_BASE_URL must include a host")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise SystemExit("GATEWAY_BASE_URL must be a root origin without query data")
    return value.rstrip("/")


def validate_application_sha(value: str) -> str:
    normalized = value.strip().lower()
    if APPLICATION_SHA_RE.fullmatch(normalized) is None:
        raise SystemExit("application SHA must be exactly 40 hexadecimal characters")
    return normalized


def public_error_code(response: httpx.Response) -> str:
    """Extract only the gateway's normalized public error code."""
    try:
        payload = response.json()
    except ValueError:
        return f"http_{response.status_code}"
    if not isinstance(payload, dict):
        return f"http_{response.status_code}"
    error = payload.get("error")
    if not isinstance(error, dict):
        return f"http_{response.status_code}"
    code = error.get("code")
    return code if isinstance(code, str) else f"http_{response.status_code}"


def stream_error_code(value: object) -> str:
    """Allow only the gateway's lowercase underscore error-code shape."""
    if not isinstance(value, str):
        return "stream_error"
    if value and all(char.islower() or char.isdigit() or char == "_" for char in value):
        return value
    return "stream_error"


def normalized_nonstream_result(value: object) -> dict | None:
    """Return only validated, non-content response facts."""
    if not isinstance(value, dict):
        return None
    usage = value.get("usage")
    content = value.get("content")
    gateway_request_id = value.get("gateway_request_id")
    provider = value.get("provider")
    model = value.get("model")
    if (
        not isinstance(gateway_request_id, int)
        or isinstance(gateway_request_id, bool)
        or gateway_request_id < 1
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
        decimal_cost = Decimal(cost_usd)
    except InvalidOperation:
        return None
    if not decimal_cost.is_finite() or decimal_cost < 0:
        return None

    return {
        "gateway_request_id": gateway_request_id,
        "provider": provider,
        "model": model,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost_usd": cost_usd,
        "response_chars": len(content),
    }


def redacted_result(
    *,
    case: dict,
    trace_id: str,
    stream: bool,
    application_sha: str,
) -> dict:
    return {
        "application_sha": application_sha,
        "case_id": case["id"],
        "category": case.get("category", "general"),
        "stream": stream,
        "trace_id": trace_id,
        "http_status": 0,
        "terminal_event": None,
        "gateway_request_id": None,
        "provider": None,
        "model": None,
        "input_tokens": None,
        "output_tokens": None,
        "cost_usd": None,
        "client_e2e_ms": 0.0,
        "stream_ttft_ms": None,
        "response_chars": 0,
        "error_code": None,
    }


async def run_case(
    client: httpx.AsyncClient,
    case: dict,
    *,
    base_url: str,
    api_key: str,
    model: str,
    application_sha: str,
) -> dict:
    trace_id = f"dogfood-{case['id']}-{uuid.uuid4().hex}"
    started = time.perf_counter()
    headers = {"X-API-Key": api_key, "X-Trace-ID": trace_id}
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": case["prompt"]}],
        "max_tokens": 400,
        "stream": bool(case.get("stream", False)),
    }
    result = redacted_result(
        case=case,
        trace_id=trace_id,
        stream=payload["stream"],
        application_sha=application_sha,
    )

    try:
        url = f"{base_url.rstrip('/')}/v1/chat/completions"
        if not payload["stream"]:
            response = await client.post(
                url,
                headers=headers,
                json=payload,
                timeout=60.0,
            )
            result["client_e2e_ms"] = round(
                (time.perf_counter() - started) * 1000,
                2,
            )
            result["http_status"] = response.status_code
            if response.status_code == 200:
                try:
                    normalized = normalized_nonstream_result(response.json())
                except ValueError:
                    normalized = None
                if normalized is None:
                    result["terminal_event"] = "invalid_response"
                    result["error_code"] = "invalid_gateway_response"
                else:
                    result.update(normalized)
                    result["terminal_event"] = "success"
            else:
                result["terminal_event"] = "http_error"
                result["error_code"] = public_error_code(response)
        else:
            ttft: float | None = None
            char_count = 0
            terminal: str | None = None
            async with client.stream(
                "POST",
                url,
                headers=headers,
                json=payload,
                timeout=60.0,
            ) as response:
                result["http_status"] = response.status_code
                if response.status_code == 200:
                    async for line in response.aiter_lines():
                        if not line.startswith("data:"):
                            continue
                        raw = line.removeprefix("data:").strip()
                        if raw == "[DONE]":
                            terminal = "done"
                            break
                        try:
                            event = json.loads(raw)
                        except json.JSONDecodeError:
                            terminal = "error"
                            result["error_code"] = "malformed_sse_event"
                            break
                        if not isinstance(event, dict):
                            terminal = "error"
                            result["error_code"] = "malformed_sse_event"
                            break
                        event_type = event.get("type")
                        if event_type == "delta":
                            delta = event.get("content")
                            if isinstance(delta, str) and delta:
                                if ttft is None:
                                    ttft = round(
                                        (time.perf_counter() - started) * 1000,
                                        2,
                                    )
                                char_count += len(delta)
                        elif event_type == "error":
                            terminal = "error"
                            result["error_code"] = stream_error_code(
                                event.get("content")
                            )
                            break
                else:
                    await response.aread()
                    terminal = "http_error"
                    result["error_code"] = public_error_code(response)

            if terminal is None and result["error_code"] is None:
                terminal = "error"
                result["error_code"] = "stream_terminated_without_terminal"
            result["client_e2e_ms"] = round(
                (time.perf_counter() - started) * 1000,
                2,
            )
            result["stream_ttft_ms"] = ttft
            result["response_chars"] = char_count
            result["terminal_event"] = terminal
    except (httpx.HTTPError, TimeoutError) as exc:
        result["client_e2e_ms"] = round(
            (time.perf_counter() - started) * 1000,
            2,
        )
        result["terminal_event"] = "transport_error"
        result["error_code"] = type(exc).__name__
    return result


def load_cases(path: Path) -> list[dict]:
    cases: list[dict] = []
    case_ids: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                case = json.loads(stripped)
            except json.JSONDecodeError:
                raise SystemExit(
                    f"invalid JSON in prompt file at line {line_number}"
                ) from None
            if not isinstance(case, dict):
                raise SystemExit(
                    f"prompt file line {line_number} must be a JSON object"
                )
            case_id = case.get("id")
            prompt = case.get("prompt")
            stream = case.get("stream", False)
            if not isinstance(case_id, str) or not case_id:
                raise SystemExit("every case requires a non-empty string id")
            if case_id in case_ids:
                raise SystemExit(f"duplicate case id: {case_id}")
            if not isinstance(prompt, str) or not prompt:
                raise SystemExit(f"case {case_id} requires a prompt")
            if not isinstance(stream, bool):
                raise SystemExit(f"case {case_id} stream must be boolean")
            case_ids.add(case_id)
            cases.append(case)
    return cases


async def execute_cases(
    cases: list[dict],
    *,
    output_path: Path,
    base_url: str,
    api_key: str,
    model: str,
    application_sha: str,
    client: httpx.AsyncClient | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        raise SystemExit(f"refusing to append to existing result file: {output_path}")

    owns_client = client is None
    active_client = client or httpx.AsyncClient()
    failed_cases = 0
    seen_trace_ids: set[str] = set()
    try:
        with output_path.open("x", encoding="utf-8") as out:
            for index, case in enumerate(cases, 1):
                label = "STREAM" if case.get("stream") else "SYNC"
                print(
                    f"[{index}/{len(cases)}] {case['id']} ({label})... ",
                    end="",
                    flush=True,
                )
                result = await run_case(
                    active_client,
                    case,
                    base_url=base_url,
                    api_key=api_key,
                    model=model,
                    application_sha=application_sha,
                )
                if result["trace_id"] in seen_trace_ids:
                    raise RuntimeError("dogfood runner generated a duplicate trace ID")
                seen_trace_ids.add(result["trace_id"])
                out.write(json.dumps(result, ensure_ascii=False) + "\n")
                out.flush()
                if result["terminal_event"] not in SUCCESS_TERMINALS:
                    failed_cases += 1
                print(
                    f"HTTP {result['http_status']} | "
                    f"{result['client_e2e_ms']}ms | "
                    f"{result['response_chars']} chars"
                )
                await asyncio.sleep(0.3)
    finally:
        if owns_client:
            await active_client.aclose()
    return failed_cases


async def main() -> None:
    parser = argparse.ArgumentParser(description="GatewayLLM Dogfood Runner")
    parser.add_argument("--input", required=True, help="Path to prompts JSONL")
    parser.add_argument("--output", required=True, help="Path to results JSONL")
    parser.add_argument(
        "--application-sha",
        required=True,
        help="Exact 40-character Git SHA of the application under test",
    )
    args = parser.parse_args()

    base_url = validate_base_url(require_env("GATEWAY_BASE_URL"))
    api_key = require_env("GATEWAY_API_KEY")
    model = os.getenv("GATEWAY_MODEL", "openai/gpt-oss-20b")
    application_sha = validate_application_sha(args.application_sha)
    cases = load_cases(Path(args.input))
    print(f"[dogfood] {len(cases)} cases against {base_url}")
    failed_cases = await execute_cases(
        cases,
        output_path=Path(args.output),
        base_url=base_url,
        api_key=api_key,
        model=model,
        application_sha=application_sha,
    )
    print(f"Results written to {args.output}")
    if failed_cases:
        raise SystemExit(
            f"{failed_cases} dogfood case(s) failed; inspect redacted results"
        )


if __name__ == "__main__":
    asyncio.run(main())
