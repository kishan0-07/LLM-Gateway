from __future__ import annotations

import json

import httpx
import pytest

from scripts.demo.concurrent_requests import (
    one_request,
    print_results,
    run_requests,
    safe_success_fields,
    successful_run,
    validate_base_url,
)

BASE_URL = "https://gateway.example"
API_KEY = "secret-demo-key"
MODEL = "openai/gpt-oss-20b"


def _normalized_response(*, content: str = "SECRET_RESPONSE") -> dict[str, object]:
    return {
        "gateway_request_id": 42,
        "content": content,
        "provider": "groq",
        "model": MODEL,
        "usage": {
            "input_tokens": 8,
            "output_tokens": 13,
            "cost_usd": "0.000007",
        },
    }


@pytest.mark.asyncio
async def test_five_calls_have_unique_full_trace_ids_and_safe_output(capsys) -> None:
    seen_traces: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        trace_id = request.headers["X-Trace-ID"]
        seen_traces.append(trace_id)
        return httpx.Response(200, json=_normalized_response())

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        results = await run_requests(client, BASE_URL, API_KEY, MODEL)

    print_results(results)
    output = capsys.readouterr().out

    assert successful_run(results)
    assert len(seen_traces) == 5
    assert len(set(seen_traces)) == 5
    assert all(len(trace.rsplit("-", 1)[-1]) == 32 for trace in seen_traces)
    assert all("content" not in result for result in results)
    assert API_KEY not in output
    assert "SECRET_RESPONSE" not in output
    assert "What is a hash table?" not in output


@pytest.mark.parametrize(
    "value",
    [
        None,
        [],
        {},
        {"gateway_request_id": True},
        {
            **_normalized_response(),
            "content": None,
        },
        {
            **_normalized_response(),
            "provider": "",
        },
        {
            **_normalized_response(),
            "usage": {
                "input_tokens": -1,
                "output_tokens": 1,
                "cost_usd": "0.1",
            },
        },
        {
            **_normalized_response(),
            "usage": {
                "input_tokens": 1,
                "output_tokens": 1,
                "cost_usd": "NaN",
            },
        },
    ],
)
def test_safe_success_fields_rejects_invalid_contracts(value: object) -> None:
    assert safe_success_fields(value) is None


def test_safe_success_fields_excludes_model_content() -> None:
    result = safe_success_fields(_normalized_response())

    assert result == {
        "gateway_request_id": 42,
        "provider": "groq",
        "model": MODEL,
        "input_tokens": 8,
        "output_tokens": 13,
        "cost_usd": "0.000007",
    }


@pytest.mark.parametrize(
    "value",
    [
        "ftp://gateway.example",
        "https://user:password@gateway.example",
        "https://gateway.example/path",
        "https://gateway.example?token=secret",
        "gateway.example",
    ],
)
def test_validate_base_url_rejects_unsafe_values_without_echoing_them(
    value: str,
) -> None:
    with pytest.raises(ValueError) as exc_info:
        validate_base_url(value)

    assert value not in str(exc_info.value)


@pytest.mark.asyncio
async def test_non_200_response_fails_without_copying_error_body() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={"error": {"message": "SECRET_PROVIDER_ERROR"}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await one_request(client, BASE_URL, API_KEY, MODEL, 0)

    assert result["status"] == 503
    assert result["error"] == "http_error"
    assert "SECRET_PROVIDER_ERROR" not in json.dumps(result)


@pytest.mark.asyncio
async def test_invalid_json_and_transport_errors_expose_only_safe_error_types() -> None:
    async def invalid_json(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(invalid_json),
    ) as client:
        invalid = await one_request(client, BASE_URL, API_KEY, MODEL, 0)

    async def transport_error(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("SECRET_CONNECTION_DETAIL")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(transport_error),
    ) as client:
        failed = await one_request(client, BASE_URL, API_KEY, MODEL, 0)

    assert invalid["error"] == "invalid_gateway_response"
    assert failed["error"] == "ConnectError"
    assert "SECRET_CONNECTION_DETAIL" not in json.dumps(failed)


def test_any_failed_request_makes_the_run_unsuccessful() -> None:
    results = [{"trace_id": f"trace-{index}", "status": 200} for index in range(5)]
    results[-1]["status"] = 503
    results[-1]["error"] = "http_error"

    assert not successful_run(results)
