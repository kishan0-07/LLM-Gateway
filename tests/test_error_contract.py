import json
from unittest.mock import patch

import pytest
from fastapi import Request
from fastapi.exceptions import RequestValidationError
from httpx import ASGITransport, AsyncClient

from app.api.errors import validation_exception_handler
from app.api.routes.completions import _http_error_for_provider_error
from app.domain.provider import ProviderError
from app.main import app


def assert_error_payload(response, code: str, trace_id: str) -> None:
    body = response.json()
    assert body["error"]["code"] == code
    assert body["error"]["trace_id"] == trace_id
    assert isinstance(body["error"]["message"], str)
    assert "traceback" not in body["error"]["message"].lower()


@pytest.mark.asyncio
async def test_validation_error_emits_safe_structured_trace_log() -> None:
    request = Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/v1/chat/completions",
            "headers": [],
            "state": {"trace_id": "wp4-negative-control"},
        }
    )
    validation_error = RequestValidationError(
        [
            {
                "type": "missing",
                "loc": ("body", "messages"),
                "msg": "Field required",
                "input": {"model": "gpt-5.4-mini"},
            }
        ]
    )

    with patch("app.api.errors.logger.info") as log_info:
        response = await validation_exception_handler(request, validation_error)

    assert response.status_code == 422
    assert json.loads(response.body)["error"]["code"] == "validation_error"
    log_info.assert_called_once_with(
        "request_validation_failed",
        trace_id="wp4-negative-control",
        status_code=422,
        error_count=1,
    )


@pytest.mark.asyncio
async def test_unknown_route_uses_standard_error_and_trace_id() -> None:
    trace_id = "day11-not-found"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/not-a-route",
            headers={"X-Trace-ID": trace_id},
        )

    assert response.status_code == 404
    assert response.headers["X-Trace-ID"] == trace_id
    assert_error_payload(response, "not_found", trace_id)


@pytest.mark.asyncio
async def test_missing_key_uses_standard_error_and_trace_id() -> None:
    trace_id = "day11-missing-key"
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/whoami",
            headers={"X-Trace-ID": trace_id},
        )

    assert response.status_code == 401
    assert response.headers["X-Trace-ID"] == trace_id
    assert_error_payload(response, "authentication_failed", trace_id)


def test_provider_invalid_request_does_not_expose_sdk_message() -> None:
    error = _http_error_for_provider_error(
        ProviderError(
            provider="groq",
            category="invalid_request",
            message=(
                "POST https://api.groq.com failed with Authorization: secret-value"
            ),
            retryable=False,
        )
    )

    assert error.status_code == 400
    assert error.detail == {
        "code": "invalid_request",
        "message": "The selected model provider rejected the request",
    }


def test_gateway_wide_provider_unavailability_is_503() -> None:
    error = _http_error_for_provider_error(
        ProviderError(
            provider="gateway",
            category="server_error",
            message="no provider candidates are configured",
            retryable=True,
        )
    )

    assert error.status_code == 503
    assert error.detail == {
        "code": "provider_unavailable",
        "message": "All configured providers are currently unavailable",
    }
