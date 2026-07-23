import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


def assert_error_payload(response, code: str, trace_id: str) -> None:
    body = response.json()
    assert body["error"]["code"] == code
    assert body["error"]["trace_id"] == trace_id
    assert isinstance(body["error"]["message"], str)
    assert "traceback" not in body["error"]["message"].lower()


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
