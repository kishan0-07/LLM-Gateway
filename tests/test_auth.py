import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app


@pytest.mark.asyncio
async def test_health_does_not_require_auth():
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_missing_api_key_returns_401():
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )

    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "authentication_failed"
    assert body["error"]["message"] == "Missing API key"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_invalid_api_key_returns_401():
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
            },
            headers={"X-API-Key": "this-key-does-not-exist"},
        )

    assert response.status_code == 401
    body = response.json()
    assert body["error"]["code"] == "authentication_failed"
    assert body["error"]["message"] == "Invalid API key"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_valid_api_key_returns_principal(test_env):
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/whoami",
            headers={"X-API-Key": test_env["api_key"]},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["tenant_id"] == test_env["tenant_id"]
    assert body["api_key_id"] == test_env["api_key_id"]


@pytest.mark.asyncio
async def test_trace_id_returned_on_auth_failure():
    trace_id = "auth-trace-test"

    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/chat/completions",
            json={
                "model": "gpt-5.4-mini",
                "messages": [{"role": "user", "content": "hello"}],
            },
            headers={"X-Trace-ID": trace_id},
        )

    assert response.status_code == 401
    assert response.headers["X-Trace-ID"] == trace_id
    assert response.json()["error"]["trace_id"] == trace_id
