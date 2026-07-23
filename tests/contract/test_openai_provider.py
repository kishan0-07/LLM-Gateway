import pytest
from httpx import Request, Response
from unittest.mock import AsyncMock
from types import SimpleNamespace
from openai import BadRequestError, InternalServerError

from app.infrastructure.providers.openai import OpenAIProvider
from app.domain.provider import ProviderError, ProviderStreamEvent


def completion_response(content="answer", input_tokens=4, output_tokens=6):
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=content))],
        usage=SimpleNamespace(
            prompt_tokens=input_tokens,
            completion_tokens=output_tokens,
        ),
    )


def install_completion_client(provider, response):
    create = AsyncMock(return_value=response)
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    return create


@pytest.mark.asyncio
async def test_openai_normal_completion():
    provider = OpenAIProvider(api_key="fake-key")
    resp = completion_response("hello world", 10, 15)
    create_mock = install_completion_client(provider, resp)

    res = await provider.complete(
        "gpt-5.4-mini", [{"role": "user", "content": "hi"}], max_tokens=50
    )

    assert res.provider == "openai"
    assert res.model == "gpt-5.4-mini"
    assert res.content == "hello world"
    assert res.input_tokens == 10
    assert res.output_tokens == 15
    create_mock.assert_called_once_with(
        model="gpt-5.4-mini",
        messages=[{"role": "user", "content": "hi"}],
        max_completion_tokens=50,
    )


@pytest.mark.asyncio
async def test_openai_bad_request_error():
    provider = OpenAIProvider(api_key="fake-key")
    http_resp = Response(400, request=Request("POST", "https://api.openai.com"))
    exc = BadRequestError("Invalid model parameter", response=http_resp, body=None)

    create_mock = AsyncMock(side_effect=exc)
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock))
    )

    with pytest.raises(ProviderError) as exc_info:
        await provider.complete("gpt-5.4-mini", [], max_tokens=50)

    assert exc_info.value.category == "invalid_request"
    assert not exc_info.value.retryable


@pytest.mark.asyncio
async def test_openai_server_error():
    provider = OpenAIProvider(api_key="fake-key")
    http_resp = Response(500, request=Request("POST", "https://api.openai.com"))
    exc = InternalServerError("Internal server error", response=http_resp, body=None)

    create_mock = AsyncMock(side_effect=exc)
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock))
    )

    with pytest.raises(ProviderError) as exc_info:
        await provider.complete("gpt-5.4-mini", [], max_tokens=50)

    assert exc_info.value.category == "server_error"
    assert exc_info.value.retryable


@pytest.mark.asyncio
async def test_openai_stream_normal():
    provider = OpenAIProvider(api_key="fake-key")

    chunks = [
        SimpleNamespace(
            choices=[SimpleNamespace(delta=SimpleNamespace(content="hello"))],
            usage=None,
        ),
        SimpleNamespace(
            choices=[SimpleNamespace(delta=SimpleNamespace(content=" world"))],
            usage=None,
        ),
        SimpleNamespace(
            choices=[], usage=SimpleNamespace(prompt_tokens=10, completion_tokens=5)
        ),
    ]

    async def fake_stream_iter():
        for c in chunks:
            yield c

    create_mock = AsyncMock(return_value=fake_stream_iter())
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock))
    )

    events = [
        e
        async for e in provider.stream(
            "gpt-5.4-mini", [{"role": "user", "content": "hi"}], max_tokens=50
        )
    ]

    assert len(events) == 4
    assert events[0] == ProviderStreamEvent(type="delta", content="hello")
    assert events[1] == ProviderStreamEvent(type="delta", content=" world")
    assert events[2] == ProviderStreamEvent(
        type="usage", input_tokens=10, output_tokens=5
    )
    assert events[3] == ProviderStreamEvent(type="done")
    create_mock.assert_called_once_with(
        model="gpt-5.4-mini",
        messages=[{"role": "user", "content": "hi"}],
        max_completion_tokens=50,
        stream=True,
        stream_options={"include_usage": True},
    )


@pytest.mark.asyncio
async def test_openai_stream_bad_request():
    provider = OpenAIProvider(api_key="fake-key")
    http_resp = Response(400, request=Request("POST", "https://api.openai.com"))
    exc = BadRequestError("Invalid model parameter", response=http_resp, body=None)

    async def fake_stream_iter():
        raise exc
        yield None

    create_mock = AsyncMock(return_value=fake_stream_iter())
    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create_mock))
    )

    events = [e async for e in provider.stream("gpt-5.4-mini", [], max_tokens=50)]
    assert len(events) == 1
    assert events[0].type == "error"
    assert "invalid_request" in events[0].content
