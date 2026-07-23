import asyncio
from app.infrastructure.providers.groq import GroqProvider
from app.infrastructure.providers.openai import OpenAIProvider
from app.infrastructure.providers.mock import MockProvider
from app.application.ports.provider_client import ProviderClient
from app.core.config import settings


async def main():
    groq = GroqProvider(api_key=settings.groq_api_key)
    openai = OpenAIProvider(api_key=settings.openai_api_key)
    mock = MockProvider(mode="success")

    for name, provider, model in [
        ("groq", groq, "openai/gpt-oss-20b"),
        ("openai", openai, "gpt-5.4-mini"),
        ("mock", mock, "mock-model"),
    ]:
        assert isinstance(provider, ProviderClient), (
            f"{name} does not structurally satisfy ProviderClient"
        )
        result = await provider.complete(
            model, [{"role": "user", "content": "Say hi in 3 words."}]
        )
        print(name, result)
        assert result.provider and result.content and result.input_tokens >= 0

    stream_mock = MockProvider(mode="stream_delta")
    events = [e async for e in stream_mock.stream("mock-model", [])]
    assert events[0].type == "delta"
    assert events[-1].type == "done"
    print("stream events:", events)

    for mode in ("timeout", "error", "empty"):
        failing = MockProvider(mode=mode)
        try:
            await failing.complete("mock-model", [])
            raise AssertionError(f"{mode} should have raised")
        except Exception as e:
            print(f"{mode} correctly raised: {e}")

    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
