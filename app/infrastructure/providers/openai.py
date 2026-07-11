import time
from typing import AsyncIterator
from openai import AsyncOpenAI
from openai import APITimeoutError, RateLimitError, APIConnectionError, APIStatusError
from app.domain.provider import ProviderResult , ProviderStreamEvent
from app.infrastructure.providers.base import BaseProvider, ProviderMetadata

METADATA = ProviderMetadata(
    name="openai",
    models=["gpt-5.4-mini"],
    supports_streaming_usage=True,
    tokenizer_hint="o200k_base",
    pricing={
        "gpt-5.4-mini": {"input_per_1m": 0.75, "output_per_1m": 4.50},  
    },
)


class OpenAIProvider(BaseProvider):
    metadata = METADATA

    def __init__(self, api_key: str):
        self._client = AsyncOpenAI(api_key=api_key)

    async def complete(self, model: str, messages: list[dict]) -> ProviderResult:
        start = time.perf_counter()
        try:
            response = await self._client.chat.completions.create(model=model, messages=messages)
        except APITimeoutError as exc:
            raise self._wrap_error("timeout", str(exc), retryable=True) from exc
        except RateLimitError as exc:
            raise self._wrap_error("rate_limited", str(exc), retryable=True) from exc
        except APIConnectionError as exc:
            raise self._wrap_error("timeout", str(exc), retryable=True) from exc
        except APIStatusError as exc:
            raise self._wrap_error("server_error", str(exc), retryable=exc.status_code >= 500) from exc

        latency_ms = int((time.perf_counter() - start) * 1000)
        choice = response.choices[0]
        if not choice.message.content:
            raise self._wrap_error("empty_output", "provider returned empty content", retryable=False)

        return ProviderResult(
            provider="openai",
            model=model,
            content=choice.message.content,
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            usage_source="actual",
            latency_ms=latency_ms,
        )

    async def stream(self, model: str, messages: list[dict]) -> AsyncIterator[ProviderStreamEvent]:
        try:
            stream = await self._client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True,
                stream_options={"include_usage": True},
            )
            async for chunk in stream:
                if chunk.usage:
                    yield ProviderStreamEvent(
                        type="usage",
                        input_tokens=chunk.usage.prompt_tokens,
                        output_tokens=chunk.usage.completion_tokens,
                    )
                    continue

                if chunk.choices and chunk.choices[0].delta.content:
                    yield ProviderStreamEvent(
                        type="delta",
                        content=chunk.choices[0].delta.content,
                    )

            yield ProviderStreamEvent(type="done")

        except APITimeoutError as exc:
            yield ProviderStreamEvent(type="error", content=f"timeout: {exc}")
        except RateLimitError as exc:
            yield ProviderStreamEvent(type="error", content=f"rate_limited: {exc}")
        except APIConnectionError as exc:
            yield ProviderStreamEvent(type="error", content=f"timeout: {exc}")
        except APIStatusError as exc:
            yield ProviderStreamEvent(type="error", content=f"server_error: {exc}")