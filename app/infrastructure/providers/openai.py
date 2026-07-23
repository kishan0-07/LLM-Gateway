import time
from typing import AsyncIterator, Any, cast
from openai import AsyncOpenAI
from openai import APITimeoutError, RateLimitError, APIConnectionError, APIStatusError
from app.domain.provider import ProviderResult, ProviderStreamEvent
from app.infrastructure.providers.base import (
    BaseProvider,
    ProviderMetadata,
    ProviderError,
)

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

    def _status_error(self, exc: APIStatusError) -> ProviderError:
        if exc.status_code in {400, 404, 422}:
            return self._wrap_error("invalid_request", str(exc), retryable=False)
        return self._wrap_error(
            "server_error", str(exc), retryable=exc.status_code >= 500
        )

    async def complete(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> ProviderResult:
        start = time.perf_counter()
        try:
            response = await self._client.chat.completions.create(
                model=model,
                messages=cast(Any, messages),
                max_completion_tokens=max_tokens,
            )
        except APITimeoutError as exc:
            raise self._wrap_error("timeout", str(exc), retryable=True) from exc
        except RateLimitError as exc:
            raise self._wrap_error("rate_limited", str(exc), retryable=True) from exc
        except APIConnectionError as exc:
            raise self._wrap_error("timeout", str(exc), retryable=True) from exc
        except APIStatusError as exc:
            raise self._status_error(exc) from exc

        latency_ms = int((time.perf_counter() - start) * 1000)
        choice = response.choices[0]
        if not choice.message.content:
            raise self._wrap_error(
                "empty_output", "provider returned empty content", retryable=False
            )

        usage = response.usage
        if usage is None:
            return ProviderResult(
                provider="openai",
                model=model,
                content=choice.message.content,
                input_tokens=0,
                output_tokens=0,
                usage_source="estimated",
                latency_ms=latency_ms,
            )

        return ProviderResult(
            provider="openai",
            model=model,
            content=choice.message.content,
            input_tokens=usage.prompt_tokens,
            output_tokens=usage.completion_tokens,
            usage_source="actual",
            latency_ms=latency_ms,
        )

    async def stream(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> AsyncIterator[ProviderStreamEvent]:
        try:
            response_stream = cast(
                Any,
                await self._client.chat.completions.create(
                    model=model,
                    messages=cast(Any, messages),
                    max_completion_tokens=max_tokens,
                    stream=True,
                    stream_options={"include_usage": True},
                ),
            )
            async for chunk in response_stream:
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

        except APITimeoutError:
            yield ProviderStreamEvent(type="error", content="timeout")
        except RateLimitError:
            yield ProviderStreamEvent(type="error", content="rate_limited")
        except APIConnectionError:
            yield ProviderStreamEvent(type="error", content="timeout")
        except APIStatusError as exc:
            category = (
                "invalid_request"
                if exc.status_code in {400, 404, 422}
                else "server_error"
            )
            yield ProviderStreamEvent(type="error", content=category)
