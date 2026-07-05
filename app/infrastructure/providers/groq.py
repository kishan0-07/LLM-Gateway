import time
from groq import AsyncGroq
from groq import APITimeoutError, RateLimitError, APIConnectionError, APIStatusError
from app.domain.provider import ProviderResult
from app.infrastructure.providers.base import BaseProvider, ProviderMetadata

METADATA = ProviderMetadata(
    name="groq",
    models=["openai/gpt-oss-20b", "openai/gpt-oss-120b"],
    supports_streaming_usage=True,
    tokenizer_hint="o200k_base",
    pricing={
        "openai/gpt-oss-20b": {"input_per_1m": 0.075, "output_per_1m": 0.30},   
        "openai/gpt-oss-120b": {"input_per_1m": 0.15, "output_per_1m": 0.60},
    },
)


class GroqProvider(BaseProvider):
    metadata = METADATA

    def __init__(self, api_key: str):
        self._client = AsyncGroq(api_key=api_key)

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
            provider="groq",
            model=model,
            content=choice.message.content,
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            usage_source="actual",
            latency_ms=latency_ms,
        )

    def stream(self, model: str, messages: list[dict]):
        raise NotImplementedError("real streaming lands Days 8-9 — signature only today")