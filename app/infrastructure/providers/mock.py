import asyncio
from typing import AsyncIterator, Literal
from app.domain.provider import ProviderResult, ProviderStreamEvent
from app.infrastructure.providers.base import BaseProvider, ProviderMetadata

METADATA = ProviderMetadata(
    name="mock",
    models=["mock-model"],
    supports_streaming_usage=True,
    tokenizer_hint="mock",
    pricing={"mock-model": {"input_per_1m": 0.0, "output_per_1m": 0.0}},
)

Mode = Literal["success", "timeout", "error", "empty", "stream_delta", "stream_error"]


class MockProvider(BaseProvider):
    metadata = METADATA

    def __init__(self, mode: Mode = "success"):
        self.mode = mode

    async def complete(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> ProviderResult:
        if self.mode == "timeout":
            raise self._wrap_error("timeout", "mock forced timeout", retryable=True)
        if self.mode == "error":
            raise self._wrap_error("server_error", "mock forced error", retryable=True)
        if self.mode == "empty":
            raise self._wrap_error(
                "empty_output", "mock forced empty output", retryable=False
            )

        return ProviderResult(
            provider="mock",
            model=model,
            content="mock response content",
            input_tokens=10,
            output_tokens=5,
            usage_source="actual",
            latency_ms=1,
        )

    async def stream(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> AsyncIterator[ProviderStreamEvent]:
        if self.mode == "stream_error":
            yield ProviderStreamEvent(type="delta", content="partial ")
            yield ProviderStreamEvent(
                type="error", content="timeout: mock forced timeout"
            )
            return
        if self.mode == "stream_delta":
            for chunk in ["mock ", "stream ", "content"]:
                await asyncio.sleep(0)
                yield ProviderStreamEvent(type="delta", content=chunk)
            yield ProviderStreamEvent(type="usage", input_tokens=10, output_tokens=5)
            yield ProviderStreamEvent(type="done")
            return
        raise self._wrap_error(
            "invalid_request", "unsupported mock stream mode", retryable=False
        )
