from dataclasses import dataclass
from abc import ABC, abstractmethod
from typing import AsyncIterator, Literal
from app.domain.provider import ProviderError , ProviderResult, ProviderStreamEvent

ProviderErrorCategory = Literal["timeout", "rate_limited", "server_error", "invalid_request", "empty_output"]

@dataclass(frozen=True)
class ProviderMetadata:
    name: str
    models: list[str]
    supports_streaming_usage: bool
    tokenizer_hint: str
    pricing: dict[str, dict[str, float]]


class BaseProvider(ABC):
    metadata: ProviderMetadata

    def _wrap_error(self, category: ProviderErrorCategory, message: str, retryable: bool) -> ProviderError:
        return ProviderError(provider=self.metadata.name, category=category, message=message, retryable=retryable)

    @abstractmethod
    async def complete(self, model: str, messages: list[dict], *, max_tokens: int) -> ProviderResult:
        ...

    @abstractmethod
    def stream(self, model: str, messages: list[dict], *, max_tokens: int) -> AsyncIterator[ProviderStreamEvent]:
        ...