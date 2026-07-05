from dataclasses import dataclass
from app.domain.provider import ProviderError


@dataclass(frozen=True)
class ProviderMetadata:
    name: str
    models: list[str]
    supports_streaming_usage: bool
    tokenizer_hint: str
    pricing: dict[str, dict[str, float]]


class BaseProvider:
    metadata: ProviderMetadata

    def _wrap_error(self, category: str, message: str, retryable: bool) -> ProviderError:
        return ProviderError(provider=self.metadata.name, category=category, message=message, retryable=retryable)