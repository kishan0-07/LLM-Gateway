from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class ProviderResult:
    provider: str
    model: str
    content: str
    input_tokens: int
    output_tokens: int
    usage_source: Literal["actual", "estimated"]
    latency_ms: int


@dataclass(frozen=True)
class ProviderStreamEvent:
    type: Literal["delta", "usage", "error", "done"]
    content: str | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None


@dataclass
class ProviderError(Exception):
    provider: str
    category: Literal["timeout", "rate_limited", "server_error", "invalid_request", "empty_output"]
    message: str
    retryable: bool

    def __str__(self) -> str:
        return f"[{self.provider}] {self.category}: {self.message}"