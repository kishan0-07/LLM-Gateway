from typing import AsyncIterator, Protocol, runtime_checkable
from app.domain.provider import ProviderResult, ProviderStreamEvent


@runtime_checkable
class ProviderClient(Protocol):
    async def complete(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> ProviderResult: ...

    def stream(
        self, model: str, messages: list[dict], *, max_tokens: int
    ) -> AsyncIterator[ProviderStreamEvent]: ...
