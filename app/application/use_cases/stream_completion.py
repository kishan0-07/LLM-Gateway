from typing import AsyncIterator
from app.domain.provider import ProviderStreamEvent
from app.application.services.budget_authorizer import BudgetAuthorizer

class StreamCompletion:
    """Real implementation Days 8-9. Finalizer must settle exactly once for
    success, timeout, exception, budget cutoff, and client disconnect (Decision 8)."""

    def __init__(self, budget_authorizer: BudgetAuthorizer, provider):
        self._budget_authorizer = budget_authorizer
        self._provider = provider

    async def stream(self, request) -> AsyncIterator[ProviderStreamEvent]:
        raise NotImplementedError("StreamCompletion lands Days 8-9")

    async def _finalize(self, reservation_id: str, outcome: str) -> None:
        raise NotImplementedError("finalizer lands Days 8-9")