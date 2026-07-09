from typing import Protocol


class RateLimiter(Protocol):
    async def check(self, tenant_id: int, api_key_id: int) -> None: ...