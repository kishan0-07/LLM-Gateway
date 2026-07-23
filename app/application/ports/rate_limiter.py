from typing import Protocol


class RateLimitExceeded(Exception):
    def __init__(self, retry_after_seconds: int) -> None:
        super().__init__("rate limit exceeded")
        self.retry_after_seconds = max(1, retry_after_seconds)


class RateLimitBackendUnavailable(Exception):
    """Raised only when the configured policy is fail_closed."""


class RateLimiter(Protocol):
    async def check(self, tenant_id: int, api_key_id: int) -> None: ...
