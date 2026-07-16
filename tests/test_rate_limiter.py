import pytest
import asyncio
from app.infrastructure.redis.rate_limiter import RedisRateLimiter
from app.application.ports.rate_limiter import RateLimitExceeded
from app.infrastructure.redis.client import get_redis


@pytest.mark.integration
@pytest.mark.asyncio
async def test_allows_requests_within_limit(test_env):
    r = get_redis()
    await r.delete(f"rate_limit:tenant:{test_env['tenant_id']}")
    await r.delete(f"rate_limit:api_key:{test_env['api_key_id']}")

    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=5, api_key_limit=5)

    # 5 requests should all pass
    for _ in range(5):
        await limiter.check(test_env["tenant_id"], test_env["api_key_id"])


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rejects_requests_over_limit(test_env):
    """The 6th request must raise RateLimitExceeded when limit is 5."""
    r = get_redis()
    await r.delete(f"rate_limit:tenant:{test_env['tenant_id']}")
    await r.delete(f"rate_limit:api_key:{test_env['api_key_id']}")

    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=5, api_key_limit=5)

    for _ in range(5):
        await limiter.check(test_env["tenant_id"], test_env["api_key_id"])

    with pytest.raises(RateLimitExceeded) as exc_info:
        await limiter.check(test_env["tenant_id"], test_env["api_key_id"])

    assert exc_info.value.retry_after_seconds >= 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_requests_respect_limit(test_env):
    """100 concurrent requests against a limit of 10 must produce exactly 10 passes."""
    r = get_redis()
    await r.delete(f"rate_limit:tenant:{test_env['tenant_id']}")
    await r.delete(f"rate_limit:api_key:{test_env['api_key_id']}")

    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=10, api_key_limit=10)

    async def try_check():
        try:
            await limiter.check(test_env["tenant_id"], test_env["api_key_id"])
            return True
        except RateLimitExceeded:
            return False

    results = await asyncio.gather(*[try_check() for _ in range(100)])
    allowed = sum(1 for r in results if r)
    assert allowed == 10, f"Expected 10 allowed, got {allowed}"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retry_after_is_positive(test_env):
    """retry_after_seconds must be a positive integer."""
    r = get_redis()
    await r.delete(f"rate_limit:tenant:{test_env['tenant_id']}")
    await r.delete(f"rate_limit:api_key:{test_env['api_key_id']}")

    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=1, api_key_limit=1)
    await limiter.check(test_env["tenant_id"], test_env["api_key_id"])

    with pytest.raises(RateLimitExceeded) as exc_info:
        await limiter.check(test_env["tenant_id"], test_env["api_key_id"])

    assert exc_info.value.retry_after_seconds >= 1
    assert isinstance(exc_info.value.retry_after_seconds, int)