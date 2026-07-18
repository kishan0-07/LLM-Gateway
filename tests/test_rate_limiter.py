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

@pytest.mark.integration
@pytest.mark.asyncio
async def test_tenant_limit_is_shared_across_different_api_keys(test_env):
    from app.infrastructure.redis.client import get_redis
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.application.ports.rate_limiter import RateLimitExceeded

    redis_client = get_redis()
    tenant_id = test_env["tenant_id"]
    key_one = test_env["api_key_id"]
    key_two = key_one + 1

    for key in (
        f"rate_limit:tenant:{tenant_id}",
        f"rate_limit:api_key:{key_one}",
        f"rate_limit:api_key:{key_two}",
    ):
        await redis_client.delete(key)

    limiter = RedisRateLimiter(
        window_seconds=60,
        tenant_limit=2,
        api_key_limit=10,
    )
    await limiter.check(tenant_id, key_one)
    await limiter.check(tenant_id, key_two)

    with pytest.raises(RateLimitExceeded):
        await limiter.check(tenant_id, key_two)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_key_limit_does_not_throttle_sibling_key(test_env):
    from app.infrastructure.redis.client import get_redis
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.application.ports.rate_limiter import RateLimitExceeded

    redis_client = get_redis()
    tenant_id = test_env["tenant_id"]
    key_one = test_env["api_key_id"]
    key_two = key_one + 1

    for key in (
        f"rate_limit:tenant:{tenant_id}",
        f"rate_limit:api_key:{key_one}",
        f"rate_limit:api_key:{key_two}",
    ):
        await redis_client.delete(key)

    limiter = RedisRateLimiter(
        window_seconds=60,
        tenant_limit=10,
        api_key_limit=1,
    )
    
    # key_one consumes its limit and gets throttled
    await limiter.check(tenant_id, key_one)
    with pytest.raises(RateLimitExceeded):
        await limiter.check(tenant_id, key_one)
        
    # key_two belongs to a sibling api key and is unthrottled
    await limiter.check(tenant_id, key_two)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_failure_behavior_respects_configuration(test_env):
    from unittest.mock import AsyncMock
    from redis.exceptions import RedisError
    from app.infrastructure.redis.rate_limiter import RedisRateLimiter
    from app.application.ports.rate_limiter import RateLimitBackendUnavailable

    # Case A: Fail closed
    limiter_closed = RedisRateLimiter(failure_mode="fail_closed")
    limiter_closed._script = AsyncMock(side_effect=RedisError("Redis is down"))
    with pytest.raises(RateLimitBackendUnavailable):
        await limiter_closed.check(test_env["tenant_id"], test_env["api_key_id"])

    # Case B: Fail open
    limiter_open = RedisRateLimiter(failure_mode="fail_open")
    limiter_open._script = AsyncMock(side_effect=RedisError("Redis is down"))
    # Should complete without raising an error
    await limiter_open.check(test_env["tenant_id"], test_env["api_key_id"])