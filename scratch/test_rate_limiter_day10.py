import asyncio
from app.infrastructure.redis.rate_limiter import RedisRateLimiter
from app.application.ports.rate_limiter import RateLimitExceeded, RateLimitBackendUnavailable
from app.infrastructure.redis.client import get_redis

async def clear_keys(tenant_id: int, api_key_id: int):
    r = get_redis()
    await r.delete(f"rate_limit:tenant:{tenant_id}")
    await r.delete(f"rate_limit:api_key:{api_key_id}")

async def test_concurrent_checks():
    tenant_id = 99
    api_key_id = 999
    await clear_keys(tenant_id, api_key_id)
    
    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=10, api_key_limit=5)
    
    async def run_check():
        try:
            await limiter.check(tenant_id, api_key_id)
            return True
        except RateLimitExceeded:
            return False

    results = await asyncio.gather(*(run_check() for _ in range(20)))
    allowed = sum(1 for r in results if r)
    denied = sum(1 for r in results if not r)
    
    print(f"Concurrent Test: Allowed={allowed}, Denied={denied}")
    assert allowed == 5
    assert denied == 15
    print("  [Pass] Concurrent rate limit isolation.")

async def test_redis_failure():
    tenant_id = 99
    api_key_id = 999
    
    # Mock limiter with dummy client to trigger connection error
    limiter = RedisRateLimiter(window_seconds=60, tenant_limit=5, api_key_limit=5, failure_mode="fail_closed")
    # Overwrite script target to simulate failure
    class FailedScript:
        async def __call__(self, *args, **kwargs):
            from redis.exceptions import ConnectionError
            raise ConnectionError("Mock connection failure")
    limiter._script = FailedScript()
    
    try:
        await limiter.check(tenant_id, api_key_id)
        raise AssertionError("Expected RateLimitBackendUnavailable was not thrown")
    except RateLimitBackendUnavailable:
        print("  [Pass] Fail-closed logic matches unavailable backend.")

async def main():
    print("=== Running RedisRateLimiter Tests ===")
    await test_concurrent_checks()
    await test_redis_failure()
    print("All rate limiter tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())