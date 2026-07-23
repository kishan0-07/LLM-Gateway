import asyncio
from app.infrastructure.redis.circuit_breaker import CircuitBreaker
from app.infrastructure.redis.client import get_redis


async def main():
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=5)
    provider, model = "test-provider", "test-model"

    redis = get_redis()
    for suffix in ("failures", "opened_at", "state"):
        await redis.delete(f"circuit:{provider}:{model}:{suffix}")

    assert await cb.is_available(provider, model), "fresh circuit should be CLOSED"
    print(" Fresh circuit is CLOSED (available)")

    await cb.record_failure(provider, model)
    await cb.record_failure(provider, model)
    assert await cb.is_available(provider, model), (
        "2 failures < threshold=3, still CLOSED"
    )
    print(" 2 failures, still CLOSED")

    await cb.record_failure(provider, model)
    assert not await cb.is_available(provider, model), (
        "3 failures = threshold, should be OPEN"
    )
    print(" 3 failures, circuit OPEN")

    print("   Waiting 6 seconds for recovery timeout...")
    await asyncio.sleep(6)
    assert await cb.is_available(provider, model), (
        "after recovery_timeout, should be HALF_OPEN (available)"
    )
    state = await redis.get(f"circuit:{provider}:{model}:state")
    assert state == "half_open", f"expected half_open, got {state}"
    print(" Recovery timeout expired, circuit is HALF_OPEN")

    await cb.record_success(provider, model)
    assert await cb.is_available(provider, model), "success should reset to CLOSED"
    failures = await redis.get(f"circuit:{provider}:{model}:failures")
    assert failures == "0" or failures is None, (
        f"failures should be reset, got {failures}"
    )
    print(" Success resets circuit to CLOSED")

    for _ in range(3):
        await cb.record_failure(provider, model)
    assert not await cb.is_available(provider, model), "should be OPEN again"
    await asyncio.sleep(6)
    assert await cb.is_available(provider, model), "should be HALF_OPEN"
    await cb.record_failure(provider, model)
    assert not await cb.is_available(provider, model), (
        "failure in HALF_OPEN re-opens circuit"
    )
    print(" Failure in HALF_OPEN re-opens circuit")

    print("\nALL CIRCUIT BREAKER TESTS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
