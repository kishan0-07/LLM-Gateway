import time
from app.infrastructure.redis.client import get_redis
from app.core.logging import logger


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self._redis = get_redis()
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout

    def _key(self, provider: str, model: str, suffix: str) -> str:
        return f"circuit:{provider}:{model}:{suffix}"

    async def is_available(self, provider: str, model: str) -> bool:
        try:
            state = await self._redis.get(self._key(provider, model, "state"))
        except Exception:
            logger.warning("circuit_state_unavailable", provider=provider, model=model)
            return True  # fail open — Decision 7 explicit policy for local/demo

        if state is None or state == "closed":
            return True

        if state == "half_open":
            return True  # allow the trial request through

        if state == "open":
            opened_at = await self._redis.get(self._key(provider, model, "opened_at"))
            if opened_at and (time.time() - float(opened_at)) >= self._recovery_timeout:
                # recovery timeout expired → transition to half_open
                await self._redis.set(self._key(provider, model, "state"), "half_open")
                logger.info("circuit_half_open", provider=provider, model=model)
                return True
            return False

        return True  # unknown state → fail open

    async def record_success(self, provider: str, model: str) -> None:
        pipe = self._redis.pipeline()
        pipe.set(self._key(provider, model, "state"), "closed")
        pipe.set(self._key(provider, model, "failures"), 0)
        pipe.delete(self._key(provider, model, "opened_at"))
        await pipe.execute()

    async def record_failure(self, provider: str, model: str) -> None:
        state = await self._redis.get(self._key(provider, model, "state"))

        # If in half_open, the trial request failed — immediately re-open
        if state == "half_open":
            await self._redis.set(self._key(provider, model, "state"), "open")
            await self._redis.set(self._key(provider, model, "opened_at"), str(time.time()))
            logger.warning("circuit_reopened_from_half_open", provider=provider, model=model)
            return

        # Normal CLOSED path - count consecutive failures
        failures_key = self._key(provider, model, "failures")
        count = await self._redis.incr(failures_key)
        await self._redis.expire(failures_key, self._recovery_timeout * 3)

        if count >= self._failure_threshold:
            await self._redis.set(self._key(provider, model, "state"), "open")
            await self._redis.set(self._key(provider, model, "opened_at"), str(time.time()))
            logger.warning("circuit_opened", provider=provider, model=model, failures=count)