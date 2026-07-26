import time

from app.core.logging import logger
from app.infrastructure.redis.client import get_redis


class CircuitBreaker:
    """Best-effort provider health state; it never owns request correctness."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self._redis = get_redis()
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout

    def _key(self, provider: str, model: str, suffix: str) -> str:
        return f"circuit:{provider}:{model}:{suffix}"

    async def _acquire_half_open_lease(self, provider: str, model: str) -> bool:
        acquired = await self._redis.set(
            self._key(provider, model, "half_open_lease"),
            "1",
            nx=True,
            ex=self._recovery_timeout,
        )
        return bool(acquired)

    async def is_available(self, provider: str, model: str) -> bool:
        try:
            state = await self._redis.get(self._key(provider, model, "state"))
            if state is None or state == "closed":
                return True

            if state == "half_open":
                return await self._acquire_half_open_lease(provider, model)

            if state == "open":
                opened_at = await self._redis.get(
                    self._key(provider, model, "opened_at")
                )
                if not opened_at:
                    return False
                if (time.time() - float(opened_at)) < self._recovery_timeout:
                    return False
                if not await self._acquire_half_open_lease(provider, model):
                    return False
                await self._redis.set(
                    self._key(provider, model, "state"),
                    "half_open",
                )
                logger.info(
                    "circuit_half_open",
                    provider=provider,
                    model=model,
                )
                return True

            return True
        except Exception as exc:
            logger.warning(
                "circuit_state_unavailable",
                provider=provider,
                model=model,
                error_type=type(exc).__name__,
            )
            return True

    async def record_success(self, provider: str, model: str) -> None:
        try:
            pipe = self._redis.pipeline()
            pipe.set(self._key(provider, model, "state"), "closed")
            pipe.delete(self._key(provider, model, "failures"))
            pipe.delete(self._key(provider, model, "opened_at"))
            pipe.delete(self._key(provider, model, "half_open_lease"))
            await pipe.execute()
        except Exception as exc:
            logger.warning(
                "circuit_success_update_failed",
                provider=provider,
                model=model,
                error_type=type(exc).__name__,
            )

    async def record_failure(self, provider: str, model: str) -> None:
        try:
            state = await self._redis.get(self._key(provider, model, "state"))

            if state == "half_open":
                pipe = self._redis.pipeline()
                pipe.set(self._key(provider, model, "state"), "open")
                pipe.set(
                    self._key(provider, model, "opened_at"),
                    str(time.time()),
                )
                pipe.delete(self._key(provider, model, "half_open_lease"))
                await pipe.execute()
                logger.warning(
                    "circuit_reopened_from_half_open",
                    provider=provider,
                    model=model,
                )
                return

            failures_key = self._key(provider, model, "failures")
            count = await self._redis.incr(failures_key)
            await self._redis.expire(
                failures_key,
                self._recovery_timeout * 3,
            )
            if count >= self._failure_threshold:
                pipe = self._redis.pipeline()
                pipe.set(self._key(provider, model, "state"), "open")
                pipe.set(
                    self._key(provider, model, "opened_at"),
                    str(time.time()),
                )
                pipe.delete(self._key(provider, model, "half_open_lease"))
                await pipe.execute()
                logger.warning(
                    "circuit_opened",
                    provider=provider,
                    model=model,
                    failures=count,
                )
        except Exception as exc:
            logger.warning(
                "circuit_failure_update_failed",
                provider=provider,
                model=model,
                error_type=type(exc).__name__,
            )
