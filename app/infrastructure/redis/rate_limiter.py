import time
from redis.exceptions import RedisError
from app.application.ports.rate_limiter import (
    RateLimitBackendUnavailable,
    RateLimitExceeded,
)
from app.core.config import settings
from app.core.logging import logger
from app.infrastructure.redis.client import get_redis

RATE_LIMIT_LUA = """
local tenant_key = KEYS[1]
local api_key_key = KEYS[2]
local nonce_key = KEYS[3]

local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local tenant_limit = tonumber(ARGV[3])
local api_key_limit = tonumber(ARGV[4])
local ttl_seconds = tonumber(ARGV[5])

redis.call("ZREMRANGEBYSCORE", tenant_key, 0, now_ms - window_ms)
redis.call("ZREMRANGEBYSCORE", api_key_key, 0, now_ms - window_ms)

local tenant_count = redis.call("ZCARD", tenant_key)
local api_key_count = redis.call("ZCARD", api_key_key)

local function retry_after_seconds(key)
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    if #oldest == 0 then
        return 1
    end

    local remaining_ms = tonumber(oldest[2]) + window_ms - now_ms
    return math.max(1, math.ceil(remaining_ms / 1000))
end

if tenant_count >= tenant_limit or api_key_count >= api_key_limit then
    local retry_after = 1
    if tenant_count >= tenant_limit then
        retry_after = math.max(retry_after, retry_after_seconds(tenant_key))
    end
    if api_key_count >= api_key_limit then
        retry_after = math.max(retry_after, retry_after_seconds(api_key_key))
    end
    return {0, retry_after}
end

local nonce = redis.call("INCR", nonce_key)
local member = tostring(now_ms) .. ":" .. tostring(nonce)

redis.call("ZADD", tenant_key, now_ms, member)
redis.call("ZADD", api_key_key, now_ms, member)
redis.call("EXPIRE", tenant_key, ttl_seconds)
redis.call("EXPIRE", api_key_key, ttl_seconds)
redis.call("EXPIRE", nonce_key, ttl_seconds)

return {1, 0}
"""


class RedisRateLimiter:
    def __init__(
        self,
        *,
        window_seconds: int | None = None,
        tenant_limit: int | None = None,
        api_key_limit: int | None = None,
        failure_mode: str | None = None,
    ) -> None:
        self._redis = get_redis()
        self._script = self._redis.register_script(RATE_LIMIT_LUA)
        self._window_seconds = (
            window_seconds
            if window_seconds is not None
            else settings.rate_limit_window_seconds
        )
        self._tenant_limit = (
            tenant_limit
            if tenant_limit is not None
            else settings.rate_limit_tenant_requests
        )
        self._api_key_limit = (
            api_key_limit
            if api_key_limit is not None
            else settings.rate_limit_api_key_requests
        )
        self._failure_mode = (
            failure_mode
            if failure_mode is not None
            else settings.rate_limit_redis_failure_mode
        )

        if self._window_seconds <= 0:
            raise ValueError("rate-limit window must be positive")
        if self._tenant_limit <= 0 or self._api_key_limit <= 0:
            raise ValueError("rate limits must be positive")

    @staticmethod
    def _tenant_key(tenant_id: int) -> str:
        return f"rate_limit:tenant:{tenant_id}"

    @staticmethod
    def _api_key_key(api_key_id: int) -> str:
        return f"rate_limit:api_key:{api_key_id}"

    async def check(self, tenant_id: int, api_key_id: int) -> None:
        now_ms = int(time.time() * 1000)
        ttl_seconds = self._window_seconds + 1

        try:
            result = await self._script(
                keys=[
                    self._tenant_key(tenant_id),
                    self._api_key_key(api_key_id),
                    "rate_limit:nonce",
                ],
                args=[
                    now_ms,
                    self._window_seconds * 1000,
                    self._tenant_limit,
                    self._api_key_limit,
                    ttl_seconds,
                ],
            )
        except RedisError as exc:
            if self._failure_mode == "fail_open":
                logger.warning(
                    "rate_limiter_unavailable",
                    tenant_id=tenant_id,
                    api_key_id=api_key_id,
                    policy="fail_open",
                    error_type=type(exc).__name__,
                )
                return

            logger.error(
                "rate_limiter_unavailable",
                tenant_id=tenant_id,
                api_key_id=api_key_id,
                policy="fail_closed",
                error_type=type(exc).__name__,
            )
            raise RateLimitBackendUnavailable() from exc

        allowed = int(result[0])
        retry_after_seconds = int(result[1])
        if allowed == 0:
            raise RateLimitExceeded(retry_after_seconds)
