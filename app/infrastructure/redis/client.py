import redis.asyncio as redis
from app.core.config import settings

_pool = redis.ConnectionPool.from_url(
    settings.redis_url, max_connections=100, decode_responses=True
)


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)


async def close_redis() -> None:
    """Disconnect the shared async Redis connection pool during shutdown."""
    await _pool.disconnect(inuse_connections=True)
