from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from redis.exceptions import RedisError

from app.infrastructure.db.session import engine
from app.infrastructure.redis.client import get_redis
from app.core.logging import logger

router = APIRouter()


@router.get("/health")
async def health():
    return {"status": "ok"}


@router.get("/ready")
async def ready() -> dict[str, str]:
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
        await get_redis().ping()
    except (SQLAlchemyError, RedisError, OSError, ConnectionError) as exc:
        logger.warning(
            "readiness_dependency_unavailable", error_type=type(exc).__name__
        )
        raise HTTPException(
            status_code=503,
            detail={
                "code": "dependency_unavailable",
                "message": "Gateway dependencies unavailable",
            },
        ) from exc
    return {"status": "ready"}
