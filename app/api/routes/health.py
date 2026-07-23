from fastapi import APIRouter
from app.core.logging import logger

router = APIRouter()


@router.get("/health")
async def health():
    logger.info("health_check_hit")
    return {"status": "ok"}
