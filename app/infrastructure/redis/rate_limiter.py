from app.core.logging import logger

class PermissiveRateLimiter:
    async def check(self, tenant_id: int, api_key_id: int) -> None:
        logger.debug("rate_limiter_check", tenant_id=tenant_id, api_key_id=api_key_id, enforced=False)