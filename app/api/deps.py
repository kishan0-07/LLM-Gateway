import hashlib
from fastapi import Header, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from app.infrastructure.db.session import get_db
from app.infrastructure.db.models import ApiKey
from app.domain.auth import Principal


async def get_principal(x_api_key: str | None = Header(None, alias="X-API-Key"),db: AsyncSession = Depends(get_db),) -> Principal:
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="Missing API key")

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    result = await db.execute(
        select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.status == "active")
    )
    api_key = result.scalar_one_or_none()
    if api_key is None:
        raise HTTPException(status_code=401, detail="Invalid API key")

    return Principal(tenant_id=api_key.tenant_id, api_key_id=api_key.id)