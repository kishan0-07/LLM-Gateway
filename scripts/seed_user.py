import asyncio, hashlib, secrets
from app.infrastructure.db.session import AsyncSessionLocal
from app.infrastructure.db.models import Tenant, ApiKey, BudgetAccount


async def seed():
    raw_key = f"sk-test-{secrets.token_hex(16)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()

    async with AsyncSessionLocal() as session:
        tenant = Tenant(name="dev-tenant")
        session.add(tenant)
        await session.flush() 

        session.add(ApiKey(tenant_id=tenant.id, prefix=raw_key[:12], key_hash=key_hash, status="active"))
        session.add(BudgetAccount(tenant_id=tenant.id, monthly_limit_usd=10))
        await session.commit()

    print(f"Raw API key (save it, never shown again): {raw_key}")


if __name__ == "__main__":
    asyncio.run(seed())