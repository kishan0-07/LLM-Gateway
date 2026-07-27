import argparse
import asyncio
import hashlib
import json
import secrets
import uuid
from decimal import Decimal

from sqlalchemy import delete, select

from app.infrastructure.db.models import (
    ApiKey,
    BudgetAccount,
    BudgetPeriod,
    BudgetReservation,
    GatewayRequest,
    ProviderAttempt,
    Tenant,
    UsageLedger,
)
from app.infrastructure.db.session import AsyncSessionLocal


async def seed() -> None:
    suffix = uuid.uuid4().hex
    raw_key = f"sk-day20-{secrets.token_hex(24)}"

    async with AsyncSessionLocal() as session:
        tenant = Tenant(name=f"day20-chaos-{suffix}")
        session.add(tenant)
        await session.flush()

        key = ApiKey(
            tenant_id=tenant.id,
            prefix=raw_key[:12],
            key_hash=hashlib.sha256(raw_key.encode()).hexdigest(),
            status="active",
        )
        session.add(key)
        session.add(
            BudgetAccount(
                tenant_id=tenant.id,
                monthly_limit_usd=Decimal("5.0000"),
            )
        )
        await session.commit()

    print(
        json.dumps(
            {
                "tenant_id": tenant.id,
                "api_key_id": key.id,
                "api_key": raw_key,
            }
        )
    )


async def cleanup(tenant_id: int) -> None:
    async with AsyncSessionLocal() as session:
        request_ids = list(
            await session.scalars(
                select(GatewayRequest.id).where(GatewayRequest.tenant_id == tenant_id)
            )
        )

        if request_ids:
            await session.execute(
                delete(UsageLedger).where(
                    UsageLedger.gateway_request_id.in_(request_ids)
                )
            )
            await session.execute(
                delete(BudgetReservation).where(
                    BudgetReservation.gateway_request_id.in_(request_ids)
                )
            )
            await session.execute(
                delete(ProviderAttempt).where(
                    ProviderAttempt.gateway_request_id.in_(request_ids)
                )
            )
            await session.execute(
                delete(GatewayRequest).where(GatewayRequest.id.in_(request_ids))
            )

        await session.execute(
            delete(BudgetPeriod).where(BudgetPeriod.tenant_id == tenant_id)
        )
        await session.execute(
            delete(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
        )
        await session.execute(delete(ApiKey).where(ApiKey.tenant_id == tenant_id))
        await session.execute(delete(Tenant).where(Tenant.id == tenant_id))
        await session.commit()

    print(json.dumps({"cleaned_tenant_id": tenant_id}))


async def main() -> None:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("seed")
    cleanup_parser = subcommands.add_parser("cleanup")
    cleanup_parser.add_argument("--tenant-id", type=int, required=True)
    args = parser.parse_args()

    if args.command == "seed":
        await seed()
    else:
        await cleanup(args.tenant_id)


if __name__ == "__main__":
    asyncio.run(main())
