"""Create or rotate a production GatewayLLM API key safely.

The raw API key is returned to the operator exactly once. Only its prefix and
SHA-256 digest are persisted.
"""

import argparse
import asyncio
import hashlib
import secrets
import sys
from collections.abc import Sequence
from decimal import Decimal, InvalidOperation

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError

from app.infrastructure.db.models import ApiKey, BudgetAccount, Tenant
from app.infrastructure.db.session import AsyncSessionLocal

MIN_MONTHLY_LIMIT_USD = Decimal("0.0001")
MAX_MONTHLY_LIMIT_USD = Decimal("10000")
MAX_TENANT_NAME_LENGTH = 200


def validate_monthly_limit(value: Decimal) -> Decimal:
    """Validate an operator-supplied monthly limit before opening a DB session."""
    if not value.is_finite():
        raise ValueError("monthly limit must be finite")
    if value < MIN_MONTHLY_LIMIT_USD:
        raise ValueError("monthly limit must be at least $0.0001")
    if value > MAX_MONTHLY_LIMIT_USD:
        raise ValueError("monthly limit exceeds safety bound ($10,000)")
    exponent = value.as_tuple().exponent
    if not isinstance(exponent, int) or exponent < -4:
        raise ValueError("monthly limit supports at most four decimal places")
    return value


def parse_monthly_limit(raw_value: str) -> Decimal:
    """Parse and validate a CLI decimal without floating-point conversion."""
    try:
        value = Decimal(raw_value)
    except InvalidOperation as exc:
        raise ValueError("monthly limit must be a valid decimal amount") from exc
    return validate_monthly_limit(value)


def normalize_tenant_name(tenant_name: str) -> str:
    """Apply the canonical operator-facing tenant-name normalization."""
    normalized = tenant_name.strip()
    if not normalized:
        raise ValueError("tenant name must not be empty")
    if len(normalized) > MAX_TENANT_NAME_LENGTH:
        raise ValueError("tenant name is too long")
    return normalized


async def create_api_key(
    tenant_name: str,
    monthly_limit_usd: Decimal,
    *,
    rotate: bool = False,
    expected_active_prefix: str | None = None,
) -> str:
    """Create a tenant key or atomically rotate its one active key."""
    normalized_name = normalize_tenant_name(tenant_name)
    validated_limit = validate_monthly_limit(monthly_limit_usd)

    if rotate and expected_active_prefix is None:
        raise ValueError("--expected-active-prefix is required with --rotate")
    if not rotate and expected_active_prefix is not None:
        raise ValueError("--expected-active-prefix is valid only with --rotate")

    async with AsyncSessionLocal() as session:
        async with session.begin():
            # The advisory lock protects the no-row-yet case that SELECT FOR
            # UPDATE cannot serialize.
            lock_acquired = await session.scalar(
                select(
                    func.pg_try_advisory_xact_lock(
                        func.hashtextextended(normalized_name, 0)
                    )
                )
            )
            if lock_acquired is not True:
                raise RuntimeError(
                    "another key operation is already in progress for tenant"
                )

            tenants = list(
                await session.scalars(
                    select(Tenant)
                    .where(Tenant.name == normalized_name)
                    .with_for_update()
                )
            )
            if len(tenants) > 1:
                raise RuntimeError("duplicate tenant name requires operator repair")

            if tenants:
                tenant = tenants[0]
            else:
                tenant = Tenant(name=normalized_name)
                session.add(tenant)
                await session.flush()

            budget_account = await session.scalar(
                select(BudgetAccount)
                .where(BudgetAccount.tenant_id == tenant.id)
                .with_for_update()
            )
            if budget_account is None:
                session.add(
                    BudgetAccount(
                        tenant_id=tenant.id,
                        monthly_limit_usd=validated_limit,
                    )
                )
            elif budget_account.monthly_limit_usd != validated_limit:
                raise RuntimeError(
                    "existing budget differs; use a separate audited budget "
                    "update operation"
                )

            active_keys = list(
                await session.scalars(
                    select(ApiKey)
                    .where(
                        ApiKey.tenant_id == tenant.id,
                        ApiKey.status == "active",
                    )
                    .order_by(ApiKey.id)
                    .with_for_update()
                )
            )

            if active_keys and not rotate:
                raise RuntimeError(
                    f"Tenant '{normalized_name}' already has {len(active_keys)} "
                    "active key(s). Use --rotate to create a new key and revoke "
                    "the old key."
                )

            if rotate:
                if len(active_keys) != 1:
                    raise RuntimeError(
                        "rotation requires exactly one active key; "
                        "operator repair is required"
                    )
                active_key = active_keys[0]
                if active_key.prefix != expected_active_prefix:
                    raise RuntimeError(
                        "active key changed; inspect current state and retry"
                    )
                active_key.status = "revoked"

            raw_key = f"sk-gw-{secrets.token_hex(24)}"
            session.add(
                ApiKey(
                    tenant_id=tenant.id,
                    prefix=raw_key[:12],
                    key_hash=hashlib.sha256(raw_key.encode()).hexdigest(),
                    status="active",
                )
            )

    return raw_key


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create or rotate a GatewayLLM API key"
    )
    parser.add_argument("--tenant-name", required=True)
    parser.add_argument(
        "--monthly-limit-usd",
        required=True,
        help="Monthly budget limit in USD (for example, 5.00)",
    )
    parser.add_argument(
        "--rotate",
        action="store_true",
        help="Revoke the current active key and create a new one",
    )
    parser.add_argument(
        "--expected-active-prefix",
        help=(
            "Required with --rotate; optimistic concurrency check against "
            "the current active key"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    try:
        limit = parse_monthly_limit(args.monthly_limit_usd)
        raw_key = asyncio.run(
            create_api_key(
                args.tenant_name,
                limit,
                rotate=args.rotate,
                expected_active_prefix=args.expected_active_prefix,
            )
        )
    except (ValueError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except SQLAlchemyError as exc:
        print("ERROR: database operation failed", file=sys.stderr)
        raise SystemExit(1) from exc

    print("\n" + "=" * 50)
    print("API Key (save it now; it will not be shown again):")
    print(f"  {raw_key}")
    print("=" * 50 + "\n")


if __name__ == "__main__":
    main()
