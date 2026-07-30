import asyncio
import hashlib
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import delete, select

import app.cli.create_api_key as cli_module
from app.cli.create_api_key import (
    create_api_key,
    main,
    parse_monthly_limit,
)
from app.infrastructure.db.models import ApiKey, BudgetAccount, Tenant
from app.infrastructure.db.session import AsyncSessionLocal


class DatabaseAccessForbidden:
    def __call__(self) -> None:
        raise AssertionError("database access occurred before input validation")


async def cleanup_tenant_name(tenant_name: str) -> None:
    async with AsyncSessionLocal() as session:
        tenant_ids = list(
            await session.scalars(select(Tenant.id).where(Tenant.name == tenant_name))
        )
        if tenant_ids:
            await session.execute(
                delete(ApiKey).where(ApiKey.tenant_id.in_(tenant_ids))
            )
            await session.execute(
                delete(BudgetAccount).where(BudgetAccount.tenant_id.in_(tenant_ids))
            )
            await session.execute(delete(Tenant).where(Tenant.id.in_(tenant_ids)))
            await session.commit()


@pytest_asyncio.fixture
async def cli_tenant_name(_check_infra: object):
    tenant_name = f"wp5-cli-{uuid.uuid4().hex}"
    yield tenant_name
    await cleanup_tenant_name(tenant_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("tenant_name", ["   ", "x" * 201])
async def test_invalid_tenant_name_fails_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
    tenant_name: str,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "AsyncSessionLocal",
        DatabaseAccessForbidden(),
    )

    with pytest.raises(ValueError):
        await create_api_key(tenant_name, Decimal("5.00"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit",
    [
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("0"),
        Decimal("0.00001"),
        Decimal("10000.0001"),
        Decimal("1.00001"),
    ],
)
async def test_invalid_budget_fails_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
    limit: Decimal,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "AsyncSessionLocal",
        DatabaseAccessForbidden(),
    )

    with pytest.raises(ValueError):
        await create_api_key("valid-tenant", limit)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("0.0001", Decimal("0.0001")),
        ("5.00", Decimal("5.00")),
        ("10000", Decimal("10000")),
    ],
)
def test_parse_monthly_limit_accepts_safe_decimal_values(
    raw_value: str,
    expected: Decimal,
) -> None:
    assert parse_monthly_limit(raw_value) == expected


def test_parse_monthly_limit_rejects_non_decimal() -> None:
    with pytest.raises(ValueError, match="valid decimal"):
        parse_monthly_limit("not-a-number")


@pytest.mark.asyncio
async def test_rotation_flags_fail_before_database_access(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli_module,
        "AsyncSessionLocal",
        DatabaseAccessForbidden(),
    )

    with pytest.raises(ValueError, match="required with --rotate"):
        await create_api_key("tenant", Decimal("5"), rotate=True)

    with pytest.raises(ValueError, match="valid only with --rotate"):
        await create_api_key(
            "tenant",
            Decimal("5"),
            expected_active_prefix="sk-gw-stale",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_first_create_persists_only_prefix_and_hash(
    cli_tenant_name: str,
) -> None:
    raw_key = await create_api_key(cli_tenant_name, Decimal("5.0000"))

    async with AsyncSessionLocal() as session:
        tenant = await session.scalar(
            select(Tenant).where(Tenant.name == cli_tenant_name)
        )
        assert tenant is not None
        budget = await session.scalar(
            select(BudgetAccount).where(BudgetAccount.tenant_id == tenant.id)
        )
        stored_key = await session.scalar(
            select(ApiKey).where(ApiKey.tenant_id == tenant.id)
        )

    assert budget is not None
    assert budget.monthly_limit_usd == Decimal("5.0000")
    assert stored_key is not None
    assert stored_key.status == "active"
    assert stored_key.prefix == raw_key[:12]
    assert stored_key.key_hash == hashlib.sha256(raw_key.encode()).hexdigest()
    assert raw_key not in {stored_key.prefix, stored_key.key_hash}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_active_key_requires_explicit_rotation(
    cli_tenant_name: str,
) -> None:
    await create_api_key(cli_tenant_name, Decimal("5.0000"))

    with pytest.raises(RuntimeError, match="already has 1 active key"):
        await create_api_key(cli_tenant_name, Decimal("5.0000"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rotation_revokes_expected_key_and_creates_one_active_key(
    cli_tenant_name: str,
) -> None:
    old_raw_key = await create_api_key(cli_tenant_name, Decimal("5.0000"))
    new_raw_key = await create_api_key(
        cli_tenant_name,
        Decimal("5.0000"),
        rotate=True,
        expected_active_prefix=old_raw_key[:12],
    )

    async with AsyncSessionLocal() as session:
        tenant_id = await session.scalar(
            select(Tenant.id).where(Tenant.name == cli_tenant_name)
        )
        keys = list(
            await session.scalars(
                select(ApiKey).where(ApiKey.tenant_id == tenant_id).order_by(ApiKey.id)
            )
        )

    assert [key.status for key in keys] == ["revoked", "active"]
    assert keys[0].key_hash == hashlib.sha256(old_raw_key.encode()).hexdigest()
    assert keys[1].key_hash == hashlib.sha256(new_raw_key.encode()).hexdigest()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_stale_rotation_prefix_preserves_current_key(
    cli_tenant_name: str,
) -> None:
    raw_key = await create_api_key(cli_tenant_name, Decimal("5.0000"))

    with pytest.raises(RuntimeError, match="active key changed"):
        await create_api_key(
            cli_tenant_name,
            Decimal("5.0000"),
            rotate=True,
            expected_active_prefix="sk-gw-stale",
        )

    async with AsyncSessionLocal() as session:
        tenant_id = await session.scalar(
            select(Tenant.id).where(Tenant.name == cli_tenant_name)
        )
        keys = list(
            await session.scalars(select(ApiKey).where(ApiKey.tenant_id == tenant_id))
        )

    assert len(keys) == 1
    assert keys[0].status == "active"
    assert keys[0].key_hash == hashlib.sha256(raw_key.encode()).hexdigest()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("active_key_count", [0, 2])
async def test_rotation_refuses_ambiguous_active_key_state(
    cli_tenant_name: str,
    active_key_count: int,
) -> None:
    async with AsyncSessionLocal() as session:
        tenant = Tenant(name=cli_tenant_name)
        session.add(tenant)
        await session.flush()
        session.add(
            BudgetAccount(
                tenant_id=tenant.id,
                monthly_limit_usd=Decimal("5.0000"),
            )
        )
        for index in range(active_key_count):
            raw = f"existing-{index}-{uuid.uuid4().hex}"
            session.add(
                ApiKey(
                    tenant_id=tenant.id,
                    prefix=raw[:12],
                    key_hash=hashlib.sha256(raw.encode()).hexdigest(),
                    status="active",
                )
            )
        await session.commit()

    with pytest.raises(RuntimeError, match="exactly one active key"):
        await create_api_key(
            cli_tenant_name,
            Decimal("5.0000"),
            rotate=True,
            expected_active_prefix="existing",
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_missing_budget_account_is_repaired(cli_tenant_name: str) -> None:
    async with AsyncSessionLocal() as session:
        session.add(Tenant(name=cli_tenant_name))
        await session.commit()

    await create_api_key(cli_tenant_name, Decimal("7.5000"))

    async with AsyncSessionLocal() as session:
        tenant_id = await session.scalar(
            select(Tenant.id).where(Tenant.name == cli_tenant_name)
        )
        budgets = list(
            await session.scalars(
                select(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
            )
        )

    assert len(budgets) == 1
    assert budgets[0].monthly_limit_usd == Decimal("7.5000")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_existing_budget_is_not_silently_changed(
    cli_tenant_name: str,
) -> None:
    async with AsyncSessionLocal() as session:
        tenant = Tenant(name=cli_tenant_name)
        session.add(tenant)
        await session.flush()
        session.add(
            BudgetAccount(
                tenant_id=tenant.id,
                monthly_limit_usd=Decimal("5.0000"),
            )
        )
        await session.commit()

    with pytest.raises(RuntimeError, match="existing budget differs"):
        await create_api_key(cli_tenant_name, Decimal("6.0000"))

    async with AsyncSessionLocal() as session:
        tenant_id = await session.scalar(
            select(Tenant.id).where(Tenant.name == cli_tenant_name)
        )
        budget = await session.scalar(
            select(BudgetAccount).where(BudgetAccount.tenant_id == tenant_id)
        )
        keys = list(
            await session.scalars(select(ApiKey).where(ApiKey.tenant_id == tenant_id))
        )

    assert budget is not None
    assert budget.monthly_limit_usd == Decimal("5.0000")
    assert keys == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_first_create_produces_one_active_key(
    cli_tenant_name: str,
) -> None:
    results = await asyncio.gather(
        create_api_key(cli_tenant_name, Decimal("5.0000")),
        create_api_key(f"  {cli_tenant_name}  ", Decimal("5.0000")),
        return_exceptions=True,
    )

    assert sum(isinstance(result, str) for result in results) == 1
    assert sum(isinstance(result, RuntimeError) for result in results) == 1

    async with AsyncSessionLocal() as session:
        tenants = list(
            await session.scalars(select(Tenant).where(Tenant.name == cli_tenant_name))
        )
        tenant_ids = [tenant.id for tenant in tenants]
        budgets = list(
            await session.scalars(
                select(BudgetAccount).where(BudgetAccount.tenant_id.in_(tenant_ids))
            )
        )
        active_keys = list(
            await session.scalars(
                select(ApiKey).where(
                    ApiKey.tenant_id.in_(tenant_ids),
                    ApiKey.status == "active",
                )
            )
        )

    assert len(tenants) == 1
    assert len(budgets) == 1
    assert len(active_keys) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_rotation_allows_at_most_one_winner(
    cli_tenant_name: str,
) -> None:
    old_raw_key = await create_api_key(cli_tenant_name, Decimal("5.0000"))
    expected_prefix = old_raw_key[:12]

    results = await asyncio.gather(
        create_api_key(
            cli_tenant_name,
            Decimal("5.0000"),
            rotate=True,
            expected_active_prefix=expected_prefix,
        ),
        create_api_key(
            cli_tenant_name,
            Decimal("5.0000"),
            rotate=True,
            expected_active_prefix=expected_prefix,
        ),
        return_exceptions=True,
    )

    assert sum(isinstance(result, str) for result in results) == 1
    assert sum(isinstance(result, RuntimeError) for result in results) == 1

    async with AsyncSessionLocal() as session:
        tenant_id = await session.scalar(
            select(Tenant.id).where(Tenant.name == cli_tenant_name)
        )
        keys = list(
            await session.scalars(select(ApiKey).where(ApiKey.tenant_id == tenant_id))
        )

    assert len(keys) == 2
    assert sum(key.status == "active" for key in keys) == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_duplicate_tenant_name_requires_operator_repair(
    cli_tenant_name: str,
) -> None:
    async with AsyncSessionLocal() as session:
        session.add_all(
            [
                Tenant(name=cli_tenant_name),
                Tenant(name=cli_tenant_name),
            ]
        )
        await session.commit()

    with pytest.raises(RuntimeError, match="duplicate tenant name"):
        await create_api_key(cli_tenant_name, Decimal("5.0000"))


def test_cli_failure_exits_nonzero_without_printing_raw_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    create = AsyncMock(side_effect=RuntimeError("safe operator error"))
    monkeypatch.setattr(cli_module, "create_api_key", create)

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--tenant-name",
                "ship-demo",
                "--monthly-limit-usd",
                "5.00",
            ]
        )

    captured = capsys.readouterr()
    assert exc_info.value.code == 1
    assert "safe operator error" in captured.err
    assert "API Key" not in captured.out
    assert "sk-gw-" not in captured.out + captured.err


def test_cli_success_prints_raw_key_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    raw_key = "example-one-time-key"
    create = AsyncMock(return_value=raw_key)
    monkeypatch.setattr(cli_module, "create_api_key", create)

    main(
        [
            "--tenant-name",
            "ship-demo",
            "--monthly-limit-usd",
            "5.00",
        ]
    )

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count(raw_key) == 1
