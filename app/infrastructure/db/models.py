import datetime
import decimal
import uuid
from sqlalchemy import ForeignKey, Numeric, Index, func , DateTime
from sqlalchemy.orm import Mapped, mapped_column
from app.infrastructure.db.session import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), index=True)
    prefix: Mapped[str]
    key_hash: Mapped[str] = mapped_column(unique=True, index=True)
    status: Mapped[str] = mapped_column(default="active")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())
    last_used_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True),default=None)


class BudgetAccount(Base):
    __tablename__ = "budget_accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), unique=True)
    monthly_limit_usd: Mapped[decimal.Decimal] = mapped_column(Numeric(10, 4), default=decimal.Decimal("10.0"))
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())


class GatewayRequest(Base):
    __tablename__ = "gateway_requests"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), index=True)
    trace_id: Mapped[str] = mapped_column(index=True)
    status: Mapped[str] = mapped_column(default="pending")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())


class BudgetReservation(Base):
    __tablename__ = "budget_reservations"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), index=True)
    gateway_request_id: Mapped[int] = mapped_column(ForeignKey("gateway_requests.id", ondelete="CASCADE"))
    estimated_tokens: Mapped[int]
    estimated_cost_usd: Mapped[decimal.Decimal] = mapped_column(Numeric(10, 6))
    status: Mapped[str] = mapped_column(default="reserved")
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())
    settled_at: Mapped[datetime.datetime | None] = mapped_column(DateTime(timezone=True),default=None)


class ProviderAttempt(Base):
    __tablename__ = "provider_attempts"

    id: Mapped[int] = mapped_column(primary_key=True)
    gateway_request_id: Mapped[int] = mapped_column(ForeignKey("gateway_requests.id", ondelete="CASCADE"), index=True)
    provider: Mapped[str]
    model: Mapped[str]
    attempt_number: Mapped[int] = mapped_column(default=1)
    status: Mapped[str]
    latency_ms: Mapped[int | None] = mapped_column(default=None)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())


class UsageLedger(Base):
    __tablename__ = "usage_ledger"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(ForeignKey("tenants.id", ondelete="CASCADE"), index=True)
    gateway_request_id: Mapped[int] = mapped_column(ForeignKey("gateway_requests.id", ondelete="CASCADE"))
    reservation_id: Mapped[str] = mapped_column(ForeignKey("budget_reservations.id", ondelete="RESTRICT"))
    provider: Mapped[str]
    model: Mapped[str]
    input_tokens: Mapped[int]
    output_tokens: Mapped[int]
    cost_usd: Mapped[decimal.Decimal] = mapped_column(Numeric(10, 6))
    usage_source: Mapped[str]
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True),server_default=func.now())

    __table_args__ = (Index("ix_usage_ledger_tenant_created", "tenant_id", "created_at"),)