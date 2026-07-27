import datetime
import decimal
import uuid
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    UniqueConstraint,
    false,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column
from app.infrastructure.db.session import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), index=True
    )
    prefix: Mapped[str]
    key_hash: Mapped[str] = mapped_column(unique=True, index=True)
    status: Mapped[str] = mapped_column(default="active")
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_used_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )


class BudgetAccount(Base):
    __tablename__ = "budget_accounts"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), unique=True
    )
    monthly_limit_usd: Mapped[decimal.Decimal] = mapped_column(
        Numeric(10, 4), default=decimal.Decimal("10.0")
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class BudgetPeriod(Base):
    """Authoritative mutable balance for one tenant and one UTC month."""

    __tablename__ = "budget_periods"

    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        primary_key=True,
    )
    period_start: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
    )
    period_end: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    limit_micros: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reserved_micros: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    spent_micros: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    __table_args__ = (
        CheckConstraint(
            "limit_micros >= 0",
            name="ck_budget_period_limit_nonnegative",
        ),
        CheckConstraint(
            "reserved_micros >= 0",
            name="ck_budget_period_reserved_nonnegative",
        ),
        CheckConstraint(
            "spent_micros >= 0",
            name="ck_budget_period_spent_nonnegative",
        ),
        CheckConstraint(
            "period_end > period_start",
            name="ck_budget_period_valid_range",
        ),
    )


class GatewayRequest(Base):
    __tablename__ = "gateway_requests"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), index=True
    )
    api_key_id: Mapped[int | None] = mapped_column(
        ForeignKey("api_keys.id", ondelete="RESTRICT"),
        index=True,
        nullable=True,
    )
    trace_id: Mapped[str] = mapped_column(index=True)
    status: Mapped[str] = mapped_column(default="pending")
    is_stream: Mapped[bool] = mapped_column(
        Boolean,
        server_default=false(),
        nullable=False,
    )
    gateway_overhead_ms: Mapped[int | None] = mapped_column(default=None)
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class BudgetReservation(Base):
    __tablename__ = "budget_reservations"

    id: Mapped[str] = mapped_column(primary_key=True, default=lambda: str(uuid.uuid4()))
    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), index=True
    )
    gateway_request_id: Mapped[int] = mapped_column(
        ForeignKey("gateway_requests.id", ondelete="CASCADE"),
        index=True,
    )
    estimated_tokens: Mapped[int]
    estimated_cost_usd: Mapped[decimal.Decimal] = mapped_column(Numeric(10, 6))
    status: Mapped[str] = mapped_column(default="reserved")
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    settled_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )
    reconciliation_state: Mapped[str] = mapped_column(
        default="none",
        server_default=text("'none'"),
        nullable=False,
    )
    reconciliation_reason: Mapped[str | None] = mapped_column(
        default=None,
        nullable=True,
    )
    requested_model: Mapped[str | None] = mapped_column(default=None, nullable=True)
    estimated_input_tokens: Mapped[int | None] = mapped_column(
        default=None, nullable=True
    )
    estimated_output_tokens: Mapped[int | None] = mapped_column(
        default=None, nullable=True
    )
    period_start: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    held_micros: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    consumed_micros: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )
    final_status: Mapped[str | None] = mapped_column(default=None, nullable=True)
    finalized_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        nullable=True,
    )
    reconciliation_requested_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        default=None,
        nullable=True,
    )

    __table_args__ = (
        UniqueConstraint(
            "gateway_request_id",
            name="uq_budget_reservations_gateway_request",
        ),
        CheckConstraint(
            "held_micros >= 0",
            name="ck_budget_reservation_held_nonnegative",
        ),
        CheckConstraint(
            "consumed_micros >= 0",
            name="ck_budget_reservation_consumed_nonnegative",
        ),
        Index(
            "ix_budget_reservations_reconciliation",
            "reconciliation_state",
            "reconciliation_requested_at",
        ),
    )


class ProviderAttempt(Base):
    __tablename__ = "provider_attempts"

    id: Mapped[int] = mapped_column(primary_key=True)
    gateway_request_id: Mapped[int] = mapped_column(
        ForeignKey("gateway_requests.id", ondelete="CASCADE"), index=True
    )
    provider: Mapped[str]
    model: Mapped[str]
    attempt_number: Mapped[int] = mapped_column(default=1)
    status: Mapped[str]
    latency_ms: Mapped[int | None] = mapped_column(default=None)
    authorized_cost_micros: Mapped[int | None] = mapped_column(
        BigInteger,
        default=None,
        nullable=True,
    )
    estimated_input_tokens: Mapped[int | None] = mapped_column(
        default=None,
        nullable=True,
    )
    estimated_output_tokens: Mapped[int | None] = mapped_column(
        default=None,
        nullable=True,
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "gateway_request_id",
            "attempt_number",
            name="uq_provider_attempt_request_number",
        ),
        CheckConstraint(
            "authorized_cost_micros >= 0",
            name="ck_provider_attempt_authorized_cost_nonnegative",
        ),
    )


class UsageLedger(Base):
    __tablename__ = "usage_ledger"

    id: Mapped[int] = mapped_column(primary_key=True)
    tenant_id: Mapped[int] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), index=True
    )
    gateway_request_id: Mapped[int] = mapped_column(
        ForeignKey("gateway_requests.id", ondelete="CASCADE"),
        index=True,
    )
    reservation_id: Mapped[str] = mapped_column(
        ForeignKey("budget_reservations.id", ondelete="RESTRICT")
    )
    provider_attempt_id: Mapped[int | None] = mapped_column(
        ForeignKey("provider_attempts.id", ondelete="RESTRICT"),
        nullable=True,
    )
    provider: Mapped[str]
    model: Mapped[str]
    input_tokens: Mapped[int]
    output_tokens: Mapped[int]
    cost_usd: Mapped[decimal.Decimal] = mapped_column(Numeric(10, 6))
    cost_micros: Mapped[int] = mapped_column(BigInteger, nullable=False)
    usage_source: Mapped[str]
    billing_status: Mapped[str] = mapped_column(
        nullable=False,
        server_default=text("'known'"),
    )
    created_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "provider_attempt_id",
            name="uq_usage_ledger_provider_attempt",
        ),
        CheckConstraint(
            "input_tokens >= 0",
            name="ck_usage_ledger_input_tokens_nonnegative",
        ),
        CheckConstraint(
            "output_tokens >= 0",
            name="ck_usage_ledger_output_tokens_nonnegative",
        ),
        CheckConstraint(
            "cost_micros >= 0",
            name="ck_usage_ledger_cost_nonnegative",
        ),
        Index("ix_usage_ledger_tenant_created", "tenant_id", "created_at"),
    )
