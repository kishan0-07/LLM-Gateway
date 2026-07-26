"""postgres budget authority

Revision ID: d4c8b1a7e2f0
Revises: a952f6e4f80c
Create Date: 2026-07-26

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "d4c8b1a7e2f0"
down_revision: Union[str, Sequence[str], None] = "a952f6e4f80c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "budget_periods",
        sa.Column("tenant_id", sa.Integer(), nullable=False),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("limit_micros", sa.BigInteger(), nullable=False),
        sa.Column(
            "reserved_micros",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "spent_micros",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "limit_micros >= 0",
            name="ck_budget_period_limit_nonnegative",
        ),
        sa.CheckConstraint(
            "reserved_micros >= 0",
            name="ck_budget_period_reserved_nonnegative",
        ),
        sa.CheckConstraint(
            "spent_micros >= 0",
            name="ck_budget_period_spent_nonnegative",
        ),
        sa.CheckConstraint(
            "period_end > period_start",
            name="ck_budget_period_valid_range",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "period_start"),
    )

    op.add_column(
        "budget_reservations",
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "budget_reservations",
        sa.Column(
            "held_micros",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "budget_reservations",
        sa.Column(
            "consumed_micros",
            sa.BigInteger(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "budget_reservations",
        sa.Column("final_status", sa.String(), nullable=True),
    )
    op.add_column(
        "budget_reservations",
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "budget_reservations",
        sa.Column(
            "reconciliation_requested_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        op.f("ix_budget_reservations_gateway_request_id"),
        "budget_reservations",
        ["gateway_request_id"],
        unique=False,
    )

    op.add_column(
        "usage_ledger",
        sa.Column("provider_attempt_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "usage_ledger",
        sa.Column("cost_micros", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "usage_ledger",
        sa.Column(
            "billing_status",
            sa.String(),
            server_default=sa.text("'known'"),
            nullable=False,
        ),
    )
    op.create_index(
        op.f("ix_usage_ledger_gateway_request_id"),
        "usage_ledger",
        ["gateway_request_id"],
        unique=False,
    )
    op.add_column(
        "provider_attempts",
        sa.Column("authorized_cost_micros", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "provider_attempts",
        sa.Column("estimated_input_tokens", sa.Integer(), nullable=True),
    )
    op.add_column(
        "provider_attempts",
        sa.Column("estimated_output_tokens", sa.Integer(), nullable=True),
    )

    op.execute(
        """
        UPDATE budget_reservations
        SET
            period_start =
                date_trunc('month', created_at AT TIME ZONE 'UTC')
                AT TIME ZONE 'UTC',
            held_micros = CASE
                WHEN status = 'reserved'
                THEN ROUND(estimated_cost_usd * 1000000)::bigint
                ELSE 0
            END,
            consumed_micros = 0
        """
    )
    op.execute(
        """
        UPDATE usage_ledger
        SET
            cost_micros = ROUND(cost_usd * 1000000)::bigint,
            billing_status = CASE
                WHEN usage_source = 'actual' THEN 'known'
                ELSE 'estimated'
            END
        """
    )
    op.alter_column("budget_reservations", "period_start", nullable=False)
    op.alter_column("usage_ledger", "cost_micros", nullable=False)

    op.drop_index(
        "ix_budget_reservations_reconciliation",
        table_name="budget_reservations",
    )
    op.create_index(
        "ix_budget_reservations_reconciliation",
        "budget_reservations",
        ["reconciliation_state", "reconciliation_requested_at"],
        unique=False,
    )

    op.create_unique_constraint(
        "uq_budget_reservations_gateway_request",
        "budget_reservations",
        ["gateway_request_id"],
    )
    op.create_check_constraint(
        "ck_budget_reservation_held_nonnegative",
        "budget_reservations",
        "held_micros >= 0",
    )
    op.create_check_constraint(
        "ck_budget_reservation_consumed_nonnegative",
        "budget_reservations",
        "consumed_micros >= 0",
    )
    op.create_unique_constraint(
        "uq_provider_attempt_request_number",
        "provider_attempts",
        ["gateway_request_id", "attempt_number"],
    )
    op.create_check_constraint(
        "ck_provider_attempt_authorized_cost_nonnegative",
        "provider_attempts",
        "authorized_cost_micros >= 0",
    )
    op.create_foreign_key(
        "fk_usage_ledger_provider_attempt",
        "usage_ledger",
        "provider_attempts",
        ["provider_attempt_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_unique_constraint(
        "uq_usage_ledger_provider_attempt",
        "usage_ledger",
        ["provider_attempt_id"],
    )
    op.create_check_constraint(
        "ck_usage_ledger_input_tokens_nonnegative",
        "usage_ledger",
        "input_tokens >= 0",
    )
    op.create_check_constraint(
        "ck_usage_ledger_output_tokens_nonnegative",
        "usage_ledger",
        "output_tokens >= 0",
    )
    op.create_check_constraint(
        "ck_usage_ledger_cost_nonnegative",
        "usage_ledger",
        "cost_micros >= 0",
    )

    # Reconstruct the authoritative active-month balance from durable rows.
    op.execute(
        """
        WITH bounds AS (
            SELECT
                date_trunc('month', now() AT TIME ZONE 'UTC')
                    AT TIME ZONE 'UTC' AS period_start,
                (
                    date_trunc('month', now() AT TIME ZONE 'UTC')
                    + interval '1 month'
                ) AT TIME ZONE 'UTC' AS period_end
        ),
        settled AS (
            SELECT
                br.tenant_id,
                COALESCE(SUM(ul.cost_micros), 0)::bigint AS spent_micros
            FROM usage_ledger AS ul
            JOIN budget_reservations AS br
              ON br.id = ul.reservation_id
            CROSS JOIN bounds AS b
            WHERE br.created_at >= b.period_start
              AND br.created_at < b.period_end
            GROUP BY br.tenant_id
        ),
        active AS (
            SELECT
                br.tenant_id,
                COALESCE(SUM(br.held_micros), 0)::bigint AS reserved_micros
            FROM budget_reservations AS br
            CROSS JOIN bounds AS b
            WHERE br.status = 'reserved'
              AND br.created_at >= b.period_start
              AND br.created_at < b.period_end
            GROUP BY br.tenant_id
        )
        INSERT INTO budget_periods (
            tenant_id,
            period_start,
            period_end,
            limit_micros,
            reserved_micros,
            spent_micros
        )
        SELECT
            ba.tenant_id,
            b.period_start,
            b.period_end,
            ROUND(ba.monthly_limit_usd * 1000000)::bigint,
            COALESCE(a.reserved_micros, 0),
            COALESCE(s.spent_micros, 0)
        FROM budget_accounts AS ba
        CROSS JOIN bounds AS b
        LEFT JOIN active AS a ON a.tenant_id = ba.tenant_id
        LEFT JOIN settled AS s ON s.tenant_id = ba.tenant_id
        """
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_usage_ledger_cost_nonnegative",
        "usage_ledger",
        type_="check",
    )
    op.drop_constraint(
        "ck_usage_ledger_output_tokens_nonnegative",
        "usage_ledger",
        type_="check",
    )
    op.drop_constraint(
        "ck_usage_ledger_input_tokens_nonnegative",
        "usage_ledger",
        type_="check",
    )
    op.drop_constraint(
        "uq_usage_ledger_provider_attempt",
        "usage_ledger",
        type_="unique",
    )
    op.drop_constraint(
        "fk_usage_ledger_provider_attempt",
        "usage_ledger",
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_provider_attempt_authorized_cost_nonnegative",
        "provider_attempts",
        type_="check",
    )
    op.drop_constraint(
        "uq_provider_attempt_request_number",
        "provider_attempts",
        type_="unique",
    )
    op.drop_constraint(
        "ck_budget_reservation_consumed_nonnegative",
        "budget_reservations",
        type_="check",
    )
    op.drop_constraint(
        "ck_budget_reservation_held_nonnegative",
        "budget_reservations",
        type_="check",
    )
    op.drop_constraint(
        "uq_budget_reservations_gateway_request",
        "budget_reservations",
        type_="unique",
    )

    op.drop_index(
        "ix_budget_reservations_reconciliation",
        table_name="budget_reservations",
    )
    op.create_index(
        "ix_budget_reservations_reconciliation",
        "budget_reservations",
        ["reconciliation_state", "created_at"],
        unique=False,
    )
    op.drop_index(
        op.f("ix_usage_ledger_gateway_request_id"),
        table_name="usage_ledger",
    )
    op.drop_column("usage_ledger", "billing_status")
    op.drop_column("usage_ledger", "cost_micros")
    op.drop_column("usage_ledger", "provider_attempt_id")
    op.drop_column("provider_attempts", "estimated_output_tokens")
    op.drop_column("provider_attempts", "estimated_input_tokens")
    op.drop_column("provider_attempts", "authorized_cost_micros")

    op.drop_index(
        op.f("ix_budget_reservations_gateway_request_id"),
        table_name="budget_reservations",
    )
    op.drop_column("budget_reservations", "reconciliation_requested_at")
    op.drop_column("budget_reservations", "finalized_at")
    op.drop_column("budget_reservations", "final_status")
    op.drop_column("budget_reservations", "consumed_micros")
    op.drop_column("budget_reservations", "held_micros")
    op.drop_column("budget_reservations", "period_start")
    op.drop_table("budget_periods")
