"""day10_rate_limit_stats

Revision ID: 603101e1b90c
Revises: a865dc26ed88
Create Date: 2026-07-13 00:34:51.661414

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '603101e1b90c'
down_revision: Union[str, Sequence[str], None] = 'a865dc26ed88'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "gateway_requests",
        sa.Column("api_key_id", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_gateway_requests_api_key_id_api_keys",
        "gateway_requests",
        "api_keys",
        ["api_key_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_gateway_requests_api_key_id",
        "gateway_requests",
        ["api_key_id"],
        unique=False,
    )
    op.add_column(
        "gateway_requests",
        sa.Column(
            "is_stream",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    op.add_column(
        "gateway_requests",
        sa.Column("gateway_overhead_ms", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("gateway_requests", "gateway_overhead_ms")
    op.drop_column("gateway_requests", "is_stream")
    op.drop_index("ix_gateway_requests_api_key_id", table_name="gateway_requests")
    op.drop_constraint(
        "fk_gateway_requests_api_key_id_api_keys",
        "gateway_requests",
        type_="foreignkey",
    )
    op.drop_column("gateway_requests", "api_key_id")