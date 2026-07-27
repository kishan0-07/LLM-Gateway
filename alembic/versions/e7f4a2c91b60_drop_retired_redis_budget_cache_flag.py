"""drop retired redis budget cache flag

Revision ID: e7f4a2c91b60
Revises: d4c8b1a7e2f0
Create Date: 2026-07-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e7f4a2c91b60"
down_revision: str | None = "d4c8b1a7e2f0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("budget_reservations", "cache_sync_required")


def downgrade() -> None:
    op.add_column(
        "budget_reservations",
        sa.Column(
            "cache_sync_required",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
    )
