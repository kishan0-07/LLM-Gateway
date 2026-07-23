"""day18_snapshots

Revision ID: a952f6e4f80c
Revises: 9e5c10fac9dd
Create Date: 2026-07-23 15:54:36.604748

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a952f6e4f80c'
down_revision: Union[str, Sequence[str], None] = '9e5c10fac9dd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("budget_reservations", sa.Column("requested_model", sa.String(), nullable=True))
    op.add_column("budget_reservations", sa.Column("estimated_input_tokens", sa.Integer(), nullable=True))
    op.add_column("budget_reservations", sa.Column("estimated_output_tokens", sa.Integer(), nullable=True))

def downgrade() -> None:
    op.drop_column("budget_reservations", "estimated_output_tokens")
    op.drop_column("budget_reservations", "estimated_input_tokens")
    op.drop_column("budget_reservations", "requested_model")