"""day16_budget_reconciliation

Revision ID: 9e5c10fac9dd
Revises: 603101e1b90c
Create Date: 2026-07-20 23:08:04.226158

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9e5c10fac9dd'
down_revision: Union[str, Sequence[str], None] = '603101e1b90c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('budget_reservations', sa.Column('cache_sync_required', sa.Boolean(), server_default=sa.text('false'), nullable=False))
    op.add_column('budget_reservations', sa.Column('reconciliation_state', sa.String(), server_default=sa.text("'none'"), nullable=False))
    op.add_column('budget_reservations', sa.Column('reconciliation_reason', sa.String(), nullable=True))
    op.create_index('ix_budget_reservations_reconciliation', 'budget_reservations', ['reconciliation_state', 'created_at'], unique=False)

def downgrade() -> None:
    op.drop_index('ix_budget_reservations_reconciliation', table_name='budget_reservations')
    op.drop_column('budget_reservations', 'reconciliation_reason')
    op.drop_column('budget_reservations', 'reconciliation_state')
    op.drop_column('budget_reservations', 'cache_sync_required')
