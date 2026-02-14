"""add_is_public_to_portfolios

Revision ID: 98a9b08e90d8
Revises: 9ec4f30b91d8
Create Date: 2026-02-13 20:32:33.552421

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '98a9b08e90d8'
down_revision: Union[str, None] = '9ec4f30b91d8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add is_public column to portfolios table, default True
    op.add_column('portfolios', sa.Column('is_public', sa.Boolean(), nullable=False, server_default='true'))


def downgrade() -> None:
    # Remove is_public column from portfolios table
    op.drop_column('portfolios', 'is_public')
