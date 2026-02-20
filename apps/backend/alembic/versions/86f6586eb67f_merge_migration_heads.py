"""merge_migration_heads

Revision ID: 86f6586eb67f
Revises: 2f6353feecdf, 6df839cb045e
Create Date: 2026-02-03 16:02:58.040201

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '86f6586eb67f'
down_revision: Union[str, None] = ('2f6353feecdf', '6df839cb045e')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
