"""add comment edited_at

Revision ID: 202602061230
Revises: 202602051300
Create Date: 2026-02-06

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '202602061230'
down_revision: Union[str, None] = '202602051300'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('comments', sa.Column('edited_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('comments', 'edited_at')
