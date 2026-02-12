"""merge_conflict_fix

Revision ID: 630fb2c9f916
Revises: 202602061230, a5dbfb597bfc
Create Date: 2026-02-09 18:41:52.252020

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '630fb2c9f916'
down_revision: Union[str, None] = ('202602061230', 'a5dbfb597bfc')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
