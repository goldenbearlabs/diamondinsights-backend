"""merge_mobile_pull

Revision ID: 9ec4f30b91d8
Revises: 4c1254c5553b, 630fb2c9f916
Create Date: 2026-02-11 19:23:11.598408

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '9ec4f30b91d8'
down_revision: Union[str, None] = ('4c1254c5553b', '630fb2c9f916')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
