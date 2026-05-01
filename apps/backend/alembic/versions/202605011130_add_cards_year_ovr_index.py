"""add cards year ovr index

Revision ID: 202605011130
Revises: 202605011000
Create Date: 2026-05-01 11:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "202605011130"
down_revision: Union[str, None] = "202605011000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index("ix_cards_year_ovr", "cards", ["year", "ovr"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_cards_year_ovr", table_name="cards")
