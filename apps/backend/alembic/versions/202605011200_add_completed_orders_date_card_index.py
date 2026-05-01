"""add completed orders date card index

Revision ID: 202605011200
Revises: 202605011130
Create Date: 2026-05-01 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "202605011200"
down_revision: Union[str, None] = "202605011130"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_completed_orders_date_card_id",
        "completed_orders",
        ["date", "card_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_completed_orders_date_card_id", table_name="completed_orders")
