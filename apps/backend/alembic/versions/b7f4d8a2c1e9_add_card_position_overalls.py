"""add card position overalls table

Revision ID: b7f4d8a2c1e9
Revises: a5dbfb597bfc
Create Date: 2026-02-10 16:25:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b7f4d8a2c1e9"
down_revision: Union[str, None] = "a5dbfb597bfc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "card_position_overalls",
        sa.Column("card_id", sa.String(), nullable=False),
        sa.Column("position", sa.String(length=8), nullable=False),
        sa.Column("is_primary", sa.Boolean(), nullable=False),
        sa.Column("is_hitter", sa.Boolean(), nullable=False),
        sa.Column("true_overall", sa.Float(), nullable=False),
        sa.Column("true_overall_rounded", sa.Integer(), nullable=False),
        sa.Column("meta_overall", sa.Float(), nullable=False),
        sa.Column("meta_overall_rounded", sa.Integer(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["card_id"], ["cards.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("card_id", "position"),
    )
    op.create_index(
        op.f("ix_card_position_overalls_computed_at"),
        "card_position_overalls",
        ["computed_at"],
        unique=False,
    )
    op.create_index(
        "ix_card_position_overalls_position",
        "card_position_overalls",
        ["position"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_card_position_overalls_position", table_name="card_position_overalls")
    op.drop_index(op.f("ix_card_position_overalls_computed_at"), table_name="card_position_overalls")
    op.drop_table("card_position_overalls")
