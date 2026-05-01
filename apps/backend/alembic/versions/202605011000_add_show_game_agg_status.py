"""add show game aggregation status

Revision ID: 202605011000
Revises: 625dcb4bdb5e
Create Date: 2026-05-01 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "202605011000"
down_revision: Union[str, None] = "625dcb4bdb5e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "show_game_agg_status",
        sa.Column("game_id", sa.String(), nullable=False),
        sa.Column("agg_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="done"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("aggregated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["game_id"], ["show_game_summary.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("game_id", "agg_version"),
    )
    op.create_index(
        "ix_show_game_agg_status_status_version",
        "show_game_agg_status",
        ["status", "agg_version"],
        unique=False,
    )
    op.create_index(
        "ix_show_game_agg_status_updated_at",
        "show_game_agg_status",
        ["updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_show_game_agg_status_updated_at", table_name="show_game_agg_status")
    op.drop_index("ix_show_game_agg_status_status_version", table_name="show_game_agg_status")
    op.drop_table("show_game_agg_status")
