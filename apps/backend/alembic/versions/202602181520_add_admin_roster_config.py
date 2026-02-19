"""add_admin_roster_config

Revision ID: 202602181520
Revises: 202602181430
Create Date: 2026-02-18 15:20:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "202602181520"
down_revision: Union[str, None] = "202602181430"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "admin_roster_config",
        sa.Column("singleton_id", sa.SmallInteger(), nullable=False),
        sa.Column("next_roster_update_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("singleton_id = 1", name="ck_admin_roster_config_singleton"),
        sa.PrimaryKeyConstraint("singleton_id"),
    )


def downgrade() -> None:
    op.drop_table("admin_roster_config")
