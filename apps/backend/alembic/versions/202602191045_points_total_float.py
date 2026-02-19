"""change_user_update_scores_points_total_to_float

Revision ID: 202602191045
Revises: 202602181520
Create Date: 2026-02-19 10:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "202602191045"
down_revision: Union[str, None] = "202602181520"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "user_update_scores",
        "points_total",
        existing_type=sa.Integer(),
        type_=sa.Float(),
        existing_nullable=False,
        postgresql_using="points_total::double precision",
    )


def downgrade() -> None:
    op.alter_column(
        "user_update_scores",
        "points_total",
        existing_type=sa.Float(),
        type_=sa.Integer(),
        existing_nullable=False,
        postgresql_using="ROUND(points_total)::integer",
    )
