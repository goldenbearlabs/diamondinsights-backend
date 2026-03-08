"""add_user_description

Revision ID: 33d7b7f31702
Revises: 202602191045
Create Date: 2026-03-07 10:55:56.123226

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '33d7b7f31702'
down_revision: Union[str, None] = '202602191045'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- ONLY ADD THE DESCRIPTION COLUMN ---
    op.add_column('users', sa.Column('description', sa.Text(), nullable=True))
    # ### end Alembic commands ###


def downgrade() -> None:
    # --- ONLY DROP THE DESCRIPTION COLUMN ---
    op.drop_column('users', 'description')
    # ### end Alembic commands ###
