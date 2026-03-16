"""Add MLB 26 card attributes

Revision ID: 625dcb4bdb5e
Revises: d96e2a3f6196
Create Date: 2026-03-15 19:47:27.253886

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '625dcb4bdb5e'
down_revision: Union[str, None] = 'd96e2a3f6196'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- ADD MLB 26 STATS TO CARDS TABLE ---
    op.add_column('cards', sa.Column('hits_per_bf_left', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('hits_per_bf_right', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('k_per_bf_left', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('k_per_bf_right', sa.Integer(), server_default='0', nullable=False))
    
    op.add_column('cards', sa.Column('reaction_left', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('reaction_right', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('reaction_forward', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('reaction_back', sa.Integer(), server_default='0', nullable=False))
    
    op.add_column('cards', sa.Column('pop_time', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('base_stealing', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('contact_rating', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('power_rating', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('speed_rating', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('arm_rating', sa.Integer(), server_default='0', nullable=False))
    op.add_column('cards', sa.Column('fielding_rating', sa.Integer(), server_default='0', nullable=False))


def downgrade() -> None:
    # --- REMOVE MLB 26 STATS ---
    op.drop_column('cards', 'fielding_rating')
    op.drop_column('cards', 'arm_rating')
    op.drop_column('cards', 'speed_rating')
    op.drop_column('cards', 'power_rating')
    op.drop_column('cards', 'contact_rating')
    op.drop_column('cards', 'base_stealing')
    op.drop_column('cards', 'pop_time')
    
    op.drop_column('cards', 'reaction_back')
    op.drop_column('cards', 'reaction_forward')
    op.drop_column('cards', 'reaction_right')
    op.drop_column('cards', 'reaction_left')
    
    op.drop_column('cards', 'k_per_bf_right')
    op.drop_column('cards', 'k_per_bf_left')
    op.drop_column('cards', 'hits_per_bf_right')
    op.drop_column('cards', 'hits_per_bf_left')