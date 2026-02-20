"""add search_name columns and trigram indexes

Revision ID: c1d9a80b6d35
Revises: fc33ced5d5f2
Create Date: 2026-01-26 18:07:45.780064

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c1d9a80b6d35'
down_revision: Union[str, None] = 'fc33ced5d5f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("cards", sa.Column("search_name", sa.Text(), nullable=True))
    op.add_column("users", sa.Column("search_display_name", sa.Text(), nullable=True))

    # Backfill using unaccent() at write time (fine to call here)
    op.execute("UPDATE cards SET search_name = unaccent(lower(name)) WHERE search_name IS NULL;")
    op.execute("UPDATE users SET search_display_name = unaccent(lower(display_name)) WHERE search_display_name IS NULL;")

    # Make them NOT NULL once filled (optional but nice)
    op.alter_column("cards", "search_name", nullable=False)
    op.alter_column("users", "search_display_name", nullable=False)

    # Create trigram indexes (no unaccent() in the index expression anymore)
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    with op.get_context().autocommit_block():
        op.execute("SET maintenance_work_mem = '256MB';")
        op.execute("SET max_parallel_maintenance_workers = 0;")

        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_cards_search_name_trgm
            ON cards USING gin (search_name gin_trgm_ops);
        """)

        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_search_display_name_trgm
            ON users USING gin (search_display_name gin_trgm_ops);
        """)


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_cards_search_name_trgm;")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_users_search_display_name_trgm;")

    op.drop_column("cards", "search_name")
    op.drop_column("users", "search_display_name")
