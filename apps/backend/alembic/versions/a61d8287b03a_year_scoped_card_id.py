"""year_scoped_card_id

Revision ID: a61d8287b03a
Revises: 39afa544a843
Create Date: 2025-12-29 15:54:00.398849

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a61d8287b03a'
down_revision: Union[str, None] = '39afa544a843'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        TRUNCATE TABLE
            card_attribute_changes,
            card_updates,
            roster_updates,
            market_candles,
            completed_orders,
            price_history,
            listings,
            pitches,
            card_quirks,
            card_locations,
            cards
        RESTART IDENTITY CASCADE;
        """
    )

    op.execute("ALTER TABLE cards ADD COLUMN source_uuid TEXT;")

    op.execute("UPDATE cards SET source_uuid = id;")
    op.execute("UPDATE cards SET id = (year::text || ':' || source_uuid);")

    op.execute("ALTER TABLE cards ALTER COLUMN source_uuid SET NOT NULL;")
    op.execute("ALTER TABLE cards ADD CONSTRAINT uq_cards_year_source_uuid UNIQUE (year, source_uuid);")

    op.execute("ALTER TABLE card_quirks DROP CONSTRAINT IF EXISTS card_quirks_card_id_fkey;")
    op.execute("ALTER TABLE card_locations DROP CONSTRAINT IF EXISTS card_locations_card_id_fkey;")
    op.execute("ALTER TABLE pitches DROP CONSTRAINT IF EXISTS pitches_card_id_fkey;")
    op.execute("ALTER TABLE listings DROP CONSTRAINT IF EXISTS listings_card_id_fkey;")
    op.execute("ALTER TABLE card_updates DROP CONSTRAINT IF EXISTS card_updates_card_id_fkey;")

    op.create_foreign_key(
        "card_quirks_card_id_fkey",
        "card_quirks",
        "cards",
        ["card_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "card_locations_card_id_fkey",
        "card_locations",
        "cards",
        ["card_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "pitches_card_id_fkey",
        "pitches",
        "cards",
        ["card_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "listings_card_id_fkey",
        "listings",
        "cards",
        ["card_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "card_updates_card_id_fkey",
        "card_updates",
        "cards",
        ["card_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    op.execute(
        """
        TRUNCATE TABLE
            card_attribute_changes,
            card_updates,
            roster_updates,
            market_candles,
            completed_orders,
            price_history,
            listings,
            pitches,
            card_quirks,
            card_locations,
            cards
        RESTART IDENTITY CASCADE;
        """
    )

    op.execute("ALTER TABLE cards DROP CONSTRAINT IF EXISTS uq_cards_year_source_uuid;")
    op.execute("ALTER TABLE cards DROP COLUMN IF EXISTS source_uuid;")
