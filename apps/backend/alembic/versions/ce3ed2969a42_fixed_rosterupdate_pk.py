"""fixed rosterupdate pk

Revision ID: ce3ed2969a42
Revises: 15db95071e37
Create Date: 2025-12-10 12:00:00.000000
"""

from alembic import op
import sqlalchemy as sa

revision = "ce3ed2969a42"
down_revision = "15db95071e37"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1) Add update_date columns (nullable) to card_updates + card_attribute_changes
    op.add_column(
        "card_updates",
        sa.Column("update_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "card_attribute_changes",
        sa.Column("update_date", sa.Date(), nullable=True),
    )

    # 2) Backfill card_updates.update_date from roster_updates.date
    op.execute(
        """
        UPDATE card_updates AS cu
        SET update_date = ru.date
        FROM roster_updates AS ru
        WHERE cu.update_id = ru.id
        """
    )

    # 3) Backfill card_attribute_changes.update_date from card_updates.update_date
    op.execute(
        """
        UPDATE card_attribute_changes AS cac
        SET update_date = cu.update_date
        FROM card_updates AS cu
        WHERE cac.update_id = cu.update_id
          AND cac.card_id = cu.card_id
        """
    )

    # 4) Make update_date NOT NULL
    op.alter_column(
        "card_updates",
        "update_date",
        existing_type=sa.Date(),
        nullable=False,
    )
    op.alter_column(
        "card_attribute_changes",
        "update_date",
        existing_type=sa.Date(),
        nullable=False,
    )

    # 5) Drop old FKs/PKs (use IF EXISTS to be tolerant)
    op.execute(
        "ALTER TABLE card_attribute_changes "
        "DROP CONSTRAINT IF EXISTS card_attribute_changes_update_id_card_id_fkey"
    )
    op.execute(
        "ALTER TABLE card_updates "
        "DROP CONSTRAINT IF EXISTS card_updates_update_id_fkey"
    )
    op.execute(
        "ALTER TABLE card_updates "
        "DROP CONSTRAINT IF EXISTS card_updates_pkey"
    )
    op.execute(
        "ALTER TABLE roster_updates "
        "DROP CONSTRAINT IF EXISTS roster_updates_pkey"
    )

    # 6) Create new PK on roster_updates (id, date)
    op.create_primary_key(
        "roster_updates_pkey",
        "roster_updates",
        ["id", "date"],
    )

    # 7) Create new PK on card_updates (update_id, update_date, card_id)
    op.create_primary_key(
        "card_updates_pkey",
        "card_updates",
        ["update_id", "update_date", "card_id"],
    )

    # 8) Create new FK card_updates(update_id, update_date) -> roster_updates(id, date)
    op.create_foreign_key(
        "card_updates_update_id_update_date_fkey",
        "card_updates",
        "roster_updates",
        ["update_id", "update_date"],
        ["id", "date"],
    )

    # 9) Create new FK card_attribute_changes(update_id, update_date, card_id)
    #    -> card_updates(update_id, update_date, card_id)
    op.create_foreign_key(
        "card_attribute_changes_update_id_update_date_card_id_fkey",
        "card_attribute_changes",
        "card_updates",
        ["update_id", "update_date", "card_id"],
        ["update_id", "update_date", "card_id"],
    )


def downgrade() -> None:
    # 1) Drop new FKs
    op.execute(
        "ALTER TABLE card_attribute_changes "
        "DROP CONSTRAINT IF EXISTS card_attribute_changes_update_id_update_date_card_id_fkey"
    )
    op.execute(
        "ALTER TABLE card_updates "
        "DROP CONSTRAINT IF EXISTS card_updates_update_id_update_date_fkey"
    )

    # 2) Drop new PKs
    op.execute(
        "ALTER TABLE card_updates "
        "DROP CONSTRAINT IF EXISTS card_updates_pkey"
    )
    op.execute(
        "ALTER TABLE roster_updates "
        "DROP CONSTRAINT IF EXISTS roster_updates_pkey"
    )

    # 3) Restore original PK on roster_updates(id)
    op.create_primary_key(
        "roster_updates_pkey",
        "roster_updates",
        ["id"],
    )

    # 4) Restore original PK on card_updates(update_id, card_id)
    op.create_primary_key(
        "card_updates_pkey",
        "card_updates",
        ["update_id", "card_id"],
    )

    # 5) Restore original FKs
    op.create_foreign_key(
        "card_updates_update_id_fkey",
        "card_updates",
        "roster_updates",
        ["update_id"],
        ["id"],
    )
    op.create_foreign_key(
        "card_attribute_changes_update_id_card_id_fkey",
        "card_attribute_changes",
        "card_updates",
        ["update_id", "card_id"],
        ["update_id", "card_id"],
    )

    # 6) Drop update_date columns (back to original schema)
    op.drop_column("card_attribute_changes", "update_date")
    op.drop_column("card_updates", "update_date")
