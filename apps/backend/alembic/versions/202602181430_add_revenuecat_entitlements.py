"""add_revenuecat_entitlements

Revision ID: 202602181430
Revises: 98a9b08e90d8
Create Date: 2026-02-18 14:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "202602181430"
down_revision: Union[str, None] = "98a9b08e90d8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "revenuecat_webhook_events",
        sa.Column("external_event_id", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("app_user_id", sa.String(length=255), nullable=True),
        sa.Column("original_app_user_id", sa.String(length=255), nullable=True),
        sa.Column("api_version", sa.String(length=32), nullable=True),
        sa.Column("product_id", sa.String(length=255), nullable=True),
        sa.Column("store", sa.String(length=64), nullable=True),
        sa.Column("environment", sa.String(length=32), nullable=True),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("processing_error", sa.Text(), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("external_event_id"),
    )
    op.create_index(
        "ix_revenuecat_webhook_events_event_type",
        "revenuecat_webhook_events",
        ["event_type"],
        unique=False,
    )
    op.create_index(
        "ix_revenuecat_webhook_events_app_user_id",
        "revenuecat_webhook_events",
        ["app_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_revenuecat_webhook_events_original_app_user_id",
        "revenuecat_webhook_events",
        ["original_app_user_id"],
        unique=False,
    )
    op.create_index(
        "ix_revenuecat_webhook_events_event_timestamp",
        "revenuecat_webhook_events",
        ["event_timestamp"],
        unique=False,
    )

    op.create_table(
        "user_entitlements",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("entitlement_id", sa.String(length=128), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("product_identifier", sa.String(length=255), nullable=True),
        sa.Column("store", sa.String(length=64), nullable=True),
        sa.Column("ownership_type", sa.String(length=64), nullable=True),
        sa.Column("environment", sa.String(length=32), nullable=True),
        sa.Column("purchased_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latest_event_type", sa.String(length=64), nullable=True),
        sa.Column("latest_event_id", sa.String(length=128), nullable=True),
        sa.Column("latest_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id", "entitlement_id"),
    )
    op.create_index(
        "ix_user_entitlements_user_active",
        "user_entitlements",
        ["user_id", "is_active"],
        unique=False,
    )
    op.create_index(
        "ix_user_entitlements_expires_at",
        "user_entitlements",
        ["expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_user_entitlements_expires_at", table_name="user_entitlements")
    op.drop_index("ix_user_entitlements_user_active", table_name="user_entitlements")
    op.drop_table("user_entitlements")

    op.drop_index(
        "ix_revenuecat_webhook_events_event_timestamp",
        table_name="revenuecat_webhook_events",
    )
    op.drop_index(
        "ix_revenuecat_webhook_events_original_app_user_id",
        table_name="revenuecat_webhook_events",
    )
    op.drop_index(
        "ix_revenuecat_webhook_events_app_user_id",
        table_name="revenuecat_webhook_events",
    )
    op.drop_index(
        "ix_revenuecat_webhook_events_event_type",
        table_name="revenuecat_webhook_events",
    )
    op.drop_table("revenuecat_webhook_events")
