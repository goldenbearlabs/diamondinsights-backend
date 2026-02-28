import datetime

from src.api.routes.entitlements import (
    _event_implies_active,
    _extract_entitlement_ids,
    _extract_target_user_ids,
    _row_is_currently_active,
)
from shared.db.models import UserEntitlement


def test_extract_target_user_ids_prefers_transferred_to_on_transfer() -> None:
    event = {
        "app_user_id": "source_uid",
        "original_app_user_id": "source_uid",
        "aliases": ["source_uid", "other_alias"],
        "transferred_to": ["dest_uid", "source_uid", "$RCAnonymousID:ignore"],
    }

    assert _extract_target_user_ids(event, "TRANSFER") == [
        "dest_uid",
        "source_uid",
        "other_alias",
    ]


def test_extract_target_user_ids_keeps_default_order_on_non_transfer() -> None:
    event = {
        "app_user_id": "user_a",
        "original_app_user_id": "user_a",
        "aliases": ["$RCAnonymousID:ignore", "user_b", "user_a"],
        "transferred_to": ["user_c"],
    }

    assert _extract_target_user_ids(event, "INITIAL_PURCHASE") == ["user_a", "user_b", "user_c"]


def test_extract_entitlement_ids_supports_array_and_singular_and_dedupes() -> None:
    event = {
        "entitlement_ids": ["pro", " premium ", "pro", "", 42],
        "entitlement_id": "premium",
    }

    assert _extract_entitlement_ids(event) == ["pro", "premium"]


def test_event_implies_active_expiration_and_past_expiry() -> None:
    now = datetime.datetime.now(datetime.timezone.utc)

    assert _event_implies_active("EXPIRATION", None) is False
    assert _event_implies_active("RENEWAL", now - datetime.timedelta(seconds=1)) is False
    assert _event_implies_active("CANCELLATION", now + datetime.timedelta(days=1)) is True


def test_row_is_currently_active_respects_is_active_and_expiry() -> None:
    now = datetime.datetime.now(datetime.timezone.utc)

    row = UserEntitlement(user_id=1, entitlement_id="pro")
    row.is_active = True
    row.expires_at = now + datetime.timedelta(minutes=10)
    assert _row_is_currently_active(row, now) is True

    row.expires_at = now - datetime.timedelta(seconds=1)
    assert _row_is_currently_active(row, now) is False

    row.expires_at = None
    assert _row_is_currently_active(row, now) is True

    row.is_active = False
    assert _row_is_currently_active(row, now) is False
