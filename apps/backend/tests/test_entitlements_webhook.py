import datetime

from src.api.routes.entitlements import (
    _event_implies_active,
    _extract_entitlement_ids,
    _extract_target_user_ids,
)


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
