from __future__ import annotations

import datetime as dt
import os
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import quote

import requests
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from shared.db.models import UserEntitlement, Users


DEFAULT_REVENUECAT_API_BASE_URL = "https://api.revenuecat.com/v1"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _parse_iso(value: Any) -> Optional[dt.datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _resolve_revenuecat_api_key() -> Optional[str]:
    for env_key in (
        "REVENUECAT_SECRET_API_KEY",
        "REVENUECAT_API_KEY",
        "REVENUECAT_SECRET_KEY",
    ):
        raw = (os.getenv(env_key) or "").strip()
        if raw:
            return raw
    return None


@dataclass(frozen=True)
class RevenueCatEntitlementSnapshot:
    entitlement_id: str
    is_active: bool
    product_identifier: Optional[str]
    store: Optional[str]
    ownership_type: Optional[str]
    environment: Optional[str]
    purchased_at: Optional[dt.datetime]
    expires_at: Optional[dt.datetime]


class RevenueCatEntitlementsReconcile(Job):
    def __init__(
        self,
        *,
        user_id: int | None = None,
        firebase_id: str | None = None,
        batch_limit: int | None = None,
    ) -> None:
        super().__init__()
        self.user_id = user_id
        self.firebase_id = firebase_id
        self.batch_limit = batch_limit

        self.api_base_url = (
            (os.getenv("REVENUECAT_API_BASE_URL") or DEFAULT_REVENUECAT_API_BASE_URL).strip()
            or DEFAULT_REVENUECAT_API_BASE_URL
        ).rstrip("/")
        self.api_key = _resolve_revenuecat_api_key()
        self.lookback_days = max(1, int(os.getenv("REVENUECAT_RECONCILE_LOOKBACK_DAYS", "30")))
        self.default_batch_limit = max(1, int(os.getenv("REVENUECAT_RECONCILE_BATCH_LIMIT", "500")))
        self.request_timeout_seconds = max(
            5.0, float(os.getenv("REVENUECAT_RECONCILE_TIMEOUT_SECONDS", "20"))
        )
        self.max_retries = max(0, int(os.getenv("REVENUECAT_RECONCILE_MAX_RETRIES", "4")))

    def run(self, db_session: Session) -> None:
        if not self.api_key:
            raise RuntimeError(
                "Missing RevenueCat API key. Set REVENUECAT_SECRET_API_KEY (or REVENUECAT_API_KEY)."
            )

        users = self._resolve_target_users(db_session)
        if not users:
            self.logger.info("revenuecat-entitlements-reconcile skipped reason=no_target_users")
            return

        now = _utcnow()
        processed_users = 0
        failed_users = 0
        updated_rows = 0
        deactivated_rows = 0
        event_id = f"reconcile:{int(now.timestamp())}"

        self._log_start(
            users=len(users),
            mode="targeted" if (self.user_id is not None or self.firebase_id) else "batch",
        )

        with requests.Session() as session:
            session.headers.update(
                {
                    "Authorization": f"Bearer {self.api_key}",
                    "Accept": "application/json",
                }
            )

            for user in users:
                firebase_uid = (user.firebase_id or "").strip()
                if not firebase_uid:
                    continue

                try:
                    payload = self._fetch_subscriber_payload(session, firebase_uid)
                    snapshots = self._build_entitlement_snapshots(payload, now=now)
                    updated, deactivated = self._reconcile_user_rows(
                        db_session,
                        user=user,
                        snapshots=snapshots,
                        event_id=event_id,
                        event_at=now,
                    )
                    processed_users += 1
                    updated_rows += updated
                    deactivated_rows += deactivated
                except Exception as exc:
                    failed_users += 1
                    self.logger.exception(
                        "revenuecat-entitlements-reconcile user_error user_id=%s firebase_id=%s err=%s",
                        user.id,
                        firebase_uid,
                        exc,
                    )

        try:
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

        self._log_end(
            processed_users=processed_users,
            failed_users=failed_users,
            updated_rows=updated_rows,
            deactivated_rows=deactivated_rows,
        )

    def _resolve_target_users(self, db_session: Session) -> list[Users]:
        if self.user_id is not None:
            user = db_session.scalar(select(Users).where(Users.id == int(self.user_id)))
            return [user] if user else []

        if self.firebase_id:
            normalized = self.firebase_id.strip()
            if not normalized:
                return []
            user = db_session.scalar(select(Users).where(Users.firebase_id == normalized))
            return [user] if user else []

        cutoff = _utcnow() - dt.timedelta(days=self.lookback_days)
        limit = self.batch_limit if self.batch_limit is not None else self.default_batch_limit
        stmt = (
            select(Users)
            .join(UserEntitlement, UserEntitlement.user_id == Users.id)
            .where(Users.firebase_id.is_not(None))
            .where(
                or_(
                    UserEntitlement.is_active.is_(True),
                    UserEntitlement.updated_at >= cutoff,
                )
            )
            .distinct()
            .order_by(Users.id.asc())
            .limit(int(limit))
        )
        return list(db_session.scalars(stmt).all())

    def _fetch_subscriber_payload(
        self,
        session: requests.Session,
        firebase_uid: str,
    ) -> Optional[dict[str, Any]]:
        path = quote(firebase_uid, safe="")
        url = f"{self.api_base_url}/subscribers/{path}"

        last_response: Optional[requests.Response] = None
        for attempt in range(self.max_retries + 1):
            response = session.get(url, timeout=self.request_timeout_seconds)
            last_response = response

            if response.status_code == 404:
                return None

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_retries:
                backoff_seconds = min(20.0, (2 ** attempt) * 0.6)
                time.sleep(backoff_seconds)
                continue

            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError(f"RevenueCat response was not a JSON object for uid={firebase_uid}")
            return payload

        if last_response is None:
            raise RuntimeError(f"No response received from RevenueCat for uid={firebase_uid}")

        last_response.raise_for_status()
        return None

    def _build_entitlement_snapshots(
        self,
        payload: Optional[dict[str, Any]],
        *,
        now: dt.datetime,
    ) -> list[RevenueCatEntitlementSnapshot]:
        if payload is None:
            return []

        subscriber_raw = payload.get("subscriber")
        subscriber = subscriber_raw if isinstance(subscriber_raw, dict) else {}

        entitlements_raw = subscriber.get("entitlements")
        entitlements = entitlements_raw if isinstance(entitlements_raw, dict) else {}

        subscriptions_raw = subscriber.get("subscriptions")
        subscriptions = subscriptions_raw if isinstance(subscriptions_raw, dict) else {}

        out: list[RevenueCatEntitlementSnapshot] = []

        for raw_id, raw_value in entitlements.items():
            entitlement_id = str(raw_id or "").strip()
            if not entitlement_id:
                continue

            row = raw_value if isinstance(raw_value, dict) else {}
            product_identifier = str(row.get("product_identifier") or "").strip() or None

            subscription_row = (
                subscriptions.get(product_identifier)
                if product_identifier and isinstance(subscriptions, dict)
                else None
            )
            subscription = subscription_row if isinstance(subscription_row, dict) else {}

            expires_at = _parse_iso(row.get("grace_period_expires_date")) or _parse_iso(
                row.get("expires_date")
            )
            purchased_at = _parse_iso(row.get("purchase_date")) or _parse_iso(
                row.get("original_purchase_date")
            )

            if purchased_at is None:
                purchased_at = _parse_iso(subscription.get("purchase_date")) or _parse_iso(
                    subscription.get("original_purchase_date")
                )

            is_active = expires_at is None or expires_at > now
            store = str(row.get("store") or subscription.get("store") or "").strip() or None
            ownership_type = (
                str(row.get("ownership_type") or subscription.get("ownership_type") or "").strip() or None
            )

            out.append(
                RevenueCatEntitlementSnapshot(
                    entitlement_id=entitlement_id,
                    is_active=is_active,
                    product_identifier=product_identifier,
                    store=store,
                    ownership_type=ownership_type,
                    environment=None,
                    purchased_at=purchased_at,
                    expires_at=expires_at,
                )
            )

        return out

    def _reconcile_user_rows(
        self,
        db_session: Session,
        *,
        user: Users,
        snapshots: list[RevenueCatEntitlementSnapshot],
        event_id: str,
        event_at: dt.datetime,
    ) -> tuple[int, int]:
        existing_rows = db_session.scalars(
            select(UserEntitlement).where(UserEntitlement.user_id == user.id)
        ).all()
        existing_by_id = {row.entitlement_id: row for row in existing_rows}

        updated_rows = 0
        deactivated_rows = 0
        seen_ids: set[str] = set()

        for snapshot in snapshots:
            seen_ids.add(snapshot.entitlement_id)

            row = existing_by_id.get(snapshot.entitlement_id)
            if row is None:
                row = UserEntitlement(user_id=user.id, entitlement_id=snapshot.entitlement_id)
                db_session.add(row)

            row.is_active = snapshot.is_active
            row.product_identifier = snapshot.product_identifier
            row.store = snapshot.store
            row.ownership_type = snapshot.ownership_type
            row.environment = snapshot.environment
            row.purchased_at = snapshot.purchased_at
            row.expires_at = snapshot.expires_at
            row.latest_event_type = "RECONCILE"
            row.latest_event_id = event_id
            row.latest_event_at = event_at
            row.updated_at = event_at
            updated_rows += 1

        for row in existing_rows:
            if row.entitlement_id in seen_ids:
                continue
            if row.is_active:
                deactivated_rows += 1
            row.is_active = False
            row.latest_event_type = "RECONCILE_MISSING"
            row.latest_event_id = event_id
            row.latest_event_at = event_at
            row.updated_at = event_at

        return updated_rows, deactivated_rows
