from __future__ import annotations

import datetime
import hmac
import os
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import RevenueCatWebhookEvent, UserEntitlement, Users
from src.api.routes.users import firebase_claims

PRO_ENTITLEMENT_ID = (os.getenv("REVENUECAT_PRO_ENTITLEMENT_ID") or "pro").strip() or "pro"
REVENUECAT_WEBHOOK_AUTH = (os.getenv("REVENUECAT_WEBHOOK_AUTH") or "").strip()

router = APIRouter(prefix="/entitlements", tags=["entitlements"])
webhook_router = APIRouter(prefix="/billing", tags=["billing"])


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_ms(value: Any) -> Optional[datetime.datetime]:
    if value is None:
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.datetime.fromtimestamp(ms / 1000.0, tz=datetime.timezone.utc)


def _parse_iso(value: Any) -> Optional[datetime.datetime]:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    return dt.astimezone(datetime.timezone.utc)


def _event_datetime(event: dict, *, ms_key: str, iso_key: str) -> Optional[datetime.datetime]:
    return _parse_ms(event.get(ms_key)) or _parse_iso(event.get(iso_key))


def _normalize_user_ids(values: list[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        if not isinstance(raw, str):
            continue
        value = raw.strip()
        if not value or value.startswith("$RCAnonymousID:"):
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _extract_candidate_user_ids(event: dict) -> list[str]:
    raw_ids: list[Any] = [event.get("app_user_id"), event.get("original_app_user_id")]

    aliases = event.get("aliases")
    if isinstance(aliases, list):
        raw_ids.extend(aliases)

    transferred_to = event.get("transferred_to")
    if isinstance(transferred_to, list):
        raw_ids.extend(transferred_to)

    return _normalize_user_ids(raw_ids)


def _extract_target_user_ids(event: dict, event_type: str) -> list[str]:
    candidate_user_ids = _extract_candidate_user_ids(event)
    if event_type != "TRANSFER":
        return candidate_user_ids

    transferred_to = event.get("transferred_to")
    transferred_to_ids = _normalize_user_ids(transferred_to if isinstance(transferred_to, list) else [])

    ordered: list[str] = []
    seen: set[str] = set()
    for uid in transferred_to_ids + candidate_user_ids:
        if uid in seen:
            continue
        seen.add(uid)
        ordered.append(uid)
    return ordered


def _extract_transferred_from_ids(event: dict) -> list[str]:
    transferred_from = event.get("transferred_from")
    if isinstance(transferred_from, list):
        return _normalize_user_ids(transferred_from)
    return []


def _extract_entitlement_ids(event: dict) -> list[str]:
    raw_values: list[Any] = []
    ids = event.get("entitlement_ids")
    if isinstance(ids, list):
        raw_values.extend(ids)

    # Older/newer payload variants may send a singular entitlement id.
    raw_values.append(event.get("entitlement_id"))

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        if not isinstance(raw, str):
            continue
        value = raw.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _resolve_user_by_firebase_id(db: Session, firebase_id: str) -> Optional[Users]:
    return db.scalar(select(Users).where(Users.firebase_id == firebase_id))


def _event_implies_active(event_type: str, expires_at: Optional[datetime.datetime]) -> bool:
    if event_type == "EXPIRATION":
        return False
    if expires_at is not None and expires_at <= _utcnow():
        return False
    return True


def _upsert_entitlement(
    db: Session,
    *,
    user_id: int,
    entitlement_id: str,
    event_id: str,
    event_type: str,
    event_at: datetime.datetime,
    is_active: bool,
    product_id: Optional[str],
    store: Optional[str],
    ownership_type: Optional[str],
    environment: Optional[str],
    purchased_at: Optional[datetime.datetime],
    expires_at: Optional[datetime.datetime],
) -> None:
    row = db.scalar(
        select(UserEntitlement).where(
            UserEntitlement.user_id == user_id,
            UserEntitlement.entitlement_id == entitlement_id,
        )
    )

    if row is None:
        row = UserEntitlement(user_id=user_id, entitlement_id=entitlement_id)
        db.add(row)
    elif row.latest_event_at and row.latest_event_at > event_at:
        # Ignore out-of-order events so newer state is preserved.
        return

    row.is_active = is_active
    row.product_identifier = product_id
    row.store = store
    row.ownership_type = ownership_type
    row.environment = environment
    row.purchased_at = purchased_at
    row.expires_at = expires_at if expires_at is not None else row.expires_at
    row.latest_event_id = event_id
    row.latest_event_type = event_type
    row.latest_event_at = event_at
    row.updated_at = _utcnow()


def _validate_webhook_auth(authorization: Optional[str]) -> None:
    if not REVENUECAT_WEBHOOK_AUTH:
        return

    provided = (authorization or "").strip()
    if provided.lower().startswith("bearer "):
        provided = provided.split(" ", 1)[1].strip()

    if not provided or not hmac.compare_digest(provided, REVENUECAT_WEBHOOK_AUTH):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid webhook authorization")


class EntitlementOut(BaseModel):
    entitlement_id: str
    is_active: bool
    product_identifier: Optional[str] = None
    store: Optional[str] = None
    environment: Optional[str] = None
    expires_at: Optional[datetime.datetime] = None
    updated_at: datetime.datetime


class EntitlementsMeOut(BaseModel):
    has_pro: bool
    pro_entitlement_id: str
    pro_expires_at: Optional[datetime.datetime] = None
    entitlements: list[EntitlementOut]


@router.get("/me", response_model=EntitlementsMeOut)
def get_my_entitlements(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> EntitlementsMeOut:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    rows = db.scalars(
        select(UserEntitlement)
        .where(UserEntitlement.user_id == user.id)
        .order_by(UserEntitlement.entitlement_id.asc())
    ).all()

    pro_rows = [r for r in rows if r.entitlement_id == PRO_ENTITLEMENT_ID and r.is_active]
    pro_expires_at = max(
        (r.expires_at for r in pro_rows if r.expires_at is not None),
        default=None,
    )

    return EntitlementsMeOut(
        has_pro=bool(pro_rows),
        pro_entitlement_id=PRO_ENTITLEMENT_ID,
        pro_expires_at=pro_expires_at,
        entitlements=[
            EntitlementOut(
                entitlement_id=row.entitlement_id,
                is_active=row.is_active,
                product_identifier=row.product_identifier,
                store=row.store,
                environment=row.environment,
                expires_at=row.expires_at,
                updated_at=row.updated_at,
            )
            for row in rows
        ],
    )


@webhook_router.post("/revenuecat/webhook")
async def revenuecat_webhook(
    request: Request,
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(default=None),
):
    _validate_webhook_auth(authorization)

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid JSON payload")

    if not isinstance(payload, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid webhook payload")

    event = payload.get("event")
    if not isinstance(event, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing event object")

    event_id = str(event.get("id") or "").strip()
    event_type = str(event.get("type") or "").strip().upper()
    if not event_id or not event_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing event id or type")

    existing = db.get(RevenueCatWebhookEvent, event_id)
    if existing:
        return {"ok": True, "duplicate": True}

    event_at = (
        _parse_ms(event.get("event_timestamp_ms"))
        or _parse_iso(event.get("event_timestamp"))
        or _event_datetime(event, ms_key="purchased_at_ms", iso_key="purchased_at")
        or _utcnow()
    )
    purchased_at = _event_datetime(event, ms_key="purchased_at_ms", iso_key="purchased_at")
    expires_at = _event_datetime(event, ms_key="expiration_at_ms", iso_key="expiration_at")

    entitlement_ids = _extract_entitlement_ids(event)
    product_id = str(event.get("product_id") or "").strip() or None
    store = str(event.get("store") or "").strip() or None
    ownership_type = str(event.get("ownership_type") or "").strip() or None
    environment = str(event.get("environment") or "").strip() or None
    app_user_id = str(event.get("app_user_id") or "").strip() or None
    original_app_user_id = str(event.get("original_app_user_id") or "").strip() or None
    is_active = _event_implies_active(event_type, expires_at)

    webhook_event = RevenueCatWebhookEvent(
        external_event_id=event_id,
        event_type=event_type,
        app_user_id=app_user_id,
        original_app_user_id=original_app_user_id,
        api_version=str(payload.get("api_version") or "").strip() or None,
        product_id=product_id,
        store=store,
        environment=environment,
        event_timestamp=event_at,
        raw_payload=payload,
        received_at=_utcnow(),
    )
    db.add(webhook_event)

    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        return {"ok": True, "duplicate": True}

    target_user: Optional[Users] = None
    for candidate_uid in _extract_target_user_ids(event, event_type):
        target_user = _resolve_user_by_firebase_id(db, candidate_uid)
        if target_user:
            break

    if target_user and entitlement_ids:
        for entitlement_id in entitlement_ids:
            _upsert_entitlement(
                db,
                user_id=target_user.id,
                entitlement_id=entitlement_id,
                event_id=event_id,
                event_type=event_type,
                event_at=event_at,
                is_active=is_active,
                product_id=product_id,
                store=store,
                ownership_type=ownership_type,
                environment=environment,
                purchased_at=purchased_at,
                expires_at=expires_at,
            )

    if event_type == "TRANSFER" and entitlement_ids:
        for from_firebase_id in _extract_transferred_from_ids(event):
            from_user = _resolve_user_by_firebase_id(db, from_firebase_id)
            if not from_user:
                continue
            if target_user and from_user.id == target_user.id:
                continue
            for entitlement_id in entitlement_ids:
                _upsert_entitlement(
                    db,
                    user_id=from_user.id,
                    entitlement_id=entitlement_id,
                    event_id=event_id,
                    event_type=event_type,
                    event_at=event_at,
                    is_active=False,
                    product_id=product_id,
                    store=store,
                    ownership_type=ownership_type,
                    environment=environment,
                    purchased_at=purchased_at,
                    expires_at=expires_at,
                )

    if entitlement_ids and not target_user:
        webhook_event.processing_error = "No matching user found for webhook app_user_id/original_app_user_id/aliases"
        webhook_event.processed = False
        webhook_event.processed_at = _utcnow()
        db.commit()
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={"ok": True, "processed": False, "reason": "no_matching_user"},
        )

    webhook_event.processing_error = None
    webhook_event.processed = True
    webhook_event.processed_at = _utcnow()
    db.commit()
    return {"ok": True, "processed": True}
