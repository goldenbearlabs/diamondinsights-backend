from __future__ import annotations

import logging
import os
from typing import Optional, Dict, Any, Tuple

import requests
from fastapi import APIRouter, Depends, HTTPException, Response, status
from redis import Redis
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from shared.db.database import get_db
from shared.db.models import Users, ShowProfile, ShowProfileOnlineStats
from src.api.routes.users import firebase_claims
from src.core.cache import build_cache_key, get_cache_client, get_cached_json, set_cached_json

from .common import _utcnow, _to_int, _to_float
from .models import LinkShowBody, ShowProfileOut
from shared.core.show_api import build_show_search_request


router = APIRouter()
public_router = APIRouter()
logger = logging.getLogger(__name__)


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(float(raw))
    except ValueError:
        return default
    return value if value > 0 else default


_SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC = _read_positive_int_env("SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC", 21600)
_SHOW_PROFILE_PAGE_CLIENT_CACHE_TTL_SEC = _read_positive_int_env("SHOW_PROFILE_PAGE_CLIENT_CACHE_TTL_SEC", 21600)


def _normalize_username(value: str) -> str:
    return str(value or "").strip().lower()


def _set_profile_http_cache_headers(response: Response, *, is_public: bool) -> None:
    max_age = max(1, _SHOW_PROFILE_PAGE_CLIENT_CACHE_TTL_SEC)
    scope = "public" if is_public else "private"
    response.headers["Cache-Control"] = f"{scope}, max-age={max_age}, stale-while-revalidate=60"
    if not is_public:
        response.headers["Vary"] = "Authorization, Cookie"


def _fetch_show_profile(username: str) -> Tuple[dict, dict]:
    url, params, headers = build_show_search_request(username)
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
    except requests.RequestException:
        logger.exception("show profile fetch transport failure username=%s url=%s", username, url)
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to reach The Show API")

    if r.status_code != 200:
        body_sample = (r.text or "")[:200].replace("\n", " ").strip()
        logger.warning(
            "show profile fetch upstream error username=%s url=%s status=%s body_sample=%s",
            username,
            url,
            r.status_code,
            body_sample,
        )
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="The Show API returned an error")

    try:
        data = r.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="The Show API returned invalid JSON")

    profiles = data.get("universal_profiles") or []
    if not profiles:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")

    return profiles[0], data


def _get_authed_user(db: Session, claims: dict) -> Users:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return user


def _get_profile_for_user(db: Session, user_id: int) -> Optional[ShowProfile]:
    return db.scalar(
        select(ShowProfile)
        .options(selectinload(ShowProfile.online_stats))
        .where(ShowProfile.user_id == user_id)
    )


def _get_profile_by_username(db: Session, username: str) -> Optional[ShowProfile]:
    return db.scalar(
        select(ShowProfile)
        .options(selectinload(ShowProfile.online_stats))
        .where(func.lower(ShowProfile.username) == func.lower(username))
    )


def _ensure_username_unclaimed(db: Session, username: str, current_user_id: int) -> None:
    existing = db.scalar(
        select(ShowProfile).where(func.lower(ShowProfile.username) == func.lower(username))
    )
    if existing and existing.user_id not in (None, current_user_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already linked by another user")


def _parsed_online_stats(profile_payload: dict) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for row in (profile_payload.get("online_data") or []):
        yr = _to_int(row.get("year"))
        if yr is None:
            continue

        losses_val = row.get("losses")
        if losses_val is None:
            losses_val = row.get("loses")

        out[yr] = {
            "wins": _to_int(row.get("wins")),
            "losses": _to_int(losses_val),
            "hr": _to_int(row.get("hr")),
            "runs_per_game": _to_float(row.get("runs_per_game")),
            "stolen_bases": _to_int(row.get("stolen_bases")),
            "batting_average": _to_float(row.get("batting_average")),
            "era": _to_float(row.get("era")),
            "k_per_9": _to_float(row.get("k_per_9")),
            "whip": _to_float(row.get("whip")),
        }
    return out


def _apply_online_stats(sp: ShowProfile, incoming: Dict[int, Dict[str, Any]]) -> None:
    existing_by_year = {s.year: s for s in (sp.online_stats or [])}
    keep_years = set(incoming.keys())

    for year, vals in incoming.items():
        row = existing_by_year.get(year)
        if row:
            row.wins = vals["wins"]
            row.losses = vals["losses"]
            row.hr = vals["hr"]
            row.runs_per_game = vals["runs_per_game"]
            row.stolen_bases = vals["stolen_bases"]
            row.batting_average = vals["batting_average"]
            row.era = vals["era"]
            row.k_per_9 = vals["k_per_9"]
            row.whip = vals["whip"]
        else:
            sp.online_stats.append(ShowProfileOnlineStats(year=year, **vals))

    sp.online_stats = [s for s in sp.online_stats if s.year in keep_years]


def _upsert_show_profile(db: Session, user: Users, username: str, profile_payload: dict, raw: dict) -> ShowProfile:
    vanity = profile_payload.get("vanity") or {}
    now = _utcnow()

    existing_for_user = _get_profile_for_user(db, user.id)
    existing_by_username = db.scalar(
        select(ShowProfile).where(func.lower(ShowProfile.username) == func.lower(username))
    )

    if existing_for_user and existing_for_user.username.lower() != username.lower():
        existing_for_user.user_id = None
        existing_for_user.claimed_at = None
        existing_for_user = None

    sp = existing_by_username or existing_for_user

    if not sp:
        sp = ShowProfile(
            user_id=user.id,
            username=username,
            first_seen_at=now,
            claimed_at=now,
            display_level=_to_int(profile_payload.get("display_level")),
            games_played=_to_int(profile_payload.get("games_played")),
            nameplate_equipped=vanity.get("nameplate_equipped"),
            icon_equipped=vanity.get("icon_equipped"),
            raw_json=raw,
            last_refreshed_at=now,
        )
        db.add(sp)
        _apply_online_stats(sp, _parsed_online_stats(profile_payload))
        return sp

    if sp.user_id is None:
        sp.user_id = user.id
        sp.claimed_at = now

    sp.display_level = _to_int(profile_payload.get("display_level"))
    sp.games_played = _to_int(profile_payload.get("games_played"))
    sp.nameplate_equipped = vanity.get("nameplate_equipped")
    sp.icon_equipped = vanity.get("icon_equipped")
    sp.raw_json = raw
    if sp.first_seen_at is None:
        sp.first_seen_at = now
    sp.last_refreshed_at = now
    _apply_online_stats(sp, _parsed_online_stats(profile_payload))
    return sp


@router.post("/link", response_model=ShowProfileOut)
def link_show_username(
    body: LinkShowBody,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowProfileOut:
    user = _get_authed_user(db, claims)
    username = body.username.strip()
    if not username:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username is required")

    _ensure_username_unclaimed(db, username, user.id)
    profile_payload, raw = _fetch_show_profile(username)

    sp = _upsert_show_profile(db, user, username, profile_payload, raw)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing = db.scalar(
            select(ShowProfile).where(func.lower(ShowProfile.username) == func.lower(username))
        )
        if existing and existing.user_id not in (None, user.id):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already linked")
        raise

    db.refresh(sp)
    return ShowProfileOut.from_orm_profile(sp)


@router.post("/refresh", response_model=ShowProfileOut)
def refresh_show_profile(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowProfileOut:
    user = _get_authed_user(db, claims)
    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    profile_payload, raw = _fetch_show_profile(sp.username)
    sp = _upsert_show_profile(db, user, sp.username, profile_payload, raw)
    db.commit()
    db.refresh(sp)
    return ShowProfileOut.from_orm_profile(sp)


@router.get("", response_model=ShowProfileOut)
def get_show_profile(
    response: Response,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowProfileOut:
    _set_profile_http_cache_headers(response, is_public=False)
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    cache_key = build_cache_key(
        "show",
        "profile",
        "me",
        "v1",
        _normalize_username(sp.username),
        str((claims or {}).get("uid") or ""),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowProfileOut.model_validate(cached)

    payload = ShowProfileOut.from_orm_profile(sp)
    set_cached_json(
        cache,
        cache_key,
        payload.model_dump(mode="json"),
        ttl_sec=max(1, _SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC),
    )
    return payload


@public_router.get("/show/{username}", response_model=ShowProfileOut)
def get_show_profile_by_username(
    username: str,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowProfileOut:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")

    cache_key = build_cache_key(
        "show",
        "profile",
        "username",
        "v1",
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowProfileOut.model_validate(cached)

    payload = ShowProfileOut.from_orm_profile(sp)
    set_cached_json(
        cache,
        cache_key,
        payload.model_dump(mode="json"),
        ttl_sec=max(1, _SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC),
    )
    return payload


@public_router.get("/{user_id}/show", response_model=ShowProfileOut)
def get_show_profile_for_user(
    user_id: int,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowProfileOut:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")

    cache_key = build_cache_key(
        "show",
        "profile",
        "user-id",
        "v1",
        user_id,
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowProfileOut.model_validate(cached)

    payload = ShowProfileOut.from_orm_profile(sp)
    set_cached_json(
        cache,
        cache_key,
        payload.model_dump(mode="json"),
        ttl_sec=max(1, _SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC),
    )
    return payload
