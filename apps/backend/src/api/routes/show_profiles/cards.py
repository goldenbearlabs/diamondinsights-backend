from __future__ import annotations

import os
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import ValidationError
from redis import Redis
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Player
from src.api.routes.users import firebase_claims
from src.core.cache import build_cache_key, get_cache_client, get_cached_json, set_cached_json

from .common import _load_facts_df_for_username, _user_masks
from .analytics import (
    _aggregate_stats_for_df,
    _aggregate_pitching_stats_for_df,
    _compute_strikeout_stats,
    _compute_hit_data_stats_for_df,
)
from .models import ShowCardStatsOut, ShowCardPitchingStatsOut
from .profile import _get_authed_user, _get_profile_for_user, _get_profile_by_username


router = APIRouter()
public_router = APIRouter()


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


def _cache_ttl_sec() -> int:
    return max(1, _SHOW_PROFILE_PAGE_REDIS_CACHE_TTL_SEC)


def _card_stats_for_username(db: Session, username: str) -> List[ShowCardStatsOut]:
    df = _load_facts_df_for_username(username)
    user_hitting, _, _ = _user_masks(df, username)
    user_df = df[user_hitting]
    batter_col = user_df.get("batter_mlb_id")
    if batter_col is None:
        return []

    batter_ids = pd.to_numeric(batter_col, errors="coerce")
    user_df = user_df[batter_ids.notna()]
    if user_df.empty:
        return []

    user_df = user_df.copy()
    user_df["_batter_id"] = batter_ids[user_df.index].astype(int)

    grouped = user_df.groupby("_batter_id", sort=False)
    mlb_ids = [int(pid) for pid in grouped.groups.keys() if int(pid) > 0]
    if not mlb_ids:
        return []

    players = db.query(Player).filter(Player.mlb_id.in_(mlb_ids)).all()
    players_by_id = {p.mlb_id: p for p in players}

    rows: List[ShowCardStatsOut] = []
    for mlb_id, sub in grouped:
        mlb_id = int(mlb_id)
        if mlb_id <= 0:
            continue
        stats = _aggregate_stats_for_df(sub)
        strikeout_stats = _compute_strikeout_stats(sub)
        strikeout_stats.pop("k_pct", None)
        hit_stats = _compute_hit_data_stats_for_df(sub)
        player = players_by_id.get(mlb_id)
        rows.append(
            ShowCardStatsOut(
                mlb_id=mlb_id,
                full_name=player.full_name if player else None,
                first_name=player.first_name if player else None,
                last_name=player.last_name if player else None,
                **stats.dict(),
                **strikeout_stats,
                **hit_stats,
            )
        )

    rows.sort(key=lambda row: row.pa, reverse=True)
    return rows


def _pitcher_card_stats_for_username(db: Session, username: str) -> List[ShowCardPitchingStatsOut]:
    df = _load_facts_df_for_username(username)
    _, user_pitching, _ = _user_masks(df, username)
    user_df = df[user_pitching]
    pitcher_col = user_df.get("pitcher_mlb_id")
    if pitcher_col is None:
        return []

    pitcher_ids = pd.to_numeric(pitcher_col, errors="coerce")
    user_df = user_df[pitcher_ids.notna()]
    if user_df.empty:
        return []

    user_df = user_df.copy()
    user_df["_pitcher_id"] = pitcher_ids[user_df.index].astype(int)

    grouped = user_df.groupby("_pitcher_id", sort=False)
    mlb_ids = [int(pid) for pid in grouped.groups.keys() if int(pid) > 0]
    if not mlb_ids:
        return []

    players = db.query(Player).filter(Player.mlb_id.in_(mlb_ids)).all()
    players_by_id = {p.mlb_id: p for p in players}

    rows: List[ShowCardPitchingStatsOut] = []
    for mlb_id, sub in grouped:
        mlb_id = int(mlb_id)
        if mlb_id <= 0:
            continue
        stats = _aggregate_pitching_stats_for_df(sub)
        strikeout_stats = _compute_strikeout_stats(sub)
        strikeout_stats.pop("k_pct", None)
        hit_stats = _compute_hit_data_stats_for_df(sub)
        player = players_by_id.get(mlb_id)
        rows.append(
            ShowCardPitchingStatsOut(
                mlb_id=mlb_id,
                full_name=player.full_name if player else None,
                first_name=player.first_name if player else None,
                last_name=player.last_name if player else None,
                **stats,
                **strikeout_stats,
                **hit_stats,
            )
        )

    rows.sort(key=lambda row: row.pa, reverse=True)
    return rows


@router.get("/cards", response_model=List[ShowCardStatsOut])
def get_show_cards(
    response: Response,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardStatsOut]:
    _set_profile_http_cache_headers(response, is_public=False)
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    cache_key = build_cache_key(
        "show",
        "cards",
        "hitting",
        "me",
        "v1",
        _normalize_username(sp.username),
        str((claims or {}).get("uid") or ""),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload


@router.get("/cards/pitching", response_model=List[ShowCardPitchingStatsOut])
def get_show_pitching_cards(
    response: Response,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardPitchingStatsOut]:
    _set_profile_http_cache_headers(response, is_public=False)
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    cache_key = build_cache_key(
        "show",
        "cards",
        "pitching",
        "me",
        "v1",
        _normalize_username(sp.username),
        str((claims or {}).get("uid") or ""),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardPitchingStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _pitcher_card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload


@public_router.get("/show/{username}/cards", response_model=List[ShowCardStatsOut])
def get_show_cards_by_username(
    username: str,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardStatsOut]:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    cache_key = build_cache_key(
        "show",
        "cards",
        "hitting",
        "username",
        "v1",
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload


@public_router.get("/show/{username}/cards/pitching", response_model=List[ShowCardPitchingStatsOut])
def get_show_pitching_cards_by_username(
    username: str,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardPitchingStatsOut]:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    cache_key = build_cache_key(
        "show",
        "cards",
        "pitching",
        "username",
        "v1",
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardPitchingStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _pitcher_card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload


@public_router.get("/{user_id}/show/cards", response_model=List[ShowCardStatsOut])
def get_show_cards_for_user(
    user_id: int,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardStatsOut]:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    cache_key = build_cache_key(
        "show",
        "cards",
        "hitting",
        "user-id",
        "v1",
        user_id,
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload


@public_router.get("/{user_id}/show/cards/pitching", response_model=List[ShowCardPitchingStatsOut])
def get_show_pitching_cards_for_user(
    user_id: int,
    response: Response,
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> List[ShowCardPitchingStatsOut]:
    _set_profile_http_cache_headers(response, is_public=True)
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    cache_key = build_cache_key(
        "show",
        "cards",
        "pitching",
        "user-id",
        "v1",
        user_id,
        _normalize_username(sp.username),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        items = cached.get("items")
        if isinstance(items, list):
            try:
                return [ShowCardPitchingStatsOut.model_validate(item) for item in items]
            except (TypeError, ValueError, ValidationError):
                pass

    payload = _pitcher_card_stats_for_username(db, sp.username)
    set_cached_json(
        cache,
        cache_key,
        {"items": [item.model_dump(mode="json") for item in payload]},
        ttl_sec=_cache_ttl_sec(),
    )
    return payload
