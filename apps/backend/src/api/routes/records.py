from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Literal, Optional, TypeVar

import pandas as pd
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel
from redis import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Player, ShowBallParks, ShowGameSummary, ShowProfile, Users
from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from src.api.routes.users import firebase_claims_optional
from src.core.cache import build_cache_key, get_cache_client, get_cached_json, set_cached_json
from src.schemas.records import (
    HardHitRecordResponse,
    HardHitRecordsResponse,
    HomeRunRecordResponse,
    HomeRunRecordsResponse,
    RecordMode,
)

router = APIRouter(prefix="/records", tags=["records"])


def _read_positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


HOME_RUNS_RECORDS_KEY = "records/home_runs.parquet"
HARDEST_HITS_RECORDS_KEY = "records/hardest_hits.parquet"
DIFFICULTY_ORDER = ["goat", "legend", "hall of fame", "all star", "veteran", "rookie"]
TimeRange = Literal["24h", "1w", "1m", "all"]
RECORDS_LEADERBOARD_CACHE_TTL_SEC = int(_read_positive_float_env("RECORDS_LEADERBOARD_CACHE_TTL_SEC", 14400))
RECORDS_SPACES_CLIENT_TTL_SEC = _read_positive_float_env("RECORDS_SPACES_CLIENT_TTL_SEC", 1800)
RECORDS_CLIENT_HTTP_CACHE_TTL_SEC = int(_read_positive_float_env("RECORDS_CLIENT_HTTP_CACHE_TTL_SEC", 1800))
_SPACES_CACHE_LOCK = threading.Lock()
_spaces_connector_cache: tuple[float, SpacesConnector] | None = None
_SPACES_NETWORK_CACHE_LOCK = threading.Lock()
_spaces_bytes_cache: dict[str, tuple[float, bytes]] = {}
ResponseModelT = TypeVar("ResponseModelT", bound=BaseModel)


@router.get("/home-runs", response_model=HomeRunRecordsResponse)
def get_home_run_records(
    response: Response,
    mode: RecordMode = Query(
        default="normal",
        description="Sort mode: normal uses distance_ft, plus uses distance_plus_ft.",
    ),
    difficulty: Optional[str] = Query(
        default=None,
        description="Exact difficulty filter (case-insensitive).",
    ),
    hitter_username: Optional[str] = Query(
        default=None,
        description="Partial hitter username match (case-insensitive).",
    ),
    pitcher_username: Optional[str] = Query(
        default=None,
        description="Partial pitcher username match (case-insensitive).",
    ),
    profile_username: Optional[str] = Query(
        default=None,
        description="Partial profile username match against home/away profiles (case-insensitive).",
    ),
    hitter_mlb_id: Optional[int] = Query(
        default=None,
        description="Exact hitter MLB player ID.",
    ),
    pitcher_mlb_id: Optional[int] = Query(
        default=None,
        description="Exact pitcher MLB player ID.",
    ),
    time_range: TimeRange = Query(
        default="all",
        description="Relative time filter from now: 24h, 1w, 1m, or all.",
    ),
    limit: int = Query(default=100, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
    cache: Redis | None = Depends(get_cache_client),
) -> HomeRunRecordsResponse:
    _set_client_cache_headers(response)

    cache_key = build_cache_key(
        "records",
        "home-runs",
        "v1",
        mode,
        _normalize_cache_text(difficulty),
        _normalize_cache_text(hitter_username),
        _normalize_cache_text(pitcher_username),
        _normalize_cache_text(profile_username),
        hitter_mlb_id if hitter_mlb_id is not None else "",
        pitcher_mlb_id if pitcher_mlb_id is not None else "",
        time_range,
        limit,
        offset,
        _claims_uid(claims) or "anon",
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return HomeRunRecordsResponse.model_validate(cached)

    try:
        raw = _get_spaces_bytes_cached(HOME_RUNS_RECORDS_KEY)
    except KeyError as exc:
        raise HTTPException(status_code=503, detail=f"Spaces configuration missing: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Unable to load home run records from storage") from exc

    if not raw:
        return _cache_records_response(cache, cache_key, HomeRunRecordsResponse(
            items=[],
            available_difficulties=[],
            my_top_hr_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
            mode=mode,
        ))

    try:
        table = pq.read_table(BytesIO(raw))
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to parse records parquet") from exc

    df = table.to_pandas()
    if df.empty:
        return _cache_records_response(cache, cache_key, HomeRunRecordsResponse(
            items=[],
            available_difficulties=[],
            my_top_hr_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
            mode=mode,
        ))

    for col in ("distance_ft", "distance_plus_ft", "elevation"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in (
        "event_id",
        "batter_mlb_id",
        "pitcher_mlb_id",
        "rank",
        "difficulty_rank",
        "rank_plus",
        "difficulty_rank_plus",
    ):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in (
        "difficulty",
        "hitter_username",
        "pitcher_username",
        "home_profile_username",
        "away_profile_username",
        "date",
        "game_id",
    ):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    available_difficulties = _order_difficulties(
        {
            str(v).strip()
            for v in df["difficulty"].tolist()
            if isinstance(v, str) and str(v).strip()
        }
    )

    distance_col = "distance_plus_ft" if mode == "plus" else "distance_ft"
    difficulty_rank_col = "difficulty_rank_plus" if mode == "plus" else "difficulty_rank"

    base_df = df[df[distance_col].notna()].copy()
    if base_df.empty:
        return _cache_records_response(cache, cache_key, HomeRunRecordsResponse(
            items=[],
            available_difficulties=available_difficulties,
            my_top_hr_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
            mode=mode,
        ))

    overall_rank_lookup = _competition_rank_lookup(df=base_df, value_col=distance_col)

    filtered_df = base_df
    if difficulty:
        needle = difficulty.strip().lower()
        if needle:
            filtered_df = filtered_df[filtered_df["difficulty"].str.lower() == needle]

    if hitter_username:
        needle = hitter_username.strip()
        if needle:
            filtered_df = filtered_df[
                filtered_df["hitter_username"].str.contains(needle, case=False, regex=False, na=False)
            ]

    if pitcher_username:
        needle = pitcher_username.strip()
        if needle:
            filtered_df = filtered_df[
                filtered_df["pitcher_username"].str.contains(needle, case=False, regex=False, na=False)
            ]

    if profile_username:
        needle = profile_username.strip()
        if needle:
            mask_home = filtered_df["home_profile_username"].str.contains(needle, case=False, regex=False, na=False)
            mask_away = filtered_df["away_profile_username"].str.contains(needle, case=False, regex=False, na=False)
            filtered_df = filtered_df[mask_home | mask_away]

    if hitter_mlb_id is not None:
        filtered_df = filtered_df[filtered_df["batter_mlb_id"] == hitter_mlb_id]

    if pitcher_mlb_id is not None:
        filtered_df = filtered_df[filtered_df["pitcher_mlb_id"] == pitcher_mlb_id]

    parsed_dates = pd.to_datetime(filtered_df["date"], errors="coerce", utc=True)
    if time_range != "all":
        now_utc = datetime.now(timezone.utc)
        if time_range == "24h":
            cutoff = now_utc - timedelta(hours=24)
        elif time_range == "1w":
            cutoff = now_utc - timedelta(days=7)
        else:
            cutoff = now_utc - timedelta(days=30)
        filtered_df = filtered_df[parsed_dates.notna() & (parsed_dates >= cutoff)]
        parsed_dates = parsed_dates.loc[filtered_df.index]

    filtered_df = filtered_df.copy()
    if filtered_df.empty:
        return _cache_records_response(cache, cache_key, HomeRunRecordsResponse(
            items=[],
            available_difficulties=available_difficulties,
            my_top_hr_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
            mode=mode,
        ))

    filtered_rank_lookup = _competition_rank_lookup(df=filtered_df, value_col=distance_col)

    filtered_df["_parsed_date"] = parsed_dates
    filtered_df = filtered_df.sort_values(
        by=[distance_col, "_parsed_date", "game_id"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    my_top_hr_ovr_rank = _resolve_my_top_rank(
        df=filtered_df, overall_rank_lookup=overall_rank_lookup, db=db, claims=claims
    )

    total = int(len(filtered_df))
    page_df = filtered_df.iloc[offset : offset + limit].copy()

    batter_ids = pd.to_numeric(page_df["batter_mlb_id"], errors="coerce").dropna().astype("int64")
    pitcher_ids = pd.to_numeric(page_df["pitcher_mlb_id"], errors="coerce").dropna().astype("int64")
    player_ids = {int(v) for v in batter_ids.tolist()} | {int(v) for v in pitcher_ids.tolist()}

    name_by_mlb_id: dict[int, str] = {}
    if player_ids:
        rows = db.execute(
            select(Player.mlb_id, Player.full_name).where(Player.mlb_id.in_(player_ids))
        ).all()
        name_by_mlb_id = {int(player_id): full_name for player_id, full_name in rows}

    game_ids = {
        str(v).strip()
        for v in page_df["game_id"].tolist()
        if v is not None and str(v).strip()
    }
    ball_park_name_by_game_id: dict[str, str] = {}
    if game_ids:
        park_rows = db.execute(
            select(ShowGameSummary.id, ShowBallParks.name)
            .select_from(ShowGameSummary)
            .join(ShowBallParks, ShowBallParks.id == ShowGameSummary.ball_park_id, isouter=True)
            .where(ShowGameSummary.id.in_(game_ids))
        ).all()
        ball_park_name_by_game_id = {
            str(game_id): str(park_name)
            for game_id, park_name in park_rows
            if game_id is not None and park_name
        }

    items: list[HomeRunRecordResponse] = []
    for row in page_df.to_dict(orient="records"):
        batter_id = _to_int(row.get("batter_mlb_id"))
        pitcher_id = _to_int(row.get("pitcher_mlb_id"))

        items.append(
            HomeRunRecordResponse(
                game_id=str(row.get("game_id") or ""),
                event_id=_to_int(row.get("event_id")),
                date=_to_opt_str(row.get("date")),
                difficulty=_to_opt_str(row.get("difficulty")),
                home_profile_username=_to_opt_str(row.get("home_profile_username")),
                away_profile_username=_to_opt_str(row.get("away_profile_username")),
                hitter_username=_to_opt_str(row.get("hitter_username")),
                pitcher_username=_to_opt_str(row.get("pitcher_username")),
                batter_mlb_id=batter_id,
                pitcher_mlb_id=pitcher_id,
                hitter_name=name_by_mlb_id.get(batter_id) if batter_id is not None else None,
                pitcher_name=name_by_mlb_id.get(pitcher_id) if pitcher_id is not None else None,
                ball_park_name=ball_park_name_by_game_id.get(str(row.get("game_id") or "")),
                is_home_batting=_to_bool(row.get("is_home_batting")),
                elevation=_to_float(row.get("elevation")),
                distance_ft=_to_float(row.get("distance_ft")),
                distance_plus_ft=_to_float(row.get("distance_plus_ft")),
                rank=_to_int(row.get("rank")),
                difficulty_rank=_to_int(row.get("difficulty_rank")),
                rank_plus=_to_int(row.get("rank_plus")),
                difficulty_rank_plus=_to_int(row.get("difficulty_rank_plus")),
                selected_distance_ft=_to_float(row.get(distance_col)),
                filtered_rank=filtered_rank_lookup.get(_row_key_from_row(row)),
                selected_rank=overall_rank_lookup.get(_row_key_from_row(row)),
                selected_difficulty_rank=_to_int(row.get(difficulty_rank_col)),
            )
        )

    return _cache_records_response(cache, cache_key, HomeRunRecordsResponse(
        items=items,
        available_difficulties=available_difficulties,
        my_top_hr_ovr_rank=my_top_hr_ovr_rank,
        total=total,
        limit=limit,
        offset=offset,
        mode=mode,
    ))


@router.get("/hardest-hits", response_model=HardHitRecordsResponse)
def get_hardest_hit_records(
    response: Response,
    difficulty: Optional[str] = Query(
        default=None,
        description="Exact difficulty filter (case-insensitive).",
    ),
    hitter_username: Optional[str] = Query(
        default=None,
        description="Partial hitter username match (case-insensitive).",
    ),
    pitcher_username: Optional[str] = Query(
        default=None,
        description="Partial pitcher username match (case-insensitive).",
    ),
    profile_username: Optional[str] = Query(
        default=None,
        description="Partial profile username match against home/away profiles (case-insensitive).",
    ),
    hitter_mlb_id: Optional[int] = Query(
        default=None,
        description="Exact hitter MLB player ID.",
    ),
    pitcher_mlb_id: Optional[int] = Query(
        default=None,
        description="Exact pitcher MLB player ID.",
    ),
    time_range: TimeRange = Query(
        default="all",
        description="Relative time filter from now: 24h, 1w, 1m, or all.",
    ),
    limit: int = Query(default=100, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
    cache: Redis | None = Depends(get_cache_client),
) -> HardHitRecordsResponse:
    _set_client_cache_headers(response)

    cache_key = build_cache_key(
        "records",
        "hardest-hits",
        "v1",
        _normalize_cache_text(difficulty),
        _normalize_cache_text(hitter_username),
        _normalize_cache_text(pitcher_username),
        _normalize_cache_text(profile_username),
        hitter_mlb_id if hitter_mlb_id is not None else "",
        pitcher_mlb_id if pitcher_mlb_id is not None else "",
        time_range,
        limit,
        offset,
        _claims_uid(claims) or "anon",
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return HardHitRecordsResponse.model_validate(cached)

    try:
        raw = _get_spaces_bytes_cached(HARDEST_HITS_RECORDS_KEY)
    except KeyError as exc:
        raise HTTPException(status_code=503, detail=f"Spaces configuration missing: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail="Unable to load hard hit records from storage") from exc

    if not raw:
        return _cache_records_response(cache, cache_key, HardHitRecordsResponse(
            items=[],
            available_difficulties=[],
            my_top_hit_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
        ))

    try:
        table = pq.read_table(BytesIO(raw))
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to parse records parquet") from exc

    df = table.to_pandas()
    if df.empty:
        return _cache_records_response(cache, cache_key, HardHitRecordsResponse(
            items=[],
            available_difficulties=[],
            my_top_hit_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
        ))

    for col in ("exit_vel_mph",):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in (
        "event_id",
        "batter_mlb_id",
        "pitcher_mlb_id",
        "rank",
        "difficulty_rank",
    ):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in (
        "difficulty",
        "hitter_username",
        "pitcher_username",
        "home_profile_username",
        "away_profile_username",
        "date",
        "game_id",
    ):
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str)

    available_difficulties = _order_difficulties(
        {
            str(v).strip()
            for v in df["difficulty"].tolist()
            if isinstance(v, str) and str(v).strip()
        }
    )

    value_col = "exit_vel_mph"
    difficulty_rank_col = "difficulty_rank"

    base_df = df[df[value_col].notna()].copy()
    if base_df.empty:
        return _cache_records_response(cache, cache_key, HardHitRecordsResponse(
            items=[],
            available_difficulties=available_difficulties,
            my_top_hit_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
        ))

    overall_rank_lookup = _competition_rank_lookup(df=base_df, value_col=value_col)

    filtered_df = base_df
    if difficulty:
        needle = difficulty.strip().lower()
        if needle:
            filtered_df = filtered_df[filtered_df["difficulty"].str.lower() == needle]

    if hitter_username:
        needle = hitter_username.strip()
        if needle:
            filtered_df = filtered_df[
                filtered_df["hitter_username"].str.contains(needle, case=False, regex=False, na=False)
            ]

    if pitcher_username:
        needle = pitcher_username.strip()
        if needle:
            filtered_df = filtered_df[
                filtered_df["pitcher_username"].str.contains(needle, case=False, regex=False, na=False)
            ]

    if profile_username:
        needle = profile_username.strip()
        if needle:
            mask_home = filtered_df["home_profile_username"].str.contains(needle, case=False, regex=False, na=False)
            mask_away = filtered_df["away_profile_username"].str.contains(needle, case=False, regex=False, na=False)
            filtered_df = filtered_df[mask_home | mask_away]

    if hitter_mlb_id is not None:
        filtered_df = filtered_df[filtered_df["batter_mlb_id"] == hitter_mlb_id]

    if pitcher_mlb_id is not None:
        filtered_df = filtered_df[filtered_df["pitcher_mlb_id"] == pitcher_mlb_id]

    parsed_dates = pd.to_datetime(filtered_df["date"], errors="coerce", utc=True)
    if time_range != "all":
        now_utc = datetime.now(timezone.utc)
        if time_range == "24h":
            cutoff = now_utc - timedelta(hours=24)
        elif time_range == "1w":
            cutoff = now_utc - timedelta(days=7)
        else:
            cutoff = now_utc - timedelta(days=30)
        filtered_df = filtered_df[parsed_dates.notna() & (parsed_dates >= cutoff)]
        parsed_dates = parsed_dates.loc[filtered_df.index]

    filtered_df = filtered_df.copy()
    if filtered_df.empty:
        return _cache_records_response(cache, cache_key, HardHitRecordsResponse(
            items=[],
            available_difficulties=available_difficulties,
            my_top_hit_ovr_rank=None,
            total=0,
            limit=limit,
            offset=offset,
        ))

    filtered_rank_lookup = _competition_rank_lookup(df=filtered_df, value_col=value_col)

    filtered_df["_parsed_date"] = parsed_dates
    filtered_df = filtered_df.sort_values(
        by=[value_col, "_parsed_date", "game_id"],
        ascending=[False, False, True],
        kind="mergesort",
    )
    my_top_hit_ovr_rank = _resolve_my_top_rank(
        df=filtered_df, overall_rank_lookup=overall_rank_lookup, db=db, claims=claims
    )

    total = int(len(filtered_df))
    page_df = filtered_df.iloc[offset : offset + limit].copy()

    batter_ids = pd.to_numeric(page_df["batter_mlb_id"], errors="coerce").dropna().astype("int64")
    pitcher_ids = pd.to_numeric(page_df["pitcher_mlb_id"], errors="coerce").dropna().astype("int64")
    player_ids = {int(v) for v in batter_ids.tolist()} | {int(v) for v in pitcher_ids.tolist()}

    name_by_mlb_id: dict[int, str] = {}
    if player_ids:
        rows = db.execute(
            select(Player.mlb_id, Player.full_name).where(Player.mlb_id.in_(player_ids))
        ).all()
        name_by_mlb_id = {int(player_id): full_name for player_id, full_name in rows}

    game_ids = {
        str(v).strip()
        for v in page_df["game_id"].tolist()
        if v is not None and str(v).strip()
    }
    ball_park_name_by_game_id: dict[str, str] = {}
    if game_ids:
        park_rows = db.execute(
            select(ShowGameSummary.id, ShowBallParks.name)
            .select_from(ShowGameSummary)
            .join(ShowBallParks, ShowBallParks.id == ShowGameSummary.ball_park_id, isouter=True)
            .where(ShowGameSummary.id.in_(game_ids))
        ).all()
        ball_park_name_by_game_id = {
            str(game_id): str(park_name)
            for game_id, park_name in park_rows
            if game_id is not None and park_name
        }

    items: list[HardHitRecordResponse] = []
    for row in page_df.to_dict(orient="records"):
        batter_id = _to_int(row.get("batter_mlb_id"))
        pitcher_id = _to_int(row.get("pitcher_mlb_id"))
        items.append(
            HardHitRecordResponse(
                game_id=str(row.get("game_id") or ""),
                event_id=_to_int(row.get("event_id")),
                date=_to_opt_str(row.get("date")),
                difficulty=_to_opt_str(row.get("difficulty")),
                home_profile_username=_to_opt_str(row.get("home_profile_username")),
                away_profile_username=_to_opt_str(row.get("away_profile_username")),
                hitter_username=_to_opt_str(row.get("hitter_username")),
                pitcher_username=_to_opt_str(row.get("pitcher_username")),
                batter_mlb_id=batter_id,
                pitcher_mlb_id=pitcher_id,
                hitter_name=name_by_mlb_id.get(batter_id) if batter_id is not None else None,
                pitcher_name=name_by_mlb_id.get(pitcher_id) if pitcher_id is not None else None,
                ball_park_name=ball_park_name_by_game_id.get(str(row.get("game_id") or "")),
                is_home_batting=_to_bool(row.get("is_home_batting")),
                exit_vel_mph=_to_float(row.get("exit_vel_mph")),
                rank=_to_int(row.get("rank")),
                difficulty_rank=_to_int(row.get("difficulty_rank")),
                selected_exit_vel_mph=_to_float(row.get(value_col)),
                filtered_rank=filtered_rank_lookup.get(_row_key_from_row(row)),
                selected_rank=overall_rank_lookup.get(_row_key_from_row(row)),
                selected_difficulty_rank=_to_int(row.get(difficulty_rank_col)),
            )
        )

    return _cache_records_response(cache, cache_key, HardHitRecordsResponse(
        items=items,
        available_difficulties=available_difficulties,
        my_top_hit_ovr_rank=my_top_hit_ovr_rank,
        total=total,
        limit=limit,
        offset=offset,
    ))


def _get_cached_spaces_connector() -> SpacesConnector:
    global _spaces_connector_cache

    now = time.monotonic()
    with _SPACES_CACHE_LOCK:
        cached = _spaces_connector_cache
        if cached and cached[0] > now:
            return cached[1]

        connector = SpacesConnector(SpacesConfig.from_env())
        _spaces_connector_cache = (now + RECORDS_SPACES_CLIENT_TTL_SEC, connector)
        return connector


def _get_spaces_bytes_cached(key: str) -> bytes:
    now = time.monotonic()
    with _SPACES_NETWORK_CACHE_LOCK:
        cached = _spaces_bytes_cache.get(key)
        if cached and cached[0] > now:
            return cached[1]
        if cached and cached[0] <= now:
            _spaces_bytes_cache.pop(key, None)

    payload = _get_cached_spaces_connector().get_bytes(key)
    with _SPACES_NETWORK_CACHE_LOCK:
        _spaces_bytes_cache[key] = (time.monotonic() + RECORDS_SPACES_CLIENT_TTL_SEC, payload)
    return payload


def _cache_records_response(
    cache: Redis | None,
    key: str,
    response: ResponseModelT,
) -> ResponseModelT:
    set_cached_json(
        cache,
        key,
        response.model_dump(mode="json"),
        ttl_sec=RECORDS_LEADERBOARD_CACHE_TTL_SEC,
    )
    return response


def _normalize_cache_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _set_client_cache_headers(response: Response) -> None:
    max_age = max(1, RECORDS_CLIENT_HTTP_CACHE_TTL_SEC)
    response.headers["Cache-Control"] = f"private, max-age={max_age}, stale-while-revalidate=60"
    response.headers["Vary"] = "Authorization, Cookie"


def _claims_uid(claims: dict) -> str:
    return str((claims or {}).get("uid") or "").strip()


def _to_int(v: object) -> Optional[int]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except TypeError:
        pass
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _to_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except TypeError:
        pass
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _to_opt_str(v: object) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _to_bool(v: object) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    if isinstance(v, (int, float)):
        if v == 1:
            return True
        if v == 0:
            return False
    s = str(v).strip().lower()
    if s in {"true", "t", "yes", "y", "1"}:
        return True
    if s in {"false", "f", "no", "n", "0"}:
        return False
    return None


def _normalize_difficulty(value: str) -> str:
    return value.strip().lower().replace("-", " ")


def _difficulty_sort_key(value: str) -> tuple[int, int, str]:
    normalized = _normalize_difficulty(value)
    try:
        idx = DIFFICULTY_ORDER.index(normalized)
        return (0, idx, normalized)
    except ValueError:
        return (1, 999, normalized)


def _order_difficulties(values: set[str]) -> list[str]:
    return sorted(values, key=_difficulty_sort_key)


def _resolve_my_top_rank(
    *,
    df: pd.DataFrame,
    overall_rank_lookup: dict[tuple[str, int], int],
    db: Session,
    claims: dict,
) -> Optional[int]:
    uid = str(claims.get("uid") or "").strip()
    if not uid:
        return None

    user_id = db.scalar(select(Users.id).where(Users.firebase_id == uid))
    if user_id is None:
        return None

    show_username = db.scalar(select(ShowProfile.username).where(ShowProfile.user_id == user_id))
    if not show_username:
        return None

    hitter_series = df["hitter_username"].fillna("").astype(str).str.strip().str.lower()
    matched = df[hitter_series == str(show_username).strip().lower()]
    if matched.empty:
        return None

    ranks = [
        overall_rank_lookup.get(_row_key_from_row(row))
        for row in matched.to_dict(orient="records")
    ]
    ranks = [r for r in ranks if isinstance(r, int)]
    if not ranks:
        return None

    return int(min(ranks))


def _row_key_from_row(row: dict[str, object]) -> tuple[str, int]:
    game_id = str(row.get("game_id") or "").strip()
    event_id = _to_int(row.get("event_id")) or -1
    return (game_id, event_id)


def _competition_rank_lookup(
    *,
    df: pd.DataFrame,
    value_col: str,
) -> dict[tuple[str, int], int]:
    tmp = df[["game_id", "event_id", value_col]].copy()
    tmp["game_id"] = tmp["game_id"].fillna("").astype(str).str.strip()
    tmp["event_id"] = pd.to_numeric(tmp["event_id"], errors="coerce").fillna(-1).astype("int64")
    values = pd.to_numeric(tmp[value_col], errors="coerce")
    ranks = values.rank(method="min", ascending=False)
    tmp["_rank"] = ranks
    tmp = tmp[tmp["_rank"].notna()].copy()
    tmp["_rank"] = tmp["_rank"].astype("int64")
    return {
        (str(game_id), int(event_id)): int(rank)
        for game_id, event_id, rank in tmp[["game_id", "event_id", "_rank"]].itertuples(index=False, name=None)
    }
