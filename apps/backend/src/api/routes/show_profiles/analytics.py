from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from dataclasses import dataclass
from io import BytesIO
import math
import os
import re
import threading
import time
from typing import Optional, List, Dict, Any, Iterable

import pandas as pd
import pyarrow.parquet as pq
from fastapi import APIRouter, Depends, HTTPException, Response, status, Query
from redis import Redis
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Card, Pitch, ShowBallParks
from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from src.api.routes.users import firebase_claims
from src.core.cache import build_cache_key, get_cache_client, get_cached_json, set_cached_json

from .common import (
    _to_int,
    _to_float,
    _load_facts_df_for_username,
    _load_facts_df_for_username_columns,
    _filter_df_by_pitcher,
    _filter_df_by_hitter,
    _bool_col,
    _str_col,
    _num_col,
    _user_masks,
    _pitching_masks,
)
from .models import (
    PlateAppearanceStatsOut,
    ShowSkillsOut,
    ShowAggregateStatsOut,
    PowerSkillOut,
    TimingSkillOut,
    BattingArchetypeOut,
    PitchingArchetypeOut,
    CombinedArchetypeOut,
    StrikeoutZoneMapOut,
    HitDataMapOut,
    ShowYourOvrWeightOut,
    ShowYourOvrWeightsOut,
)
from .profile import _get_authed_user, _get_profile_for_user, _get_profile_by_username


router = APIRouter()
public_router = APIRouter()

SHOW_CARD_YEAR = 25

_DIFFICULTY_ORDER = [
    "rookie",
    "veteran",
    "all star",
    "hall of fame",
    "legend",
    "goat",
]
_DIFFICULTY_ALIASES = {
    "all-star": "all star",
    "allstar": "all star",
    "hall_of_fame": "hall of fame",
    "hof": "hall of fame",
}
_DIFFICULTY_STEP = 0.15
_MIN_REFERENCE_USER_PA = 80
_MIN_REFERENCE_KEEP_RATIO = 0.20
_MAX_BFS_DEPTH = 2
_MAX_BFS_NODES = 140
_BASE_BRANCH_BY_DEPTH = {0: 32, 1: 24, 2: 16}
_MAX_BRANCH_FACTOR = 40
_REF_HITTING_TARGET_PA = 25000
_REF_PITCHING_TARGET_PA = 25000
_BFS_TIME_BUDGET_SEC = float(os.getenv("SHOW_SKILLS_BFS_BUDGET_SEC", "5"))
_FACTS_CACHE_TTL_SEC = float(os.getenv("SHOW_SKILLS_FACTS_CACHE_TTL_SEC", "600"))
_COMBINED_CACHE_TTL_SEC = float(os.getenv("SHOW_SKILLS_COMBINED_CACHE_TTL_SEC", "180"))
_FACT_LOAD_WORKERS = max(2, int(os.getenv("SHOW_SKILLS_FACT_LOAD_WORKERS", "6")))
_MAX_CANDIDATES_PER_NODE = 120
_PITCHING_K_WEIGHT_LOC = 1.0
_PITCHING_K_WEIGHT_OTHER = 0.80
_PITCHING_K_WEIGHT_TIMING = 0.70
_PITCHING_K_SD_FLOOR = 0.04
_PITCHING_K_Z_CLAMP_PER_DIFF = 3.5
_PITCHING_K_Z_CLAMP_FINAL = 4.5
_POWER_SD_FLOOR = 0.02
_POWER_Z_CLAMP_BASE = 3.5
_POWER_Z_CLAMP_PER_DIFF = 3.0
_POWER_Z_CLAMP_FINAL = 4.5
_POWER_MIN_DIFF_PA = 20

_SKILL_FACT_COLUMNS: tuple[str, ...] = (
    "game_id",
    "result",
    "is_strikeout",
    "is_sac_fly",
    "is_sac_bunt",
    "is_double_play",
    "is_perfect_perfect",
    "batted_ball_type",
    "hit_direction",
    "fielder_pos",
    "batter_side",
    "k_timing",
    "k_is_chase",
    "k_is_looking",
    "k_loc_height",
    "k_loc_width",
    "difficulty_id",
    "ballpark_id",
    "batter_mlb_id",
    "pitcher_mlb_id",
    "is_out",
    "is_error",
    "times_seen_pitcher",
    "home_profile_username",
    "away_profile_username",
    "home_team_id",
    "away_team_id",
    "home_name",
    "away_name",
    "is_home_batting",
)

_facts_cache_lock = threading.Lock()
_facts_cache: dict[tuple[str, tuple[str, ...]], tuple[float, pd.DataFrame]] = {}
_combined_cache_lock = threading.Lock()
_combined_archetype_cache: dict[
    tuple[str, Optional[int], Optional[int]], tuple[float, CombinedArchetypeOut]
] = {}
_YOUR_OVR_CACHE_TTL_SEC = float(os.getenv("SHOW_YOUR_OVR_CACHE_TTL_SEC", "300"))
_your_ovr_cache_lock = threading.Lock()
_your_ovr_cache: dict[str, tuple[float, ShowYourOvrWeightsOut]] = {}


def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        parsed = int(float(raw))
    except ValueError:
        return default
    return parsed if parsed > 0 else default


_SHOW_STATS_REDIS_CACHE_TTL_SEC = _read_positive_int_env("SHOW_STATS_REDIS_CACHE_TTL_SEC", 14400)
_SHOW_STATS_CLIENT_CACHE_TTL_SEC = _read_positive_int_env("SHOW_STATS_CLIENT_CACHE_TTL_SEC", 3600)
_SHOW_STATS_STORAGE_CACHE_TTL_SEC = _read_positive_int_env("SHOW_STATS_STORAGE_CACHE_TTL_SEC", 3600)
_stats_storage_cache_lock = threading.Lock()
_stats_facts_df_cache: dict[str, tuple[float, pd.DataFrame]] = {}


@dataclass
class _SkillContext:
    root_df: pd.DataFrame
    ref_hitting_df: pd.DataFrame
    ref_pitching_df: pd.DataFrame


def _normalize_username(value: str) -> str:
    return str(value or "").strip().lower()


def _set_stats_http_cache_headers(response: Response, *, is_public: bool) -> None:
    max_age = max(1, _SHOW_STATS_CLIENT_CACHE_TTL_SEC)
    scope = "public" if is_public else "private"
    response.headers["Cache-Control"] = f"{scope}, max-age={max_age}, stale-while-revalidate=60"
    if not is_public:
        response.headers["Vary"] = "Authorization, Cookie"


def _cache_stats_response(
    cache: Redis | None,
    cache_key: str,
    response: ShowAggregateStatsOut,
) -> ShowAggregateStatsOut:
    set_cached_json(
        cache,
        cache_key,
        response.model_dump(mode="json"),
        ttl_sec=max(1, _SHOW_STATS_REDIS_CACHE_TTL_SEC),
    )
    return response


def _load_stats_facts_df_cached(username: str) -> pd.DataFrame:
    now = time.monotonic()
    uname = _normalize_username(username)
    with _stats_storage_cache_lock:
        cached = _stats_facts_df_cache.get(uname)
        if cached and cached[0] > now:
            return cached[1].copy(deep=False)
        if cached and cached[0] <= now:
            _stats_facts_df_cache.pop(uname, None)

    loaded = _load_facts_df_for_username(username)
    with _stats_storage_cache_lock:
        _stats_facts_df_cache[uname] = (time.monotonic() + max(1, _SHOW_STATS_STORAGE_CACHE_TTL_SEC), loaded)
    return loaded.copy(deep=False)


def _facts_cache_key(username: str, columns: tuple[str, ...]) -> tuple[str, tuple[str, ...]]:
    return (_normalize_username(username), columns)


def _load_facts_df_cached(username: str, columns: tuple[str, ...]) -> pd.DataFrame:
    now = time.monotonic()
    key = _facts_cache_key(username, columns)
    with _facts_cache_lock:
        cached = _facts_cache.get(key)
        if cached and cached[0] > now:
            return cached[1].copy(deep=False)
        if cached and cached[0] <= now:
            _facts_cache.pop(key, None)

    loaded = _load_facts_df_for_username_columns(username, columns)
    with _facts_cache_lock:
        _facts_cache[key] = (now + _FACTS_CACHE_TTL_SEC, loaded)
    return loaded.copy(deep=False)


def _load_skill_facts_df(username: str) -> pd.DataFrame:
    return _load_facts_df_cached(username, _SKILL_FACT_COLUMNS)


def _load_combined_from_cache(
    username: str,
    pitcher_mlb_id: Optional[int],
    hitter_mlb_id: Optional[int],
) -> Optional[CombinedArchetypeOut]:
    key = (_normalize_username(username), _to_int(pitcher_mlb_id), _to_int(hitter_mlb_id))
    now = time.monotonic()
    with _combined_cache_lock:
        cached = _combined_archetype_cache.get(key)
        if cached and cached[0] > now:
            return cached[1]
        if cached and cached[0] <= now:
            _combined_archetype_cache.pop(key, None)
    return None


def _store_combined_cache(
    username: str,
    pitcher_mlb_id: Optional[int],
    hitter_mlb_id: Optional[int],
    value: CombinedArchetypeOut,
) -> None:
    key = (_normalize_username(username), _to_int(pitcher_mlb_id), _to_int(hitter_mlb_id))
    now = time.monotonic()
    with _combined_cache_lock:
        _combined_archetype_cache[key] = (now + _COMBINED_CACHE_TTL_SEC, value)


def _resolve_your_ovr_key(spaces: SpacesConnector, username: str) -> Optional[str]:
    key = f"facts/{username}/your_ovr.parquet"
    legacy_key = f"di-storage/facts/{username}/your_ovr.parquet"
    if spaces.exists(key):
        return key
    if spaces.exists(legacy_key):
        return legacy_key
    return None


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    text = str(v or "").strip().lower()
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    return False


def _load_your_ovr_weights_cached(username: str) -> ShowYourOvrWeightsOut:
    uname = _normalize_username(username)
    now = time.monotonic()
    with _your_ovr_cache_lock:
        cached = _your_ovr_cache.get(uname)
        if cached and cached[0] > now:
            return cached[1]
        if cached and cached[0] <= now:
            _your_ovr_cache.pop(uname, None)

    cfg = SpacesConfig.from_env()
    spaces = SpacesConnector(cfg)
    key = _resolve_your_ovr_key(spaces, username)
    if not key:
        payload = ShowYourOvrWeightsOut(
            username=username,
            updated_at=None,
            total_weights=0,
            hitting_weights=0,
            pitching_weights=0,
            weights=[],
        )
        with _your_ovr_cache_lock:
            _your_ovr_cache[uname] = (now + _YOUR_OVR_CACHE_TTL_SEC, payload)
        return payload

    raw = spaces.get_bytes(key)
    if not raw:
        payload = ShowYourOvrWeightsOut(
            username=username,
            updated_at=None,
            total_weights=0,
            hitting_weights=0,
            pitching_weights=0,
            weights=[],
        )
        with _your_ovr_cache_lock:
            _your_ovr_cache[uname] = (now + _YOUR_OVR_CACHE_TTL_SEC, payload)
        return payload

    buf = BytesIO(raw)
    parquet = pq.ParquetFile(buf)
    desired_columns = ["mlb_id", "role", "weight", "pa", "meets_min_pa", "updated_at"]
    available = set(parquet.schema.names)
    cols = [c for c in desired_columns if c in available]
    if not cols:
        payload = ShowYourOvrWeightsOut(
            username=username,
            updated_at=None,
            total_weights=0,
            hitting_weights=0,
            pitching_weights=0,
            weights=[],
        )
        with _your_ovr_cache_lock:
            _your_ovr_cache[uname] = (now + _YOUR_OVR_CACHE_TTL_SEC, payload)
        return payload

    buf.seek(0)
    df = pd.read_parquet(buf, columns=cols)
    for col in desired_columns:
        if col not in df.columns:
            df[col] = None

    items: list[ShowYourOvrWeightOut] = []
    updated_at: Optional[str] = None

    for row in df.to_dict(orient="records"):
        mlb_id = _to_int(row.get("mlb_id"))
        if mlb_id is None:
            continue

        role = str(row.get("role") or "").strip().lower()
        if role not in {"hitting", "pitching"}:
            continue

        weight = _to_float(row.get("weight"))
        if weight is None or not math.isfinite(weight):
            continue

        pa = _to_int(row.get("pa")) or 0
        meets_min_pa = _to_bool(row.get("meets_min_pa"))

        maybe_updated = str(row.get("updated_at") or "").strip() or None
        if maybe_updated and (updated_at is None or maybe_updated > updated_at):
            updated_at = maybe_updated

        items.append(
            ShowYourOvrWeightOut(
                mlb_id=mlb_id,
                role=role,
                weight=float(weight),
                pa=max(0, int(pa)),
                meets_min_pa=meets_min_pa,
            )
        )

    items.sort(key=lambda item: (item.role, -item.weight, -item.pa, item.mlb_id))
    hitting_count = sum(1 for item in items if item.role == "hitting")
    pitching_count = sum(1 for item in items if item.role == "pitching")
    payload = ShowYourOvrWeightsOut(
        username=username,
        updated_at=updated_at,
        total_weights=len(items),
        hitting_weights=hitting_count,
        pitching_weights=pitching_count,
        weights=items,
    )
    with _your_ovr_cache_lock:
        _your_ovr_cache[uname] = (now + _YOUR_OVR_CACHE_TTL_SEC, payload)
    return payload


def _difficulty_index(value: object) -> int:
    if value is None:
        return 0

    if isinstance(value, (int, float)):
        try:
            raw = int(value)
        except Exception:
            raw = 0
        if 0 <= raw <= 5:
            return raw
        if 1 <= raw <= 6:
            return raw - 1
        return 0

    text = str(value).strip().lower().replace("_", " ").replace("-", " ")
    text = " ".join(text.split())
    if not text:
        return 0

    if text.isdigit():
        raw = int(text)
        if 0 <= raw <= 5:
            return raw
        if 1 <= raw <= 6:
            return raw - 1

    text = _DIFFICULTY_ALIASES.get(text, text)
    try:
        return _DIFFICULTY_ORDER.index(text)
    except ValueError:
        return 0


def _difficulty_multiplier_series(df: pd.DataFrame) -> pd.Series:
    col = df.get("difficulty_id")
    if col is None:
        return pd.Series([1.0] * len(df), index=df.index, dtype=float)
    values = [1.0 + (_DIFFICULTY_STEP * _difficulty_index(v)) for v in col.tolist()]
    return pd.Series(values, index=df.index, dtype=float)


def _apply_difficulty_event_adjustment(evt: pd.Series, df: pd.DataFrame) -> pd.Series:
    if evt.empty:
        return evt
    mult = _difficulty_multiplier_series(df)
    adjusted = evt.copy()
    adjusted = adjusted.mask(evt > 0, evt * mult)
    adjusted = adjusted.mask(evt < 0, evt / mult)
    return adjusted.clip(lower=-1.0, upper=1.0)


def _difficulty_adjusted_pa_profile(sub: pd.DataFrame) -> dict[str, float]:
    pa = len(sub)
    if pa == 0:
        return {
            "pa_eff": 0.0,
            "avg": 0.0,
            "obp": 0.0,
            "slg": 0.0,
            "ops": 0.0,
            "k_rate": 0.0,
            "bb_rate": 0.0,
        }

    results = _str_col(sub, "result").str.lower()
    mult = _difficulty_multiplier_series(sub)

    singles = results == "single"
    doubles = results == "double"
    triples = results == "triple"
    homeruns = results.isin(_HR_RESULTS)
    walks = results.isin(_WALK_RESULTS)
    hbp = results.isin(_HBP_RESULTS)
    strikeouts = _bool_col(sub, "is_strikeout") | results.isin(["strikeout", "strike out"])
    sac_flies = _bool_col(sub, "is_sac_fly")
    sac_bunts = _bool_col(sub, "is_sac_bunt")

    weighted_hits = float(((singles | doubles | triples | homeruns).astype(float) * mult).sum())
    weighted_tb = float(
        (singles.astype(float) * mult).sum()
        + (2.0 * doubles.astype(float) * mult).sum()
        + (3.0 * triples.astype(float) * mult).sum()
        + (4.0 * homeruns.astype(float) * mult).sum()
    )
    weighted_walks = float((walks.astype(float) * mult).sum())
    weighted_hbp = float((hbp.astype(float) * mult).sum())
    weighted_strikeouts = float((strikeouts.astype(float) / mult.clip(lower=1e-6)).sum())

    is_ab = ~(walks | hbp | sac_flies | sac_bunts)
    non_hit_ab = is_ab & ~(singles | doubles | triples | homeruns)
    weighted_non_hit_ab = float(non_hit_ab.astype(float).sum())
    weighted_ab = weighted_hits + weighted_non_hit_ab
    weighted_pa = weighted_ab + weighted_walks + weighted_hbp + float(sac_flies.astype(float).sum())

    avg = weighted_hits / weighted_ab if weighted_ab > 0 else 0.0
    obp = (weighted_hits + weighted_walks + weighted_hbp) / weighted_pa if weighted_pa > 0 else 0.0
    slg = weighted_tb / weighted_ab if weighted_ab > 0 else 0.0
    ops = obp + slg
    k_rate = weighted_strikeouts / weighted_pa if weighted_pa > 0 else 0.0
    bb_rate = weighted_walks / weighted_pa if weighted_pa > 0 else 0.0

    return {
        "pa_eff": weighted_pa,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "k_rate": k_rate,
        "bb_rate": bb_rate,
    }


def _dynamic_branch_factor(depth: int, root_pa: int, node_pa: int) -> int:
    base = _BASE_BRANCH_BY_DEPTH.get(depth, 2)
    bonus = 0
    if root_pa < _MIN_REFERENCE_USER_PA:
        bonus += 2
    if root_pa < (_MIN_REFERENCE_USER_PA // 2):
        bonus += 2
    if node_pa < _MIN_REFERENCE_USER_PA:
        bonus += 1
    return max(2, min(_MAX_BRANCH_FACTOR, base + bonus))


def _ranked_opponents(df: pd.DataFrame, username: str) -> list[str]:
    name = _normalize_username(username)
    if not name or df.empty:
        return []

    candidates = [
        ("home_profile_username", "away_profile_username"),
        ("home_team_id", "away_team_id"),
        ("home_name", "away_name"),
    ]

    for home_col, away_col in candidates:
        if home_col not in df.columns or away_col not in df.columns:
            continue
        home = _str_col(df, home_col).str.lower()
        away = _str_col(df, away_col).str.lower()
        is_home = home == name
        is_away = away == name
        has_match = is_home | is_away
        if not has_match.any():
            continue

        opp = pd.Series([""] * len(df), index=df.index, dtype=object)
        opp = opp.mask(is_home, away)
        opp = opp.mask(is_away, home)
        opp = opp.fillna("").astype(str).str.strip().str.lower()
        valid = has_match & opp.ne("") & opp.ne(name)
        if not valid.any():
            return []

        game_ids = _str_col(df, "game_id").str.strip()
        tmp = pd.DataFrame({"opponent": opp[valid], "game_id": game_ids[valid]}, index=opp[valid].index)
        grouped = tmp.groupby("opponent", sort=False).agg(
            pa=("opponent", "size"),
            games=("game_id", lambda s: int((s[s != ""]).nunique())),
        )
        grouped = grouped.sort_values(by=["games", "pa", "opponent"], ascending=[False, False, True])
        return grouped.index.tolist()
    return []


def _downweight_small_sample(sub: pd.DataFrame, source_pa: int) -> pd.DataFrame:
    if sub.empty or source_pa >= _MIN_REFERENCE_USER_PA:
        return sub

    ratio = max(_MIN_REFERENCE_KEEP_RATIO, float(source_pa) / float(_MIN_REFERENCE_USER_PA))
    keep_count = max(1, int(round(len(sub) * ratio)))
    if keep_count >= len(sub):
        return sub

    game_ids = _str_col(sub, "game_id")
    event_seq = _str_col(sub, "event_seq")
    stable_key = game_ids + "|" + event_seq + "|" + _str_col(sub, "result")
    hashes = pd.util.hash_pandas_object(stable_key, index=False)
    keep_idx = hashes.sort_values().index[:keep_count]
    return sub.loc[keep_idx]


def _dedupe_by_game_id(sub: pd.DataFrame, seen: set[str]) -> pd.DataFrame:
    if sub.empty or "game_id" not in sub.columns:
        return sub

    game_ids = _str_col(sub, "game_id").str.strip()
    keep_mask = game_ids.eq("") | ~game_ids.isin(seen)
    kept = sub[keep_mask]
    for gid in game_ids[keep_mask]:
        if gid:
            seen.add(gid)
    return kept


def _load_facts_df_safe(
    username: str,
    cache: dict[str, pd.DataFrame],
    *,
    columns: tuple[str, ...] = _SKILL_FACT_COLUMNS,
    deadline: Optional[float] = None,
) -> pd.DataFrame:
    if deadline is not None and time.monotonic() >= deadline:
        return pd.DataFrame()
    uname = _normalize_username(username)
    if uname in cache:
        return cache[uname]
    try:
        cache[uname] = _load_facts_df_cached(uname, columns)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            cache[uname] = pd.DataFrame()
        else:
            raise
    except Exception:
        cache[uname] = pd.DataFrame()
    return cache[uname]


def _load_many_facts_df_safe(
    usernames: list[str],
    cache: dict[str, pd.DataFrame],
    *,
    columns: tuple[str, ...] = _SKILL_FACT_COLUMNS,
    deadline: Optional[float] = None,
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    pending: list[str] = []

    for raw in usernames:
        uname = _normalize_username(raw)
        if not uname:
            continue
        if uname in cache:
            out[uname] = cache[uname]
            continue
        pending.append(uname)

    if not pending:
        return out
    if deadline is not None and time.monotonic() >= deadline:
        return out

    workers = max(1, min(_FACT_LOAD_WORKERS, len(pending)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(_load_facts_df_safe, uname, cache, columns=columns, deadline=deadline): uname for uname in pending}
        for fut in as_completed(future_map):
            if deadline is not None and time.monotonic() >= deadline:
                break
            uname = future_map[fut]
            try:
                out[uname] = fut.result()
            except Exception:
                out[uname] = pd.DataFrame()
                cache[uname] = out[uname]
    return out


def _estimated_reference_rows(sub: pd.DataFrame) -> float:
    pa = len(sub)
    if pa <= 0:
        return 0.0
    if pa >= _MIN_REFERENCE_USER_PA:
        return float(pa)
    ratio = max(_MIN_REFERENCE_KEEP_RATIO, float(pa) / float(_MIN_REFERENCE_USER_PA))
    return float(pa) * ratio


def _build_skill_context(
    username: str,
    root_df_full: pd.DataFrame,
    *,
    pitcher_mlb_id: Optional[int] = None,
    hitter_mlb_id: Optional[int] = None,
) -> _SkillContext:
    root_name = _normalize_username(username)
    cache: dict[str, pd.DataFrame] = {root_name: root_df_full}
    root_df = _filter_df_by_hitter(_filter_df_by_pitcher(root_df_full, pitcher_mlb_id), hitter_mlb_id)
    root_pa = len(root_df)
    deadline = time.monotonic() + _BFS_TIME_BUDGET_SEC

    queue: deque[tuple[str, int]] = deque([(root_name, 0)])
    seen_users: set[str] = {root_name}
    sample_users: list[str] = [root_name]
    est_hit_rows = 0.0
    est_pitch_rows = 0.0

    while queue and len(sample_users) < _MAX_BFS_NODES:
        if time.monotonic() >= deadline:
            break
        if est_hit_rows >= _REF_HITTING_TARGET_PA and est_pitch_rows >= _REF_PITCHING_TARGET_PA:
            break
        node, depth = queue.popleft()
        if depth >= _MAX_BFS_DEPTH:
            continue
        node_df_full = _load_facts_df_safe(node, cache, deadline=deadline)
        if node_df_full.empty:
            continue

        node_df = _filter_df_by_hitter(_filter_df_by_pitcher(node_df_full, pitcher_mlb_id), hitter_mlb_id)
        if node != root_name and not node_df.empty:
            node_hit_mask, node_pitch_mask, _ = _user_masks(node_df, node)
            est_hit_rows += _estimated_reference_rows(node_df[node_hit_mask])
            est_pitch_rows += _estimated_reference_rows(node_df[node_pitch_mask])

        branch = _dynamic_branch_factor(depth, root_pa=root_pa, node_pa=len(node_df))
        ranked = _ranked_opponents(node_df_full, node)
        if not ranked:
            continue
        candidates: list[str] = []
        for opp in ranked:
            opp_norm = _normalize_username(opp)
            if not opp_norm or opp_norm in seen_users:
                continue
            candidates.append(opp_norm)
            if len(candidates) >= _MAX_CANDIDATES_PER_NODE:
                break

        if not candidates:
            continue

        loaded = _load_many_facts_df_safe(candidates, cache, deadline=deadline)
        added = 0
        for opp in candidates:
            if time.monotonic() >= deadline:
                break
            if opp in seen_users:
                continue
            opp_df = loaded.get(opp, pd.DataFrame())
            if opp_df.empty:
                continue
            seen_users.add(opp)
            sample_users.append(opp)
            queue.append((opp, depth + 1))
            added += 1
            if len(sample_users) >= _MAX_BFS_NODES or added >= branch:
                break

    root_game_ids = set(_str_col(root_df, "game_id").str.strip().tolist()) if "game_id" in root_df.columns else set()
    root_game_ids.discard("")
    seen_hit_games = set(root_game_ids)
    seen_pitch_games = set(root_game_ids)
    hit_parts: list[pd.DataFrame] = []
    pitch_parts: list[pd.DataFrame] = []

    for uname in sample_users[1:]:
        if time.monotonic() >= deadline and (hit_parts or pitch_parts):
            break
        node_df_full = _load_facts_df_safe(uname, cache, deadline=deadline)
        if node_df_full.empty:
            continue
        node_df = _filter_df_by_hitter(_filter_df_by_pitcher(node_df_full, pitcher_mlb_id), hitter_mlb_id)
        if node_df.empty:
            continue

        user_hitting, user_pitching, _ = _user_masks(node_df, uname)
        hit_src = node_df[user_hitting]
        pitch_src = node_df[user_pitching]
        if hit_src.empty and pitch_src.empty:
            continue

        hit_sub = _dedupe_by_game_id(hit_src, seen_hit_games)
        pitch_sub = _dedupe_by_game_id(pitch_src, seen_pitch_games)
        hit_sub = _downweight_small_sample(hit_sub, source_pa=len(hit_src))
        pitch_sub = _downweight_small_sample(pitch_sub, source_pa=len(pitch_src))

        if not hit_sub.empty:
            hit_parts.append(hit_sub)
        if not pitch_sub.empty:
            pitch_parts.append(pitch_sub)

    if hit_parts:
        ref_hitting_df = pd.concat(hit_parts, ignore_index=True)
    else:
        _, _, opp_hitting = _user_masks(root_df, root_name)
        ref_hitting_df = root_df[opp_hitting].copy()

    if pitch_parts:
        ref_pitching_df = pd.concat(pitch_parts, ignore_index=True)
    else:
        _, opp_pitching = _pitching_masks(root_df, root_name)
        ref_pitching_df = root_df[opp_pitching].copy()

    return _SkillContext(
        root_df=root_df,
        ref_hitting_df=ref_hitting_df,
        ref_pitching_df=ref_pitching_df,
    )

def _blank_strikeout_stats() -> dict[str, float]:
    return {
        "k_pct": 0.0,
        "chase_pct": 0.0,
        "freeze_pct": 0.0,
        "timing_pct": 0.0,
        "timing_k_pct": 0.0,
        "eye_k_pct": 0.0,
        "location_k_pct": 0.0,
        "heart_miss_k_pct": 0.0,
        "inzone_swing_k_pct": 0.0,
    }


def _stats_from_counts(
    *,
    k_count: int,
    base_pa: int,
    chase_k: int,
    look_k: int,
    eye_k: int,
    early_k: int,
    late_k: int,
) -> dict[str, float]:
    if base_pa <= 0:
        return _blank_strikeout_stats()

    k_pct = 100.0 * k_count / float(base_pa)
    if k_count == 0:
        out = _blank_strikeout_stats()
        out["k_pct"] = k_pct
        return out

    timing_total = early_k + late_k
    if timing_total:
        timing_pct = 100.0 * ((early_k - late_k) / float(timing_total))
    else:
        timing_pct = 0.0

    timing_k_pct = 100.0 * timing_total / float(k_count)
    eye_k_pct = 100.0 * eye_k / float(k_count)
    location_k_pct = _clamp(100.0 - (timing_k_pct + eye_k_pct), 0.0, 100.0)

    return {
        "k_pct": k_pct,
        "chase_pct": 100.0 * chase_k / float(k_count),
        "freeze_pct": 100.0 * look_k / float(k_count),
        "timing_pct": timing_pct,
        "timing_k_pct": timing_k_pct,
        "eye_k_pct": eye_k_pct,
        "location_k_pct": location_k_pct,
        "heart_miss_k_pct": 0.0,
        "inzone_swing_k_pct": 0.0,
    }


def _compute_strikeout_stats(user_df: pd.DataFrame) -> dict[str, float]:
    base_pa = len(user_df)
    if base_pa == 0:
        return _blank_strikeout_stats()

    results = _str_col(user_df, "result").str.lower()
    is_strikeout = _bool_col(user_df, "is_strikeout") | results.isin(["strikeout", "strike out"])
    k_count = int(is_strikeout.sum())
    if k_count == 0:
        out = _blank_strikeout_stats()
        out["k_pct"] = 0.0
        return out

    k_is_chase = _bool_col(user_df, "k_is_chase")
    k_is_looking = _bool_col(user_df, "k_is_looking")
    k_timing = _str_col(user_df, "k_timing").str.lower()

    chase_k = int((is_strikeout & k_is_chase).sum())
    look_k = int((is_strikeout & k_is_looking).sum())
    eye_k = int((is_strikeout & (k_is_chase | k_is_looking)).sum())
    early_k = int((is_strikeout & (k_timing == "early")).sum())
    late_k = int((is_strikeout & (k_timing == "late")).sum())

    return _stats_from_counts(
        k_count=k_count,
        base_pa=base_pa,
        chase_k=chase_k,
        look_k=look_k,
        eye_k=eye_k,
        early_k=early_k,
        late_k=late_k,
    )


def _compute_pa_stats(
    df: pd.DataFrame,
    username: str,
    *,
    reference_hitting_df: Optional[pd.DataFrame] = None,
    reference_pitching_df: Optional[pd.DataFrame] = None,
) -> tuple[PlateAppearanceStatsOut, PlateAppearanceStatsOut]:
    if df.empty:
        empty = PlateAppearanceStatsOut(
            plate_appearances=0,
            hits=0,
            walks=0,
            strikeouts=0,
            avg=0.0,
            obp=0.0,
            slg=0.0,
            ops=0.0,
            kbb=None,
        )
        return empty, empty

    hitting_mask, pitching_mask, _ = _user_masks(df, username)

    def calc(mask: pd.Series, reference_df: Optional[pd.DataFrame]) -> PlateAppearanceStatsOut:
        sub = df[mask]
        pa = len(sub)
        if pa == 0:
            return PlateAppearanceStatsOut(
                plate_appearances=0,
                hits=0,
                walks=0,
                strikeouts=0,
                avg=0.0,
                obp=0.0,
                slg=0.0,
                ops=0.0,
                kbb=None,
            )

        results = sub.get("result")
        if results is None:
            results = pd.Series([""] * pa)
        results = results.fillna("").astype(str).str.lower()

        singles = results.isin(["single"])
        doubles = results.isin(["double"])
        triples = results.isin(["triple"])
        homeruns = results.isin(["homerun", "home_run", "home run"])
        walks = results.isin(["walk"])
        hbp = results.isin(["hit_by_pitch", "hit by pitch", "hbp"])

        strikeouts = _bool_col(sub, "is_strikeout") | results.isin(
            ["strikeout", "strike out"]
        )

        sac_flies = _bool_col(sub, "is_sac_fly")
        sac_bunts = _bool_col(sub, "is_sac_bunt")

        hits = int(singles.sum() + doubles.sum() + triples.sum() + homeruns.sum())
        walk_count = int(walks.sum())
        hbp_count = int(hbp.sum())
        strikeout_count = int(strikeouts.sum())
        total_bases = int(
            singles.sum()
            + (2 * doubles.sum())
            + (3 * triples.sum())
            + (4 * homeruns.sum())
        )

        ab = pa - walk_count - hbp_count - int(sac_flies.sum()) - int(sac_bunts.sum())
        ab_float = float(max(ab, 0))
        obp_denom = ab_float + float(walk_count + hbp_count + int(sac_flies.sum()))

        avg = hits / ab_float if ab_float else 0.0
        obp = (hits + walk_count + hbp_count) / obp_denom if obp_denom else 0.0
        slg = total_bases / ab_float if ab_float else 0.0
        ops = obp + slg
        kbb = None
        if walk_count > 0:
            kbb = strikeout_count / float(walk_count)

        user_adj = _difficulty_adjusted_pa_profile(sub)
        sample_adj = _difficulty_adjusted_pa_profile(
            reference_df if reference_df is not None else pd.DataFrame()
        )

        avg_adj = float(user_adj["avg"])
        obp_adj = float(user_adj["obp"])
        slg_adj = float(user_adj["slg"])
        ops_adj = float(user_adj["ops"])
        kbb_adj = kbb

        if sample_adj["pa_eff"] > 0:
            # Small samples get stronger pull to the sampled population baseline.
            prior_pa = min(220.0, max(30.0, float(max(0, 120 - pa)) + 35.0))
            denom = user_adj["pa_eff"] + prior_pa
            if denom > 0:
                avg_adj = (user_adj["avg"] * user_adj["pa_eff"] + sample_adj["avg"] * prior_pa) / denom
                obp_adj = (user_adj["obp"] * user_adj["pa_eff"] + sample_adj["obp"] * prior_pa) / denom
                slg_adj = (user_adj["slg"] * user_adj["pa_eff"] + sample_adj["slg"] * prior_pa) / denom
                ops_adj = obp_adj + slg_adj
                smooth_k_rate = (
                    user_adj["k_rate"] * user_adj["pa_eff"] + sample_adj["k_rate"] * prior_pa
                ) / denom
                smooth_bb_rate = (
                    user_adj["bb_rate"] * user_adj["pa_eff"] + sample_adj["bb_rate"] * prior_pa
                ) / denom
                if smooth_bb_rate > 1e-6:
                    kbb_adj = smooth_k_rate / smooth_bb_rate
                else:
                    kbb_adj = None

        return PlateAppearanceStatsOut(
            plate_appearances=pa,
            hits=hits,
            walks=walk_count,
            strikeouts=strikeout_count,
            avg=avg_adj,
            obp=obp_adj,
            slg=slg_adj,
            ops=ops_adj,
            kbb=kbb_adj,
        )

    return calc(hitting_mask, reference_hitting_df), calc(pitching_mask, reference_pitching_df)


def _compute_aggregate_stats(
    df: pd.DataFrame,
    username: str,
    view: Optional[str] = None,
) -> ShowAggregateStatsOut:
    user_hitting, user_pitching, _ = _user_masks(df, username)
    view_norm = (view or "hitting").strip().lower()
    if view_norm in ("pitching", "pitch"):
        user_df = df[user_pitching]
    else:
        user_df = df[user_hitting]

    return _aggregate_stats_for_df(user_df)


def _aggregate_stats_for_df(user_df: pd.DataFrame) -> ShowAggregateStatsOut:
    pa = len(user_df)
    if pa == 0:
        return ShowAggregateStatsOut(
            pa=0,
            ab=0,
            r=0,
            h=0,
            rbi=0,
            singles=0,
            doubles=0,
            triples=0,
            hr=0,
            bb=0,
            so=0,
            avg=0.0,
            obp=0.0,
            slg=0.0,
            ops=0.0,
            lob=0,
            gidp=0,
            gidp_pct=None,
            woba=0.0,
            iso=0.0,
            babip=0.0,
            k_pct=0.0,
            bb_pct=0.0,
            hr_pct=0.0,
            xbh_pct=0.0,
            rs_pct=0.0,
        )

    results = _str_col(user_df, "result").str.lower()
    singles = results == "single"
    doubles = results == "double"
    triples = results == "triple"
    homeruns = results.isin(_HR_RESULTS)
    walks = results.isin(_WALK_RESULTS)
    hbp = results.isin(_HBP_RESULTS)

    sac_fly = _bool_col(user_df, "is_sac_fly")
    sac_bunt = _bool_col(user_df, "is_sac_bunt")
    strikeouts = _bool_col(user_df, "is_strikeout") | results.isin(
        ["strikeout", "strike out"]
    )
    runner_on_first = _bool_col(user_df, "runner_on_first")
    runner_on_second = _bool_col(user_df, "runner_on_second")
    runner_on_third = _bool_col(user_df, "runner_on_third")
    runner_on_base = runner_on_first | runner_on_second | runner_on_third
    outs_before = _num_col(user_df, "outs_before")
    is_out = _bool_col(user_df, "is_out")

    singles_count = int(singles.sum())
    doubles_count = int(doubles.sum())
    triples_count = int(triples.sum())
    hr_count = int(homeruns.sum())
    bb_count = int(walks.sum())
    so_count = int(strikeouts.sum())
    hbp_count = int(hbp.sum())
    sf_count = int(sac_fly.sum())
    sh_count = int(sac_bunt.sum())

    hits = singles_count + doubles_count + triples_count + hr_count
    total_bases = singles_count + (2 * doubles_count) + (3 * triples_count) + (4 * hr_count)

    ab = pa - bb_count - hbp_count - sf_count - sh_count
    if ab < 0:
        ab = 0

    runs = int(_num_col(user_df, "runs_scored").sum())
    rbi = int(_num_col(user_df, "rbi").sum())
    gidp_flags = _bool_col(user_df, "is_double_play")
    gidp = int((gidp_flags & runner_on_first).sum())
    gidp_opps = int(runner_on_first.sum())
    gidp_pct = (gidp / gidp_opps) * 100 if gidp_opps > 0 else None

    avg = hits / ab if ab else 0.0
    obp_denom = ab + bb_count + hbp_count + sf_count
    obp = (hits + bb_count + hbp_count) / obp_denom if obp_denom else 0.0
    slg = total_bases / ab if ab else 0.0
    ops = obp + slg

    woba_denom = obp_denom
    if woba_denom:
        woba_num = (
            _WOBA_WEIGHTS["bb"] * bb_count
            + _WOBA_WEIGHTS["hbp"] * hbp_count
            + _WOBA_WEIGHTS["single"] * singles_count
            + _WOBA_WEIGHTS["double"] * doubles_count
            + _WOBA_WEIGHTS["triple"] * triples_count
            + _WOBA_WEIGHTS["hr"] * hr_count
        )
        woba = woba_num / float(woba_denom)
    else:
        woba = 0.0

    iso = (total_bases - hits) / ab if ab else 0.0
    babip_denom = ab - so_count - hr_count + sf_count
    babip = (hits - hr_count) / babip_denom if babip_denom > 0 else 0.0

    k_pct = (so_count / pa) * 100 if pa else 0.0
    bb_pct = (bb_count / pa) * 100 if pa else 0.0
    hr_pct = (hr_count / pa) * 100 if pa else 0.0
    xbh_pct = ((doubles_count + triples_count + hr_count) / pa) * 100 if pa else 0.0

    times_on_base = hits + bb_count + hbp_count
    rs_denom = times_on_base - hr_count
    rs_num = runs - hr_count
    rs_pct = (max(0, rs_num) / rs_denom) * 100 if rs_denom > 0 else 0.0

    lob = int(((outs_before == 2) & is_out & runner_on_base).sum())

    return ShowAggregateStatsOut(
        pa=pa,
        ab=ab,
        r=runs,
        h=hits,
        rbi=rbi,
        singles=singles_count,
        doubles=doubles_count,
        triples=triples_count,
        hr=hr_count,
        bb=bb_count,
        so=so_count,
        avg=avg,
        obp=obp,
        slg=slg,
        ops=ops,
        lob=lob,
        gidp=gidp,
        gidp_pct=gidp_pct,
        woba=woba,
        iso=iso,
        babip=babip,
        k_pct=k_pct,
        bb_pct=bb_pct,
        hr_pct=hr_pct,
        xbh_pct=xbh_pct,
        rs_pct=rs_pct,
    )


def _aggregate_pitching_stats_for_df(user_df: pd.DataFrame) -> dict[str, Any]:
    pa = len(user_df)
    if pa == 0:
        return {
            "pa": 0,
            "outs_pitched": 0,
            "h": 0,
            "r": 0,
            "hr": 0,
            "bb": 0,
            "so": 0,
            "hbp": 0,
            "avg": 0.0,
            "obp": 0.0,
            "slg": 0.0,
            "ops": 0.0,
            "woba": 0.0,
            "babip": 0.0,
            "k_pct": 0.0,
            "bb_pct": 0.0,
            "hr_pct": 0.0,
            "xbh_pct": 0.0,
            "era": None,
            "whip": None,
            "kbb": None,
        }

    results = _str_col(user_df, "result").str.lower()
    singles = results == "single"
    doubles = results == "double"
    triples = results == "triple"
    homeruns = results.isin(_HR_RESULTS)
    walks = results.isin(_WALK_RESULTS)
    hbp = results.isin(_HBP_RESULTS)

    sac_fly = _bool_col(user_df, "is_sac_fly")
    sac_bunt = _bool_col(user_df, "is_sac_bunt")
    strikeouts = _bool_col(user_df, "is_strikeout") | results.isin(
        ["strikeout", "strike out"]
    )

    singles_count = int(singles.sum())
    doubles_count = int(doubles.sum())
    triples_count = int(triples.sum())
    hr_count = int(homeruns.sum())
    bb_count = int(walks.sum())
    so_count = int(strikeouts.sum())
    hbp_count = int(hbp.sum())
    sf_count = int(sac_fly.sum())
    sh_count = int(sac_bunt.sum())

    hits = singles_count + doubles_count + triples_count + hr_count
    total_bases = singles_count + (2 * doubles_count) + (3 * triples_count) + (4 * hr_count)

    ab = pa - bb_count - hbp_count - sf_count - sh_count
    if ab < 0:
        ab = 0

    runs = int(_num_col(user_df, "runs_scored").sum())
    is_out = _bool_col(user_df, "is_out")
    double_play = _bool_col(user_df, "is_double_play")
    outs_pitched = int((is_out.astype(int) + double_play.astype(int)).sum())
    ip = outs_pitched / 3.0 if outs_pitched > 0 else 0.0

    avg = hits / ab if ab else 0.0
    obp_denom = ab + bb_count + hbp_count + sf_count
    obp = (hits + bb_count + hbp_count) / obp_denom if obp_denom else 0.0
    slg = total_bases / ab if ab else 0.0
    ops = obp + slg

    woba_denom = obp_denom
    if woba_denom:
        woba_num = (
            _WOBA_WEIGHTS["bb"] * bb_count
            + _WOBA_WEIGHTS["hbp"] * hbp_count
            + _WOBA_WEIGHTS["single"] * singles_count
            + _WOBA_WEIGHTS["double"] * doubles_count
            + _WOBA_WEIGHTS["triple"] * triples_count
            + _WOBA_WEIGHTS["hr"] * hr_count
        )
        woba = woba_num / float(woba_denom)
    else:
        woba = 0.0

    babip_denom = ab - so_count - hr_count + sf_count
    babip = (hits - hr_count) / babip_denom if babip_denom > 0 else 0.0

    k_pct = (so_count / pa) * 100 if pa else 0.0
    bb_pct = (bb_count / pa) * 100 if pa else 0.0
    hr_pct = (hr_count / pa) * 100 if pa else 0.0
    xbh_pct = ((doubles_count + triples_count + hr_count) / pa) * 100 if pa else 0.0

    era = (runs * 9.0 / ip) if ip > 0 else None
    whip = ((bb_count + hits) / ip) if ip > 0 else None
    kbb = (so_count / float(bb_count)) if bb_count > 0 else None

    return {
        "pa": pa,
        "outs_pitched": outs_pitched,
        "h": hits,
        "r": runs,
        "hr": hr_count,
        "bb": bb_count,
        "so": so_count,
        "hbp": hbp_count,
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": ops,
        "woba": woba,
        "babip": babip,
        "k_pct": k_pct,
        "bb_pct": bb_pct,
        "hr_pct": hr_pct,
        "xbh_pct": xbh_pct,
        "era": era,
        "whip": whip,
        "kbb": kbb,
    }


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _clamp01(value: float) -> float:
    return _clamp(value, 0.0, 1.0)


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _parse_pitch_types_param(pitch_types: Optional[str]) -> Optional[list[str]]:
    if not pitch_types:
        return None
    items = [pt.strip() for pt in pitch_types.split(",")]
    cleaned = [pt for pt in items if pt]
    return cleaned or None


def _normalize_pitch_label(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower()).split())


_PITCH_TYPE_NAME_ORDER_RAW: dict[str, list[str]] = {
    "fastball": ["4-Seam Fastball", "Cutter", "2-Seam Fastball"],
    "sinker": ["Sinker"],
    "curveball": [
        "12-6 Curve",
        "Curveball",
        "Knuckle-curve",
        "Slurve",
        "Sweeper",
        "Screwball",
        "Knuckle",
        "Sweeping Curve",
    ],
    "changeup": ["Circle Change", "Changeup", "Vulcan Change"],
    "slider": ["Slider"],
    "splitter": ["Splitter", "Palmball", "Forkball"],
}
_PITCH_TYPE_NAME_ORDER: dict[str, list[str]] = {
    key: [_normalize_pitch_label(name) for name in names]
    for key, names in _PITCH_TYPE_NAME_ORDER_RAW.items()
}
_PITCH_TYPE_FALLBACK_TOKENS: dict[str, list[str]] = {
    "fastball": ["4 seam", "4seam", "2 seam", "2seam", "cutter", "fastball"],
    "sinker": ["sinker"],
    "curveball": [
        "12 6",
        "curveball",
        "knuckle curve",
        "knucklecurve",
        "slurve",
        "sweeper",
        "sweeping curve",
        "screwball",
        "knuckle",
        "curve",
    ],
    "changeup": ["circle change", "changeup", "vulcan", "change"],
    "slider": ["slider"],
    "splitter": ["splitter", "palmball", "forkball", "split", "fork", "palm"],
}


def _clamp_speed_filter(value: Optional[int]) -> Optional[int]:
    if value is None:
        return None
    try:
        num = int(value)
    except Exception:
        return None
    return max(0, min(99, num))


def _build_pitch_type_speed_lookup(
    db: Session, pitcher_ids: list[int]
) -> dict[int, dict[str, int]]:
    if not pitcher_ids:
        return {}
    rows = db.execute(
        select(Card.mlb_id, Pitch.name, Pitch.speed)
        .join(Pitch, Pitch.card_id == Card.id)
        .where(Card.year == SHOW_CARD_YEAR, Card.mlb_id.in_(pitcher_ids))
    ).all()
    name_speed: dict[int, dict[str, int]] = {}
    for mlb_id, pitch_name, speed in rows:
        if mlb_id is None or pitch_name is None or speed is None:
            continue
        pid = int(mlb_id)
        label = _normalize_pitch_label(pitch_name)
        if not label:
            continue
        bucket = name_speed.setdefault(pid, {})
        prev = bucket.get(label)
        if prev is None or speed > prev:
            bucket[label] = int(speed)

    pitch_type_speed: dict[int, dict[str, int]] = {}
    for pid, names in name_speed.items():
        resolved: dict[str, int] = {}
        for pitch_type, candidates in _PITCH_TYPE_NAME_ORDER.items():
            for candidate in candidates:
                speed = names.get(candidate)
                if speed is not None:
                    resolved[pitch_type] = speed
                    break
            if pitch_type in resolved:
                continue
            tokens = _PITCH_TYPE_FALLBACK_TOKENS.get(pitch_type, [])
            if not tokens:
                continue
            for token in tokens:
                for label, speed in sorted(names.items()):
                    if token in label:
                        resolved[pitch_type] = speed
                        break
                if pitch_type in resolved:
                    break
        if resolved:
            pitch_type_speed[pid] = resolved
    return pitch_type_speed


def _filter_k_df_by_speed(
    db: Session,
    k_df: pd.DataFrame,
    *,
    min_speed: Optional[int] = None,
    max_speed: Optional[int] = None,
) -> pd.DataFrame:
    if k_df.empty:
        return k_df
    if min_speed is None and max_speed is None:
        return k_df

    pitcher_col = k_df.get("pitcher_mlb_id")
    if pitcher_col is None:
        return k_df.iloc[0:0]
    pitcher_numeric = pd.to_numeric(pitcher_col, errors="coerce")
    pitcher_ids = pitcher_numeric[pitcher_numeric.notna()]
    if pitcher_ids.empty:
        return k_df.iloc[0:0]

    unique_ids = sorted({int(v) for v in pitcher_ids.tolist()})
    pitch_speed_lookup = _build_pitch_type_speed_lookup(db, unique_ids)
    if not pitch_speed_lookup:
        return k_df.iloc[0:0]

    pitch_types = _str_col(k_df, "k_pitch_type").str.strip().str.lower()
    speed_vals: list[Optional[int]] = []
    for pid, pitch_type in zip(pitcher_numeric.tolist(), pitch_types.tolist()):
        if pid is None or (isinstance(pid, float) and math.isnan(pid)) or not pitch_type:
            speed_vals.append(None)
            continue
        speed = pitch_speed_lookup.get(int(pid), {}).get(pitch_type)
        speed_vals.append(speed)

    speed_series = pd.Series(speed_vals, index=k_df.index)
    mask = speed_series.notna()
    if min_speed is not None:
        mask &= speed_series >= min_speed
    if max_speed is not None:
        mask &= speed_series <= max_speed
    return k_df[mask]


def _compute_overall_hitting(power: int, timing: int, location: int, pa: int) -> int:
    p = power / 100.0
    t = timing / 100.0
    l = location / 100.0

    foundation = math.sqrt(max(t * l, 0.0))
    gate = 0.55 + 0.45 * foundation
    earned_power = p * gate

    raw = 0.55 * earned_power + 0.25 * foundation + 0.20 * (0.5 * t + 0.5 * l)
    raw2 = raw + 0.05 * min(t, l)

    conf = _clamp(pa / 250.0, 0.0, 1.0)
    final01 = 0.60 * (1.0 - conf) + conf * raw2
    final01 = _clamp(final01, 0.0, 1.0)
    return int(round(100.0 * final01))


def _terciles(values: pd.Series) -> tuple[float, float]:
    values = values.dropna()
    if values.empty:
        return 0.0, 0.0
    q1 = float(values.quantile(0.33))
    q2 = float(values.quantile(0.66))
    if q1 == q2:
        vmin = float(values.min())
        vmax = float(values.max())
        if vmin == vmax:
            return vmin, vmax
        step = (vmax - vmin) / 3.0
        return vmin + step, vmin + 2.0 * step
    return q1, q2


def _bin_3(value: float, q1: float, q2: float) -> int:
    if q1 == q2:
        return 1
    if value <= q1:
        return 0
    if value <= q2:
        return 1
    return 2


def _compute_strikeout_zone_map(
    df: pd.DataFrame,
    username: str,
    db: Session,
    view: Optional[str] = None,
    hitter_side: Optional[str] = None,
    pitcher_hand: Optional[str] = None,
    pitch_types: Optional[list[str]] = None,
    timing: Optional[str] = None,
    out_type: Optional[str] = None,
    min_speed: Optional[int] = None,
    max_speed: Optional[int] = None,
) -> StrikeoutZoneMapOut:
    user_hitting, user_pitching, _ = _user_masks(df, username)
    view_norm = (view or "hitting").strip().lower()
    if view_norm in ("pitching", "pitch"):
        user_df = df[user_pitching]
    else:
        user_df = df[user_hitting]
    zones = [[0, 0, 0], [0, 0, 0], [0, 0, 0]]
    outside = {
        "top_left": 0,
        "top": 0,
        "top_right": 0,
        "right": 0,
        "bottom_right": 0,
        "bottom": 0,
        "bottom_left": 0,
        "left": 0,
    }

    hitter_side_norm = (hitter_side or "").strip().upper()
    if hitter_side_norm in ("L", "R"):
        user_df = user_df[_str_col(user_df, "batter_side").str.upper() == hitter_side_norm]

    pitcher_hand_norm = (pitcher_hand or "").strip().upper()
    if pitcher_hand_norm in ("L", "R"):
        user_df = user_df[_str_col(user_df, "pitcher_throws").str.upper() == pitcher_hand_norm]

    pa = len(user_df)
    pitch_type_options: list[str] = []
    stats_by_zone = [[_blank_strikeout_stats() for _ in range(3)] for _ in range(3)]
    stats_by_outside = {k: _blank_strikeout_stats() for k in outside.keys()}
    empty_counts_by_zone = [[{"k": 0, "chase": 0, "look": 0, "eye": 0, "early": 0, "late": 0} for _ in range(3)] for _ in range(3)]
    empty_counts_by_outside = {k: {"k": 0, "chase": 0, "look": 0, "eye": 0, "early": 0, "late": 0} for k in outside.keys()}
    stats = _stats_from_counts(
        k_count=0,
        base_pa=pa,
        chase_k=0,
        look_k=0,
        eye_k=0,
        early_k=0,
        late_k=0,
    )
    if user_df.empty:
        return StrikeoutZoneMapOut(
            zones=zones,
            outside=outside,
            total=0,
            pa=pa,
            pitch_type_options=pitch_type_options,
            stats=stats,
            stats_by_zone=stats_by_zone,
            stats_by_outside=stats_by_outside,
            counts_by_zone=empty_counts_by_zone,
            counts_by_outside=empty_counts_by_outside,
        )

    results = _str_col(user_df, "result").str.lower()
    is_strikeout = _bool_col(user_df, "is_strikeout") | results.isin(["strikeout", "strike out"])
    all_k_df = user_df[is_strikeout]
    if not all_k_df.empty:
        pitch_type_series = _str_col(all_k_df, "k_pitch_type").str.strip().str.lower()
        pitch_type_options = sorted({pt for pt in pitch_type_series.tolist() if pt})

    k_df = all_k_df
    if pitch_types:
        pitch_set = {pt.strip().lower() for pt in pitch_types if pt and pt.strip()}
        if pitch_set:
            k_df = k_df[_str_col(k_df, "k_pitch_type").str.strip().str.lower().isin(pitch_set)]
    timing_norm = (timing or "").strip().lower()
    if timing_norm in ("early", "late"):
        k_df = k_df[_str_col(k_df, "k_timing").str.strip().str.lower() == timing_norm]
    out_type_norm = (out_type or "").strip().lower()
    if out_type_norm in ("chasing", "chase"):
        k_df = k_df[_bool_col(k_df, "k_is_chase")]
    elif out_type_norm in ("looking", "look"):
        k_df = k_df[_bool_col(k_df, "k_is_looking")]

    min_speed = _clamp_speed_filter(min_speed)
    max_speed = _clamp_speed_filter(max_speed)
    if min_speed is not None and max_speed is not None and min_speed > max_speed:
        min_speed, max_speed = max_speed, min_speed
    if min_speed is not None or max_speed is not None:
        k_df = _filter_k_df_by_speed(db, k_df, min_speed=min_speed, max_speed=max_speed)

    if k_df.empty:
        return StrikeoutZoneMapOut(
            zones=zones,
            outside=outside,
            total=0,
            pa=pa,
            pitch_type_options=pitch_type_options,
            stats=stats,
            stats_by_zone=stats_by_zone,
            stats_by_outside=stats_by_outside,
            counts_by_zone=empty_counts_by_zone,
            counts_by_outside=empty_counts_by_outside,
        )

    k_is_chase = _bool_col(k_df, "k_is_chase")
    k_is_looking = _bool_col(k_df, "k_is_looking")
    k_timing = _str_col(k_df, "k_timing").str.lower()
    k_count = len(k_df)
    chase_k = int(k_is_chase.sum())
    look_k = int(k_is_looking.sum())
    eye_k = int((k_is_chase | k_is_looking).sum())
    early_k = int((k_timing == "early").sum())
    late_k = int((k_timing == "late").sum())
    stats = _stats_from_counts(
        k_count=k_count,
        base_pa=pa,
        chase_k=chase_k,
        look_k=look_k,
        eye_k=eye_k,
        early_k=early_k,
        late_k=late_k,
    )
    k_height_raw = _str_col(k_df, "k_loc_height").str.lower()
    k_width_raw = _str_col(k_df, "k_loc_width").str.lower()

    height_num = pd.to_numeric(k_df.get("k_loc_height"), errors="coerce")
    width_num = pd.to_numeric(k_df.get("k_loc_width"), errors="coerce")

    height_is_middle = pd.Series([False] * len(k_df), index=k_df.index)
    height_is_high = pd.Series([False] * len(k_df), index=k_df.index)
    width_is_center = pd.Series([False] * len(k_df), index=k_df.index)

    height_cat = k_height_raw.replace({"mid": "middle", "center": "middle", "": "middle"})
    width_cat = k_width_raw.replace({"mid": "center", "middle": "center", "": "center"})

    height_cat_mask = height_cat.isin(["high", "middle", "low"])
    width_cat_mask = width_cat.isin(["left", "center", "right"])

    if height_cat_mask.any():
        height_is_middle[height_cat_mask] = height_cat[height_cat_mask] == "middle"
        height_is_high[height_cat_mask] = height_cat[height_cat_mask] == "high"
    if width_cat_mask.any():
        width_is_center[width_cat_mask] = width_cat[width_cat_mask] == "center"

    height_num_mask = height_num.notna()
    if height_num_mask.any():
        h_q1, h_q2 = _terciles(height_num[height_num_mask])
        h_bins = height_num[height_num_mask].apply(lambda x: _bin_3(float(x), h_q1, h_q2))
        height_is_middle[height_num_mask] = h_bins == 1
        height_is_high[height_num_mask] = h_bins == 2

    width_num_mask = width_num.notna()
    if width_num_mask.any():
        w_q1, w_q2 = _terciles(width_num[width_num_mask])
        w_bins = width_num[width_num_mask].apply(lambda x: _bin_3(float(x), w_q1, w_q2))
        width_is_center[width_num_mask] = w_bins == 1

    k_is_swinging = ~k_is_looking
    k_timing_blank = k_timing == ""
    swing_k = int(k_is_swinging.sum())
    heart_miss_k = int(
        (k_is_swinging & ~k_is_chase & k_timing_blank & height_is_middle & width_is_center).sum()
    )
    inzone_swing_k = int((k_is_swinging & ~k_is_chase & height_is_high).sum())
    stats["heart_miss_k_pct"] = (100.0 * heart_miss_k / float(swing_k)) if swing_k > 0 else 0.0
    stats["inzone_swing_k_pct"] = (100.0 * inzone_swing_k / float(k_count)) if k_count > 0 else 0.0

    numeric_mask = height_num.notna() & width_num.notna()

    cat_mask = height_cat.isin(["high", "middle", "low"]) & width_cat.isin(
        ["left", "center", "right"]
    )

    total = int((numeric_mask | cat_mask).sum())
    if total == 0:
        return StrikeoutZoneMapOut(
            zones=zones,
            outside=outside,
            total=0,
            pa=pa,
            pitch_type_options=pitch_type_options,
            stats=stats,
            stats_by_zone=stats_by_zone,
            stats_by_outside=stats_by_outside,
            counts_by_zone=empty_counts_by_zone,
            counts_by_outside=empty_counts_by_outside,
        )

    zone_counts = [[{"k": 0, "chase": 0, "look": 0, "eye": 0, "early": 0, "late": 0} for _ in range(3)] for _ in range(3)]
    outside_counts = {
        k: {"k": 0, "chase": 0, "look": 0, "eye": 0, "early": 0, "late": 0}
        for k in outside.keys()
    }

    def _apply_counts(target: dict[str, int], *, chase: bool, look: bool, timing: str) -> None:
        target["k"] += 1
        if chase:
            target["chase"] += 1
        if look:
            target["look"] += 1
        if chase or look:
            target["eye"] += 1
        if timing == "early":
            target["early"] += 1
        elif timing == "late":
            target["late"] += 1

    if numeric_mask.any():
        h_num = height_num[numeric_mask]
        w_num = width_num[numeric_mask]
        chase_num = k_is_chase[numeric_mask]
        look_num = k_is_looking[numeric_mask]
        timing_num = k_timing[numeric_mask]

        h_q1, h_q2 = _terciles(h_num)
        w_q1, w_q2 = _terciles(w_num)

        for h, w, chase, look, timing in zip(
            h_num.tolist(),
            w_num.tolist(),
            chase_num.tolist(),
            look_num.tolist(),
            timing_num.tolist(),
        ):
            h_bin = _bin_3(float(h), h_q1, h_q2)
            w_bin = 2 - _bin_3(float(w), w_q1, w_q2)
            row = 2 - h_bin
            col = w_bin

            if chase:
                if h_bin == 1 and w_bin == 1:
                    continue
                if h_bin == 0 and w_bin == 0:
                    outside["top_left"] += 1
                    _apply_counts(outside_counts["top_left"], chase=chase, look=look, timing=timing)
                elif h_bin == 0 and w_bin == 1:
                    outside["top"] += 1
                    _apply_counts(outside_counts["top"], chase=chase, look=look, timing=timing)
                elif h_bin == 0 and w_bin == 2:
                    outside["top_right"] += 1
                    _apply_counts(outside_counts["top_right"], chase=chase, look=look, timing=timing)
                elif h_bin == 1 and w_bin == 0:
                    outside["left"] += 1
                    _apply_counts(outside_counts["left"], chase=chase, look=look, timing=timing)
                elif h_bin == 1 and w_bin == 2:
                    outside["right"] += 1
                    _apply_counts(outside_counts["right"], chase=chase, look=look, timing=timing)
                elif h_bin == 2 and w_bin == 0:
                    outside["bottom_left"] += 1
                    _apply_counts(outside_counts["bottom_left"], chase=chase, look=look, timing=timing)
                elif h_bin == 2 and w_bin == 1:
                    outside["bottom"] += 1
                    _apply_counts(outside_counts["bottom"], chase=chase, look=look, timing=timing)
                elif h_bin == 2 and w_bin == 2:
                    outside["bottom_right"] += 1
                    _apply_counts(outside_counts["bottom_right"], chase=chase, look=look, timing=timing)
                else:
                    zones[row][col] += 1
                    _apply_counts(zone_counts[row][col], chase=chase, look=look, timing=timing)
                continue

            zones[row][col] += 1
            _apply_counts(zone_counts[row][col], chase=chase, look=look, timing=timing)

    if cat_mask.any():
        height_map = {"high": 0, "middle": 1, "low": 2}
        width_map = {"left": 2, "center": 1, "right": 0}
        height_vals = height_cat[cat_mask].tolist()
        width_vals = width_cat[cat_mask].tolist()
        chase_vals = k_is_chase[cat_mask].tolist()
        look_vals = k_is_looking[cat_mask].tolist()
        timing_vals = k_timing[cat_mask].tolist()

        for h, w, chase, look, timing in zip(
            height_vals,
            width_vals,
            chase_vals,
            look_vals,
            timing_vals,
        ):
            row = height_map.get(h)
            col = width_map.get(w)
            if row is None or col is None:
                continue
            if chase:
                if row == 1 and col == 1:
                    continue
                if row == 0 and col == 0:
                    outside["top_left"] += 1
                    _apply_counts(outside_counts["top_left"], chase=chase, look=look, timing=timing)
                elif row == 0 and col == 1:
                    outside["top"] += 1
                    _apply_counts(outside_counts["top"], chase=chase, look=look, timing=timing)
                elif row == 0 and col == 2:
                    outside["top_right"] += 1
                    _apply_counts(outside_counts["top_right"], chase=chase, look=look, timing=timing)
                elif row == 1 and col == 0:
                    outside["left"] += 1
                    _apply_counts(outside_counts["left"], chase=chase, look=look, timing=timing)
                elif row == 1 and col == 2:
                    outside["right"] += 1
                    _apply_counts(outside_counts["right"], chase=chase, look=look, timing=timing)
                elif row == 2 and col == 0:
                    outside["bottom_left"] += 1
                    _apply_counts(outside_counts["bottom_left"], chase=chase, look=look, timing=timing)
                elif row == 2 and col == 1:
                    outside["bottom"] += 1
                    _apply_counts(outside_counts["bottom"], chase=chase, look=look, timing=timing)
                elif row == 2 and col == 2:
                    outside["bottom_right"] += 1
                    _apply_counts(outside_counts["bottom_right"], chase=chase, look=look, timing=timing)
                else:
                    zones[row][col] += 1
                    _apply_counts(zone_counts[row][col], chase=chase, look=look, timing=timing)
            else:
                zones[row][col] += 1
                _apply_counts(zone_counts[row][col], chase=chase, look=look, timing=timing)

    base_pa = pa
    for r in range(3):
        for c in range(3):
            counts = zone_counts[r][c]
            stats_by_zone[r][c] = _stats_from_counts(
                k_count=counts["k"],
                base_pa=base_pa,
                chase_k=counts["chase"],
                look_k=counts["look"],
                eye_k=counts["eye"],
                early_k=counts["early"],
                late_k=counts["late"],
            )

    for key, counts in outside_counts.items():
        stats_by_outside[key] = _stats_from_counts(
            k_count=counts["k"],
            base_pa=base_pa,
            chase_k=counts["chase"],
            look_k=counts["look"],
            eye_k=counts["eye"],
            early_k=counts["early"],
            late_k=counts["late"],
        )

    return StrikeoutZoneMapOut(
        zones=zones,
        outside=outside,
        total=total,
        pa=pa,
        pitch_type_options=pitch_type_options,
        stats=stats,
        stats_by_zone=stats_by_zone,
        stats_by_outside=stats_by_outside,
        counts_by_zone=zone_counts,
        counts_by_outside=outside_counts,
    )


_HIT_RESULTS = {"single", "double", "triple", "homerun", "home_run", "home run", "home-run", "hr"}
_HR_RESULTS = {"homerun", "home_run", "home run", "home-run", "hr"}
_WALK_RESULTS = {"walk", "intentional walk", "intentional_walk", "ibb", "bb"}
_HBP_RESULTS = {"hit_by_pitch", "hit by pitch", "hbp"}

_WOBA_WEIGHTS = {
    "bb": 0.69,
    "hbp": 0.72,
    "single": 0.88,
    "double": 1.247,
    "triple": 1.578,
    "hr": 2.031,
}

_HIT_ZONE_KEYS = [
    "infield_left",
    "infield_right",
    "outfield_left",
    "outfield_center",
    "outfield_right",
    "homerun_left",
    "homerun_center",
    "homerun_right",
]

def _normalize_hit_direction(value: str) -> Optional[str]:
    if not value:
        return None
    raw = re.sub(r"[^a-z]", "", str(value).lower())
    if not raw:
        return None
    if raw.startswith("l") or "left" in raw:
        return "left"
    if raw.startswith("r") or "right" in raw:
        return "right"
    if "center" in raw or "centre" in raw or "middle" in raw:
        return "center"
    if "oppo" in raw or "opposite" in raw:
        return "right"
    if "pull" in raw:
        return "left"
    return None


def _normalize_batted_type(value: str) -> Optional[str]:
    if not value:
        return None
    raw = re.sub(r"[^a-z]", "", str(value).lower())
    if not raw:
        return None
    if "ground" in raw:
        return "ground"
    if "line" in raw:
        return "line"
    if "fly" in raw:
        return "fly"
    if "popup" in raw or raw == "pop":
        return "popup"
    return None


def _fielder_info(value: str) -> tuple[Optional[str], Optional[str]]:
    if not value:
        return None, None
    raw = re.sub(r"[^a-z0-9]", "", str(value).lower())
    if not raw:
        return None, None
    if raw in {"7", "lf", "leftfield"}:
        return "left", "outfield"
    if raw in {"8", "cf", "centerfield", "centrefield"}:
        return "center", "outfield"
    if raw in {"9", "rf", "rightfield"}:
        return "right", "outfield"
    if raw in {"5", "3b", "thirdbase"}:
        return "left", "infield"
    if raw in {"6", "ss", "shortstop"}:
        return "left", "infield"
    if raw in {"4", "2b", "secondbase"}:
        return "right", "infield"
    if raw in {"3", "1b", "firstbase"}:
        return "right", "infield"
    if raw in {"1", "p", "pitcher"}:
        return "center", "infield"
    if raw in {"2", "c", "catcher"}:
        return "center", "infield"
    return None, None


def _resolve_infield_side(direction: Optional[str], batter_side: str) -> str:
    if direction == "left":
        return "left"
    if direction == "right":
        return "right"
    side = (batter_side or "").strip().upper()
    if side == "L":
        return "right"
    if side == "R":
        return "left"
    return "right"


def _compute_hit_data_map(
    df: pd.DataFrame,
    username: str,
    view: Optional[str] = None,
    hitter_side: Optional[str] = None,
    pitcher_hand: Optional[str] = None,
    stat: Optional[str] = None,
    base_state: Optional[str] = None,
    outs: Optional[str] = None,
    ab_count: Optional[str] = None,
    min_seen: Optional[int] = None,
    max_seen: Optional[int] = None,
    pitcher_count: Optional[str] = None,
    focus_zone: Optional[str] = None,
) -> HitDataMapOut:
    user_hitting, user_pitching, _ = _user_masks(df, username)
    view_norm = (view or "hitting").strip().lower()
    if view_norm in ("pitching", "pitch"):
        user_df = df[user_pitching]
    else:
        user_df = df[user_hitting]

    hitter_side_norm = (hitter_side or "").strip().upper()
    if hitter_side_norm in ("L", "R"):
        user_df = user_df[_str_col(user_df, "batter_side").str.upper() == hitter_side_norm]

    pitcher_hand_norm = (pitcher_hand or "").strip().upper()
    if pitcher_hand_norm in ("L", "R"):
        user_df = user_df[_str_col(user_df, "pitcher_throws").str.upper() == pitcher_hand_norm]

    focus_zone_norm = (focus_zone or "").strip().lower()
    if focus_zone_norm not in _HIT_ZONE_KEYS:
        focus_zone_norm = ""

    base_state_norm = (base_state or "").strip().lower()
    if base_state_norm in ("runner_on", "risp", "loaded"):
        runner_on_first = _bool_col(user_df, "runner_on_first")
        runner_on_second = _bool_col(user_df, "runner_on_second")
        runner_on_third = _bool_col(user_df, "runner_on_third")
        if base_state_norm == "runner_on":
            user_df = user_df[runner_on_first | runner_on_second | runner_on_third]
        elif base_state_norm == "risp":
            user_df = user_df[runner_on_second | runner_on_third]
        else:
            user_df = user_df[runner_on_first & runner_on_second & runner_on_third]

    outs_norm = (outs or "").strip().lower()
    if outs_norm in ("0", "1", "2"):
        outs_col = pd.to_numeric(_str_col(user_df, "outs_before"), errors="coerce")
        user_df = user_df[outs_col == int(outs_norm)]

    ab_norm = (ab_count or "").strip().lower()
    if ab_norm in ("1", "2", "3plus"):
        ab_col = pd.to_numeric(_str_col(user_df, "num_abs_with_hitter"), errors="coerce")
        if ab_norm == "1":
            user_df = user_df[ab_col == 1]
        elif ab_norm == "2":
            user_df = user_df[ab_col == 2]
        else:
            user_df = user_df[ab_col >= 3]

    min_seen = _to_int(min_seen)
    max_seen = _to_int(max_seen)
    if min_seen is not None and max_seen is not None and min_seen > max_seen:
        min_seen, max_seen = max_seen, min_seen
    if min_seen is not None or max_seen is not None:
        seen_col = pd.to_numeric(_str_col(user_df, "times_seen_pitcher"), errors="coerce")
        if min_seen is not None:
            user_df = user_df[seen_col >= min_seen]
        if max_seen is not None:
            user_df = user_df[seen_col <= max_seen]

    pitcher_count_norm = (pitcher_count or "").strip().lower()
    if pitcher_count_norm in ("1", "2", "3plus"):
        pitcher_col = pd.to_numeric(_str_col(user_df, "num_pitchers"), errors="coerce")
        if pitcher_count_norm == "1":
            user_df = user_df[pitcher_col == 1]
        elif pitcher_count_norm == "2":
            user_df = user_df[pitcher_col == 2]
        else:
            user_df = user_df[pitcher_col >= 3]

    pa = len(user_df)
    zone_counts = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_hits = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_hr = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_total_bases = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_ab = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_sac_fly = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_bb = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_hbp = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_1b = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_2b = {k: 0 for k in _HIT_ZONE_KEYS}
    zone_3b = {k: 0 for k in _HIT_ZONE_KEYS}
    bip = 0
    line_drives = 0
    fly_balls = 0
    ground_balls = 0
    popups = 0
    popups_nopp = 0
    perfect_perfect = 0
    sweet_spot = 0
    air_total = 0
    pulled_air = 0
    oppo_air = 0

    stat_norm = (stat or "count").strip().lower()
    if stat_norm not in {"count", "share", "babip", "woba", "slug"}:
        stat_norm = "count"

    if user_df.empty:
        return HitDataMapOut(
            zones={k: 0.0 for k in _HIT_ZONE_KEYS},
            total=0,
            pa=0,
            stat=stat_norm,
            stats={
                "sweet_spot_pct": 0.0,
                "popup_rate": 0.0,
                "flyball_rate": 0.0,
                "groundball_rate": 0.0,
                "gb_air_ratio": 0.0,
                "pulled_air_rate": 0.0,
                "oppo_air_rate": 0.0,
                "perfect_perfect_pct": 0.0,
                "extreme_contact_nopp_pct": 0.0,
            },
        )

    results = _str_col(user_df, "result").str.lower()
    batted_types = _str_col(user_df, "batted_ball_type").str.lower()
    hit_dirs = _str_col(user_df, "hit_direction")
    fielder_pos = _str_col(user_df, "fielder_pos")
    batter_side = _str_col(user_df, "batter_side")
    is_sac_fly = _bool_col(user_df, "is_sac_fly")
    is_sac_bunt = _bool_col(user_df, "is_sac_bunt")
    is_perfect = _bool_col(user_df, "is_perfect_perfect")

    for idx in user_df.index:
        result = results.at[idx]
        hit_dir = _normalize_hit_direction(hit_dirs.at[idx])
        f_dir, f_zone = _fielder_info(fielder_pos.at[idx])
        batted = batted_types.at[idx]
        batted_norm = _normalize_batted_type(batted)
        batter = batter_side.at[idx]

        is_hit = result in _HIT_RESULTS
        is_hr = result in _HR_RESULTS
        is_ground = batted_norm == "ground"

        zone_key: Optional[str] = None
        if is_hr:
            direction = hit_dir or f_dir or "center"
            zone_key = f"homerun_{direction}"
        elif is_hit:
            if is_ground:
                side = _resolve_infield_side(hit_dir or f_dir, batter)
                zone_key = f"infield_{side}"
            else:
                direction = hit_dir or f_dir or "center"
                zone_key = f"outfield_{direction}"
        else:
            if f_zone == "outfield":
                direction = f_dir or "center"
                zone_key = f"outfield_{direction}"
            elif f_zone == "infield":
                side = _resolve_infield_side(f_dir, batter)
                zone_key = f"infield_{side}"
            elif is_ground:
                side = _resolve_infield_side(hit_dir, batter)
                zone_key = f"infield_{side}"
            elif hit_dir:
                zone_key = f"outfield_{hit_dir}"

        focus_match = not focus_zone_norm or zone_key == focus_zone_norm
        if focus_match:
            is_bip = batted_norm in {"ground", "fly", "line", "popup"} or is_hr
            if is_bip:
                bip += 1
                if batted_norm == "line":
                    line_drives += 1
                elif batted_norm == "fly":
                    fly_balls += 1
                elif batted_norm == "ground":
                    ground_balls += 1
                elif batted_norm == "popup":
                    popups += 1
                    if not is_perfect.at[idx]:
                        popups_nopp += 1

                if is_perfect.at[idx]:
                    perfect_perfect += 1

                deep_fly = (
                    batted_norm == "fly"
                    and (
                        is_hr
                        or (zone_key and zone_key.startswith(("outfield_", "homerun_")))
                        or f_zone == "outfield"
                    )
                )
                if batted_norm == "line" or deep_fly or is_perfect.at[idx]:
                    sweet_spot += 1

                is_air = batted_norm in {"fly", "line"} or is_hr
                if is_air:
                    air_total += 1
                    direction = hit_dir or f_dir
                    batter_side_norm = str(batter).strip().upper()
                    pull_side = None
                    oppo_side = None
                    if batter_side_norm == "L":
                        pull_side = "right"
                        oppo_side = "left"
                    elif batter_side_norm == "R":
                        pull_side = "left"
                        oppo_side = "right"
                    if direction and pull_side and direction == pull_side:
                        pulled_air += 1
                    elif direction and oppo_side and direction == oppo_side:
                        oppo_air += 1

        if zone_key not in zone_counts:
            continue

        zone_counts[zone_key] += 1
        if is_hit:
            zone_hits[zone_key] += 1
            if result in _HR_RESULTS:
                zone_total_bases[zone_key] += 4
                zone_1b[zone_key] += 0
                zone_2b[zone_key] += 0
                zone_3b[zone_key] += 0
            elif result == "triple":
                zone_total_bases[zone_key] += 3
                zone_3b[zone_key] += 1
            elif result == "double":
                zone_total_bases[zone_key] += 2
                zone_2b[zone_key] += 1
            else:
                zone_total_bases[zone_key] += 1
                zone_1b[zone_key] += 1
        if is_hr:
            zone_hr[zone_key] += 1
        if is_sac_fly.at[idx]:
            zone_sac_fly[zone_key] += 1
        if result in _WALK_RESULTS:
            zone_bb[zone_key] += 1
        if result in _HBP_RESULTS:
            zone_hbp[zone_key] += 1

        if not (result in _WALK_RESULTS or result in _HBP_RESULTS or is_sac_fly.at[idx] or is_sac_bunt.at[idx]):
            zone_ab[zone_key] += 1

    total = sum(zone_counts.values())
    if stat_norm == "count":
        zones = {k: float(zone_counts[k]) for k in _HIT_ZONE_KEYS}
    elif stat_norm == "share":
        zones = {
            k: (100.0 * zone_counts[k] / float(total) if total > 0 else 0.0)
            for k in _HIT_ZONE_KEYS
        }
    elif stat_norm == "babip":
        zones = {}
        for k in _HIT_ZONE_KEYS:
            denom = zone_ab[k] - zone_hr[k] + zone_sac_fly[k]
            num = zone_hits[k] - zone_hr[k]
            zones[k] = (num / float(denom)) if denom > 0 else 0.0
    elif stat_norm == "slug":
        zones = {}
        for k in _HIT_ZONE_KEYS:
            ab = zone_ab[k]
            if ab <= 0:
                zones[k] = 0.0
            else:
                zones[k] = zone_total_bases[k] / float(ab)
    else:
        zones = {}
        for k in _HIT_ZONE_KEYS:
            denom = zone_ab[k] + zone_bb[k] + zone_hbp[k] + zone_sac_fly[k]
            if denom <= 0:
                zones[k] = 0.0
                continue
            numerator = (
                _WOBA_WEIGHTS["bb"] * zone_bb[k]
                + _WOBA_WEIGHTS["hbp"] * zone_hbp[k]
                + _WOBA_WEIGHTS["single"] * zone_1b[k]
                + _WOBA_WEIGHTS["double"] * zone_2b[k]
                + _WOBA_WEIGHTS["triple"] * zone_3b[k]
                + _WOBA_WEIGHTS["hr"] * zone_hr[k]
            )
            zones[k] = numerator / float(denom)

    air_denominator = float(air_total) if air_total > 0 else 0.0
    stats = {
        "sweet_spot_pct": (100.0 * sweet_spot / float(bip)) if bip > 0 else 0.0,
        "popup_rate": (100.0 * popups / float(bip)) if bip > 0 else 0.0,
        "flyball_rate": (100.0 * fly_balls / float(bip)) if bip > 0 else 0.0,
        "groundball_rate": (100.0 * ground_balls / float(bip)) if bip > 0 else 0.0,
        "gb_air_ratio": (100.0 * ground_balls / float(line_drives + fly_balls)) if (line_drives + fly_balls) > 0 else 0.0,
        "pulled_air_rate": (100.0 * pulled_air / air_denominator) if air_denominator > 0 else 0.0,
        "oppo_air_rate": (100.0 * oppo_air / air_denominator) if air_denominator > 0 else 0.0,
        "perfect_perfect_pct": (100.0 * perfect_perfect / float(bip)) if bip > 0 else 0.0,
        "extreme_contact_nopp_pct": (100.0 * (ground_balls + popups_nopp) / float(bip)) if bip > 0 else 0.0,
    }

    return HitDataMapOut(zones=zones, total=total, pa=pa, stat=stat_norm, stats=stats)


def _compute_hit_data_stats_for_df(user_df: pd.DataFrame) -> dict[str, float]:
    if user_df.empty:
        return {
            "sweet_spot_pct": 0.0,
            "popup_rate": 0.0,
            "flyball_rate": 0.0,
            "groundball_rate": 0.0,
            "gb_air_ratio": 0.0,
            "pulled_air_rate": 0.0,
            "oppo_air_rate": 0.0,
            "perfect_perfect_pct": 0.0,
            "extreme_contact_nopp_pct": 0.0,
        }

    results = _str_col(user_df, "result").str.lower()
    batted_types = _str_col(user_df, "batted_ball_type").str.lower()
    hit_dirs = _str_col(user_df, "hit_direction")
    fielder_pos = _str_col(user_df, "fielder_pos")
    batter_side = _str_col(user_df, "batter_side")
    is_perfect = _bool_col(user_df, "is_perfect_perfect")

    bip = 0
    line_drives = 0
    fly_balls = 0
    ground_balls = 0
    popups = 0
    popups_nopp = 0
    perfect_perfect = 0
    sweet_spot = 0
    air_total = 0
    pulled_air = 0
    oppo_air = 0

    for idx in user_df.index:
        result = results.at[idx]
        hit_dir = _normalize_hit_direction(hit_dirs.at[idx])
        f_dir, f_zone = _fielder_info(fielder_pos.at[idx])
        batted_norm = _normalize_batted_type(batted_types.at[idx])
        batter = batter_side.at[idx]

        is_hit = result in _HIT_RESULTS
        is_hr = result in _HR_RESULTS
        is_ground = batted_norm == "ground"

        zone_key: Optional[str] = None
        if is_hr:
            direction = hit_dir or f_dir or "center"
            zone_key = f"homerun_{direction}"
        elif is_hit:
            if is_ground:
                side = _resolve_infield_side(hit_dir or f_dir, batter)
                zone_key = f"infield_{side}"
            else:
                direction = hit_dir or f_dir or "center"
                zone_key = f"outfield_{direction}"
        else:
            if f_zone == "outfield":
                direction = f_dir or "center"
                zone_key = f"outfield_{direction}"
            elif f_zone == "infield":
                side = _resolve_infield_side(f_dir, batter)
                zone_key = f"infield_{side}"
            elif is_ground:
                side = _resolve_infield_side(hit_dir, batter)
                zone_key = f"infield_{side}"
            elif hit_dir:
                zone_key = f"outfield_{hit_dir}"

        is_bip = batted_norm in {"ground", "fly", "line", "popup"} or is_hr
        if not is_bip:
            continue

        bip += 1
        if batted_norm == "line":
            line_drives += 1
        elif batted_norm == "fly":
            fly_balls += 1
        elif batted_norm == "ground":
            ground_balls += 1
        elif batted_norm == "popup":
            popups += 1
            if not is_perfect.at[idx]:
                popups_nopp += 1

        if is_perfect.at[idx]:
            perfect_perfect += 1

        deep_fly = (
            batted_norm == "fly"
            and (
                is_hr
                or (zone_key and zone_key.startswith(("outfield_", "homerun_")))
                or f_zone == "outfield"
            )
        )
        if batted_norm == "line" or deep_fly or is_perfect.at[idx]:
            sweet_spot += 1

        is_air = batted_norm in {"fly", "line"} or is_hr
        if is_air:
            air_total += 1
            direction = hit_dir or f_dir
            batter_side_norm = str(batter).strip().upper()
            pull_side = None
            oppo_side = None
            if batter_side_norm == "L":
                pull_side = "right"
                oppo_side = "left"
            elif batter_side_norm == "R":
                pull_side = "left"
                oppo_side = "right"
            if direction and pull_side and direction == pull_side:
                pulled_air += 1
            elif direction and oppo_side and direction == oppo_side:
                oppo_air += 1

    air_denominator = float(air_total) if air_total > 0 else 0.0
    return {
        "sweet_spot_pct": (100.0 * sweet_spot / float(bip)) if bip > 0 else 0.0,
        "popup_rate": (100.0 * popups / float(bip)) if bip > 0 else 0.0,
        "flyball_rate": (100.0 * fly_balls / float(bip)) if bip > 0 else 0.0,
        "groundball_rate": (100.0 * ground_balls / float(bip)) if bip > 0 else 0.0,
        "gb_air_ratio": (100.0 * ground_balls / float(line_drives + fly_balls))
        if (line_drives + fly_balls) > 0
        else 0.0,
        "pulled_air_rate": (100.0 * pulled_air / air_denominator) if air_denominator > 0 else 0.0,
        "oppo_air_rate": (100.0 * oppo_air / air_denominator) if air_denominator > 0 else 0.0,
        "perfect_perfect_pct": (100.0 * perfect_perfect / float(bip)) if bip > 0 else 0.0,
        "extreme_contact_nopp_pct": (100.0 * (ground_balls + popups_nopp) / float(bip)) if bip > 0 else 0.0,
    }


def _power_counts(df: pd.DataFrame) -> dict[str, float]:
    pa = len(df)
    if pa == 0:
        return {
            "pa": 0,
            "ab": 0,
            "hits": 0,
            "hr": 0,
            "total_bases": 0,
            "iso": 0.0,
            "hr_rate": 0.0,
            "power_rate": 0.0,
        }

    results = df.get("result")
    if results is None:
        results = pd.Series([""] * pa)
    results = results.fillna("").astype(str).str.lower()

    singles = results == "single"
    doubles = results == "double"
    triples = results == "triple"
    homeruns = results.isin(["homerun", "home_run", "home run"])
    walks = results == "walk"
    hbp = results.isin(["hit_by_pitch", "hit by pitch", "hbp"])

    sac_fly_flags = df.get("is_sac_fly")
    if sac_fly_flags is None:
        sac_fly_flags = pd.Series([False] * pa)
    sac_flies = sac_fly_flags.fillna(False).astype(bool)

    sac_bunt_flags = df.get("is_sac_bunt")
    if sac_bunt_flags is None:
        sac_bunt_flags = pd.Series([False] * pa)
    sac_bunts = sac_bunt_flags.fillna(False).astype(bool)

    mult = _difficulty_multiplier_series(df)
    hit_mask = singles | doubles | triples | homeruns
    weighted_hits = float((hit_mask.astype(float) * mult).sum())
    weighted_hr = float((homeruns.astype(float) * mult).sum())
    weighted_total_bases = float(
        (singles.astype(float) * mult).sum()
        + (2.0 * doubles.astype(float) * mult).sum()
        + (3.0 * triples.astype(float) * mult).sum()
        + (4.0 * homeruns.astype(float) * mult).sum()
    )
    ab_mask = ~(walks | hbp | sac_flies | sac_bunts)
    non_hit_ab = ab_mask & ~hit_mask
    weighted_non_hit_ab = float(non_hit_ab.astype(float).sum())
    weighted_ab = weighted_hits + weighted_non_hit_ab
    weighted_pa = (
        weighted_ab
        + float((walks.astype(float) * mult).sum())
        + float((hbp.astype(float) * mult).sum())
        + float(sac_flies.astype(float).sum())
    )

    hits = int(hit_mask.sum())
    hr = int(homeruns.sum())
    total_bases = int(hit_mask.astype(int).sum() + doubles.astype(int).sum() + 2 * triples.astype(int).sum() + 3 * homeruns.astype(int).sum())
    ab = int(ab_mask.sum())

    iso = (weighted_total_bases - weighted_hits) / weighted_ab if weighted_ab > 0 else 0.0
    hr_rate = weighted_hr / weighted_pa if weighted_pa > 0 else 0.0
    power_rate = 0.7 * iso + 0.3 * hr_rate

    return {
        "pa": pa,
        "ab": ab,
        "hits": hits,
        "hr": hr,
        "total_bases": total_bases,
        "iso": iso,
        "hr_rate": hr_rate,
        "power_rate": power_rate,
    }


def _group_power_rates(df: pd.DataFrame, group_cols: Iterable[str]) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=float)

    missing = [col for col in group_cols if col not in df.columns]
    if missing:
        return pd.Series([], dtype=float)

    results = df.get("result")
    if results is None:
        results = pd.Series([""] * len(df))
    results = results.fillna("").astype(str).str.lower()

    sf = df.get("is_sac_fly")
    if sf is None:
        sf = pd.Series([False] * len(df), index=df.index)
    sh = df.get("is_sac_bunt")
    if sh is None:
        sh = pd.Series([False] * len(df), index=df.index)

    mult = _difficulty_multiplier_series(df)
    hit_mask = (results == "single") | (results == "double") | (results == "triple") | results.isin(["homerun", "home_run", "home run"])

    data = pd.DataFrame(
        {
            "single_w": (results == "single").astype(float) * mult,
            "double_w": (results == "double").astype(float) * mult,
            "triple_w": (results == "triple").astype(float) * mult,
            "hr_w": results.isin(["homerun", "home_run", "home run"]).astype(float) * mult,
            "non_hit_ab": (~hit_mask & ~((results == "walk") | results.isin(["hit_by_pitch", "hit by pitch", "hbp"]) | sf.fillna(False).astype(bool) | sh.fillna(False).astype(bool))).astype(float),
            "walk_w": (results == "walk").astype(float) * mult,
            "hbp_w": results.isin(["hit_by_pitch", "hit by pitch", "hbp"]).astype(float) * mult,
            "sf": sf.fillna(False).astype(bool).astype(float),
        },
        index=df.index,
    )

    for col in group_cols:
        data[col] = df[col]

    grouped = data.groupby(list(group_cols)).sum(numeric_only=True)
    hits = grouped["single_w"] + grouped["double_w"] + grouped["triple_w"] + grouped["hr_w"]
    total_bases = (
        grouped["single_w"]
        + (2 * grouped["double_w"])
        + (3 * grouped["triple_w"])
        + (4 * grouped["hr_w"])
    )
    ab = hits + grouped["non_hit_ab"]
    pa_eff = ab + grouped["walk_w"] + grouped["hbp_w"] + grouped["sf"]

    iso = pd.Series(0.0, index=grouped.index)
    iso = iso.where(ab <= 0, (total_bases - hits) / ab)

    hr_rate = pd.Series(0.0, index=grouped.index)
    hr_rate = hr_rate.where(pa_eff <= 0, grouped["hr_w"] / pa_eff)

    return 0.7 * iso + 0.3 * hr_rate


def _compute_power_skill(
    df: pd.DataFrame,
    username: str,
    db: Session,
    pa_smooth: int = 150,
    reference_hitting_df: Optional[pd.DataFrame] = None,
) -> PowerSkillOut:
    user_hitting, _, opponent_hitting = _user_masks(df, username)
    user_df = df[user_hitting]
    opp_df = reference_hitting_df if reference_hitting_df is not None else df[opponent_hitting]

    user_counts = _power_counts(user_df)
    sample_counts = _power_counts(opp_df)

    user_rate = float(user_counts["power_rate"])
    sample_mean_rate = float(sample_counts["power_rate"])
    pa_user = int(user_counts["pa"])

    sample_rates = _group_power_rates(opp_df, ["game_id"])
    sample_sd = float(sample_rates.std(ddof=0)) if len(sample_rates) else 0.0

    smoothed_rate = user_rate
    if pa_user + pa_smooth > 0:
        smoothed_rate = (user_rate * pa_user + sample_mean_rate * pa_smooth) / (
            pa_user + pa_smooth
        )

    diff_score = 0.0
    if pa_user > 0 and "difficulty_id" in user_df.columns:
        difficulties = user_df.get("difficulty_id")
        if difficulties is not None:
            for diff in difficulties.dropna().unique().tolist():
                user_d = user_df[user_df["difficulty_id"] == diff]
                opp_d = opp_df[opp_df["difficulty_id"] == diff]
                user_d_counts = _power_counts(user_d)
                opp_d_counts = _power_counts(opp_d)

                user_rate_d = float(user_d_counts["power_rate"])
                sample_rate_d = float(opp_d_counts["power_rate"])
                pa_user_d = int(user_d_counts["pa"])
                pa_opp_d = int(opp_d_counts["pa"])

                if pa_user_d < _POWER_MIN_DIFF_PA or pa_opp_d < _POWER_MIN_DIFF_PA:
                    continue

                sample_rates_d = _group_power_rates(opp_d, ["game_id"])
                sample_sd_d = float(sample_rates_d.std(ddof=0)) if len(sample_rates_d) else sample_sd
                denom = max(sample_sd_d, _POWER_SD_FLOOR)
                rel_d = _clamp(
                    (user_rate_d - sample_rate_d) / denom,
                    -_POWER_Z_CLAMP_PER_DIFF,
                    _POWER_Z_CLAMP_PER_DIFF,
                )
                diff_score += (pa_user_d / pa_user) * rel_d

    elev_robust = 0.5
    if "ballpark_id" in user_df.columns and "ballpark_id" in opp_df.columns:
        user_ballpark_ids = pd.to_numeric(user_df["ballpark_id"], errors="coerce")
        opp_ballpark_ids = pd.to_numeric(opp_df["ballpark_id"], errors="coerce")
        unique_ids = sorted(
            {
                int(v)
                for v in pd.concat([user_ballpark_ids, opp_ballpark_ids], ignore_index=True)
                .dropna()
                .unique()
                .tolist()
            }
        )
        if unique_ids:
            elevations = {
                row.id: (row.elevation if row.elevation is not None else None)
                for row in db.scalars(select(ShowBallParks).where(ShowBallParks.id.in_(unique_ids))).all()
            }
            opp_elev = opp_ballpark_ids.map(elevations)
            user_elev = user_ballpark_ids.map(elevations)

            opp_elev_non_null = opp_elev.dropna()
            user_elev_non_null = user_elev.dropna()

            if len(opp_elev_non_null) >= 10 and len(user_elev_non_null) >= 10:
                q_low = float(opp_elev_non_null.quantile(0.33))
                q_high = float(opp_elev_non_null.quantile(0.66))

                def bucket_mask(series: pd.Series, bucket: str) -> pd.Series:
                    if bucket == "low":
                        return series <= q_low
                    if bucket == "high":
                        return series > q_high
                    return (series > q_low) & (series <= q_high)

                opp_low = opp_df[bucket_mask(opp_elev, "low") & opp_elev.notna()]
                opp_high = opp_df[bucket_mask(opp_elev, "high") & opp_elev.notna()]
                user_low = user_df[bucket_mask(user_elev, "low") & user_elev.notna()]
                user_high = user_df[bucket_mask(user_elev, "high") & user_elev.notna()]

                opp_rate_low = _power_counts(opp_low)["power_rate"]
                opp_rate_high = _power_counts(opp_high)["power_rate"]
                user_rate_low = _power_counts(user_low)["power_rate"]
                user_rate_high = _power_counts(user_high)["power_rate"]

                sample_elev_boost = float(opp_rate_high - opp_rate_low)
                user_elev_boost = float(user_rate_high - user_rate_low)
                elev_depend = (user_elev_boost - sample_elev_boost) / (
                    abs(sample_elev_boost) + 1e-6
                )
                elev_depend = _clamp(elev_depend, -1.0, 2.0)
                elev_robust = 1.0 - _clamp01((elev_depend + 0.2) / 1.2)

    card_score = 0.5
    if "batter_mlb_id" in user_df.columns:
        card_ids = user_df["batter_mlb_id"].dropna().astype(str)
        total_cards = len(card_ids)
        if total_cards > 0:
            counts = card_ids.value_counts()
            shares = counts / total_cards
            top_share = float(shares.max())
            effective_cards = 1.0 / float((shares**2).sum())
            depth = _clamp01((effective_cards - 1.0) / 8.0)
            one_card_penalty = _clamp01((top_share - 0.25) / 0.35)
            card_score = 0.7 * depth + 0.3 * (1.0 - one_card_penalty)

    base_z = _clamp(
        (smoothed_rate - sample_mean_rate) / max(sample_sd, _POWER_SD_FLOOR),
        -_POWER_Z_CLAMP_BASE,
        _POWER_Z_CLAMP_BASE,
    )

    final_z = _clamp(
        base_z + 0.8 * diff_score + 0.6 * (elev_robust - 0.5) + 0.6 * (card_score - 0.5),
        -_POWER_Z_CLAMP_FINAL,
        _POWER_Z_CLAMP_FINAL,
    )
    power = int(round(100.0 * _sigmoid(final_z / 2.0)))

    return PowerSkillOut(
        power=power,
        pa=pa_user,
        smoothed_rate=float(smoothed_rate),
        sample_mean_rate=float(sample_mean_rate),
        sample_sd=float(sample_sd),
        diff_score=float(diff_score),
        elev_robust=float(elev_robust),
        card_score=float(card_score),
    )


def _direction_scores(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=float)

    side = df.get("batter_side")
    if side is None:
        side = pd.Series([""] * len(df), index=df.index)
    side = side.fillna("").astype(str).str.upper()

    hit_dir = df.get("hit_direction")
    if hit_dir is None:
        hit_dir = pd.Series([""] * len(df), index=df.index)
    hit_dir = hit_dir.fillna("").astype(str).str.lower()

    fielder = df.get("fielder_pos")
    if fielder is None:
        fielder = pd.Series([""] * len(df), index=df.index)
    fielder = fielder.fillna("").astype(str).str.upper()

    direction = pd.Series([""] * len(df), index=df.index)
    direction = direction.mask(hit_dir.str.contains("left") | hit_dir.isin(["lf"]), "left")
    direction = direction.mask(hit_dir.str.contains("right") | hit_dir.isin(["rf"]), "right")
    direction = direction.mask(hit_dir.str.contains("center") | hit_dir.isin(["cf"]), "center")

    direction = direction.mask(direction == "", fielder.map({
        "LF": "left",
        "3B": "left",
        "SS": "left",
        "CF": "center",
        "2B": "center",
        "RF": "right",
        "1B": "right",
    }).fillna(""))

    score = pd.Series(0.0, index=df.index)

    is_left_side = side == "L"
    is_right_side = side == "R"
    good_left = (is_left_side) & direction.isin(["right", "center"])
    bad_left = (is_left_side) & (direction == "left")
    good_right = (is_right_side) & direction.isin(["left", "center"])
    bad_right = (is_right_side) & (direction == "right")

    score = score.mask(good_left | good_right, 0.12)
    score = score.mask(bad_left | bad_right, -0.08)

    center_bonus = (direction == "center") | (fielder == "CF")
    score = score + center_bonus.astype(float) * 0.04

    ss_2b_penalty = fielder.isin(["SS", "2B"])
    score = score - ss_2b_penalty.astype(float) * 0.04

    return score


def _timing_event_scores(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=float)

    results = df.get("result")
    if results is None:
        results = pd.Series([""] * len(df), index=df.index)
    results = results.fillna("").astype(str).str.lower()

    is_strikeout = df.get("is_strikeout")
    if is_strikeout is None:
        is_strikeout = pd.Series([False] * len(df), index=df.index)
    is_strikeout = is_strikeout.fillna(False).astype(bool)

    k_timing = df.get("k_timing")
    if k_timing is None:
        k_timing = pd.Series([""] * len(df), index=df.index)
    k_timing = k_timing.fillna("").astype(str).str.lower()

    is_pp = df.get("is_perfect_perfect")
    if is_pp is None:
        is_pp = pd.Series([False] * len(df), index=df.index)
    is_pp = is_pp.fillna(False).astype(bool)

    contact = pd.Series(0.0, index=df.index)
    timing_k = is_strikeout & k_timing.isin(["early", "late"])
    contact = contact.mask(timing_k, -0.70)
    contact = contact.mask(is_strikeout & ~timing_k, -0.45)

    non_k = ~is_strikeout
    contact = contact.mask(non_k & is_pp, 0.70)

    hit_scores = pd.Series(0.0, index=df.index)
    hit_scores = hit_scores.mask(results.isin(["homerun", "home_run", "home run"]), 0.55)
    hit_scores = hit_scores.mask(results == "triple", 0.40)
    hit_scores = hit_scores.mask(results == "double", 0.30)
    hit_scores = hit_scores.mask(results == "single", 0.10)
    hit_scores = hit_scores.mask(
        results.isin(["reach_on_error", "reached_on_error", "error"]), 0.10
    )

    is_walk = results == "walk"
    is_hbp = results.isin(["hit_by_pitch", "hit by pitch", "hbp"])
    is_hit = hit_scores != 0
    is_out_in_play = non_k & ~is_pp & ~is_hit & ~is_walk & ~is_hbp

    contact = contact.mask(non_k & ~is_pp & is_hit, hit_scores)
    contact = contact.mask(is_out_in_play, -0.10)

    direction = _direction_scores(df)
    evt = contact + direction
    evt = evt.clip(lower=-1.0, upper=1.0)
    return _apply_difficulty_event_adjustment(evt, df)


def _location_event_scores(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=float)

    results = df.get("result")
    if results is None:
        results = pd.Series([""] * len(df), index=df.index)
    results = results.fillna("").astype(str).str.lower()

    is_strikeout = df.get("is_strikeout")
    if is_strikeout is None:
        is_strikeout = pd.Series([False] * len(df), index=df.index)
    is_strikeout = is_strikeout.fillna(False).astype(bool)

    batted_ball = df.get("batted_ball_type")
    if batted_ball is None:
        batted_ball = pd.Series([""] * len(df), index=df.index)
    batted_ball = batted_ball.fillna("").astype(str).str.lower()

    is_pp = df.get("is_perfect_perfect")
    if is_pp is None:
        is_pp = pd.Series([False] * len(df), index=df.index)
    is_pp = is_pp.fillna(False).astype(bool)

    k_is_chase = df.get("k_is_chase")
    if k_is_chase is None:
        k_is_chase = pd.Series([False] * len(df), index=df.index)
    k_is_chase = k_is_chase.fillna(False).astype(bool)

    k_is_looking = df.get("k_is_looking")
    if k_is_looking is None:
        k_is_looking = pd.Series([False] * len(df), index=df.index)
    k_is_looking = k_is_looking.fillna(False).astype(bool)

    evt = pd.Series(0.0, index=df.index)

    k_evt = -0.40 - 0.20 * k_is_chase.astype(float) - 0.10 * k_is_looking.astype(float)
    evt = evt.mask(is_strikeout, k_evt)

    non_k = ~is_strikeout
    type_score = pd.Series(0.0, index=df.index)
    type_score = type_score.mask(batted_ball == "line", 0.35)
    type_score = type_score.mask(batted_ball == "fly", 0.15)
    type_score = type_score.mask(batted_ball == "ground", 0.05)
    type_score = type_score.mask(batted_ball == "popup", -0.35)

    pp_bonus = is_pp.astype(float) * 0.40
    outcome_bump = pd.Series(0.0, index=df.index)
    outcome_bump = outcome_bump.mask(
        results.isin(["double", "triple", "homerun", "home_run", "home run"]), 0.10
    )
    outcome_bump = outcome_bump.mask(results == "single", 0.03)

    evt_non_k = type_score + pp_bonus + outcome_bump
    evt = evt.mask(non_k, evt_non_k)
    evt = evt.clip(lower=-1.0, upper=1.0)
    return _apply_difficulty_event_adjustment(evt, df)


def _group_k_scores(df: pd.DataFrame, group_cols: Iterable[str]) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=float)

    missing = [col for col in group_cols if col not in df.columns]
    if missing:
        return pd.Series([], dtype=float)

    is_strikeout = _bool_col(df, "is_strikeout")
    k_timing = _str_col(df, "k_timing").str.lower()
    k_is_chase = _bool_col(df, "k_is_chase")
    k_is_looking = _bool_col(df, "k_is_looking")

    k_timing_mask = is_strikeout & k_timing.isin(["early", "late"])
    k_loc_mask = is_strikeout & (k_is_chase | k_is_looking) & ~k_timing_mask
    k_other_mask = is_strikeout & ~k_timing_mask & ~k_loc_mask

    data = pd.DataFrame(
        {
            "pa": 1,
            "k_loc": k_loc_mask.astype(int),
            "k_other": k_other_mask.astype(int),
            "k_timing": k_timing_mask.astype(int),
        },
        index=df.index,
    )
    for col in group_cols:
        data[col] = df[col]

    grouped = data.groupby(list(group_cols)).sum(numeric_only=True)
    denom = grouped["pa"].replace(0, 1)
    kscore = (
        _PITCHING_K_WEIGHT_LOC * grouped["k_loc"]
        + _PITCHING_K_WEIGHT_OTHER * grouped["k_other"]
        + _PITCHING_K_WEIGHT_TIMING * grouped["k_timing"]
    ) / denom
    return kscore


def _gimme_rate(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    results = _str_col(df, "result").str.lower()
    is_pp = _bool_col(df, "is_perfect_perfect")
    gimme = is_pp | results.isin(["homerun", "home_run", "home run", "triple", "double"])
    return float(gimme.sum()) / float(len(df)) if len(df) else 0.0


def _compute_pitching_archetype(
    df: pd.DataFrame,
    username: str,
    db: Session,
    pa_smooth: int = 150,
    reference_pitching_df: Optional[pd.DataFrame] = None,
) -> PitchingArchetypeOut:
    user_pitching, opp_pitching = _pitching_masks(df, username)
    user_df = df[user_pitching]
    opp_df = reference_pitching_df if reference_pitching_df is not None else df[opp_pitching]

    pa_user = len(user_df)
    if pa_user == 0:
        return PitchingArchetypeOut(overall=55, consistency=50, strikeout=50, location=50, pa=0)

    def consistency_event_scores(sub: pd.DataFrame) -> pd.Series:
        results = _str_col(sub, "result").str.lower()
        is_pp = _bool_col(sub, "is_perfect_perfect")
        is_out = _bool_col(sub, "is_out")
        is_error = _bool_col(sub, "is_error")

        evt = pd.Series(0.0, index=sub.index)
        evt = evt.mask(is_pp, -0.70)
        evt = evt.mask(~is_pp & results.isin(["homerun", "home_run", "home run"]), -0.55)
        evt = evt.mask(~is_pp & (results == "triple"), -0.35)
        evt = evt.mask(~is_pp & (results == "double"), -0.25)
        evt = evt.mask(~is_pp & (results == "single"), -0.10)
        evt = evt.mask(~is_pp & is_out, 0.08)
        evt = evt + (is_error.astype(float) * -0.05)
        evt = evt.clip(lower=-1.0, upper=1.0)
        return _apply_difficulty_event_adjustment(evt, sub)

    user_evt = consistency_event_scores(user_df)
    opp_evt = consistency_event_scores(opp_df)

    cons_z = 0.0
    if "difficulty_id" in user_df.columns and "difficulty_id" in opp_df.columns:
        user_diffs = user_df.get("difficulty_id")
        opp_diffs = opp_df.get("difficulty_id")
        if user_diffs is not None and opp_diffs is not None:
            diffs = [d for d in user_diffs.dropna().unique().tolist()]
            for diff in diffs:
                user_d = user_evt[user_diffs == diff]
                opp_d = opp_evt[opp_diffs == diff]
                if len(user_d) == 0 or len(opp_d) == 0:
                    continue
                mu_d = float(opp_d.mean())
                sd_d = float(opp_d.std(ddof=0))
                denom = sd_d if sd_d > 0 else 1e-6
                z_d = (float(user_d.mean()) - mu_d) / denom
                cons_z += (len(user_d) / pa_user) * z_d

    elev_adj = 0.0
    if "ballpark_id" in user_df.columns and "ballpark_id" in opp_df.columns:
        user_ballpark_ids = pd.to_numeric(user_df["ballpark_id"], errors="coerce")
        opp_ballpark_ids = pd.to_numeric(opp_df["ballpark_id"], errors="coerce")
        unique_ids = sorted(
            {
                int(v)
                for v in pd.concat([user_ballpark_ids, opp_ballpark_ids], ignore_index=True)
                .dropna()
                .unique()
                .tolist()
            }
        )
        if unique_ids:
            elevations = {
                row.id: (row.elevation if row.elevation is not None else None)
                for row in db.scalars(select(ShowBallParks).where(ShowBallParks.id.in_(unique_ids))).all()
            }
            opp_elev = opp_ballpark_ids.map(elevations)
            user_elev = user_ballpark_ids.map(elevations)
            opp_elev_non_null = opp_elev.dropna()
            user_elev_non_null = user_elev.dropna()
            if len(opp_elev_non_null) >= 10 and len(user_elev_non_null) >= 10:
                q_low = float(opp_elev_non_null.quantile(0.33))
                q_high = float(opp_elev_non_null.quantile(0.66))

                def bucket_mask(series: pd.Series, bucket: str) -> pd.Series:
                    if bucket == "low":
                        return series <= q_low
                    if bucket == "high":
                        return series > q_high
                    return (series > q_low) & (series <= q_high)

                opp_low = opp_df[bucket_mask(opp_elev, "low") & opp_elev.notna()]
                opp_high = opp_df[bucket_mask(opp_elev, "high") & opp_elev.notna()]
                user_low = user_df[bucket_mask(user_elev, "low") & user_elev.notna()]
                user_high = user_df[bucket_mask(user_elev, "high") & user_elev.notna()]

                user_boost = _gimme_rate(user_high) - _gimme_rate(user_low)
                sample_boost = _gimme_rate(opp_high) - _gimme_rate(opp_low)
                elev_depend = (user_boost - sample_boost) / (abs(sample_boost) + 0.01)
                elev_adj = -0.6 * _clamp(elev_depend, -0.5, 2.0)

    merchant_adj = 0.0
    if "pitcher_mlb_id" in user_df.columns:
        pitcher_ids = user_df["pitcher_mlb_id"].dropna().astype(str)
        if len(pitcher_ids):
            counts = pitcher_ids.value_counts()
            shares = counts / counts.sum()
            top_share = float(shares.max())
            effective_pitchers = 1.0 / float((shares**2).sum())
            depth = _clamp01((effective_pitchers - 1.0) / 6.0)
            one_penalty = _clamp01((top_share - 0.35) / 0.45)
            merchant_adj = 0.4 * depth - 0.6 * one_penalty

    cons_final_z = cons_z + elev_adj + merchant_adj
    consistency = int(round(100.0 * _sigmoid(cons_final_z / 2.0)))

    def k_score_from_df(sub: pd.DataFrame) -> float:
        pa = len(sub)
        if pa == 0:
            return 0.0
        is_strikeout = _bool_col(sub, "is_strikeout")
        k_timing = _str_col(sub, "k_timing").str.lower()
        k_is_chase = _bool_col(sub, "k_is_chase")
        k_is_looking = _bool_col(sub, "k_is_looking")

        k_timing_mask = is_strikeout & k_timing.isin(["early", "late"])
        k_loc_mask = is_strikeout & (k_is_chase | k_is_looking) & ~k_timing_mask
        k_other_mask = is_strikeout & ~k_timing_mask & ~k_loc_mask
        return (
            (
                _PITCHING_K_WEIGHT_LOC * k_loc_mask.sum()
                + _PITCHING_K_WEIGHT_OTHER * k_other_mask.sum()
                + _PITCHING_K_WEIGHT_TIMING * k_timing_mask.sum()
            )
            / float(pa)
        )

    k_z = 0.0
    if "difficulty_id" in user_df.columns and "difficulty_id" in opp_df.columns:
        user_diffs = user_df.get("difficulty_id")
        opp_diffs = opp_df.get("difficulty_id")
        if user_diffs is not None and opp_diffs is not None:
            diffs = [d for d in user_diffs.dropna().unique().tolist()]
            for diff in diffs:
                user_mask = user_diffs == diff
                opp_mask = opp_diffs == diff
                user_d = user_df[user_mask]
                opp_d = opp_df[opp_mask]
                if user_d.empty or opp_d.empty:
                    continue
                user_kscore = k_score_from_df(user_d)
                opp_scores = _group_k_scores(opp_d, ["game_id"])
                opp_mu = float(opp_scores.mean()) if len(opp_scores) else 0.0
                opp_sd = float(opp_scores.std(ddof=0)) if len(opp_scores) else 0.0
                denom = max(opp_sd, _PITCHING_K_SD_FLOOR)
                z_d = _clamp(
                    (user_kscore - opp_mu) / denom,
                    -_PITCHING_K_Z_CLAMP_PER_DIFF,
                    _PITCHING_K_Z_CLAMP_PER_DIFF,
                )
                k_z += (len(user_d) / pa_user) * z_d

    k_z = _clamp(k_z, -_PITCHING_K_Z_CLAMP_FINAL, _PITCHING_K_Z_CLAMP_FINAL)
    strikeout = int(round(100.0 * _sigmoid(k_z / 2.0)))

    loc_z = 0.0
    if "difficulty_id" in user_df.columns and "difficulty_id" in opp_df.columns:
        user_diffs = user_df.get("difficulty_id")
        opp_diffs = opp_df.get("difficulty_id")
        if user_diffs is not None and opp_diffs is not None:
            diffs = [d for d in user_diffs.dropna().unique().tolist()]
            for diff in diffs:
                user_mask = user_diffs == diff
                opp_mask = opp_diffs == diff
                user_d = user_df[user_mask]
                opp_d = opp_df[opp_mask]
                if user_d.empty or opp_d.empty:
                    continue

                user_pa = len(user_d)
                opp_pa = len(opp_d)

                user_k_timing = _bool_col(user_d, "is_strikeout") & _str_col(user_d, "k_timing").str.lower().isin(["early", "late"])
                opp_k_timing = _bool_col(opp_d, "is_strikeout") & _str_col(opp_d, "k_timing").str.lower().isin(["early", "late"])
                user_k_chase = _bool_col(user_d, "is_strikeout") & _bool_col(user_d, "k_is_chase") & ~user_k_timing
                user_k_look = _bool_col(user_d, "is_strikeout") & _bool_col(user_d, "k_is_looking") & ~user_k_timing
                opp_k_chase = _bool_col(opp_d, "is_strikeout") & _bool_col(opp_d, "k_is_chase") & ~opp_k_timing
                opp_k_look = _bool_col(opp_d, "is_strikeout") & _bool_col(opp_d, "k_is_looking") & ~opp_k_timing

                user_chase_rate = user_k_chase.sum() / user_pa if user_pa else 0.0
                user_look_rate = user_k_look.sum() / user_pa if user_pa else 0.0
                user_pp_rate = _bool_col(user_d, "is_perfect_perfect").sum() / user_pa if user_pa else 0.0

                opp_chase_rate = opp_k_chase.sum() / opp_pa if opp_pa else 0.0
                opp_look_rate = opp_k_look.sum() / opp_pa if opp_pa else 0.0
                opp_pp_rate = _bool_col(opp_d, "is_perfect_perfect").sum() / opp_pa if opp_pa else 0.0

                r1 = (user_chase_rate - opp_chase_rate) / (opp_chase_rate + 0.01)
                r2 = (user_look_rate - opp_look_rate) / (opp_look_rate + 0.01)
                r3 = (user_pp_rate - opp_pp_rate) / (opp_pp_rate + 0.005)

                loc_z += (user_pa / pa_user) * (0.9 * r1 + 0.7 * r2 - 1.0 * r3)

    spread01 = 0.5
    user_k = user_df[_bool_col(user_df, "is_strikeout")]
    user_k_timing_mask = _str_col(user_k, "k_timing").str.lower().isin(["early", "late"])
    user_k_loc = user_k[
        (_bool_col(user_k, "k_is_chase") | _bool_col(user_k, "k_is_looking")) & ~user_k_timing_mask
    ]
    k_loc_height = user_k_loc.get("k_loc_height")
    k_loc_width = user_k_loc.get("k_loc_width")
    if k_loc_height is not None and k_loc_width is not None and len(user_k_loc) >= 6:
        opp_k = opp_df[_bool_col(opp_df, "is_strikeout")]
        opp_k_loc = opp_k[
            (_bool_col(opp_k, "k_is_chase") | _bool_col(opp_k, "k_is_looking"))
            & ~_str_col(opp_k, "k_timing").str.lower().isin(["early", "late"])
        ]
        opp_h = pd.to_numeric(opp_k_loc.get("k_loc_height"), errors="coerce").dropna()
        opp_w = pd.to_numeric(opp_k_loc.get("k_loc_width"), errors="coerce").dropna()
        if len(opp_h) >= 6 and len(opp_w) >= 6:
            h_q1, h_q2 = opp_h.quantile(0.33), opp_h.quantile(0.66)
            w_q1, w_q2 = opp_w.quantile(0.33), opp_w.quantile(0.66)
            if h_q1 != h_q2 and w_q1 != w_q2:
                user_h = pd.to_numeric(k_loc_height, errors="coerce")
                user_w = pd.to_numeric(k_loc_width, errors="coerce")
                h_bin = pd.cut(user_h, [-float("inf"), h_q1, h_q2, float("inf")], labels=[0, 1, 2])
                w_bin = pd.cut(user_w, [-float("inf"), w_q1, w_q2, float("inf")], labels=[0, 1, 2])
                valid = h_bin.notna() & w_bin.notna()
                if valid.any():
                    bins = (h_bin.astype(int) * 3 + w_bin.astype(int))[valid]
                    counts = bins.value_counts()
                    shares = counts / counts.sum()
                    eff_bins = 1.0 / float((shares**2).sum())
                    spread01 = _clamp01((eff_bins - 1.0) / 8.0)

    loc_final = loc_z + 0.4 * (spread01 - 0.5)
    location = int(round(100.0 * _sigmoid(loc_final / 2.0)))

    c = consistency / 100.0
    k = strikeout / 100.0
    l = location / 100.0
    earned_k = k * (0.60 + 0.40 * l)
    raw = 0.50 * c + 0.30 * earned_k + 0.20 * l
    conf = _clamp(pa_user / 250.0, 0.0, 1.0)
    final01 = 0.55 * (1.0 - conf) + conf * raw
    overall = int(round(100.0 * _clamp(final01, 0.0, 1.0)))

    return PitchingArchetypeOut(
        overall=overall,
        consistency=consistency,
        strikeout=strikeout,
        location=location,
        pa=pa_user,
    )


def _compute_timing_skill(
    df: pd.DataFrame,
    username: str,
    pa_smooth: int = 200,
    reference_hitting_df: Optional[pd.DataFrame] = None,
) -> TimingSkillOut:
    user_hitting, _, opponent_hitting = _user_masks(df, username)
    user_df = df[user_hitting]
    opp_df = reference_hitting_df if reference_hitting_df is not None else df[opponent_hitting]

    pa_user = len(user_df)
    if pa_user == 0:
        return TimingSkillOut(timing=50, location=50, pa=0, timing_z=0.0, k_pen=0.0, pp_bonus=0.0)

    user_evt = _timing_event_scores(user_df)
    opp_evt = _timing_event_scores(opp_df)
    user_mean = float(user_evt.mean()) if len(user_evt) else 0.0
    opp_mean = float(opp_evt.mean()) if len(opp_evt) else 0.0
    timing_user_smoothed = (user_mean * pa_user + opp_mean * pa_smooth) / (pa_user + pa_smooth)

    timing_z = 0.0
    if "difficulty_id" in user_df.columns and "difficulty_id" in opp_df.columns and len(user_evt):
        user_diffs = user_df.get("difficulty_id")
        if user_diffs is not None:
            diffs = [d for d in user_diffs.dropna().unique().tolist()]
            if diffs:
                for diff in diffs:
                    user_d = user_evt[user_diffs == diff]
                    opp_d = opp_evt[opp_df.get("difficulty_id") == diff]
                    if len(user_d) == 0 or len(opp_d) == 0:
                        continue
                    mu_d = float(opp_d.mean())
                    sd_d = float(opp_d.std(ddof=0))
                    denom = sd_d if sd_d > 0 else 1e-6
                    z_d = (float(user_d.mean()) - mu_d) / denom
                    timing_z += (len(user_d) / pa_user) * z_d

    is_strikeout = user_df.get("is_strikeout")
    if is_strikeout is None:
        is_strikeout = pd.Series([False] * len(user_df), index=user_df.index)
    is_strikeout = is_strikeout.fillna(False).astype(bool)
    k_timing = user_df.get("k_timing")
    if k_timing is None:
        k_timing = pd.Series([""] * len(user_df), index=user_df.index)
    k_timing = k_timing.fillna("").astype(str).str.lower()
    k_fail_user = ((is_strikeout) & k_timing.isin(["early", "late"])).sum()
    k_fail_rate_user = k_fail_user / pa_user if pa_user else 0.0

    opp_is_strikeout = opp_df.get("is_strikeout")
    if opp_is_strikeout is None:
        opp_is_strikeout = pd.Series([False] * len(opp_df), index=opp_df.index)
    opp_is_strikeout = opp_is_strikeout.fillna(False).astype(bool)
    opp_k_timing = opp_df.get("k_timing")
    if opp_k_timing is None:
        opp_k_timing = pd.Series([""] * len(opp_df), index=opp_df.index)
    opp_k_timing = opp_k_timing.fillna("").astype(str).str.lower()
    k_fail_opp = ((opp_is_strikeout) & opp_k_timing.isin(["early", "late"])).sum()
    k_fail_rate_opp = k_fail_opp / len(opp_df) if len(opp_df) else 0.0

    k_fail_rel = (k_fail_rate_user - k_fail_rate_opp) / (k_fail_rate_opp + 0.01)
    k_pen = -_clamp(k_fail_rel, -0.5, 2.0) * 0.8

    pp_user = user_df.get("is_perfect_perfect")
    if pp_user is None:
        pp_user = pd.Series([False] * len(user_df), index=user_df.index)
    pp_user = pp_user.fillna(False).astype(bool)
    pp_rate_user = pp_user.sum() / pa_user if pa_user else 0.0

    pp_opp = opp_df.get("is_perfect_perfect")
    if pp_opp is None:
        pp_opp = pd.Series([False] * len(opp_df), index=opp_df.index)
    pp_opp = pp_opp.fillna(False).astype(bool)
    pp_rate_opp = pp_opp.sum() / len(opp_df) if len(opp_df) else 0.0

    pp_rel = (pp_rate_user - pp_rate_opp) / (pp_rate_opp + 0.005)
    pp_bonus = _clamp(pp_rel, -1.0, 3.0) * 0.6

    final_z = timing_z + k_pen + pp_bonus
    timing = int(round(100.0 * _sigmoid(final_z / 2.0)))

    user_loc_evt = _location_event_scores(user_df)
    opp_loc_evt = _location_event_scores(opp_df)

    loc_z = 0.0
    if "difficulty_id" in user_df.columns and "difficulty_id" in opp_df.columns and len(user_loc_evt):
        user_diffs = user_df.get("difficulty_id")
        if user_diffs is not None:
            diffs = [d for d in user_diffs.dropna().unique().tolist()]
            if diffs:
                for diff in diffs:
                    user_d = user_loc_evt[user_diffs == diff]
                    opp_d = opp_loc_evt[opp_df.get("difficulty_id") == diff]
                    if len(user_d) == 0 or len(opp_d) == 0:
                        continue
                    mu_d = float(opp_d.mean())
                    sd_d = float(opp_d.std(ddof=0))
                    denom = sd_d if sd_d > 0 else 1e-6
                    z_d = (float(user_d.mean()) - mu_d) / denom
                    loc_z += (len(user_d) / pa_user) * z_d

    chase_adj = 0.0
    user_k = user_df[_bool_col(user_df, "is_strikeout")]
    opp_k = opp_df[_bool_col(opp_df, "is_strikeout")]

    user_chase = user_k.get("k_is_chase")
    opp_chase = opp_k.get("k_is_chase")
    user_chase_rate = None
    opp_chase_rate = None
    if user_chase is not None:
        user_chase = user_chase.dropna().astype(bool)
        if len(user_chase):
            user_chase_rate = float(user_chase.mean())
    if opp_chase is not None:
        opp_chase = opp_chase.dropna().astype(bool)
        if len(opp_chase):
            opp_chase_rate = float(opp_chase.mean())
    if user_chase_rate is not None and opp_chase_rate is not None:
        chase_rel = (user_chase_rate - opp_chase_rate) / (opp_chase_rate + 0.01)
        chase_adj = -0.8 * _clamp(chase_rel, -0.5, 2.0)

    variety_adj = 0.0
    k_loc_height_user = user_k.get("k_loc_height")
    k_loc_width_user = user_k.get("k_loc_width")
    k_loc_height_opp = opp_k.get("k_loc_height")
    k_loc_width_opp = opp_k.get("k_loc_width")

    if k_loc_height_user is None or k_loc_width_user is None:
        user_loc_k = user_k.iloc[0:0]
    else:
        user_loc_k = user_k[k_loc_height_user.notna() & k_loc_width_user.notna()]

    if k_loc_height_opp is None or k_loc_width_opp is None:
        opp_loc_k = opp_k.iloc[0:0]
    else:
        opp_loc_k = opp_k[k_loc_height_opp.notna() & k_loc_width_opp.notna()]
    if len(user_loc_k) >= 6 and len(opp_loc_k) >= 12:
        opp_h = pd.to_numeric(opp_loc_k.get("k_loc_height"), errors="coerce").dropna()
        opp_w = pd.to_numeric(opp_loc_k.get("k_loc_width"), errors="coerce").dropna()
        if len(opp_h) and len(opp_w):
            h_q1, h_q2 = opp_h.quantile(0.33), opp_h.quantile(0.66)
            w_q1, w_q2 = opp_w.quantile(0.33), opp_w.quantile(0.66)
            if h_q1 != h_q2 and w_q1 != w_q2:
                user_h = pd.to_numeric(user_loc_k.get("k_loc_height"), errors="coerce")
                user_w = pd.to_numeric(user_loc_k.get("k_loc_width"), errors="coerce")
                h_bin = pd.cut(user_h, [-float("inf"), h_q1, h_q2, float("inf")], labels=[0, 1, 2])
                w_bin = pd.cut(user_w, [-float("inf"), w_q1, w_q2, float("inf")], labels=[0, 1, 2])
                valid = h_bin.notna() & w_bin.notna()
                if valid.any():
                    bins = (h_bin.astype(int) * 3 + w_bin.astype(int))[valid]
                    counts = bins.value_counts()
                    shares = counts / counts.sum()
                    eff_bins = 1.0 / float((shares**2).sum())
                    eff01 = _clamp01((eff_bins - 1.0) / 8.0)
                    variety_adj = -0.7 * _clamp(eff01 - 0.3, -0.3, 0.3)

    adj_bonus = 0.0
    if "times_seen_pitcher" in user_df.columns and "game_id" in user_df.columns:
        times = pd.to_numeric(user_df.get("times_seen_pitcher"), errors="coerce")
        mean1 = float(user_loc_evt[times == 1].mean()) if (times == 1).any() else None
        mean2p = float(user_loc_evt[times >= 2].mean()) if (times >= 2).any() else None
        if mean1 is not None and mean2p is not None:
            adj_raw = mean2p - mean1
            opp_adj_series: list[float] = []
            if "times_seen_pitcher" in opp_df.columns:
                opp_times = pd.to_numeric(opp_df.get("times_seen_pitcher"), errors="coerce")
                opp_games = opp_df.get("game_id")
                if opp_games is not None:
                    for gid in opp_games.dropna().unique().tolist():
                        mask = opp_games == gid
                        g_evt = opp_loc_evt[mask]
                        g_times = opp_times[mask]
                        if (g_times == 1).any() and (g_times >= 2).any():
                            g_mean1 = float(g_evt[g_times == 1].mean())
                            g_mean2p = float(g_evt[g_times >= 2].mean())
                            opp_adj_series.append(g_mean2p - g_mean1)
            opp_adj_mean = float(pd.Series(opp_adj_series).mean()) if opp_adj_series else 0.0
            opp_adj_sd = float(pd.Series(opp_adj_series).std(ddof=0)) if opp_adj_series else 0.0
            if opp_adj_sd > 0:
                adj_z = (adj_raw - opp_adj_mean) / (opp_adj_sd + 1e-6)
                adj_bonus = _clamp(adj_z, -1.0, 1.0) * 0.4

    loc_final_z = loc_z + chase_adj + variety_adj + adj_bonus
    location = int(round(100.0 * _sigmoid(loc_final_z / 2.0)))

    return TimingSkillOut(
        timing=timing,
        location=location,
        pa=pa_user,
        timing_z=float(timing_z),
        k_pen=float(k_pen),
        pp_bonus=float(pp_bonus),
    )


def _local_reference_samples(df: pd.DataFrame, username: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    _, _, opp_hitting = _user_masks(df, username)
    _, opp_pitching = _pitching_masks(df, username)
    return df[opp_hitting], df[opp_pitching]


def _compute_combined_archetype_for_df(
    *,
    df_full: pd.DataFrame,
    username: str,
    db: Session,
    pitcher_mlb_id: Optional[int] = None,
    hitter_mlb_id: Optional[int] = None,
) -> CombinedArchetypeOut:
    ctx = _build_skill_context(
        username,
        df_full,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )

    power_skill = _compute_power_skill(
        ctx.root_df,
        username,
        db,
        reference_hitting_df=ctx.ref_hitting_df,
    )
    timing_skill = _compute_timing_skill(
        ctx.root_df,
        username,
        reference_hitting_df=ctx.ref_hitting_df,
    )
    pa = max(power_skill.pa, timing_skill.pa)
    batting_overall = _compute_overall_hitting(
        power=power_skill.power,
        timing=timing_skill.timing,
        location=timing_skill.location,
        pa=pa,
    )
    batting = BattingArchetypeOut(
        overall=batting_overall,
        power=power_skill.power,
        timing=timing_skill.timing,
        location=timing_skill.location,
        pa=pa,
    )

    pitching = _compute_pitching_archetype(
        ctx.root_df,
        username,
        db,
        reference_pitching_df=ctx.ref_pitching_df,
    )

    return CombinedArchetypeOut(batting=batting, pitching=pitching)


def _get_combined_archetype_cached(
    *,
    username: str,
    db: Session,
    pitcher_mlb_id: Optional[int] = None,
    hitter_mlb_id: Optional[int] = None,
) -> CombinedArchetypeOut:
    cached = _load_combined_from_cache(username, pitcher_mlb_id, hitter_mlb_id)
    if cached is not None:
        return cached

    computed = _compute_combined_archetype_for_df(
        df_full=_load_skill_facts_df(username),
        username=username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    _store_combined_cache(username, pitcher_mlb_id, hitter_mlb_id, computed)
    return computed


@router.get("/skills", response_model=ShowSkillsOut)
def get_show_skills(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> ShowSkillsOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    username = sp.username
    df = _load_skill_facts_df(username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    ref_hitting_df, ref_pitching_df = _local_reference_samples(df, username)
    hitting, pitching = _compute_pa_stats(
        df,
        username,
        reference_hitting_df=ref_hitting_df,
        reference_pitching_df=ref_pitching_df,
    )

    return ShowSkillsOut(hitting=hitting, pitching=pitching)


@router.get("/stats", response_model=ShowAggregateStatsOut)
def get_show_stats(
    response: Response,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    view: Optional[str] = Query(default=None, max_length=8),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowAggregateStatsOut:
    _set_stats_http_cache_headers(response, is_public=False)
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    view_norm = (view or "").strip().lower()
    cache_key = build_cache_key(
        "show",
        "stats",
        "me",
        "v1",
        _normalize_username(sp.username),
        view_norm,
        str((claims or {}).get("uid") or ""),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowAggregateStatsOut.model_validate(cached)

    df = _load_stats_facts_df_cached(sp.username)
    computed = _compute_aggregate_stats(df, sp.username, view=view)
    return _cache_stats_response(cache, cache_key, computed)


@router.get("/your-ovr", response_model=ShowYourOvrWeightsOut)
def get_show_your_ovr(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowYourOvrWeightsOut:
    user = _get_authed_user(db, claims)
    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    return _load_your_ovr_weights_cached(sp.username)


@router.get("/archetype/power", response_model=PowerSkillOut)
def get_show_power_skill(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> PowerSkillOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    username = sp.username
    df = _load_skill_facts_df(username)
    ctx = _build_skill_context(username, df)
    return _compute_power_skill(
        ctx.root_df,
        username,
        db,
        reference_hitting_df=ctx.ref_hitting_df,
    )


@router.get("/archetype/timing", response_model=TimingSkillOut)
def get_show_timing_skill(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> TimingSkillOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    username = sp.username
    df = _load_skill_facts_df(username)
    ctx = _build_skill_context(username, df)
    return _compute_timing_skill(
        ctx.root_df,
        username,
        reference_hitting_df=ctx.ref_hitting_df,
    )


@router.get("/archetype/batting", response_model=BattingArchetypeOut)
def get_show_batting_archetype(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> BattingArchetypeOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    username = sp.username
    combined = _get_combined_archetype_cached(
        username=username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.batting


@router.get("/archetype/pitching", response_model=PitchingArchetypeOut)
def get_show_pitching_archetype(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> PitchingArchetypeOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    combined = _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.pitching


@router.get("/archetype/combined", response_model=CombinedArchetypeOut)
def get_show_combined_archetype(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> CombinedArchetypeOut:
    user = _get_authed_user(db, claims)
    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    return _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )


@router.get("/strikeout-map", response_model=StrikeoutZoneMapOut)
def get_show_strikeout_map(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    pitch_types: Optional[str] = Query(default=None),
    min_speed: Optional[int] = Query(default=None, ge=0, le=999),
    max_speed: Optional[int] = Query(default=None, ge=0, le=999),
    timing: Optional[str] = Query(default=None, max_length=8),
    out_type: Optional[str] = Query(default=None, max_length=8),
) -> StrikeoutZoneMapOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_strikeout_zone_map(
        df,
        sp.username,
        db=db,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        pitch_types=_parse_pitch_types_param(pitch_types),
        timing=timing,
        out_type=out_type,
        min_speed=min_speed,
        max_speed=max_speed,
    )


@router.get("/hit-map", response_model=HitDataMapOut)
def get_show_hit_map(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    stat: Optional[str] = Query(default=None, max_length=12),
    base_state: Optional[str] = Query(default=None, max_length=16),
    outs: Optional[str] = Query(default=None, max_length=4),
    ab_count: Optional[str] = Query(default=None, max_length=8),
    min_seen: Optional[int] = Query(default=None, ge=0, le=999),
    max_seen: Optional[int] = Query(default=None, ge=0, le=999),
    pitcher_count: Optional[str] = Query(default=None, max_length=8),
    focus_zone: Optional[str] = Query(default=None, max_length=24),
) -> HitDataMapOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_hit_data_map(
        df,
        sp.username,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        stat=stat,
        base_state=base_state,
        outs=outs,
        ab_count=ab_count,
        min_seen=min_seen,
        max_seen=max_seen,
        pitcher_count=pitcher_count,
        focus_zone=focus_zone,
    )

@public_router.get("/show/{username}/skills", response_model=ShowSkillsOut)
def get_show_skills_by_username(
    username: str,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> ShowSkillsOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_skill_facts_df(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    ref_hitting_df, ref_pitching_df = _local_reference_samples(df, sp.username)
    hitting, pitching = _compute_pa_stats(
        df,
        sp.username,
        reference_hitting_df=ref_hitting_df,
        reference_pitching_df=ref_pitching_df,
    )
    return ShowSkillsOut(hitting=hitting, pitching=pitching)


@public_router.get("/show/{username}/stats", response_model=ShowAggregateStatsOut)
def get_show_stats_by_username(
    username: str,
    response: Response,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowAggregateStatsOut:
    _set_stats_http_cache_headers(response, is_public=True)
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    view_norm = (view or "").strip().lower()
    cache_key = build_cache_key(
        "show",
        "stats",
        "username",
        "v1",
        _normalize_username(sp.username),
        view_norm,
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowAggregateStatsOut.model_validate(cached)

    df = _load_stats_facts_df_cached(sp.username)
    computed = _compute_aggregate_stats(df, sp.username, view=view)
    return _cache_stats_response(cache, cache_key, computed)


@public_router.get("/show/{username}/archetype/batting", response_model=BattingArchetypeOut)
def get_show_batting_archetype_by_username(
    username: str,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> BattingArchetypeOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    combined = _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.batting


@public_router.get("/show/{username}/archetype/pitching", response_model=PitchingArchetypeOut)
def get_show_pitching_archetype_by_username(
    username: str,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> PitchingArchetypeOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    combined = _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.pitching


@public_router.get("/show/{username}/archetype/combined", response_model=CombinedArchetypeOut)
def get_show_combined_archetype_by_username(
    username: str,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> CombinedArchetypeOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    return _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )


@public_router.get("/show/{username}/strikeout-map", response_model=StrikeoutZoneMapOut)
def get_show_strikeout_map_by_username(
    username: str,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    pitch_types: Optional[str] = Query(default=None),
    min_speed: Optional[int] = Query(default=None, ge=0, le=999),
    max_speed: Optional[int] = Query(default=None, ge=0, le=999),
    timing: Optional[str] = Query(default=None, max_length=8),
    out_type: Optional[str] = Query(default=None, max_length=8),
) -> StrikeoutZoneMapOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_strikeout_zone_map(
        df,
        sp.username,
        db=db,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        pitch_types=_parse_pitch_types_param(pitch_types),
        timing=timing,
        out_type=out_type,
        min_speed=min_speed,
        max_speed=max_speed,
    )


@public_router.get("/show/{username}/hit-map", response_model=HitDataMapOut)
def get_show_hit_map_by_username(
    username: str,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    stat: Optional[str] = Query(default=None, max_length=12),
    base_state: Optional[str] = Query(default=None, max_length=16),
    outs: Optional[str] = Query(default=None, max_length=4),
    ab_count: Optional[str] = Query(default=None, max_length=8),
    min_seen: Optional[int] = Query(default=None, ge=0, le=999),
    max_seen: Optional[int] = Query(default=None, ge=0, le=999),
    pitcher_count: Optional[str] = Query(default=None, max_length=8),
    focus_zone: Optional[str] = Query(default=None, max_length=24),
) -> HitDataMapOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_hit_data_map(
        df,
        sp.username,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        stat=stat,
        base_state=base_state,
        outs=outs,
        ab_count=ab_count,
        min_seen=min_seen,
        max_seen=max_seen,
        pitcher_count=pitcher_count,
        focus_zone=focus_zone,
    )


@public_router.get("/{user_id}/show/skills", response_model=ShowSkillsOut)
def get_show_skills_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> ShowSkillsOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_skill_facts_df(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    ref_hitting_df, ref_pitching_df = _local_reference_samples(df, sp.username)
    hitting, pitching = _compute_pa_stats(
        df,
        sp.username,
        reference_hitting_df=ref_hitting_df,
        reference_pitching_df=ref_pitching_df,
    )
    return ShowSkillsOut(hitting=hitting, pitching=pitching)


@public_router.get("/{user_id}/show/stats", response_model=ShowAggregateStatsOut)
def get_show_stats_for_user(
    user_id: int,
    response: Response,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    cache: Redis | None = Depends(get_cache_client),
) -> ShowAggregateStatsOut:
    _set_stats_http_cache_headers(response, is_public=True)
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    view_norm = (view or "").strip().lower()
    cache_key = build_cache_key(
        "show",
        "stats",
        "user-id",
        "v1",
        user_id,
        _normalize_username(sp.username),
        view_norm,
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return ShowAggregateStatsOut.model_validate(cached)

    df = _load_stats_facts_df_cached(sp.username)
    computed = _compute_aggregate_stats(df, sp.username, view=view)
    return _cache_stats_response(cache, cache_key, computed)


@public_router.get("/{user_id}/show/archetype/batting", response_model=BattingArchetypeOut)
def get_show_batting_archetype_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> BattingArchetypeOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    combined = _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.batting


@public_router.get("/{user_id}/show/archetype/pitching", response_model=PitchingArchetypeOut)
def get_show_pitching_archetype_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> PitchingArchetypeOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    combined = _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )
    return combined.pitching


@public_router.get("/{user_id}/show/archetype/combined", response_model=CombinedArchetypeOut)
def get_show_combined_archetype_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
) -> CombinedArchetypeOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    return _get_combined_archetype_cached(
        username=sp.username,
        db=db,
        pitcher_mlb_id=pitcher_mlb_id,
        hitter_mlb_id=hitter_mlb_id,
    )


@public_router.get("/{user_id}/show/strikeout-map", response_model=StrikeoutZoneMapOut)
def get_show_strikeout_map_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    pitch_types: Optional[str] = Query(default=None),
    min_speed: Optional[int] = Query(default=None, ge=0, le=999),
    max_speed: Optional[int] = Query(default=None, ge=0, le=999),
    timing: Optional[str] = Query(default=None, max_length=8),
    out_type: Optional[str] = Query(default=None, max_length=8),
) -> StrikeoutZoneMapOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_strikeout_zone_map(
        df,
        sp.username,
        db=db,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        pitch_types=_parse_pitch_types_param(pitch_types),
        timing=timing,
        out_type=out_type,
        min_speed=min_speed,
        max_speed=max_speed,
    )


@public_router.get("/{user_id}/show/hit-map", response_model=HitDataMapOut)
def get_show_hit_map_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    view: Optional[str] = Query(default=None, max_length=8),
    hitter_side: Optional[str] = Query(default=None, max_length=1),
    pitcher_hand: Optional[str] = Query(default=None, max_length=1),
    pitcher_mlb_id: Optional[int] = Query(default=None, ge=0),
    hitter_mlb_id: Optional[int] = Query(default=None, ge=0),
    stat: Optional[str] = Query(default=None, max_length=12),
    base_state: Optional[str] = Query(default=None, max_length=16),
    outs: Optional[str] = Query(default=None, max_length=4),
    ab_count: Optional[str] = Query(default=None, max_length=8),
    min_seen: Optional[int] = Query(default=None, ge=0, le=999),
    max_seen: Optional[int] = Query(default=None, ge=0, le=999),
    pitcher_count: Optional[str] = Query(default=None, max_length=8),
    focus_zone: Optional[str] = Query(default=None, max_length=24),
) -> HitDataMapOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")
    df = _load_facts_df_for_username(sp.username)
    df = _filter_df_by_pitcher(df, pitcher_mlb_id)
    df = _filter_df_by_hitter(df, hitter_mlb_id)
    return _compute_hit_data_map(
        df,
        sp.username,
        view=view,
        hitter_side=hitter_side,
        pitcher_hand=pitcher_hand,
        stat=stat,
        base_state=base_state,
        outs=outs,
        ab_count=ab_count,
        min_seen=min_seen,
        max_seen=max_seen,
        pitcher_count=pitcher_count,
        focus_zone=focus_zone,
    )
