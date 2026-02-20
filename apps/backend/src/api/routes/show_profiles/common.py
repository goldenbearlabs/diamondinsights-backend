from __future__ import annotations

import datetime
from io import BytesIO
from typing import Optional, Tuple, Sequence

import pandas as pd
import pyarrow.parquet as pq
from fastapi import HTTPException, status

from shared.storage.spaces_connector import SpacesConfig, SpacesConnector


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _to_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).strip().replace("%", ""))
    except Exception:
        return None


_DEFAULT_PAS_COLUMNS: list[str] = [
    "game_id",
    "result",
    "is_strikeout",
    "is_sac_fly",
    "is_sac_bunt",
    "runs_scored",
    "rbi",
    "is_double_play",
    "is_perfect_perfect",
    "batted_ball_type",
    "hit_direction",
    "fielder_pos",
    "batter_side",
    "pitcher_throws",
    "k_timing",
    "k_is_chase",
    "k_is_looking",
    "k_loc_height",
    "k_loc_width",
    "difficulty_id",
    "ballpark_id",
    "batter_mlb_id",
    "pitcher_mlb_id",
    "k_pitch_type",
    "is_out",
    "is_error",
    "times_seen_pitcher",
    "num_pitchers",
    "num_abs_with_hitter",
    "outs_before",
    "runner_on_first",
    "runner_on_second",
    "runner_on_third",
    "home_profile_username",
    "away_profile_username",
    "home_team_id",
    "away_team_id",
    "home_name",
    "away_name",
    "is_home_batting",
]


def _load_pas_df(
    spaces: SpacesConnector,
    key: str,
    desired_columns: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    raw = spaces.get_bytes(key)
    desired = list(desired_columns) if desired_columns is not None else list(_DEFAULT_PAS_COLUMNS)
    buf = BytesIO(raw)
    parquet = pq.ParquetFile(buf)
    available = set(parquet.schema.names)
    cols = [c for c in desired if c in available]
    buf.seek(0)
    df = pd.read_parquet(buf, columns=cols)
    for col in desired:
        if col not in df.columns:
            df[col] = None
    return df


def _resolve_facts_key(spaces: SpacesConnector, username: str) -> str:
    key = f"facts/{username}/pas.parquet"
    legacy_key = f"di-storage/facts/{username}/pas.parquet"
    if spaces.exists(key):
        return key
    if spaces.exists(legacy_key):
        return legacy_key
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No facts file found")


def _load_facts_df_for_username(username: str) -> pd.DataFrame:
    cfg = SpacesConfig.from_env()
    spaces = SpacesConnector(cfg)
    key = _resolve_facts_key(spaces, username)
    return _load_pas_df(spaces, key)


def _load_facts_df_for_username_columns(username: str, columns: Sequence[str]) -> pd.DataFrame:
    cfg = SpacesConfig.from_env()
    spaces = SpacesConnector(cfg)
    key = _resolve_facts_key(spaces, username)
    return _load_pas_df(spaces, key, desired_columns=columns)


def _filter_df_by_pitcher(df: pd.DataFrame, pitcher_mlb_id: Optional[int]) -> pd.DataFrame:
    pid = _to_int(pitcher_mlb_id)
    if pid is None:
        return df
    pitcher_col = df.get("pitcher_mlb_id")
    if pitcher_col is None:
        return df.iloc[0:0]
    pitcher_ids = pd.to_numeric(pitcher_col, errors="coerce")
    return df[pitcher_ids == pid]


def _filter_df_by_hitter(df: pd.DataFrame, hitter_mlb_id: Optional[int]) -> pd.DataFrame:
    hid = _to_int(hitter_mlb_id)
    if hid is None:
        return df
    batter_col = df.get("batter_mlb_id")
    if batter_col is None:
        return df.iloc[0:0]
    batter_ids = pd.to_numeric(batter_col, errors="coerce")
    return df[batter_ids == hid]


def _bool_col(df: pd.DataFrame, name: str) -> pd.Series:
    col = df.get(name)
    if col is None:
        return pd.Series([False] * len(df), index=df.index)
    col = col.where(col.notna(), False)
    if col.dtype == object:
        col = col.infer_objects(copy=False)
    return col.astype(bool)


def _str_col(df: pd.DataFrame, name: str) -> pd.Series:
    col = df.get(name)
    if col is None:
        return pd.Series([""] * len(df), index=df.index)
    return col.fillna("").astype(str)


def _num_col(df: pd.DataFrame, name: str) -> pd.Series:
    col = df.get(name)
    if col is None:
        return pd.Series([0] * len(df), index=df.index)
    return pd.to_numeric(col, errors="coerce").fillna(0)


def _user_masks(df: pd.DataFrame, username: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    name = str(username).strip().lower()
    if "is_home_batting" not in df.columns:
        empty = pd.Series([False] * len(df), index=df.index)
        return empty, empty, empty

    is_home_batting = _bool_col(df, "is_home_batting")
    candidates = [
        ("home_profile_username", "away_profile_username"),
        ("home_team_id", "away_team_id"),
        ("home_name", "away_name"),
    ]

    def build_masks(home_col: str, away_col: str) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        home_team = _str_col(df, home_col).str.lower()
        away_team = _str_col(df, away_col).str.lower()
        user_is_home = home_team == name
        user_is_away = away_team == name

        user_hitting = (user_is_home & is_home_batting) | (user_is_away & ~is_home_batting)
        user_pitching = (user_is_home & ~is_home_batting) | (user_is_away & is_home_batting)
        opponent_hitting = ~user_hitting & (user_is_home | user_is_away)
        has_match = user_is_home | user_is_away
        return user_hitting, user_pitching, opponent_hitting, has_match

    for home_col, away_col in candidates:
        if home_col in df.columns and away_col in df.columns:
            user_hitting, user_pitching, opponent_hitting, has_match = build_masks(home_col, away_col)
            if has_match.any():
                return user_hitting, user_pitching, opponent_hitting

    for home_col, away_col in candidates:
        if home_col in df.columns and away_col in df.columns:
            user_hitting, user_pitching, opponent_hitting, _ = build_masks(home_col, away_col)
            return user_hitting, user_pitching, opponent_hitting

    empty = pd.Series([False] * len(df), index=df.index)
    return empty, empty, empty


def _pitching_masks(df: pd.DataFrame, username: str) -> tuple[pd.Series, pd.Series]:
    user_hitting, user_pitching, _ = _user_masks(df, username)
    opponent_pitching = user_hitting
    return user_pitching, opponent_pitching
