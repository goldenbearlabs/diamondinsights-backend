from __future__ import annotations

from typing import Optional, List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import select, func, or_
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Player, ShowProfile, Users
from src.api.routes.users import firebase_claims

from .common import _load_facts_df_for_username, _user_masks
from .models import ShowProfileSearchOut, ShowPitcherSearchOut, ShowHitterSearchOut
from .profile import _get_authed_user, _get_profile_for_user, _get_profile_by_username


router = APIRouter()
public_router = APIRouter()


def _pitcher_counts_for_username(df: pd.DataFrame, username: str) -> pd.Series:
    _, user_pitching, _ = _user_masks(df, username)
    user_df = df[user_pitching]
    pitcher_col = user_df.get("pitcher_mlb_id")
    if pitcher_col is None:
        return pd.Series(dtype=int)
    pitcher_ids = pd.to_numeric(pitcher_col, errors="coerce").dropna().astype(int)
    if pitcher_ids.empty:
        return pd.Series(dtype=int)
    return pitcher_ids.value_counts()


def _batter_counts_for_username(
    df: pd.DataFrame,
    username: str,
    view: Optional[str] = None,
) -> pd.Series:
    user_hitting, user_pitching, _ = _user_masks(df, username)
    view_norm = (view or "").strip().lower()
    if view_norm in ("pitching", "pitch"):
        user_df = df[user_pitching]
    elif view_norm in ("hitting", "hit"):
        user_df = df[user_hitting]
    else:
        user_df = df[user_hitting | user_pitching]
    batter_col = user_df.get("batter_mlb_id")
    if batter_col is None:
        return pd.Series(dtype=int)
    batter_ids = pd.to_numeric(batter_col, errors="coerce").dropna().astype(int)
    if batter_ids.empty:
        return pd.Series(dtype=int)
    return batter_ids.value_counts()


@router.get("/pitchers", response_model=List[ShowPitcherSearchOut])
def search_show_pitchers(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    q: Optional[str] = Query(default=None, max_length=64),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowPitcherSearchOut]:
    user = _get_authed_user(db, claims)
    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _pitcher_counts_for_username(df, sp.username)
    if counts.empty:
        return []
    pitcher_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(pitcher_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowPitcherSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            pitch_hand_code=p.pitch_hand_code,
        )
        for p in results
    ]


@router.get("/hitters", response_model=List[ShowHitterSearchOut])
def search_show_hitters(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    q: Optional[str] = Query(default=None, max_length=64),
    view: Optional[str] = Query(default=None, max_length=8),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowHitterSearchOut]:
    user = _get_authed_user(db, claims)
    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _batter_counts_for_username(df, sp.username, view=view)
    if counts.empty:
        return []
    batter_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(batter_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowHitterSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            bat_side_code=p.bat_side_code,
        )
        for p in results
    ]


@public_router.get("/show/search", response_model=List[ShowProfileSearchOut])
def search_show_profiles(
    q: str = Query(min_length=1, max_length=64),
    limit: int = Query(default=10, ge=1, le=50),
    db: Session = Depends(get_db),
) -> List[ShowProfileSearchOut]:
    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    stmt = (
        select(ShowProfile, Users)
        .outerjoin(Users, Users.id == ShowProfile.user_id)
        .where(func.lower(ShowProfile.username).ilike(f"%{q_norm}%"))
        .order_by(ShowProfile.username.asc())
        .limit(limit)
    )
    rows = db.execute(stmt).all()
    return [
        ShowProfileSearchOut(
            user_id=sp.user_id,
            username=sp.username,
            display_name=user.display_name if user else None,
            profile_img_url=user.profile_img_url if user else None,
        )
        for sp, user in rows
    ]


@public_router.get("/show/{username}/pitchers", response_model=List[ShowPitcherSearchOut])
def search_show_pitchers_by_username(
    username: str,
    db: Session = Depends(get_db),
    q: Optional[str] = Query(default=None, max_length=64),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowPitcherSearchOut]:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _pitcher_counts_for_username(df, sp.username)
    if counts.empty:
        return []
    pitcher_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(pitcher_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowPitcherSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            pitch_hand_code=p.pitch_hand_code,
        )
        for p in results
    ]


@public_router.get("/show/{username}/hitters", response_model=List[ShowHitterSearchOut])
def search_show_hitters_by_username(
    username: str,
    db: Session = Depends(get_db),
    q: Optional[str] = Query(default=None, max_length=64),
    view: Optional[str] = Query(default=None, max_length=8),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowHitterSearchOut]:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _batter_counts_for_username(df, sp.username, view=view)
    if counts.empty:
        return []
    batter_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(batter_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowHitterSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            bat_side_code=p.bat_side_code,
        )
        for p in results
    ]


@public_router.get("/{user_id}/show/pitchers", response_model=List[ShowPitcherSearchOut])
def search_show_pitchers_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    q: Optional[str] = Query(default=None, max_length=64),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowPitcherSearchOut]:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _pitcher_counts_for_username(df, sp.username)
    if counts.empty:
        return []
    pitcher_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(pitcher_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowPitcherSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            pitch_hand_code=p.pitch_hand_code,
        )
        for p in results
    ]


@public_router.get("/{user_id}/show/hitters", response_model=List[ShowHitterSearchOut])
def search_show_hitters_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    q: Optional[str] = Query(default=None, max_length=64),
    view: Optional[str] = Query(default=None, max_length=8),
    limit: int = Query(default=12, ge=1, le=50),
) -> List[ShowHitterSearchOut]:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    q_norm = (q or "").strip().lower()
    if not q_norm:
        return []

    df = _load_facts_df_for_username(sp.username)
    counts = _batter_counts_for_username(df, sp.username, view=view)
    if counts.empty:
        return []
    batter_ids = counts.index.tolist()

    like = f"%{q_norm}%"
    players = (
        db.query(Player)
        .filter(Player.mlb_id.in_(batter_ids))
        .filter(
            or_(
                func.lower(Player.full_name).ilike(like),
                func.lower(Player.first_name).ilike(like),
                func.lower(Player.last_name).ilike(like),
            )
        )
        .all()
    )
    if not players:
        return []

    def sort_key(player: Player) -> tuple:
        return (
            -int(counts.get(player.mlb_id, 0)),
            (player.last_name or "").lower(),
            (player.first_name or "").lower(),
        )

    players.sort(key=sort_key)
    results = players[:limit]
    return [
        ShowHitterSearchOut(
            mlb_id=p.mlb_id,
            full_name=p.full_name,
            first_name=p.first_name,
            last_name=p.last_name,
            bat_side_code=p.bat_side_code,
        )
        for p in results
    ]
