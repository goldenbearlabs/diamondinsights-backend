from __future__ import annotations

import os
import datetime
from typing import Optional, List, Tuple, Dict, Any

import requests
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from src.database.database import get_db
from src.database.models import Users, ShowProfile, ShowProfileOnlineStats
from src.api.routes.users import firebase_claims


SHOW_SEARCH_URL = os.getenv("SHOW_SEARCH_URL", "https://mlb25.theshow.com/apis/player_search.json")


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


def _fetch_show_profile(username: str) -> Tuple[dict, dict]:
    try:
        r = requests.get(SHOW_SEARCH_URL, params={"username": username}, timeout=10)
    except requests.RequestException:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Failed to reach The Show API")

    if r.status_code != 200:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="The Show API returned an error")

    try:
        data = r.json()
    except Exception:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="The Show API returned invalid JSON")

    profiles = data.get("universal_profiles") or []
    if not profiles:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")

    return profiles[0], data


router = APIRouter(prefix="/users/me/show", tags=["show-profile"])
public_router = APIRouter(prefix="/users", tags=["show-profile"])


class LinkShowBody(BaseModel):
    username: str = Field(min_length=1, max_length=64)


class OnlineStatsOut(BaseModel):
    year: str
    wins: Optional[int] = None
    losses: Optional[int] = None
    hr: Optional[int] = None
    runs_per_game: Optional[float] = None
    stolen_bases: Optional[int] = None
    batting_average: Optional[float] = None
    era: Optional[float] = None
    k_per_9: Optional[float] = None
    whip: Optional[float] = None

    @staticmethod
    def from_orm_row(row: ShowProfileOnlineStats) -> "OnlineStatsOut":
        return OnlineStatsOut(
            year=row.year,
            wins=row.wins,
            losses=row.losses,
            hr=row.hr,
            runs_per_game=row.runs_per_game,
            stolen_bases=row.stolen_bases,
            batting_average=row.batting_average,
            era=row.era,
            k_per_9=row.k_per_9,
            whip=row.whip,
        )


class ShowProfileOut(BaseModel):
    username: str
    display_level: Optional[int] = None
    games_played: Optional[int] = None
    nameplate_equipped: Optional[str] = None
    icon_equipped: Optional[str] = None
    linked_at: datetime.datetime
    last_refreshed_at: datetime.datetime
    online_stats: List[OnlineStatsOut] = Field(default_factory=list)

    @staticmethod
    def from_orm_profile(p: ShowProfile) -> "ShowProfileOut":
        def _sort_key(s: ShowProfileOnlineStats):
            if s.year == "Total":
                return (0, 10**9)
            y = _to_int(s.year)
            return (1, y if y is not None else 0)

        stats = sorted(p.online_stats or [], key=_sort_key)
        return ShowProfileOut(
            username=p.username,
            display_level=p.display_level,
            games_played=p.games_played,
            nameplate_equipped=p.nameplate_equipped,
            icon_equipped=p.icon_equipped,
            linked_at=p.linked_at,
            last_refreshed_at=p.last_refreshed_at,
            online_stats=[OnlineStatsOut.from_orm_row(s) for s in stats],
        )


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


def _ensure_username_unclaimed(db: Session, username: str, current_user_id: int) -> None:
    existing = db.scalar(
        select(ShowProfile).where(func.lower(ShowProfile.username) == func.lower(username))
    )
    if existing and existing.user_id != current_user_id:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already linked by another user")


def _parsed_online_stats(profile_payload: dict) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in (profile_payload.get("online_data") or []):
        yr = row.get("year")
        if not yr:
            continue
        yr = str(yr)

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


def _apply_online_stats(sp: ShowProfile, incoming: Dict[str, Dict[str, Any]]) -> None:
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

    sp = _get_profile_for_user(db, user.id)

    if not sp:
        sp = ShowProfile(
            user_id=user.id,
            username=username,
            display_level=_to_int(profile_payload.get("display_level")),
            games_played=_to_int(profile_payload.get("games_played")),
            nameplate_equipped=vanity.get("nameplate_equipped"),
            icon_equipped=vanity.get("icon_equipped"),
            raw_json=raw,
            linked_at=now,
            last_refreshed_at=now,
        )
        db.add(sp)
        _apply_online_stats(sp, _parsed_online_stats(profile_payload))
        return sp

    sp.username = username
    sp.display_level = _to_int(profile_payload.get("display_level"))
    sp.games_played = _to_int(profile_payload.get("games_played"))
    sp.nameplate_equipped = vanity.get("nameplate_equipped")
    sp.icon_equipped = vanity.get("icon_equipped")
    sp.raw_json = raw
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
        if existing and existing.user_id != user.id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already linked by another user")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to link username")

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load linked profile")
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

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load profile")
    return ShowProfileOut.from_orm_profile(sp)


@router.get("", response_model=ShowProfileOut)
def get_show_profile(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowProfileOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    return ShowProfileOut.from_orm_profile(sp)


@public_router.get("/{user_id}/show", response_model=ShowProfileOut)
def get_show_profile_for_user(
    user_id: int,
    db: Session = Depends(get_db),
) -> ShowProfileOut:
    sp = db.scalar(select(ShowProfile).where(ShowProfile.user_id == user_id))
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    return ShowProfileOut.from_orm_profile(sp)
