from __future__ import annotations

import json
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy import select, func, case, and_, or_
from sqlalchemy.orm import Session, selectinload

from shared.db.database import get_db
from shared.db.models import ShowGameSummary
from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from src.api.routes.users import firebase_claims

from .models import (
    ShowGameSummaryOut,
    ShowGameLogItemOut,
    ShowGameEventOut,
    ShowGameBundleOut,
)
from .profile import _get_authed_user, _get_profile_for_user, _get_profile_by_username


router = APIRouter()
public_router = APIRouter()


def _show_game_summary_for_username(db: Session, username: str) -> ShowGameSummaryOut:
    filters = or_(
        ShowGameSummary.home_profile_username == username,
        ShowGameSummary.away_profile_username == username,
    )

    wins_case = case(
        (
            and_(
                ShowGameSummary.home_profile_username == username,
                ShowGameSummary.home_result == "W",
            ),
            1,
        ),
        (
            and_(
                ShowGameSummary.away_profile_username == username,
                ShowGameSummary.away_result == "W",
            ),
            1,
        ),
        else_=0,
    )
    losses_case = case(
        (
            and_(
                ShowGameSummary.home_profile_username == username,
                ShowGameSummary.home_result == "L",
            ),
            1,
        ),
        (
            and_(
                ShowGameSummary.away_profile_username == username,
                ShowGameSummary.away_result == "L",
            ),
            1,
        ),
        else_=0,
    )

    summary_row = db.execute(
        select(
            func.count(ShowGameSummary.id).label("games_played"),
            func.coalesce(func.sum(wins_case), 0).label("wins"),
            func.coalesce(func.sum(losses_case), 0).label("losses"),
        ).where(filters)
    ).one()

    last_game = db.scalar(
        select(ShowGameSummary).where(filters).order_by(ShowGameSummary.date.desc())
    )

    games_played = int(summary_row.games_played or 0)
    wins = int(summary_row.wins or 0)
    losses = int(summary_row.losses or 0)

    return ShowGameSummaryOut(
        games_played=games_played,
        wins=wins,
        losses=losses,
        record=f"{wins}-{losses}",
        last_game_date=last_game.date if last_game else None,
        last_game_difficulty=last_game.difficulty if last_game else None,
    )


def _show_game_log_for_username(
    db: Session,
    username: str,
    limit: int,
) -> List[ShowGameLogItemOut]:
    filters = or_(
        ShowGameSummary.home_profile_username == username,
        ShowGameSummary.away_profile_username == username,
    )

    rows = (
        db.scalars(
            select(ShowGameSummary)
            .options(selectinload(ShowGameSummary.ball_park))
            .where(filters)
            .order_by(ShowGameSummary.date.desc())
            .limit(limit)
        )
        .unique()
        .all()
    )

    return [ShowGameLogItemOut.from_orm_row(row) for row in rows]


def _assert_game_belongs_to_username(db: Session, username: str, game_id: str) -> None:
    filters = or_(
        ShowGameSummary.home_profile_username == username,
        ShowGameSummary.away_profile_username == username,
    )
    row = db.scalar(
        select(ShowGameSummary.id).where(
            ShowGameSummary.id == game_id,
            filters,
        )
    )
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Game not found")


def _load_game_jsonl_from_spaces(
    game_id: str,
    filename: str,
    *,
    required: bool = False,
) -> List[dict]:
    key = f"games/{game_id}/{filename}"
    cfg = SpacesConfig.from_env()
    spaces = SpacesConnector(cfg)
    try:
        raw = spaces.get_bytes(key)
    except Exception:
        if required:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Game file not found")
        return []
    if not raw:
        return []
    text = raw.decode("utf-8", errors="ignore")
    events: List[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _load_game_events_from_spaces(game_id: str) -> List[dict]:
    return _load_game_jsonl_from_spaces(game_id, "events.jsonl", required=True)


@router.get("/summary", response_model=ShowGameSummaryOut)
def get_show_game_summary(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowGameSummaryOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    return _show_game_summary_for_username(db, sp.username)


@router.get("/game-log", response_model=List[ShowGameLogItemOut])
def get_show_game_log(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
    limit: int = Query(default=200, ge=1, le=500),
) -> List[ShowGameLogItemOut]:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    return _show_game_log_for_username(db, sp.username, limit)


@router.get("/game-events/{game_id}", response_model=List[ShowGameEventOut])
def get_show_game_events(
    game_id: str,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> List[ShowGameEventOut]:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    _assert_game_belongs_to_username(db, sp.username, game_id)
    return _load_game_events_from_spaces(game_id)


@router.get("/game-bundle/{game_id}", response_model=ShowGameBundleOut)
def get_show_game_bundle(
    game_id: str,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> ShowGameBundleOut:
    user = _get_authed_user(db, claims)

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No linked username")

    _assert_game_belongs_to_username(db, sp.username, game_id)
    return ShowGameBundleOut(
        events=_load_game_jsonl_from_spaces(game_id, "events.jsonl", required=True),
        half_innings=_load_game_jsonl_from_spaces(game_id, "half_innings.jsonl"),
        plate_appearances=_load_game_jsonl_from_spaces(game_id, "plate_appearances.jsonl"),
        batter_boxscores=_load_game_jsonl_from_spaces(game_id, "batter_boxscores.jsonl"),
        pitcher_boxscores=_load_game_jsonl_from_spaces(game_id, "pitcher_boxscores.jsonl"),
    )


@public_router.get("/show/{username}/summary", response_model=ShowGameSummaryOut)
def get_show_game_summary_by_username(
    username: str,
    db: Session = Depends(get_db),
) -> ShowGameSummaryOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")
    return _show_game_summary_for_username(db, sp.username)


@public_router.get("/show/{username}/game-log", response_model=List[ShowGameLogItemOut])
def get_show_game_log_by_username(
    username: str,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=500),
) -> List[ShowGameLogItemOut]:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")
    return _show_game_log_for_username(db, sp.username, limit)


@public_router.get("/show/{username}/game-events/{game_id}", response_model=List[ShowGameEventOut])
def get_show_game_events_by_username(
    username: str,
    game_id: str,
    db: Session = Depends(get_db),
) -> List[ShowGameEventOut]:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")
    _assert_game_belongs_to_username(db, sp.username, game_id)
    return _load_game_events_from_spaces(game_id)


@public_router.get("/show/{username}/game-bundle/{game_id}", response_model=ShowGameBundleOut)
def get_show_game_bundle_by_username(
    username: str,
    game_id: str,
    db: Session = Depends(get_db),
) -> ShowGameBundleOut:
    sp = _get_profile_by_username(db, username)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Username not found")
    _assert_game_belongs_to_username(db, sp.username, game_id)
    return ShowGameBundleOut(
        events=_load_game_jsonl_from_spaces(game_id, "events.jsonl", required=True),
        half_innings=_load_game_jsonl_from_spaces(game_id, "half_innings.jsonl"),
        plate_appearances=_load_game_jsonl_from_spaces(game_id, "plate_appearances.jsonl"),
        batter_boxscores=_load_game_jsonl_from_spaces(game_id, "batter_boxscores.jsonl"),
        pitcher_boxscores=_load_game_jsonl_from_spaces(game_id, "pitcher_boxscores.jsonl"),
    )


@public_router.get("/{user_id}/show/summary", response_model=ShowGameSummaryOut)
def get_show_game_summary_for_user(
    user_id: int,
    db: Session = Depends(get_db),
) -> ShowGameSummaryOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")
    return _show_game_summary_for_username(db, sp.username)


@public_router.get("/{user_id}/show/game-log", response_model=List[ShowGameLogItemOut])
def get_show_game_log_for_user(
    user_id: int,
    db: Session = Depends(get_db),
    limit: int = Query(default=200, ge=1, le=500),
) -> List[ShowGameLogItemOut]:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")
    return _show_game_log_for_username(db, sp.username, limit)


@public_router.get("/{user_id}/show/game-events/{game_id}", response_model=List[ShowGameEventOut])
def get_show_game_events_for_user(
    user_id: int,
    game_id: str,
    db: Session = Depends(get_db),
) -> List[ShowGameEventOut]:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")
    _assert_game_belongs_to_username(db, sp.username, game_id)
    return _load_game_events_from_spaces(game_id)


@public_router.get("/{user_id}/show/game-bundle/{game_id}", response_model=ShowGameBundleOut)
def get_show_game_bundle_for_user(
    user_id: int,
    game_id: str,
    db: Session = Depends(get_db),
) -> ShowGameBundleOut:
    sp = _get_profile_for_user(db, user_id)
    if not sp:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Profile not found")
    _assert_game_belongs_to_username(db, sp.username, game_id)
    return ShowGameBundleOut(
        events=_load_game_jsonl_from_spaces(game_id, "events.jsonl", required=True),
        half_innings=_load_game_jsonl_from_spaces(game_id, "half_innings.jsonl"),
        plate_appearances=_load_game_jsonl_from_spaces(game_id, "plate_appearances.jsonl"),
        batter_boxscores=_load_game_jsonl_from_spaces(game_id, "batter_boxscores.jsonl"),
        pitcher_boxscores=_load_game_jsonl_from_spaces(game_id, "pitcher_boxscores.jsonl"),
    )
