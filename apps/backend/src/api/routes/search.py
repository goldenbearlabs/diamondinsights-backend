from __future__ import annotations

import unicodedata
from typing import List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from redis import Redis
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from src.core.cache import (
    CACHE_DEFAULT_TTL_SEC,
    build_cache_key,
    get_cache_client,
    get_cached_json,
    set_cached_json,
)
from shared.db.database import get_db
from shared.db.models import Card, CardPositionOverall, Users

router = APIRouter(prefix="/search", tags=["search"])


def _normalize_search(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


class UserSearchOut(BaseModel):
    id: int
    display_name: str
    profile_img_url: str

    @staticmethod
    def from_orm(u: Users) -> "UserSearchOut":
        return UserSearchOut(
            id=u.id,
            display_name=u.display_name,
            profile_img_url=u.profile_img_url,
        )


class CardSearchOut(BaseModel):
    id: str
    name: str
    year: int
    ovr: int
    is_live_set: bool
    series_name: str
    rarity: str
    img: str | None = None
    baked_img: str | None = None
    meta_overall_rounded: int | None = None

    @staticmethod
    def from_orm(c: Card, meta_overall_rounded: int | None = None) -> "CardSearchOut":
        return CardSearchOut(
            id=c.id,
            name=c.name,
            year=c.year,
            ovr=c.ovr,
            is_live_set=c.is_live_set,
            series_name=c.series_name,
            rarity=c.rarity,
            img=c.img,
            baked_img=c.baked_img,
            meta_overall_rounded=meta_overall_rounded,
        )


class SearchResponse(BaseModel):
    users: List[UserSearchOut] = Field(default_factory=list)
    cards: List[CardSearchOut] = Field(default_factory=list)


@router.get("", response_model=SearchResponse)
def search(
    q: str = Query(min_length=1, max_length=80),
    users_only: bool = Query(default=False),
    cards_only: bool = Query(default=False),
    year: int | None = Query(default=25), # Added default year filter
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
    cache: Redis | None = Depends(get_cache_client),
) -> SearchResponse:
    q_norm = _normalize_search(q)
    if not q_norm:
        return SearchResponse()

    search_users = True
    search_cards = True
    if users_only and not cards_only:
        search_cards = False
    elif cards_only and not users_only:
        search_users = False

    cache_key = build_cache_key("search_meta_v2", q_norm, users_only, cards_only, year, limit)
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        return SearchResponse.model_validate(cached)

    users_out: List[UserSearchOut] = []
    cards_out: List[CardSearchOut] = []
 
    if search_users:
        users_stmt = (
            select(Users)
            .where(Users.search_display_name.ilike(f"%{q_norm}%"))
            .order_by(Users.display_name.asc())
            .limit(limit)
        )
        users = db.scalars(users_stmt).all()
        users_out = [UserSearchOut.from_orm(u) for u in users]

    if search_cards:
        preferred_position_meta_overall = (
            select(CardPositionOverall.meta_overall_rounded)
            .where(CardPositionOverall.card_id == Card.id)
            .where(func.upper(CardPositionOverall.position) == func.upper(Card.display_position))
            .correlate(Card)
            .limit(1)
            .scalar_subquery()
        )
        primary_flag_meta_overall = (
            select(CardPositionOverall.meta_overall_rounded)
            .where(CardPositionOverall.card_id == Card.id)
            .where(CardPositionOverall.is_primary.is_(True))
            .order_by(desc(CardPositionOverall.meta_overall_rounded))
            .correlate(Card)
            .limit(1)
            .scalar_subquery()
        )
        max_meta_overall = (
            select(CardPositionOverall.meta_overall_rounded)
            .where(CardPositionOverall.card_id == Card.id)
            .order_by(desc(CardPositionOverall.meta_overall_rounded))
            .correlate(Card)
            .limit(1)
            .scalar_subquery()
        )
        meta_overall_rounded = func.coalesce(
            preferred_position_meta_overall,
            primary_flag_meta_overall,
            max_meta_overall,
        ).label("meta_overall_rounded")
        meta_sort_value = func.coalesce(meta_overall_rounded, Card.ovr)

        cards_stmt = (
            select(Card, meta_overall_rounded)
            .where(Card.search_name.ilike(f"%{q_norm}%"))
        )
        
        if year is not None:
            cards_stmt = cards_stmt.where(Card.year == year)
            
        cards_stmt = cards_stmt.order_by(
            desc(Card.year),
            desc(Card.is_live_set),
            desc(meta_sort_value),
            desc(Card.ovr),
        ).limit(limit)

        cards = db.execute(cards_stmt).all()
        cards_out = [
            CardSearchOut.from_orm(
                card,
                meta_overall_rounded=int(meta_ovr) if meta_ovr is not None else None,
            )
            for card, meta_ovr in cards
        ]

    response = SearchResponse(users=users_out, cards=cards_out)
    set_cached_json(cache, cache_key, response.model_dump(mode="json"), ttl_sec=CACHE_DEFAULT_TTL_SEC)
    return response
