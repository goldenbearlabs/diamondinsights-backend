from __future__ import annotations

import unicodedata
from typing import List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from src.database.database import get_db
from src.database.models import Card, Users

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
    img: str

    @staticmethod
    def from_orm(c: Card) -> "CardSearchOut":
        return CardSearchOut(
            id=c.id,
            name=c.name,
            year=c.year,
            ovr=c.ovr,
            is_live_set=c.is_live_set,
            series_name=c.series_name,
            rarity=c.rarity,
            img=c.img,
        )


class SearchResponse(BaseModel):
    users: List[UserSearchOut] = Field(default_factory=list)
    cards: List[CardSearchOut] = Field(default_factory=list)


@router.get("", response_model=SearchResponse)
def search(
    q: str = Query(min_length=1, max_length=80),
    users_only: bool = Query(default=False),
    cards_only: bool = Query(default=False),
    limit: int = Query(default=10, ge=1, le=100),
    db: Session = Depends(get_db),
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
        cards_stmt = (
            select(Card)
            .where(Card.search_name.ilike(f"%{q_norm}%"))
            .order_by(
                desc(Card.year),
                desc(Card.is_live_set),
                desc(Card.ovr),
            )
            .limit(limit)
        )
        cards = db.scalars(cards_stmt).all()
        cards_out = [CardSearchOut.from_orm(c) for c in cards]

    return SearchResponse(users=users_out, cards=cards_out)
