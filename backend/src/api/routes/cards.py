

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import Card
from backend.src.schemas.card import CardResponse

router = APIRouter(prefix="/cards", tags=["cards"])



@router.get("/{card_id}", response_model=CardResponse)
def get_card(card_id: str, db: Session = Depends(get_db)):
    """
    gets a single card by its id |
    Response Time: ~190ms
    
    """
    
    card = db.get(Card, card_id)
    
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
        
    return card

@router.get("/", response_model=List[CardResponse])
def get_cards(
    is_hitter: Optional[bool] = Query(None), 
    team: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    rarity: Optional[str] = Query(None),
    desc: bool = Query(True),
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    """
    gets multiple cards (with optional filters) |
    Response Time: ~150 - 240ms for first time loading |
    These queries don't join with any other tables.
    """

    query = db.query(Card)

    # filtering
    if is_hitter is not None:
        query = query.filter(Card.is_hitter == is_hitter)

    if team is not None:
        query = query.filter(Card.team_short_name.ilike(team)) 

    if name is not None:
        query = query.filter(Card.name.ilike(f"%{name}%"))

    if series is not None:
        query = query.filter(Card.series_name.ilike(series))

    if year is not None:
        query = query.filter(Card.year == year)

    if rarity is not None:
        query = query.filter(Card.rarity.ilike(rarity))

    # order by ovr desc (by default)
    if desc:
        query = query.order_by(Card.ovr.desc())
    else:
        query = query.order_by(Card.ovr.asc())

    cards = query.limit(limit).offset(offset).all()
    return cards

