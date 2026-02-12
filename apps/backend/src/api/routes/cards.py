

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy import or_, func, nullslast
from sqlalchemy.orm import Session, selectinload
from shared.db.database import get_db
from shared.db.models import Card, Comment, UserPrediction, CardPrediction
from src.schemas.card import CardResponse

router = APIRouter(prefix="/cards", tags=["cards"])



@router.get("/{card_id}", response_model=CardResponse)
def get_card(card_id: str, db: Session = Depends(get_db)):
    """
    gets a single card by its id |
    Response Time: ~190ms
    
    """
    comment_count_sub = (
        db.query(func.count(Comment.id))
        .filter(Comment.card_id == Card.id, Comment.is_deleted == False)
        .correlate(Card)
        .scalar_subquery()
        .label("comment_count")
    )
    prediction_count_sub = (
        db.query(func.count(UserPrediction.user_id))
        .filter(UserPrediction.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("user_prediction_count")
    )
    predicted_ovr_sub = (
        db.query(CardPrediction.predicted_ovr)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_ovr")
    )
    predicted_attrs_sub = (
        db.query(CardPrediction.predicted_attributes)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_attributes")
    )

    row = (
        db.query(Card, comment_count_sub, prediction_count_sub, predicted_ovr_sub, predicted_attrs_sub)
        .options(selectinload(Card.position_overalls))
        .filter(Card.id == card_id)
        .first()
    )
    
    if not row:
        raise HTTPException(status_code=404, detail="Card not found")
    
    card, comment_count, user_prediction_count, predicted_ovr, predicted_attributes = row
    card.comment_count = comment_count or 0
    card.user_prediction_count = user_prediction_count or 0
    card.predicted_ovr = predicted_ovr
    card.predicted_attributes = predicted_attributes
    return card

@router.get("/", response_model=List[CardResponse])
def get_cards(
    is_hitter: Optional[bool] = Query(None), 
    team: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    rarity: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
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

    comment_count_sub = (
        db.query(func.count(Comment.id))
        .filter(Comment.card_id == Card.id, Comment.is_deleted == False)
        .correlate(Card)
        .scalar_subquery()
        .label("comment_count")
    )
    prediction_count_sub = (
        db.query(func.count(UserPrediction.user_id))
        .filter(UserPrediction.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("user_prediction_count")
    )
    predicted_ovr_sub = (
        db.query(CardPrediction.predicted_ovr)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_ovr")
    )
    predicted_attrs_sub = (
        db.query(CardPrediction.predicted_attributes)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_attributes")
    )

    query = db.query(Card, comment_count_sub, prediction_count_sub, predicted_ovr_sub, predicted_attrs_sub).options(selectinload(Card.position_overalls))

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
        rarities = [r.strip() for r in rarity.split(',')]
        if len(rarities) == 1:
            query = query.filter(Card.rarity.ilike(rarities[0]))
        else:
            rarity_filters = [Card.rarity.ilike(r) for r in rarities]
            query = query.filter(or_(*rarity_filters))


    # Determine sort column
    if sort_by == "popularity":
        sort_column = prediction_count_sub
    elif sort_by == "predicted_ovr_delta":
        sort_column = predicted_ovr_sub - Card.ovr
    else:
        sort_column = Card.ovr

    if sort_by == "predicted_ovr_delta":
        # Push NULLs (cards with no predictions) to the bottom
        if desc:
            query = query.order_by(nullslast(sort_column.desc()))
        else:
            query = query.order_by(nullslast(sort_column.asc()))
    elif desc:
        query = query.order_by(sort_column.desc())
    else:
        query = query.order_by(sort_column.asc())

    cards = query.limit(limit).offset(offset).all()
    result = []
    for card, comment_count, user_prediction_count, predicted_ovr, predicted_attributes in cards:
        card.comment_count = comment_count or 0
        card.user_prediction_count = user_prediction_count or 0
        card.predicted_ovr = predicted_ovr
        card.predicted_attributes = predicted_attributes
        result.append(card)
    return result
