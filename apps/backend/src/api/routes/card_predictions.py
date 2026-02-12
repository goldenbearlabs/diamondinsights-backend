from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy import or_
from sqlalchemy.orm import Session
from shared.db.database import get_db
from shared.db.models import Card, CardPrediction
from src.schemas.card import CardResponse
from src.schemas.card_prediction import CardPredictionResponse

router = APIRouter(prefix="/card_predictions", tags=["card_predictions"])

@router.get("/{card_id}", response_model=CardPredictionResponse)
def get_card_prediction(card_id: str, db: Session = Depends(get_db)):
    """
    gets the latest card prediction for a given card id |
    Response Time: ~200ms
    
    """
    
    result = (
        db.query(CardPrediction, Card)
        .join(Card, Card.id == CardPrediction.card_id)
        .filter(CardPrediction.card_id == card_id)
        .order_by(CardPrediction.run_id.desc())
        .first()
    )
    
    if not result:
        raise HTTPException(status_code=404, detail="Card prediction not found")
    
    pred, card = result

    return {
        "card_id": pred.card_id,
        "name": card.name,
        "team_short_name": card.team_short_name,
        "series_name": card.series_name,
        "current_ovr": card.ovr,
        "predicted_ovr": pred.predicted_ovr,
        "predicted_rarity": pred.predicted_rarity,
        "predicted_attributes": pred.predicted_attributes,
        "created_at": pred.created_at
    }
    