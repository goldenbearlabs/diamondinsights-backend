from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from shared.db.database import get_db
from shared.db.models import Card, Pitch
from src.schemas.pitch import PitchResponse

router = APIRouter(prefix="/pitches", tags=["pitches"])

@router.get("/{card_id}", response_model=List[PitchResponse])
def get_pitch_for_card(
    card_id: str,
    db: Session = Depends(get_db)
):
    "gets the pitch attributes for a given card"

    pitches = db.query(Pitch).filter(Pitch.card_id == card_id).all()
    if not pitches:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pitch not found")

    return pitches