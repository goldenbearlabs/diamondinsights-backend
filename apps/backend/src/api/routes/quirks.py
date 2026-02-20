from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from shared.db.database import get_db
from shared.db.models import Quirk, card_quirk_association
from src.schemas.quirk import QuirkResponse, CardQuirkResponse

router = APIRouter(prefix="/quirks", tags=["quirks"])


@router.get("/", response_model=List[QuirkResponse])
def get_quirks(
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    "gets all quirks"

    query = db.query(Quirk)

    results = query.limit(limit).offset(offset).all()

    return results


@router.get("/{card_id}", response_model=List[CardQuirkResponse])
def get_quirks_for_card(
    card_id: str,
    db: Session = Depends(get_db)
):
    "gets all quirks for a given card"

    results = (
        db.query(Quirk)
        .join(card_quirk_association, card_quirk_association.c.quirk_name == Quirk.name)
        .filter(card_quirk_association.c.card_id == card_id)
        .all()
    )

    return [
        CardQuirkResponse(card_id=card_id, name=q.name, description=q.description, img=q.img)
        for q in results
    ]
