from typing import List

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import PriceHistory
from src.schemas.price_history import PriceHistoryRow


router = APIRouter(prefix="/price_history", tags=["price_history"])


@router.get("/{card_id}/history", response_model=List[PriceHistoryRow])
def get_card_price_history(
    card_id: str,
    limit: int = Query(365, ge=1, le=2000),
    db: Session = Depends(get_db),
):
    """
    Returns day-by-day price history for a card.
    Ordered oldest -> newest for left-to-right charting.
    """
    rows = (
        db.query(PriceHistory)
        .filter(PriceHistory.card_id == card_id)
        .order_by(PriceHistory.date.asc())
        .limit(limit)
        .all()
    )

    if not rows:
        return []

    return rows
