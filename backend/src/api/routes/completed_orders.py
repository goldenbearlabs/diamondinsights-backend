from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import Card, CompletedOrder
from backend.src.schemas.completed_order import CompletedOrderResponse
from datetime import datetime


router = APIRouter(prefix="/completed_orders", tags=["completed_orders"])

@router.get("/", response_model=List[CompletedOrderResponse])
def get_completed_orders(
    card_id: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    desc: bool = Query(True),
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    
    """
    gets the completed orders for cards (can filter by specific card_id) |
    Response Time: ~230 - 330 ms | 
    Joins with CARD table ON card_id, to get name, team, ovr, and series
    
    """

    query = db.query(CompletedOrder, Card).join(Card, CompletedOrder.card_id == Card.id)

    if card_id is not None:
        query = query.filter(CompletedOrder.card_id == card_id)

    if desc:
        query = query.order_by(CompletedOrder.date.desc())
    else:
        query = query.order_by(CompletedOrder.date.asc())

    if series:
        query = query.filter(Card.series_name.ilike(series))

    results = query.limit(limit).offset(offset).all()

    completed_orders = []
    for completed_order, card in results:
        completed_orders.append({
            "card_id": completed_order.card_id,
            "name": card.name,
            "team": card.team_short_name,
            "ovr": card.ovr,
            "series": card.series_name,
            "date": completed_order.date,
            "price": completed_order.price,
            "is_buy": completed_order.is_buy

        })

    return completed_orders

    

