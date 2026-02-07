from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy import func
from sqlalchemy.orm import Session
from shared.db.database import get_db
from shared.db.models import Card, CompletedOrder
from src.schemas.completed_order import CompletedOrderResponse, CompletedOrderRow


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


@router.get("/latest", response_model=List[CompletedOrderRow])
def get_latest_completed_orders(
    card_id: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    is_buy: Optional[bool] = Query(None),
    limit: int = Query(50, le=100),
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    gets latest completed order per card (optionally filtered by series or name)
    """

    latest_subq = (
        db.query(
            CompletedOrder.card_id,
            func.max(CompletedOrder.date).label("max_date")
        )
        .group_by(CompletedOrder.card_id)
        .subquery()
    )

    query = (
        db.query(CompletedOrder)
        .join(
            latest_subq,
            (CompletedOrder.card_id == latest_subq.c.card_id)
            & (CompletedOrder.date == latest_subq.c.max_date)
        )
        .join(Card, CompletedOrder.card_id == Card.id)
    )

    if card_id is not None:
        query = query.filter(CompletedOrder.card_id == card_id)

    if series is not None:
        query = query.filter(Card.series_name.ilike(series))

    if name is not None:
        query = query.filter(Card.name.ilike(f"%{name}%"))

    if is_buy is not None:
        query = query.filter(CompletedOrder.is_buy == is_buy)

    query = query.order_by(CompletedOrder.date.desc())

    return query.limit(limit).offset(offset).all()

    
@router.get("/{card_id}/history", response_model=List[CompletedOrderRow])
def get_card_order_history(
    card_id: str,
    limit: int = Query(500, le=1000), 
    db: Session = Depends(get_db)
):
    """
    Gets raw transaction history for a specific card.
    Ordered by Oldest -> Newest for graphing.
    """
    orders = (
        db.query(CompletedOrder)
        .filter(CompletedOrder.card_id == card_id)
        .order_by(CompletedOrder.date.asc()) # Critical for graphs
        .limit(limit)
        .all()
    )
    
    if not orders:
        return []

    return orders