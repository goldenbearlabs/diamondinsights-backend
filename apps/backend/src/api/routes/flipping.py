from datetime import datetime, timedelta
from math import floor
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Card, CompletedOrder, Listing
from src.schemas.flipping import FlippingRow


router = APIRouter(prefix="/flipping", tags=["flipping"])


def _base_quicksell_for_ovr(ovr: int) -> int:
    if ovr >= 92:
        return 10000

    values = {
        91: 9000,
        90: 8000,
        89: 7000,
        88: 5500,
        87: 4500,
        86: 3750,
        85: 3000,
        84: 1500,
        83: 1200,
        82: 900,
        81: 600,
        80: 400,
        79: 150,
        78: 125,
        77: 100,
        76: 75,
        75: 50,
    }
    if ovr in values:
        return values[ovr]
    if ovr >= 65:
        return 25
    return 5


def _quicksell_for_card(ovr: int, series_name: Optional[str]) -> int:
    base = _base_quicksell_for_ovr(ovr)
    is_live_series = (series_name or "").strip().lower() == "live"
    if is_live_series:
        return base
    return floor(base * 0.5)


@router.get("", response_model=List[FlippingRow], include_in_schema=False)
@router.get("/", response_model=List[FlippingRow])
def get_flipping_rows(
    series: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    profitable_only: bool = Query(False),
    sort_by: str = Query("profit_per_min"),
    sort_dir: str = Query("desc", regex="^(asc|desc)$"),
    min_buy: Optional[int] = Query(None, ge=0),
    max_buy: Optional[int] = Query(None, ge=0),
    min_sell: Optional[int] = Query(None, ge=0),
    max_sell: Optional[int] = Query(None, ge=0),
    min_ovr: Optional[int] = Query(None, ge=0, le=99),
    max_ovr: Optional[int] = Query(None, ge=0, le=99),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Returns card-level flipping rows with quicksell fallback and 1h activity.
    """

    cutoff = datetime.utcnow() - timedelta(hours=1)

    activity_subquery = (
        db.query(
            CompletedOrder.card_id.label("card_id"),
            func.count().label("orders_1h"),
            func.sum(case((CompletedOrder.is_buy.is_(True), 1), else_=0)).label(
                "buys_1h"
            ),
            func.sum(case((CompletedOrder.is_buy.is_(False), 1), else_=0)).label(
                "sells_1h"
            ),
            func.avg(CompletedOrder.price).label("avg_completed_price_1h"),
            func.max(CompletedOrder.date).label("latest_completed_order_at"),
        )
        .filter(CompletedOrder.date >= cutoff)
        .group_by(CompletedOrder.card_id)
        .subquery()
    )

    query = (
        db.query(
            Listing,
            Card,
            activity_subquery.c.orders_1h,
            activity_subquery.c.buys_1h,
            activity_subquery.c.sells_1h,
            activity_subquery.c.avg_completed_price_1h,
            activity_subquery.c.latest_completed_order_at,
        )
        .join(Card, Listing.card_id == Card.id)
        .outerjoin(activity_subquery, Listing.card_id == activity_subquery.c.card_id)
    )

    if series:
        query = query.filter(Card.series_name.ilike(f"%{series}%"))
    if name:
        query = query.filter(Card.name.ilike(f"%{name}%"))
    if min_ovr is not None:
        query = query.filter(Card.ovr >= min_ovr)
    if max_ovr is not None:
        query = query.filter(Card.ovr <= max_ovr)

    results = query.all()

    rows = []
    for (
        listing,
        card,
        orders_1h,
        buys_1h,
        sells_1h,
        avg_completed_price_1h,
        latest_completed_order_at,
    ) in results:
        best_sell_price = int(listing.best_sell_price or 0)
        best_buy_price = int(listing.best_buy_price or 0)
        quicksell_price = _quicksell_for_card(int(card.ovr), card.series_name)

        uses_quicksell_buy = best_buy_price <= 0
        effective_buy_price = quicksell_price if uses_quicksell_buy else best_buy_price
        after_tax_sell_price = floor(best_sell_price * 0.9)

        spread = best_sell_price - effective_buy_price
        profit = after_tax_sell_price - effective_buy_price

        if min_buy is not None and effective_buy_price < min_buy:
            continue
        if max_buy is not None and effective_buy_price > max_buy:
            continue
        if min_sell is not None and best_sell_price < min_sell:
            continue
        if max_sell is not None and best_sell_price > max_sell:
            continue

        if profitable_only and profit <= 0:
            continue

        profit_margin_pct = None
        if effective_buy_price > 0:
            profit_margin_pct = round((profit / effective_buy_price) * 100, 2)

        avg_price = None
        if avg_completed_price_1h is not None:
            avg_price = int(round(float(avg_completed_price_1h)))

        rows.append(
            {
                "card_id": listing.card_id,
                "name": card.name,
                "team": card.team_short_name,
                "ovr": card.ovr,
                "series": card.series_name,
                "year": card.year,
                "baked_img": card.baked_img,
                "best_sell_price": best_sell_price,
                "best_buy_price": best_buy_price,
                "effective_buy_price": effective_buy_price,
                "quicksell_price": quicksell_price,
                "uses_quicksell_buy": uses_quicksell_buy,
                "after_tax_sell_price": after_tax_sell_price,
                "spread": spread,
                "profit": profit,
                "profit_margin_pct": profit_margin_pct,
                "orders_1h": int(orders_1h or 0),
                "buys_1h": int(buys_1h or 0),
                "sells_1h": int(sells_1h or 0),
                "avg_completed_price_1h": avg_price,
                "latest_completed_order_at": latest_completed_order_at,
            }
        )

    sort_key_map = {
        "profit": lambda item: item["profit"],
        "spread": lambda item: item["spread"],
        "profit_per_min": lambda item: ((item["orders_1h"] / 2) * item["profit"]) / 60,
        "margin": lambda item: (
            item["profit_margin_pct"]
            if item["profit_margin_pct"] is not None
            else float("-inf")
        ),
        "orders": lambda item: item["orders_1h"],
        "buys": lambda item: item["buys_1h"],
        "sells": lambda item: item["sells_1h"],
        "buys_sells": lambda item: (item["buys_1h"], item["sells_1h"]),
        "buy": lambda item: item["effective_buy_price"],
        "sell": lambda item: item["best_sell_price"],
        "ovr": lambda item: item["ovr"],
        "name": lambda item: (item["name"] or "").lower(),
    }

    sort_key = sort_key_map.get(sort_by, sort_key_map["profit"])
    reverse = sort_dir.lower() == "desc"
    rows.sort(key=sort_key, reverse=reverse)

    return rows[offset : offset + limit]
