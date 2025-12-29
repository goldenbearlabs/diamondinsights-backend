from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import MarketCandle, Card
from backend.src.schemas.market_candle import MarketCandleResponse


router = APIRouter(prefix="/market_candles", tags=["market_candles"])

@router.get("/", response_model=List[MarketCandleResponse])
def get_market_candles(
    sort_by: str = Query("buy_volume"),
    series: Optional[str] = Query(None),
    desc: bool = Query(True),
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    "gets all market candles ordered by 'buy_volume desc'"

    query = db.query(MarketCandle, Card).join(Card, MarketCandle.card_id == Card.id)

    # getattr grabs the actual column object from the string name (e.g., "buy_volume")
    if hasattr(MarketCandle, sort_by):
        sort_column = getattr(MarketCandle, sort_by)
        
        if desc:
            query = query.order_by(sort_column.desc())
        else:
            query = query.order_by(sort_column.asc())
    else:
        # if user types a column that doesn't exist
        query = query.order_by(MarketCandle.buy_volume.desc())


    if series:
        query = query.filter(Card.series_name.ilike(series))

    results = query.limit(limit).offset(offset).all()

    market_candles = []
    for market_candle, card in results:
        market_candles.append({
            "card_id": market_candle.card_id,
            "name": card.name,
            "team": card.team_short_name,
            "ovr": card.ovr,
            "series": card.series_name,
            "start_time": market_candle.start_time,
            "open_buy_price": market_candle.open_buy_price,
            "open_sell_price": market_candle.open_sell_price,
            "low_buy_price": market_candle.low_buy_price,
            "high_buy_price": market_candle.high_buy_price,
            "high_sell_price": market_candle.high_sell_price,
            "close_buy_price": market_candle.close_buy_price,
            "close_sell_price": market_candle.close_sell_price,
            "sell_volume": market_candle.sell_volume,
            "buy_volume": market_candle.buy_volume

        })
    


    return market_candles

@router.get("/{card_id}", response_model=MarketCandleResponse)
def get_market_candle(card_id: str, db: Session = Depends(get_db)):
    "gets market candle for a single card by its id"

    query = (
        db.query(MarketCandle, Card)
        .join(Card, MarketCandle.card_id == Card.id)
        .filter(MarketCandle.card_id == card_id)
        .first()
    )
    
    if not query:
        raise HTTPException(status_code=404, detail="Market candle not found")
    
    market_candle, card = query

    return {
        "card_id": market_candle.card_id,
        "name": card.name,
        "team": card.team_short_name,
        "ovr": card.ovr,
        "series": card.series_name,
        "start_time": market_candle.start_time,
        "open_buy_price": market_candle.open_buy_price,
        "open_sell_price": market_candle.open_sell_price,
        "low_buy_price": market_candle.low_buy_price,
        "high_buy_price": market_candle.high_buy_price,
        "high_sell_price": market_candle.high_sell_price,
        "close_buy_price": market_candle.close_buy_price,
        "close_sell_price": market_candle.close_sell_price,
        "sell_volume": market_candle.sell_volume,
        "buy_volume": market_candle.buy_volume
    }

