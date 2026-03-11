from datetime import date
from typing import Optional

from pydantic import BaseModel


class PriceHistoryRow(BaseModel):
    card_id: str
    date: date
    best_buy_price: Optional[int] = None
    best_sell_price: Optional[int] = None
    volume: Optional[int] = None

    class Config:
        from_attributes = True
