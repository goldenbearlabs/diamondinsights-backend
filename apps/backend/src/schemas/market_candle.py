from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class MarketCandleResponse(BaseModel):
    card_id: str
    name: Optional[str] = None
    team: Optional[str] = None
    ovr: Optional[int] = None
    series: Optional[str] = None
    start_time: Optional[datetime] = None
    open_buy_price: int
    open_sell_price: int
    low_buy_price: int
    high_buy_price: int
    high_sell_price: int
    close_buy_price: int
    close_sell_price: int
    sell_volume: int
    buy_volume: int
   
    

    class Config:
        from_attributes = True