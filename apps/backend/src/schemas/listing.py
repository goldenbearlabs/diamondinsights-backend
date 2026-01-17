from pydantic import BaseModel
from typing import Optional

class ListingResponse(BaseModel):
    card_id: str
    name: Optional[str] = None
    team: Optional[str] = None
    ovr: Optional[int] = None
    series: Optional[str] = None
    best_sell_price: int
    best_buy_price: int
    

    class Config:
        from_attributes = True