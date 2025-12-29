from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class CompletedOrderResponse(BaseModel):
    card_id: str
    name: Optional[str] = None
    team: Optional[str] = None
    ovr: Optional[int] = None
    series: Optional[str] =None
    date: Optional[datetime] = None
    price: int
    is_buy: Optional[bool] = None
    

    class Config:
        from_attributes = True