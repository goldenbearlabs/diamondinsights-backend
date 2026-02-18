from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class FlippingRow(BaseModel):
    card_id: str
    name: Optional[str] = None
    team: Optional[str] = None
    ovr: int
    series: Optional[str] = None
    year: Optional[int] = None
    baked_img: Optional[str] = None
    best_sell_price: int
    best_buy_price: int
    effective_buy_price: int
    quicksell_price: int
    uses_quicksell_buy: bool
    after_tax_sell_price: int
    spread: int
    profit: int
    profit_margin_pct: Optional[float] = None
    orders_1h: int
    buys_1h: int
    sells_1h: int
    avg_completed_price_1h: Optional[int] = None
    latest_completed_order_at: Optional[datetime] = None

    class Config:
        from_attributes = True
