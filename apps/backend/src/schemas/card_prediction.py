from typing import Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime


class CardPredictionResponse(BaseModel):
    card_id: str
    name: str
    team_short_name: Optional[str] = None
    current_ovr: int #card.ovr
    series_name: str
    predicted_ovr: int
    predicted_attributes: Optional[Dict[str, Any]] = None
    created_at: datetime

    

    class Config:
        from_attributes = True