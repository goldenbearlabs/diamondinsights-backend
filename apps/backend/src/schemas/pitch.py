from pydantic import BaseModel
from typing import Optional, Dict

class PitchResponse(BaseModel):
    card_id: str
    name: str
    speed: int
    control: int
    movement: int
    
    class Config:
        from_attributes = True