from pydantic import BaseModel
from typing import Optional
from datetime import date

class PlayerResponse(BaseModel):
    mlb_id: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    number: Optional[str] = None
    birth_date: Optional[date] = None
    current_age: Optional[int] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    active: bool
    
    current_team_id: Optional[int] = None
    position_id: Optional[int] = None
    
    draft_year: Optional[int] = None
    mlb_debut_date: Optional[date] = None
    
    bat_side_code: Optional[str] = None
    pitch_hand_code: Optional[str] = None

    class Config:
        from_attributes = True