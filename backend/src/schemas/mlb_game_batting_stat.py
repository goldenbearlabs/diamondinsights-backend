from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class MLBGameBattingStatResponse(BaseModel):
    game_id: int
    player_id: int
    split: Optional[str] = None  
    
    season: Optional[int] = None
    game_date: Optional[datetime] = None
    pa: Optional[int] = 0
    ab: Optional[int] = 0
    r: Optional[int] = 0
    h: Optional[int] = 0
    doubles: Optional[int] = 0
    triples: Optional[int] = 0
    hr: Optional[int] = 0
    rbi: Optional[int] = 0
    so: Optional[int] = 0
    bb: Optional[int] = 0
    hbp: Optional[int] = 0
    tb: Optional[int] = 0
    intentional_walks: Optional[int] = 0
    
    flyOuts: Optional[int] = 0
    groundOuts: Optional[int] = 0
    airOuts: Optional[int] = 0
    gidp: Optional[int] = 0
    gitp: Optional[int] = 0
    lob: Optional[int] = 0
    sac_bunts: Optional[int] = 0
    sac_flies: Optional[int] = 0
    pop_outs: Optional[int] = 0
    line_outs: Optional[int] = 0

    class Config:
        from_attributes = True