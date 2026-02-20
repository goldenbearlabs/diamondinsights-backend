from pydantic import BaseModel
from typing import Optional, List


class BattingSplitStats(BaseModel):
    split: str
    pa: int = 0
    ab: int = 0
    r: int = 0
    h: int = 0
    doubles: int = 0
    triples: int = 0
    hr: int = 0
    rbi: int = 0
    bb: int = 0
    so: int = 0
    hbp: int = 0
    tb: int = 0
    sac_flies: int = 0
    avg: float = 0.0
    obp: float = 0.0
    slg: float = 0.0
    ops: float = 0.0

    class Config:
        from_attributes = True


class PitchingSplitStats(BaseModel):
    split: str
    ip: float = 0.0
    h: int = 0
    r: int = 0
    er: int = 0
    hr: int = 0
    bb: int = 0
    k: int = 0
    batters_faced: int = 0
    strike_pct: float = 0.0
    era: float = 0.0
    whip: float = 0.0
    k9: float = 0.0

    class Config:
        from_attributes = True


class BattingSeasonStats(BaseModel):
    overall: BattingSplitStats
    splits: List[BattingSplitStats]


class PitchingSeasonStats(BaseModel):
    overall: PitchingSplitStats
    splits: List[PitchingSplitStats]


class SeasonStatsResponse(BaseModel):
    is_hitter: bool
    season: int
    batting: Optional[BattingSeasonStats] = None
    pitching: Optional[PitchingSeasonStats] = None
