from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


# ── Request schemas ─────────────────────────────────────────────────────────────

class HoldingCreate(BaseModel):
    card_id: str
    quantity: int = Field(..., ge=1)
    avg_price: int = Field(..., ge=0)
    user_predicted_ovr: int = Field(..., ge=0, le=99)


class HoldingUpdate(BaseModel):
    quantity: Optional[int] = Field(None, ge=1)
    avg_price: Optional[int] = Field(None, ge=0)
    user_predicted_ovr: Optional[int] = Field(None, ge=0, le=99)


class PortfolioPrivacyUpdate(BaseModel):
    is_public: bool


# ── Response schemas ────────────────────────────────────────────────────────────

class HoldingCardInfo(BaseModel):
    id: str
    name: str
    team_short_name: str
    ovr: int
    baked_img: str
    display_position: str
    rarity: str
    predicted_ovr: Optional[int] = None
    is_hitter: bool
    
    # Pitching attributes
    stamina: Optional[int] = None
    pitching_clutch: Optional[int] = None
    hits_per_bf: Optional[int] = None
    k_per_bf: Optional[int] = None
    bb_per_bf: Optional[int] = None
    hr_per_bf: Optional[int] = None
    
    # Batting attributes
    contact_left: Optional[int] = None
    contact_right: Optional[int] = None
    power_left: Optional[int] = None
    power_right: Optional[int] = None
    plate_vision: Optional[int] = None
    plate_discipline: Optional[int] = None
    batting_clutch: Optional[int] = None
    bunting_ability: Optional[int] = None
    
    # Baserunning attributes
    baserunning_ability: Optional[int] = None
    speed: Optional[int] = None
    
    # Fielding attributes
    arm_strength: Optional[int] = None
    arm_accuracy: Optional[int] = None
    reaction_time: Optional[int] = None
    blocking: Optional[int] = None
    fielding_ability: Optional[int] = None
    
    # Predicted attributes
    predicted_attributes: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True


class HoldingResponse(BaseModel):
    card_id: str
    quantity: int
    avg_price: Optional[int] = None
    user_predicted_ovr: Optional[int] = None
    card: HoldingCardInfo

    class Config:
        from_attributes = True


class PortfolioResponse(BaseModel):
    id: int
    name: str
    is_public: bool
    holdings: List[HoldingResponse] = []

    class Config:
        from_attributes = True
