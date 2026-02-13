from pydantic import BaseModel
from typing import Dict, Optional
from src.schemas.quirk import QuirkResponse

# This class defines the JSON structure that gets returned
class CardResponse(BaseModel):
    id: str
    name: str
    team_short_name: str
    ovr: int
    rarity: str
    display_position: str
    display_primary_position: Optional[str] = None
    display_secondary_positions: Optional[str] = None
    display_seconday_position: Optional[str] = None
    true_overall: Optional[float] = None
    true_overall_rounded: Optional[int] = None
    meta_overall: Optional[float] = None
    meta_overall_rounded: Optional[int] = None
    true_overall_by_position: Optional[Dict[str, int]] = None
    meta_overall_by_position: Optional[Dict[str, int]] = None
    jersey_number: int
    age: int
    bat_hand: str
    throw_hand: str
    weight: str
    height: str
    born: str
    is_hitter: bool
    series_name: str
    baked_img: str
    img: str
    hit_rank_image: Optional[str] = None
    fielding_rank_image: Optional[str] = None
    is_sellable: Optional[bool] = None
    has_augment: Optional[bool]= None # ???
    has_matchup: bool
    new_rank: int
    is_live_set: bool
    mlb_id: Optional[int] = None
    year: int

    # pitching attributes
    stamina: int
    pitching_clutch: int
    hits_per_bf: int
    k_per_bf: int
    bb_per_bf: int
    hr_per_bf: int
    pitch_velocity: int
    pitch_control: int
    pitch_movement: int

    #hitting attributes
    contact_left: int
    contact_right: int
    power_left:int
    power_right: int
    plate_vision: int
    plate_discipline: int
    batting_clutch: int
    bunting_ability: int
    drag_bunting_ability: int
    hitting_durability: int

    #fielding
    fielding_durability: int
    fielding_ability: int
    arm_strength: int
    arm_accuracy: int
    reaction_time: int
    blocking: int
    speed: int
    baserunning_ability: int
    baserunning_aggression: int
    quirks: Optional[list[QuirkResponse]] = None

    

    class Config:
        from_attributes = True
