from typing import Optional

from pydantic import BaseModel, Field


class UserPredictionCreate(BaseModel):
    card_id: str
    predicted_ovr: int = Field(..., ge=0, le=99)


class UserPredictionResponse(BaseModel):
    user_id: int
    card_id: str
    predicted_ovr: int

    class Config:
        from_attributes = True


class LeaderboardEntry(BaseModel):
    rank: int
    user_id: int
    display_name: str
    profile_img_path: str
    prediction_count: int
    score: Optional[float] = None

    class Config:
        from_attributes = True


class PredictionLeaderboardResponse(BaseModel):
    items: list[LeaderboardEntry] = Field(default_factory=list)
    total_participants: int
    my_rank: Optional[int] = None
    my_prediction_count: Optional[int] = None