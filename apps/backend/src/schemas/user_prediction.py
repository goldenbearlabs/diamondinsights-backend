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