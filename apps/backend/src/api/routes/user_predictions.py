from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Users, UserPrediction
from src.api.routes.users import firebase_claims
from src.schemas.user_prediction import UserPredictionCreate, UserPredictionResponse

router = APIRouter(prefix="/user-predictions", tags=["user_predictions"])

def get_current_user(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims)
) -> Users:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user

@router.post("/", response_model=UserPredictionResponse)
def upsert_prediction(
    body: UserPredictionCreate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    existing_pred = db.scalar(
        select(UserPrediction).where(
            UserPrediction.user_id == user.id,
            UserPrediction.card_id == body.card_id
        )
    )

    if existing_pred:
        existing_pred.predicted_ovr = body.predicted_ovr
    else:
        existing_pred = UserPrediction(
            user_id=user.id,
            card_id=body.card_id,
            predicted_ovr=body.predicted_ovr
        )
        db.add(existing_pred)
    
    db.commit()
    db.refresh(existing_pred)
    return existing_pred

@router.get("/{card_id}", response_model=UserPredictionResponse)
def get_prediction(
    card_id: str,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    pred = db.scalar(
        select(UserPrediction).where(
            UserPrediction.user_id == user.id,
            UserPrediction.card_id == card_id
        )
    )
    
    if not pred:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Prediction not found")
        
    return pred