from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from shared.db.database import get_db
from shared.db.models import Users, UserPrediction
from src.api.routes.users import firebase_claims, firebase_claims_optional
from src.schemas.user_prediction import (
    LeaderboardEntry,
    PredictionLeaderboardResponse,
    UserPredictionCreate,
    UserPredictionResponse,
)
from redis import Redis
from src.core.cache import get_cache_client

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


@router.get("/leaderboard", response_model=PredictionLeaderboardResponse)
def get_prediction_leaderboard(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
):
    # Subquery: count predictions per user, ordered by count desc
    count_sq = (
        select(
            UserPrediction.user_id,
            func.count().label("prediction_count"),
        )
        .group_by(UserPrediction.user_id)
        .subquery()
    )

    # Ranked query joining with users table
    rows = db.execute(
        select(
            Users.id,
            Users.display_name,
            Users.profile_img_url,
            count_sq.c.prediction_count,
        )
        .join(count_sq, Users.id == count_sq.c.user_id)
        .order_by(count_sq.c.prediction_count.desc(), Users.display_name.asc())
        .limit(50)
    ).all()

    total_participants = db.scalar(
        select(func.count(func.distinct(UserPrediction.user_id)))
    ) or 0

    items = [
        LeaderboardEntry(
            rank=idx + 1,
            user_id=row.id,
            display_name=row.display_name,
            profile_img_path=row.profile_img_url,
            prediction_count=row.prediction_count,
            score=None,
        )
        for idx, row in enumerate(rows)
    ]

    # Resolve caller's rank if authenticated
    my_rank = None
    my_prediction_count = None
    uid = claims.get("uid")
    if uid:
        caller = db.scalar(select(Users).where(Users.firebase_id == uid))
        if caller:
            caller_count = db.scalar(
                select(func.count()).where(UserPrediction.user_id == caller.id)
            ) or 0
            if caller_count > 0:
                my_prediction_count = caller_count
                # Rank = number of users with strictly more predictions + 1
                higher_count = db.scalar(
                    select(func.count()).select_from(
                        select(UserPrediction.user_id)
                        .group_by(UserPrediction.user_id)
                        .having(func.count() > caller_count)
                        .subquery()
                    )
                ) or 0
                my_rank = higher_count + 1

    return PredictionLeaderboardResponse(
        items=items,
        total_participants=total_participants,
        my_rank=my_rank,
        my_prediction_count=my_prediction_count,
    )


@router.post("/", response_model=UserPredictionResponse)
def upsert_prediction(
    body: UserPredictionCreate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
    cache: Redis | None = Depends(get_cache_client),
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
    """
    if cache:
        # Search Redis for any card ranking queries tied specifically to this user's ID
        pattern = f"*:cards:rankings:{user.firebase_id}:*"
        
        # Safely iterate through the matching keys and delete them
        for key in cache.scan_iter(pattern):
            cache.delete(key)
    """
    if cache:
        # 1. Broaden the wildcard pattern significantly. 
        # This says: "Find any key that contains 'cards' AND the user's firebase ID anywhere inside it"
        pattern = f"*cards*{user.firebase_id}*"
        
        # 2. Gather all matching keys into a list first (safer than deleting while iterating)
        keys_to_delete = list(cache.scan_iter(match=pattern))
        
        # 3. Batch delete and log the result to the terminal!
        if keys_to_delete:
            print(f"SUCCESS: Destroying {len(keys_to_delete)} stale card caches for {user.firebase_id}")
            cache.delete(*keys_to_delete) # Unpacks the list and deletes them all in one atomic command
        else:
            print(f"WARNING: Cache Miss! No keys found for pattern: {pattern}")
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