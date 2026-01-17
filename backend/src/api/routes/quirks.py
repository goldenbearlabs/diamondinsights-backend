from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import Quirk
from backend.src.schemas.quirk import QuirkResponse

router = APIRouter(prefix="/quirks", tags=["quirks"])


@router.get("/", response_model=List[QuirkResponse])
def get_quirks(
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    "gets all quirks"

    query = db.query(Quirk)

    results = query.limit(limit).offset(offset).all()

    return results
