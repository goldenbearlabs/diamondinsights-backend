from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import Player  
from backend.src.schemas.player import PlayerResponse

router = APIRouter(prefix="/players", tags=["players"])


@router.get("/", response_model=List[PlayerResponse])
def get_players(
    active: Optional[bool] = Query(None, description="Filter by active status"),
    team_id: Optional[int] = Query(None, description="Filter by Team ID"),
    name: Optional[str] = Query(None, description="Search by name (partial match)"),
    limit: int = Query(50, le=1000),
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    Get a list of players with optional filtering | 
    Response Times: <200ms
    """
    query = db.query(Player)

    if active is not None:
        query = query.filter(Player.active == active)

    # should probably join teams
    if team_id:
        query = query.filter(Player.current_team_id == team_id)

    if name:
        query = query.filter(Player.full_name.ilike(f"%{name}%"))

    query = query.order_by(Player.last_name.asc())

    return query.limit(limit).offset(offset).all()


@router.get("/{mlb_id}", response_model=PlayerResponse)
def get_player_by_id(mlb_id: int, db: Session = Depends(get_db)):
    """
    Get a specific player's profile by their MLB ID | 
    Response Time: ~150ms
    """
    player = db.query(Player).filter(Player.mlb_id == mlb_id).first()

    if not player:
        raise HTTPException(status_code=404, detail="Player not found")

    return player