from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import MLBGameBattingStats, MLBGame
from backend.src.schemas.mlb_game_batting_stat import MLBGameBattingStatResponse

router = APIRouter(prefix="/mlb_game_batting_stats", tags=["mlb_game_batting_stats"])

@router.get("/{player_id}", response_model=List[MLBGameBattingStatResponse])
def get_player_batting_stats(
    player_id: int,
    split: Optional[str] = None,
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)
):
    
    """
    gets the battings stats for a player with {player_id}, joined with Game Data
    ordered by date desc (recent games first) | 
    Response Times ({card_id}): ~200-300ms | ?split={risp, vsrhp, vslhp}: ~100ms

    """

    query = db.query(MLBGameBattingStats, MLBGame).join(MLBGame, MLBGameBattingStats.game_id == MLBGame.id)
    query = query.filter(MLBGameBattingStats.player_id == player_id)
    
    if split:
        query = query.filter(MLBGameBattingStats.split.ilike(split))


    query = query.order_by(MLBGame.game_date.desc())
    results = query.limit(limit).offset(offset).all()

    if not results:
        return []
    
    batting_stats = []

    for batting_stat, game in results:
        batting_stats.append({
            "game_id": batting_stat.game_id,
            "player_id": batting_stat.player_id,
            "split": batting_stat.split,
            "season": game.season,
            "game_date": game.game_date,
            "pa": batting_stat.pa,
            "ab": batting_stat.ab,
            "r": batting_stat.r,
            "h": batting_stat.h,
            "doubles": batting_stat.doubles,
            "triples": batting_stat.triples,
            "hr": batting_stat.hr,
            "rbi": batting_stat.rbi,
            "so": batting_stat.so,
            "bb": batting_stat.bb,
            "hbp": batting_stat.hbp,
            "tb": batting_stat.tb,
            "intentional_walks": batting_stat.intentional_walks,
            "flyOuts": batting_stat.flyOuts,
            "groundOuts": batting_stat.groundOuts,
            "airOuts": batting_stat.airOuts,
            "gidp": batting_stat.gidp,
            "gitp": batting_stat.gitp,
            "lob": batting_stat.lob,
            "sac_bunts": batting_stat.sac_bunts,
            "sac_flies": batting_stat.sac_flies,
            "pop_outs": batting_stat.pop_outs,
            "line_outs": batting_stat.line_outs

        })

    return batting_stats