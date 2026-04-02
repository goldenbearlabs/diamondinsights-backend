import datetime
from typing import Optional
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from shared.core.config import CURRENT_MLB_SEASON
from shared.db.database import get_db
from shared.db.models import Card, MLBGame, MLBGameBattingStats, MLBGamePitchingStats, RosterUpdate
from src.schemas.mlb_season_stats import (
    SeasonStatsResponse,
    BattingSeasonStats,
    PitchingSeasonStats,
    BattingSplitStats,
    PitchingSplitStats,
)

router = APIRouter(prefix="/mlb_stats", tags=["mlb_stats"])

BATTING_HAND_SPLITS = ("vslhp", "vsrhp")
PITCHING_HAND_SPLITS = ("vslhb", "vsrhb")
TWO_WAY_PLAYERS = {"Shohei Ohtani"}


def _compute_batting_rates(stats: dict) -> dict:
    ab = stats.get("ab", 0)
    h = stats.get("h", 0)
    bb = stats.get("bb", 0)
    hbp = stats.get("hbp", 0)
    sf = stats.get("sac_flies", 0)
    tb = stats.get("tb", 0)
    pa = stats.get("pa", 0)

    avg = round(h / ab, 3) if ab > 0 else 0.0
    obp = round((h + bb + hbp) / (ab + bb + hbp + sf), 3) if (ab + bb + hbp + sf) > 0 else 0.0
    slg = round(tb / ab, 3) if ab > 0 else 0.0
    ops = round(obp + slg, 3)

    stats["avg"] = avg
    stats["obp"] = obp
    stats["slg"] = slg
    stats["ops"] = ops
    return stats


def _compute_pitching_rates(stats: dict, outs_pitched: int) -> dict:
    """Compute display IP and rate stats (ERA, WHIP, K/9) from raw outs_pitched."""
    h = stats.get("h", 0)
    er = stats.get("er", 0)
    bb = stats.get("bb", 0)
    k = stats.get("k", 0)

    true_ip = outs_pitched / 3.0
    ip_display = (outs_pitched // 3) + (outs_pitched % 3) * 0.1

    stats["ip"] = round(ip_display, 1)
    stats["era"] = round((er * 9) / true_ip, 2) if true_ip > 0 else 0.0
    stats["whip"] = round((bb + h) / true_ip, 2) if true_ip > 0 else 0.0
    stats["k9"] = round((k * 9) / true_ip, 2) if true_ip > 0 else 0.0
    return stats




def _parse_cutoff(window: Optional[str]) -> Optional[datetime.datetime]:
    """Return a timezone-naive datetime cutoff for fixed-day windows ('7d', '14d')."""
    if not window:
        return None
    window_days: dict[str, int] = {"7d": 7, "14d": 14}
    days = window_days.get(window)
    if days is None:
        return None
    cutoff_date = datetime.date.today() - datetime.timedelta(days=days)
    return datetime.datetime.combine(cutoff_date, datetime.time.min)


def _get_last_major_update_cutoff(db: Session) -> Optional[datetime.datetime]:
    """Return a timezone-naive datetime for the date of the most recent major roster update."""
    result = (
        db.query(func.max(RosterUpdate.date))
        .filter(RosterUpdate.is_major == True)  # noqa: E712
        .scalar()
    )
    if result is None:
        return None
    return datetime.datetime.combine(result, datetime.time.min)


@router.get("/season/{card_id}", response_model=SeasonStatsResponse)
def get_season_stats(
    card_id: str,
    season: int = Query(CURRENT_MLB_SEASON),
    window: Optional[str] = Query(None, description="Time window filter: '7d', '14d', or 'last_update'. Omit for full season."),
    db: Session = Depends(get_db),
):
    """Get aggregated season stats for a card, with per-split and overall totals."""

    card = db.query(Card).filter(Card.id == card_id).first()

    if not card or not card.mlb_id:
        return SeasonStatsResponse(
            is_hitter=card.is_hitter if card else True,
            season=season,
        )

    mlb_id = card.mlb_id
    is_two_way = card.name in TWO_WAY_PLAYERS
    if window == "last_update":
        cutoff = _get_last_major_update_cutoff(db)
    else:
        cutoff = _parse_cutoff(window)

    if is_two_way:
        batting = _aggregate_batting(db, mlb_id, season, cutoff)
        pitching = _aggregate_pitching(db, mlb_id, season, cutoff)
        return SeasonStatsResponse(is_hitter=True, season=season, batting=batting, pitching=pitching)
    elif card.is_hitter:
        batting = _aggregate_batting(db, mlb_id, season, cutoff)
        return SeasonStatsResponse(is_hitter=True, season=season, batting=batting)
    else:
        pitching = _aggregate_pitching(db, mlb_id, season, cutoff)
        return SeasonStatsResponse(is_hitter=False, season=season, pitching=pitching)


def _aggregate_batting(
    db: Session,
    mlb_id: int,
    season: int,
    cutoff: Optional[datetime.datetime] = None,
) -> BattingSeasonStats | None:
    filters = [
        MLBGameBattingStats.player_id == mlb_id,
        MLBGame.season == season,
        MLBGame.game_type == "R",
    ]
    if cutoff is not None:
        filters.append(MLBGame.game_date >= cutoff)

    rows = (
        db.query(
            MLBGameBattingStats.split,
            func.sum(MLBGameBattingStats.pa).label("pa"),
            func.sum(MLBGameBattingStats.ab).label("ab"),
            func.sum(MLBGameBattingStats.r).label("r"),
            func.sum(MLBGameBattingStats.h).label("h"),
            func.sum(MLBGameBattingStats.doubles).label("doubles"),
            func.sum(MLBGameBattingStats.triples).label("triples"),
            func.sum(MLBGameBattingStats.hr).label("hr"),
            func.sum(MLBGameBattingStats.rbi).label("rbi"),
            func.sum(MLBGameBattingStats.bb).label("bb"),
            func.sum(MLBGameBattingStats.so).label("so"),
            func.sum(MLBGameBattingStats.hbp).label("hbp"),
            func.sum(MLBGameBattingStats.tb).label("tb"),
            func.sum(MLBGameBattingStats.sac_flies).label("sac_flies"),
        )
        .join(MLBGame, MLBGameBattingStats.game_id == MLBGame.id)
        .filter(*filters)
        .group_by(MLBGameBattingStats.split)
        .all()
    )

    if not rows:
        return None

    split_map: dict[str, dict] = {}
    for row in rows:
        stats = {
            "split": row.split,
            "pa": row.pa or 0,
            "ab": row.ab or 0,
            "r": row.r or 0,
            "h": row.h or 0,
            "doubles": row.doubles or 0,
            "triples": row.triples or 0,
            "hr": row.hr or 0,
            "rbi": row.rbi or 0,
            "bb": row.bb or 0,
            "so": row.so or 0,
            "hbp": row.hbp or 0,
            "tb": row.tb or 0,
            "sac_flies": row.sac_flies or 0,
        }
        stats = _compute_batting_rates(stats)
        split_map[row.split] = stats

    # Overall = sum of hand-based splits only (exclude risp to avoid double-counting)
    overall_counts = {
        "split": "overall",
        "pa": 0, "ab": 0, "r": 0, "h": 0, "doubles": 0, "triples": 0,
        "hr": 0, "rbi": 0, "bb": 0, "so": 0, "hbp": 0, "tb": 0, "sac_flies": 0,
    }
    for s in BATTING_HAND_SPLITS:
        if s in split_map:
            for key in overall_counts:
                if key == "split":
                    continue
                overall_counts[key] += split_map[s][key]

    overall_counts = _compute_batting_rates(overall_counts)

    splits = [BattingSplitStats(**v) for k, v in split_map.items()]
    overall = BattingSplitStats(**overall_counts)

    return BattingSeasonStats(overall=overall, splits=splits)


def _aggregate_pitching(
    db: Session,
    mlb_id: int,
    season: int,
    cutoff: Optional[datetime.datetime] = None,
) -> PitchingSeasonStats | None:
    filters = [
        MLBGamePitchingStats.player_id == mlb_id,
        MLBGame.season == season,
        MLBGame.game_type == "R",
    ]
    if cutoff is not None:
        filters.append(MLBGame.game_date >= cutoff)

    rows = (
        db.query(
            MLBGamePitchingStats.split,
            func.sum(MLBGamePitchingStats.outs_pitched).label("outs_pitched"),
            func.sum(MLBGamePitchingStats.h).label("h"),
            func.sum(MLBGamePitchingStats.r).label("r"),
            func.sum(MLBGamePitchingStats.er).label("er"),
            func.sum(MLBGamePitchingStats.hr).label("hr"),
            func.sum(MLBGamePitchingStats.bb).label("bb"),
            func.sum(MLBGamePitchingStats.k).label("k"),
            func.sum(MLBGamePitchingStats.batters_faced).label("batters_faced"),
            func.sum(MLBGamePitchingStats.strikes_thrown).label("strikes_thrown"),
            func.sum(MLBGamePitchingStats.balls_thrown).label("balls_thrown"),
        )
        .join(MLBGame, MLBGamePitchingStats.game_id == MLBGame.id)
        .filter(*filters)
        .group_by(MLBGamePitchingStats.split)
        .all()
    )

    if not rows:
        return None

    split_map: dict[str, dict] = {}
    for row in rows:
        outs = row.outs_pitched or 0

        strikes = row.strikes_thrown or 0
        balls = row.balls_thrown or 0
        stats = {
            "split": row.split,
            "ip": 0.0,
            "h": row.h or 0,
            "r": row.r or 0,
            "er": row.er or 0,
            "hr": row.hr or 0,
            "bb": row.bb or 0,
            "k": row.k or 0,
            "batters_faced": row.batters_faced or 0,
            "strike_pct": round(strikes / (strikes + balls), 3) if (strikes + balls) > 0 else 0.0,
            "_outs": outs,
            "_strikes": strikes,
            "_balls": balls,
        }
        stats = _compute_pitching_rates(stats, outs)
        split_map[row.split] = stats

    # Overall = sum of hand-based splits only
    overall_outs = 0
    overall_counts: dict = {
        "split": "overall",
        "ip": 0.0, "h": 0, "r": 0, "er": 0, "hr": 0, "bb": 0, "k": 0,
        "batters_faced": 0, "strike_pct": 0.0, "_strikes": 0, "_balls": 0,
    }
    for s in PITCHING_HAND_SPLITS:
        if s in split_map:
            for key in overall_counts:
                if key in ("split", "ip", "strike_pct"):
                    continue
                overall_counts[key] += split_map[s][key]
            overall_outs += split_map[s]["_outs"]

    o_strikes = overall_counts["_strikes"]
    o_balls = overall_counts["_balls"]
    overall_counts["strike_pct"] = round(o_strikes / (o_strikes + o_balls), 3) if (o_strikes + o_balls) > 0 else 0.0
    overall_counts = _compute_pitching_rates(overall_counts, overall_outs)

    splits = [PitchingSplitStats(**{k: v for k, v in s.items() if not k.startswith("_")}) for s in split_map.values()]
    overall = PitchingSplitStats(**{k: v for k, v in overall_counts.items() if not k.startswith("_")})

    return PitchingSeasonStats(overall=overall, splits=splits)
