from __future__ import annotations

import datetime
from typing import Optional, List, Dict

from pydantic import BaseModel, Field

from shared.db.models import ShowProfile, ShowProfileOnlineStats, ShowGameSummary


class LinkShowBody(BaseModel):
    username: str = Field(min_length=1, max_length=64)


class OnlineStatsOut(BaseModel):
    year: int
    wins: Optional[int] = None
    losses: Optional[int] = None
    hr: Optional[int] = None
    runs_per_game: Optional[float] = None
    stolen_bases: Optional[int] = None
    batting_average: Optional[float] = None
    era: Optional[float] = None
    k_per_9: Optional[float] = None
    whip: Optional[float] = None

    @staticmethod
    def from_orm_row(row: ShowProfileOnlineStats) -> "OnlineStatsOut":
        return OnlineStatsOut(
            year=row.year,
            wins=row.wins,
            losses=row.losses,
            hr=row.hr,
            runs_per_game=row.runs_per_game,
            stolen_bases=row.stolen_bases,
            batting_average=row.batting_average,
            era=row.era,
            k_per_9=row.k_per_9,
            whip=row.whip,
        )


class ShowProfileOut(BaseModel):
    username: str
    display_level: Optional[int] = None
    games_played: Optional[int] = None
    nameplate_equipped: Optional[str] = None
    icon_equipped: Optional[str] = None
    first_seen_at: datetime.datetime
    claimed_at: Optional[datetime.datetime] = None
    last_refreshed_at: datetime.datetime
    online_stats: List[OnlineStatsOut] = Field(default_factory=list)

    @staticmethod
    def from_orm_profile(p: ShowProfile) -> "ShowProfileOut":
        stats = sorted(p.online_stats or [], key=lambda s: s.year)
        return ShowProfileOut(
            username=p.username,
            display_level=p.display_level,
            games_played=p.games_played,
            nameplate_equipped=p.nameplate_equipped,
            icon_equipped=p.icon_equipped,
            first_seen_at=p.first_seen_at,
            claimed_at=p.claimed_at,
            last_refreshed_at=p.last_refreshed_at,
            online_stats=[OnlineStatsOut.from_orm_row(s) for s in stats],
        )


class ShowGameSummaryOut(BaseModel):
    games_played: int
    wins: int
    losses: int
    record: str
    last_game_date: Optional[datetime.datetime] = None
    last_game_difficulty: Optional[str] = None


class ShowGameEventOut(BaseModel):
    game_id: str
    seq: int
    inning: Optional[int] = None
    is_home_batting: Optional[bool] = None
    outs_before: Optional[int] = None
    outs_after: Optional[int] = None
    home_score_before: Optional[int] = None
    away_score_before: Optional[int] = None
    home_score_after: Optional[int] = None
    away_score_after: Optional[int] = None
    pre_on_1b: Optional[bool] = None
    pre_on_2b: Optional[bool] = None
    pre_on_3b: Optional[bool] = None
    post_on_1b: Optional[bool] = None
    post_on_2b: Optional[bool] = None
    post_on_3b: Optional[bool] = None
    event_type: str
    event_text: str
    event_seq_in_half: Optional[int] = None
    parser_version: Optional[str] = None


class ShowHalfInningOut(BaseModel):
    game_id: str
    inning: int
    is_home_batting: bool
    runs: int
    hits: int
    walks: int
    errors: int
    pitches: int
    runners_left_on: int


class ShowPlateAppearanceOut(BaseModel):
    game_id: str
    event_seq: int
    batter_name_raw: str
    pitcher_name_raw: str
    batter_mlb_id: Optional[int] = None
    pitcher_mlb_id: Optional[int] = None
    result: Optional[str] = None
    batted_ball_type: Optional[str] = None
    fielder_pos: Optional[str] = None
    putout_code: Optional[str] = None
    is_out: Optional[bool] = None
    is_double_play: Optional[bool] = None
    is_sac_fly: Optional[bool] = None
    is_sac_bunt: Optional[bool] = None
    runs_scored: Optional[int] = None
    rbi: Optional[int] = None
    hr_distance_ft: Optional[int] = None
    is_perfect_perfect: Optional[bool] = None
    exit_vel_mph: Optional[int] = None
    is_strikeout: Optional[bool] = None
    k_pitch_type: Optional[str] = None
    k_loc_height: Optional[str] = None
    k_loc_width: Optional[str] = None
    k_is_chase: Optional[bool] = None
    k_is_looking: Optional[bool] = None
    k_timing: Optional[str] = None
    batter_side: Optional[str] = None
    pitcher_throws: Optional[str] = None
    hit_direction: Optional[str] = None
    is_error: Optional[bool] = None
    error_pos: Optional[str] = None


class ShowBatterBoxscoreOut(BaseModel):
    game_id: str
    is_home: bool
    appearance_idx: int
    replaced_apperance_idx: Optional[int] = None
    player_name_raw: str
    mlb_id: Optional[int] = None
    ab: int
    h: int
    r: int
    rbi: int
    bb: int
    so: int
    doubles: int
    triples: int
    hr: int
    sh: int
    sf: int
    gidp: int
    e: int
    pb: int
    hbp: int
    sb: int
    cs: int
    innings: int
    pos: int


class ShowPitcherBoxscoreOut(BaseModel):
    game_id: str
    is_home: bool
    appearance_idx: int
    player_name_raw: str
    mlb_id: Optional[int] = None
    ip_raw: str
    outs_pitched: int
    r: int
    h: int
    er: int
    bb: int
    so: int
    era: Optional[float] = None
    wp: int
    win: int
    loss: int
    save: int
    b_save: int
    hold: int
    s_wins: int
    s_losses: int
    s_saves: int
    s_b_saves: int
    s_holds: int


class ShowGameBundleOut(BaseModel):
    events: List[ShowGameEventOut] = Field(default_factory=list)
    half_innings: List[ShowHalfInningOut] = Field(default_factory=list)
    plate_appearances: List[ShowPlateAppearanceOut] = Field(default_factory=list)
    batter_boxscores: List[ShowBatterBoxscoreOut] = Field(default_factory=list)
    pitcher_boxscores: List[ShowPitcherBoxscoreOut] = Field(default_factory=list)


class ShowGameLogItemOut(BaseModel):
    game_id: str
    date: datetime.datetime
    difficulty: Optional[str] = None
    is_online: Optional[bool] = None
    ball_park_name: Optional[str] = None
    home_profile_username: str
    away_profile_username: str
    home_full_name: str
    away_full_name: str
    home_result: str
    away_result: str
    home_runs: int
    away_runs: int
    home_hits: int
    away_hits: int
    home_errors: int
    away_errors: int
    innings: int
    summary: Optional[str] = None

    @staticmethod
    def from_orm_row(row: ShowGameSummary) -> "ShowGameLogItemOut":
        return ShowGameLogItemOut(
            game_id=row.id,
            date=row.date,
            difficulty=row.difficulty,
            is_online=row.is_online,
            ball_park_name=row.ball_park.name if row.ball_park else None,
            home_profile_username=row.home_profile_username,
            away_profile_username=row.away_profile_username,
            home_full_name=row.home_full_name,
            away_full_name=row.away_full_name,
            home_result=row.home_result,
            away_result=row.away_result,
            home_runs=row.home_runs,
            away_runs=row.away_runs,
            home_hits=row.home_hits,
            away_hits=row.away_hits,
            home_errors=row.home_errors,
            away_errors=row.away_errors,
            innings=row.innings,
            summary=row.summary,
        )


class PlateAppearanceStatsOut(BaseModel):
    plate_appearances: int
    hits: int
    walks: int
    strikeouts: int
    avg: Optional[float] = None
    obp: Optional[float] = None
    slg: Optional[float] = None
    ops: Optional[float] = None
    kbb: Optional[float] = None


class ShowSkillsOut(BaseModel):
    hitting: PlateAppearanceStatsOut
    pitching: PlateAppearanceStatsOut


class ShowAggregateStatsOut(BaseModel):
    pa: int
    ab: int
    r: int
    h: int
    rbi: int
    singles: int
    doubles: int
    triples: int
    hr: int
    bb: int
    so: int
    avg: float
    obp: float
    slg: float
    ops: float
    lob: int
    gidp: int
    gidp_pct: Optional[float] = None
    woba: float
    iso: float
    babip: float
    k_pct: float
    bb_pct: float
    hr_pct: float
    xbh_pct: float
    rs_pct: float


class ShowCardStatsOut(BaseModel):
    mlb_id: int
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    pa: int
    ab: int
    r: int
    h: int
    rbi: int
    singles: int
    doubles: int
    triples: int
    hr: int
    bb: int
    so: int
    avg: float
    obp: float
    slg: float
    ops: float
    lob: int
    gidp: int
    gidp_pct: Optional[float] = None
    woba: float
    iso: float
    babip: float
    k_pct: float
    bb_pct: float
    hr_pct: float
    xbh_pct: float
    rs_pct: float
    chase_pct: float
    freeze_pct: float
    timing_pct: float
    timing_k_pct: float
    eye_k_pct: float
    location_k_pct: float
    sweet_spot_pct: float
    popup_rate: float
    flyball_rate: float
    gb_air_ratio: float
    pulled_air_rate: float
    oppo_air_rate: float
    perfect_perfect_pct: float


class ShowCardPitchingStatsOut(BaseModel):
    mlb_id: int
    full_name: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    pa: int
    outs_pitched: int
    h: int
    r: int
    hr: int
    bb: int
    so: int
    hbp: int
    avg: float
    obp: float
    slg: float
    ops: float
    woba: float
    babip: float
    k_pct: float
    bb_pct: float
    hr_pct: float
    xbh_pct: float
    chase_pct: float
    freeze_pct: float
    timing_pct: float
    timing_k_pct: float
    eye_k_pct: float
    location_k_pct: float
    sweet_spot_pct: float
    popup_rate: float
    flyball_rate: float
    gb_air_ratio: float
    pulled_air_rate: float
    oppo_air_rate: float
    perfect_perfect_pct: float
    era: Optional[float] = None
    whip: Optional[float] = None
    kbb: Optional[float] = None


class ShowProfileSearchOut(BaseModel):
    user_id: Optional[int] = None
    username: str
    display_name: Optional[str] = None
    profile_img_url: Optional[str] = None


class ShowPitcherSearchOut(BaseModel):
    mlb_id: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    pitch_hand_code: Optional[str] = None


class ShowHitterSearchOut(BaseModel):
    mlb_id: int
    full_name: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    bat_side_code: Optional[str] = None


class PowerSkillOut(BaseModel):
    power: int
    pa: int
    smoothed_rate: float
    sample_mean_rate: float
    sample_sd: float
    diff_score: float
    elev_robust: float
    card_score: float


class TimingSkillOut(BaseModel):
    timing: int
    location: int
    pa: int
    timing_z: float
    k_pen: float
    pp_bonus: float


class BattingArchetypeOut(BaseModel):
    overall: int
    power: int
    timing: int
    location: int
    pa: int


class PitchingArchetypeOut(BaseModel):
    overall: int
    consistency: int
    strikeout: int
    location: int
    pa: int


class CombinedArchetypeOut(BaseModel):
    batting: BattingArchetypeOut
    pitching: PitchingArchetypeOut


class StrikeoutZoneMapOut(BaseModel):
    zones: List[List[int]]
    outside: Dict[str, int]
    total: int
    pa: int
    pitch_type_options: List[str]
    stats: Dict[str, float]
    stats_by_zone: List[List[Dict[str, float]]]
    stats_by_outside: Dict[str, Dict[str, float]]
    counts_by_zone: List[List[Dict[str, int]]]
    counts_by_outside: Dict[str, Dict[str, int]]


class HitDataMapOut(BaseModel):
    zones: Dict[str, float]
    total: int
    pa: int
    stat: str
    stats: Dict[str, float]


class ShowYourOvrWeightOut(BaseModel):
    mlb_id: int
    role: str
    weight: float
    pa: int = 0
    meets_min_pa: bool = False


class ShowYourOvrWeightsOut(BaseModel):
    username: str
    updated_at: Optional[str] = None
    total_weights: int
    hitting_weights: int
    pitching_weights: int
    weights: List[ShowYourOvrWeightOut] = Field(default_factory=list)
