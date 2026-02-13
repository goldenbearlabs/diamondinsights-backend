from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


RecordMode = Literal["normal", "plus"]


class HomeRunRecordResponse(BaseModel):
    game_id: str
    event_id: Optional[int] = None
    date: Optional[str] = None
    difficulty: Optional[str] = None

    home_profile_username: Optional[str] = None
    away_profile_username: Optional[str] = None
    hitter_username: Optional[str] = None
    pitcher_username: Optional[str] = None

    batter_mlb_id: Optional[int] = None
    pitcher_mlb_id: Optional[int] = None
    hitter_name: Optional[str] = None
    pitcher_name: Optional[str] = None
    ball_park_name: Optional[str] = None

    is_home_batting: Optional[bool] = None
    elevation: Optional[float] = None

    distance_ft: Optional[float] = None
    distance_plus_ft: Optional[float] = None

    rank: Optional[int] = None
    difficulty_rank: Optional[int] = None
    rank_plus: Optional[int] = None
    difficulty_rank_plus: Optional[int] = None

    selected_distance_ft: Optional[float] = None
    filtered_rank: Optional[int] = None
    selected_rank: Optional[int] = None
    selected_difficulty_rank: Optional[int] = None


class HomeRunRecordsResponse(BaseModel):
    items: list[HomeRunRecordResponse] = Field(default_factory=list)
    available_difficulties: list[str] = Field(default_factory=list)
    my_top_hr_ovr_rank: Optional[int] = None
    total: int
    limit: int
    offset: int
    mode: RecordMode


class HardHitRecordResponse(BaseModel):
    game_id: str
    event_id: Optional[int] = None
    date: Optional[str] = None
    difficulty: Optional[str] = None

    home_profile_username: Optional[str] = None
    away_profile_username: Optional[str] = None
    hitter_username: Optional[str] = None
    pitcher_username: Optional[str] = None

    batter_mlb_id: Optional[int] = None
    pitcher_mlb_id: Optional[int] = None
    hitter_name: Optional[str] = None
    pitcher_name: Optional[str] = None
    ball_park_name: Optional[str] = None

    is_home_batting: Optional[bool] = None
    exit_vel_mph: Optional[float] = None

    rank: Optional[int] = None
    difficulty_rank: Optional[int] = None

    selected_exit_vel_mph: Optional[float] = None
    filtered_rank: Optional[int] = None
    selected_rank: Optional[int] = None
    selected_difficulty_rank: Optional[int] = None


class HardHitRecordsResponse(BaseModel):
    items: list[HardHitRecordResponse] = Field(default_factory=list)
    available_difficulties: list[str] = Field(default_factory=list)
    my_top_hit_ovr_rank: Optional[int] = None
    total: int
    limit: int
    offset: int
