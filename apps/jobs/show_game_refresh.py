
import os
import random
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Iterable, Optional, Set, Tuple, List, Dict, Any, Mapping
from dataclasses import dataclass, field

from sqlalchemy import func, or_, select, exists, and_, delete, case
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from shared.db.models import (
    Card,
    MLBPosition,
    Player,
    ShowBallParks,
    ShowGameEvent,
    ShowGameHalfInning,
    ShowGamePlateAppearance,
    ShowProfile,
    ShowGameSummary,
    ShowGamePitcherGameScore,
    ShowGameSubstitution,
    ShowEventRunnerMove,
    ShowEventCardCandidate,
    ShowGamePitchingChange,
    ShowPitcherBoxscore,
    ShowBatterBoxscore
)
from shared.core.config import CURRENT_SHOW_YEAR
from shared.core.game_log_text_regex import GameLogTextRegexHandler

GAME_HISTORY_URL = os.getenv("GAME_HISTORY_URL", "https://mlb25.theshow.com/apis/game_history.json")
GAME_LOG_URL = os.getenv("GAME_LOG_URL", "https://mlb25.theshow.com/apis/game_log.json")
MLB_THE_SHOW_YEAR = CURRENT_SHOW_YEAR

_PITCH_TYPES = {
    "4-seam fastball",
    "2-seam fastball",
    "fastball",
    "sinker",
    "cutter",
    "slider",
    "changeup",
    "curveball",
    "splitter",
    "forkball",
    "circle change",
    "vulcan change",
    "vulcan chaneup",
    "palmball",
    "sweeper",
    "sweeping curve",
    "knuckle curve",
    "slurve",
    "screwball",
    "knuckleball",
}



def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _to_dt(s: Any, fmt: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(str(s), fmt)
    except ValueError:
        return None


@dataclass
class GameHistory:
    id: str
    home_full_name: str
    away_full_name: str
    home_display_result: str
    away_display_result: str
    home_runs: int
    away_runs: int
    home_hits: int
    away_hits: int
    home_errors: int
    away_errors: int
    summary: str
    home_name: str
    away_name: str
    date: Optional[datetime]

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> 'GameHistory':
        return cls(
            id=str(data.get("id", "")),
            home_full_name=str(data.get("home_full_name", "")),
            away_full_name=str(data.get("away_full_name", "")),
            home_display_result=str(data.get("home_display_result", "")),
            away_display_result=str(data.get("away_display_result", "")),
            home_runs=_to_int(data.get("home_runs")),
            away_runs=_to_int(data.get("away_runs")),
            home_hits=_to_int(data.get("home_hits")),
            away_hits=_to_int(data.get("away_hits")),
            home_errors=_to_int(data.get("home_errors")),
            away_errors=_to_int(data.get("away_errors")),
            summary=str(data.get("display_pitcher_info", "")),
            home_name=str(data.get("home_name", "")),
            away_name=str(data.get("away_name", "")),
            date=_to_dt(data.get("display_date"), "%m/%d/%Y %H:%M:%S"),
        )


@dataclass
class BattingBoxscore:
    player_name: str
    ab: int
    r: int
    h: int
    rbi: int
    bb: int
    so: int
    avg: float
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
    innings: float
    sub_inn: float
    p_inx: int
    replaced: int
    pos: int
    sub_inx: int
    sub_type: int

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> 'BattingBoxscore':
        return cls(
            player_name=str(data.get("player_name", "")),
            ab=_to_int(data.get("ab")),
            r=_to_int(data.get("r")),
            h=_to_int(data.get("h")),
            rbi=_to_int(data.get("rbi")),
            bb=_to_int(data.get("bb")),
            so=_to_int(data.get("so")),
            avg=_to_float(data.get("avg")),
            doubles=_to_int(data.get("doubles")),
            triples=_to_int(data.get("triples")),
            hr=_to_int(data.get("hr")),
            sh=_to_int(data.get("sh")),
            sf=_to_int(data.get("sf")),
            gidp=_to_int(data.get("gidp")),
            e=_to_int(data.get("e")),
            pb=_to_int(data.get("pb")),
            hbp=_to_int(data.get("hbp")),
            sb=_to_int(data.get("sb")),
            cs=_to_int(data.get("cs")),
            innings=_to_float(data.get("innings")),
            sub_inn=_to_float(data.get("sub_inn")),
            p_inx=_to_int(data.get("p_inx")),
            replaced=_to_int(data.get("replaced")),
            pos=_to_int(data.get("pos")),
            sub_inx=_to_int(data.get("sub_inx")),
            sub_type=_to_int(data.get("sub_type")),
        )


@dataclass
class PitchingBoxscore:
    player_name: str
    ip: float
    r: int
    h: int
    er: int
    bb: int
    so: int
    era: float
    p_idx: int
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

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> 'PitchingBoxscore':
        return cls(
            player_name=str(data.get("player_name", "")),
            ip=_to_float(data.get("ip")),
            r=_to_int(data.get("r")),
            h=_to_int(data.get("h")),
            er=_to_int(data.get("er")),
            bb=_to_int(data.get("bb")),
            so=_to_int(data.get("so")),
            era=_to_float(data.get("era")),
            p_idx=_to_int(data.get("p_idx")),
            wp=_to_int(data.get("wp")),
            win=_to_int(data.get("win")),
            loss=_to_int(data.get("loss")),
            save=_to_int(data.get("save")),
            b_save=_to_int(data.get("b_save")),
            hold=_to_int(data.get("hold")),
            s_wins=_to_int(data.get("s_wins")),
            s_losses=_to_int(data.get("s_losses")),
            s_saves=_to_int(data.get("s_saves")),
            s_b_saves=_to_int(data.get("s_b_saves")),
            s_holds=_to_int(data.get("s_holds")),
        )


@dataclass
class GameLog:
    id: str
    innings: str
    game_log_string: str
    home_batting_boxscores: List[BattingBoxscore] = field(default_factory=list)
    away_batting_boxscores: List[BattingBoxscore] = field(default_factory=list)
    home_pitching_boxscores: List[PitchingBoxscore] = field(default_factory=list)
    away_pitching_boxscores: List[PitchingBoxscore] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: Mapping[str, Any], game_id: str) -> 'GameLog':
        game = data.get("game") or []
        line_score = (game[0][1] if len(game) > 0 else {}) or {}
        game_log_string = (game[1][1] if len(game) > 1 else "") or ""
        teams_block = (game[2][1] if len(game) > 2 else []) or []

        def _team_stats(team_obj: Mapping[str, Any]) -> Mapping[str, Any]:
            team_id = team_obj.get("team_id")
            return (
                team_obj.get(str(team_id))
                or team_obj.get(team_id)
                or {}
            )

        home_obj = teams_block[0] if len(teams_block) > 0 else {}
        away_obj = teams_block[1] if len(teams_block) > 1 else {}

        home_stats = _team_stats(home_obj)
        away_stats = _team_stats(away_obj)

        home_bat = [BattingBoxscore.from_json(x) for x in (home_stats.get("batting_stats") or [])]
        home_pit = [PitchingBoxscore.from_json(x) for x in (home_stats.get("pitching_stats") or [])]
        away_bat = [BattingBoxscore.from_json(x) for x in (away_stats.get("batting_stats") or [])]
        away_pit = [PitchingBoxscore.from_json(x) for x in (away_stats.get("pitching_stats") or [])]

        return cls(
            id=game_id,
            innings=str(line_score.get("innings", "")),
            game_log_string=str(game_log_string),
            home_batting_boxscores=home_bat,
            away_batting_boxscores=away_bat,
            home_pitching_boxscores=home_pit,
            away_pitching_boxscores=away_pit,
        )

class ShowGameRefresh(Job):

    def __init__(self):
        super().__init__()

    def run(self, db_session: Session):
        usernames = [u for u in db_session.scalars(select(ShowProfile.username)) if u]
        self._log_start(total_usernames=len(usernames))
        self.logger.info("show game refresh start usernames=%s", len(usernames))
        errors = 0
        
        for username in usernames:
            username = (username or "").strip()

            # DEBUG ME
            if username != "wizzy47911779":
                self.logger.debug("show game refresh skip username=%s reason=debug_filter", username)
                continue

            if not username:
                self.logger.debug("show game refresh skip username=%s reason=blank", username)
                continue

            self.logger.info("show game refresh processing username=%s", username)
            unprocessed_games = self._fetch_unprocessed_games(db_session, username)

            self.logger.info(
                "show game refresh username=%s games_to_process=%s",
                username,
                len(unprocessed_games),
            )

            for game in unprocessed_games:

                self.logger.info("show game refresh processing game_id=%s username=%s", game.id, username)
                self._process_game(db_session, username, game)

        try:
            db_session.commit()
            self.logger.info("show game refresh commit succeeded")
        except Exception as e:
            errors += 1
            db_session.rollback()
            self.logger.exception("show game refresh final commit failed err=%s", e)

    def _fetch_unprocessed_games(self, db_session: Session, username: str) -> List[GameHistory]:
        '''
            Gets all unprocessed & non-cpu game objects from the mlb the show api
            params:
                db_session -> the current db_session connection
                username -> the current mlb the show username
            returns:
                the list of unprocessed non cpu games

        '''
        
        existing_games = self._existing_game_ids(db_session, username)
        self.logger.debug(
            "show game refresh existing games username=%s count=%s",
            username,
            len(existing_games),
        )

        api_games = self._fetch_game_history(username)
        if not api_games:
            self.logger.info("show game refresh no api games username=%s", username)
            return []
        
        # Filter out CPU games.
        cpu_filtered = 0
        for game in api_games:
            if game.home_full_name == "CPU" or game.away_full_name == "CPU":
                cpu_filtered += 1
                api_games.remove(game)

        remaining = [g for g in api_games if g.id not in existing_games]
        self.logger.info(
            "show game refresh api_games username=%s total=%s cpu_filtered=%s unprocessed=%s",
            username,
            len(api_games) + cpu_filtered,
            cpu_filtered,
            len(remaining),
        )
        return remaining

    def _existing_game_ids(self, db_session: Session, username: str) -> Set[str]:
        '''
            Gets all exiting game ids in the db for the given username
            params:
                db_session -> the current db_session connection
                username -> the current mlb the show username
            returns:
                the set of game_ids that the user has played that are stored in the db
        '''
        stmt = select(ShowGameSummary.id).where(
            or_(
                ShowGameSummary.home_profile_username == username,
                ShowGameSummary.away_profile_username == username,
            )
        )
        existing = {gid for gid in db_session.scalars(stmt) if gid}
        self.logger.debug("show game refresh existing ids username=%s count=%s", username, len(existing))
        return existing

    def _fetch_game_history(self, username: str) -> None | List[GameHistory]:
        '''
            Gets all game_history objects from the mlb the show api
            params:
                username -> the current mlb the show username
            returns:
                the list of game_history objects from the mlb the show api or None if no results from api
        '''
        params = {"username": username, "platform": "xbox"}
        self.logger.debug("show game refresh fetch game history username=%s", username)
        game_objs = self._fetch_paginated_data(GAME_HISTORY_URL, params, items_key="game_history")
        if not game_objs:
            self.logger.info("show game refresh game history empty username=%s", username)
            return []
        
        games = [GameHistory.from_json(o) for o in game_objs]
        self.logger.info("show game refresh game history fetched username=%s count=%s", username, len(games))
        return games
    
    def _fetch_game_log(self, username: str, game_id: str) -> GameLog:
        '''
            Gets all game_log objects from the mlb the show api
            params:
                username -> the current mlb the show username
                game_id -> the current game id
            returns:
                the game_log object from the mlb the show api
        '''
        params = {"username": username, "id": game_id}
        self.logger.debug("show game refresh fetch game log username=%s game_id=%s", username, game_id)
        game_obj = self._api_client.get(GAME_LOG_URL, params)
        game_log = GameLog.from_json(game_obj, game_id)
        self.logger.debug("show game refresh fetched game log username=%s game_id=%s", username, game_id)
        return game_log

    def _process_game(self, db_session: Session, username: str, game: GameHistory) -> Optional[dict]:
        self.logger.info("show game refresh process game start game_id=%s username=%s", game.id, username)
        game_log = self._fetch_game_log(username, game.id)

        if not game_log:
            self.logger.warning("show game refresh missing game log game_id=%s username=%s", game.id, username)
            return None
        game_log_text_regex = GameLogTextRegexHandler(game_log.game_log_string)

        home_username, away_username = self._ensure_profiles(db_session, username, game)
        if not home_username or not away_username:
            self.logger.warning(
                "show game refresh missing usernames game_id=%s username=%s home=%s away=%s",
                game.id,
                username,
                home_username,
                away_username,
            )
            return None

        # Ensure profiles exist before inserting game summary (FK constraint).
        db_session.flush()
        
        ball_park = self._ensure_ball_park(db_session, game_log_text_regex)
        if ball_park:
            self.logger.debug(
                "show game refresh ball park game_id=%s name=%s elevation=%s",
                game.id,
                ball_park.name,
                ball_park.elevation,
            )
        else:
            self.logger.debug("show game refresh ball park missing game_id=%s", game.id)

        summary = self._upsert_game_summary(
            db_session,
            game=game,
            game_log=game_log,
            ball_park=ball_park,
            home_username=home_username,
            away_username=away_username,
            game_log_text_regex=game_log_text_regex,
        )

        if summary is not None:
            # Ensure the parent row exists before inserting dependent rows.
            db_session.flush([summary])

        self._upsert_batter_boxscores(db_session, game=game, game_log=game_log)

        self._upsert_pitcher_boxscores(db_session, game=game, game_log=game_log)

        self._upsert_half_innings(db_session, game=game, game_log_text_regex=game_log_text_regex)

        # Flush summary/boxscores/half-innings before events so FK constraints are satisfied.
        db_session.flush()

        self._upsert_game_event(db_session, game=game, game_log=game_log, game_log_text_regex=game_log_text_regex)

        self.logger.info("show game refresh process game done game_id=%s", game.id)
        
        return game_log

    def _ensure_profiles(self, db_session: Session, username: str, game: GameHistory) -> Tuple[Optional[str], Optional[str]]:
        '''
            Handler to ensure that both show profiles exist in the db
            params:
                db_session -> current db_session
                username -> the current mlb the show username
                game -> the current game object
            returns:
                home_username, away_username
        '''
        home_username, away_username = self._resolve_game_usernames(game, username)
        self.logger.debug(
            "show game refresh resolved usernames game_id=%s current=%s home=%s away=%s",
            getattr(game, "id", None),
            username,
            home_username,
            away_username,
        )
        if home_username:
            self._ensure_show_profile(db_session, home_username)
        if away_username:
            self._ensure_show_profile(db_session, away_username)
        return home_username, away_username

    def _ensure_show_profile(self, db_session: Session, username: Optional[str]) -> Optional[ShowProfile]:
        '''
            Method to insert the username into the show profile table. Or fetch it if it exists
            params:
                db_session -> current db_session
                username -> the mlb the show username (already cleaned)
            returns:
                the show profile object
        '''
        for obj in db_session.new:
            if isinstance(obj, ShowProfile) and obj.username == username:
                self.logger.debug("show profile exists in session username=%s", username)
                return obj

        existing = db_session.get(ShowProfile, username)
        if existing:
            self.logger.debug("show profile exists username=%s", username)
            return existing

        profile = ShowProfile(username=username)
        db_session.add(profile)
        self.logger.info("show profile created username=%s", username)
        return profile

    def _resolve_game_usernames(
        self,
        game: GameHistory | Mapping[str, Any],
        username: str,
    ) -> Tuple[Optional[str], Optional[str]]:
        '''
            Finds the correct home and away usernames for the game
            params:
                game -> the current game object
                username -> the current mlb the show username
            returns:
                home_username, away_username
        '''
        if not game:
            return None, None

        current = self._clean_username(username or "")
        if isinstance(game, Mapping):
            home_raw = game.get("home_name")
            away_raw = game.get("away_name")
        else:
            home_raw = getattr(game, "home_name", None)
            away_raw = getattr(game, "away_name", None)

        home_username = self._resolve_username_slot(home_raw, current)
        away_username = self._resolve_username_slot(away_raw, current)
        self.logger.debug(
            "show game refresh resolve usernames raw_home=%s raw_away=%s current=%s resolved_home=%s resolved_away=%s",
            home_raw,
            away_raw,
            current,
            home_username,
            away_username,
        )
        return home_username, away_username

    def _clean_username(self, raw: str) -> Optional[str]:
        '''
            Cleans username strings with regex patterns
            params:
                raw -> the raw string
            returns:
                the cleaned username or nothing if no username found
        '''
        if not raw:
            return None
        name = raw.strip()
        name = re.sub(r"\s*\^b\d+\^\s*$", "", name)
        name = name.strip()
        return name or None

    def _resolve_username_slot(self, raw: Optional[str], current: Optional[str]) -> Optional[str]:
        '''
            Returns the correct username from the name string. Cleans the CPU bug
            params:
                raw -> the raw string
                current -> the current username
            returns:
                the cleaned username or nothing if no username found
        '''
        clean = self._clean_username(raw or "")
        if clean and clean.upper() != "CPU":
            return clean
        if current and current.upper() != "CPU":
            return current
        return clean

    def _ensure_ball_park(self, db_session: Session, game_log_text_regex: GameLogTextRegexHandler) -> Optional[ShowBallParks]:
        '''
            Handler to ensure that the ball park exists in the db
            params:
                db_session -> current db_session
                game_log -> the current game_log object
            returns:
                the ball park object
        '''
        name, elevation = game_log_text_regex.extract_ball_park()
        if not name:
            self.logger.debug("show ball park missing name")
            return None

        for obj in db_session.new:
            if isinstance(obj, ShowBallParks) and obj.name == name:
                db_session.flush([obj])
                self.logger.debug("show ball park exists in session name=%s", name)
                return obj

        stmt = select(ShowBallParks).where(ShowBallParks.name == name)
        existing = db_session.scalars(stmt).first()
        if existing:
            if existing.elevation is None and elevation is not None:
                existing.elevation = elevation
                self.logger.info("show ball park elevation updated name=%s elevation=%s", name, elevation)
            return existing

        ball_park = ShowBallParks(name=name, elevation=elevation)
        db_session.add(ball_park)
        db_session.flush([ball_park])
        self.logger.info("show ball park created name=%s elevation=%s", name, elevation)
        return ball_park

    def _upsert_half_innings(self, db_session: Session, game: GameHistory, game_log_text_regex: GameLogTextRegexHandler) -> int:
        '''
            Upserts the half game summary object into the db
            params:
                db_session -> current db_session
                game -> the current game object
                game_log_text_regex -> the current game_log_text_regex object
            returns:
                the number of inserted rows
        '''
        half_innings = game_log_text_regex.extract_half_innings(game.home_name, game.away_name)
        if not half_innings:
            self.logger.debug("show half innings none game_id=%s", game.id)
            return 0

        stmt = select(ShowGameHalfInning).where(ShowGameHalfInning.game_id == game.id)
        existing_rows = list(db_session.scalars(stmt))
        existing_by_key = {(row.inning, row.is_home_batting): row for row in existing_rows}

        inserted = 0
        for row_data in half_innings:
            key = (row_data["inning"], row_data["is_home_batting"])
            existing = existing_by_key.get(key)
            if existing is None:
                db_session.add(ShowGameHalfInning(game_id=game.id, **row_data))
                inserted += 1
                continue
            for field, value in row_data.items():
                setattr(existing, field, value)

        self.logger.info(
            "show half innings upserted game_id=%s total=%s inserted=%s",
            game.id,
            len(half_innings),
            inserted,
        )
        return inserted

    def _upsert_game_summary(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        ball_park: Optional[ShowBallParks],
        home_username: str,
        away_username: str,
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> Optional[ShowGameSummary]:
        '''
            Upserts the half game summary object into the db
            params:
                db_session -> current db_session
                game -> the current game object
                game_log -> the current game_log object
                ball_park -> the current ball_park object
                home_username -> the current home username
                away_username -> the current away username
                game_log_text_regex -> the current game_log_text_regex object
            returns:
                the show game summary object
        '''
        
        game_id = game.id
        difficulty = game_log_text_regex.extract_difficulty()
        weather_degrees, weather_description, weather_wind = game_log_text_regex.extract_weather()

        summary_fields = {
            "id": game_id,
            "home_profile_username": home_username,
            "away_profile_username": away_username,
            "home_name": home_username,
            "away_name": away_username,
            "home_full_name": game.home_full_name.strip(),
            "away_full_name": game.away_full_name.strip(),
            "home_result": game.home_display_result,
            "away_result": game.away_display_result,
            "home_runs": game.home_runs,
            "away_runs": game.away_runs,
            "home_hits": game.home_hits,
            "away_hits": game.away_hits,
            "home_errors": game.home_errors,
            "away_errors": game.away_errors,
            "innings": game_log.innings,
            "date": game.date,
            "difficulty": difficulty,
            "is_online": True,
            "weather_degrees": weather_degrees,
            "weather_description": weather_description,
            "weather_wind": weather_wind,
            "summary": game.summary
        }

        existing = db_session.get(ShowGameSummary, game_id)
        if existing is None:
            summary = ShowGameSummary(**summary_fields, ball_park=ball_park)
            db_session.add(summary)
            self.logger.info("show game summary created game_id=%s", game_id)
            return summary

        for field, value in summary_fields.items():
            if value is None:
                continue
            setattr(existing, field, value)

        existing.ball_park = ball_park
        self.logger.info("show game summary updated game_id=%s", game_id)
        return existing

    def _upsert_game_event(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> int:
        events = game_log_text_regex.extract_game_events(game.home_name, game.away_name)
        if not events:
            self.logger.debug("show game events none game_id=%s", game.id)
            return 0

        self.logger.info("show game events extracted game_id=%s count=%s", game.id, len(events))
        stmt = select(ShowGameHalfInning).where(ShowGameHalfInning.game_id == game.id)
        half_rows = list(db_session.scalars(stmt))
        half_by_key = {(row.inning, row.is_home_batting): row for row in half_rows}

        stmt = select(ShowGameEvent).where(ShowGameEvent.game_id == game.id)
        existing_events = list(db_session.scalars(stmt))
        existing_by_seq = {row.seq: row for row in existing_events}

        inserted = 0
        home_score = 0
        away_score = 0
        half_outs = 0
        bases = (False, False, False)
        current_half_key = None
        event_seq_in_half = 0

        for seq, event_data in enumerate(events, start=1):
            inning = event_data["inning"]
            is_home = event_data["is_home_batting"]
            event_text = event_data["event_text"]
            event_type = event_data.get("event_type") or "play"
            outs_delta = int(event_data.get("outs_delta") or 0)
            runs_delta = int(event_data.get("runs_delta") or 0)

            half_key = (inning, is_home)
            if half_key != current_half_key:
                current_half_key = half_key
                half_outs = 0
                bases = (False, inning >= 10, False)
                event_seq_in_half = 0

            outs_before = half_outs
            outs_after = min(3, outs_before + outs_delta)
            half_outs = outs_after
            event_seq_in_half += 1

            half_row = half_by_key.get(half_key)
            half_id = half_row.id if half_row is not None else None

            pre_on_1b, pre_on_2b, pre_on_3b = bases
            post_on_1b, post_on_2b, post_on_3b = self._apply_bases_from_event(
                bases,
                inning=inning,
                outs_after=outs_after,
                event_type=event_type,
                event_text=event_text,
            )
            bases = (post_on_1b, post_on_2b, post_on_3b)

            home_before = home_score
            away_before = away_score
            if is_home:
                home_score += runs_delta
            else:
                away_score += runs_delta
            home_after = home_score
            away_after = away_score

            event_fields = {
                "game_id": game.id,
                "half_inning_id": half_id,
                "seq": seq,
                "inning": inning,
                "is_home_batting": is_home,
                "event_text": event_text,
                "event_type": event_type,
                "outs_before": outs_before,
                "outs_after": outs_after,
                "home_score_before": home_before,
                "away_score_before": away_before,
                "home_score_after": home_after,
                "away_score_after": away_after,
                "pre_on_1b": pre_on_1b,
                "pre_on_2b": pre_on_2b,
                "pre_on_3b": pre_on_3b,
                "post_on_1b": post_on_1b,
                "post_on_2b": post_on_2b,
                "post_on_3b": post_on_3b,
                "event_seq_in_half": event_seq_in_half,
            }

            existing = existing_by_seq.get(seq)
            if existing is None:
                event_row = ShowGameEvent(**event_fields)
                db_session.add(event_row)
                inserted += 1
                existing_by_seq[seq] = event_row
            else:
                event_row = existing
                for k, v in event_fields.items():
                    if v is None:
                        continue
                    setattr(event_row, k, v)

            if event_row.id is None:
                db_session.flush([event_row])

            if event_type == "pa":
                self._upsert_plate_appearance(
                    db_session,
                    game,
                    game_log,
                    event_row,
                    events,
                    game_log_text_regex,
                )

            self._upsert_pitching_change(db_session, game, event_row, events, game_log_text_regex)

            self._upsert_substitution(db_session, game, game_log, event_row, game_log_text_regex)

            self._upsert_runner_moves(db_session, game, game_log, event_row, events, game_log_text_regex)

            self._upsert_event_card_candidates(db_session, game, event_row)

        self._upsert_pitcher_game_scores(db_session, game, game_log, events, game_log_text_regex)

        self.logger.info(
            "show game events upserted game_id=%s inserted=%s total=%s",
            game.id,
            inserted,
            len(events),
        )
        return inserted

    def _upsert_plate_appearance(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        event_row: ShowGameEvent,
        events: List[dict],
        game_log_text_regex: GameLogTextRegexHandler
    ) -> int:
        if (event_row.event_text or "").strip() == "":
            self.logger.debug("show plate appearance skip empty event event_id=%s", event_row.id)
            return 0

        text = (event_row.event_text or "").strip()
        event_type = (getattr(event_row, "event_type", None) or "play").lower()
        if event_type != "pa":
            return 0
        
        batter_name = game_log_text_regex.extract_batter_name(text)
        if not batter_name:
            self.logger.debug("show plate appearance missing batter event_id=%s", event_row.id)
            return 0
        batting_boxscores = (
            game_log.home_batting_boxscores
            if event_row.is_home_batting
            else game_log.away_batting_boxscores
        )

        batter_box = next(
            (bs for bs in batting_boxscores if self._matches_batter(batter_name, bs.player_name)),
            None,
        )

        batter_pos_code = self._extract_pos_code(batter_box.player_name) if batter_box else None

        batter_mlb_id = self._resolve_batter_mlb_id(db_session, batter_name, batter_pos_code)

        pitcher_name = None
        target_is_home_batting = event_row.is_home_batting

        scan_idx = event_row.seq - 2
        while scan_idx >= 0:
            e = events[scan_idx]
            if (e.get("event_type") or "").lower() == "pitching_change" and e.get("is_home_batting") == target_is_home_batting:
                pitcher_name = game_log_text_regex.extract_pitcher_name(e.get("event_text") or "")
                if pitcher_name:
                    break
            scan_idx -= 1

        pitcher_mlb_id = self._resolve_pitcher_mlb_id(db_session, pitcher_name)

        player_ids = [pid for pid in (batter_mlb_id, pitcher_mlb_id) if pid is not None]
        players_by_id = {}
        if player_ids:
            rows = db_session.scalars(select(Player).where(Player.mlb_id.in_(player_ids))).all()
            players_by_id = {p.mlb_id: p for p in rows}

        pitcher_throws = None
        pitcher = players_by_id.get(pitcher_mlb_id)
        if pitcher is not None:
            ph = self._norm_hand(pitcher.pitch_hand_code)
            pitcher_throws = ph if ph in ("L", "R") else None

        batter_side = None
        batter = players_by_id.get(batter_mlb_id)
        if batter is not None:
            batter_side = self._batter_side_vs_pitcher(batter.bat_side_code, pitcher_throws)


        fields = game_log_text_regex._parse_pa_outcome_fields(event_row.event_text or "")
        runs_scored = game_log_text_regex.extract_runs_scored(event_row.event_text or "")
        rbi = game_log_text_regex.extract_rbi(event_row.event_text or "")

        is_pp, exit_vel = self._apply_perfect_perfect_to_pa(game_log_text_regex, text)
        k = self.parse_strikeout_extras(event_row.event_text or "", batter_side=batter_side)

        pa_fields = {
            "event_id": event_row.id,
            "batter_name_raw": batter_name,
            "pitcher_name_raw": pitcher_name,

            "batter_mlb_id": batter_mlb_id,
            "pitcher_mlb_id": pitcher_mlb_id,

            "result": fields["result"],
            "batted_ball_type": fields["batted_ball_type"],
            "fielder_pos": fields["fielder_pos"],
            "putout_code": fields["putout_code"],

            "is_out": fields["is_out"],
            "is_double_play": fields["is_double_play"],
            "is_sac_fly": fields["is_sac_fly"],
            "is_sac_bunt": fields["is_sac_bunt"],

            "runs_scored": runs_scored,
            "rbi": rbi,

            "hr_distance_ft": fields["hr_distance_ft"],
            "is_perfect_perfect": is_pp,
            "exit_vel_mph": exit_vel,

            "hit_direction": fields["hit_direction"],
            "is_error": fields["is_error"],
            "error_pos": fields["error_pos"],

            "is_strikeout": k["is_strikeout"],
            "k_pitch_type": k["k_pitch_type"],
            "k_loc_height": k["k_loc_height"],
            "k_loc_width": k["k_loc_width"],
            "k_is_chase": k["k_is_chase"],
            "k_is_looking": k["k_is_looking"],
            "k_timing": k["k_timing"],

            "batter_side": batter_side,
            "pitcher_throws": pitcher_throws,

        }
        
        pitcher_name_raw = pitcher_name or "Unknown"
        pa_fields["pitcher_name_raw"] = pitcher_name_raw

        row = db_session.get(ShowGamePlateAppearance, event_row.id)

        if row is None:
            db_session.add(ShowGamePlateAppearance(**pa_fields))
            self.logger.debug(
                "show plate appearance created event_id=%s batter=%s pitcher=%s",
                event_row.id,
                batter_name,
                pitcher_name_raw,
            )
            return 1

        update_fields = dict(pa_fields)
        update_fields.pop("event_id", None)
        for k, v in update_fields.items():
            setattr(row, k, v)

        self.logger.debug(
            "show plate appearance updated event_id=%s batter=%s pitcher=%s",
            event_row.id,
            batter_name,
            pitcher_name_raw,
        )
        return 0
    
    def _upsert_pitcher_game_scores(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        events: list[dict],
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> int:
        items = game_log_text_regex.extract_pitcher_game_scores()
        if not items:
            self.logger.debug("show pitcher game scores none game_id=%s", game.id)
            return 0

        inserted = 0
        for it in items:
            name = it["pitcher_name_raw"]
            is_home = it["is_home"]
            gs = it["game_score"]

            mlb_id = self._resolve_pitcher_mlb_id(db_session, name)

            row = db_session.get(ShowGamePitcherGameScore, (game.id, name, is_home))
            if row is None:
                row = ShowGamePitcherGameScore(game_id=game.id, pitcher_name_raw=name, is_home=is_home, game_score=gs)
                db_session.add(row)
            else:
                row.game_score = gs

            row.pitcher_mlb_id = mlb_id
            inserted += 1

        self.logger.info(
            "show pitcher game scores upserted game_id=%s count=%s",
            game.id,
            inserted,
        )
        return inserted
    
    def _upsert_event_card_candidates(self, db_session: Session, game: GameHistory, event_row: ShowGameEvent) -> int:
        inserted = 0

        if (event_row.event_type or "").lower() == "pa":
            pa = db_session.get(ShowGamePlateAppearance, event_row.id)
            if pa is None:
                return 0

            inserted += self._add_candidates_for_player(db_session, event_row.id, "batter", pa.batter_mlb_id)
            inserted += self._add_candidates_for_player(db_session, event_row.id, "pitcher", pa.pitcher_mlb_id)

        if inserted:
            self.logger.debug("show event card candidates inserted event_id=%s count=%s", event_row.id, inserted)
        return inserted

    def _add_candidates_for_player(self, db_session: Session, event_id: int, role: str, mlb_id: Optional[int]) -> int:
        if not mlb_id:
            return 0

        card_ids = db_session.scalars(
            select(Card.id).where(Card.mlb_id == mlb_id, Card.year == 25)
        ).all()

        if not card_ids:
            return 0

        db_session.execute(
            delete(ShowEventCardCandidate).where(
                ShowEventCardCandidate.event_id == event_id,
                ShowEventCardCandidate.role == role,
            )
        )

        for cid in card_ids:
            db_session.add(ShowEventCardCandidate(event_id=event_id, role=role, card_id=cid, score=None))
        self.logger.debug(
            "show event card candidates set event_id=%s role=%s cards=%s",
            event_id,
            role,
            len(card_ids),
        )
        return len(card_ids)
    
    def _upsert_runner_moves(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        event_row: ShowGameEvent,
        events: list[dict],
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> int:
        _ADV_RE = re.compile(r"(?P<name>[^.]+?)\s+advances to\s+(?P<base>2nd|3rd)\.", re.IGNORECASE)
        _SCORE_RE = re.compile(r"(?P<name>[^.]+?)\s+scores\.", re.IGNORECASE)
        _OUT_AT_RE = re.compile(r"(?P<name>[^.]+?)\s+out at\s+(?P<base>2nd|3rd|home)\.", re.IGNORECASE)
        _STEAL_RE = re.compile(r"(?P<name>[^.]+?)\s+stole\s+(?P<base>2nd|3rd|home)\.", re.IGNORECASE)
        _CS_RE = re.compile(r"(?P<name>[^.]+?)\s+caught stealing\s+(?P<base>2nd|3rd|home)\.", re.IGNORECASE)

        _BASE_TO_NUM = {"2nd": 2, "3rd": 3, "home": 4}
        text = (event_row.event_text or "").strip()
        if not text:
            return 0
        
        db_session.execute(delete(ShowEventRunnerMove).where(ShowEventRunnerMove.event_id == event_row.id))

        moves: list[tuple[str, Optional[int], Optional[int], str, str]] = []

        for m in _ADV_RE.finditer(text):
            name = m.group("name").strip()
            to_base = _BASE_TO_NUM[m.group("base")]
            moves.append((name, None, to_base, "advance", m.group(0).strip()))

        for m in _SCORE_RE.finditer(text):
            name = m.group("name").strip()
            moves.append((name, None, 4, "score", m.group(0).strip()))

        for m in _OUT_AT_RE.finditer(text):
            name = m.group("name").strip()
            moves.append((name, None, -1, "out", m.group(0).strip()))

        for m in _STEAL_RE.finditer(text):
            name = m.group("name").strip()
            to_base = _BASE_TO_NUM[m.group("base")]
            moves.append((name, None, to_base, "stolen_base", m.group(0).strip()))

        for m in _CS_RE.finditer(text):
            name = m.group("name").strip()
            moves.append((name, None, -1, "caught_stealing", m.group(0).strip()))

        if not moves:
            return 0

        inserted = 0
        for runner_name, from_base, to_base, move_type, note in moves:
            runner_id = self._resolve_batter_mlb_id(db_session, runner_name, None)

            row = ShowEventRunnerMove(
                event_id=event_row.id,
                runner_name_raw=runner_name,
                runner_mlb_id=runner_id,
                from_base=from_base,
                to_base=to_base,
                move_type=move_type,
                note=note[:128] if note else None,
            )
            db_session.add(row)
            inserted += 1

        self.logger.debug(
            "show runner moves upserted event_id=%s count=%s",
            event_row.id,
            inserted,
        )
        return inserted
    
    def _upsert_batter_boxscores(self, db_session: Session, game: GameHistory, game_log: GameLog) -> int:
        inserted = 0

        for is_home, batting_stats in (
            (True, game_log.home_batting_boxscores),
            (False, game_log.away_batting_boxscores),
        ):
            for idx, bs in enumerate(batting_stats, start=1):
                raw_name = (bs.player_name or "").strip()
                if not raw_name:
                    continue

                batter_name = self._strip_boxscore_name(raw_name)
                pos_code = self._pos_code_from_boxscore(raw_name)
                mlb_id = self._resolve_batter_mlb_id(db_session, batter_name, pos_code)

                replaced_idx = bs.replaced if getattr(bs, "replaced", 0) else None
                if replaced_idx is not None and replaced_idx <= 0:
                    replaced_idx = None

                fields = {
                    "game_id": game.id,
                    "is_home": is_home,
                    "appearance_idx": idx,
                    "replaced_apperance_idx": replaced_idx,
                    "player_name_raw": raw_name,
                    "mlb_id": mlb_id,
                    "ab": bs.ab,
                    "h": bs.h,
                    "r": bs.r,
                    "rbi": bs.rbi,
                    "bb": bs.bb,
                    "so": bs.so,
                    "doubles": bs.doubles,
                    "triples": bs.triples,
                    "hr": bs.hr,
                    "sh": bs.sh,
                    "sf": bs.sf,
                    "gidp": bs.gidp,
                    "e": bs.e,
                    "pb": bs.pb,
                    "hbp": bs.hbp,
                    "sb": bs.sb,
                    "cs": bs.cs,
                    "innings": int(bs.innings) if bs.innings is not None else 0,
                    "pos": bs.pos,
                }

                row = db_session.get(
                    ShowBatterBoxscore,
                    (game.id, is_home, idx),
                )

                if row is None:
                    row = ShowBatterBoxscore(**fields)
                    db_session.add(row)
                    inserted += 1
                else:
                    for k, v in fields.items():
                        setattr(row, k, v)

        self.logger.info(
            "show batter boxscores upserted game_id=%s inserted=%s",
            game.id,
            inserted,
        )
        return inserted
    
    def _upsert_pitcher_boxscores(self, db_session: Session, game: GameHistory, game_log: GameLog) -> int:
        inserted = 0

        for is_home, pitching_stats in (
            (True, game_log.home_pitching_boxscores),
            (False, game_log.away_pitching_boxscores),
        ):
            for idx, ps in enumerate(pitching_stats, start=1):
                raw_name = (ps.player_name or "").strip()
                if not raw_name:
                    continue

                pitcher_name = self._strip_boxscore_name(raw_name)
                mlb_id = self._resolve_pitcher_mlb_id(db_session, pitcher_name)

                ip_raw, outs_pitched = self._ip_to_outs(ps.ip)

                fields = {
                    "game_id": game.id,
                    "is_home": is_home,
                    "appearance_idx": idx,
                    "player_name_raw": raw_name,
                    "mlb_id": mlb_id,
                    "ip_raw": ip_raw,
                    "outs_pitched": outs_pitched,
                    "r": ps.r,
                    "h": ps.h,
                    "er": ps.er,
                    "bb": ps.bb,
                    "so": ps.so,
                    "era": float(ps.era) if ps.era is not None else None,
                    "wp": ps.wp,
                    "win": ps.win,
                    "loss": ps.loss,
                    "save": ps.save,
                    "b_save": ps.b_save,
                    "hold": ps.hold,
                    "s_wins": ps.s_wins,
                    "s_losses": ps.s_losses,
                    "s_saves": ps.s_saves,
                    "s_b_saves": ps.s_b_saves,
                    "s_holds": ps.s_holds,
                }

                row = db_session.get(
                    ShowPitcherBoxscore,
                    (game.id, is_home, idx),
                )

                if row is None:
                    row = ShowPitcherBoxscore(**fields)
                    db_session.add(row)
                    inserted += 1
                else:
                    for k, v in fields.items():
                        setattr(row, k, v)

        self.logger.info(
            "show pitcher boxscores upserted game_id=%s inserted=%s",
            game.id,
            inserted,
        )
        return inserted


    def _strip_boxscore_name(self, name: str) -> str:
        if not name:
            return ""
        cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
        if "," in cleaned:
            cleaned = cleaned.split(",", 1)[0].strip()
        return cleaned
    
    def _pos_code_from_boxscore(self, player_name: str) -> Optional[str]:
        if not player_name:
            return None
        m = re.search(r"\(([^)]+)\)\s*$", player_name)
        if not m:
            return None
        return m.group(1).strip().upper() or None

    def _ip_to_outs(self, ip: float) -> tuple[str, int]:
        ip_str = str(ip)
        whole = int(ip)
        frac = round(ip - whole, 1)
        if frac < 0.05:
            outs = whole * 3
            ip_raw = f"{whole}.0"
        elif 0.05 <= frac <= 0.15:
            outs = whole * 3 + 1
            ip_raw = f"{whole}.1"
        elif 0.15 < frac <= 0.25:
            outs = whole * 3 + 2
            ip_raw = f"{whole}.2"
        else:
            outs = int(round(ip * 3))
            ip_raw = ip_str
        return ip_raw, outs

    def _parse_pinch_hit(self, text: str) -> tuple[Optional[str], Optional[str]]:
        _PINCH_HIT_RE = re.compile(
            r"^(?P<in>.+?)\s+pinch hit for\s+(?P<out>.+?)\.$",
            re.IGNORECASE,
        )
        m = _PINCH_HIT_RE.match((text or "").strip())
        if not m:
            return None, None
        return m.group("in").strip(), m.group("out").strip()
    
    def _upsert_substitution(
        self,
        db_session: Session,
        game: GameHistory,
        game_log: GameLog,
        event_row: ShowGameEvent,
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> int:
        if (event_row.event_type or "").lower() != "pinch_hit":
            return 0

        incoming, outgoing = self._parse_pinch_hit(event_row.event_text or "")
        if not incoming:
            return 0

        incoming_id = self._resolve_batter_mlb_id(db_session, incoming, None)  # hitter pool
        outgoing_id = self._resolve_batter_mlb_id(db_session, outgoing, None) if outgoing else None

        row = db_session.get(ShowGameSubstitution, event_row.id)
        if row is None:
            row = ShowGameSubstitution(event_id=event_row.id)
            db_session.add(row)

        row.sub_type = "pinch_hit"
        row.incoming_player_name_raw = incoming
        row.outgoing_player_name_raw = outgoing
        row.incoming_mlb_id = incoming_id
        row.outgoing_mlb_id = outgoing_id
        self.logger.debug(
            "show substitution upserted event_id=%s incoming=%s outgoing=%s",
            event_row.id,
            incoming,
            outgoing,
        )
        return 1
    
    def _upsert_pitching_change(
        self,
        db_session: Session,
        game: GameHistory,
        event_row: ShowGameEvent,
        events: list[dict],
        game_log_text_regex: GameLogTextRegexHandler,
    ) -> int:
        if (event_row.event_type or "").lower() != "pitching_change":
            return 0

        incoming = game_log_text_regex.extract_pitcher_name(event_row.event_text or "")
        if not incoming:
            return 0

        replaced = None
        scan_idx = event_row.seq - 2
        target_is_home_batting = event_row.is_home_batting
        while scan_idx >= 0:
            e = events[scan_idx]
            if (e.get("event_type") or "").lower() == "pitching_change" and e.get("is_home_batting") == target_is_home_batting:
                replaced = game_log_text_regex.extract_pitcher_name(e.get("event_text") or "")
                break
            scan_idx -= 1

        pitcher_mlb_id = self._resolve_pitcher_mlb_id(db_session, incoming)

        row = db_session.get(ShowGamePitchingChange, event_row.id)
        if row is None:
            row = ShowGamePitchingChange(event_id=event_row.id)
            db_session.add(row)

        row.pitcher_name_raw = incoming
        row.pitcher_mlb_id = pitcher_mlb_id
        row.replaced_pitcher_name_raw = replaced
        self.logger.debug(
            "show pitching change upserted event_id=%s incoming=%s replaced=%s",
            event_row.id,
            incoming,
            replaced,
        )
        return 1
    
    def _norm_hand(self, v: str | None) -> str | None:
        if not v:
            return None
        v = v.strip().upper()
        return v if v in ("L", "R", "S") else None

    def _batter_side_vs_pitcher(self, bat_side_code: str | None, pitcher_throws: str | None) -> str | None:
        b = self._norm_hand(bat_side_code)
        p = self._norm_hand(pitcher_throws)
        if b in ("L", "R"):
            return b
        if b == "S" and p in ("L", "R"):
            return "R" if p == "L" else "L"
        return None
    
    def parse_strikeout_extras(self, event_text: str, batter_side: Optional[str]) -> Dict[str, Any]:
        def _extract_pitch_type(t: str) -> Optional[str]:
            for pt in sorted(_PITCH_TYPES, key=len, reverse=True):
                if re.search(rf"\b{re.escape(pt)}\b", t):
                    return pt
            return None

        def _extract_k_location(t: str, *, is_chase: bool, batter_side: Optional[str]) -> tuple[Optional[str], Optional[str]]:
            if re.search(r"\bdown the middle\b", t):
                base_width = "center"
            else:
                is_inside = bool(re.search(r"\binside\b", t) or re.search(r"\band\s+in\b", t))
                is_outside = bool(re.search(r"\boutside\b", t) or re.search(r"\band\s+away\b", t) or re.search(r"\baway\b", t))

                base_width = None
                if is_inside:
                    if batter_side == "R":
                        base_width = "left"
                    elif batter_side == "L":
                        base_width = "right"
                elif is_outside:
                    if batter_side == "R":
                        base_width = "right"
                    elif batter_side == "L":
                        base_width = "left"

            # Keep width values within column limits; chase is tracked separately.
            width = base_width

            height = None
            if re.search(r"\bhigh\b", t):
                height = "high"
            elif re.search(r"\blow\b", t):
                height = "low"
            elif re.search(r"\bdown the middle\b", t) or re.search(r"\bmiddle\b", t):
                height = "middle"

            return height, width

        s = (event_text or "").strip()
        t = s.lower()
        b = (batter_side or "").strip().upper() or None

        is_k = bool(re.search(r"\bstruck out\b", t) or re.search(r"\bcalled out on strikes\b", t))
        if not is_k:
            return {
                "is_strikeout": False,
                "k_pitch_type": None,
                "k_loc_height": None,
                "k_loc_width": None,
                "k_is_chase": None,
                "k_is_looking": None,
                "k_timing": None,
            }

        is_chase = "chasing" in t
        is_looking = "looking" in t

        k_timing = None
        if re.search(r"\bearly\b", t):
            k_timing = "early"
        elif re.search(r"\blate\b", t):
            k_timing = "late"

        pitch_type = _extract_pitch_type(t)
        loc_height, loc_width = _extract_k_location(t, is_chase=is_chase, batter_side=b)

        return {
            "is_strikeout": True,
            "k_pitch_type": pitch_type,
            "k_loc_height": loc_height,
            "k_loc_width": loc_width,
            "k_is_chase": is_chase,
            "k_is_looking": is_looking,
            "k_timing": k_timing,
        }
    
    def _apply_perfect_perfect_to_pa(
        self,
        game_log_text_regex: GameLogTextRegexHandler,
        pa_event_text: str,
    ) -> tuple[Optional[bool], Optional[int]]:
        pa_key = self._pa_first_sentence_key(pa_event_text)
        if not pa_key:
            return None, None

        for item in game_log_text_regex.extract_perfect_perfect_hits():
            if item.get("used"):
                continue
            if item.get("event_key") == pa_key:
                item["used"] = True
                return True, int(item.get("mph")) if item.get("mph") is not None else None

        return None, None

    def _pa_first_sentence_key(self, s: str) -> str:
        if not s:
            return ""
        out = re.sub(r"\s+", " ", s.replace("*", "")).strip()
        dot = out.find(".")
        if dot != -1:
            out = out[: dot + 1].strip()
        if out and not out.endswith("."):
            out += "."
        return out.lower()
    
    def _norm_name(self, s: str) -> str:
        s = (s or "").lower().strip()
        s = s.replace(".", "")
        s = re.sub(r"[^a-z0-9\s'-]", " ", s)
        return " ".join(s.split())

    def _boxscore_name_key(self, player_name: str) -> str:
        base = (player_name or "").strip()
        if "(" in base:
            base = base.split("(", 1)[0].strip()
        if "," in base:
            base = base.split(",", 1)[0].strip()
        return self._norm_name(base)

    def _matches_batter(self, batter_name: str, boxscore_player_name: str) -> bool:
        b = self._norm_name(batter_name)
        p = self._boxscore_name_key(boxscore_player_name)
        if not b or not p:
            return False
        if p == b:
            return True
        if p.startswith(b + " "):
            return True
        if p.endswith(" " + b):
            return True
        if p.startswith(b):
            return True
        return False

    def _extract_pos_code(self, boxscore_player_name: str) -> Optional[str]:
        s = (boxscore_player_name or "").strip()

        m = re.search(r"\(([^)]+)\)", s)
        if m:
            code = m.group(1).strip()
            return code.upper() or None

        if "," in s:
            right = s.split(",", 1)[1].strip()
            code = right.split()[0].strip("()")
            return code.upper() or None

        return None

    def _parse_pa_outcome(self, event_text: str) -> dict:
        if not event_text:
            return {}
        text = event_text.lower()

        result = None
        if "homered" in text:
            result = "hr"
        elif "tripled" in text:
            result = "triple"
        elif "doubled" in text:
            result = "double"
        elif "singled" in text:
            result = "single"
        elif "hit by pitch" in text:
            result = "hbp"
        elif "intentionally walked" in text or "walked" in text or "walks" in text:
            result = "walk"
        elif "struck out" in text or "was called out on strikes" in text:
            result = "so"
        elif "grounded into a double play" in text or "double play" in text:
            result = "double_play"
        elif "sac fly" in text or "sacrifice fly" in text:
            result = "sac_fly"
        elif "sac bunt" in text or "sacrifice bunt" in text:
            result = "sac_bunt"
        elif "grounded out" in text:
            result = "groundout"
        elif "flied out" in text:
            result = "flyout"
        elif "popped out" in text:
            result = "popout"
        elif "lined out" in text:
            result = "lineout"
        elif "fouled out" in text:
            result = "foulout"
        elif "reached on error" in text or "reached on a fielder" in text or "reached on fielder" in text:
            result = "reached_on_error"

        batted_ball_type = None
        if "grounded" in text:
            batted_ball_type = "ground"
        elif "flied" in text:
            batted_ball_type = "fly"
        elif "popped" in text:
            batted_ball_type = "popup"
        elif "lined" in text:
            batted_ball_type = "line"
        elif "fouled" in text:
            batted_ball_type = "foul"

        is_double_play = "double play" in text
        is_sac_fly = "sac fly" in text or "sacrifice fly" in text
        is_sac_bunt = "sac bunt" in text or "sacrifice bunt" in text
        is_out = result in {
            "so",
            "groundout",
            "flyout",
            "popout",
            "lineout",
            "foulout",
            "double_play",
            "sac_fly",
            "sac_bunt",
        }

        runs_scored = len(re.findall(r"\bscored\b|\bscores\b", event_text, re.IGNORECASE))
        rbi_match = re.search(r"(\d+)\s*RBI", event_text, re.IGNORECASE)
        rbi = int(rbi_match.group(1)) if rbi_match else (runs_scored if runs_scored > 0 else None)
        is_scoring_play = True if runs_scored > 0 else None

        hr_distance_ft = None
        if "homered" in text:
            dist_match = re.search(r"\((\d+)\s*feet\)", event_text, re.IGNORECASE)
            if dist_match:
                hr_distance_ft = int(dist_match.group(1))

        is_perfect_perfect = "perfect-perfect" in text
        exit_vel_mph = None
        mph_match = re.search(r"(\d+)\s*mph", event_text, re.IGNORECASE)
        if mph_match:
            exit_vel_mph = int(mph_match.group(1))

        hit_direction = self._extract_hit_direction(event_text)
        is_error = True if "error" in text else None
        error_pos = self._extract_error_pos(event_text)

        strikeout_fields = self._parse_strikeout_fields(event_text, result)

        putout_code = self._extract_putout_code(event_text)
        fielder_pos = self._fielder_pos_from_putout(putout_code)

        return {
            "result": result,
            "batted_ball_type": batted_ball_type,
            "fielder_pos": fielder_pos,
            "putout_code": putout_code,
            "is_out": is_out,
            "is_double_play": is_double_play,
            "is_sac_fly": is_sac_fly,
            "is_sac_bunt": is_sac_bunt,
            "runs_scored": runs_scored if runs_scored > 0 else None,
            "rbi": rbi,
            "hr_distance_ft": hr_distance_ft,
            "is_perfect_perfect": True if is_perfect_perfect else None,
            "exit_vel_mph": exit_vel_mph,
            "is_scoring_play": is_scoring_play,
            "hit_direction": hit_direction,
            "is_error": is_error,
            "error_pos": error_pos,
            **strikeout_fields,
        }

    def _player_pitch_hand(self, db_session: Session, mlb_id: Optional[int]) -> Optional[str]:
        if not mlb_id:
            return None
        cached = self._player_hand_cache.get(mlb_id)
        if cached is not None:
            return cached[1]
        player = db_session.get(Player, mlb_id)
        bat_side = player.bat_side_code.upper() if player and player.bat_side_code else None
        pitch_hand = player.pitch_hand_code.upper() if player and player.pitch_hand_code else None
        self._player_hand_cache[mlb_id] = (bat_side, pitch_hand)
        return pitch_hand

    def _player_bat_side(self, db_session: Session, mlb_id: Optional[int]) -> Optional[str]:
        if not mlb_id:
            return None
        cached = self._player_hand_cache.get(mlb_id)
        if cached is not None:
            return cached[0]
        player = db_session.get(Player, mlb_id)
        bat_side = player.bat_side_code.upper() if player and player.bat_side_code else None
        pitch_hand = player.pitch_hand_code.upper() if player and player.pitch_hand_code else None
        self._player_hand_cache[mlb_id] = (bat_side, pitch_hand)
        return bat_side

    def _parse_strikeout_fields(self, event_text: str, result: Optional[str]) -> dict:
        if result != "so" or not event_text:
            return {
                "is_strikeout": None,
                "k_pitch_type": None,
                "k_loc_height": None,
                "k_loc_width": None,
                "k_is_chase": None,
                "k_is_looking": None,
                "k_timing": None,
            }

        text = event_text.lower()
        is_looking = "called out on strikes" in text or "called out on strike" in text or "looking" in text
        is_chase = "chasing" in text

        pitch_match = re.search(
            r"chasing a ([a-z\\- ]+?)(?:\\s+(high|middle|low))?(?:\\s+and\\s+(inside|outside|center))?\\.?$",
            text,
        )
        if pitch_match:
            pitch_type = pitch_match.group(1).strip()
            loc_height = pitch_match.group(2)
            loc_width = pitch_match.group(3)
        else:
            pitch_match = re.search(
                r"on an? ([a-z\\- ]+?)(?:\\s+(high|middle|low))?(?:\\s+and\\s+(inside|outside|center))?\\.?$",
                text,
            )
            pitch_type = pitch_match.group(1).strip() if pitch_match else None
            loc_height = pitch_match.group(2) if pitch_match else None
            loc_width = pitch_match.group(3) if pitch_match else None

        timing = None
        if "early" in text:
            timing = "early"
        elif "late" in text:
            timing = "late"

        return {
            "is_strikeout": True,
            "k_pitch_type": pitch_type if pitch_type else None,
            "k_loc_height": loc_height,
            "k_loc_width": loc_width,
            "k_is_chase": True if is_chase else None,
            "k_is_looking": True if is_looking else None,
            "k_timing": timing,
        }

    def _extract_hit_direction(self, event_text: str) -> Optional[str]:
        if not event_text:
            return None
        match = re.search(r"\bto (left|center|right)\b", event_text, re.IGNORECASE)
        if match:
            return match.group(1).lower()
        match = re.search(r"\bto (left|center|right) field\b", event_text, re.IGNORECASE)
        if match:
            return match.group(1).lower()
        return None

    def _extract_error_pos(self, event_text: str) -> Optional[str]:
        if not event_text:
            return None
        match = re.search(r"\bE([1-9])\b", event_text, re.IGNORECASE)
        if match:
            return self._fielder_pos_from_putout(match.group(1))
        match = re.search(r"\(E([1-9])\)", event_text, re.IGNORECASE)
        if match:
            return self._fielder_pos_from_putout(match.group(1))
        return None

    def _extract_putout_code(self, event_text: str) -> Optional[str]:
        if not event_text:
            return None
        for match in re.finditer(r"\(([^)]+)\)", event_text):
            content = match.group(1).strip()
            if "feet" in content.lower():
                continue
            if re.search(r"\b[FLPU]\d\b", content) or re.search(r"\b\d-\d", content) or re.search(r"\b\dU\b", content):
                return content
        return None

    def _fielder_pos_from_putout(self, putout_code: Optional[str]) -> Optional[str]:
        if not putout_code:
            return None
        match = re.search(r"([1-9])", putout_code)
        if not match:
            return None
        pos_map = {
            "1": "P",
            "2": "C",
            "3": "1B",
            "4": "2B",
            "5": "3B",
            "6": "SS",
            "7": "LF",
            "8": "CF",
            "9": "RF",
        }
        return pos_map.get(match.group(1))

    def _resolve_batter_mlb_id(
        self,
        db_session: Session,
        batter_last_name: str,
        batter_pos_code: Optional[str],
        year: int = MLB_THE_SHOW_YEAR,
    ) -> Optional[int]:
        last = self._norm_last(batter_last_name)
        if not last:
            return None

        pos_code = self._norm_pos(batter_pos_code) if batter_pos_code else None

        # Base pool:
        # 1) non-pitchers
        # 2) must have at least one card in the given year
        base_pool = (
            select(Player.mlb_id)
            .join(Player.position)
            .where(MLBPosition.abbreviation != "P")
            .where(
                select(Card.id)
                .where(and_(Card.mlb_id == Player.mlb_id, Card.year == year))
                .exists()
            )
            .subquery()
        )

        # Helper: get candidate mlb_ids by last name (can be multiple)
        cand_stmt = (
            select(Player.mlb_id)
            .where(Player.mlb_id.in_(select(base_pool.c.mlb_id)))
            .where(func.lower(Player.last_name) == last)
        )
        cand_ids = list(db_session.scalars(cand_stmt))

        if len(cand_ids) == 0:
            # If last_name isn't matching exactly (hyphens, spaces, etc.), fall back to normalized compare
            # (still within the same base pool)
            cand_stmt = (
                select(Player.mlb_id)
                .where(Player.mlb_id.in_(select(base_pool.c.mlb_id)))
                .where(func.lower(func.replace(Player.last_name, ".", "")) == last)
            )
            cand_ids = list(db_session.scalars(cand_stmt))

        if len(cand_ids) == 1:
            return cand_ids[0]
        if len(cand_ids) == 0:
            return None

        # 3) Position filter on Player.position abbreviation (if it narrows to 1; if it narrows to 0, ignore)
        if pos_code:
            pos_filtered = list(
                db_session.scalars(
                    select(Player.mlb_id)
                    .join(Player.position)
                    .where(Player.mlb_id.in_(cand_ids))
                    .where(MLBPosition.abbreviation == pos_code)
                )
            )
            if len(pos_filtered) == 1:
                return pos_filtered[0]
            if len(pos_filtered) > 0:
                cand_ids = pos_filtered

        # From here on, we score candidates using their 2025 cards
        # 4) most matches on Card.display_position == pos_code
        # 5) then matches in Card.display_secondary_positions
        # 6) then most cards
        # 7) then highest max ovr
        # 8) then smallest mlb_id (deterministic "random")
        if not pos_code:
            # If we have no pos code, skip position scoring and just use count/max(ovr) tie-breakers
            score_stmt = (
                select(
                    Card.mlb_id.label("mlb_id"),
                    func.count(Card.id).label("card_count"),
                    func.max(Card.ovr).label("max_ovr"),
                )
                .where(Card.year == year)
                .where(Card.mlb_id.in_(cand_ids))
                .group_by(Card.mlb_id)
                .order_by(
                    func.count(Card.id).desc(),
                    func.max(Card.ovr).desc(),
                    Card.mlb_id.asc(),
                )
            )
            row = db_session.execute(score_stmt).first()
            return row[0] if row else None

        # Secondary positions contains check (case-insensitive, with boundary-ish patterns)
        # Works for "2B, 3B" and "2B" etc.
        sec_like_1 = f"{pos_code},%"
        sec_like_2 = f"%, {pos_code},%"
        sec_like_3 = f"%, {pos_code}"
        sec_like_4 = f"{pos_code}"

        score_stmt = (
            select(
                Card.mlb_id.label("mlb_id"),
                func.sum(case((Card.display_position == pos_code, 1), else_=0)).label("pos_matches"),
                func.sum(
                    case(
                        (
                            and_(
                                Card.display_secondary_positions.is_not(None),
                                or_(
                                    func.upper(Card.display_secondary_positions).like(sec_like_1),
                                    func.upper(Card.display_secondary_positions).like(sec_like_2),
                                    func.upper(Card.display_secondary_positions).like(sec_like_3),
                                    func.upper(Card.display_secondary_positions) == sec_like_4,
                                ),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).label("sec_matches"),
                func.count(Card.id).label("card_count"),
                func.max(Card.ovr).label("max_ovr"),
            )
            .where(Card.year == year)
            .where(Card.mlb_id.in_(cand_ids))
            .group_by(Card.mlb_id)
            .order_by(
                func.sum(case((Card.display_position == pos_code, 1), else_=0)).desc(),
                func.sum(
                    case(
                        (
                            and_(
                                Card.display_secondary_positions.is_not(None),
                                or_(
                                    func.upper(Card.display_secondary_positions).like(sec_like_1),
                                    func.upper(Card.display_secondary_positions).like(sec_like_2),
                                    func.upper(Card.display_secondary_positions).like(sec_like_3),
                                    func.upper(Card.display_secondary_positions) == sec_like_4,
                                ),
                            ),
                            1,
                        ),
                        else_=0,
                    )
                ).desc(),
                func.count(Card.id).desc(),
                func.max(Card.ovr).desc(),
                Card.mlb_id.asc(),
            )
        )

        row = db_session.execute(score_stmt).first()
        return row[0] if row else None

    def _norm_last(self, s: str) -> str:
        s = (s or "").strip().lower()
        s = s.replace(".", "")
        s = re.sub(r"[^a-z'\- ]", " ", s)
        return " ".join(s.split())

    def _norm_pos(self, s: str) -> str:
        return (s or "").strip().upper()
    
    def _resolve_pitcher_mlb_id(self, db: Session, pitcher_last_name: str) -> int | None:
        ln = (pitcher_last_name or "").strip().lower()
        if not ln:
            return None

        base_q = (
            select(Player.mlb_id)
            .join(MLBPosition, MLBPosition.id == Player.position_id)
            .where(MLBPosition.abbreviation == "P")
            .where(func.lower(Player.last_name) == ln)
            .where(
                exists(
                    select(1).where(
                        (Card.mlb_id == Player.mlb_id) &
                        (Card.year == 25)
                    )
                )
            )
        )

        ids = list(db.scalars(base_q))
        if not ids:
            return None
        if len(ids) == 1:
            return ids[0]

        ranked = (
            select(
                Player.mlb_id,
                func.count(Card.id).label("card_count"),
                func.max(Card.ovr).label("max_ovr"),
                func.avg(Card.ovr).label("avg_ovr"),
            )
            .join(MLBPosition, MLBPosition.id == Player.position_id)
            .join(Card, Card.mlb_id == Player.mlb_id)
            .where(MLBPosition.abbreviation == "P")
            .where(func.lower(Player.last_name) == ln)
            .where(Card.year == MLB_THE_SHOW_YEAR)
            .group_by(Player.mlb_id)
            .order_by(
                func.count(Card.id).desc(),
                func.max(Card.ovr).desc(),
                func.avg(Card.ovr).desc(),
                Player.mlb_id.asc(),
            )
        )

        row = db.execute(ranked).first()
        return row[0] if row else None

    def _normalize_name(self, name_raw: str) -> str:
        name = (name_raw or "").strip()
        if not name:
            return ""
        name = unicodedata.normalize("NFKD", name)
        name = "".join(ch for ch in name if not unicodedata.combining(ch))
        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", " ", name).strip().lower()
        return name

    def _split_boxscore_player(self, player_name: str) -> Tuple[Optional[str], Optional[str]]:
        if not player_name:
            return None, None
        parts = [part.strip() for part in player_name.split(",", 1)]
        name_part = parts[0] if parts else ""
        name_part = re.sub(r"^[a-z]-", "", name_part, flags=re.IGNORECASE).strip()
        name_part = re.sub(r"\s*\(.*?\)\s*$", "", name_part).strip()
        pos_part = parts[1].strip() if len(parts) > 1 else ""
        if not pos_part:
            return name_part or None, None
        pos_part = pos_part.split()[0]
        if "-" in pos_part:
            segs = [seg for seg in pos_part.split("-") if seg]
            pos_part = segs[-1] if segs else pos_part
        pos_part = pos_part.upper()
        return name_part or None, pos_part or None

    def _apply_bases_from_event(
        self,
        bases: Tuple[bool, bool, bool],
        *,
        inning: int,
        outs_after: int,
        event_type: str,
        event_text: str,
    ) -> Tuple[bool, bool, bool]:
        on_1b, on_2b, on_3b = bases
        text = (event_text or "").lower()

        if event_type in ("pitching_change", "pinch_hit", "pinch_run"):
            return on_1b, on_2b, on_3b

        if event_type == "caught_stealing":
            if "caught stealing home" in text:
                on_3b = False
            elif "caught stealing 3rd" in text:
                on_2b = False
            elif "caught stealing 2nd" in text:
                on_1b = False
            return self._clear_if_three_outs((on_1b, on_2b, on_3b), outs_after)

        if event_type == "steal":
            if "stole home" in text:
                on_3b = False
            elif "stole 3rd" in text:
                if on_2b:
                    on_2b = False
                elif on_1b:
                    on_1b = False
                on_3b = True
            elif "stole 2nd" in text:
                if on_1b:
                    on_1b = False
                on_2b = True
            return self._clear_if_three_outs((on_1b, on_2b, on_3b), outs_after)

        if "homered" in text:
            return self._clear_if_three_outs((False, False, False), outs_after)

        if "tripled" in text:
            on_1b, on_2b, on_3b = False, False, True
        elif "doubled" in text:
            on_1b, on_2b, on_3b = False, True, False
        elif "singled" in text or "reached on error" in text or "reached on a fielder" in text or "reached on fielder" in text:
            on_1b = True

        if ("hit by pitch" in text) or re.search(r"\bintentionally walked\b|\bwalked\b|\bwalks\b(?!:)", text):
            on_1b, on_2b, on_3b = self._apply_forced_walk((on_1b, on_2b, on_3b))

        for m in re.finditer(r"advance(?:s|d)? to (2nd|3rd)", text):
            dest = m.group(1)
            if dest == "2nd":
                if on_1b:
                    on_1b = False
                on_2b = True
            else:  # 3rd
                if on_2b:
                    on_2b = False
                elif on_1b:
                    on_1b = False
                on_3b = True

        for m in re.finditer(r"out at (2nd|3rd|home)", text):
            dest = m.group(1)
            if dest == "2nd":
                if on_2b:
                    on_2b = False
                elif on_1b:
                    on_1b = False
            elif dest == "3rd":
                if on_3b:
                    on_3b = False
                elif on_2b:
                    on_2b = False
                elif on_1b:
                    on_1b = False
            else:  # home
                if on_3b:
                    on_3b = False
                elif on_2b:
                    on_2b = False
                elif on_1b:
                    on_1b = False

        return self._clear_if_three_outs((on_1b, on_2b, on_3b), outs_after)

    def _apply_forced_walk(self, bases: Tuple[bool, bool, bool]) -> Tuple[bool, bool, bool]:
        on_1b, on_2b, on_3b = bases
        if on_2b:
            on_3b = True
        if on_1b:
            on_2b = True
        on_1b = True
        return on_1b, on_2b, on_3b
    
    def _clear_if_three_outs(
        self,
        bases: Tuple[bool, bool, bool],
        outs_after: int,
    ) -> Tuple[bool, bool, bool]:
        if outs_after >= 3:
            return (False, False, False)
        return bases
