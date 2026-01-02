from __future__ import annotations

import datetime
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert

from src.core.batting_aggregator import MLBPlayByPlayBattingAggregator
from src.core.baserunning_aggregator import MLBPlayByPlayBaserunningAggregator
from src.core.pitching_aggregator import MLBPlayByPlayPitchingAggregator
from src.core.http_client import APIClient
from src.database.models import (
    BirthLocation,
    MLBGame,
    MLBGameBattingStats,
    MLBGameBaserunningStats,
    MLBGameFieldingStats,
    MLBGameBoxscore,
    MLBGamePitchingStats,
    MLBPosition,
    MLBTeam,
    Player,
)
from src.jobs.base import BaseJob


MAX_WORKERS = 6
JITTER_RANGE_S = (0.05, 0.25)

IGNORED_GAME_TYPES = {"S", "A", "I", "E"}


class GameBoxscoreSync(BaseJob):
    def __init__(
        self,
        season_year: Optional[int] = 2025,
        window_start_month: int = 2,
        window_start_day: int = 1,
        window_end_month: int = 12,
        window_end_day: int = 1,
        game_chunk_size: int = 1000,
        boxscore_chunk_size: int = 5000,
        batting_chunk_size: int = 5000,
        rerun_all_boxscores: bool = True,
    ):
        super().__init__()
        self.set_child_instance(self)
        self.season_year = season_year
        self.window_start_month = window_start_month
        self.window_start_day = window_start_day
        self.window_end_month = window_end_month
        self.window_end_day = window_end_day
        self.game_chunk_size = game_chunk_size
        self.boxscore_chunk_size = boxscore_chunk_size
        self.batting_chunk_size = batting_chunk_size
        self.rerun_all_boxscores = rerun_all_boxscores

        self._birth_loc_cache: Dict[Tuple[str, Optional[str], str], int] = {}
        self._player_exists_cache: Set[int] = set()

    def execute(self, db_session):
        season_year, start_date, end_date = self._season_window()
        self.logger.info(
            f"Starting GameBoxscoreSync... season_year={season_year} start={start_date} end={end_date} "
            f"rerun_all_boxscores={self.rerun_all_boxscores}"
        )

        schedule = self._fetch_schedule(start_date, end_date)
        dates = schedule.get("dates") or []
        self.logger.info(f"Schedule dates={len(dates)}")

        games = self._collect_games(dates)
        self.logger.info(f"Collected games={len(games)} (excluded types={sorted(IGNORED_GAME_TYPES)})")

        team_ids = self._collect_team_ids(games)
        self.logger.info(f"Unique team_ids={len(team_ids)}")

        self._upsert_teams(db_session, team_ids, season_year)
        db_session.commit()
        teams_in_db = set(db_session.execute(select(MLBTeam.id)).scalars().all())

        game_rows: List[Dict[str, Any]] = []
        for g in games:
            game_pk = g.get("gamePk")
            if game_pk is None:
                continue

            away_id, home_id = self._extract_home_away_ids(g)
            if away_id is None or home_id is None:
                continue
            if away_id not in teams_in_db or home_id not in teams_in_db:
                continue

            game_type = (g.get("gameType") or "").strip()
            if not game_type:
                continue

            season = int(g.get("season") or season_year)
            game_date = self._parse_dt_utc_naive(g.get("gameDate"))
            status = g.get("status") or {}
            status_code = (status.get("statusCode") or "").strip()

            if game_date is None:
                continue

            game_rows.append(
                {
                    "id": int(game_pk),
                    "game_type": game_type,
                    "season": season,
                    "game_date": game_date,
                    "status_code": status_code,
                    "home_team_id": int(home_id),
                    "away_team_id": int(away_id),
                }
            )

        self._upsert_games(db_session, game_rows, chunk_size=self.game_chunk_size)
        db_session.commit()

        target_game_ids = self._target_game_ids_for_boxscores(db_session, season_year)
        self.logger.info(f"Boxscore target games={len(target_game_ids)}")

        if not target_game_ids:
            self.logger.info("Done. Nothing to sync for mlb_game_boxscores.")
            return

        self._prime_player_exists_cache(db_session)

        # --- REFACTORED: Process Boxscores in Batches to avoid SQL Parameter Limit ---
        per_game_player_ids: Dict[int, Set[int]] = {}
        failed_games = 0
        
        BOXSCORE_BATCH_SIZE = 100
        total_boxscore_games = len(target_game_ids)
        
        for i in range(0, total_boxscore_games, BOXSCORE_BATCH_SIZE):
            batch_game_ids = target_game_ids[i : i + BOXSCORE_BATCH_SIZE]
            self.logger.info(f"Processing Boxscore batch {i} to {i+len(batch_game_ids)} of {total_boxscore_games}")

            boxscore_rows_buffer: List[Dict[str, Any]] = []
            fielding_rows_buffer: List[Dict[str, Any]] = []
            batch_player_ids: Set[int] = set()

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {pool.submit(self._fetch_boxscore_worker, gid): gid for gid in batch_game_ids}
                for fut in as_completed(futures):
                    gid = futures[fut]
                    try:
                        rows_for_game, f_rows_for_game, player_ids_for_game = fut.result()
                    except Exception as e:
                        failed_games += 1
                        self.logger.info(f"[BOXSCORE_FAILED] game_id={gid} err='{e}'")
                        continue

                    if not rows_for_game:
                        self.logger.info(f"[BOXSCORE_EMPTY] game_id={gid}")
                    if not player_ids_for_game:
                        self.logger.info(f"[BOXSCORE_NO_PLAYERS] game_id={gid} rows={len(rows_for_game)}")
                    
                    if rows_for_game:
                        boxscore_rows_buffer.extend(rows_for_game)

                    if f_rows_for_game:
                        fielding_rows_buffer.extend(f_rows_for_game)

                    if player_ids_for_game:
                        batch_player_ids |= player_ids_for_game
                        # Persist for PBP phase
                        per_game_player_ids[int(gid)] = set(player_ids_for_game)

            # Sync Players found in this batch
            if batch_player_ids:
                missing = {pid for pid in batch_player_ids if pid not in self._player_exists_cache}
                if missing:
                    self.logger.info(f"Missing players to upsert in batch={len(missing)}")
                    people = self._fetch_people_bulk(missing)
                    created_p, updated_p, failed_p = self._upsert_people_from_people_payload(db_session, people)
                    db_session.commit()

            # Filter Boxscore Rows (ensure player exists)
            filtered_boxscore_rows: List[Dict[str, Any]] = []
            skipped_missing_player = 0
            for r in boxscore_rows_buffer:
                pid = int(r["player_id"])
                if pid not in self._player_exists_cache:
                    skipped_missing_player += 1
                    continue
                filtered_boxscore_rows.append(r)

            # Upsert Boxscores
            if filtered_boxscore_rows:
                created, updated = self._upsert_game_boxscores(db_session, filtered_boxscore_rows)
            
            # Filter and Upsert Fielding Rows
            filtered_fielding_rows: List[Dict[str, Any]] = []
            for r in fielding_rows_buffer:
                if int(r["player_id"]) in self._player_exists_cache:
                    filtered_fielding_rows.append(r)

            if filtered_fielding_rows:
                self._upsert_fielding_stats(db_session, filtered_fielding_rows)

            db_session.commit()
            self.logger.info(f"Boxscore Batch {i} committed. skipped_players={skipped_missing_player}")

        self.logger.info(
            f"Finished fetching boxscores. games_ok={len(per_game_player_ids)} failed_games={failed_games}"
        )

        missing_boxscore_games = list(
            db_session.execute(
                select(MLBGame.id).where(
                    MLBGame.season == season_year,
                    ~MLBGame.game_type.in_(IGNORED_GAME_TYPES),
                    ~exists(select(1).where(MLBGameBoxscore.game_id == MLBGame.id)),
                ).limit(50)
            ).scalars().all()
        )
        if missing_boxscore_games:
            self.logger.info(f"[BOXSCORE_MISSING_IN_DB] sample_game_ids={missing_boxscore_games}")

        self.logger.info("Starting batting/baserunning/pitching aggregation from playByPlay...")

        batting_agg = MLBPlayByPlayBattingAggregator()
        baserunning_agg = MLBPlayByPlayBaserunningAggregator()
        pitching_agg = MLBPlayByPlayPitchingAggregator()

        batting_rows_buffer: List[Dict[str, Any]] = []
        baserunning_rows_buffer: List[Dict[str, Any]] = []
        pitching_rows_buffer: List[Dict[str, Any]] = []

        failed_pbp = 0
        games_with_pbp = 0

        all_game_ids = list(per_game_player_ids.keys())
        GAME_BATCH_SIZE = 100
        total_games = len(all_game_ids)

        for i in range(0, total_games, GAME_BATCH_SIZE):
            batch_ids = all_game_ids[i : i + GAME_BATCH_SIZE]
            self.logger.info(f"Processing PBP batch {i} to {i+len(batch_ids)} of {total_games}")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
                futures = {pool.submit(self._fetch_playbyplay_worker, gid): gid for gid in batch_ids}
                
                for fut in as_completed(futures):
                    gid = futures[fut]
                    try:
                        payload = fut.result()
                    except Exception as e:
                        failed_pbp += 1
                        self.logger.info(f"[PBP_FAILED] game_id={gid} err='{e}'")
                        continue

                    if not payload:
                        continue

                    allowed = per_game_player_ids.get(int(gid)) or set()
                    if not allowed:
                        self.logger.info(f"[PBP_NO_ALLOWED] game_id={gid}")
                        continue

                    b_rows = batting_agg.build_rows(int(gid), payload) or []
                    kept_b = [r for r in b_rows if int(r["player_id"]) in allowed and int(r["player_id"]) in self._player_exists_cache]
                    batting_rows_buffer.extend(kept_b)

                    br_rows = baserunning_agg.build_rows(int(gid), payload) or []
                    kept_br = [r for r in br_rows if int(r["player_id"]) in allowed and int(r["player_id"]) in self._player_exists_cache]
                    baserunning_rows_buffer.extend(kept_br)

                    p_rows = pitching_agg.build_rows(int(gid), payload) or []
                    kept_p = [r for r in p_rows if int(r["player_id"]) in allowed and int(r["player_id"]) in self._player_exists_cache]
                    pitching_rows_buffer.extend(kept_p)

                    games_with_pbp += 1

            self.logger.info(
                f"[PBP_BATCH_FLUSH] batting={len(batting_rows_buffer)} baserun={len(baserunning_rows_buffer)} "
                f"pitching={len(pitching_rows_buffer)}"
            )

            if batting_rows_buffer:
                self._upsert_batting_stats(db_session, batting_rows_buffer)
                batting_rows_buffer.clear()
            
            if baserunning_rows_buffer:
                self._upsert_baserunning_stats(db_session, baserunning_rows_buffer)
                baserunning_rows_buffer.clear()
                
            if pitching_rows_buffer:
                self._upsert_pitching_stats(db_session, pitching_rows_buffer)
                pitching_rows_buffer.clear()
                
            db_session.commit()
            self.logger.info(f"Batch {i} committed.")
        self.logger.info(
            f"Done. games_with_pbp={games_with_pbp} failed_pbp_games={failed_pbp}"
        )

    def _season_window(self) -> Tuple[int, str, str]:
        today = datetime.date.today()

        if self.season_year is not None:
            season_year = int(self.season_year)
        else:
            anchor = datetime.date(today.year, self.window_end_month, self.window_end_day)
            season_year = today.year if today >= anchor else today.year - 1

        start = datetime.date(season_year, self.window_start_month, self.window_start_day)
        end = datetime.date(season_year + 1, self.window_end_month, self.window_end_day)

        return season_year, start.isoformat(), end.isoformat()

    def _fetch_schedule(self, start_date: str, end_date: str) -> Dict[str, Any]:
        url = "https://statsapi.mlb.com/api/v1/schedule"
        params = {"sportId": 1, "startDate": start_date, "endDate": end_date}
        return self.api_client.get(url, params) or {}

    def _collect_games(self, dates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_pk: Dict[int, Dict[str, Any]] = {}
        for d in dates:
            for g in (d.get("games") or []):
                game_type = (g.get("gameType") or "").strip()
                if game_type in IGNORED_GAME_TYPES:
                    continue
                game_pk = g.get("gamePk")
                if game_pk is None:
                    continue
                try:
                    pk_int = int(game_pk)
                except Exception:
                    continue
                by_pk[pk_int] = g
        return list(by_pk.values())

    def _collect_team_ids(self, games: List[Dict[str, Any]]) -> Set[int]:
        team_ids: Set[int] = set()
        for g in games:
            away_id, home_id = self._extract_home_away_ids(g)
            if away_id is not None:
                team_ids.add(int(away_id))
            if home_id is not None:
                team_ids.add(int(home_id))
        return team_ids

    def _extract_home_away_ids(self, game: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
        teams = game.get("teams") or {}
        away = (teams.get("away") or {}).get("team") or {}
        home = (teams.get("home") or {}).get("team") or {}
        away_id = away.get("id")
        home_id = home.get("id")
        return (
            int(away_id) if away_id is not None else None,
            int(home_id) if home_id is not None else None,
        )

    def _upsert_teams(self, db_session, team_ids: Set[int], season_year: int) -> None:
        if not team_ids:
            return

        existing_ids = set(db_session.execute(select(MLBTeam.id)).scalars().all())

        results: List[Tuple[int, Optional[Dict[str, Any]], Optional[str]]] = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_team_worker, tid, season_year): tid for tid in sorted(team_ids)}
            for fut in as_completed(futures):
                tid = futures[fut]
                try:
                    team_obj = fut.result()
                    results.append((tid, team_obj, None))
                except Exception as e:
                    results.append((tid, None, str(e)))

        created = 0
        updated = 0
        failed = 0

        for tid, team_obj, err in results:
            if err or not team_obj:
                failed += 1
                self.logger.info(f"[TEAM_FETCH_FAILED] team_id={tid} err='{err}'")
                continue

            team = MLBTeam(
                id=int(team_obj["id"]),
                name=str(team_obj.get("name") or ""),
                abbreviation=str(team_obj.get("abbreviation") or ""),
                location_name=str(team_obj.get("locationName") or ""),
                team_name=str(team_obj.get("teamName") or ""),
                active=bool(team_obj.get("active") or False),
            )
            db_session.merge(team)

            if tid in existing_ids:
                updated += 1
            else:
                created += 1
                existing_ids.add(tid)

        db_session.flush()
        self.logger.info(f"Teams upserted. created={created} updated={updated} failed={failed}")

    def _fetch_team_worker(self, team_id: int, season_year: int) -> Dict[str, Any]:
        time.sleep(random.uniform(*JITTER_RANGE_S))
        client = APIClient()
        url = f"https://statsapi.mlb.com/api/v1/teams/{team_id}"
        params = {"season": season_year}
        res = client.get(url, params) or {}
        teams = res.get("teams") or []
        if not teams:
            raise RuntimeError("empty teams response")
        return teams[0]

    def _upsert_games(self, db_session, rows: List[Dict[str, Any]], chunk_size: int = 1000) -> None:
        if not rows:
            return

        cols = MLBGame.__table__.columns
        update_cols = {c.name: insert(MLBGame).excluded[c.name] for c in cols if c.name != "id"}

        total = len(rows)
        for start in range(0, total, chunk_size):
            chunk = rows[start : start + chunk_size]
            stmt = (
                insert(MLBGame)
                .values(chunk)
                .on_conflict_do_update(index_elements=["id"], set_=update_cols)
            )
            db_session.execute(stmt)
            db_session.flush()

    def _target_game_ids_for_boxscores(self, db_session, season_year: int) -> List[int]:
        base = select(MLBGame.id).where(
            MLBGame.season == season_year,
            ~MLBGame.game_type.in_(IGNORED_GAME_TYPES),
        )

        if self.rerun_all_boxscores:
            return list(db_session.execute(base).scalars().all())

        subq = select(MLBGameBoxscore.game_id).where(MLBGameBoxscore.game_id == MLBGame.id).limit(1)
        stmt = base.where(~exists(subq))
        return list(db_session.execute(stmt).scalars().all())

    def _fetch_boxscore_worker(self, game_id: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Set[int]]:
        time.sleep(random.uniform(*JITTER_RANGE_S))
        client = APIClient()
        url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore"
        res = client.get(url, params=None) or {}
        teams = res.get("teams") or {}

        player_ids: Set[int] = set()

        # Ensure unique (game_id, player_id) for boxscores
        boxscore_team_by_pid: Dict[int, int] = {}

        # Ensure unique (game_id, player_id) for fielding rows
        fielding_by_pid: Dict[int, Dict[str, Any]] = {}

        def process_side(side: str) -> None:
            t = teams.get(side) or {}
            team_id_raw = (t.get("team") or {}).get("id")
            if team_id_raw is None:
                return

            team_id = int(team_id_raw)
            players = t.get("players") or {}

            for pdata in players.values():
                person = pdata.get("person") or {}
                pid_raw = person.get("id")
                if pid_raw is None:
                    continue

                pid = int(pid_raw)
                player_ids.add(pid)

                prev_team = boxscore_team_by_pid.get(pid)
                if prev_team is None:
                    boxscore_team_by_pid[pid] = team_id
                elif prev_team != team_id:
                    self.logger.info(
                        f"[BOXSCORE_BOTH_TEAMS] game_id={int(game_id)} player_id={pid} "
                        f"team_prev={prev_team} team_new={team_id}"
                    )
                    boxscore_team_by_pid[pid] = prev_team

                stats = pdata.get("stats") or {}
                f_stats = stats.get("fielding") or {}

                assists = self._safe_int(f_stats.get("assists")) or 0
                put_outs = self._safe_int(f_stats.get("putOuts")) or 0
                errors = self._safe_int(f_stats.get("errors")) or 0
                passed_balls = self._safe_int(f_stats.get("passedBall")) or 0
                pickoffs = self._safe_int(f_stats.get("pickoffs")) or 0

                sb_allowed = self._safe_int(f_stats.get("stolenBases")) or 0
                cs_allowed = self._safe_int(f_stats.get("caughtStealing")) or 0

                chances = self._safe_int(f_stats.get("chances")) or (assists + put_outs + errors)

                has_fielding_stat = (
                    (chances > 0)
                    or (passed_balls > 0)
                    or (pickoffs > 0)
                    or (sb_allowed > 0)
                    or (cs_allowed > 0)
                )

                if not has_fielding_stat:
                    continue

                existing = fielding_by_pid.get(pid)
                if existing is None:
                    fielding_by_pid[pid] = {
                        "game_id": int(game_id),
                        "player_id": pid,
                        "assists": assists,
                        "put_outs": put_outs,
                        "errors": errors,
                        "chances": chances,
                        "passed_balls": passed_balls,
                        "pickoffs": pickoffs,
                        "stolen_bases_allowed": sb_allowed,
                        "caught_stealing": cs_allowed,
                    }
                else:
                    existing["assists"] += assists
                    existing["put_outs"] += put_outs
                    existing["errors"] += errors
                    existing["chances"] += chances
                    existing["passed_balls"] += passed_balls
                    existing["pickoffs"] += pickoffs
                    existing["stolen_bases_allowed"] += sb_allowed
                    existing["caught_stealing"] += cs_allowed

        process_side("home")
        process_side("away")

        boxscore_rows: List[Dict[str, Any]] = [
            {"game_id": int(game_id), "player_id": pid, "team_id": tid}
            for pid, tid in boxscore_team_by_pid.items()
        ]
        fielding_rows: List[Dict[str, Any]] = list(fielding_by_pid.values())

        return boxscore_rows, fielding_rows, player_ids

    def _fetch_playbyplay_worker(self, game_id: int) -> Optional[Dict[str, Any]]:
        time.sleep(random.uniform(*JITTER_RANGE_S))
        client = APIClient()
        url = f"https://statsapi.mlb.com/api/v1/game/{game_id}/playByPlay"
        res = client.get(url, params=None) or {}
        if not res.get("allPlays"):
            return None
        return res

    def _upsert_game_boxscores(self, db_session, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not rows:
            return 0, 0

        stmt = (
            insert(MLBGameBoxscore)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["game_id", "player_id"],
                set_={"team_id": insert(MLBGameBoxscore).excluded["team_id"]},
            )
        )
        db_session.execute(stmt)
        db_session.flush()
        return len(rows), len(rows)

    def _upsert_batting_stats(self, db_session, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        cols = MLBGameBattingStats.__table__.columns
        update_cols = {
            c.name: insert(MLBGameBattingStats).excluded[c.name]
            for c in cols
            if c.name not in {"game_id", "player_id", "split"}
        }

        stmt = (
            insert(MLBGameBattingStats)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["game_id", "player_id", "split"],
                set_=update_cols,
            )
        )
        db_session.execute(stmt)
        db_session.flush()

    def _upsert_baserunning_stats(self, db_session, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        stmt = (
            insert(MLBGameBaserunningStats)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["game_id", "player_id"],
                set_={
                    "sb": insert(MLBGameBaserunningStats).excluded["sb"],
                    "caught_stealing": insert(MLBGameBaserunningStats).excluded["caught_stealing"],
                },
            )
        )
        db_session.execute(stmt)
        db_session.flush()

    def _upsert_fielding_stats(self, db_session, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        stmt = (
            insert(MLBGameFieldingStats)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["game_id", "player_id"],
                set_={
                    "assists": insert(MLBGameFieldingStats).excluded["assists"],
                    "put_outs": insert(MLBGameFieldingStats).excluded["put_outs"],
                    "errors": insert(MLBGameFieldingStats).excluded["errors"],
                    "chances": insert(MLBGameFieldingStats).excluded["chances"],
                    "passed_balls": insert(MLBGameFieldingStats).excluded["passed_balls"],
                    "pickoffs": insert(MLBGameFieldingStats).excluded["pickoffs"],
                    "stolen_bases_allowed": insert(MLBGameFieldingStats).excluded["stolen_bases_allowed"],
                    "caught_stealing": insert(MLBGameFieldingStats).excluded["caught_stealing"],
                },
            )
        )
        db_session.execute(stmt)
        db_session.flush()

    def _upsert_pitching_stats(self, db_session, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return

        cols = MLBGamePitchingStats.__table__.columns
        # Upsert everything except PKs
        update_cols = {
            c.name: insert(MLBGamePitchingStats).excluded[c.name]
            for c in cols
            if c.name not in {"game_id", "player_id", "split"}
        }

        stmt = (
            insert(MLBGamePitchingStats)
            .values(rows)
            .on_conflict_do_update(
                index_elements=["game_id", "player_id", "split"],
                set_=update_cols,
            )
        )
        db_session.execute(stmt)
        db_session.flush()

    def _prime_player_exists_cache(self, db_session) -> None:
        self._player_exists_cache = set(db_session.execute(select(Player.mlb_id)).scalars().all())

    def _fetch_people_bulk(self, mlb_ids: Set[int]) -> Dict[int, Optional[Dict[str, Any]]]:
        out: Dict[int, Optional[Dict[str, Any]]] = {}

        def worker(pid: int) -> Tuple[int, Optional[Dict[str, Any]]]:
            time.sleep(random.uniform(*JITTER_RANGE_S))
            client = APIClient()
            url = f"https://statsapi.mlb.com/api/v1/people/{pid}"
            res = client.get(url, params=None) or {}
            people = res.get("people") or []
            return int(pid), (people[0] if people else None)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(worker, int(pid)): int(pid) for pid in sorted(mlb_ids)}
            for fut in as_completed(futures):
                pid = futures[fut]
                try:
                    k, person = fut.result()
                    out[int(k)] = person
                except Exception:
                    out[int(pid)] = None

        return out

    def _upsert_people_from_people_payload(
        self, db_session, people_by_id: Dict[int, Optional[Dict[str, Any]]]
    ) -> Tuple[int, int, int]:
        created = 0
        updated = 0
        failed = 0

        for mlb_id, person in people_by_id.items():
            if person is None:
                failed += 1
                continue

            pos = person.get("primaryPosition") or {}
            pos_id = self._safe_int(pos.get("code"))
            if pos_id is None:
                failed += 1
                continue

            self._upsert_position(db_session, pos_id, str(pos.get("name") or ""), str(pos.get("abbreviation") or ""))

            birth_loc_id = self._upsert_birth_location(
                db_session,
                city=str(person.get("birthCity") or ""),
                state_province=(str(person.get("birthStateProvince")) if person.get("birthStateProvince") else None),
                country=str(person.get("birthCountry") or ""),
            )

            row = self._player_row_from_person(person)
            if not row:
                failed += 1
                continue

            row["birth_location_id"] = birth_loc_id
            row["position_id"] = int(pos_id)

            existed = int(row["mlb_id"]) in self._player_exists_cache
            self._upsert_player(db_session, row)
            db_session.flush()

            self._player_exists_cache.add(int(row["mlb_id"]))
            if existed:
                updated += 1
            else:
                created += 1

        return created, updated, failed

    def _upsert_position(self, db_session, pos_id: int, name: str, abbr: str) -> None:
        stmt = (
            insert(MLBPosition)
            .values({"id": int(pos_id), "name": name, "abbreviation": abbr})
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "name": insert(MLBPosition).excluded["name"],
                    "abbreviation": insert(MLBPosition).excluded["abbreviation"],
                },
            )
        )
        db_session.execute(stmt)

    def _upsert_birth_location(
        self, db_session, city: str, state_province: Optional[str], country: str
    ) -> Optional[int]:
        city = (city or "").strip()
        country = (country or "").strip()
        state_province = (state_province or "").strip() or None

        if not city or not country:
            return None

        key = (city.lower(), state_province, country.lower())
        if key in self._birth_loc_cache:
            return self._birth_loc_cache[key]

        stmt = select(BirthLocation.id).where(
            BirthLocation.city == city,
            BirthLocation.country == country,
            (BirthLocation.state_province.is_(None) if state_province is None else BirthLocation.state_province == state_province),
        )
        existing = db_session.execute(stmt).scalar_one_or_none()
        if existing is not None:
            self._birth_loc_cache[key] = int(existing)
            return int(existing)

        obj = BirthLocation(city=city, state_province=state_province, country=country)
        db_session.add(obj)
        db_session.flush()

        self._birth_loc_cache[key] = int(obj.id)
        return int(obj.id)

    def _upsert_player(self, db_session, row: Dict[str, Any]) -> None:
        stmt = (
            insert(Player)
            .values(row)
            .on_conflict_do_update(
                index_elements=["mlb_id"],
                set_={
                    "full_name": insert(Player).excluded["full_name"],
                    "first_name": insert(Player).excluded["first_name"],
                    "last_name": insert(Player).excluded["last_name"],
                    "number": insert(Player).excluded["number"],
                    "birth_date": insert(Player).excluded["birth_date"],
                    "current_age": insert(Player).excluded["current_age"],
                    "birth_location_id": insert(Player).excluded["birth_location_id"],
                    "height": insert(Player).excluded["height"],
                    "weight": insert(Player).excluded["weight"],
                    "active": insert(Player).excluded["active"],
                    "current_team_id": insert(Player).excluded["current_team_id"],
                    "position_id": insert(Player).excluded["position_id"],
                    "boxscore_name": insert(Player).excluded["boxscore_name"],
                    "draft_year": insert(Player).excluded["draft_year"],
                    "mlb_debut_date": insert(Player).excluded["mlb_debut_date"],
                    "bat_side_code": insert(Player).excluded["bat_side_code"],
                    "pitch_hand_code": insert(Player).excluded["pitch_hand_code"],
                    "strike_zone_top": insert(Player).excluded["strike_zone_top"],
                    "strike_zone_bottom": insert(Player).excluded["strike_zone_bottom"],
                },
            )
        )
        db_session.execute(stmt)

    def _player_row_from_person(self, p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        mlb_id = self._safe_int(p.get("id"))
        if mlb_id is None:
            return None

        birth_date = self._parse_date(p.get("birthDate"))
        if birth_date is None:
            return None

        current_age = self._safe_int(p.get("currentAge")) or 0
        active = bool(p.get("active") or False)

        number = str(p.get("primaryNumber") or "")
        height = str(p.get("height") or "") if p.get("height") else None
        weight = str(p.get("weight") or "") if p.get("weight") is not None else None

        draft_year = self._safe_int(p.get("draftYear"))
        debut = self._parse_date(p.get("mlbDebutDate"))

        bat_side = p.get("batSide") or {}
        pitch_hand = p.get("pitchHand") or {}

        strike_top = str(p.get("strikeZoneTop") or "")
        strike_bottom = str(p.get("strikeZoneBottom") or "")

        return {
            "mlb_id": mlb_id,
            "full_name": str(p.get("fullName") or ""),
            "first_name": str(p.get("firstName") or ""),
            "last_name": str(p.get("lastName") or ""),
            "number": number,
            "birth_date": birth_date,
            "current_age": current_age,
            "height": height,
            "weight": weight,
            "active": active,
            "current_team_id": None,
            "position_id": 0,
            "boxscore_name": str(p.get("boxscoreName") or ""),
            "draft_year": draft_year,
            "mlb_debut_date": debut,
            "bat_side_code": str((bat_side.get("code") or "")),
            "pitch_hand_code": str((pitch_hand.get("code") or "")),
            "strike_zone_top": strike_top,
            "strike_zone_bottom": strike_bottom,
            "birth_location_id": None,
        }

    def _parse_dt_utc_naive(self, s: Any) -> Optional[datetime.datetime]:
        if not s:
            return None
        try:
            txt = str(s).strip()
            if txt.endswith("Z"):
                txt = txt[:-1] + "+00:00"
            dt = datetime.datetime.fromisoformat(txt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    def _parse_date(self, s: Any) -> Optional[datetime.date]:
        if not s:
            return None
        try:
            return datetime.date.fromisoformat(str(s).strip())
        except Exception:
            return None

    def _safe_int(self, v: Any) -> Optional[int]:
        try:
            if v is None:
                return None
            return int(v)
        except Exception:
            return None