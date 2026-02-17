from __future__ import annotations

from typing import Any, Mapping, Optional
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
import json
import math
import os
import random
from datetime import datetime, timezone
from io import BytesIO
from time import perf_counter

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from apps.jobs.job import Job
from shared.db.models import ShowBallParks, ShowProfile, ShowGameSummary


RECORDS_KEY = "records/records.parquet"
RECORDS_HOME_RUNS_KEY = "records/home_runs.parquet"
RECORDS_HARDEST_HITS_KEY = "records/hardest_hits.parquet"
CHECKPOINT_FILENAME = "checkpoint.json"
BUNDLE_FETCH_WORKERS_DEFAULT = 4
BUNDLE_FETCH_MAX_IN_FLIGHT_DEFAULT = 8

# Legacy constants kept for compatibility with older unit tests/importers.
RECORD_FURTHEST_HR = "furthest_homeruns"
RECORD_FURTHEST_HR_PLUS = "furthest_homeruns_plus"
RECORD_HARDEST_HIT = "hardest_hit_balls"


class ShowGameAgg(Job):
    def __init__(self):
        super().__init__()
        self.spaces = SpacesConnector(SpacesConfig.from_env())

    def _game_val(self, game: Mapping[str, Any] | ShowGameSummary, key: str) -> Any:
        if isinstance(game, Mapping):
            return game.get(key)
        return getattr(game, key, None)

    def _env_int(self, name: str, default: int, minimum: int = 1) -> int:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            value = int(raw)
        except ValueError:
            return default
        return max(minimum, value)

    def run(self, db_session: Session) -> None:
        run_started = perf_counter()
        bundle_fetch_workers = self._env_int(
            "SHOW_GAME_AGG_BUNDLE_FETCH_WORKERS",
            BUNDLE_FETCH_WORKERS_DEFAULT,
        )
        bundle_fetch_max_in_flight = self._env_int(
            "SHOW_GAME_AGG_BUNDLE_FETCH_MAX_IN_FLIGHT",
            BUNDLE_FETCH_MAX_IN_FLIGHT_DEFAULT,
        )
        if bundle_fetch_max_in_flight < bundle_fetch_workers:
            bundle_fetch_max_in_flight = bundle_fetch_workers
        self.logger.info(
            "show game agg config bundle_fetch_workers=%s bundle_fetch_max_in_flight=%s",
            bundle_fetch_workers,
            bundle_fetch_max_in_flight,
        )

        t0 = perf_counter()
        usernames = [u.strip() for u in db_session.scalars(select(ShowProfile.username)) if u and u.strip()]
        random.shuffle(usernames)
        load_usernames_s = perf_counter() - t0
        self.logger.info(
            "show game agg timing phase=load_usernames elapsed_s=%.3f usernames=%s",
            load_usernames_s,
            len(usernames),
        )
        if not usernames:
            return

        load_checkpoints_s = 0.0

        t0 = perf_counter()
        ballpark_elevation_by_id = self._fetch_ballpark_elevations(db_session)
        load_ballparks_s = perf_counter() - t0
        self.logger.info(
            "show game agg timing phase=load_ballparks elapsed_s=%.3f parks=%s",
            load_ballparks_s,
            len(ballpark_elevation_by_id),
        )
        users_changed_count = 0
        records_written = False
        total_hr_candidates = 0
        total_hard_candidates = 0
        timings: dict[str, float] = {
            "iterate_games_s": 0.0,
            "load_bundle_s": 0.0,
            "build_facts_s": 0.0,
            "collect_records_s": 0.0,
            "load_user_state_s": 0.0,
            "update_user_state_s": 0.0,
            "write_users_s": 0.0,
            "write_records_s": 0.0,
        }
        counters: dict[str, int] = {
            "games_scanned": 0,
            "games_targeted": 0,
            "target_user_assignments": 0,
            "pas_rows_total": 0,
            "batting_rows_total": 0,
            "pitching_rows_total": 0,
        }

        for user_idx, username in enumerate(usernames, start=1):
            user_started = perf_counter()

            t0 = perf_counter()
            checkpoint = self._read_checkpoint_game_ids(username)
            load_checkpoints_s += perf_counter() - t0

            user_state: Optional[dict[str, Any]] = None
            existing_pas_game_ids: Optional[set[str]] = None
            user_changed = False
            user_hr_candidates: list[dict[str, Any]] = []
            user_hard_candidates: list[dict[str, Any]] = []
            user_games_scanned = 0
            user_games_targeted = 0

            iter_started = perf_counter()
            with ThreadPoolExecutor(max_workers=bundle_fetch_workers) as pool:
                in_flight: dict[Future[tuple[dict[str, Any], float]], tuple[Mapping[str, Any] | ShowGameSummary, str]] = {}

                def process_completed_future(
                    fut: Future[tuple[dict[str, Any], float]],
                    game: Mapping[str, Any] | ShowGameSummary,
                    game_id: str,
                ) -> None:
                    nonlocal user_state, existing_pas_game_ids, user_changed

                    bundle, load_elapsed_s = fut.result()
                    timings["load_bundle_s"] += load_elapsed_s

                    t_user = perf_counter()
                    if user_state is None:
                        t0 = perf_counter()
                        user_state = self._load_user_state(username)
                        existing_pas_game_ids = self._extract_pas_game_ids(user_state["pas_existing"])
                        checkpoint.update(existing_pas_game_ids)
                        timings["load_user_state_s"] += perf_counter() - t0

                    if existing_pas_game_ids is not None and game_id in existing_pas_game_ids:
                        checkpoint.add(game_id)
                        timings["update_user_state_s"] += perf_counter() - t_user
                        return

                    t0 = perf_counter()
                    self._build_facts_for_games(game, bundle)
                    timings["build_facts_s"] += perf_counter() - t0

                    pas = bundle.get("plate_appearances", []) or []
                    batting_box = bundle.get("batting_boxscores", []) or []
                    pitching_box = bundle.get("pitching_boxscores", []) or []
                    counters["pas_rows_total"] += len(pas)
                    counters["batting_rows_total"] += len(batting_box)
                    counters["pitching_rows_total"] += len(pitching_box)

                    elevation = ballpark_elevation_by_id.get(self._coerce_int(self._game_val(game, "ball_park_id")))
                    t0 = perf_counter()
                    self._collect_record_candidates(
                        game=game,
                        pas=pas,
                        elevation=elevation,
                        homerun_candidates=user_hr_candidates,
                        hard_hit_candidates=user_hard_candidates,
                    )
                    timings["collect_records_s"] += perf_counter() - t0

                    user_state["pas_new"].extend(pas)
                    self._agg_batting(user_state["batting_box_agg"], batting_box)
                    self._agg_pitching(user_state["pitching_box_agg"], pitching_box)

                    checkpoint.add(game_id)
                    assert existing_pas_game_ids is not None
                    existing_pas_game_ids.add(game_id)
                    user_changed = True
                    timings["update_user_state_s"] += perf_counter() - t_user

                scheduled_game_ids: set[str] = set()
                for game in self._fetch_all_games(db_session, [username]):
                    counters["games_scanned"] += 1
                    user_games_scanned += 1
                    if user_games_scanned % 10 == 0:
                        self.logger.info(
                            "show game agg progress username=%s user_index=%s/%s user_games_scanned=%s user_games_targeted=%s total_games_scanned=%s in_flight=%s",
                            username,
                            user_idx,
                            len(usernames),
                            user_games_scanned,
                            user_games_targeted,
                            counters["games_scanned"],
                            len(in_flight),
                        )
                    game_id = str(self._game_val(game, "id") or "")
                    if not game_id or game_id in checkpoint or game_id in scheduled_game_ids:
                        continue

                    counters["games_targeted"] += 1
                    counters["target_user_assignments"] += 1
                    user_games_targeted += 1

                    fut = pool.submit(self._load_game_bundle_timed, game_id)
                    in_flight[fut] = (game, game_id)
                    scheduled_game_ids.add(game_id)

                    while len(in_flight) >= bundle_fetch_max_in_flight:
                        done, _ = wait(in_flight.keys(), return_when=FIRST_COMPLETED)
                        for completed in done:
                            done_game, done_game_id = in_flight.pop(completed)
                            process_completed_future(completed, done_game, done_game_id)

                for completed in as_completed(list(in_flight.keys())):
                    done_game, done_game_id = in_flight.pop(completed)
                    process_completed_future(completed, done_game, done_game_id)
            timings["iterate_games_s"] += perf_counter() - iter_started

            if user_changed and user_state is not None:
                write_started = perf_counter()
                base_prefix = f"facts/{username}"
                merged_pas = self._merge_pas_rows(user_state["pas_existing"], user_state["pas_new"])
                self._put_parquet(f"{base_prefix}/pas.parquet", merged_pas)
                self._put_parquet(
                    f"{base_prefix}/batting_boxscores.parquet",
                    list(user_state["batting_box_agg"].values()),
                )
                self._put_parquet(
                    f"{base_prefix}/pitching_boxscores.parquet",
                    list(user_state["pitching_box_agg"].values()),
                )
                self._write_checkpoint_game_ids(username, checkpoint)
                timings["write_users_s"] += perf_counter() - write_started
                users_changed_count += 1

            if user_hr_candidates or user_hard_candidates:
                t0 = perf_counter()
                self._append_and_write_records(user_hr_candidates, user_hard_candidates)
                timings["write_records_s"] += perf_counter() - t0
                records_written = True

            total_hr_candidates += len(user_hr_candidates)
            total_hard_candidates += len(user_hard_candidates)
            self.logger.info(
                "show game agg user summary username=%s user_index=%s/%s user_changed=%s games_scanned=%s games_targeted=%s hr_candidates=%s hard_candidates=%s elapsed_s=%.3f",
                username,
                user_idx,
                len(usernames),
                user_changed,
                user_games_scanned,
                user_games_targeted,
                len(user_hr_candidates),
                len(user_hard_candidates),
                perf_counter() - user_started,
            )

        if not records_written:
            t0 = perf_counter()
            self._append_and_write_records([], [])
            timings["write_records_s"] += perf_counter() - t0

        total_elapsed_s = perf_counter() - run_started
        self.logger.info(
            (
                "show game agg timing summary total_s=%.3f load_usernames_s=%.3f "
                "load_checkpoints_s=%.3f load_ballparks_s=%.3f iterate_games_s=%.3f "
                "load_bundle_s=%.3f build_facts_s=%.3f collect_records_s=%.3f "
                "load_user_state_s=%.3f update_user_state_s=%.3f write_users_s=%.3f "
                "write_records_s=%.3f users=%s users_changed=%s games_scanned=%s "
                "games_targeted=%s target_user_assignments=%s pas_rows_total=%s "
                "batting_rows_total=%s pitching_rows_total=%s hr_candidates=%s hard_candidates=%s"
            ),
            total_elapsed_s,
            load_usernames_s,
            load_checkpoints_s,
            load_ballparks_s,
            timings["iterate_games_s"],
            timings["load_bundle_s"],
            timings["build_facts_s"],
            timings["collect_records_s"],
            timings["load_user_state_s"],
            timings["update_user_state_s"],
            timings["write_users_s"],
            timings["write_records_s"],
            len(usernames),
            users_changed_count,
            counters["games_scanned"],
            counters["games_targeted"],
            counters["target_user_assignments"],
            counters["pas_rows_total"],
            counters["batting_rows_total"],
            counters["pitching_rows_total"],
            total_hr_candidates,
            total_hard_candidates,
        )

    def _append_and_write_records(
        self,
        homerun_candidates: list[dict[str, Any]],
        hard_hit_candidates: list[dict[str, Any]],
    ) -> None:
        t0 = perf_counter()
        existing_hr_rows = self._read_parquet_optional(RECORDS_HOME_RUNS_KEY)
        existing_hard_rows = self._read_parquet_optional(RECORDS_HARDEST_HITS_KEY)
        read_existing_s = perf_counter() - t0

        t0 = perf_counter()
        new_hr_rows = [self._home_run_record_row(row) for row in homerun_candidates]
        new_hard_rows = [self._hard_hit_record_row(row) for row in hard_hit_candidates]

        merged_hr = self._merge_record_rows(existing_hr_rows, new_hr_rows)
        merged_hard = self._merge_record_rows(existing_hard_rows, new_hard_rows)
        merged_hr, removed_hr_event = self._dedupe_rows_by_key(
            merged_hr,
            self._record_event_key,
        )
        merged_hard, removed_hard_event = self._dedupe_rows_by_key(
            merged_hard,
            self._record_event_key,
        )
        merged_hr, removed_hr_business = self._dedupe_rows_by_key(
            merged_hr,
            self._home_run_business_key,
        )
        merged_hard, removed_hard_business = self._dedupe_rows_by_key(
            merged_hard,
            self._hard_hit_business_key,
        )
        duplicate_hr_after_scan = self._count_duplicate_rows_by_key(merged_hr, self._home_run_business_key)
        duplicate_hard_after_scan = self._count_duplicate_rows_by_key(merged_hard, self._hard_hit_business_key)
        merge_rows_s = perf_counter() - t0

        write_hr_s = 0.0
        if merged_hr:
            t0 = perf_counter()
            slope = self._fit_elevation_slope(merged_hr)
            for row in merged_hr:
                dist = self._coerce_float(row.get("distance_ft"))
                elevation = self._coerce_float(row.get("elevation")) or 0.0
                row["distance_plus_ft"] = None if dist is None else float(dist - (slope * elevation))
            self._assign_ranks(
                merged_hr,
                value_field="distance_ft",
                global_rank_field="rank",
                difficulty_rank_field="difficulty_rank",
            )
            self._assign_ranks(
                merged_hr,
                value_field="distance_plus_ft",
                global_rank_field="rank_plus",
                difficulty_rank_field="difficulty_rank_plus",
            )
            self._put_records_parquet(
                RECORDS_HOME_RUNS_KEY,
                merged_hr,
                schema=self._home_runs_records_schema(),
            )
            write_hr_s = perf_counter() - t0

        write_hard_s = 0.0
        if merged_hard:
            t0 = perf_counter()
            self._assign_ranks(
                merged_hard,
                value_field="exit_vel_mph",
                global_rank_field="rank",
                difficulty_rank_field="difficulty_rank",
            )
            self._put_records_parquet(
                RECORDS_HARDEST_HITS_KEY,
                merged_hard,
                schema=self._hard_hits_records_schema(),
            )
            write_hard_s = perf_counter() - t0

        self.logger.info(
            (
                "show game agg timing phase=records elapsed_read_existing_s=%.3f "
                "elapsed_merge_rows_s=%.3f elapsed_write_hr_s=%.3f elapsed_write_hard_s=%.3f "
                "existing_hr=%s new_hr=%s merged_hr=%s existing_hard=%s new_hard=%s merged_hard=%s "
                "removed_hr_event=%s removed_hard_event=%s removed_hr_business=%s removed_hard_business=%s "
                "duplicate_hr_after_scan=%s duplicate_hard_after_scan=%s"
            ),
            read_existing_s,
            merge_rows_s,
            write_hr_s,
            write_hard_s,
            len(existing_hr_rows),
            len(new_hr_rows),
            len(merged_hr),
            len(existing_hard_rows),
            len(new_hard_rows),
            len(merged_hard),
            removed_hr_event,
            removed_hard_event,
            removed_hr_business,
            removed_hard_business,
            duplicate_hr_after_scan,
            duplicate_hard_after_scan,
        )
        if duplicate_hr_after_scan > 0 or duplicate_hard_after_scan > 0:
            self.logger.warning(
                "show game agg duplicate scan found residual duplicates hr=%s hard=%s",
                duplicate_hr_after_scan,
                duplicate_hard_after_scan,
            )

    def _home_run_record_row(self, source_row: dict[str, Any]) -> dict[str, Any]:
        return {
            "game_id": source_row.get("game_id"),
            "event_id": self._coerce_int(source_row.get("event_id")),
            "date": source_row.get("date"),
            "difficulty": source_row.get("difficulty"),
            "home_profile_username": source_row.get("home_profile_username"),
            "away_profile_username": source_row.get("away_profile_username"),
            "hitter_username": source_row.get("hitter_username"),
            "pitcher_username": source_row.get("pitcher_username"),
            "batter_mlb_id": self._coerce_int(source_row.get("batter_mlb_id")),
            "pitcher_mlb_id": self._coerce_int(source_row.get("pitcher_mlb_id")),
            "is_home_batting": source_row.get("is_home_batting"),
            "elevation": self._coerce_float(source_row.get("elevation")),
            "distance_ft": self._coerce_float(source_row.get("distance_ft")),
            "distance_plus_ft": None,
            "rank": None,
            "difficulty_rank": None,
            "rank_plus": None,
            "difficulty_rank_plus": None,
        }

    def _hard_hit_record_row(self, source_row: dict[str, Any]) -> dict[str, Any]:
        return {
            "game_id": source_row.get("game_id"),
            "event_id": self._coerce_int(source_row.get("event_id")),
            "date": source_row.get("date"),
            "difficulty": source_row.get("difficulty"),
            "home_profile_username": source_row.get("home_profile_username"),
            "away_profile_username": source_row.get("away_profile_username"),
            "hitter_username": source_row.get("hitter_username"),
            "pitcher_username": source_row.get("pitcher_username"),
            "batter_mlb_id": self._coerce_int(source_row.get("batter_mlb_id")),
            "pitcher_mlb_id": self._coerce_int(source_row.get("pitcher_mlb_id")),
            "is_home_batting": source_row.get("is_home_batting"),
            "exit_vel_mph": self._coerce_float(source_row.get("exit_vel_mph")),
            "rank": None,
            "difficulty_rank": None,
        }

    def _merge_record_rows(
        self,
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        combined_rows = [dict(r) for r in existing_rows] + [dict(r) for r in new_rows]
        if not combined_rows:
            return []

        merged = pd.DataFrame(combined_rows)

        if "game_id" not in merged.columns:
            merged["game_id"] = ""
        if "event_id" not in merged.columns:
            merged["event_id"] = -1

        merged["_game_id"] = merged["game_id"].fillna("").astype(str)
        merged["_event_id"] = pd.to_numeric(merged["event_id"], errors="coerce").fillna(-1).astype("int64")
        merged = merged.drop_duplicates(subset=["_game_id", "_event_id"], keep="first")
        merged = merged.drop(columns=["_game_id", "_event_id"])
        return merged.to_dict(orient="records")

    def _dedupe_rows_by_key(
        self,
        rows: list[dict[str, Any]],
        key_builder: Any,
    ) -> tuple[list[dict[str, Any]], int]:
        seen: set[Any] = set()
        deduped: list[dict[str, Any]] = []
        removed = 0
        for row in rows:
            key = key_builder(row)
            if key in seen:
                removed += 1
                continue
            seen.add(key)
            deduped.append(row)
        return deduped, removed

    def _count_duplicate_rows_by_key(
        self,
        rows: list[dict[str, Any]],
        key_builder: Any,
    ) -> int:
        seen: set[Any] = set()
        duplicates = 0
        for row in rows:
            key = key_builder(row)
            if key in seen:
                duplicates += 1
            else:
                seen.add(key)
        return duplicates

    def _home_run_business_key(self, row: dict[str, Any]) -> tuple[str, str, Optional[float], Optional[int]]:
        return (
            str(row.get("game_id") or "").strip(),
            str(row.get("hitter_username") or "").strip().lower(),
            self._rounded_float(row.get("distance_ft")),
            self._coerce_int(row.get("batter_mlb_id")),
        )

    def _hard_hit_business_key(self, row: dict[str, Any]) -> tuple[str, str, Optional[float], Optional[int]]:
        return (
            str(row.get("game_id") or "").strip(),
            str(row.get("hitter_username") or "").strip().lower(),
            self._rounded_float(row.get("exit_vel_mph")),
            self._coerce_int(row.get("batter_mlb_id")),
        )

    def _record_event_key(self, row: dict[str, Any]) -> tuple[str, Optional[int]]:
        return (
            str(row.get("game_id") or "").strip(),
            self._coerce_int(row.get("event_id")),
        )

    def _rounded_float(self, value: Any, digits: int = 3) -> Optional[float]:
        coerced = self._coerce_float(value)
        if coerced is None:
            return None
        return round(coerced, digits)

    def _assign_ranks(
        self,
        rows: list[dict[str, Any]],
        *,
        value_field: str,
        global_rank_field: str,
        difficulty_rank_field: str,
    ) -> None:
        if not rows:
            return

        df = pd.DataFrame(rows)
        if "game_id" not in df.columns:
            df["game_id"] = ""
        if "event_id" not in df.columns:
            df["event_id"] = 0
        if "difficulty" not in df.columns:
            df["difficulty"] = ""

        df["_value"] = pd.to_numeric(df.get(value_field), errors="coerce")
        df["_value"] = df["_value"].fillna(float("-inf"))
        df["_game_id"] = df["game_id"].fillna("").astype(str)
        df["_event_id"] = pd.to_numeric(df["event_id"], errors="coerce").fillna(0).astype("int64")
        df["_difficulty"] = df["difficulty"].fillna("").astype(str)

        sorted_df = df.sort_values(
            by=["_value", "_game_id", "_event_id"],
            ascending=[False, True, True],
            kind="mergesort",
        )
        sorted_df[global_rank_field] = pd.Series(range(1, len(sorted_df) + 1), index=sorted_df.index)
        sorted_df[difficulty_rank_field] = (
            sorted_df.groupby("_difficulty", sort=False).cumcount().astype("int64") + 1
        )

        rank_lookup = sorted_df.set_index(["_game_id", "_event_id"])[[global_rank_field, difficulty_rank_field]]
        df = df.set_index(["_game_id", "_event_id"])
        df[global_rank_field] = rank_lookup[global_rank_field]
        df[difficulty_rank_field] = rank_lookup[difficulty_rank_field]
        df = df.reset_index(drop=True)

        df = df.drop(columns=["_value", "_difficulty"], errors="ignore")
        rows[:] = df.to_dict(orient="records")

    def _load_user_state(self, username: str) -> dict[str, Any]:
        base_prefix = f"facts/{username}"
        existing_pas = self._read_parquet_optional(f"{base_prefix}/pas.parquet")
        existing_batting_rows = self._read_parquet_optional(f"{base_prefix}/batting_boxscores.parquet")
        existing_pitching_rows = self._read_parquet_optional(f"{base_prefix}/pitching_boxscores.parquet")

        return {
            "pas_existing": existing_pas,
            "pas_new": [],
            "batting_box_agg": self._index_agg_rows(existing_batting_rows),
            "pitching_box_agg": self._index_agg_rows(existing_pitching_rows),
        }

    def _index_agg_rows(self, rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        out: dict[int, dict[str, Any]] = {}
        for row in rows:
            player_id = self._coerce_int(row.get("mlb_id"))
            if player_id is None:
                continue
            out[player_id] = dict(row)
        return out

    def _merge_pas_rows(
        self,
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        combined_rows = [dict(r) for r in existing_rows] + [dict(r) for r in new_rows]
        if not combined_rows:
            return []

        merged = pd.DataFrame(combined_rows)

        for col, default in (
            ("game_id", ""),
            ("event_seq", -1),
            ("batter_mlb_id", -1),
            ("pitcher_mlb_id", -1),
            ("result", ""),
        ):
            if col not in merged.columns:
                merged[col] = default

        merged["_game_id"] = merged["game_id"].fillna("").astype(str)
        merged["_event_seq"] = pd.to_numeric(merged["event_seq"], errors="coerce").fillna(-1).astype("int64")
        merged["_batter_mlb_id"] = (
            pd.to_numeric(merged["batter_mlb_id"], errors="coerce").fillna(-1).astype("int64")
        )
        merged["_pitcher_mlb_id"] = (
            pd.to_numeric(merged["pitcher_mlb_id"], errors="coerce").fillna(-1).astype("int64")
        )
        merged["_result"] = merged["result"].fillna("").astype(str)

        merged = merged.drop_duplicates(
            subset=["_game_id", "_event_seq", "_batter_mlb_id", "_pitcher_mlb_id", "_result"],
            keep="first",
        )
        merged = merged.drop(
            columns=["_game_id", "_event_seq", "_batter_mlb_id", "_pitcher_mlb_id", "_result"],
            errors="ignore",
        )
        return merged.to_dict(orient="records")

    def _extract_pas_game_ids(self, rows: list[dict[str, Any]]) -> set[str]:
        out: set[str] = set()
        for row in rows:
            game_id = str(row.get("game_id") or "").strip()
            if game_id:
                out.add(game_id)
        return out

    def _checkpoint_key(self, username: str) -> str:
        return f"facts/{username}/{CHECKPOINT_FILENAME}"

    def _read_checkpoint_game_ids(self, username: str) -> set[str]:
        key = self._checkpoint_key(username)
        try:
            payload = self.spaces.get_json(key)
        except Exception:
            return set()
        if not isinstance(payload, dict):
            return set()
        game_ids = payload.get("aggregated_game_ids")
        if not isinstance(game_ids, list):
            return set()
        return {str(gid).strip() for gid in game_ids if str(gid).strip()}

    def _write_checkpoint_game_ids(self, username: str, game_ids: set[str]) -> None:
        payload = {
            "version": 1,
            "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "aggregated_game_ids": sorted(game_ids),
        }
        self.spaces.put_json(
            key=self._checkpoint_key(username),
            obj=payload,
            cache_control="no-cache",
        )

    def _put_parquet(self, key: str, rows: list[dict[str, Any]]) -> None:
        data = self._parquet_bytes(rows)
        self.spaces.put_bytes(
            key=key,
            data=data,
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _put_records_parquet(
        self,
        key: str,
        rows: list[dict[str, Any]],
        schema: Optional[pa.Schema] = None,
    ) -> None:
        normalized_rows = self._normalize_rows_for_schema(rows, schema) if schema is not None else rows
        data = self._parquet_bytes(normalized_rows, schema=schema)
        self.spaces.put_bytes(
            key=key,
            data=data,
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _normalize_rows_for_schema(self, rows: list[dict[str, Any]], schema: pa.Schema) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for row in rows:
            normalized = dict(row)
            for field in schema:
                value = normalized.get(field.name)
                if pa.types.is_integer(field.type):
                    normalized[field.name] = self._coerce_int(value)
                elif pa.types.is_floating(field.type):
                    normalized[field.name] = self._coerce_float(value)
                elif pa.types.is_boolean(field.type):
                    normalized[field.name] = value if isinstance(value, bool) else None
                elif pa.types.is_string(field.type):
                    normalized[field.name] = None if value is None else str(value)
            out.append(normalized)
        return out

    def _parquet_bytes(self, rows: list[dict[str, Any]], schema: Optional[pa.Schema] = None) -> bytes:
        if schema is None:
            table = pa.Table.from_pylist(rows or [])
        else:
            table = pa.Table.from_pylist(rows or [], schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        return sink.getvalue().to_pybytes()

    def _read_parquet_optional(self, key: str) -> list[dict[str, Any]]:
        try:
            raw = self.spaces.get_bytes(key)
        except Exception:
            return []
        if not raw:
            return []
        try:
            table = pq.read_table(BytesIO(raw))
            return table.to_pylist()
        except Exception:
            return []

    def _home_runs_records_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("game_id", pa.string()),
                pa.field("event_id", pa.int64()),
                pa.field("date", pa.string()),
                pa.field("difficulty", pa.string()),
                pa.field("home_profile_username", pa.string()),
                pa.field("away_profile_username", pa.string()),
                pa.field("hitter_username", pa.string()),
                pa.field("pitcher_username", pa.string()),
                pa.field("batter_mlb_id", pa.int64()),
                pa.field("pitcher_mlb_id", pa.int64()),
                pa.field("is_home_batting", pa.bool_()),
                pa.field("elevation", pa.float64()),
                pa.field("distance_ft", pa.float64()),
                pa.field("distance_plus_ft", pa.float64()),
                pa.field("rank", pa.int64()),
                pa.field("difficulty_rank", pa.int64()),
                pa.field("rank_plus", pa.int64()),
                pa.field("difficulty_rank_plus", pa.int64()),
            ]
        )

    def _hard_hits_records_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("game_id", pa.string()),
                pa.field("event_id", pa.int64()),
                pa.field("date", pa.string()),
                pa.field("difficulty", pa.string()),
                pa.field("home_profile_username", pa.string()),
                pa.field("away_profile_username", pa.string()),
                pa.field("hitter_username", pa.string()),
                pa.field("pitcher_username", pa.string()),
                pa.field("batter_mlb_id", pa.int64()),
                pa.field("pitcher_mlb_id", pa.int64()),
                pa.field("is_home_batting", pa.bool_()),
                pa.field("exit_vel_mph", pa.float64()),
                pa.field("rank", pa.int64()),
                pa.field("difficulty_rank", pa.int64()),
            ]
        )

    def _build_facts_for_games(self, game: Mapping[str, Any] | ShowGameSummary, bundle: dict[str, Any]) -> None:
        pas: list[dict[str, Any]] = bundle.get("plate_appearances", []) or []
        evts: list[dict[str, Any]] = bundle.get("events", []) or []

        evt_by_seq: dict[int, dict[str, Any]] = {}
        for e in evts:
            seq = e.get("seq")
            if isinstance(seq, int):
                evt_by_seq[seq] = e

        shared = {
            "ballpark_id": self._game_val(game, "ball_park_id"),
            "weather_degrees": self._game_val(game, "weather_degrees"),
            "weather_desc": self._game_val(game, "weather_description"),
            "difficulty_id": self._game_val(game, "difficulty"),
            "date": self._game_val(game, "date"),
            "home_profile_username": self._game_val(game, "home_profile_username"),
            "away_profile_username": self._game_val(game, "away_profile_username"),
            "home_team_id": self._game_val(game, "home_name"),
            "away_team_id": self._game_val(game, "away_name"),
        }

        for pa_row in pas:
            pa_row.update(shared)

            event_seq = pa_row.get("event_seq")
            evt = evt_by_seq.get(event_seq) if isinstance(event_seq, int) else None
            if not evt:
                continue

            pa_row.update(
                {
                    "inning": evt.get("inning"),
                    "is_home_batting": evt.get("is_home_batting"),
                    "outs_before": evt.get("outs_before"),
                    "home_score_before": evt.get("home_score_before"),
                    "away_score_before": evt.get("away_score_before"),
                    "runner_on_first": evt.get("pre_on_1b"),
                    "runner_on_second": evt.get("pre_on_2b"),
                    "runner_on_third": evt.get("pre_on_3b"),
                }
            )

        self._apply_pitcher_hitter_tracking(pas)

    def _apply_pitcher_hitter_tracking(self, pas: list[dict[str, Any]]) -> None:
        if not self._is_pas_sorted_by_event_seq(pas):
            pas.sort(key=self._pa_event_seq_sort_key)

        state = {
            "away": {"pitcher_seen": set(), "pitcher_counts": {}, "hitter_counts": {}},
            "home": {"pitcher_seen": set(), "pitcher_counts": {}, "hitter_counts": {}},
        }

        for pa_row in pas:
            is_home_batting = pa_row.get("is_home_batting")
            side = "home" if is_home_batting else "away" if is_home_batting is not None else None
            if side is None:
                continue

            pitcher_id = pa_row.get("pitcher_mlb_id")
            batter_id = pa_row.get("batter_mlb_id")

            s = state[side]

            if pitcher_id is not None:
                s["pitcher_seen"].add(pitcher_id)
                s["pitcher_counts"][pitcher_id] = s["pitcher_counts"].get(pitcher_id, 0) + 1
                pa_row["num_pitchers"] = len(s["pitcher_seen"])
                pa_row["times_seen_pitcher"] = s["pitcher_counts"][pitcher_id]
            else:
                pa_row["num_pitchers"] = len(s["pitcher_seen"])
                pa_row["times_seen_pitcher"] = None

            if batter_id is not None:
                s["hitter_counts"][batter_id] = s["hitter_counts"].get(batter_id, 0) + 1
                pa_row["num_abs_with_hitter"] = s["hitter_counts"][batter_id]
            else:
                pa_row["num_abs_with_hitter"] = None

    def _pa_event_seq_sort_key(self, pa_row: dict[str, Any]) -> tuple[int, int]:
        event_seq = self._coerce_int(pa_row.get("event_seq"))
        if event_seq is None:
            return (1, 0)
        return (0, event_seq)

    def _is_pas_sorted_by_event_seq(self, pas: list[dict[str, Any]]) -> bool:
        if len(pas) < 2:
            return True
        prev = self._pa_event_seq_sort_key(pas[0])
        for row in pas[1:]:
            cur = self._pa_event_seq_sort_key(row)
            if cur < prev:
                return False
            prev = cur
        return True

    def _agg_batting(self, batting_box_agg: dict[int, dict[str, Any]], boxscores: list[dict[str, Any]]) -> None:
        skip = {
            "game_id",
            "is_home",
            "appearance_idx",
            "replaced_apperance_idx",
            "replaced_appearance_idx",
            "innings",
            "pos",
        }

        id_key = "mlb_id"
        name_key = "player_name_raw"

        for bs in boxscores:
            player_id = bs.get(id_key)
            if player_id is None:
                continue

            if player_id not in batting_box_agg:
                agg: dict[str, Any] = {id_key: player_id}
                if name_key in bs:
                    agg[name_key] = bs[name_key]
                for k, v in bs.items():
                    if k in skip or k in (id_key, name_key):
                        continue
                    if isinstance(v, (int, float)):
                        agg[k] = v
                batting_box_agg[player_id] = agg
                continue

            agg = batting_box_agg[player_id]
            if name_key in bs and name_key not in agg:
                agg[name_key] = bs[name_key]

            for k, v in bs.items():
                if k in skip or k in (id_key, name_key):
                    continue
                if not isinstance(v, (int, float)):
                    continue
                agg[k] = agg.get(k, 0) + v

    def _agg_pitching(self, pitching_box_agg: dict[int, dict[str, Any]], boxscores: list[dict[str, Any]]) -> None:
        skip = {"game_id", "is_home", "appearance_idx", "ip_raw", "era"}
        id_key = "mlb_id"
        name_key = "player_name_raw"

        for bs in boxscores:
            player_id = bs.get(id_key)
            if player_id is None:
                continue

            if player_id not in pitching_box_agg:
                agg: dict[str, Any] = {id_key: player_id}
                if name_key in bs:
                    agg[name_key] = bs[name_key]
                for k, v in bs.items():
                    if k in skip or k in (id_key, name_key):
                        continue
                    if isinstance(v, (int, float)):
                        agg[k] = v
                pitching_box_agg[player_id] = agg
                continue

            agg = pitching_box_agg[player_id]
            if name_key in bs and name_key not in agg:
                agg[name_key] = bs[name_key]

            for k, v in bs.items():
                if k in skip or k in (id_key, name_key):
                    continue
                if not isinstance(v, (int, float)):
                    continue
                agg[k] = agg.get(k, 0) + v

    def _fetch_ballpark_elevations(self, db_session: Session) -> dict[int, Optional[int]]:
        rows = db_session.execute(select(ShowBallParks.id, ShowBallParks.elevation)).all()
        out: dict[int, Optional[int]] = {}
        for park_id, elevation in rows:
            park_id_int = self._coerce_int(park_id)
            if park_id_int is None:
                continue
            out[park_id_int] = self._coerce_int(elevation)
        return out

    def _collect_record_candidates(
        self,
        game: Mapping[str, Any] | ShowGameSummary,
        pas: list[dict[str, Any]],
        elevation: Optional[int],
        homerun_candidates: list[dict[str, Any]],
        hard_hit_candidates: list[dict[str, Any]],
    ) -> None:
        game_id = str(self._game_val(game, "id") or "")
        if not game_id:
            return

        elevation_value = self._coerce_float(elevation)
        difficulty = self._game_val(game, "difficulty")
        date_value = self._format_date(self._game_val(game, "date"))
        home_username = self._game_val(game, "home_profile_username")
        away_username = self._game_val(game, "away_profile_username")

        for pa_row in pas:
            event_id = self._coerce_int(pa_row.get("event_seq"))
            if event_id is None:
                continue

            is_home_batting = pa_row.get("is_home_batting")
            hitter_username, pitcher_username = self._resolve_hitter_pitcher_usernames(game, is_home_batting)
            common_row = {
                "game_id": game_id,
                "event_id": event_id,
                "date": date_value,
                "difficulty": difficulty,
                "home_profile_username": home_username,
                "away_profile_username": away_username,
                "batter_mlb_id": self._coerce_int(pa_row.get("batter_mlb_id")),
                "pitcher_mlb_id": self._coerce_int(pa_row.get("pitcher_mlb_id")),
                "hitter_username": hitter_username,
                "pitcher_username": pitcher_username,
                "is_home_batting": is_home_batting,
            }

            hr_distance = self._coerce_float(pa_row.get("hr_distance_ft"))
            if pa_row.get("result") == "home_run" and hr_distance is not None:
                homerun_candidates.append(
                    {
                        **common_row,
                        "distance_ft": hr_distance,
                        "elevation": elevation_value,
                    }
                )

            exit_vel = self._coerce_float(pa_row.get("exit_vel_mph"))
            if exit_vel is not None:
                hard_hit_candidates.append(
                    {
                        **common_row,
                        "exit_vel_mph": exit_vel,
                    }
                )

    def _format_date(self, dt: Any) -> Optional[str]:
        if dt is None:
            return None
        if isinstance(dt, datetime):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
            return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return str(dt)

    def _resolve_hitter_pitcher_usernames(
        self, game: Mapping[str, Any] | ShowGameSummary, is_home_batting: Any
    ) -> tuple[Optional[str], Optional[str]]:
        home_username = self._game_val(game, "home_profile_username")
        away_username = self._game_val(game, "away_profile_username")
        if is_home_batting is True:
            return home_username, away_username
        if is_home_batting is False:
            return away_username, home_username
        return None, None

    def _fit_elevation_slope(self, homerun_rows: list[dict[str, Any]]) -> float:
        points: list[tuple[float, float]] = []
        for row in homerun_rows:
            elevation = row.get("elevation")
            if not isinstance(elevation, (int, float)):
                continue
            value = self._coerce_float(row.get("distance_ft"))
            if value is None:
                continue
            if not math.isfinite(float(elevation)) or not math.isfinite(value):
                continue
            points.append((float(elevation), value))

        if len(points) < 2:
            return 0.0

        n = float(len(points))
        sum_x = sum(x for x, _ in points)
        sum_y = sum(y for _, y in points)
        sum_xx = sum(x * x for x, _ in points)
        sum_xy = sum(x * y for x, y in points)

        denominator = (n * sum_xx) - (sum_x * sum_x)
        if abs(denominator) < 1e-12:
            return 0.0

        slope = ((n * sum_xy) - (sum_x * sum_y)) / denominator
        if not math.isfinite(slope):
            return 0.0
        return slope

    def _coerce_int(self, value: Any) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if not math.isfinite(value):
                return None
            if not value.is_integer():
                return None
            return int(value)
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return None

    def _coerce_float(self, value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            out = float(value)
            return out if math.isfinite(out) else None
        try:
            out = float(str(value))
            return out if math.isfinite(out) else None
        except (TypeError, ValueError):
            return None

    def _load_game_bundle(self, game_id: str) -> dict[str, Any]:
        return {
            "plate_appearances": self._read_jsonl_optional(self._key(game_id, "plate_appearances.jsonl")),
            "events": self._read_jsonl_optional(self._key(game_id, "events.jsonl")),
            "batting_boxscores": self._read_jsonl_optional(self._key(game_id, "batter_boxscores.jsonl")),
            "pitching_boxscores": self._read_jsonl_optional(self._key(game_id, "pitcher_boxscores.jsonl")),
        }

    def _load_game_bundle_timed(self, game_id: str) -> tuple[dict[str, Any], float]:
        started = perf_counter()
        return self._load_game_bundle(game_id), perf_counter() - started

    def _key(self, game_id: str, filename: str) -> str:
        return f"games/{game_id}/{filename}"

    def _read_jsonl_optional(self, key: str) -> list[dict[str, Any]]:
        try:
            raw = self.spaces.get_bytes(key)
        except Exception:
            return []
        if not raw:
            return []
        out: list[dict[str, Any]] = []
        for line in raw.splitlines():
            if line:
                out.append(json.loads(line))
        return out

    def _fetch_all_games(self, db_session: Session, usernames: list[str]):
        username_filter = or_(
            ShowGameSummary.home_profile_username.in_(usernames),
            ShowGameSummary.away_profile_username.in_(usernames),
        )
        stmt = (
            select(
                ShowGameSummary.id,
                ShowGameSummary.ball_park_id,
                ShowGameSummary.weather_degrees,
                ShowGameSummary.weather_description,
                ShowGameSummary.difficulty,
                ShowGameSummary.date,
                ShowGameSummary.home_profile_username,
                ShowGameSummary.away_profile_username,
                ShowGameSummary.home_name,
                ShowGameSummary.away_name,
            )
            .where(username_filter)
            .execution_options(yield_per=1000, stream_results=True)
        )
        return db_session.execute(stmt).mappings()
