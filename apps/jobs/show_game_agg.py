from typing import Any, Optional
import json
import math

import pyarrow as pa
import pyarrow.parquet as pq

from sqlalchemy import select
from sqlalchemy.orm import Session

from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from apps.jobs.job import Job
from shared.db.models import ShowBallParks, ShowProfile, ShowGameSummary


RECORDS_KEY = "records/records.parquet"
RECORDS_LIMIT_PER_COMBO = 1000
RECORD_FURTHEST_HR = "furthest_homeruns"
RECORD_FURTHEST_HR_PLUS = "furthest_homeruns_plus"
RECORD_HARDEST_HIT = "hardest_hit_balls"


class ShowGameAgg(Job):
    def __init__(self):
        super().__init__()
        self.spaces = SpacesConnector(SpacesConfig.from_env())

    def run(self, db_session: Session) -> None:
        usernames = [u.strip() for u in db_session.scalars(select(ShowProfile.username)) if u and u.strip()]
        user_agg: dict[str, dict[str, Any]] = {
            username: {
                "pas_all": [],
                "batting_box_agg": {},
                "pitching_box_agg": {},
            }
            for username in usernames
        }

        ballpark_elevation_by_id = self._fetch_ballpark_elevations(db_session)
        homerun_candidates: list[dict[str, Any]] = []
        hard_hit_candidates: list[dict[str, Any]] = []

        for game in self._fetch_all_games(db_session):
            bundle = self._load_game_bundle(game.id)
            self._build_facts_for_games(game, bundle)

            pas = bundle.get("plate_appearances", []) or []
            batting_box = bundle.get("batting_boxscores", []) or []
            pitching_box = bundle.get("pitching_boxscores", []) or []

            elevation = ballpark_elevation_by_id.get(getattr(game, "ball_park_id", None))
            self._collect_record_candidates(
                game=game,
                pas=pas,
                elevation=elevation,
                homerun_candidates=homerun_candidates,
                hard_hit_candidates=hard_hit_candidates,
            )

            game_usernames = [
                getattr(game, "home_profile_username", None),
                getattr(game, "away_profile_username", None),
            ]
            for username in game_usernames:
                if not username:
                    continue
                user_bucket = user_agg.setdefault(
                    username,
                    {
                        "pas_all": [],
                        "batting_box_agg": {},
                        "pitching_box_agg": {},
                    },
                )
                user_bucket["pas_all"].extend(pas)
                self._agg_batting(user_bucket["batting_box_agg"], batting_box)
                self._agg_pitching(user_bucket["pitching_box_agg"], pitching_box)

        for username, agg in user_agg.items():
            base_prefix = f"facts/{username}"
            self._put_parquet(f"{base_prefix}/pas.parquet", agg["pas_all"])
            self._put_parquet(f"{base_prefix}/batting_boxscores.parquet", list(agg["batting_box_agg"].values()))
            self._put_parquet(f"{base_prefix}/pitching_boxscores.parquet", list(agg["pitching_box_agg"].values()))

        records_rows = self._build_records_rows(homerun_candidates, hard_hit_candidates)
        self._put_records_parquet(RECORDS_KEY, records_rows)

    def _put_parquet(self, key: str, rows: list[dict[str, Any]]) -> None:
        data = self._parquet_bytes(rows)
        self.spaces.put_bytes(
            key=key,
            data=data,
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _put_records_parquet(self, key: str, rows: list[dict[str, Any]]) -> None:
        data = self._parquet_bytes(rows, schema=self._records_schema())
        self.spaces.put_bytes(
            key=key,
            data=data,
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _parquet_bytes(self, rows: list[dict[str, Any]], schema: Optional[pa.Schema] = None) -> bytes:
        if schema is None:
            table = pa.Table.from_pylist(rows or [])
        else:
            table = pa.Table.from_pylist(rows or [], schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        return sink.getvalue().to_pybytes()

    def _records_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("game_id", pa.string()),
                pa.field("event_id", pa.int64()),
                pa.field("record", pa.string()),
                pa.field("record_rank", pa.int32()),
                pa.field("value", pa.float64()),
                pa.field("batter_mlb_id", pa.int64()),
                pa.field("pitcher_mlb_id", pa.int64()),
                pa.field("hitter_username", pa.string()),
                pa.field("pitcher_username", pa.string()),
                pa.field("difficulty", pa.string()),
            ]
        )

    def _build_facts_for_games(self, game: ShowGameSummary, bundle: dict[str, Any]) -> None:
        pas: list[dict[str, Any]] = bundle.get("plate_appearances", []) or []
        evts: list[dict[str, Any]] = bundle.get("events", []) or []

        evt_by_seq: dict[int, dict[str, Any]] = {}
        for e in evts:
            seq = e.get("seq")
            if isinstance(seq, int):
                evt_by_seq[seq] = e

        shared = {
            "ballpark_id": getattr(game, "ball_park_id", None),
            "weather_degrees": getattr(game, "weather_degrees", None),
            "weather_desc": getattr(game, "weather_description", None),
            "difficulty_id": getattr(game, "difficulty", None),
            "date": getattr(game, "date", None),
            "home_profile_username": getattr(game, "home_profile_username", None),
            "away_profile_username": getattr(game, "away_profile_username", None),
            "home_team_id": getattr(game, "home_name", None),
            "away_team_id": getattr(game, "away_name", None),
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
        game: ShowGameSummary,
        pas: list[dict[str, Any]],
        elevation: Optional[int],
        homerun_candidates: list[dict[str, Any]],
        hard_hit_candidates: list[dict[str, Any]],
    ) -> None:
        game_id = str(getattr(game, "id", "") or "")
        if not game_id:
            return

        elevation_value = self._coerce_float(elevation)
        difficulty = getattr(game, "difficulty", None)

        for pa_row in pas:
            event_id = self._coerce_int(pa_row.get("event_seq"))
            if event_id is None:
                continue

            hitter_username, pitcher_username = self._resolve_hitter_pitcher_usernames(
                game, pa_row.get("is_home_batting")
            )
            common_row = {
                "game_id": game_id,
                "event_id": event_id,
                "batter_mlb_id": self._coerce_int(pa_row.get("batter_mlb_id")),
                "pitcher_mlb_id": self._coerce_int(pa_row.get("pitcher_mlb_id")),
                "hitter_username": hitter_username,
                "pitcher_username": pitcher_username,
                "difficulty": difficulty,
            }

            hr_distance = self._coerce_float(pa_row.get("hr_distance_ft"))
            if pa_row.get("result") == "home_run" and hr_distance is not None:
                homerun_candidates.append(
                    {
                        **common_row,
                        "value": hr_distance,
                        "elevation": elevation_value,
                    }
                )

            exit_vel = self._coerce_float(pa_row.get("exit_vel_mph"))
            if exit_vel is not None:
                hard_hit_candidates.append(
                    {
                        **common_row,
                        "value": exit_vel,
                    }
                )

    def _resolve_hitter_pitcher_usernames(
        self, game: ShowGameSummary, is_home_batting: Any
    ) -> tuple[Optional[str], Optional[str]]:
        home_username = getattr(game, "home_profile_username", None)
        away_username = getattr(game, "away_profile_username", None)
        if is_home_batting is True:
            return home_username, away_username
        if is_home_batting is False:
            return away_username, home_username
        return None, None

    def _build_records_rows(
        self,
        homerun_candidates: list[dict[str, Any]],
        hard_hit_candidates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        slope = self._fit_elevation_slope(homerun_candidates)
        records: list[dict[str, Any]] = []

        for row in homerun_candidates:
            records.append(self._record_row(RECORD_FURTHEST_HR, row, row["value"]))

            elevation = row.get("elevation")
            elevation_num = elevation if isinstance(elevation, (int, float)) else 0.0
            adjusted_value = row["value"] - (slope * float(elevation_num))
            records.append(self._record_row(RECORD_FURTHEST_HR_PLUS, row, adjusted_value))

        for row in hard_hit_candidates:
            records.append(self._record_row(RECORD_HARDEST_HIT, row, row["value"]))

        return self._rank_and_limit_records(records, RECORDS_LIMIT_PER_COMBO)

    def _record_row(self, record_name: str, source_row: dict[str, Any], value: float) -> dict[str, Any]:
        return {
            "game_id": source_row["game_id"],
            "event_id": source_row["event_id"],
            "record": record_name,
            "value": float(value),
            "batter_mlb_id": source_row.get("batter_mlb_id"),
            "pitcher_mlb_id": source_row.get("pitcher_mlb_id"),
            "hitter_username": source_row.get("hitter_username"),
            "pitcher_username": source_row.get("pitcher_username"),
            "difficulty": source_row.get("difficulty"),
        }

    def _rank_and_limit_records(self, rows: list[dict[str, Any]], limit_per_combo: int) -> list[dict[str, Any]]:
        grouped: dict[tuple[str, Optional[str]], list[dict[str, Any]]] = {}
        for row in rows:
            key = (str(row["record"]), row.get("difficulty"))
            grouped.setdefault(key, []).append(row)

        ranked_rows: list[dict[str, Any]] = []
        for key in sorted(grouped.keys(), key=lambda x: (x[0], "" if x[1] is None else str(x[1]))):
            ranked = sorted(grouped[key], key=self._record_sort_key)
            for rank, row in enumerate(ranked[:limit_per_combo], start=1):
                ranked_rows.append(
                    {
                        "game_id": row["game_id"],
                        "event_id": row["event_id"],
                        "record": row["record"],
                        "record_rank": rank,
                        "value": row["value"],
                        "batter_mlb_id": row.get("batter_mlb_id"),
                        "pitcher_mlb_id": row.get("pitcher_mlb_id"),
                        "hitter_username": row.get("hitter_username"),
                        "pitcher_username": row.get("pitcher_username"),
                        "difficulty": row.get("difficulty"),
                    }
                )
        return ranked_rows

    def _record_sort_key(self, row: dict[str, Any]) -> tuple[float, str, int]:
        game_id = str(row.get("game_id") or "")
        event_id = self._coerce_int(row.get("event_id")) or 0
        return (-float(row["value"]), game_id, event_id)

    def _fit_elevation_slope(self, homerun_candidates: list[dict[str, Any]]) -> float:
        points: list[tuple[float, float]] = []
        for row in homerun_candidates:
            elevation = row.get("elevation")
            if not isinstance(elevation, (int, float)):
                continue
            value = self._coerce_float(row.get("value"))
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

    def _fetch_all_games(self, db_session: Session):
        stmt = select(ShowGameSummary).execution_options(yield_per=1000)
        return db_session.scalars(stmt)
