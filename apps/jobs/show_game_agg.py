from typing import Any
import json

import pyarrow as pa
import pyarrow.parquet as pq

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from shared.storage.spaces_connector import SpacesConfig, SpacesConnector
from apps.jobs.job import Job
from shared.db.models import ShowProfile, ShowGameSummary


class ShowGameAgg(Job):
    def __init__(self):
        super().__init__()
        self.spaces = SpacesConnector(SpacesConfig.from_env())

    def run(self, db_session: Session) -> None:
        usernames = [u for u in db_session.scalars(select(ShowProfile.username)) if u]

        for username in usernames:
            username = (username or "").strip()
            if not username:
                continue

            user_games = self._fetch_user_games(db_session, username)

            pas_all: list[dict[str, Any]] = []
            batting_box_agg: dict[int, dict[str, Any]] = {}
            pitching_box_agg: dict[int, dict[str, Any]] = {}

            for game in user_games:
                bundle = self._load_game_bundle(game.id)

                self._build_facts_for_games(game, bundle)
                pas_all.extend(bundle.get("plate_appearances", []) or [])

                self._agg_batting(batting_box_agg, bundle.get("batting_boxscores", []) or [])
                self._agg_pitching(pitching_box_agg, bundle.get("pitching_boxscores", []) or [])

            base_prefix = f"facts/{username}"

            self._put_parquet(f"{base_prefix}/pas.parquet", pas_all)
            self._put_parquet(f"{base_prefix}/batting_boxscores.parquet", list(batting_box_agg.values()))
            self._put_parquet(f"{base_prefix}/pitching_boxscores.parquet", list(pitching_box_agg.values()))

    def _put_parquet(self, key: str, rows: list[dict[str, Any]]) -> None:
        data = self._parquet_bytes(rows)
        self.spaces.put_bytes(
            key=key,
            data=data,
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _parquet_bytes(self, rows: list[dict[str, Any]]) -> bytes:
        table = pa.Table.from_pylist(rows or [])
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        return sink.getvalue().to_pybytes()

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
        pas.sort(key=lambda x: (x.get("event_seq") is None, x.get("event_seq", 0)))

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

    def _load_game_bundle(self, game_id: str) -> dict[str, Any]:
        return {
            "plate_appearances": self._read_jsonl_optional(self._key(game_id, "plate_appearances.jsonl")),
            "events": self._read_jsonl_optional(self._key(game_id, "events.jsonl")),
            "half_inning_summary": self._read_jsonl_optional(self._key(game_id, "half_inning_summary.jsonl")),
            "runner_moves": self._read_jsonl_optional(self._key(game_id, "runner_moves.jsonl")),
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

    def _fetch_user_games(self, db_session: Session, username: str) -> list[ShowGameSummary]:
        stmt = select(ShowGameSummary).where(
            or_(
                ShowGameSummary.home_profile_username == username,
                ShowGameSummary.away_profile_username == username,
            )
        )
        return db_session.scalars(stmt).all()
