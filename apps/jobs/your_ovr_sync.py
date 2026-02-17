from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
import json
import math
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Optional
import zlib

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from shared.db.models import ShowProfile
from shared.storage.spaces_connector import SpacesConfig, SpacesConnector


PAS_COLUMNS: tuple[str, ...] = (
    "result",
    "is_strikeout",
    "is_sac_fly",
    "is_sac_bunt",
    "runs_scored",
    "rbi",
    "is_double_play",
    "is_out",
    "is_home_batting",
    "batter_mlb_id",
    "pitcher_mlb_id",
    "home_profile_username",
    "away_profile_username",
    "home_team_id",
    "away_team_id",
    "home_name",
    "away_name",
)

_HR_RESULTS = {"homerun", "home_run", "home run", "home-run", "hr"}
_WALK_RESULTS = {"walk", "intentional walk", "intentional_walk", "ibb", "bb"}
_HBP_RESULTS = {"hit_by_pitch", "hit by pitch", "hbp"}

_WOBA_WEIGHTS = {
    "bb": 0.69,
    "hbp": 0.72,
    "single": 0.88,
    "double": 1.247,
    "triple": 1.578,
    "hr": 2.031,
}

FACTS_KEY_PATTERN = re.compile(r"^facts/([^/]+)/pas\.parquet$")
LEGACY_FACTS_KEY_PATTERN = re.compile(r"^di-storage/facts/([^/]+)/pas\.parquet$")

GLOBAL_YOUR_OVR_KEY = "facts/your_ovr_all.parquet"


class YourOvrSync(Job):
    def __init__(
        self,
        *,
        facts_scan_limit: Optional[int] = None,
        min_pa_per_mlb_id: Optional[int] = None,
        weight_smoothing_pa: Optional[float] = None,
    ):
        super().__init__()
        self.spaces = SpacesConnector(SpacesConfig.from_env())
        self.facts_scan_limit = max(1000, int(facts_scan_limit or os.getenv("YOUR_OVR_FACTS_SCAN_LIMIT", "200000")))
        self.min_pa_per_mlb_id = max(1, int(min_pa_per_mlb_id or os.getenv("YOUR_OVR_MIN_PA_PER_MLB_ID", "10")))
        self.user_workers = max(1, int(os.getenv("YOUR_OVR_USER_WORKERS", "4")))
        self.user_batch_size = max(1, int(os.getenv("YOUR_OVR_USER_BATCH_SIZE", "500")))
        self.global_rank_shards = max(8, int(os.getenv("YOUR_OVR_GLOBAL_RANK_SHARDS", "128")))
        self.db_fallback_verify_pas = self._env_bool("YOUR_OVR_DB_FALLBACK_VERIFY_PAS", default=False)
        self.weight_smoothing_pa = max(
            1.0,
            float(weight_smoothing_pa or os.getenv("YOUR_OVR_WEIGHT_SMOOTHING_PA", "30")),
        )
        self.weight_min = 0.75
        self.weight_max = 1.25

    def run(self, db_session: Session) -> None:
        source, usernames_iter, users_discovered = self._usernames_source(db_session)
        self._log_start(
            users_discovered=(users_discovered if users_discovered is not None else "streaming"),
            min_pa=self.min_pa_per_mlb_id,
            smoothing_pa=self.weight_smoothing_pa,
            user_workers=self.user_workers,
            user_batch_size=self.user_batch_size,
            global_rank_shards=self.global_rank_shards,
            user_source=source,
        )
        users_seen = 0
        users_written = 0
        total_rows = 0
        global_rows_written = 0
        batch: list[str] = []

        with tempfile.TemporaryDirectory(prefix="your_ovr_sync_") as tmp_dir:
            shard_dir = Path(tmp_dir) / "global_rank_shards"
            shard_dir.mkdir(parents=True, exist_ok=True)
            shard_paths = [shard_dir / f"shard_{idx:04d}.jsonl" for idx in range(self.global_rank_shards)]
            shard_handles = [path.open("a", encoding="utf-8") for path in shard_paths]

            try:
                for username in usernames_iter:
                    users_seen += 1
                    batch.append(username)
                    if len(batch) >= self.user_batch_size:
                        written, rows_count = self._process_user_batch(batch, shard_handles)
                        users_written += written
                        total_rows += rows_count
                        batch.clear()

                if batch:
                    written, rows_count = self._process_user_batch(batch, shard_handles)
                    users_written += written
                    total_rows += rows_count
                    batch.clear()
            finally:
                for handle in shard_handles:
                    try:
                        handle.flush()
                        handle.close()
                    except Exception:
                        pass

            if users_written == 0:
                self._put_your_ovr_parquet(GLOBAL_YOUR_OVR_KEY, [])
                self._log_end(users_seen=users_seen, users_written=0, total_rows=0, global_rows=0)
                return

            global_rows_written = self._build_and_upload_global_from_shards(shard_paths)

        self._log_end(
            users_seen=users_seen,
            users_written=users_written,
            total_rows=total_rows,
            global_rows=global_rows_written,
        )

    def _process_user_batch(self, usernames: list[str], shard_handles: list[Any]) -> tuple[int, int]:
        users_written = 0
        rows_count = 0

        if self.user_workers <= 1 or len(usernames) <= 1:
            for username in usernames:
                try:
                    uname, rows, pas_len = self._build_rows_for_username(username)
                except Exception as exc:
                    self.logger.exception("your ovr user processing failed username=%s err=%s", username, exc)
                    continue

                if pas_len <= 0 and not rows:
                    continue

                rows.sort(key=self._user_row_sort_key)
                self._put_your_ovr_parquet(f"facts/{uname}/your_ovr.parquet", rows)
                self._spill_rows_to_shards(rows, shard_handles)
                users_written += 1
                rows_count += len(rows)
                self.logger.info(
                    "your ovr progress username=%s pas_rows=%s ovr_rows=%s",
                    uname,
                    pas_len,
                    len(rows),
                )
            return users_written, rows_count

        with ThreadPoolExecutor(max_workers=self.user_workers) as pool:
            futures = {pool.submit(self._build_rows_for_username, username): username for username in usernames}
            for fut in as_completed(futures):
                source_username = futures[fut]
                try:
                    uname, rows, pas_len = fut.result()
                except Exception as exc:
                    self.logger.exception(
                        "your ovr user processing failed username=%s err=%s",
                        source_username,
                        exc,
                    )
                    continue

                if pas_len <= 0 and not rows:
                    continue

                rows.sort(key=self._user_row_sort_key)
                self._put_your_ovr_parquet(f"facts/{uname}/your_ovr.parquet", rows)
                self._spill_rows_to_shards(rows, shard_handles)
                users_written += 1
                rows_count += len(rows)
                self.logger.info(
                    "your ovr progress username=%s pas_rows=%s ovr_rows=%s",
                    uname,
                    pas_len,
                    len(rows),
                )

        return users_written, rows_count

    def _build_rows_for_username(self, username: str) -> tuple[str, list[dict[str, Any]], int]:
        pas_df = self._read_pas_df_for_username(username)
        rows = self._build_user_rows(username=username, pas_df=pas_df)
        return username, rows, len(pas_df)

    def _spill_rows_to_shards(self, rows: list[dict[str, Any]], shard_handles: list[Any]) -> None:
        for row in rows:
            shard_idx = self._global_rank_shard_index(row)
            shard_handles[shard_idx].write(json.dumps(row, separators=(",", ":")) + "\n")

    def _global_rank_shard_index(self, row: dict[str, Any]) -> int:
        role = str(row.get("role") or "")
        mlb_id = self._coerce_int(row.get("mlb_id"))
        if mlb_id is None:
            return 0
        key = f"{role}:{mlb_id}".encode("utf-8")
        return zlib.crc32(key) % self.global_rank_shards

    def _build_and_upload_global_from_shards(self, shard_paths: list[Path]) -> int:
        schema = self._your_ovr_schema()
        global_rows_written = 0

        with tempfile.NamedTemporaryFile(prefix="your_ovr_all_", suffix=".parquet", delete=False) as tmp:
            temp_global_path = Path(tmp.name)

        try:
            writer = pq.ParquetWriter(str(temp_global_path), schema=schema, compression="zstd")
            try:
                for idx, shard_path in enumerate(shard_paths, start=1):
                    shard_rows = self._read_rank_shard_rows(shard_path)
                    if not shard_rows:
                        continue

                    ranked_rows = self._append_global_rankings(shard_rows)
                    ranked_rows.sort(key=self._global_row_sort_key)
                    normalized_rows = self._normalize_rows_for_schema(ranked_rows, schema)
                    table = pa.Table.from_pylist(normalized_rows, schema=schema)
                    if table.num_rows:
                        writer.write_table(table)
                        global_rows_written += int(table.num_rows)

                    if idx % 16 == 0:
                        self.logger.info(
                            "your ovr global rank progress shards_done=%s/%s rows_written=%s",
                            idx,
                            len(shard_paths),
                            global_rows_written,
                        )
            finally:
                writer.close()

            self.spaces.upload_file(
                local_path=str(temp_global_path),
                key=GLOBAL_YOUR_OVR_KEY,
                content_type="application/octet-stream",
                cache_control="no-cache",
            )
        finally:
            try:
                temp_global_path.unlink(missing_ok=True)
            except Exception:
                pass

        return global_rows_written

    def _read_rank_shard_rows(self, shard_path: Path) -> list[dict[str, Any]]:
        if not shard_path.exists() or shard_path.stat().st_size <= 0:
            return []
        out: list[dict[str, Any]] = []
        with shard_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                out.append(json.loads(line))
        return out

    def _usernames_source(
        self,
        db_session: Optional[Session],
    ) -> tuple[str, Any, Optional[int]]:
        listed_usernames = self._discover_usernames_from_spaces()
        if listed_usernames:
            return "spaces_listing", iter(listed_usernames), len(listed_usernames)
        return "db_stream", self._iter_db_usernames(db_session), None

    def _discover_usernames_from_spaces(self) -> list[str]:
        usernames: set[str] = set()
        prefixes = (
            ("facts/", FACTS_KEY_PATTERN),
            ("di-storage/facts/", LEGACY_FACTS_KEY_PATTERN),
        )

        for prefix, pattern in prefixes:
            try:
                keys = self.spaces.list_keys(prefix, limit=self.facts_scan_limit)
            except Exception as exc:
                self.logger.warning("your ovr facts listing failed prefix=%s err=%s", prefix, exc)
                continue

            for key in keys:
                match = pattern.match(str(key))
                if match:
                    usernames.add(match.group(1))
            if len(keys) >= self.facts_scan_limit:
                self.logger.warning(
                    "your ovr facts scan may be truncated prefix=%s limit=%s discovered_keys=%s",
                    prefix,
                    self.facts_scan_limit,
                    len(keys),
                )

        return sorted(usernames)

    def _discover_usernames_from_facts(self, db_session: Optional[Session] = None) -> list[str]:
        source, usernames_iter, users_discovered = self._usernames_source(db_session)
        if users_discovered is not None:
            return list(usernames_iter)
        out = list(usernames_iter)
        self.logger.info("your ovr discover via %s yielded=%s", source, len(out))
        return out

    def _iter_db_usernames(self, db_session: Optional[Session]):
        if db_session is None:
            return

        stmt = (
            select(ShowProfile.username)
            .where(ShowProfile.username.is_not(None))
            .distinct()
            .execution_options(yield_per=1000, stream_results=True)
        )

        scalars = db_session.scalars(stmt)
        scanned = 0
        yielded = 0
        for raw_username in scalars:
            scanned += 1
            username = str(raw_username).strip()
            if not username:
                continue

            if scanned % 1000 == 0:
                self.logger.info(
                    "your ovr db stream scan progress scanned=%s yielded=%s verify_pas=%s",
                    scanned,
                    yielded,
                    self.db_fallback_verify_pas,
                )

            if self.db_fallback_verify_pas and not self._pas_exists_for_username(username):
                continue

            yielded += 1
            if yielded % 250 == 0:
                self.logger.info(
                    "your ovr db stream progress scanned=%s yielded=%s verify_pas=%s",
                    scanned,
                    yielded,
                    self.db_fallback_verify_pas,
                )
            yield username

        self.logger.info(
            "your ovr db stream complete scanned=%s yielded=%s verify_pas=%s",
            scanned,
            yielded,
            self.db_fallback_verify_pas,
        )

    def _discover_usernames_from_db(self, db_session: Optional[Session]) -> list[str]:
        return list(self._iter_db_usernames(db_session))

    def _pas_exists_for_username(self, username: str) -> bool:
        primary = f"facts/{username}/pas.parquet"
        legacy = f"di-storage/facts/{username}/pas.parquet"

        for key in (primary, legacy):
            try:
                if self.spaces.exists(key):
                    return True
            except Exception:
                pass
            try:
                self.spaces.get_bytes(key, byte_range=(0, 0))
                return True
            except Exception:
                continue
        return False

    def _resolve_pas_key(self, username: str) -> str:
        primary = f"facts/{username}/pas.parquet"
        legacy = f"di-storage/facts/{username}/pas.parquet"

        for key in (primary, legacy):
            try:
                if self.spaces.exists(key):
                    return key
            except Exception:
                pass
            try:
                self.spaces.get_bytes(key, byte_range=(0, 0))
                return key
            except Exception:
                continue
        return primary

    def _read_pas_df_for_username(self, username: str) -> pd.DataFrame:
        primary_key = f"facts/{username}/pas.parquet"
        legacy_key = f"di-storage/facts/{username}/pas.parquet"

        primary_df = self._read_pas_df_optional(primary_key)
        if not primary_df.empty:
            return primary_df

        legacy_df = self._read_pas_df_optional(legacy_key)
        if not legacy_df.empty:
            return legacy_df

        return primary_df

    def _read_pas_df_optional(self, key: str) -> pd.DataFrame:
        try:
            raw = self.spaces.get_bytes(key)
        except Exception:
            return self._empty_pas_df()
        if not raw:
            return self._empty_pas_df()

        try:
            buf = BytesIO(raw)
            parquet = pq.ParquetFile(buf)
            available = set(parquet.schema.names)
            cols = [c for c in PAS_COLUMNS if c in available]
            buf.seek(0)
            if cols:
                table = pq.read_table(buf, columns=cols)
            else:
                table = pq.read_table(buf)
            df = table.to_pandas()
        except Exception:
            return self._empty_pas_df()

        for col in PAS_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df

    def _empty_pas_df(self) -> pd.DataFrame:
        return pd.DataFrame(columns=list(PAS_COLUMNS))

    def _build_user_rows(self, username: str, pas_df: pd.DataFrame) -> list[dict[str, Any]]:
        if pas_df.empty:
            return []

        user_hitting, user_pitching = self._user_masks(pas_df, username)
        hitting_df = pas_df[user_hitting].copy()
        pitching_df = pas_df[user_pitching].copy()

        rows: list[dict[str, Any]] = []
        rows.extend(self._build_role_rows(username=username, role="hitting", role_df=hitting_df, mlb_id_col="batter_mlb_id"))
        rows.extend(
            self._build_role_rows(username=username, role="pitching", role_df=pitching_df, mlb_id_col="pitcher_mlb_id")
        )
        self._validate_user_rows(username=username, hitting_df=hitting_df, pitching_df=pitching_df, rows=rows)
        return rows

    def _validate_user_rows(
        self,
        *,
        username: str,
        hitting_df: pd.DataFrame,
        pitching_df: pd.DataFrame,
        rows: list[dict[str, Any]],
    ) -> None:
        expected_hitting_pa = len(hitting_df)
        expected_pitching_pa = len(pitching_df)

        actual_hitting_pa = sum(
            int(self._coerce_int(r.get("pa")) or 0)
            for r in rows
            if str(r.get("role") or "") == "hitting"
        )
        actual_pitching_pa = sum(
            int(self._coerce_int(r.get("pa")) or 0)
            for r in rows
            if str(r.get("role") or "") == "pitching"
        )

        if actual_hitting_pa != expected_hitting_pa:
            self.logger.warning(
                "your ovr hitting pa mismatch username=%s expected=%s actual=%s",
                username,
                expected_hitting_pa,
                actual_hitting_pa,
            )
        if actual_pitching_pa != expected_pitching_pa:
            self.logger.warning(
                "your ovr pitching pa mismatch username=%s expected=%s actual=%s",
                username,
                expected_pitching_pa,
                actual_pitching_pa,
            )

    def _build_role_rows(
        self,
        *,
        username: str,
        role: str,
        role_df: pd.DataFrame,
        mlb_id_col: str,
    ) -> list[dict[str, Any]]:
        if role_df.empty:
            return []

        role_df = role_df.copy()
        role_df["_mlb_id"] = pd.to_numeric(role_df.get(mlb_id_col), errors="coerce")
        role_df = role_df[role_df["_mlb_id"].notna()]
        if role_df.empty:
            return []

        baseline = self._aggregate_role_stats(role_df, role=role)
        updated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        out: list[dict[str, Any]] = []
        for mlb_id_value, group in role_df.groupby("_mlb_id"):
            mlb_id = self._coerce_int(mlb_id_value)
            if mlb_id is None:
                continue

            stats = self._aggregate_role_stats(group, role=role)
            pa = int(stats.get("pa") or 0)
            signal = self._compute_signal(role=role, stats=stats, baseline=baseline)
            reliability = (float(pa) / (float(pa) + self.weight_smoothing_pa)) if pa > 0 else 0.0
            weight_raw = 1.0 + ((signal - 1.0) * reliability)
            weight = self._clamp(weight_raw, self.weight_min, self.weight_max)

            out.append(
                {
                    "username": username,
                    "mlb_id": mlb_id,
                    "role": role,
                    "pa": pa,
                    "ab": int(stats.get("ab") or 0),
                    "h": int(stats.get("h") or 0),
                    "r": int(stats.get("r") or 0),
                    "rbi": int(stats.get("rbi") or 0),
                    "singles": int(stats.get("singles") or 0),
                    "doubles": int(stats.get("doubles") or 0),
                    "triples": int(stats.get("triples") or 0),
                    "hr": int(stats.get("hr") or 0),
                    "bb": int(stats.get("bb") or 0),
                    "so": int(stats.get("so") or 0),
                    "hbp": int(stats.get("hbp") or 0),
                    "sf": int(stats.get("sf") or 0),
                    "sh": int(stats.get("sh") or 0),
                    "xbh": int(stats.get("xbh") or 0),
                    "avg": self._coerce_float(stats.get("avg")),
                    "obp": self._coerce_float(stats.get("obp")),
                    "slg": self._coerce_float(stats.get("slg")),
                    "ops": self._coerce_float(stats.get("ops")),
                    "woba": self._coerce_float(stats.get("woba")),
                    "iso": self._coerce_float(stats.get("iso")),
                    "babip": self._coerce_float(stats.get("babip")),
                    "k_pct": self._coerce_float(stats.get("k_pct")),
                    "bb_pct": self._coerce_float(stats.get("bb_pct")),
                    "hr_pct": self._coerce_float(stats.get("hr_pct")),
                    "xbh_pct": self._coerce_float(stats.get("xbh_pct")),
                    "era": self._coerce_float(stats.get("era")),
                    "whip": self._coerce_float(stats.get("whip")),
                    "kbb": self._coerce_float(stats.get("kbb")),
                    "baseline_ops": self._coerce_float(baseline.get("ops")),
                    "baseline_woba": self._coerce_float(baseline.get("woba")),
                    "baseline_iso": self._coerce_float(baseline.get("iso")),
                    "baseline_babip": self._coerce_float(baseline.get("babip")),
                    "baseline_k_pct": self._coerce_float(baseline.get("k_pct")),
                    "baseline_hr_pct": self._coerce_float(baseline.get("hr_pct")),
                    "ops_above_expected": self._delta(stats.get("ops"), baseline.get("ops")),
                    "woba_above_expected": self._delta(stats.get("woba"), baseline.get("woba")),
                    "iso_above_expected": self._delta(stats.get("iso"), baseline.get("iso")),
                    "babip_above_expected": self._delta(stats.get("babip"), baseline.get("babip")),
                    "k_pct_above_expected": self._delta(stats.get("k_pct"), baseline.get("k_pct")),
                    "signal": signal,
                    "reliability": reliability,
                    "weight_raw": weight_raw,
                    "weight": weight,
                    "meets_min_pa": pa >= self.min_pa_per_mlb_id,
                    "global_rank": None,
                    "global_percentile": None,
                    "global_cohort_size": 0,
                    "is_top_weight_for_player": False,
                    "updated_at": updated_at,
                }
            )

        return out

    def _aggregate_role_stats(self, df: pd.DataFrame, *, role: str) -> dict[str, Any]:
        pa_count = len(df)
        if pa_count == 0:
            return {
                "pa": 0,
                "ab": 0,
                "h": 0,
                "r": 0,
                "rbi": 0,
                "singles": 0,
                "doubles": 0,
                "triples": 0,
                "hr": 0,
                "bb": 0,
                "so": 0,
                "hbp": 0,
                "sf": 0,
                "sh": 0,
                "xbh": 0,
                "avg": 0.0,
                "obp": 0.0,
                "slg": 0.0,
                "ops": 0.0,
                "woba": 0.0,
                "iso": 0.0,
                "babip": 0.0,
                "k_pct": 0.0,
                "bb_pct": 0.0,
                "hr_pct": 0.0,
                "xbh_pct": 0.0,
                "era": None,
                "whip": None,
                "kbb": None,
            }

        results = self._str_col(df, "result").str.lower()
        singles = results == "single"
        doubles = results == "double"
        triples = results == "triple"
        homeruns = results.isin(_HR_RESULTS)
        walks = results.isin(_WALK_RESULTS)
        hbp = results.isin(_HBP_RESULTS)

        sac_fly = self._bool_col(df, "is_sac_fly")
        sac_bunt = self._bool_col(df, "is_sac_bunt")
        strikeouts = self._bool_col(df, "is_strikeout") | results.isin(["strikeout", "strike out"])

        singles_count = int(singles.sum())
        doubles_count = int(doubles.sum())
        triples_count = int(triples.sum())
        hr_count = int(homeruns.sum())
        bb_count = int(walks.sum())
        so_count = int(strikeouts.sum())
        hbp_count = int(hbp.sum())
        sf_count = int(sac_fly.sum())
        sh_count = int(sac_bunt.sum())

        hits = singles_count + doubles_count + triples_count + hr_count
        total_bases = singles_count + (2 * doubles_count) + (3 * triples_count) + (4 * hr_count)

        ab = pa_count - bb_count - hbp_count - sf_count - sh_count
        if ab < 0:
            ab = 0

        runs = int(self._num_col(df, "runs_scored").sum())
        rbi = int(self._num_col(df, "rbi").sum())

        avg = (hits / ab) if ab else 0.0
        obp_denom = ab + bb_count + hbp_count + sf_count
        obp = ((hits + bb_count + hbp_count) / obp_denom) if obp_denom else 0.0
        slg = (total_bases / ab) if ab else 0.0
        ops = obp + slg

        woba_denom = obp_denom
        if woba_denom:
            woba_num = (
                (_WOBA_WEIGHTS["bb"] * bb_count)
                + (_WOBA_WEIGHTS["hbp"] * hbp_count)
                + (_WOBA_WEIGHTS["single"] * singles_count)
                + (_WOBA_WEIGHTS["double"] * doubles_count)
                + (_WOBA_WEIGHTS["triple"] * triples_count)
                + (_WOBA_WEIGHTS["hr"] * hr_count)
            )
            woba = woba_num / float(woba_denom)
        else:
            woba = 0.0

        iso = ((total_bases - hits) / ab) if ab else 0.0
        babip_denom = ab - so_count - hr_count + sf_count
        babip = ((hits - hr_count) / babip_denom) if babip_denom > 0 else 0.0

        k_pct = (so_count / pa_count) * 100 if pa_count else 0.0
        bb_pct = (bb_count / pa_count) * 100 if pa_count else 0.0
        hr_pct = (hr_count / pa_count) * 100 if pa_count else 0.0
        xbh_count = doubles_count + triples_count + hr_count
        xbh_pct = (xbh_count / pa_count) * 100 if pa_count else 0.0

        era = None
        whip = None
        kbb = None
        if role == "pitching":
            is_out = self._bool_col(df, "is_out")
            double_play = self._bool_col(df, "is_double_play")
            outs_pitched = int((is_out.astype(int) + double_play.astype(int)).sum())
            ip = outs_pitched / 3.0 if outs_pitched > 0 else 0.0
            era = (runs * 9.0 / ip) if ip > 0 else None
            whip = ((bb_count + hits) / ip) if ip > 0 else None
            kbb = (so_count / float(bb_count)) if bb_count > 0 else None

        return {
            "pa": int(pa_count),
            "ab": int(ab),
            "h": int(hits),
            "r": int(runs),
            "rbi": int(rbi),
            "singles": int(singles_count),
            "doubles": int(doubles_count),
            "triples": int(triples_count),
            "hr": int(hr_count),
            "bb": int(bb_count),
            "so": int(so_count),
            "hbp": int(hbp_count),
            "sf": int(sf_count),
            "sh": int(sh_count),
            "xbh": int(xbh_count),
            "avg": float(avg),
            "obp": float(obp),
            "slg": float(slg),
            "ops": float(ops),
            "woba": float(woba),
            "iso": float(iso),
            "babip": float(babip),
            "k_pct": float(k_pct),
            "bb_pct": float(bb_pct),
            "hr_pct": float(hr_pct),
            "xbh_pct": float(xbh_pct),
            "era": self._coerce_float(era),
            "whip": self._coerce_float(whip),
            "kbb": self._coerce_float(kbb),
        }

    def _compute_signal(self, *, role: str, stats: dict[str, Any], baseline: dict[str, Any]) -> float:
        if role == "pitching":
            components = [
                self._inverse_ratio(stats.get("ops"), baseline.get("ops")),
                self._inverse_ratio(stats.get("woba"), baseline.get("woba")),
                self._inverse_ratio(stats.get("babip"), baseline.get("babip")),
                self._ratio(stats.get("k_pct"), baseline.get("k_pct")),
                self._inverse_ratio(stats.get("hr_pct"), baseline.get("hr_pct")),
            ]
        else:
            components = [
                self._ratio(stats.get("ops"), baseline.get("ops")),
                self._ratio(stats.get("woba"), baseline.get("woba")),
                self._ratio(stats.get("iso"), baseline.get("iso")),
                self._ratio(stats.get("babip"), baseline.get("babip")),
                self._inverse_ratio(stats.get("k_pct"), baseline.get("k_pct")),
            ]

        if not components:
            return 1.0
        return self._clamp(sum(components) / float(len(components)), 0.50, 1.50)

    def _ratio(self, value: Any, base: Any) -> float:
        val = self._coerce_float(value)
        base_val = self._coerce_float(base)
        if base_val is None or base_val <= 0:
            return 1.0
        if val is None or val < 0:
            return 1.0
        return self._clamp(val / base_val, 0.50, 1.50)

    def _inverse_ratio(self, value: Any, base: Any) -> float:
        val = self._coerce_float(value)
        base_val = self._coerce_float(base)
        if base_val is None or base_val <= 0:
            return 1.0
        if val is None:
            return 1.0
        if val <= 0:
            return 1.50
        return self._clamp(base_val / val, 0.50, 1.50)

    def _delta(self, value: Any, base: Any) -> Optional[float]:
        val = self._coerce_float(value)
        base_val = self._coerce_float(base)
        if val is None or base_val is None:
            return None
        return val - base_val

    def _append_global_rankings(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = [dict(r) for r in rows]
        bucketed: dict[tuple[str, int], list[int]] = defaultdict(list)

        for idx, row in enumerate(out):
            mlb_id = self._coerce_int(row.get("mlb_id"))
            role = str(row.get("role") or "")
            if mlb_id is None or not role:
                continue
            bucketed[(role, mlb_id)].append(idx)

        for indices in bucketed.values():
            eligible: list[int] = []
            for i in indices:
                pa = self._coerce_int(out[i].get("pa")) or 0
                if pa >= self.min_pa_per_mlb_id:
                    eligible.append(i)

            cohort_size = len(eligible)
            for i in indices:
                out[i]["global_cohort_size"] = cohort_size

            if cohort_size == 0:
                continue

            sorted_eligible = sorted(
                eligible,
                key=lambda i: (
                    self._coerce_float(out[i].get("weight")) or 1.0,
                    self._coerce_int(out[i].get("pa")) or 0,
                    str(out[i].get("username") or ""),
                ),
                reverse=True,
            )

            for rank, i in enumerate(sorted_eligible, start=1):
                out[i]["global_rank"] = rank
                out[i]["global_percentile"] = 1.0 if cohort_size == 1 else float((cohort_size - rank) / (cohort_size - 1))
                out[i]["is_top_weight_for_player"] = rank == 1

        return out

    def _user_row_sort_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        role = str(row.get("role") or "")
        weight = self._coerce_float(row.get("weight")) or 0.0
        pa = self._coerce_int(row.get("pa")) or 0
        mlb_id = self._coerce_int(row.get("mlb_id")) or 0
        return (role, -weight, -pa, mlb_id)

    def _global_row_sort_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        role = str(row.get("role") or "")
        mlb_id = self._coerce_int(row.get("mlb_id")) or 0
        rank = self._coerce_int(row.get("global_rank"))
        rank_key = rank if rank is not None else 10**9
        weight = self._coerce_float(row.get("weight")) or 0.0
        return (role, mlb_id, rank_key, -weight)

    def _put_your_ovr_parquet(self, key: str, rows: list[dict[str, Any]]) -> None:
        schema = self._your_ovr_schema()
        normalized = self._normalize_rows_for_schema(rows, schema)
        table = pa.Table.from_pylist(normalized, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        self.spaces.put_bytes(
            key=key,
            data=sink.getvalue().to_pybytes(),
            content_type="application/octet-stream",
            cache_control="no-cache",
        )

    def _normalize_rows_for_schema(self, rows: list[dict[str, Any]], schema: pa.Schema) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for source in rows:
            normalized: dict[str, Any] = {}
            for field in schema:
                value = source.get(field.name)
                if pa.types.is_integer(field.type):
                    normalized[field.name] = self._coerce_int(value)
                elif pa.types.is_floating(field.type):
                    normalized[field.name] = self._coerce_float(value)
                elif pa.types.is_boolean(field.type):
                    normalized[field.name] = value if isinstance(value, bool) else None
                elif pa.types.is_string(field.type):
                    normalized[field.name] = None if value is None else str(value)
                else:
                    normalized[field.name] = value
            out.append(normalized)
        return out

    def _your_ovr_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("username", pa.string()),
                pa.field("mlb_id", pa.int64()),
                pa.field("role", pa.string()),
                pa.field("pa", pa.int64()),
                pa.field("ab", pa.int64()),
                pa.field("h", pa.int64()),
                pa.field("r", pa.int64()),
                pa.field("rbi", pa.int64()),
                pa.field("singles", pa.int64()),
                pa.field("doubles", pa.int64()),
                pa.field("triples", pa.int64()),
                pa.field("hr", pa.int64()),
                pa.field("bb", pa.int64()),
                pa.field("so", pa.int64()),
                pa.field("hbp", pa.int64()),
                pa.field("sf", pa.int64()),
                pa.field("sh", pa.int64()),
                pa.field("xbh", pa.int64()),
                pa.field("avg", pa.float64()),
                pa.field("obp", pa.float64()),
                pa.field("slg", pa.float64()),
                pa.field("ops", pa.float64()),
                pa.field("woba", pa.float64()),
                pa.field("iso", pa.float64()),
                pa.field("babip", pa.float64()),
                pa.field("k_pct", pa.float64()),
                pa.field("bb_pct", pa.float64()),
                pa.field("hr_pct", pa.float64()),
                pa.field("xbh_pct", pa.float64()),
                pa.field("era", pa.float64()),
                pa.field("whip", pa.float64()),
                pa.field("kbb", pa.float64()),
                pa.field("baseline_ops", pa.float64()),
                pa.field("baseline_woba", pa.float64()),
                pa.field("baseline_iso", pa.float64()),
                pa.field("baseline_babip", pa.float64()),
                pa.field("baseline_k_pct", pa.float64()),
                pa.field("baseline_hr_pct", pa.float64()),
                pa.field("ops_above_expected", pa.float64()),
                pa.field("woba_above_expected", pa.float64()),
                pa.field("iso_above_expected", pa.float64()),
                pa.field("babip_above_expected", pa.float64()),
                pa.field("k_pct_above_expected", pa.float64()),
                pa.field("signal", pa.float64()),
                pa.field("reliability", pa.float64()),
                pa.field("weight_raw", pa.float64()),
                pa.field("weight", pa.float64()),
                pa.field("meets_min_pa", pa.bool_()),
                pa.field("global_rank", pa.int64()),
                pa.field("global_percentile", pa.float64()),
                pa.field("global_cohort_size", pa.int64()),
                pa.field("is_top_weight_for_player", pa.bool_()),
                pa.field("updated_at", pa.string()),
            ]
        )

    def _user_masks(self, df: pd.DataFrame, username: str) -> tuple[pd.Series, pd.Series]:
        name = str(username).strip().lower()
        if "is_home_batting" not in df.columns:
            empty = pd.Series([False] * len(df), index=df.index)
            return empty, empty

        is_home_batting = self._bool_col(df, "is_home_batting")
        candidates = [
            ("home_profile_username", "away_profile_username"),
            ("home_team_id", "away_team_id"),
            ("home_name", "away_name"),
        ]

        def build_masks(home_col: str, away_col: str) -> tuple[pd.Series, pd.Series, pd.Series]:
            home_team = self._str_col(df, home_col).str.lower()
            away_team = self._str_col(df, away_col).str.lower()
            user_is_home = home_team == name
            user_is_away = away_team == name

            user_hitting = (user_is_home & is_home_batting) | (user_is_away & ~is_home_batting)
            user_pitching = (user_is_home & ~is_home_batting) | (user_is_away & is_home_batting)
            has_match = user_is_home | user_is_away
            return user_hitting, user_pitching, has_match

        for home_col, away_col in candidates:
            if home_col in df.columns and away_col in df.columns:
                user_hitting, user_pitching, has_match = build_masks(home_col, away_col)
                if has_match.any():
                    return user_hitting, user_pitching

        for home_col, away_col in candidates:
            if home_col in df.columns and away_col in df.columns:
                user_hitting, user_pitching, _ = build_masks(home_col, away_col)
                return user_hitting, user_pitching

        empty = pd.Series([False] * len(df), index=df.index)
        return empty, empty

    def _bool_col(self, df: pd.DataFrame, name: str) -> pd.Series:
        col = df.get(name)
        if col is None:
            return pd.Series([False] * len(df), index=df.index)

        if pd.api.types.is_bool_dtype(col):
            return col.fillna(False).astype(bool)

        if pd.api.types.is_numeric_dtype(col):
            return pd.to_numeric(col, errors="coerce").fillna(0).astype(int) != 0

        text = col.fillna("").astype(str).str.strip().str.lower()
        true_vals = {"1", "true", "t", "yes", "y"}
        false_vals = {"", "0", "false", "f", "no", "n", "none", "null", "nan"}
        mapped = text.map(lambda v: True if v in true_vals else (False if v in false_vals else bool(v)))
        return mapped.astype(bool)

    def _str_col(self, df: pd.DataFrame, name: str) -> pd.Series:
        col = df.get(name)
        if col is None:
            return pd.Series([""] * len(df), index=df.index)
        return col.fillna("").astype(str)

    def _num_col(self, df: pd.DataFrame, name: str) -> pd.Series:
        col = df.get(name)
        if col is None:
            return pd.Series([0] * len(df), index=df.index)
        return pd.to_numeric(col, errors="coerce").fillna(0)

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

    def _clamp(self, value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _env_bool(self, name: str, *, default: bool) -> bool:
        raw = os.getenv(name)
        if raw is None:
            return default
        val = str(raw).strip().lower()
        if val in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if val in {"0", "false", "f", "no", "n", "off"}:
            return False
        return default
