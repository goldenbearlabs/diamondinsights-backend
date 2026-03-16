from __future__ import annotations

import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from apps.jobs.job import Job
from shared.core.config import CURRENT_SHOW_YEAR
from shared.db.models import ShowProfile, ShowProfileOnlineStats


SHOW_SEARCH_URL = os.getenv(
    "SHOW_SEARCH_URL",
    f"https://mlb{CURRENT_SHOW_YEAR}.theshow.com/apis/player_search.json",
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_int(v) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).strip().replace("%", ""))
    except Exception:
        return None


class ShowProfileStatsUpdater(Job):
    def __init__(
        self,
        *,
        flush_every: Optional[int] = None,
        fetch_jitter_range: Optional[Tuple[float, float]] = None,
    ):
        super().__init__()
        default_flush = int(os.getenv("SHOW_PROFILE_REFRESH_FLUSH_EVERY", "50"))
        self.flush_every = max(1, flush_every or default_flush)
        self.fetch_jitter_range = fetch_jitter_range or (0.05, 0.2)

    def run(self, db_session: Session):
        stmt = (
            select(ShowProfile)
            .options(selectinload(ShowProfile.online_stats))
            .where(ShowProfile.username.is_not(None))
        )
        profiles = list(db_session.scalars(stmt))
        self._log_start(flush_every=self.flush_every, total_profiles=len(profiles))

        processed = 0
        updated_profiles = 0
        stats_rows_touched = 0
        skipped = 0
        errors = 0

        for sp in profiles:
            processed += 1
            username = (sp.username or "").strip()
            if not username:
                skipped += 1
                continue

            try:
                profile_payload, raw = self._fetch_show_profile(username)
            except Exception as e:
                errors += 1
                self.logger.warning("show profile fetch failed username=%s err=%s", username, e)
                continue

            try:
                if self._apply_profile_update(sp, profile_payload, raw):
                    updated_profiles += 1

                incoming = self._parsed_online_stats(profile_payload)
                stats_rows_touched += self._apply_online_stats(sp, incoming)

                if processed % self.flush_every == 0:
                    db_session.commit()
            except Exception as e:
                errors += 1
                db_session.rollback()
                self.logger.exception("show profile update failed username=%s err=%s", username, e)

        try:
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            self.logger.exception("show profile refresh commit failed err=%s", e)
            errors += 1

        self._log_end(
            processed=processed,
            updated_profiles=updated_profiles,
            stats_rows_touched=stats_rows_touched,
            skipped=skipped,
            errors=errors,
        )

    def _fetch_show_profile(self, username: str) -> Tuple[dict, dict]:
        time.sleep(random.uniform(*self.fetch_jitter_range))
        data = self._api_client.get(SHOW_SEARCH_URL, {"username": username})
        profiles = data.get("universal_profiles") or []
        if not profiles:
            raise ValueError("username not found")
        return profiles[0], data

    def _apply_profile_update(self, sp: ShowProfile, profile_payload: dict, raw: dict) -> bool:
        vanity = profile_payload.get("vanity") or {}

        display_level = _to_int(profile_payload.get("display_level"))
        if display_level is not None:
            sp.display_level = display_level

        games_played = _to_int(profile_payload.get("games_played"))
        if games_played is not None:
            sp.games_played = games_played

        nameplate = vanity.get("nameplate_equipped")
        if nameplate is not None:
            sp.nameplate_equipped = nameplate

        icon = vanity.get("icon_equipped")
        if icon is not None:
            sp.icon_equipped = icon

        sp.raw_json = raw
        sp.last_refreshed_at = _utcnow()
        return True

    def _parsed_online_stats(self, profile_payload: dict) -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}
        for row in (profile_payload.get("online_data") or []):
            yr = _to_int(row.get("year"))
            if yr is None:
                continue

            losses_val = row.get("losses")
            if losses_val is None:
                losses_val = row.get("loses")

            out[yr] = {
                "wins": _to_int(row.get("wins")),
                "losses": _to_int(losses_val),
                "hr": _to_int(row.get("hr")),
                "runs_per_game": _to_float(row.get("runs_per_game")),
                "stolen_bases": _to_int(row.get("stolen_bases")),
                "batting_average": _to_float(row.get("batting_average")),
                "era": _to_float(row.get("era")),
                "k_per_9": _to_float(row.get("k_per_9")),
                "whip": _to_float(row.get("whip")),
            }
        return out

    def _apply_online_stats(self, sp: ShowProfile, incoming: Dict[int, Dict[str, Any]]) -> int:
        touched = 0
        existing_by_year = {s.year: s for s in (sp.online_stats or [])}

        for year, vals in incoming.items():
            if all(v is None for v in vals.values()) and year not in existing_by_year:
                continue

            row = existing_by_year.get(year)
            if row is None:
                row = ShowProfileOnlineStats(year=year)
                sp.online_stats.append(row)

            updated = False
            for field, value in vals.items():
                if value is None:
                    continue
                setattr(row, field, value)
                updated = True

            if updated:
                touched += 1

        return touched
