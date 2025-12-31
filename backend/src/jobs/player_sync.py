from __future__ import annotations

import datetime
import random
import re
import time
import unicodedata
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func, select, update

from src.core.http_client import APIClient
from src.database.models import BirthLocation, Card, MLBPosition, Player
from src.jobs.base import BaseJob


PITCHER_ABBRS = {"P", "SP", "RP", "CP"}
MAX_WORKERS = 6
JITTER_RANGE_S = (0.05, 0.45)


class PlayerSync(BaseJob):
    def __init__(self, rerun_all_cards: bool = False, flush_every: int = 200):
        super().__init__()
        self.set_child_instance(self)
        self.rerun_all_cards = rerun_all_cards
        self.flush_every = flush_every
        self._group_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}

    def execute(self, db_session):
        self.logger.info(f"Starting PlayerSync... rerun_all_cards={self.rerun_all_cards}")

        self._ensure_unknown_position(db_session)

        stmt = select(Card.name, Card.born).where(Card.name.is_not(None))
        if not self.rerun_all_cards:
            stmt = stmt.where(Card.mlb_id.is_(None))
        stmt = stmt.distinct()

        rows = db_session.execute(stmt).yield_per(500)

        processed = 0
        upserted_players = 0
        linked_cards = 0
        no_results = 0
        no_match = 0
        skipped = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            pending: Dict[Any, Tuple[str, str, Dict[str, Any], Tuple[str, str]]] = {}

            def drain(done_futs):
                nonlocal processed, upserted_players, linked_cards, no_results, no_match, skipped

                for fut in done_futs:
                    name, born, profile, group_key = pending.pop(fut)

                    try:
                        people = fut.result()
                    except Exception as e:
                        skipped += 1
                        self.logger.info(f"[ERROR] name='{name}' born='{born}' err='{e}'")
                        continue

                    if not people:
                        self._group_cache[group_key] = None
                        no_results += 1
                        self.logger.info(f"[NO_RESULTS] name='{name}' born='{born}'")
                        continue

                    scored = self._score_all_candidates(name, people, profile)
                    scored.sort(key=lambda x: x[0], reverse=True)
                    if not scored:
                        self._group_cache[group_key] = None
                        no_match += 1
                        self._log_top3_misses(name, born, profile, scored[:3], reason="EMPTY")
                        continue

                    person = self._pick_best_person(name, born, profile, scored)
                    if not person:
                        self._group_cache[group_key] = None
                        no_match += 1
                        self._log_top3_misses(name, born, profile, scored[:3], reason="NO_MATCH")
                        continue

                    mlb_id = person.get("id")
                    if mlb_id is None:
                        skipped += 1
                        continue
                    mlb_id = int(mlb_id)

                    person["_profile"] = profile
                    self._group_cache[group_key] = person

                    if self._upsert_player(db_session, person):
                        upserted_players += 1

                    db_session.flush()

                    res = db_session.execute(
                        update(Card)
                        .where(Card.name == name, Card.born == born, Card.mlb_id.is_(None))
                        .values(mlb_id=mlb_id)
                    )
                    updated_rows = int(res.rowcount or 0)
                    linked_cards += updated_rows

                    self.logger.info(
                        f"[SUCCESS] name='{name}' born='{born}' -> mlb='{person.get('fullName')}' "
                        f"(mlb_id={mlb_id}) cards_linked={updated_rows} two_way={profile['two_way_mode']}"
                    )

                    if processed % self.flush_every == 0:
                        db_session.flush()
                        self.logger.info(
                            f"Progress: processed={processed}, upserted_players={upserted_players}, "
                            f"linked_cards={linked_cards}, no_results={no_results}, no_match={no_match}, skipped={skipped}"
                        )

            for (raw_name, raw_born) in rows:
                processed += 1

                name = (raw_name or "").strip()
                born = (raw_born or "").strip()
                if not name:
                    skipped += 1
                    continue

                group_key = (self._norm_name(name), self._norm(born))
                if group_key in self._group_cache:
                    cached = self._group_cache[group_key]
                    if cached is None:
                        skipped += 1
                    else:
                        profile = cached["_profile"]
                        mlb_id = cached.get("id")
                        if mlb_id is None:
                            skipped += 1
                            continue
                        mlb_id = int(mlb_id)

                        if self._upsert_player(db_session, cached):
                            upserted_players += 1
                        db_session.flush()

                        res = db_session.execute(
                            update(Card)
                            .where(Card.name == name, Card.born == born, Card.mlb_id.is_(None))
                            .values(mlb_id=mlb_id)
                        )
                        linked_cards += int(res.rowcount or 0)

                    if processed % self.flush_every == 0:
                        db_session.flush()
                    continue

                profile = self._load_card_profile(db_session, name, born)
                fut = pool.submit(self._search_people_worker, name)
                pending[fut] = (name, born, profile, group_key)

                if len(pending) >= MAX_WORKERS:
                    done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                    drain(done)

            while pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                drain(done)

        db_session.flush()
        self.logger.info(
            f"Done. processed={processed}, upserted_players={upserted_players}, linked_cards={linked_cards}, "
            f"no_results={no_results}, no_match={no_match}, skipped={skipped}"
        )

    def _search_people_worker(self, name: str) -> List[Dict[str, Any]]:
        time.sleep(random.uniform(*JITTER_RANGE_S))
        client = APIClient()
        url = "https://statsapi.mlb.com/api/v1/people/search"
        params = {"names": [name], "limit": 10, "accent": False}
        res = client.get(url, params)
        people = self._json_get(res, "people", default=[]) or []
        return people

    def _pick_best_person(
        self,
        name: str,
        born: str,
        profile: Dict[str, Any],
        scored: List[Tuple[int, Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        two_way_mode = bool(profile["two_way_mode"])
        name_norm = self._norm_name(name)

        if not two_way_mode:
            role_filtered = [(s, p) for (s, p) in scored if s > -10_000]
            if not role_filtered:
                return None

            exact_role = [
                (s, p) for (s, p) in role_filtered
                if self._norm_name(p.get("fullName") or "") == name_norm
            ]
            if exact_role:
                return exact_role[0][1]
            return role_filtered[0][1]

        if scored[0][0] <= -10_000:
            return None
        return scored[0][1]

    def _load_card_profile(self, session, name: str, born: str) -> Dict[str, Any]:
        rows = session.execute(
            select(Card.is_hitter, Card.height, Card.weight, Card.born)
            .where(Card.name == name, Card.born == born)
        ).all()

        has_hitter = any(bool(r[0]) for r in rows if r[0] is not None)
        has_pitcher = any((r[0] is not None) and (not bool(r[0])) for r in rows)
        two_way_mode = has_hitter and has_pitcher

        heights: List[int] = []
        weights: List[int] = []

        for _, h, w, _b in rows:
            hi = self._height_to_inches(h)
            wi = self._weight_to_lbs(w)
            if hi is not None:
                heights.append(hi)
            if wi is not None:
                weights.append(wi)

        height_in = int(median(heights)) if heights else None
        weight_lb = int(median(weights)) if weights else None

        expected_is_hitter = None
        if not two_way_mode:
            if has_hitter and not has_pitcher:
                expected_is_hitter = True
            elif has_pitcher and not has_hitter:
                expected_is_hitter = False

        return {
            "name": name,
            "born": born,
            "born_norm": self._norm(born),
            "two_way_mode": two_way_mode,
            "expected_is_hitter": expected_is_hitter,
            "card_height_in": height_in,
            "card_weight_lb": weight_lb,
        }

    def _score_all_candidates(
        self,
        query_name: str,
        people: List[Dict[str, Any]],
        profile: Dict[str, Any],
    ) -> List[Tuple[int, Dict[str, Any]]]:
        return [(self._score_candidate(query_name, p, profile), p) for p in people]

    def _score_candidate(self, query_name: str, p: Dict[str, Any], profile: Dict[str, Any]) -> int:
        q = self._norm_name(query_name)
        full = self._norm_name(p.get("fullName") or "")
        first = self._norm(p.get("firstName") or "")
        last = self._norm(p.get("lastName") or "")

        if not full:
            return -10_000

        score = 0

        if full == q:
            score += 140
        else:
            q_parts = q.split()
            if len(q_parts) >= 2 and first == q_parts[0] and last == q_parts[-1]:
                score += 90
            if q_parts and all(part in full for part in q_parts):
                score += 50

        if bool(p.get("active") or False):
            score += 5

        pos = p.get("primaryPosition") or {}
        abbr = (pos.get("abbreviation") or "").strip().upper()
        pos_type = (pos.get("type") or "").strip().lower()
        is_pitcher_like = (abbr in PITCHER_ABBRS) or (pos_type == "pitcher")

        expected_is_hitter = profile.get("expected_is_hitter")
        two_way_mode = bool(profile.get("two_way_mode"))

        if (expected_is_hitter is not None) and (not two_way_mode):
            if expected_is_hitter and is_pitcher_like:
                return -10_000
            if (not expected_is_hitter) and (not is_pitcher_like):
                return -10_000
            score += 25

        score += self._born_score(profile.get("born_norm") or "", p)
        score += self._body_score(profile.get("card_height_in"), profile.get("card_weight_lb"), p)

        return score

    def _born_score(self, card_born_norm: str, p: Dict[str, Any]) -> int:
        if not card_born_norm:
            return 0

        city = self._norm(p.get("birthCity") or "")
        state = self._norm(p.get("birthStateProvince") or "")
        country = self._norm(p.get("birthCountry") or "")

        tokens = [t for t in (city, state, country) if t]
        if not tokens:
            return 0

        hits = sum(1 for t in tokens if t in card_born_norm)
        if hits == 3:
            return 35
        if hits == 2:
            return 20
        if hits == 1:
            return 8
        return 0

    def _body_score(self, card_height_in: Optional[int], card_weight_lb: Optional[int], p: Dict[str, Any]) -> int:
        api_height_in = self._height_to_inches(p.get("height"))
        api_weight_lb = self._weight_to_lbs(p.get("weight"))

        score = 0

        if card_height_in is not None and api_height_in is not None:
            d = abs(card_height_in - api_height_in)
            if d <= 1:
                score += 25
            elif d <= 2:
                score += 18
            elif d <= 4:
                score += 8
            else:
                score -= 10

        if card_weight_lb is not None and api_weight_lb is not None:
            d = abs(card_weight_lb - api_weight_lb)
            if d <= 5:
                score += 25
            elif d <= 10:
                score += 18
            elif d <= 20:
                score += 8
            else:
                score -= 10

        return score

    def _log_top3_misses(
        self,
        name: str,
        born: str,
        profile: Dict[str, Any],
        scored: List[Tuple[int, Dict[str, Any]]],
        reason: str,
    ) -> None:
        parts = []
        for score, p in scored[:3]:
            pos = p.get("primaryPosition") or {}
            abbr = (pos.get("abbreviation") or "").strip().upper()
            ptype = (pos.get("type") or "").strip()
            parts.append(f"{p.get('fullName')} ({abbr or '??'}/{ptype or '??'}, score={score})")

        joined = " | ".join(parts) if parts else "None"
        self.logger.info(
            f"[NO_MATCH:{reason}] name='{name}' born='{born}' two_way={profile.get('two_way_mode')} top3={joined}"
        )

    def _upsert_player(self, session, person: Dict[str, Any]) -> bool:
        mlb_id = person.get("id")
        if mlb_id is None:
            return False
        mlb_id = int(mlb_id)

        birth_date = self._parse_date(person.get("birthDate"))
        if birth_date is None:
            return False

        pos = person.get("primaryPosition") or {}
        position_id = self._upsert_position(session, pos)

        birth_location_id = self._get_or_create_birth_location_id(session, person)

        bat_side = person.get("batSide") or {}
        pitch_hand = person.get("pitchHand") or {}

        strike_zone_top = person.get("strikeZoneTop")
        strike_zone_bottom = person.get("strikeZoneBottom")

        existing = session.get(Player, mlb_id)
        is_new = existing is None

        player = Player(
            mlb_id=mlb_id,
            full_name=(person.get("fullName") or ""),
            first_name=(person.get("firstName") or ""),
            last_name=(person.get("lastName") or ""),
            number=(person.get("primaryNumber") or ""),
            birth_date=birth_date,
            current_age=int(person.get("currentAge") or 0),
            birth_location_id=birth_location_id,
            height=person.get("height"),
            weight=str(person.get("weight")) if person.get("weight") is not None else None,
            active=bool(person.get("active") or False),
            current_team_id=None,
            position_id=position_id,
            boxscore_name=(person.get("boxscoreName") or ""),
            draft_year=person.get("draftYear"),
            mlb_debut_date=self._parse_date(person.get("mlbDebutDate")),
            bat_side_code=(bat_side.get("code") or ""),
            pitch_hand_code=(pitch_hand.get("code") or ""),
            strike_zone_top=str(strike_zone_top) if strike_zone_top is not None else "",
            strike_zone_bottom=str(strike_zone_bottom) if strike_zone_bottom is not None else "",
        )

        session.merge(player)

        if is_new:
            self.logger.info(f"[PLAYER_UPSERT][CREATED] mlb_id={mlb_id} name='{player.full_name}'")
        else:
            self.logger.info(f"[PLAYER_UPSERT][UPDATED] mlb_id={mlb_id} name='{player.full_name}'")

        return True

    def _upsert_position(self, session, pos: Dict[str, Any]) -> int:
        code = (pos.get("code") or "").strip()
        if not code:
            return 0

        pos_id = int(code) if code.isdigit() else 0
        if pos_id == 0:
            return 0

        name = (pos.get("name") or "").strip() or str(pos_id)
        abbr = (pos.get("abbreviation") or "").strip()

        session.merge(MLBPosition(id=pos_id, name=name, abbreviation=abbr))
        session.flush()
        return pos_id

    def _ensure_unknown_position(self, session) -> None:
        existing = session.get(MLBPosition, 0)
        if existing is None:
            session.merge(MLBPosition(id=0, name="Unknown", abbreviation=""))
            session.flush()

    def _get_or_create_birth_location_id(self, session, person: Dict[str, Any]) -> Optional[int]:
        city = (person.get("birthCity") or "").strip()
        state = person.get("birthStateProvince")
        state = state.strip() if isinstance(state, str) else state
        country = (person.get("birthCountry") or "").strip()

        if not city or not country:
            return None

        stmt = select(BirthLocation).where(
            BirthLocation.city == city,
            BirthLocation.country == country,
        )
        if state is None:
            stmt = stmt.where(BirthLocation.state_province.is_(None))
        else:
            stmt = stmt.where(BirthLocation.state_province == state)

        existing = session.execute(stmt).scalars().first()
        if existing:
            return existing.id

        loc = BirthLocation(city=city, state_province=state, country=country)
        session.add(loc)
        session.flush()
        return loc.id

    def _parse_date(self, value: Any) -> Optional[datetime.date]:
        if not value:
            return None
        if isinstance(value, datetime.date):
            return value
        try:
            return datetime.date.fromisoformat(str(value))
        except Exception:
            return None

    def _norm(self, s: str) -> str:
        if not s:
            return ""
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.lower().strip()
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _norm_name(self, s: str) -> str:
        base = self._norm(s)
        if not base:
            return ""

        tokens = base.split()
        joined: List[str] = []
        i = 0
        while i < len(tokens):
            if len(tokens[i]) == 1:
                j = i
                buf = []
                while j < len(tokens) and len(tokens[j]) == 1:
                    buf.append(tokens[j])
                    j += 1
                if len(buf) >= 2:
                    joined.append("".join(buf))
                else:
                    joined.append(buf[0])
                i = j
            else:
                joined.append(tokens[i])
                i += 1

        return " ".join(joined)

    def _height_to_inches(self, h: Any) -> Optional[int]:
        if not h:
            return None
        s = str(h).strip()
        m = re.search(r"(\d+)\s*'\s*(\d+)", s)
        if not m:
            return None
        ft = int(m.group(1))
        inch = int(m.group(2))
        return ft * 12 + inch

    def _weight_to_lbs(self, w: Any) -> Optional[int]:
        if w is None or w == "":
            return None
        if isinstance(w, (int, float)):
            return int(w)
        s = str(w).strip().lower()
        m = re.search(r"(\d+)", s)
        return int(m.group(1)) if m else None
