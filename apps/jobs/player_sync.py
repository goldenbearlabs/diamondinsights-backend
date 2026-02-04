from __future__ import annotations

import datetime
import random
import re
import time
import unicodedata
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, update

from shared.core.http_client import APIClient
from shared.db.models import BirthLocation, Card, MLBPosition, Player
from apps.jobs.job import Job


PITCHER_ABBRS = {"P", "SP", "RP", "CP"}
MAX_WORKERS = 6
JITTER_RANGE_S = (0.05, 0.45)

FALLBACK_ENABLED = True
FALLBACK_MAX_CANDIDATES_PER_NAME = 75


class PlayerSync(Job):
    def __init__(self, rerun_all_cards: bool = False, flush_every: int = 200):
        super().__init__()
        self.rerun_all_cards = rerun_all_cards
        self.flush_every = flush_every
        self._group_cache: Dict[Tuple[str, str], Optional[Dict[str, Any]]] = {}
        self._sports_cache: Optional[List[Dict[str, Any]]] = None
        self._fallback_name_index: Optional[Dict[str, List[Dict[str, Any]]]] = None

    def run(self, db_session):
        self._log_start(rerun_all_cards=self.rerun_all_cards, flush_every=self.flush_every)
        self._ensure_unknown_position(db_session)

        processed = 0
        upserted_players = 0
        linked_cards = 0
        no_results = 0
        no_match = 0
        skipped = 0

        stmt = select(Card.name, Card.born).where(Card.name.is_not(None))
        if not self.rerun_all_cards:
            stmt = stmt.where(Card.mlb_id.is_(None))
        stmt = stmt.distinct()

        rows = db_session.execute(stmt).yield_per(500)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            pending: Dict[Any, Tuple[Any, Any, str, Dict[str, Any], Tuple[str, str]]] = {}

            def drain(done_futs):
                nonlocal processed, upserted_players, linked_cards, no_results, no_match, skipped

                for fut in done_futs:
                    raw_name, raw_born, query_name, profile, group_key = pending.pop(fut)

                    try:
                        people = fut.result()
                    except Exception:
                        skipped += 1
                        continue

                    if not people:
                        self._group_cache[group_key] = None
                        no_results += 1
                        continue

                    scored = self._score_all_candidates(query_name, people, profile)
                    scored.sort(key=lambda x: x[0], reverse=True)
                    if not scored:
                        self._group_cache[group_key] = None
                        no_match += 1
                        continue

                    person = self._pick_best_person(query_name, profile.get("born") or "", profile, scored)
                    if not person:
                        self._group_cache[group_key] = None
                        no_match += 1
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
                        .where(*self._card_key_filter(raw_name, raw_born), Card.mlb_id.is_(None))
                        .values(mlb_id=mlb_id)
                    )
                    linked_cards += int(res.rowcount or 0)

                    if processed % self.flush_every == 0:
                        db_session.flush()

            for (raw_name, raw_born) in rows:
                processed += 1

                if raw_name is None:
                    skipped += 1
                    continue

                query_name = str(raw_name).strip()
                if not query_name:
                    skipped += 1
                    continue

                born_for_key = ""
                if raw_born is not None:
                    born_for_key = str(raw_born).strip()

                group_key = (self._norm_name(query_name), self._norm(born_for_key))

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
                            .where(*self._card_key_filter(raw_name, raw_born), Card.mlb_id.is_(None))
                            .values(mlb_id=mlb_id)
                        )
                        linked_cards += int(res.rowcount or 0)

                    if processed % self.flush_every == 0:
                        db_session.flush()
                    continue

                profile = self._load_card_profile(db_session, raw_name, raw_born)
                fut = pool.submit(self._search_people_worker, query_name)
                pending[fut] = (raw_name, raw_born, query_name, profile, group_key)

                if len(pending) >= MAX_WORKERS:
                    done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                    drain(done)

            while pending:
                done, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                drain(done)

        db_session.flush()
        db_session.commit()

        fallback_processed = 0
        fallback_upserted_players = 0
        fallback_linked_cards = 0
        fallback_no_results = 0
        fallback_no_match = 0
        fallback_skipped = 0

        if FALLBACK_ENABLED:
            stmt2 = select(Card.name, Card.born).where(Card.name.is_not(None), Card.mlb_id.is_(None)).distinct()
            remaining = db_session.execute(stmt2).all()

            if remaining:
                client = APIClient()
                self._ensure_fallback_name_index(client)

                for (raw_name, raw_born) in remaining:
                    fallback_processed += 1

                    if raw_name is None:
                        fallback_skipped += 1
                        continue

                    query_name = str(raw_name).strip()
                    if not query_name:
                        fallback_skipped += 1
                        continue

                    born_for_key = ""
                    if raw_born is not None:
                        born_for_key = str(raw_born).strip()

                    group_key = (self._norm_name(query_name), self._norm(born_for_key))

                    cached = self._group_cache.get(group_key)
                    person: Optional[Dict[str, Any]] = None

                    if cached is not None:
                        if cached is None:
                            person = None
                        else:
                            person = cached

                    if person is None:
                        if not self._fallback_name_index:
                            fallback_no_results += 1
                            self._group_cache[group_key] = None
                            continue

                        people = self._fallback_name_index.get(self._norm_name(query_name), [])
                        if not people:
                            fallback_no_results += 1
                            self._group_cache[group_key] = None
                            continue

                        profile = self._load_card_profile(db_session, raw_name, raw_born)

                        scored = self._score_all_candidates(query_name, people, profile)
                        scored.sort(key=lambda x: x[0], reverse=True)
                        if not scored:
                            fallback_no_match += 1
                            self._group_cache[group_key] = None
                            continue

                        person = self._pick_best_person(query_name, profile.get("born") or "", profile, scored)
                        if not person:
                            fallback_no_match += 1
                            self._group_cache[group_key] = None
                            continue

                    mlb_id = person.get("id")
                    if mlb_id is None:
                        fallback_skipped += 1
                        self._group_cache[group_key] = None
                        continue
                    mlb_id = int(mlb_id)

                    profile = self._load_card_profile(db_session, raw_name, raw_born)
                    person["_profile"] = profile
                    self._group_cache[group_key] = person

                    if self._upsert_player(db_session, person):
                        fallback_upserted_players += 1

                    db_session.flush()

                    res = db_session.execute(
                        update(Card)
                        .where(*self._card_key_filter(raw_name, raw_born), Card.mlb_id.is_(None))
                        .values(mlb_id=mlb_id)
                    )
                    fallback_linked_cards += int(res.rowcount or 0)

                    if fallback_processed % self.flush_every == 0:
                        db_session.flush()

                db_session.flush()
                db_session.commit()

        self._log_end(
            processed=processed,
            upserted_players=upserted_players,
            linked_cards=linked_cards,
            no_results=no_results,
            no_match=no_match,
            skipped=skipped,
            fallback_processed=fallback_processed,
            fallback_upserted_players=fallback_upserted_players,
            fallback_linked_cards=fallback_linked_cards,
            fallback_no_results=fallback_no_results,
            fallback_no_match=fallback_no_match,
            fallback_skipped=fallback_skipped,
        )

    def _card_key_filter(self, raw_name: Any, raw_born: Any):
        conds = [Card.name == raw_name]
        if raw_born is None:
            conds.append(Card.born.is_(None))
        else:
            conds.append(Card.born == raw_born)
        return conds

    def _search_people_worker(self, name: str) -> List[Dict[str, Any]]:
        time.sleep(random.uniform(*JITTER_RANGE_S))
        client = APIClient()
        url = "https://statsapi.mlb.com/api/v1/people/search"

        search_name = self._api_search_name(name)
        params = {"names": [search_name], "limit": 10, "accent": False}

        res = client.get(url, params)
        people = self._json_get(res, "people", default=[]) or []
        return people

    def _api_search_name(self, s: str) -> str:
        if not s:
            return ""
        s = unicodedata.normalize("NFKD", s)
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = s.strip()
        s = re.sub(r"[^\w\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        if not s:
            return ""

        tokens = s.split()
        buf = []
        i = 0
        while i < len(tokens) and len(tokens[i]) == 1 and tokens[i].isalpha():
            buf.append(tokens[i])
            i += 1

        if len(buf) >= 2:
            tokens = ["".join(buf)] + tokens[i:]

        return " ".join(tokens)

    def _ensure_fallback_name_index(self, client: APIClient) -> None:
        if self._fallback_name_index is not None:
            return

        sports = self._get_sports(client)

        idx: Dict[str, List[Dict[str, Any]]] = {}
        for s in sports:
            sid = s.get("id")
            if sid is None:
                continue
            try:
                league_id = int(sid)
            except Exception:
                continue

            time.sleep(random.uniform(*JITTER_RANGE_S))
            url = f"https://statsapi.mlb.com/api/v1/sports/{league_id}/players"
            res = client.get(url, params={})
            people = self._json_get(res, "people", default=[]) or []

            for p in people:
                full = self._norm_name(p.get("fullName") or "")
                if not full:
                    continue
                lst = idx.get(full)
                if lst is None:
                    idx[full] = [p]
                else:
                    if FALLBACK_MAX_CANDIDATES_PER_NAME and len(lst) >= FALLBACK_MAX_CANDIDATES_PER_NAME:
                        continue
                    lst.append(p)

        self._fallback_name_index = idx

    def _get_sports(self, client: APIClient) -> List[Dict[str, Any]]:
        if self._sports_cache is not None:
            return self._sports_cache

        time.sleep(random.uniform(*JITTER_RANGE_S))
        res = client.get("https://statsapi.mlb.com/api/v1/sports", params={})
        sports = self._json_get(res, "sports", default=[]) or []
        self._sports_cache = sports
        return sports

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

            exact_role = [(s, p) for (s, p) in role_filtered if self._norm_name(p.get("fullName") or "") == name_norm]
            if exact_role:
                return exact_role[0][1]
            return role_filtered[0][1]

        if scored[0][0] <= -10_000:
            return None
        return scored[0][1]

    def _load_card_profile(self, session, raw_name: Any, raw_born: Any) -> Dict[str, Any]:
        rows = session.execute(
            select(Card.is_hitter, Card.height, Card.weight, Card.born).where(*self._card_key_filter(raw_name, raw_born))
        ).all()

        name_str = str(raw_name).strip() if raw_name is not None else ""
        born_str = str(raw_born).strip() if raw_born is not None else ""

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
            "name": name_str,
            "born": born_str,
            "born_norm": self._norm(born_str),
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

        stmt = select(BirthLocation).where(BirthLocation.city == city, BirthLocation.country == country)
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
        out: List[str] = []
        i = 0

        while i < len(tokens):
            if len(tokens[i]) == 1 and tokens[i].isalpha():
                j = i
                buf: List[str] = []
                while j < len(tokens) and len(tokens[j]) == 1 and tokens[j].isalpha():
                    buf.append(tokens[j])
                    j += 1
                if len(buf) >= 2:
                    out.append("".join(buf))
                else:
                    out.append(buf[0])
                i = j
            else:
                out.append(tokens[i])
                i += 1

        return " ".join(out)

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
