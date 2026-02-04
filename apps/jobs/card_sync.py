from apps.jobs.job import Job
from shared.core.config import THE_SHOW_YEARS
from shared.db.models import Series, Quirk, Location, Card, Pitch

from typing import List, Dict, Optional
from sqlalchemy import select, text, inspect as sa_inspect, delete
from sqlalchemy.dialects.postgresql import insert
import unicodedata

from sqlalchemy.orm import Session


class CardSync(Job):
    def __init__(self, reload_all_years: bool = False, base_url_template: Optional[str] = None):
        super().__init__()
        self.reload_all_years = reload_all_years
        self.base_url_template = base_url_template or "https://mlb{year}.theshow.com"

    def _items_url(self, year: int) -> str:
        base_url = self.base_url_template.replace("{year}", str(year))
        return f"{base_url}/apis/items.json"

    def run(self, db_session: Session):

        years_to_process = THE_SHOW_YEARS if self.reload_all_years else [THE_SHOW_YEARS[0]]
        self._log_start(reload_all_years=self.reload_all_years, years=years_to_process)

        raw_items_map = {}
        for year in years_to_process:
            url = self._items_url(year)
            params = {"type": "mlb_card"}
            data = self._fetch_paginated_data(url, params)
            self.logger.info("cards fetched year=%s count=%s", year, len(data))

            for item in data:
                source_uuid = item.get("uuid")
                if not source_uuid:
                    continue
                    
                derived_id = f"{year}:{source_uuid}"
                raw_items_map[derived_id] = item
                raw_items_map[derived_id]["year"] = year
                raw_items_map[derived_id]["source_uuid"] = source_uuid

        all_unique_items = list(raw_items_map.values())

        series_map = self._sync_series(db_session, all_unique_items)
        quirk_map = self._sync_quirks(db_session, all_unique_items)
        location_map = self._sync_locations(db_session, all_unique_items)

        card_adapter = CardAdapter(series_map, quirk_map, location_map)
        cards_to_process = card_adapter.run(all_unique_items)
        self.logger.info("cards prepared count=%s", len(cards_to_process))

        self._upsert_cards(db_session, cards_to_process, chunk_size=5000)
        self._upsert_pitches(db_session, cards_to_process, chunk_size=5000)
        self._log_end(cards_upserted=len(cards_to_process))

    def _upsert_cards(self, session: Session, cards: List[Card], chunk_size: int = 5000) -> None:
        mapper = sa_inspect(Card).mapper
        col_names = [c.key for c in mapper.column_attrs]
        cols = Card.__table__.columns

        # Preserve existing mlb_id links; card_sync should not unlink players.
        update_cols = {
            c.name: insert(Card).excluded[c.name]
            for c in cols
            if c.name not in {"id", "mlb_id"}
        }

        total = len(cards)
        for start in range(0, total, chunk_size):
            chunk = cards[start : start + chunk_size]
            rows = [{k: getattr(obj, k) for k in col_names} for obj in chunk]

            session.execute(text("SET LOCAL synchronous_commit TO OFF"))

            stmt = insert(Card).values(rows).on_conflict_do_update(
                index_elements=["id"],
                set_=update_cols,
            )
            session.execute(stmt)
            session.commit()

    def _upsert_pitches(self, session: Session, cards: List[Card], chunk_size: int = 5000) -> None:
        total = len(cards)

        set_cols = {
            "speed": insert(Pitch).excluded.speed,
            "control": insert(Pitch).excluded.control,
            "movement": insert(Pitch).excluded.movement,
        }

        for start in range(0, total, chunk_size):
            chunk = cards[start : start + chunk_size]
            card_ids = [c.id for c in chunk if c.id]

            session.execute(text("SET LOCAL synchronous_commit TO OFF"))

            if card_ids:
                session.execute(delete(Pitch).where(Pitch.card_id.in_(card_ids)))

            pitch_rows = []
            for c in chunk:
                for p in (getattr(c, "pitches", None) or []):
                    name = (p.name or "").strip()
                    if not (c.id and name):
                        continue
                    pitch_rows.append(
                        {
                            "card_id": c.id,
                            "name": name,
                            "speed": int(p.speed or 0),
                            "control": int(p.control or 0),
                            "movement": int(p.movement or 0),
                        }
                    )

            if pitch_rows:
                stmt = (
                    insert(Pitch)
                    .values(pitch_rows)
                    .on_conflict_do_update(
                        index_elements=["card_id", "name"],
                        set_=set_cols,
                    )
                )
                session.execute(stmt)

            session.commit()

    def _sync_series(self, session: Session, raw_data) -> Dict[str, Series]:
        unique_series = {}
        unique_series["UNKNOWN"] = {"name": "UNKNOWN"}
        for item in raw_data:
            s_name = (item.get("series") or "").strip() or "UNKNOWN"
            unique_series[s_name] = {"name": s_name}

        for data in unique_series.values():
            session.merge(Series(**data))

        session.flush()

        results = session.execute(select(Series)).scalars().all()
        final_map = {}
        for s in results:
            session.expunge(s)
            final_map[s.name] = s
        return final_map

    def _sync_quirks(self, session: Session, raw_data) -> Dict[str, Quirk]:
        unique_quirks = {}
        for item in raw_data:
            for q in item.get("quirks", []) or []:
                name = q.get("name")
                if name and name not in unique_quirks:
                    unique_quirks[name] = {
                        "name": name,
                        "description": q.get("description", ""),
                        "img": q.get("img", ""),
                    }

        for q_data in unique_quirks.values():
            session.merge(Quirk(**q_data))

        session.flush()

        results = session.execute(select(Quirk)).scalars().all()
        final_map = {}
        for q in results:
            session.expunge(q)
            final_map[q.name] = q
        return final_map

    def _sync_locations(self, session: Session, raw_data) -> Dict[str, Location]:
        unique_locs = set()
        for item in raw_data:
            for l in item.get("locations", []) or []:
                if l:
                    unique_locs.add(l)

        for loc_name in unique_locs:
            session.merge(Location(name=loc_name))

        session.flush()

        results = session.execute(select(Location)).scalars().all()
        final_map = {}
        for l in results:
            session.expunge(l)
            final_map[l.name] = l
        return final_map

def _normalize_search(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s

class CardAdapter:
    
    def __init__(self, series_map: Dict, quirk_map: Dict, location_map: Dict):
        super().__init__()
        self.series_map = series_map
        self.quirk_map = quirk_map
        self.location_map = location_map

    def _card_id(self, year: int, source_uuid: str) -> str:
        return f"{year}:{source_uuid}"
    
    def _json_get(self, data, key, default=None):
        if not data:
            return default
        return data.get(key, default)

    def run(self, data) -> List[Card]:
        cards = []
        for item in data:
            source_uuid = self._json_get(item, "source_uuid", "") or self._json_get(item, "uuid", "")
            year = self._json_get(item, "year", 0) or 0
            if not source_uuid or not year:
                continue
            
            card = Card()
            card.id = self._card_id(year, source_uuid)
            card.source_uuid = source_uuid
            card.year = self._json_get(item, "year", 0)
            card.name = self._json_get(item, "name", "Unknown")
            card.search_name = _normalize_search(card.name)
            card.ovr = self._json_get(item, "ovr", 0) or 0
            card.type = self._json_get(item, "type", "")
            card.img = self._json_get(item, "img", "")
            card.baked_img = self._json_get(item, "baked_img", "")
            card.short_description = self._json_get(item, "short_description", "")
            card.rarity = self._json_get(item, "rarity", "")
            card.team = self._json_get(item, "team", "")
            card.team_short_name = self._json_get(item, "team_short_name", "")
            card.display_position = self._json_get(item, "display_position", "")
            card.display_secondary_positions = self._json_get(item, "display_secondary_positions", "")
            card.jersey_number = self._json_get(item, "jersey_number", 0) or 0
            card.age = self._json_get(item, "age", 0) or 0
            card.bat_hand = self._json_get(item, "bat_hand", "")
            card.throw_hand = self._json_get(item, "throw_hand", "")
            card.weight = self._json_get(item, "weight", "")
            card.height = self._json_get(item, "height", "")
            card.born = self._json_get(item, "born", "")
            card.is_hitter = self._json_get(item, "is_hitter", False)
            card.stamina = self._json_get(item, "stamina", 0) or 0
            card.pitching_clutch = self._json_get(item, "pitching_clutch", 0) or 0
            card.hits_per_bf = self._json_get(item, "hits_per_bf", 0) or 0
            card.k_per_bf = self._json_get(item, "k_per_bf", 0) or 0
            card.bb_per_bf = self._json_get(item, "bb_per_bf", 0) or 0
            card.hr_per_bf = self._json_get(item, "hr_per_bf", 0) or 0
            card.pitch_velocity = self._json_get(item, "pitch_velocity", 0) or 0
            card.pitch_control = self._json_get(item, "pitch_control", 0) or 0
            card.pitch_movement = self._json_get(item, "pitch_movement", 0) or 0
            card.contact_left = self._json_get(item, "contact_left", 0) or 0
            card.contact_right = self._json_get(item, "contact_right", 0) or 0
            card.power_left = self._json_get(item, "power_left", 0) or 0
            card.power_right = self._json_get(item, "power_right", 0) or 0
            card.plate_vision = self._json_get(item, "plate_vision", 0) or 0
            card.plate_discipline = self._json_get(item, "plate_discipline", 0) or 0
            card.batting_clutch = self._json_get(item, "batting_clutch", 0) or 0
            card.bunting_ability = self._json_get(item, "bunting_ability", 0) or 0
            card.drag_bunting_ability = self._json_get(item, "drag_bunting_ability", 0) or 0
            card.hitting_durability = self._json_get(item, "hitting_durability", 0) or 0
            card.fielding_durability = self._json_get(item, "fielding_durability", 0) or 0
            card.fielding_ability = self._json_get(item, "fielding_ability", 0) or 0
            card.arm_strength = self._json_get(item, "arm_strength", 0) or 0
            card.arm_accuracy = self._json_get(item, "arm_accuracy", 0) or 0
            card.reaction_time = self._json_get(item, "reaction_time", 0) or 0
            card.blocking = self._json_get(item, "blocking", 0) or 0
            card.speed = self._json_get(item, "speed", 0) or 0
            card.baserunning_ability = self._json_get(item, "baserunning_ability", 0) or 0
            card.baserunning_aggression = self._json_get(item, "baserunning_aggression", 0) or 0
            card.hit_rank_image = self._json_get(item, "hit_rank_image", "")
            card.fielding_rank_image = self._json_get(item, "fielding_rank_image", "")
            card.is_sellable = self._json_get(item, "is_sellable", False)
            card.has_augment = self._json_get(item, "has_augment", False)
            card.augment_text = self._json_get(item, "augment_text", "")
            card.augment_end_date = self._json_get(item, "augment_end_date", None) 
            card.has_matchup = self._json_get(item, "has_matchup", False)
            card.stars = self._json_get(item, "stars", "")
            card.trend = self._json_get(item, "trend", "")
            card.new_rank = self._json_get(item, "new_rank", 0) or 0
            card.has_rank_change = self._json_get(item, "has_rank_change", False)
            card.event = self._json_get(item, "event", False)
            card.set_name = self._json_get(item, "set_name", "")
            card.is_live_set = self._json_get(item, "is_live_set", False)
            card.ui_anim_index = self._json_get(item, "ui_anim_index", 0) or 0

            series_name = (self._json_get(item, "series") or "").strip() or "UNKNOWN"
            if series_name and series_name in self.series_map:
                card.series_name = series_name
                card.series = self.series_map[series_name]
            
            item_quirks = self._json_get(item, "quirks", [])
            card_quirks = []
            for q in item_quirks:
                q_name = q.get("name")
                if q_name and q_name in self.quirk_map:
                    card_quirks.append(self.quirk_map[q_name])
            card.quirks = card_quirks

            item_locs = self._json_get(item, "locations", [])
            card_locs = []
            for l_name in item_locs:
                if l_name and l_name in self.location_map:
                    card_locs.append(self.location_map[l_name])
            card.locations = card_locs

            item_pitches = self._json_get(item, "pitches", [])
            pitch_objs = []
            for p in item_pitches:
                new_pitch = Pitch(
                    card_id=card.id,
                    name=p.get("name"),
                    speed=p.get("speed", 0),
                    control=p.get("control", 0),
                    movement=p.get("movement", 0)
                )
                pitch_objs.append(new_pitch)
            card.pitches = pitch_objs

            cards.append(card)

        return cards
