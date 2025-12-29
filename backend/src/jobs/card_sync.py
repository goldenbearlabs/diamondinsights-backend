from src.jobs.base import BaseJob
from src.core.config import THE_SHOW_YEARS
from src.adapters.card_adapter import CardAdapter
from src.database.models import Series, Quirk, Location, Card

from typing import List, Dict
from sqlalchemy import select, text, inspect as sa_inspect
from sqlalchemy.dialects.postgresql import insert


class CardSync(BaseJob):
    def __init__(self, reload_all_years: bool = True):
        super().__init__()
        self.set_child_instance(self)
        self.reload_all_years = reload_all_years

    def execute(self, db_session):
        self.logger.info("Starting CardSync Execute...")

        years_to_process = THE_SHOW_YEARS if self.reload_all_years else [THE_SHOW_YEARS[0]]

        raw_items_map = {}
        for year in years_to_process:
            self.logger.info(f"Fetching data for year {year}")
            url = f"https://mlb{year}.theshow.com/apis/items.json"
            params = {"type": "mlb_card"}
            data = self.fetch_paginated_data(url, params)

            for item in data:
                uuid = item.get("uuid")
                if uuid:
                    raw_items_map[uuid] = item
                    raw_items_map[uuid]["year"] = year

            self.logger.info(f"Done fetching year {year}. Unique items so far: {len(raw_items_map)}")

        all_unique_items = list(raw_items_map.values())

        series_map = self._sync_series(db_session, all_unique_items)
        quirk_map = self._sync_quirks(db_session, all_unique_items)
        location_map = self._sync_locations(db_session, all_unique_items)

        self.logger.info("Done syncing data for card relations")

        card_adapter = CardAdapter(series_map, quirk_map, location_map)
        cards_to_process = card_adapter.run(all_unique_items)

        self._upsert_cards(db_session, cards_to_process, chunk_size=5000)

        self.logger.info("Sync Complete.")

    def _upsert_cards(self, session, cards: List[Card], chunk_size: int = 5000) -> None:
        mapper = sa_inspect(Card).mapper
        col_names = [c.key for c in mapper.column_attrs]
        cols = Card.__table__.columns

        update_cols = {c.name: insert(Card).excluded[c.name] for c in cols if c.name != "id"}

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

            self.logger.info(f"Upserted cards: {min(start + chunk_size, total)}/{total}")

    def _sync_series(self, session, raw_data) -> Dict[str, Series]:
        unique_series = {}
        for item in raw_data:
            s_name = item.get("series", "")
            if s_name:
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

    def _sync_quirks(self, session, raw_data) -> Dict[str, Quirk]:
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

    def _sync_locations(self, session, raw_data) -> Dict[str, Location]:
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

    def fetch_paginated_data(self, url: str, params: Dict) -> List:
        page = 1
        params["page"] = page
        res = self.api_client.get(url, params)

        max_pages = self._json_get(res, "total_pages", default=0)
        fetched_objects = []

        while page <= max_pages:
            self.logger.info(f"fetching page {page} of {max_pages}")
            items = self._json_get(res, "items", default=[])
            fetched_objects.extend(items)

            page += 1
            params["page"] = page
            res = self.api_client.get(url, params)

        return fetched_objects
