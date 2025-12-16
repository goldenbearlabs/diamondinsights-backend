from src.jobs.base import BaseJob 
from src.core.config import THE_SHOW_YEARS
from src.adapters.card_adapter import CardAdapter
from src.database.models import Series, Quirk, Location, Card
from typing import List, Dict
from sqlalchemy import select

class CardSync(BaseJob):
    
    def __init__(self, reload_all_years: bool = True):
        super().__init__()
        self.set_child_instance(self)
        self.reload_all_years = reload_all_years


    def execute(self, db_session):

        self.logger.info("Starting CardSync Execute...")

        if self.reload_all_years:
            years_to_process = THE_SHOW_YEARS
        else:
            years_to_process = [THE_SHOW_YEARS[0]]

        raw_items_map = {}
        for year in years_to_process:
            self.logger.info(f"Fetching data for year {year}")
            url = f"https://mlb{year}.theshow.com/apis/items.json"
            params = {
                "type": "mlb_card",
            }
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

        batch_size = 500
        for i, card in enumerate(cards_to_process):
            db_session.merge(card)
            
            if (i + 1) % batch_size == 0:
                db_session.commit()
                db_session.expunge_all()
                self.logger.info(f"Committed batch {i+1}")

        db_session.commit()
        self.logger.info("Sync Complete.")

    def _sync_series(self, session, raw_data) -> Dict[str, Series]:
        """Extracts unique series, upserts them, returns dict {name: SeriesObj}"""
        unique_series = {}
        
        for item in raw_data:
            s_name = item.get("series", "")
            if s_name:
                unique_series[s_name] = {"name": s_name}

        for name, data in unique_series.items():
            session.merge(Series(**data))
        
        session.flush()

        stmt = select(Series)
        results = session.execute(stmt).scalars().all()
        final_map = {}
        for s in results:
            session.expunge(s)
            final_map[s.name] = s
        return final_map

    def _sync_quirks(self, session, raw_data) -> Dict[str, Quirk]:
        """Extracts unique quirks, upserts, returns dict {name: QuirkObj}"""
        unique_quirks = {}

        for item in raw_data:
            quirks_list = item.get("quirks", [])
            for q in quirks_list:
                name = q.get("name")
                if name and name not in unique_quirks:
                    unique_quirks[name] = {
                        "name": name,
                        "description": q.get("description", ""),
                        "img": q.get("img", "")
                    }

        for q_data in unique_quirks.values():
            session.merge(Quirk(**q_data))
        
        session.flush()

        stmt = select(Quirk)
        results = session.execute(stmt).scalars().all()
        final_map = {}
        for q in results:
            session.expunge(q)
            final_map[q.name] = q
        return final_map
    
    def _sync_locations(self, session, raw_data) -> Dict[str, Location]:
        """Extracts unique locations, upserts, returns dict {name: LocationObj}"""
        unique_locs = set()
        
        for item in raw_data:
            locs = item.get("locations", []) 
            for l in locs:
                if l: unique_locs.add(l)

        for loc_name in unique_locs:
            session.merge(Location(name=loc_name))
        
        session.flush()

        stmt = select(Location)
        results = session.execute(stmt).scalars().all()
        final_map = {}
        for l in results:
            session.expunge(l)
            final_map[l.name] = l
        return final_map

    def fetch_paginated_data(self, url: str, params: Dict) -> List:
        """
            Fetches paginated data from the show api
            params:
                url: Full url path for the api call
                params: url params for the fetch
            returns: List containing all raw json objects
        """
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