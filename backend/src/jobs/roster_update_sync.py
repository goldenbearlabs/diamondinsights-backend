from datetime import datetime
from src.jobs.base import BaseJob
from src.core.config import THE_SHOW_YEARS, MAJOR_ROSTER_UPDATES, FIELDING_ROSTER_UPDATES
from typing import Dict, List
from sqlalchemy import select
from sqlalchemy.orm import Session
import time


from src.database.models import RosterUpdate, CardUpdate, CardAttributeChange


class RosterUpdateSync(BaseJob):
    def __init__(self, reload_all_years: bool = True):
        super().__init__()
        self.set_child_instance(self)
        self.reload_all_years = reload_all_years

    def execute(self, db_session):

        self.logger.info("Starting Roster Update Sync...")

        if self.reload_all_years:
            years_to_process = THE_SHOW_YEARS
        else:
            years_to_process = [THE_SHOW_YEARS[0]]

        for year in years_to_process:
            self.logger.info(f"Fetching data for year {year}")
            url = f"https://mlb{year}.theshow.com/apis/roster_updates.json"
            raw_data = self.api_client.get(url)

            roster_updates_map = self.sync_roster_updates(db_session, year, raw_data)
            for update_obj in roster_updates_map.values():
                self.logger.info(f"Syncing details for Update {update_obj.id} on {update_obj.date}")

                if update_obj.date.year != (2000 + year):
                    self.logger.warning(
                        f"Year mismatch: processing mlb{year} but update {update_obj.id} has date {update_obj.date}"
                    )
            
                self.sync_update_details(db_session, year, update_obj.id, update_obj.date)
                
                time.sleep(1)

    def sync_roster_updates(self, session: Session, year: int, raw_data) -> Dict[int, RosterUpdate]:
        data = self._json_get(raw_data, "roster_updates", [])

        final_map: Dict[int, RosterUpdate] = {}

        for item in data:
            update_id = self._json_get(item, "id", 0)
            date_str = self._json_get(item, "name", "")

            if year == 23 and update_id == 15:
                continue
            if year == 21 and update_id == 11:
                continue

            try:
                date_obj = datetime.strptime(date_str, "%B %d, %Y").date()
            except ValueError:
                self.logger.warning(f"Could not parse date: {date_str}")
                continue

            major_update = update_id in MAJOR_ROSTER_UPDATES.get(year, [])
            fielding_update = update_id in FIELDING_ROSTER_UPDATES.get(year, [])

            ru = session.merge(
                RosterUpdate(
                    id=update_id,
                    date=date_obj,
                    is_major=major_update,
                    is_fielding=fielding_update,
                )
            )

            final_map[update_id] = ru

        session.flush()

        for ru in final_map.values():
            session.expunge(ru)

        self.logger.info(f"Synced {len(final_map)} roster updates for year {year}")
        return final_map
    
    def sync_update_details(self, session, year, update_id, update_date):
        """Fetches the specific update details and populates CardUpdate + AttributeChanges"""
        url = f"https://mlb{year}.theshow.com/apis/roster_update.json"
        params = {"id": update_id}
        
        data = self.api_client.get(url, params)
        attribute_changes = self._json_get(data, "attribute_changes", [])

        if not attribute_changes:
            self.logger.info(f"No attribute changes found for update {update_id}")
            return

        for item in attribute_changes:
            card_data = self._json_get(item, "item", {})
            card_id = self._json_get(card_data, "uuid", "")
            
            if not card_id:
                continue

            card_update = CardUpdate(
                update_id=update_id,
                update_date=update_date,
                card_id=card_id,
                new_ovr=self._json_get(item, "current_rank", 0),
                old_ovr=self._json_get(item, "old_rank", 0),
                new_rarity=self._json_get(item, "current_rarity", ""),
                old_rarity=self._json_get(item, "old_rarity", ""),
                trend_display=self._json_get(item, "trend_display", "")
            )

            changes_list = self._json_get(item, "changes", [])
            attr_change_objects = []

            for change in changes_list:
                current_val_str = self._json_get(change, "current_value", "0")
                delta_str = self._json_get(change, "delta", "0")
                try:
                    current_val = int(current_val_str)
                    delta_val = int(delta_str.strip().replace("+", "")) 
                    old_val = current_val - delta_val
                except ValueError:
                    current_val = 0
                    old_val = 0

                attr_change = CardAttributeChange(
                    name=self._json_get(change, "name", ""),
                    new_value=current_val,
                    old_value=old_val,
                    direction=self._json_get(change, "direction", ""),
                    delta=delta_str,
                    color=self._json_get(change, "color", "")
                )
                attr_change_objects.append(attr_change)

            card_update.attribute_changes = attr_change_objects

            session.merge(card_update)

        try:
            session.commit()
        except Exception as e:
            self.logger.error(f"Failed to commit update {update_id}: {e}")
            session.rollback()    