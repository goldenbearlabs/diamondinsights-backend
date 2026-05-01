from datetime import datetime
from apps.jobs.job import Job
from shared.core.config import THE_SHOW_YEARS, MAJOR_ROSTER_UPDATES, FIELDING_ROSTER_UPDATES
from typing import Dict
from sqlalchemy import select
from sqlalchemy.orm import Session
import time

from shared.db.models import RosterUpdate, CardUpdate, CardAttributeChange


class RosterUpdateSync(Job):
    def __init__(self, reload_all_years: bool = False):
        super().__init__()
        self.reload_all_years = reload_all_years

    def _card_id(self, year: int, source_uuid: str) -> str:
        return f"{year}:{source_uuid}"

    def run(self, db_session):

        years_to_process = THE_SHOW_YEARS if self.reload_all_years else [THE_SHOW_YEARS[0]]
        self._log_start(reload_all_years=self.reload_all_years, years=years_to_process)
        total_updates = 0
        skipped_existing_details = 0

        for year in years_to_process:
            url = f"https://mlb{year}.theshow.com/apis/roster_updates.json"
            raw_data = self._api_client.get(url)

            roster_updates_map = self.sync_roster_updates(db_session, year, raw_data)
            self.logger.info("roster updates fetched year=%s count=%s", year, len(roster_updates_map))
            total_updates += len(roster_updates_map)
            for update_obj in roster_updates_map.values():

                self.logger.info(
                    "roster update details start year=%s update_id=%s date=%s",
                    year,
                    update_obj.id,
                    update_obj.date,
                )
                if not self.reload_all_years and self._has_update_details(db_session, update_obj.id, update_obj.date):
                    skipped_existing_details += 1
                    self.logger.info(
                        "roster update details skipped existing year=%s update_id=%s date=%s",
                        year,
                        update_obj.id,
                        update_obj.date,
                    )
                    continue
                self.sync_update_details(db_session, year, update_obj.id, update_obj.date)
                time.sleep(1)
            self.logger.info("roster update details complete year=%s count=%s", year, len(roster_updates_map))
        self._log_end(total_updates=total_updates, skipped_existing_details=skipped_existing_details)

    def _has_update_details(self, session: Session, update_id: int, update_date) -> bool:
        try:
            stmt = (
                select(CardUpdate.card_id)
                .where(CardUpdate.update_id == update_id, CardUpdate.update_date == update_date)
                .limit(1)
            )
            return session.execute(stmt).first() is not None
        except Exception:
            self.logger.warning("_has_update_details check failed update_id=%s", update_id, exc_info=True)
            return False

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

        return final_map

    def sync_update_details(self, session: Session, year: int, update_id: int, update_date) -> None:
        url = f"https://mlb{year}.theshow.com/apis/roster_update.json"
        params = {"id": update_id}

        data = self._api_client.get(url, params)
        attribute_changes = self._json_get(data, "attribute_changes", [])

        if not attribute_changes:
            self.logger.info(
                "roster update details empty year=%s update_id=%s",
                year,
                update_id,
            )
            return
        self.logger.info(
            "roster update details fetched year=%s update_id=%s changes=%s",
            year,
            update_id,
            len(attribute_changes),
        )

        for item in attribute_changes:
            card_data = self._json_get(item, "item", {})
            source_uuid = self._json_get(card_data, "uuid", "")
            if not source_uuid:
                continue

            card_id = self._card_id(year, source_uuid)

            card_update = CardUpdate(
                update_id=update_id,
                update_date=update_date,
                card_id=card_id,
                new_ovr=self._json_get(item, "current_rank", 0),
                old_ovr=self._json_get(item, "old_rank", 0),
                new_rarity=self._json_get(item, "current_rarity", ""),
                old_rarity=self._json_get(item, "old_rarity", ""),
                trend_display=self._json_get(item, "trend_display", ""),
            )

            changes_list = self._json_get(item, "changes", []) or []
            attr_change_objects = []

            for change in changes_list:
                current_val_str = self._json_get(change, "current_value", "0")
                delta_str = self._json_get(change, "delta", "0")

                try:
                    current_val = int(current_val_str)
                    delta_val = int(str(delta_str).strip().replace("+", ""))
                    old_val = current_val - delta_val
                except ValueError:
                    current_val = 0
                    old_val = 0

                attr_change_objects.append(
                    CardAttributeChange(
                        update_id=update_id,
                        update_date=update_date,
                        card_id=card_id,
                        name=self._json_get(change, "name", ""),
                        new_value=current_val,
                        old_value=old_val,
                        direction=self._json_get(change, "direction", ""),
                        delta=delta_str,
                        color=self._json_get(change, "color", ""),
                    )
                )

            card_update.attribute_changes = attr_change_objects
            session.merge(card_update)

        try:
            session.commit()
        except Exception as e:
            session.rollback()
