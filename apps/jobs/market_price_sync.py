from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Set

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from shared.core.config import THE_SHOW_YEARS
from shared.db.models import Card, Listing


class MarketPriceSync(Job):
    def __init__(self, *, chunk_size: Optional[int] = None):
        super().__init__()
        self.year = THE_SHOW_YEARS[0]
        self.chunk_size = max(1, chunk_size or int(os.getenv("MARKET_PRICE_SYNC_CHUNK_SIZE", "500")))

    def run(self, db_session: Session):
        self._log_start(year=self.year)

        card_ids = self._get_card_ids(db_session)
        if not card_ids:
            self._log_end(cards=0, pages=0, listings=0)
            return

        total_pages = 0
        total_listings = 0
        listing_rows: List[Dict[str, Any]] = []

        page = 1
        while True:
            payload = self._fetch_listings_page(page)
            if not payload:
                break

            if total_pages == 0:
                total_pages = int(payload.get("total_pages") or 0)
                if total_pages <= 0:
                    break

            listings = payload.get("listings") or []
            if not listings:
                if page >= total_pages:
                    break
                page += 1
                continue

            for listing in listings:
                item = listing.get("item") or {}
                source_uuid = item.get("uuid")
                if not source_uuid:
                    continue

                card_id = f"{self.year}:{source_uuid}"
                if card_id not in card_ids:
                    continue

                listing_rows.append(
                    {
                        "card_id": card_id,
                        "best_buy_price": self._to_int_price(listing.get("best_buy_price")),
                        "best_sell_price": self._to_int_price(listing.get("best_sell_price")),
                    }
                )

            total_listings += len(listings)

            if len(listing_rows) >= self.chunk_size:
                self._upsert_listings(db_session, listing_rows)
                listing_rows = []

            if page >= total_pages:
                break
            page += 1

        if listing_rows:
            self._upsert_listings(db_session, listing_rows)

        self._log_end(cards=len(card_ids), pages=total_pages, listings=total_listings)

    def _fetch_listings_page(self, page: int) -> Dict[str, Any]:
        url = f"https://mlb{self.year}.theshow.com/apis/listings.json"
        params = {"type": "mlb_card", "page": page}
        return self._api_client.get(url, params)

    def _get_card_ids(self, session: Session) -> Set[str]:
        rows = session.execute(select(Card.id).where(Card.year == self.year)).scalars().all()
        return set(rows)

    def _to_int_price(self, v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, int):
            return v
        s = str(v).strip().replace(",", "")
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None

    def _upsert_listings(self, session: Session, rows: List[Dict[str, Any]]) -> None:
        uniq = {r["card_id"]: r for r in rows}
        rows = list(uniq.values())
        rows.sort(key=lambda r: r["card_id"])

        session.execute(text("SET LOCAL synchronous_commit TO OFF"))
        stmt = pg_insert(Listing).values(rows).on_conflict_do_update(
            index_elements=["card_id"],
            set_={
                "best_buy_price": pg_insert(Listing).excluded.best_buy_price,
                "best_sell_price": pg_insert(Listing).excluded.best_sell_price,
            },
        )
        session.execute(stmt)
        session.commit()
