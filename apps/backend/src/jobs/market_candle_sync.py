from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from zoneinfo import ZoneInfo
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.jobs.base import BaseJob
from src.core.config import THE_SHOW_YEARS
from src.database.models import Card, CompletedOrder, MarketCandle


class MarketCandleSync(BaseJob):
    def __init__(self):
        super().__init__()
        self.set_child_instance(self)
        self.year = THE_SHOW_YEARS[0]
        self.tz = ZoneInfo("America/Vancouver")

    def execute(self, db_session: Session):
        now_utc = datetime.utcnow()
        start_utc, end_utc = self._yesterday_window_utc(now_utc)

        card_ids = self._get_card_ids(db_session)
        if not card_ids:
            self.logger.info("No cards found for this year. Exiting.")
            return

        existing = set(
            db_session.execute(
                select(MarketCandle.card_id).where(
                    MarketCandle.start_time == start_utc,
                    MarketCandle.card_id.in_(card_ids),
                )
            ).scalars().all()
        )

        target_ids = [cid for cid in card_ids if cid not in existing]
        if not target_ids:
            self.logger.info(f"All candles already exist for {start_utc}. Nothing to do.")
            return

        rows = db_session.execute(
            select(
                CompletedOrder.card_id,
                CompletedOrder.date,
                CompletedOrder.price,
                CompletedOrder.is_buy,
            ).where(
                CompletedOrder.card_id.in_(target_ids),
                CompletedOrder.date >= start_utc,
                CompletedOrder.date < end_utc,
                CompletedOrder.is_buy.is_not(None),
            )
        ).all()

        buckets: Dict[str, Dict[bool, List[Tuple[datetime, int]]]] = defaultdict(lambda: {True: [], False: []})
        for card_id, ts, price, is_buy in rows:
            buckets[card_id][bool(is_buy)].append((ts, price))

        to_add: List[MarketCandle] = []
        for card_id, sides in buckets.items():
            buy_stats = self._agg_side(sides.get(True, []))
            sell_stats = self._agg_side(sides.get(False, []))

            if buy_stats["vol"] == 0 and sell_stats["vol"] == 0:
                continue

            to_add.append(
                MarketCandle(
                    card_id=card_id,
                    start_time=start_utc,
                    open_buy_price=buy_stats["open"],
                    low_buy_price=buy_stats["low"],
                    high_buy_price=buy_stats["high"],
                    close_buy_price=buy_stats["close"],
                    buy_volume=buy_stats["vol"],
                    open_sell_price=sell_stats["open"],
                    low_sell_price=sell_stats["low"],
                    high_sell_price=sell_stats["high"],
                    close_sell_price=sell_stats["close"],
                    sell_volume=sell_stats["vol"],
                )
            )

        if not to_add:
            self.logger.info(f"No labeled completed orders for {start_utc} (nothing to write).")
            return

        db_session.add_all(to_add)
        self.logger.info(f"Inserted {len(to_add)} market candles for start_time={start_utc}")

    def _get_card_ids(self, session: Session) -> List[str]:
        return session.execute(
            select(Card.id).where(Card.year == self.year)
        ).scalars().all()

    def _yesterday_window_utc(self, now_utc: datetime) -> Tuple[datetime, datetime]:
        now_local = now_utc.replace(tzinfo=timezone.utc).astimezone(self.tz)
        today_local = now_local.date()
        start_local = datetime(today_local.year, today_local.month, today_local.day, tzinfo=self.tz) - timedelta(days=1)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).replace(tzinfo=None)
        end_utc = end_local.astimezone(timezone.utc).replace(tzinfo=None)
        return start_utc, end_utc

    def _agg_side(self, pts: List[Tuple[datetime, int]]) -> Dict[str, int]:
        if not pts:
            return {"open": 0, "close": 0, "low": 0, "high": 0, "vol": 0}
        pts.sort(key=lambda x: x[0])
        prices = [p for _, p in pts]
        return {
            "open": prices[0],
            "close": prices[-1],
            "low": min(prices),
            "high": max(prices),
            "vol": len(prices),
        }
