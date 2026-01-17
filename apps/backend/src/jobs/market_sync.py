from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from src.core.config import THE_SHOW_YEARS
from src.database.models import Card, CompletedOrder, Listing, PriceHistory
from src.jobs.base import BaseJob


class MarketSync(BaseJob):
    def __init__(self):
        super().__init__()
        self.set_child_instance(self)
        self.year = THE_SHOW_YEARS[0]

    def execute(self, db_session: Session):
        self.logger.info("Starting MarketSync Execution...")

        season_year = 2000 + self.year
        now = datetime.utcnow()
        cutoff = now - timedelta(hours=48)

        card_keys = self._get_card_keys(db_session)  # (derived_id, source_uuid)
        self.logger.info(f"Syncing market data for {len(card_keys)} cards (mlb{self.year})")

        db_session.execute(delete(CompletedOrder).where(CompletedOrder.date < cutoff))
        db_session.commit()

        chunk_size = 200
        total = len(card_keys)

        for start in range(0, total, chunk_size):
            chunk = card_keys[start : start + chunk_size]

            payloads: List[Tuple[str, Dict[str, Any]]] = []
            with ThreadPoolExecutor(max_workers=2) as ex:
                futures = {
                    ex.submit(self._fetch_market_payload_jitter, source_uuid): (card_id, source_uuid)
                    for (card_id, source_uuid) in chunk
                }
                for fut in as_completed(futures):
                    card_id, source_uuid = futures[fut]
                    try:
                        payload = fut.result()
                    except Exception as e:
                        self.logger.error(f"Card {card_id} ({source_uuid}) fetch failed: {e}", exc_info=True)
                        continue
                    if payload:
                        payloads.append((card_id, payload))

            listing_rows: List[Dict[str, Any]] = []
            order_rows: List[Dict[str, Any]] = []
            ph_rows: List[Dict[str, Any]] = []

            for card_id, payload in payloads:
                out = self._build_rows_from_payload(
                    payload=payload,
                    card_id=card_id,
                    season_year=season_year,
                    now=now,
                    cutoff=cutoff,
                )
                if not out:
                    continue
                lrow, orows, prows = out
                if lrow:
                    listing_rows.append(lrow)
                if orows:
                    order_rows.extend(orows)
                if prows:
                    ph_rows.extend(prows)

            if listing_rows or order_rows or ph_rows:
                db_session.execute(text("SET LOCAL synchronous_commit TO OFF"))

            if listing_rows:
                stmt = pg_insert(Listing).values(listing_rows).on_conflict_do_update(
                    index_elements=["card_id"],
                    set_={
                        "best_buy_price": pg_insert(Listing).excluded.best_buy_price,
                        "best_sell_price": pg_insert(Listing).excluded.best_sell_price,
                    },
                )
                db_session.execute(stmt)

            if order_rows:
                stmt = pg_insert(CompletedOrder).values(order_rows).on_conflict_do_update(
                    index_elements=["card_id", "date"],
                    set_={
                        "price": pg_insert(CompletedOrder).excluded.price,
                        "is_buy": pg_insert(CompletedOrder).excluded.is_buy,
                    },
                )
                db_session.execute(stmt)

            if ph_rows:
                excluded = pg_insert(PriceHistory).excluded
                stmt = pg_insert(PriceHistory).values(ph_rows).on_conflict_do_update(
                    index_elements=["card_id", "date"],
                    set_={
                        "best_buy_price": excluded.best_buy_price,
                        "best_sell_price": excluded.best_sell_price,
                        "volume": func.coalesce(excluded.volume, PriceHistory.volume),
                    },
                )
                db_session.execute(stmt)

            db_session.commit()

            done = min(start + chunk_size, total)
            self.logger.info(
                f"Progress: {done}/{total} payloads={len(payloads)} "
                f"listing_rows={len(listing_rows)} order_rows={len(order_rows)} ph_rows={len(ph_rows)}"
            )

        self.logger.info("MarketSync complete.")

    def _get_card_keys(self, session: Session) -> List[Tuple[str, str]]:
        stmt = select(Card.id, Card.source_uuid).where(Card.year == self.year)
        rows = session.execute(stmt).all()
        return [(r[0], r[1]) for r in rows if r[0] and r[1]]

    def _fetch_market_payload_jitter(self, source_uuid: str) -> Optional[Dict[str, Any]]:
        time.sleep(random.uniform(0.1, 0.3))
        url = f"https://mlb{self.year}.theshow.com/apis/listing.json"
        params = {"uuid": source_uuid}
        return self.api_client.get(url, params)

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

    def _infer_buy_sell_labels(
        self,
        parsed: List[Tuple[datetime, int]],
        best_buy_price: Optional[int] = None,
        best_sell_price: Optional[int] = None,
    ) -> List[Optional[bool]]:
        n = len(parsed)
        if n == 0:
            return []

        prices = [p for _, p in parsed]

        def _valid_anchor(x: Optional[int]) -> Optional[int]:
            if x is None:
                return None
            try:
                x = int(x)
            except Exception:
                return None
            return x if x > 0 else None

        bb = _valid_anchor(best_buy_price)
        bs = _valid_anchor(best_sell_price)

        if bb is not None and bs is not None and bs < bb:
            bb, bs = bs, bb

        if bb is not None and bs is not None and bs >= bb:
            spread = bs - bb
            tol = max(1, int(round(0.10 * spread)), int(round(0.002 * max(bs, bb))))
            mid = (bb + bs) / 2.0

            out: List[Optional[bool]] = []
            for p in prices:
                if abs(p - bs) <= tol:
                    out.append(True)
                elif abs(p - bb) <= tol:
                    out.append(False)
                else:
                    out.append(p > mid)

            has_buy = any(x is True for x in out)
            has_sell = any(x is False for x in out)
            if not (has_buy and has_sell):
                return [None] * n

            return out

        p_sorted = sorted(prices)
        if p_sorted[0] == p_sorted[-1]:
            return [None] * n

        q1 = p_sorted[int(0.25 * (n - 1))]
        q3 = p_sorted[int(0.75 * (n - 1))]
        c1 = q1
        c2 = q3 if q3 != q1 else p_sorted[-1]

        for _ in range(15):
            g1_sum = g1_n = 0
            g2_sum = g2_n = 0

            for p in prices:
                if abs(p - c1) <= abs(p - c2):
                    g1_sum += p
                    g1_n += 1
                else:
                    g2_sum += p
                    g2_n += 1

            if g1_n == 0 or g2_n == 0:
                return [None] * n

            nc1 = g1_sum / g1_n
            nc2 = g2_sum / g2_n

            if abs(nc1 - c1) < 1e-6 and abs(nc2 - c2) < 1e-6:
                break

            c1, c2 = nc1, nc2

        low_mean, high_mean = (c1, c2) if c1 <= c2 else (c2, c1)
        sep = high_mean - low_mean
        if sep <= 0:
            return [None] * n

        level = max(1.0, (low_mean + high_mean) / 2.0)
        if sep < max(1.0, 0.002 * level):
            return [None] * n

        threshold = (low_mean + high_mean) / 2.0
        labels = [p > threshold for p in prices]
        buy_ct = sum(1 for x in labels if x)
        sell_ct = n - buy_ct
        if buy_ct < max(2, n // 20) or sell_ct < max(2, n // 20):
            return [None] * n

        return labels

    def _build_rows_from_payload(
        self,
        payload: Dict[str, Any],
        card_id: str,
        season_year: int,
        now: datetime,
        cutoff: datetime,
    ) -> Optional[Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]]]]:
        if not card_id:
            return None

        best_buy = self._to_int_price(payload.get("best_buy_price"))
        best_sell = self._to_int_price(payload.get("best_sell_price"))

        listing_row = {
            "card_id": card_id,
            "best_buy_price": best_buy,
            "best_sell_price": best_sell,
        }

        completed_orders_payload = payload.get("completed_orders") or []
        seen_ts: set[datetime] = set()
        parsed: List[Tuple[datetime, int]] = []

        for it in completed_orders_payload:
            dt_str = it.get("date")
            price_int = self._to_int_price(it.get("price"))
            if not dt_str or price_int is None:
                continue
            try:
                ts = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
            except ValueError:
                continue
            if ts < cutoff:
                continue
            if ts in seen_ts:
                continue
            seen_ts.add(ts)
            parsed.append((ts, price_int))

        labels = self._infer_buy_sell_labels(parsed, best_buy, best_sell)

        order_rows: List[Dict[str, Any]] = []
        for (ts, price_int), is_buy in zip(parsed, labels):
            order_rows.append(
                {
                    "card_id": card_id,
                    "date": ts,
                    "price": price_int,
                    "is_buy": is_buy,
                }
            )

        yesterday = (now - timedelta(days=1)).date()
        start_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)

        earliest_ts = min((ts for ts, _ in parsed), default=None)
        is_truncated = (len(parsed) >= 200)
        can_compute_yesterday = (earliest_ts is not None) and (earliest_ts <= start_yesterday) and (not is_truncated)

        orders_by_date: Dict[Any, int] = {}
        if can_compute_yesterday:
            for ts, _ in parsed:
                d = ts.date()
                orders_by_date[d] = orders_by_date.get(d, 0) + 1

        price_history_payload = payload.get("price_history") or []
        ph_rows: List[Dict[str, Any]] = []

        for it in price_history_payload:
            mmdd = it.get("date")
            if not mmdd:
                continue
            try:
                d = datetime.strptime(f"{season_year}/{mmdd}", "%Y/%m/%d").date()
            except ValueError:
                continue

            volume = None
            if d == yesterday and can_compute_yesterday:
                volume = orders_by_date.get(d, 0)

            ph_rows.append(
                {
                    "card_id": card_id,
                    "date": d,
                    "best_buy_price": self._to_int_price(it.get("best_buy_price")),
                    "best_sell_price": self._to_int_price(it.get("best_sell_price")),
                    "volume": volume,
                }
            )

        return listing_row, order_rows, ph_rows
