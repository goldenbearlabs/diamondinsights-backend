from __future__ import annotations

import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from shared.core.config import THE_SHOW_YEARS
from shared.db.models import Card, CompletedOrder, Listing, PriceHistory
from apps.jobs.job import Job


class MarketSync(Job):
    def __init__(
        self,
        *,
        max_workers: Optional[int] = None,
        chunk_size: Optional[int] = None,
        fetch_jitter_range: Optional[Tuple[float, float]] = None,
        ovr_min: Optional[int] = None,
        ovr_max: Optional[int] = None,
    ):
        super().__init__()
        self.year = THE_SHOW_YEARS[0]
        self.max_workers = max(1, max_workers or int(os.getenv("MARKET_SYNC_MAX_WORKERS", "6")))
        self.chunk_size = max(1, chunk_size or int(os.getenv("MARKET_SYNC_CHUNK_SIZE", "200")))
        self.fetch_jitter_range = fetch_jitter_range or (0.05, 0.2)
        self.ovr_min = ovr_min
        self.ovr_max = ovr_max

    def run(self, db_session: Session):

        season_year = 2000 + self.year
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = now - timedelta(hours=48)
        self._log_start(year=self.year, season_year=season_year, ovr_min=self.ovr_min, ovr_max=self.ovr_max)

        card_keys = self._get_card_keys(db_session)
        if not card_keys:
            self._log_end(cards=0)
            return

        db_session.execute(delete(CompletedOrder).where(CompletedOrder.date < cutoff))
        db_session.commit()

        chunk_size = self.chunk_size
        total = len(card_keys)
        total_payloads = 0
        total_listing_rows = 0
        total_order_rows = 0
        total_ph_rows = 0

        for start in range(0, total, chunk_size):
            chunk = card_keys[start : start + chunk_size]

            payloads: List[Tuple[str, Dict[str, Any]]] = []
            max_workers = min(self.max_workers, len(chunk)) or 1
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {
                    ex.submit(self._fetch_market_payload_jitter, source_uuid): (card_id, source_uuid)
                    for (card_id, source_uuid) in chunk
                }
                for fut in as_completed(futures):
                    card_id, source_uuid = futures[fut]
                    try:
                        payload = fut.result()
                    except Exception as e:
                        continue
                    if payload:
                        payloads.append((card_id, payload))
            total_payloads += len(payloads)

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
                uniq = {r["card_id"]: r for r in listing_rows}
                listing_rows = list(uniq.values())
                total_listing_rows += len(listing_rows)

                listing_rows.sort(key=lambda r: r["card_id"])
                stmt = pg_insert(Listing).values(listing_rows).on_conflict_do_update(
                    index_elements=["card_id"],
                    set_={
                        "best_buy_price": pg_insert(Listing).excluded.best_buy_price,
                        "best_sell_price": pg_insert(Listing).excluded.best_sell_price,
                    },
                )
                db_session.execute(stmt)

            if order_rows:
                uniq = {(r["card_id"], r["date"]): r for r in order_rows}
                order_rows = list(uniq.values())
                total_order_rows += len(order_rows)
                order_rows.sort(key=lambda r: (r["card_id"], r["date"]))
                stmt = pg_insert(CompletedOrder).values(order_rows).on_conflict_do_update(
                    index_elements=["card_id", "date"],
                    set_={
                        "price": pg_insert(CompletedOrder).excluded.price,
                        "is_buy": pg_insert(CompletedOrder).excluded.is_buy,
                    },
                )
                db_session.execute(stmt)

            if ph_rows:
                uniq = {}
                for r in ph_rows:
                    k = (r["card_id"], r["date"])
                    prev = uniq.get(k)
                    if prev is None:
                        uniq[k] = r
                    else:
                        if r.get("best_buy_price") is not None:
                            prev["best_buy_price"] = r["best_buy_price"]
                        if r.get("best_sell_price") is not None:
                            prev["best_sell_price"] = r["best_sell_price"]
                        if r.get("volume") is not None:
                            prev["volume"] = r["volume"]
                ph_rows = list(uniq.values())
                total_ph_rows += len(ph_rows)
                ph_rows.sort(key=lambda r: (r["card_id"], r["date"]))
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
        self._log_end(
            cards=total,
            payloads=total_payloads,
            listings=total_listing_rows,
            orders=total_order_rows,
            price_history=total_ph_rows,
        )

    def _get_card_keys(self, session: Session) -> List[Tuple[str, str]]:
        stmt = select(Card.id, Card.source_uuid).where(Card.year == self.year)
        if self.ovr_min is not None:
            stmt = stmt.where(Card.ovr >= self.ovr_min)
        if self.ovr_max is not None:
            stmt = stmt.where(Card.ovr <= self.ovr_max)
        rows = session.execute(stmt).all()
        return [(r[0], r[1]) for r in rows if r[0] and r[1]]

    def _fetch_market_payload_jitter(self, source_uuid: str) -> Optional[Dict[str, Any]]:
        time.sleep(random.uniform(*self.fetch_jitter_range))
        url = f"https://mlb{self.year}.theshow.com/apis/listing.json"
        params = {"uuid": source_uuid}
        return self._api_client.get(url, params)

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

        def _label_with_midpoint(midpoint: float, bb_anchor: Optional[int] = None, bs_anchor: Optional[int] = None) -> List[Optional[bool]]:
            tol = None
            if bb_anchor is not None and bs_anchor is not None and bs_anchor >= bb_anchor:
                spread = bs_anchor - bb_anchor
                tol = max(1, int(round(0.05 * spread)), int(round(0.002 * max(bs_anchor, bb_anchor))))

            out: List[Optional[bool]] = []
            for p in prices:
                if tol is not None:
                    if abs(p - bs_anchor) <= tol:
                        out.append(True)
                        continue
                    if abs(p - bb_anchor) <= tol:
                        out.append(False)
                        continue
                out.append(p >= midpoint)
            return out

        if bb is not None and bs is not None and bs >= bb:
            mid = (bb + bs) / 2.0
            return _label_with_midpoint(mid, bb, bs)

        p_sorted = sorted(prices)
        if p_sorted[0] == p_sorted[-1]:
            return [True] * n

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
                break

            nc1 = g1_sum / g1_n
            nc2 = g2_sum / g2_n

            if abs(nc1 - c1) < 1e-6 and abs(nc2 - c2) < 1e-6:
                break

            c1, c2 = nc1, nc2

        low_mean, high_mean = (c1, c2) if c1 <= c2 else (c2, c1)
        sep = high_mean - low_mean
        level = max(1.0, (low_mean + high_mean) / 2.0)

        if sep < max(1.0, 0.002 * level):
            midpoint = p_sorted[n // 2]
            return _label_with_midpoint(midpoint)

        midpoint = (low_mean + high_mean) / 2.0
        return _label_with_midpoint(midpoint)

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
