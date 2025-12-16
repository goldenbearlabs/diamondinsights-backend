from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select, delete
from sqlalchemy.orm import Session

from src.jobs.base import BaseJob
from src.core.config import THE_SHOW_YEARS
from src.database.models import Card, Listing, PriceHistory, CompletedOrder


class MarketSync(BaseJob):
    def __init__(self):
        super().__init__()
        self.set_child_instance(self)
        self.year = THE_SHOW_YEARS[0]

    def execute(self, db_session: Session):
        self.logger.info("Starting MarketSync Execution...")

        season_year = 2000 + self.year
        card_ids = self._get_card_ids(db_session)

        self.logger.info(f"Syncing market data for {len(card_ids)} cards (mlb{self.year})")

        now = datetime.utcnow()

        for i, card_id in enumerate(card_ids, start=1):
            try:
                with db_session.begin_nested():
                    payload = self._fetch_market_payload(card_id)
                    if not payload:
                        continue

                    self._sync_market_snapshot(
                        session=db_session,
                        payload=payload,
                        season_year=season_year,
                        now=now,
                    )
                    
                if i % 200 == 0:
                    db_session.commit()
                    self.logger.info(f"Progress: {i}/{len(card_ids)} cards (committed)")

            except Exception as e:
                self.logger.error(f"Card {card_id} failed: {e}", exc_info=True)
                continue

        db_session.commit()

    def _get_card_ids(self, session: Session) -> List[str]:
        stmt = select(Card.id).where(Card.year == self.year)
        return session.execute(stmt).scalars().all()

    def _fetch_market_payload(self, card_id: str) -> Optional[Dict[str, Any]]:
        url = f"https://mlb{self.year}.theshow.com/apis/listing.json"
        params = {"uuid": card_id}
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
                    out.append(True)   # buy side (higher cluster)
                elif abs(p - bb) <= tol:
                    out.append(False)  # sell side (lower cluster)
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

    def _sync_market_snapshot(
        self,
        session: Session,
        payload: Dict[str, Any],
        season_year: int,
        now: datetime,
    ) -> None:
        item = payload.get("item") or {}
        card_id = item.get("uuid")
        if not card_id:
            return

        best_buy = self._to_int_price(payload.get("best_buy_price"))
        best_sell = self._to_int_price(payload.get("best_sell_price"))


        self._sync_listing(session, card_id, best_buy, best_sell)

        parsed_orders = self._sync_completed_orders(
            session=session,
            card_id=card_id,
            completed_orders_payload=payload.get("completed_orders") or [],
            best_buy_price=best_buy,
            best_sell_price=best_sell,
            now=now,
        )

        self.logger.info(f"{card_id}: listing={best_buy}/{best_sell} orders={len(parsed_orders)}")

        self._sync_price_history(
            session=session,
            card_id=card_id,
            price_history_payload=payload.get("price_history") or [],
            season_year=season_year,
            parsed_orders=parsed_orders,
            now=now,
        )

        session.flush()

    def _sync_listing(
        self,
        session: Session,
        card_id: str,
        best_buy_price: Optional[int],
        best_sell_price: Optional[int],
    ) -> None:
        session.merge(
            Listing(
                card_id=card_id,
                best_buy_price=best_buy_price,
                best_sell_price=best_sell_price,
            )
        )

    def _sync_completed_orders(
        self,
        session: Session,
        card_id: str,
        completed_orders_payload: List[Dict[str, Any]],
        best_buy_price: Optional[int],
        best_sell_price: Optional[int],
        now: datetime,
    ) -> List[Tuple[datetime, int, Optional[bool]]]:
        cutoff = now - timedelta(hours=48)

        session.execute(
            delete(CompletedOrder).where(
                CompletedOrder.card_id == card_id,
                CompletedOrder.date < cutoff,
            )
        )

        existing_dates = set(
            session.execute(
                select(CompletedOrder.date).where(
                    CompletedOrder.card_id == card_id,
                    CompletedOrder.date >= cutoff,
                )
            ).scalars().all()
        )

        seen_ts: set[datetime] = set()
        parsed: List[Tuple[datetime, int]] = []
        for item in completed_orders_payload:
            dt_str = item.get("date")
            price_int = self._to_int_price(item.get("price"))
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

        labels = self._infer_buy_sell_labels(parsed, best_buy_price, best_sell_price)

        to_add = []
        added_ts: set[datetime] = set()
        out: List[Tuple[datetime, int, Optional[bool]]] = []

        for (ts, price_int), is_buy in zip(parsed, labels):
            if ts in existing_dates or ts in added_ts:
                continue
            out.append((ts, price_int, is_buy))
            added_ts.add(ts)

            if ts in existing_dates:
                continue

            to_add.append(
                CompletedOrder(
                    card_id=card_id,
                    date=ts,
                    price=price_int,
                    is_buy=is_buy,
                )
            )

        if to_add:
            session.add_all(to_add)

        return out

    def _sync_price_history(
        self,
        session: Session,
        card_id: str,
        price_history_payload: List[Dict[str, Any]],
        season_year: int,
        parsed_orders: List[Tuple[datetime, int, Optional[bool]]],
        now: datetime,
    ) -> int:
        yesterday = (now - timedelta(days=1)).date()
        start_yesterday = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0)

        earliest_ts = min((ts for ts, _, _ in parsed_orders), default=None)
        is_truncated = (len(parsed_orders) >= 200)
        can_compute_yesterday = (earliest_ts is not None) and (earliest_ts <= start_yesterday) and (not is_truncated)

        orders_by_date: Dict[Any, int] = {}
        for ts, _, _ in parsed_orders:
            d = ts.date()
            orders_by_date[d] = orders_by_date.get(d, 0) + 1

        existing_dates = set(
            session.execute(
                select(PriceHistory.date).where(PriceHistory.card_id == card_id)
            ).scalars().all()
        )

        to_insert: List[PriceHistory] = []

        for item in price_history_payload:
            mmdd = item.get("date")
            if not mmdd:
                continue

            try:
                d = datetime.strptime(f"{season_year}/{mmdd}", "%Y/%m/%d").date()
            except ValueError:
                continue

            if d in existing_dates:
                continue

            volume = None
            if d == yesterday and can_compute_yesterday:
                volume = orders_by_date.get(d, 0)

            to_insert.append(
                PriceHistory(
                    card_id=card_id,
                    date=d,
                    best_buy_price=item.get("best_buy_price"),
                    best_sell_price=item.get("best_sell_price"),
                    volume=volume,
                )
            )

        if to_insert:
            session.add_all(to_insert)

        return len(to_insert)
