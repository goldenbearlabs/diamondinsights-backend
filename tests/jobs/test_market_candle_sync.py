from datetime import datetime, timedelta, timezone

import apps.jobs.market_candle_sync as candle_sync


class FakeScalarResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items


class FakeAllResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.exec_calls = 0
        self.added = []

    def execute(self, stmt):
        self.exec_calls += 1
        return self._results.pop(0)

    def add_all(self, items):
        self.added.extend(items)


def test_yesterday_window_utc_returns_naive_bounds():
    sync = candle_sync.MarketCandleSync()

    now_utc = datetime(2026, 2, 1, 12, 0, 0, tzinfo=timezone.utc)
    start_utc, end_utc = sync._yesterday_window_utc(now_utc)

    assert start_utc.tzinfo is None
    assert end_utc.tzinfo is None
    assert end_utc - start_utc == timedelta(days=1)
    assert start_utc < end_utc

    local_start = start_utc.replace(tzinfo=timezone.utc).astimezone(sync.tz)
    local_now = now_utc.astimezone(sync.tz)
    assert local_start.date() == local_now.date() - timedelta(days=1)
    assert local_start.hour == 0


def test_agg_side_empty_and_sorted():
    sync = candle_sync.MarketCandleSync()

    assert sync._agg_side([]) == {"open": 0, "close": 0, "low": 0, "high": 0, "vol": 0}

    pts = [
        (datetime(2026, 1, 1, 2, 0, 0), 50),
        (datetime(2026, 1, 1, 1, 0, 0), 40),
        (datetime(2026, 1, 1, 3, 0, 0), 60),
    ]
    stats = sync._agg_side(pts)

    assert stats == {"open": 40, "close": 60, "low": 40, "high": 60, "vol": 3}


def test_run_returns_when_no_cards(monkeypatch):
    sync = candle_sync.MarketCandleSync()
    monkeypatch.setattr(sync, "_get_card_ids", lambda session: [])

    session = FakeSession(results=[])
    sync.run(session)

    assert session.exec_calls == 0
    assert session.added == []


def test_run_returns_when_all_existing(monkeypatch):
    sync = candle_sync.MarketCandleSync()
    monkeypatch.setattr(sync, "_get_card_ids", lambda session: ["c1"])
    monkeypatch.setattr(sync, "_yesterday_window_utc", lambda now: (datetime(2026, 1, 31), datetime(2026, 2, 1)))

    session = FakeSession(results=[FakeScalarResult(["c1"])])
    sync.run(session)

    assert session.exec_calls == 1
    assert session.added == []


def test_run_builds_candles(monkeypatch):
    sync = candle_sync.MarketCandleSync()
    monkeypatch.setattr(sync, "_get_card_ids", lambda session: ["c1", "c2"])

    start_utc = datetime(2026, 1, 31, 8, 0, 0)
    end_utc = datetime(2026, 2, 1, 8, 0, 0)
    monkeypatch.setattr(sync, "_yesterday_window_utc", lambda now: (start_utc, end_utc))

    rows = [
        ("c1", datetime(2026, 1, 31, 9, 0, 0), 100, True),
        ("c1", datetime(2026, 1, 31, 10, 0, 0), 110, True),
        ("c1", datetime(2026, 1, 31, 9, 30, 0), 120, False),
        ("c2", datetime(2026, 1, 31, 12, 0, 0), 200, True),
    ]

    session = FakeSession(results=[FakeScalarResult([]), FakeAllResult(rows)])
    sync.run(session)

    assert session.exec_calls == 2
    assert len(session.added) == 2

    c1 = next(c for c in session.added if c.card_id == "c1")
    assert c1.start_time == start_utc
    assert c1.open_buy_price == 100
    assert c1.close_buy_price == 110
    assert c1.low_buy_price == 100
    assert c1.high_buy_price == 110
    assert c1.buy_volume == 2
    assert c1.open_sell_price == 120
    assert c1.sell_volume == 1

    c2 = next(c for c in session.added if c.card_id == "c2")
    assert c2.buy_volume == 1
    assert c2.sell_volume == 0


def test_get_card_ids_returns_scalars(monkeypatch):
    sync = candle_sync.MarketCandleSync()

    session = FakeSession(results=[FakeScalarResult(["c1", "c2"])])
    assert sync._get_card_ids(session) == ["c1", "c2"]
    assert session.exec_calls == 1


def test_run_skips_zero_volume_buckets(monkeypatch):
    sync = candle_sync.MarketCandleSync()
    monkeypatch.setattr(sync, "_get_card_ids", lambda session: ["c1"])

    start_utc = datetime(2026, 1, 31, 8, 0, 0)
    end_utc = datetime(2026, 2, 1, 8, 0, 0)
    monkeypatch.setattr(sync, "_yesterday_window_utc", lambda now: (start_utc, end_utc))

    rows = [
        ("c1", datetime(2026, 1, 31, 9, 0, 0), 100, True),
    ]

    monkeypatch.setattr(sync, "_agg_side", lambda pts: {"open": 0, "close": 0, "low": 0, "high": 0, "vol": 0})

    session = FakeSession(results=[FakeScalarResult([]), FakeAllResult(rows)])
    sync.run(session)

    assert session.exec_calls == 2
    assert session.added == []
