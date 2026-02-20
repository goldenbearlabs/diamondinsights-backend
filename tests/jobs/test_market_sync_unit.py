from datetime import datetime, timedelta

import apps.jobs.market_sync as market_sync


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeSession:
    def __init__(self):
        self.exec_calls = []
        self.commits = 0

    def execute(self, stmt):
        self.exec_calls.append(stmt)
        return None

    def commit(self):
        self.commits += 1


def test_to_int_price():
    sync = market_sync.MarketSync()

    assert sync._to_int_price(None) is None
    assert sync._to_int_price(123) == 123
    assert sync._to_int_price("1,234") == 1234
    assert sync._to_int_price(" ") is None
    assert sync._to_int_price("abc") is None


def test_get_card_keys_filters_missing(monkeypatch):
    class Session:
        def execute(self, stmt):
            return FakeResult([
                ("id1", "uuid1"),
                (None, "uuid2"),
                ("id3", None),
                ("id4", "uuid4"),
            ])

    sync = market_sync.MarketSync()
    keys = sync._get_card_keys(Session())

    assert keys == [("id1", "uuid1"), ("id4", "uuid4")]


def test_fetch_market_payload_jitter_calls_api(monkeypatch):
    sync = market_sync.MarketSync(fetch_jitter_range=(0.12, 0.12))
    calls = {}

    monkeypatch.setattr(market_sync.time, "sleep", lambda s: calls.setdefault("slept", s))

    def fake_get(url, params):
        calls["url"] = url
        calls["params"] = params
        return {"ok": True}

    sync._api_client.get = fake_get

    payload = sync._fetch_market_payload_jitter("uuid1")

    assert payload == {"ok": True}
    assert calls["slept"] == 0.12
    assert calls["url"] == f"https://mlb{sync.year}.theshow.com/apis/listing.json"
    assert calls["params"] == {"uuid": "uuid1"}


def test_infer_buy_sell_labels_with_anchors():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 100),
        (datetime(2026, 1, 1, 0, 1, 0), 120),
        (datetime(2026, 1, 1, 0, 2, 0), 110),
    ]

    labels = sync._infer_buy_sell_labels(parsed, best_buy_price=100, best_sell_price=120)

    assert len(labels) == 3
    assert any(l is True for l in labels)
    assert any(l is False for l in labels)


def test_infer_buy_sell_labels_empty():
    sync = market_sync.MarketSync()
    assert sync._infer_buy_sell_labels([]) == []


def test_infer_buy_sell_labels_invalid_anchor_coercion():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 50),
        (datetime(2026, 1, 1, 0, 1, 0), 50),
    ]

    labels = sync._infer_buy_sell_labels(parsed, best_buy_price="bad", best_sell_price="100")

    assert labels == [None, None]


def test_infer_buy_sell_labels_swaps_anchor_bounds():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 100),
        (datetime(2026, 1, 1, 0, 1, 0), 200),
        (datetime(2026, 1, 1, 0, 2, 0), 150),
    ]

    labels = sync._infer_buy_sell_labels(parsed, best_buy_price=200, best_sell_price=100)

    assert len(labels) == 3
    assert any(l is True for l in labels)
    assert any(l is False for l in labels)


def test_infer_buy_sell_labels_returns_none_for_nan_cluster():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), float("nan")),
        (datetime(2026, 1, 1, 0, 1, 0), float("nan")),
    ]

    labels = sync._infer_buy_sell_labels(parsed)

    assert labels == [None, None]


def test_infer_buy_sell_labels_small_separation_returns_none():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 0.0),
        (datetime(2026, 1, 1, 0, 1, 0), 0.2),
        (datetime(2026, 1, 1, 0, 2, 0), 0.4),
        (datetime(2026, 1, 1, 0, 3, 0), 0.6),
    ]

    labels = sync._infer_buy_sell_labels(parsed)

    assert labels == [None, None, None, None]


def test_infer_buy_sell_labels_zero_separation_returns_none():
    class WeirdNum:
        def __init__(self, value, prefer=None):
            self.value = value
            self.prefer = prefer
            self.role = None

        def __lt__(self, other):
            return self.value < other.value

        def __le__(self, other):
            return self.value <= other.value

        def __eq__(self, other):
            return False

        def __sub__(self, other):
            if isinstance(other, WeirdNum) and other.role in {"c1", "c2"}:
                if self.prefer is None:
                    return self.value - other.value
                return 0 if self.prefer == other.role else 1
            if isinstance(other, WeirdNum):
                return self.value - other.value
            return self.value - other

        def __rsub__(self, other):
            return other - self.value

        def __radd__(self, other):
            return other + self.value

    sync = market_sync.MarketSync()

    prices = [
        WeirdNum(1.0),
        WeirdNum(1.0, prefer="c2"),
        WeirdNum(1.0),
        WeirdNum(1.0, prefer="c2"),
    ]
    prices[0].role = "c1"
    prices[2].role = "c2"

    parsed = [(datetime(2026, 1, 1, 0, 0, 0), p) for p in prices]

    labels = sync._infer_buy_sell_labels(parsed)

    assert labels == [None, None, None, None]


def test_infer_buy_sell_labels_anchor_missing_side_returns_none():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 120),
        (datetime(2026, 1, 1, 0, 1, 0), 121),
    ]

    labels = sync._infer_buy_sell_labels(parsed, best_buy_price=100, best_sell_price=120)

    assert labels == [None, None]


def test_infer_buy_sell_labels_no_anchors_identical_prices():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 50),
        (datetime(2026, 1, 1, 0, 1, 0), 50),
    ]

    labels = sync._infer_buy_sell_labels(parsed)

    assert labels == [None, None]


def test_infer_buy_sell_labels_no_anchors_two_points_returns_none():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 1),
        (datetime(2026, 1, 1, 0, 1, 0), 2),
    ]

    labels = sync._infer_buy_sell_labels(parsed)

    assert labels == [None, None]


def test_infer_buy_sell_labels_no_anchors_clear_separation():
    sync = market_sync.MarketSync()
    parsed = [
        (datetime(2026, 1, 1, 0, 0, 0), 10),
        (datetime(2026, 1, 1, 0, 1, 0), 11),
        (datetime(2026, 1, 1, 0, 2, 0), 12),
        (datetime(2026, 1, 1, 0, 3, 0), 100),
        (datetime(2026, 1, 1, 0, 4, 0), 101),
        (datetime(2026, 1, 1, 0, 5, 0), 102),
    ]

    labels = sync._infer_buy_sell_labels(parsed)

    assert len(labels) == 6
    assert any(l is True for l in labels)
    assert any(l is False for l in labels)


def test_build_rows_from_payload_parses_orders_and_price_history(monkeypatch):
    sync = market_sync.MarketSync()

    now = datetime(2026, 2, 1, 12, 0, 0)
    cutoff = now - timedelta(days=3)

    payload = {
        "best_buy_price": "1,000",
        "best_sell_price": "2,000",
        "completed_orders": [
            {"date": "01/31/2026 00:00:00", "price": "1500"},
            {"date": "01/31/2026 00:00:00", "price": "1500"},
            {"date": "01/31/2026 00:30:00", "price": "1600"},
            {"date": "01/20/2026 00:00:00", "price": "1700"},
            {"date": "bad", "price": "999"},
            {"date": "01/31/2026 01:00:00", "price": None},
        ],
        "price_history": [
            {"date": "01/31", "best_buy_price": "1000", "best_sell_price": "2000"},
            {"date": "bad", "best_buy_price": "", "best_sell_price": ""},
        ],
    }

    monkeypatch.setattr(sync, "_infer_buy_sell_labels", lambda parsed, best_buy_price=None, best_sell_price=None: [True, False])

    listing_row, order_rows, ph_rows = sync._build_rows_from_payload(
        payload=payload,
        card_id="card-1",
        season_year=2026,
        now=now,
        cutoff=cutoff,
    )

    assert listing_row == {"card_id": "card-1", "best_buy_price": 1000, "best_sell_price": 2000}
    assert len(order_rows) == 2
    assert order_rows[0]["is_buy"] is True
    assert order_rows[1]["is_buy"] is False

    assert len(ph_rows) == 1
    assert ph_rows[0]["date"].isoformat() == "2026-01-31"
    assert ph_rows[0]["volume"] == 2


def test_build_rows_from_payload_skips_missing_price_history_date():
    sync = market_sync.MarketSync()

    now = datetime(2026, 2, 1, 12, 0, 0)
    cutoff = now - timedelta(days=3)

    payload = {
        "best_buy_price": "1",
        "best_sell_price": "2",
        "completed_orders": [],
        "price_history": [
            {"best_buy_price": "1", "best_sell_price": "2"},
            {"date": "", "best_buy_price": "1", "best_sell_price": "2"},
        ],
    }

    listing_row, order_rows, ph_rows = sync._build_rows_from_payload(
        payload=payload,
        card_id="card-1",
        season_year=2026,
        now=now,
        cutoff=cutoff,
    )

    assert listing_row["card_id"] == "card-1"
    assert order_rows == []
    assert ph_rows == []


def test_build_rows_from_payload_requires_card_id():
    sync = market_sync.MarketSync()
    out = sync._build_rows_from_payload(
        payload={},
        card_id="",
        season_year=2026,
        now=datetime(2026, 1, 1),
        cutoff=datetime(2026, 1, 1),
    )

    assert out is None


def test_run_executes_inserts_when_rows_present(monkeypatch):
    sync = market_sync.MarketSync(max_fetch_retries=1)

    monkeypatch.setattr(sync, "_get_card_keys", lambda session: [("card-1", "uuid-1")])
    monkeypatch.setattr(sync, "_fetch_market_payload_jitter", lambda source_uuid: {"payload": True})

    def fake_build(payload, card_id, season_year, now, cutoff):
        return (
            {"card_id": card_id, "best_buy_price": 1, "best_sell_price": 2},
            [{"card_id": card_id, "date": now, "price": 1, "is_buy": True}],
            [{"card_id": card_id, "date": now.date(), "best_buy_price": 1, "best_sell_price": 2, "volume": 1}],
        )

    monkeypatch.setattr(sync, "_build_rows_from_payload", fake_build)

    class DummyFuture:
        def __init__(self, value=None, exc=None):
            self._value = value
            self._exc = exc

        def result(self):
            if self._exc:
                raise self._exc
            return self._value

    class DummyExecutor:
        def __init__(self, max_workers=2):
            self.futures = []

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, *args, **kwargs):
            try:
                value = func(*args, **kwargs)
                fut = DummyFuture(value=value)
            except Exception as e:
                fut = DummyFuture(exc=e)
            self.futures.append(fut)
            return fut

    monkeypatch.setattr(market_sync, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(market_sync, "as_completed", lambda futures: list(futures))

    session = FakeSession()
    sync.run(session)

    assert session.commits >= 2
    assert len(session.exec_calls) >= 5


def test_run_skips_failed_futures_and_empty_outputs(monkeypatch):
    sync = market_sync.MarketSync(max_fetch_retries=1)

    monkeypatch.setattr(sync, "_get_card_keys", lambda session: [("card-1", "uuid-1"), ("card-2", "uuid-2")])

    def fake_fetch(source_uuid):
        if source_uuid == "uuid-1":
            raise RuntimeError("boom")
        return {"payload": True}

    monkeypatch.setattr(sync, "_fetch_market_payload_jitter", fake_fetch)
    monkeypatch.setattr(market_sync.time, "sleep", lambda s: None)
    monkeypatch.setattr(sync, "_build_rows_from_payload", lambda *args, **kwargs: None)

    class DummyFuture:
        def __init__(self, value=None, exc=None):
            self._value = value
            self._exc = exc

        def result(self):
            if self._exc:
                raise self._exc
            return self._value

    class DummyExecutor:
        def __init__(self, max_workers=2):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, *args, **kwargs):
            try:
                value = func(*args, **kwargs)
                return DummyFuture(value=value)
            except Exception as e:
                return DummyFuture(exc=e)

    monkeypatch.setattr(market_sync, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(market_sync, "as_completed", lambda futures: list(futures))

    session = FakeSession()
    sync.run(session)

    assert session.commits >= 2


def test_run_merges_price_history_duplicates(monkeypatch):
    sync = market_sync.MarketSync(max_fetch_retries=1)

    monkeypatch.setattr(sync, "_get_card_keys", lambda session: [("card-1", "uuid-1")])
    monkeypatch.setattr(sync, "_fetch_market_payload_jitter", lambda source_uuid: {"payload": True})

    day = datetime(2026, 2, 1, 12, 0, 0).date()

    def fake_build(payload, card_id, season_year, now, cutoff):
        return (
            None,
            [],
            [
                {"card_id": card_id, "date": day, "best_buy_price": None, "best_sell_price": None, "volume": None},
                {"card_id": card_id, "date": day, "best_buy_price": 2, "best_sell_price": 3, "volume": 5},
            ],
        )

    monkeypatch.setattr(sync, "_build_rows_from_payload", fake_build)
    monkeypatch.setattr(market_sync, "text", lambda sql: f"SQL:{sql}")

    class FakeExcluded:
        def __init__(self):
            self.best_buy_price = "ex.best_buy_price"
            self.best_sell_price = "ex.best_sell_price"
            self.volume = "ex.volume"

    class FakeInsert:
        def __init__(self, table):
            self.table = table
            self.excluded = FakeExcluded()
            self.rows = None

        def values(self, rows):
            self.rows = rows
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            return {"rows": self.rows, "index_elements": index_elements, "set_": set_}

    monkeypatch.setattr(market_sync, "pg_insert", lambda table: FakeInsert(table))

    class DummyFuture:
        def __init__(self, value=None, exc=None):
            self._value = value
            self._exc = exc

        def result(self):
            if self._exc:
                raise self._exc
            return self._value

    class DummyExecutor:
        def __init__(self, max_workers=2):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, *args, **kwargs):
            try:
                value = func(*args, **kwargs)
                return DummyFuture(value=value)
            except Exception as e:
                return DummyFuture(exc=e)

    monkeypatch.setattr(market_sync, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(market_sync, "as_completed", lambda futures: list(futures))

    session = FakeSession()
    sync.run(session)

    merged_stmt = next(stmt for stmt in session.exec_calls if isinstance(stmt, dict) and "rows" in stmt)
    assert merged_stmt["rows"] == [
        {"card_id": "card-1", "date": day, "best_buy_price": 2, "best_sell_price": 3, "volume": 5}
    ]


def test_fetch_market_payload_with_retry_eventually_succeeds(monkeypatch):
    sync = market_sync.MarketSync(max_fetch_retries=3, fetch_backoff_s=0.01, fetch_backoff_cap_s=0.01)
    calls = {"count": 0}

    def flaky_fetch(source_uuid):
        calls["count"] += 1
        if calls["count"] < 3:
            raise RuntimeError("boom")
        return {"ok": True}

    monkeypatch.setattr(sync, "_fetch_market_payload_jitter", flaky_fetch)
    monkeypatch.setattr(market_sync.time, "sleep", lambda s: None)

    payload = sync._fetch_market_payload_with_retry("uuid-1")

    assert payload == {"ok": True}
    assert calls["count"] == 3
