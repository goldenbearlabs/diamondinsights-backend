import types
from types import SimpleNamespace

import apps.jobs.card_sync as card_sync
from shared.db.models import Series, Quirk, Location


class FakeResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items


class FakeSession:
    def __init__(self, results=None):
        self.results = results or []
        self.merged = []
        self.expunged = []
        self.flush_called = False

    def merge(self, obj):
        self.merged.append(obj)

    def flush(self):
        self.flush_called = True

    def execute(self, stmt):
        return FakeResult(self.results)

    def expunge(self, obj):
        self.expunged.append(obj)


class FakeSessionExec:
    def __init__(self):
        self.exec_calls = []
        self.commits = 0

    def execute(self, stmt):
        self.exec_calls.append(stmt)

    def commit(self):
        self.commits += 1


def test_sync_series_dedupes_and_includes_unknown():
    session = FakeSession(results=[Series(name="UNKNOWN"), Series(name="Live")])
    sync = card_sync.CardSync()

    raw_data = [
        {"series": " Live "},
        {"series": ""},
        {"series": None},
        {"series": "Live"},
    ]

    series_map = sync._sync_series(session, raw_data)

    assert set(series_map.keys()) == {"UNKNOWN", "Live"}
    assert session.flush_called is True
    assert len(session.merged) == 2
    assert len(session.expunged) == 2


def test_sync_quirks_dedupes_and_skips_missing_names():
    session = FakeSession(results=[Quirk(name="Q1", description="d", img="i")])
    sync = card_sync.CardSync()

    raw_data = [
        {"quirks": [{"name": "Q1", "description": "d", "img": "i"}, {"name": "Q1"}, {"name": ""}, {}]},
        {"quirks": None},
    ]

    quirk_map = sync._sync_quirks(session, raw_data)

    assert set(quirk_map.keys()) == {"Q1"}
    assert session.flush_called is True
    assert len(session.merged) == 1
    assert len(session.expunged) == 1


def test_sync_locations_dedupes_and_skips_empty():
    session = FakeSession(results=[Location(name="LOC1"), Location(name="LOC2")])
    sync = card_sync.CardSync()

    raw_data = [
        {"locations": ["LOC1", "", None, "LOC2", "LOC1"]},
        {"locations": None},
    ]

    loc_map = sync._sync_locations(session, raw_data)

    assert set(loc_map.keys()) == {"LOC1", "LOC2"}
    assert session.flush_called is True
    assert len(session.merged) == 2
    assert len(session.expunged) == 2


def test_upsert_cards_chunks_and_executes(monkeypatch):
    class DummyCol:
        def __init__(self, name):
            self.name = name

    class DummyTable:
        columns = [DummyCol("id"), DummyCol("name")]

    class DummyCard:
        __table__ = DummyTable()

        def __init__(self, card_id, name):
            self.id = card_id
            self.name = name

    class DummyAttr:
        def __init__(self, key):
            self.key = key

    def fake_inspect(model):
        return types.SimpleNamespace(mapper=types.SimpleNamespace(column_attrs=[DummyAttr("id"), DummyAttr("name")]))

    class FakeExcluded:
        def __getitem__(self, name):
            return f"excluded.{name}"

    class FakeInsert:
        def __init__(self, table):
            self.table = table
            self.excluded = FakeExcluded()
            self.rows = None

        def values(self, rows):
            self.rows = rows
            return self

        def on_conflict_do_update(self, index_elements=None, set_=None):
            return {
                "rows": self.rows,
                "index_elements": index_elements,
                "set_": set_,
            }

    monkeypatch.setattr(card_sync, "Card", DummyCard)
    monkeypatch.setattr(card_sync, "sa_inspect", fake_inspect)
    monkeypatch.setattr(card_sync, "insert", lambda table: FakeInsert(table))
    monkeypatch.setattr(card_sync, "text", lambda sql: f"SQL:{sql}")

    session = FakeSessionExec()
    sync = card_sync.CardSync()
    cards = [DummyCard("1", "A"), DummyCard("2", "B"), DummyCard("3", "C")]

    sync._upsert_cards(session, cards, chunk_size=2)

    assert session.commits == 2
    assert len(session.exec_calls) == 4
    assert session.exec_calls[0] == "SQL:SET LOCAL synchronous_commit TO OFF"
    assert session.exec_calls[2] == "SQL:SET LOCAL synchronous_commit TO OFF"

    stmt1 = session.exec_calls[1]
    assert stmt1["rows"] == [{"id": "1", "name": "A"}, {"id": "2", "name": "B"}]
    assert stmt1["index_elements"] == ["id"]
    assert stmt1["set_"] == {"name": "excluded.name"}

    stmt2 = session.exec_calls[3]
    assert stmt2["rows"] == [{"id": "3", "name": "C"}]
    assert stmt2["index_elements"] == ["id"]
    assert stmt2["set_"] == {"name": "excluded.name"}


def test_upsert_card_quirks_replaces_links_and_inserts_rows(monkeypatch):
    class DummyAssocCols:
        card_id = "card_id_col"

    class DummyAssoc:
        c = DummyAssocCols()

    class DummyDeleteWhere:
        def __init__(self, condition):
            self.condition = condition

    class DummyDelete:
        def where(self, condition):
            return DummyDeleteWhere(condition)

    class DummyCardIdCol:
        def in_(self, card_ids):
            return ("in", tuple(card_ids))

    class FakeInsertStmt:
        def __init__(self):
            self.rows = None

        def values(self, rows):
            self.rows = rows
            return self

        def on_conflict_do_nothing(self, index_elements=None):
            return {
                "rows": self.rows,
                "index_elements": index_elements,
            }

    monkeypatch.setattr(card_sync, "card_quirk_association", DummyAssoc())
    monkeypatch.setattr(card_sync.card_quirk_association, "c", SimpleNamespace(card_id=DummyCardIdCol()))
    monkeypatch.setattr(card_sync, "delete", lambda table: DummyDelete())
    monkeypatch.setattr(card_sync, "insert", lambda table: FakeInsertStmt())
    monkeypatch.setattr(card_sync, "text", lambda sql: f"SQL:{sql}")

    session = FakeSessionExec()
    sync = card_sync.CardSync()

    q1 = SimpleNamespace(name="Q1")
    q2 = SimpleNamespace(name="Q2")
    cards = [
        SimpleNamespace(id="c1", quirks=[q1, q1, q2]),
        SimpleNamespace(id="c2", quirks=[]),
        SimpleNamespace(id="c3", quirks=[SimpleNamespace(name="")]),
    ]

    sync._upsert_card_quirks(session, cards, chunk_size=2)

    assert session.commits == 2
    assert len(session.exec_calls) == 5
    assert session.exec_calls[0] == "SQL:SET LOCAL synchronous_commit TO OFF"
    assert isinstance(session.exec_calls[1], DummyDeleteWhere)
    assert session.exec_calls[1].condition == ("in", ("c1", "c2"))
    assert session.exec_calls[2]["rows"] == [
        {"card_id": "c1", "quirk_name": "Q1"},
        {"card_id": "c1", "quirk_name": "Q2"},
    ]
    assert session.exec_calls[2]["index_elements"] == ["card_id", "quirk_name"]
    assert session.exec_calls[3] == "SQL:SET LOCAL synchronous_commit TO OFF"
    assert isinstance(session.exec_calls[4], DummyDeleteWhere)
    assert session.exec_calls[4].condition == ("in", ("c3",))


def test_card_sync_run_flow_dedupes_and_calls_components(monkeypatch):
    calls = []
    received = {}

    def fake_fetch(self, url, params):
        calls.append((url, params.copy()))
        if "mlb2024" in url:
            return [
                {"uuid": "a"},
                {"uuid": "a"},
                {"uuid": None},
            ]
        if "mlb2025" in url:
            return [
                {"uuid": "b"},
            ]
        return []

    def fake_sync_series(self, session, items):
        received["series_items"] = list(items)
        return {"Series": object()}

    def fake_sync_quirks(self, session, items):
        received["quirk_items"] = list(items)
        return {"Quirk": object()}

    def fake_sync_locations(self, session, items):
        received["loc_items"] = list(items)
        return {"Location": object()}

    class FakeAdapter:
        def __init__(self, series_map, quirk_map, location_map):
            received["maps"] = (series_map, quirk_map, location_map)

        def run(self, items):
            received["adapter_items"] = list(items)
            return ["card-1", "card-2"]

    def fake_upsert(self, session, cards, chunk_size=5000):
        received["upsert_cards"] = cards
        received["chunk_size"] = chunk_size

    def fake_upsert_card_quirks(self, session, cards, chunk_size=5000):
        received["upsert_card_quirks"] = cards
        received["quirk_chunk_size"] = chunk_size

    def fake_upsert_pitches(self, session, cards, chunk_size=5000):
        received["upsert_pitches"] = cards
        received["pitch_chunk_size"] = chunk_size

    monkeypatch.setattr(card_sync, "THE_SHOW_YEARS", [2024, 2025])
    monkeypatch.setattr(card_sync.CardSync, "_fetch_paginated_data", fake_fetch)
    monkeypatch.setattr(card_sync.CardSync, "_sync_series", fake_sync_series)
    monkeypatch.setattr(card_sync.CardSync, "_sync_quirks", fake_sync_quirks)
    monkeypatch.setattr(card_sync.CardSync, "_sync_locations", fake_sync_locations)
    monkeypatch.setattr(card_sync, "CardAdapter", FakeAdapter)
    monkeypatch.setattr(card_sync.CardSync, "_upsert_cards", fake_upsert)
    monkeypatch.setattr(card_sync.CardSync, "_upsert_card_quirks", fake_upsert_card_quirks)
    monkeypatch.setattr(card_sync.CardSync, "_upsert_pitches", fake_upsert_pitches)

    sync = card_sync.CardSync(reload_all_years=True)
    sync.run(db_session=object())

    assert calls == [
        ("https://mlb2024.theshow.com/apis/items.json", {"type": "mlb_card"}),
        ("https://mlb2025.theshow.com/apis/items.json", {"type": "mlb_card"}),
    ]

    all_items = received["series_items"]
    assert len(all_items) == 2
    assert {item["source_uuid"] for item in all_items} == {"a", "b"}
    assert {item["year"] for item in all_items} == {2024, 2025}

    assert received["adapter_items"] == all_items
    assert received["upsert_cards"] == ["card-1", "card-2"]
    assert received["chunk_size"] == 5000
    assert received["upsert_card_quirks"] == ["card-1", "card-2"]
    assert received["quirk_chunk_size"] == 5000
    assert received["upsert_pitches"] == ["card-1", "card-2"]
    assert received["pitch_chunk_size"] == 5000


def test_card_sync_run_respects_reload_all_years(monkeypatch):
    calls = []

    def fake_fetch(self, url, params):
        calls.append(url)
        return []

    monkeypatch.setattr(card_sync, "THE_SHOW_YEARS", [2024, 2025])
    monkeypatch.setattr(card_sync.CardSync, "_fetch_paginated_data", fake_fetch)
    monkeypatch.setattr(card_sync.CardSync, "_sync_series", lambda self, session, items: {})
    monkeypatch.setattr(card_sync.CardSync, "_sync_quirks", lambda self, session, items: {})
    monkeypatch.setattr(card_sync.CardSync, "_sync_locations", lambda self, session, items: {})
    monkeypatch.setattr(card_sync, "CardAdapter", lambda series_map, quirk_map, location_map: types.SimpleNamespace(run=lambda items: []))
    monkeypatch.setattr(card_sync.CardSync, "_upsert_cards", lambda self, session, cards, chunk_size=5000: None)
    monkeypatch.setattr(card_sync.CardSync, "_upsert_card_quirks", lambda self, session, cards, chunk_size=5000: None)
    monkeypatch.setattr(card_sync.CardSync, "_upsert_pitches", lambda self, session, cards, chunk_size=5000: None)

    sync = card_sync.CardSync(reload_all_years=False)
    sync.run(db_session=object())

    assert calls == ["https://mlb2024.theshow.com/apis/items.json"]
