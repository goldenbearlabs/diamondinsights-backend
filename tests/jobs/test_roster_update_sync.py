from datetime import date

import pytest

import apps.jobs.roster_update_sync as roster_sync
from shared.db.models import RosterUpdate, CardUpdate


class FakeResult:
    def __init__(self, items):
        self._items = items

    def scalars(self):
        return self

    def all(self):
        return self._items


class FakeSession:
    def __init__(self, results=None, commit_raises=False):
        self.results = results or []
        self.merged = []
        self.expunged = []
        self.flushed = False
        self.commit_raises = commit_raises
        self.committed = False
        self.rolled_back = False

    def merge(self, obj):
        self.merged.append(obj)
        return obj

    def flush(self):
        self.flushed = True

    def execute(self, stmt):
        return FakeResult(self.results)

    def expunge(self, obj):
        self.expunged.append(obj)

    def commit(self):
        if self.commit_raises:
            raise RuntimeError("commit boom")
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_card_id_formats():
    sync = roster_sync.RosterUpdateSync()
    assert sync._card_id(25, "abc") == "25:abc"


def test_sync_roster_updates_skips_special_cases_and_bad_dates(monkeypatch):
    sync = roster_sync.RosterUpdateSync()

    monkeypatch.setattr(roster_sync, "MAJOR_ROSTER_UPDATES", {23: [1]})
    monkeypatch.setattr(roster_sync, "FIELDING_ROSTER_UPDATES", {23: [2]})

    raw_data = {
        "roster_updates": [
            {"id": 15, "name": "July 1, 2024"},
            {"id": 1, "name": "July 2, 2024"},
            {"id": 2, "name": "bad"},
        ]
    }

    session = FakeSession(results=[RosterUpdate(id=1, date=date(2024, 7, 2), is_major=True, is_fielding=False)])

    final_map = sync.sync_roster_updates(session, 23, raw_data)

    assert set(final_map.keys()) == {1}
    assert session.flushed is True
    assert len(session.merged) == 1
    assert len(session.expunged) == 1


def test_sync_roster_updates_skips_year_21_exception(monkeypatch):
    sync = roster_sync.RosterUpdateSync()

    raw_data = {
        "roster_updates": [
            {"id": 11, "name": "July 1, 2024"},
            {"id": 3, "name": "July 2, 2024"},
        ]
    }

    session = FakeSession(results=[RosterUpdate(id=3, date=date(2024, 7, 2), is_major=False, is_fielding=False)])

    final_map = sync.sync_roster_updates(session, 21, raw_data)

    assert set(final_map.keys()) == {3}


def test_sync_update_details_returns_when_no_changes(monkeypatch):
    sync = roster_sync.RosterUpdateSync()
    sync._api_client.get = lambda url, params=None: {"attribute_changes": []}

    session = FakeSession()
    sync.sync_update_details(session, 25, 1, date(2026, 1, 1))

    assert session.merged == []
    assert session.committed is False


def test_sync_update_details_skips_missing_uuid(monkeypatch):
    sync = roster_sync.RosterUpdateSync()
    sync._api_client.get = lambda url, params=None: {"attribute_changes": [{"item": {"uuid": ""}}]}

    session = FakeSession()
    sync.sync_update_details(session, 25, 1, date(2026, 1, 1))

    assert session.merged == []


def test_sync_update_details_builds_changes_and_handles_commit_failure(monkeypatch):
    sync = roster_sync.RosterUpdateSync()

    sync._api_client.get = lambda url, params=None: {
        "attribute_changes": [
            {
                "item": {"uuid": "u1"},
                "current_rank": 99,
                "old_rank": 90,
                "current_rarity": "R",
                "old_rarity": "C",
                "trend_display": "UP",
                "changes": [
                    {"name": "POWER", "current_value": "10", "delta": "+2", "direction": "up", "color": "green"},
                    {"name": "CONTACT", "current_value": "bad", "delta": "bad", "direction": "down", "color": "red"},
                ],
            }
        ]
    }

    session = FakeSession(commit_raises=True)
    sync.sync_update_details(session, 25, 1, date(2026, 1, 1))

    assert len(session.merged) == 1
    card_update = session.merged[0]
    assert isinstance(card_update, CardUpdate)
    assert card_update.card_id == "25:u1"
    assert len(card_update.attribute_changes) == 2

    first = card_update.attribute_changes[0]
    assert first.new_value == 10
    assert first.old_value == 8

    second = card_update.attribute_changes[1]
    assert second.new_value == 0
    assert second.old_value == 0

    assert session.rolled_back is True


def test_run_respects_reload_all_years(monkeypatch):
    sync = roster_sync.RosterUpdateSync(reload_all_years=False)

    monkeypatch.setattr(roster_sync, "THE_SHOW_YEARS", [26, 25])

    fetched = []

    def fake_get(url, params=None):
        fetched.append(url)
        return {"roster_updates": []}

    sync._api_client.get = fake_get

    monkeypatch.setattr(roster_sync.RosterUpdateSync, "sync_roster_updates", lambda self, session, year, data: {})
    monkeypatch.setattr(roster_sync.RosterUpdateSync, "sync_update_details", lambda *args, **kwargs: None)
    monkeypatch.setattr(roster_sync.time, "sleep", lambda s: None)

    sync.run(db_session=object())

    assert fetched == ["https://mlb26.theshow.com/apis/roster_updates.json"]


def test_run_calls_sync_update_and_sleep(monkeypatch):
    sync = roster_sync.RosterUpdateSync(reload_all_years=False)

    monkeypatch.setattr(roster_sync, "THE_SHOW_YEARS", [25])

    class DummyUpdate:
        def __init__(self):
            self.id = 1
            self.date = date(2026, 1, 1)

    sync._api_client.get = lambda url: {"roster_updates": []}

    calls = {"update": [], "slept": 0}

    monkeypatch.setattr(
        roster_sync.RosterUpdateSync,
        "sync_roster_updates",
        lambda self, session, year, data: {1: DummyUpdate()},
    )
    monkeypatch.setattr(
        roster_sync.RosterUpdateSync,
        "sync_update_details",
        lambda self, session, year, update_id, update_date: calls["update"].append((year, update_id, update_date)),
    )
    monkeypatch.setattr(roster_sync.time, "sleep", lambda s: calls.__setitem__("slept", calls["slept"] + s))

    sync.run(db_session=object())

    assert calls["update"] == [(25, 1, date(2026, 1, 1))]
    assert calls["slept"] == 1
