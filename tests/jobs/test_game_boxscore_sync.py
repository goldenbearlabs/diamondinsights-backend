import datetime

import pytest

import apps.jobs.game_boxscore_sync as gbs


class HelperGameBoxscoreSync(gbs.GameBoxscoreSync):
    def run(self, db_session):
        return self.execute(db_session)


class DummyScalars:
    def __init__(self, values):
        self._values = values

    def all(self):
        return self._values


class DummyScalarResult:
    def __init__(self, value=None, scalars=None):
        self._value = value
        self._scalars = scalars if scalars is not None else []

    def scalar_one_or_none(self):
        return self._value

    def scalars(self):
        return DummyScalars(self._scalars)


class DummySession:
    def __init__(self, scalar_value=None, execute_results=None):
        self.scalar_value = scalar_value
        self.execute_results = list(execute_results) if execute_results else []
        self.execute_calls = 0
        self.executed = []
        self.added = []
        self.merged = []
        self.flushed = 0
        self.commits = 0

    def execute(self, stmt):
        self.execute_calls += 1
        self.executed.append(stmt)
        if self.execute_results:
            return self.execute_results.pop(0)
        return DummyScalarResult(self.scalar_value)

    def add(self, obj):
        self.added.append(obj)

    def merge(self, obj):
        self.merged.append(obj)

    def flush(self):
        self.flushed += 1
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                obj.id = 1

    def commit(self):
        self.commits += 1


class DummyColumn:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ("eq", self.name, other)

    def is_(self, other):
        return ("is", self.name, other)


class DummyBirthLocation:
    id = DummyColumn("id")
    city = DummyColumn("city")
    state_province = DummyColumn("state_province")
    country = DummyColumn("country")

    def __init__(self, city, state_province, country):
        self.city = city
        self.state_province = state_province
        self.country = country
        self.id = None


class DummyClient:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, params=None):
        return self.payload


class DummyExcluded:
    def __getitem__(self, key):
        return f"excluded.{key}"


class DummyInsert:
    def __init__(self, model):
        self.model = model
        self.excluded = DummyExcluded()
        self.values_payload = None
        self.conflict_args = None

    def values(self, payload):
        self.values_payload = payload
        return self

    def on_conflict_do_update(self, index_elements=None, set_=None):
        self.conflict_args = (index_elements, set_)
        return self


class DummyFuture:
    def __init__(self, fn, *args, **kwargs):
        self._exc = None
        self._result = None
        try:
            self._result = fn(*args, **kwargs)
        except Exception as exc:
            self._exc = exc

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._result


class DummyPool:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers
        self.futures = []

    def submit(self, fn, *args, **kwargs):
        fut = DummyFuture(fn, *args, **kwargs)
        self.futures.append(fut)
        return fut

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyStmt:
    def where(self, *args, **kwargs):
        return self

    def limit(self, *args, **kwargs):
        return self

    def __invert__(self):
        return self


def _dummy_as_completed(futures):
    return list(futures)


def _dummy_model(columns):
    table = type("DummyTable", (), {"columns": [DummyColumn(c) for c in columns]})
    return type("DummyModel", (), {"__table__": table})


def test_season_window_with_fixed_year():
    sync = HelperGameBoxscoreSync(season_year=2024)
    season, start, end = sync._season_window()

    assert season == 2024
    assert start == "2024-02-01"
    assert end == "2025-12-01"


def test_season_window_infers_year(monkeypatch):
    class FakeDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2026, 1, 15)

    monkeypatch.setattr(gbs.datetime, "date", FakeDate)

    sync = HelperGameBoxscoreSync(season_year=None)
    season, start, end = sync._season_window()

    assert season == 2025
    assert start == "2025-02-01"
    assert end == "2026-12-01"


def test_season_window_infers_year_after_anchor(monkeypatch):
    class FakeDate(datetime.date):
        @classmethod
        def today(cls):
            return cls(2026, 12, 15)

    monkeypatch.setattr(gbs.datetime, "date", FakeDate)

    sync = HelperGameBoxscoreSync(season_year=None)
    season, start, end = sync._season_window()

    assert season == 2026
    assert start == "2026-02-01"
    assert end == "2027-12-01"


def test_collect_games_filters_and_dedupes():
    sync = HelperGameBoxscoreSync()
    dates = [
        {
            "games": [
                {"gameType": "S", "gamePk": 1},
                {"gameType": "R", "gamePk": None},
                {"gameType": "R", "gamePk": "bad"},
                {"gameType": "R", "gamePk": 2},
                {"gameType": "R", "gamePk": 2},
            ]
        }
    ]

    games = sync._collect_games(dates)

    assert len(games) == 1
    assert games[0]["gamePk"] == 2


def test_collect_team_ids_and_extract():
    sync = HelperGameBoxscoreSync()

    game = {
        "teams": {
            "away": {"team": {"id": 10}},
            "home": {"team": {"id": 20}},
        }
    }

    away, home = sync._extract_home_away_ids(game)
    assert away == 10
    assert home == 20

    ids = sync._collect_team_ids([game])
    assert ids == {10, 20}


def test_extract_home_away_ids_missing_values():
    sync = HelperGameBoxscoreSync()
    away, home = sync._extract_home_away_ids({"teams": {"away": {"team": {}}, "home": {}}})
    assert away is None
    assert home is None


def test_parse_dt_utc_naive_and_parse_date():
    sync = HelperGameBoxscoreSync()

    assert sync._parse_dt_utc_naive(None) is None

    dt = sync._parse_dt_utc_naive("2026-01-01T00:00:00Z")
    assert dt.tzinfo is None
    assert dt == datetime.datetime(2026, 1, 1, 0, 0, 0)

    dt = sync._parse_dt_utc_naive("2026-01-01T03:00:00+03:00")
    assert dt == datetime.datetime(2026, 1, 1, 0, 0, 0)

    assert sync._parse_dt_utc_naive("bad") is None

    assert sync._parse_date(None) is None
    assert sync._parse_date("2026-01-01") == datetime.date(2026, 1, 1)
    assert sync._parse_date("bad") is None


def test_safe_int():
    sync = HelperGameBoxscoreSync()
    assert sync._safe_int(None) is None
    assert sync._safe_int("10") == 10
    assert sync._safe_int("bad") is None


def test_fetch_schedule_calls_client():
    sync = HelperGameBoxscoreSync()
    calls = {}

    class RecordingClient:
        def get(self, url, params=None):
            calls["url"] = url
            calls["params"] = params
            return {"ok": True}

    sync._api_client = RecordingClient()
    out = sync._fetch_schedule("2025-02-01", "2025-12-01")

    assert out == {"ok": True}
    assert calls["params"]["sportId"] == 1


def test_fetch_playbyplay_worker(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(gbs.time, "sleep", lambda s: None)

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient({}))
    assert sync._fetch_playbyplay_worker(1) is None

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient({"allPlays": []}))
    assert sync._fetch_playbyplay_worker(1) is None

    payload = {"allPlays": [1]}
    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient(payload))
    assert sync._fetch_playbyplay_worker(1) == payload


def test_fetch_boxscore_worker_aggregates(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(gbs.time, "sleep", lambda s: None)

    payload = {
        "teams": {
            "home": {
                "team": {"id": 1},
                "players": {
                    "ID10": {
                        "person": {"id": 10},
                        "stats": {"fielding": {"assists": "1", "putOuts": "2", "errors": "0"}},
                    },
                    "ID11": {
                        "person": {"id": 11},
                        "stats": {"fielding": {"assists": "0", "putOuts": "0", "errors": "0"}},
                    },
                },
            },
            "away": {
                "team": {"id": 2},
                "players": {
                    "ID10": {
                        "person": {"id": 10},
                        "stats": {"fielding": {"assists": "2", "putOuts": "1", "errors": "1", "chances": "4", "passedBall": "1"}},
                    }
                },
            },
        }
    }

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient(payload))

    boxscore_rows, fielding_rows, player_ids = sync._fetch_boxscore_worker(99)

    assert set(player_ids) == {10, 11}
    assert sorted(boxscore_rows, key=lambda r: r["player_id"]) == [
        {"game_id": 99, "player_id": 10, "team_id": 1},
        {"game_id": 99, "player_id": 11, "team_id": 1},
    ]

    assert len(fielding_rows) == 1
    row = fielding_rows[0]
    assert row["player_id"] == 10
    assert row["assists"] == 3
    assert row["put_outs"] == 3
    assert row["errors"] == 1
    assert row["chances"] == 7
    assert row["passed_balls"] == 1


def test_fetch_boxscore_worker_skips_missing_team_and_person(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(gbs.time, "sleep", lambda s: None)

    payload = {
        "teams": {
            "home": {"team": {}, "players": {}},
            "away": {
                "team": {"id": 2},
                "players": {
                    "ID10": {"person": {}, "stats": {"fielding": {"assists": "1"}}},
                },
            },
        }
    }

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient(payload))

    boxscore_rows, fielding_rows, player_ids = sync._fetch_boxscore_worker(99)

    assert boxscore_rows == []
    assert fielding_rows == []
    assert player_ids == set()


def test_upsert_birth_location_caches(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "BirthLocation", DummyBirthLocation)

    class DummyStmt:
        def where(self, *args, **kwargs):
            return self

    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())

    session = DummySession(scalar_value=None)

    loc_id = sync._upsert_birth_location(session, city="City", state_province=None, country="Country")
    assert loc_id == 1
    assert session.execute_calls == 1

    loc_id2 = sync._upsert_birth_location(session, city="City", state_province=None, country="Country")
    assert loc_id2 == 1
    assert session.execute_calls == 1


def test_upsert_birth_location_existing(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "BirthLocation", DummyBirthLocation)

    class DummyStmt:
        def where(self, *args, **kwargs):
            return self

    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())

    session = DummySession(scalar_value=7)

    loc_id = sync._upsert_birth_location(session, city="City", state_province="ST", country="Country")
    assert loc_id == 7
    assert session.added == []


def test_upsert_birth_location_requires_city_and_country(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "BirthLocation", DummyBirthLocation)

    session = DummySession(scalar_value=None)

    assert sync._upsert_birth_location(session, city="", state_province=None, country="Country") is None
    assert sync._upsert_birth_location(session, city="City", state_province=None, country="") is None


def test_player_row_from_person():
    sync = HelperGameBoxscoreSync()

    person = {
        "id": 1,
        "birthDate": "2020-01-01",
        "fullName": "A B",
        "firstName": "A",
        "lastName": "B",
        "primaryNumber": "10",
        "currentAge": 25,
        "height": "6'1",
        "weight": 200,
        "active": True,
        "boxscoreName": "A B",
        "draftYear": 2019,
        "mlbDebutDate": "2021-04-01",
        "batSide": {"code": "R"},
        "pitchHand": {"code": "L"},
        "strikeZoneTop": 3.5,
        "strikeZoneBottom": 1.5,
    }

    row = sync._player_row_from_person(person)

    assert row["mlb_id"] == 1
    assert row["birth_date"] == datetime.date(2020, 1, 1)
    assert row["bat_side_code"] == "R"

    assert sync._player_row_from_person({"id": 1, "birthDate": "bad"}) is None
    assert sync._player_row_from_person({"birthDate": "2020-01-01"}) is None


def test_upsert_people_from_people_payload(monkeypatch):
    sync = HelperGameBoxscoreSync()
    sync._player_exists_cache = {1}

    calls = {"players": []}

    monkeypatch.setattr(sync, "_upsert_position", lambda session, pos_id, name, abbr: None)
    monkeypatch.setattr(sync, "_upsert_birth_location", lambda *args, **kwargs: 5)
    monkeypatch.setattr(sync, "_upsert_player", lambda session, row: calls["players"].append(row))

    def fake_player_row(person):
        return {
            "mlb_id": int(person.get("id")),
            "birth_date": datetime.date(2020, 1, 1),
        }

    monkeypatch.setattr(sync, "_player_row_from_person", fake_player_row)

    people_by_id = {
        1: {"id": 1, "primaryPosition": {"code": "1", "name": "P", "abbreviation": "P"}},
        2: {"id": 2, "primaryPosition": {"code": "1", "name": "P", "abbreviation": "P"}},
        3: None,
        4: {"id": 4, "primaryPosition": {"code": None}},
    }

    created, updated, failed = sync._upsert_people_from_people_payload(DummySession(), people_by_id)

    assert created == 1
    assert updated == 1
    assert failed == 2
    assert sync._player_exists_cache == {1, 2}


def test_upsert_people_from_people_payload_row_missing(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(sync, "_upsert_position", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_birth_location", lambda *args, **kwargs: 1)
    monkeypatch.setattr(sync, "_upsert_player", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_player_row_from_person", lambda person: None)

    people_by_id = {
        1: {"id": 1, "primaryPosition": {"code": "1", "name": "P", "abbreviation": "P"}},
    }

    created, updated, failed = sync._upsert_people_from_people_payload(DummySession(), people_by_id)
    assert created == 0
    assert updated == 0
    assert failed == 1


def test_fetch_team_worker_success_and_empty(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(gbs.time, "sleep", lambda s: None)

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient({"teams": [{"id": 1, "name": "A"}]}))
    team = sync._fetch_team_worker(1, 2025)
    assert team["id"] == 1

    monkeypatch.setattr(gbs, "APIClient", lambda: DummyClient({"teams": []}))
    with pytest.raises(RuntimeError):
        sync._fetch_team_worker(1, 2025)


def test_upsert_teams_handles_created_updated_failed(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs, "ThreadPoolExecutor", DummyPool)
    monkeypatch.setattr(gbs, "as_completed", _dummy_as_completed)
    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())

    class DummyTeam:
        id = DummyColumn("id")

        def __init__(self, **kwargs):
            self.kwargs = kwargs

    monkeypatch.setattr(gbs, "MLBTeam", DummyTeam)

    def fetch_team(tid, season):
        if tid == 3:
            raise RuntimeError("boom")
        return {"id": tid, "name": f"T{tid}"}

    monkeypatch.setattr(sync, "_fetch_team_worker", fetch_team)

    session = DummySession(execute_results=[DummyScalarResult(scalars=[2])])

    sync._upsert_teams(session, {1, 2, 3}, 2025)

    assert len(session.merged) == 2
    assert session.flushed == 1


def test_upsert_teams_empty_returns(monkeypatch):
    sync = HelperGameBoxscoreSync()
    session = DummySession()
    sync._upsert_teams(session, set(), 2025)
    assert session.execute_calls == 0


def test_target_game_ids_for_boxscores_paths(monkeypatch):
    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())
    monkeypatch.setattr(gbs, "exists", lambda *args, **kwargs: DummyStmt())

    sync = HelperGameBoxscoreSync(season_year=2025, rerun_all_boxscores=True)
    session = DummySession(execute_results=[DummyScalarResult(scalars=[1, 2])])
    assert sync._target_game_ids_for_boxscores(session, 2025) == [1, 2]

    sync = HelperGameBoxscoreSync(season_year=2025, rerun_all_boxscores=False)
    session = DummySession(execute_results=[DummyScalarResult(scalars=[3])])
    assert sync._target_game_ids_for_boxscores(session, 2025) == [3]


def test_prime_player_exists_cache(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())
    session = DummySession(execute_results=[DummyScalarResult(scalars=[1, 2])])
    sync._prime_player_exists_cache(session)
    assert sync._player_exists_cache == {1, 2}


def test_fetch_people_bulk_handles_errors(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(gbs, "ThreadPoolExecutor", DummyPool)
    monkeypatch.setattr(gbs, "as_completed", _dummy_as_completed)
    monkeypatch.setattr(gbs.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(gbs.time, "sleep", lambda s: None)

    class PeopleClient:
        def get(self, url, params=None):
            if url.endswith("/2"):
                raise RuntimeError("boom")
            return {"people": [{"id": int(url.split("/")[-1])}]}

    monkeypatch.setattr(gbs, "APIClient", PeopleClient)

    out = sync._fetch_people_bulk({1, 2})
    assert out[1]["id"] == 1
    assert out[2] is None


def test_upsert_games_and_stats(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "insert", lambda model: DummyInsert(model))

    monkeypatch.setattr(gbs, "MLBGame", _dummy_model(["id", "season"]))
    monkeypatch.setattr(gbs, "MLBGameBoxscore", _dummy_model(["game_id", "player_id", "team_id"]))
    monkeypatch.setattr(gbs, "MLBGameBattingStats", _dummy_model(["game_id", "player_id", "split", "ab"]))
    monkeypatch.setattr(gbs, "MLBGamePitchingStats", _dummy_model(["game_id", "player_id", "split", "outs_pitched"]))
    monkeypatch.setattr(gbs, "MLBGameBaserunningStats", _dummy_model(["game_id", "player_id", "sb", "caught_stealing"]))
    monkeypatch.setattr(gbs, "MLBGameFieldingStats", _dummy_model(["game_id", "player_id", "assists", "errors"]))

    session = DummySession()

    sync._upsert_games(session, [{"id": 1, "season": 2025}], chunk_size=1)
    assert session.execute_calls == 1
    assert session.flushed == 1

    assert sync._upsert_game_boxscores(session, [{"game_id": 1, "player_id": 2, "team_id": 3}]) == (1, 1)

    sync._upsert_batting_stats(session, [{"game_id": 1, "player_id": 2, "split": "", "ab": 1}])
    sync._upsert_pitching_stats(session, [{"game_id": 1, "player_id": 2, "split": "", "outs_pitched": 3}])
    sync._upsert_baserunning_stats(session, [{"game_id": 1, "player_id": 2, "sb": 1, "caught_stealing": 0}])
    sync._upsert_fielding_stats(
        session,
        [
            {
                "game_id": 1,
                "player_id": 2,
                "assists": 1,
                "put_outs": 1,
                "errors": 0,
                "chances": 2,
                "passed_balls": 0,
                "pickoffs": 0,
                "stolen_bases_allowed": 0,
                "caught_stealing": 0,
            }
        ],
    )

    sync._upsert_games(session, [])
    assert sync._upsert_game_boxscores(session, []) == (0, 0)
    sync._upsert_batting_stats(session, [])
    sync._upsert_pitching_stats(session, [])
    sync._upsert_baserunning_stats(session, [])
    sync._upsert_fielding_stats(session, [])


def test_upsert_position_and_player(monkeypatch):
    sync = HelperGameBoxscoreSync()
    monkeypatch.setattr(gbs, "insert", lambda model: DummyInsert(model))
    session = DummySession()

    sync._upsert_position(session, 1, "Pitcher", "P")
    sync._upsert_player(session, {"mlb_id": 1})
    assert session.execute_calls == 2


def test_execute_returns_when_no_boxscores(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(sync, "_season_window", lambda: (2025, "2025-02-01", "2025-12-01"))
    monkeypatch.setattr(sync, "_fetch_schedule", lambda start, end: {"dates": []})
    monkeypatch.setattr(sync, "_collect_games", lambda dates: [])
    monkeypatch.setattr(sync, "_collect_team_ids", lambda games: set())
    monkeypatch.setattr(sync, "_upsert_teams", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_games", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_target_game_ids_for_boxscores", lambda *args, **kwargs: [])

    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())

    session = DummySession(execute_results=[DummyScalarResult(scalars=[])])
    sync.execute(session)


def test_execute_full_flow(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(sync, "_season_window", lambda: (2025, "2025-02-01", "2025-12-01"))

    games = [
        {"gamePk": None},
        {"gamePk": 1, "gameType": "R", "teams": {"home": {"team": {"id": 20}}}},
        {"gamePk": 2, "gameType": "", "teams": {"away": {"team": {"id": 10}}, "home": {"team": {"id": 20}}}},
        {
            "gamePk": 3,
            "gameType": "R",
            "teams": {"away": {"team": {"id": 99}}, "home": {"team": {"id": 20}}},
            "gameDate": "2025-04-01T00:00:00Z",
        },
        {
            "gamePk": 4,
            "gameType": "R",
            "teams": {"away": {"team": {"id": 10}}, "home": {"team": {"id": 20}}},
            "gameDate": None,
        },
        {
            "gamePk": 5,
            "gameType": "R",
            "season": 2025,
            "gameDate": "2025-04-01T00:00:00Z",
            "status": {"statusCode": "F"},
            "teams": {"away": {"team": {"id": 10}}, "home": {"team": {"id": 20}}},
        },
    ]

    monkeypatch.setattr(sync, "_fetch_schedule", lambda start, end: {"dates": []})
    monkeypatch.setattr(sync, "_collect_games", lambda dates: games)
    monkeypatch.setattr(sync, "_collect_team_ids", lambda games: {10, 20})
    monkeypatch.setattr(sync, "_upsert_teams", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_games", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_target_game_ids_for_boxscores", lambda *args, **kwargs: [5])

    def prime_cache(_session):
        sync._player_exists_cache = {1}

    monkeypatch.setattr(sync, "_prime_player_exists_cache", prime_cache)

    def fetch_boxscore(game_id):
        return (
            [
                {"game_id": game_id, "player_id": 1, "team_id": 10},
                {"game_id": game_id, "player_id": 2, "team_id": 10},
            ],
            [
                {
                    "game_id": game_id,
                    "player_id": 1,
                    "assists": 1,
                    "put_outs": 1,
                    "errors": 0,
                    "chances": 2,
                    "passed_balls": 0,
                    "pickoffs": 0,
                    "stolen_bases_allowed": 0,
                    "caught_stealing": 0,
                }
            ],
            {1, 2},
        )

    monkeypatch.setattr(sync, "_fetch_boxscore_worker", fetch_boxscore)

    def upsert_people(_session, people):
        for pid, person in people.items():
            if person:
                sync._player_exists_cache.add(int(pid))
        return 1, 0, 0

    monkeypatch.setattr(sync, "_fetch_people_bulk", lambda ids: {2: {"id": 2}})
    monkeypatch.setattr(sync, "_upsert_people_from_people_payload", upsert_people)

    calls = {"box": 0, "field": 0, "bat": 0, "br": 0, "pit": 0}

    monkeypatch.setattr(
        sync,
        "_upsert_game_boxscores",
        lambda _session, rows: calls.__setitem__("box", calls["box"] + 1) or (len(rows), len(rows)),
    )
    monkeypatch.setattr(
        sync,
        "_upsert_fielding_stats",
        lambda _session, rows: calls.__setitem__("field", calls["field"] + 1),
    )
    monkeypatch.setattr(
        sync,
        "_upsert_batting_stats",
        lambda _session, rows: calls.__setitem__("bat", calls["bat"] + 1),
    )
    monkeypatch.setattr(
        sync,
        "_upsert_baserunning_stats",
        lambda _session, rows: calls.__setitem__("br", calls["br"] + 1),
    )
    monkeypatch.setattr(
        sync,
        "_upsert_pitching_stats",
        lambda _session, rows: calls.__setitem__("pit", calls["pit"] + 1),
    )

    monkeypatch.setattr(sync, "_fetch_playbyplay_worker", lambda gid: {"allPlays": [1]})

    class DummyAgg:
        def __init__(self, rows):
            self.rows = rows

        def build_rows(self, game_id, payload):
            return self.rows

    monkeypatch.setattr(gbs, "MLBPlayByPlayBattingAggregator", lambda: DummyAgg([{"game_id": 5, "player_id": 1, "split": "", "ab": 1}]))
    monkeypatch.setattr(gbs, "MLBPlayByPlayBaserunningAggregator", lambda: DummyAgg([{"game_id": 5, "player_id": 1, "sb": 1, "caught_stealing": 0}]))
    monkeypatch.setattr(gbs, "MLBPlayByPlayPitchingAggregator", lambda: DummyAgg([{"game_id": 5, "player_id": 1, "split": "", "outs_pitched": 3}]))

    monkeypatch.setattr(gbs, "ThreadPoolExecutor", DummyPool)
    monkeypatch.setattr(gbs, "as_completed", _dummy_as_completed)

    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())
    monkeypatch.setattr(gbs, "exists", lambda *args, **kwargs: DummyStmt())

    session = DummySession(
        execute_results=[
            DummyScalarResult(scalars=[10, 20]),
            DummyScalarResult(scalars=[]),
        ]
    )

    sync.execute(session)

    assert calls["box"] == 1
    assert calls["field"] == 1
    assert calls["bat"] == 1
    assert calls["br"] == 1
    assert calls["pit"] == 1


def test_execute_handles_boxscore_and_pbp_edge_cases(monkeypatch):
    sync = HelperGameBoxscoreSync()

    monkeypatch.setattr(sync, "_season_window", lambda: (2025, "2025-02-01", "2025-12-01"))
    monkeypatch.setattr(sync, "_fetch_schedule", lambda start, end: {"dates": []})
    monkeypatch.setattr(sync, "_collect_games", lambda dates: [{"gamePk": 1}, {"gamePk": 2}, {"gamePk": 3}, {"gamePk": 4}])
    monkeypatch.setattr(sync, "_collect_team_ids", lambda games: {10, 20})
    monkeypatch.setattr(sync, "_upsert_teams", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_games", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_target_game_ids_for_boxscores", lambda *args, **kwargs: [1, 2, 3, 4])

    monkeypatch.setattr(sync, "_prime_player_exists_cache", lambda session: setattr(sync, "_player_exists_cache", {1}))

    class TruthyEmpty(set):
        def __bool__(self):
            return True

    def fetch_boxscore(game_id):
        if game_id == 1:
            raise RuntimeError("boxscore fail")
        if game_id == 2:
            return ([{"game_id": game_id, "player_id": 1, "team_id": 10}], [], TruthyEmpty())
        if game_id == 3:
            return ([{"game_id": game_id, "player_id": 2, "team_id": 10}], [], {2})
        return ([{"game_id": game_id, "player_id": 1, "team_id": 10}], [], {1})

    monkeypatch.setattr(sync, "_fetch_boxscore_worker", fetch_boxscore)
    monkeypatch.setattr(sync, "_fetch_people_bulk", lambda ids: {})
    monkeypatch.setattr(sync, "_upsert_people_from_people_payload", lambda *args, **kwargs: (0, 0, 0))

    monkeypatch.setattr(sync, "_upsert_game_boxscores", lambda *args, **kwargs: (0, 0))
    monkeypatch.setattr(sync, "_upsert_fielding_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_batting_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_baserunning_stats", lambda *args, **kwargs: None)
    monkeypatch.setattr(sync, "_upsert_pitching_stats", lambda *args, **kwargs: None)

    def fetch_pbp(game_id):
        if game_id == 3:
            raise RuntimeError("pbp fail")
        if game_id == 4:
            return None
        return {"allPlays": [1]}

    monkeypatch.setattr(sync, "_fetch_playbyplay_worker", fetch_pbp)

    class EmptyAgg:
        def build_rows(self, game_id, payload):
            return []

    monkeypatch.setattr(gbs, "MLBPlayByPlayBattingAggregator", lambda: EmptyAgg())
    monkeypatch.setattr(gbs, "MLBPlayByPlayBaserunningAggregator", lambda: EmptyAgg())
    monkeypatch.setattr(gbs, "MLBPlayByPlayPitchingAggregator", lambda: EmptyAgg())

    monkeypatch.setattr(gbs, "ThreadPoolExecutor", DummyPool)
    monkeypatch.setattr(gbs, "as_completed", _dummy_as_completed)
    monkeypatch.setattr(gbs, "select", lambda *args, **kwargs: DummyStmt())
    monkeypatch.setattr(gbs, "exists", lambda *args, **kwargs: DummyStmt())

    session = DummySession(
        execute_results=[
            DummyScalarResult(scalars=[10, 20]),
            DummyScalarResult(scalars=[]),
        ]
    )

    sync.execute(session)
