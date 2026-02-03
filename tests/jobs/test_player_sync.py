import datetime

import apps.jobs.player_sync as player_sync


class DummyClient:
    def __init__(self, payload):
        self.payload = payload

    def get(self, url, params=None):
        return self.payload


class DummyResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows

    def scalars(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def yield_per(self, n):
        return iter(self._rows)


class DummySession:
    def __init__(self, execute_rows=None, existing=None):
        self.execute_rows = execute_rows or []
        self.existing = existing
        self.merged = []
        self.flushed = 0
        self.added = []

    def execute(self, stmt):
        return DummyResult(self.execute_rows)

    def get(self, model, key):
        return self.existing

    def merge(self, obj):
        self.merged.append(obj)
        return obj

    def flush(self):
        self.flushed += 1

    def add(self, obj):
        self.added.append(obj)
        obj.id = 1


class DummyUpdateResult:
    def __init__(self, rowcount=1):
        self.rowcount = rowcount


class DummySelectResult:
    def __init__(self, rows):
        self._rows = rows

    def yield_per(self, n):
        return iter(self._rows)

    def all(self):
        return self._rows

    def scalars(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None


def test_norm_and_norm_name():
    sync = player_sync.PlayerSync()

    assert sync._norm(" José  A.  B ") == "jose a b"
    assert sync._norm_name("J. R. R. Tolkien") == "jrr tolkien"
    assert sync._norm_name("") == ""
    assert sync._norm_name("J Tolkien") == "j tolkien"


def test_height_and_weight_parsing():
    sync = player_sync.PlayerSync()

    assert sync._height_to_inches("6'2") == 74
    assert sync._height_to_inches("bad") is None

    assert sync._weight_to_lbs(200) == 200
    assert sync._weight_to_lbs("200 lbs") == 200
    assert sync._weight_to_lbs("") is None


def test_born_score():
    sync = player_sync.PlayerSync()

    p = {"birthCity": "Seattle", "birthStateProvince": "WA", "birthCountry": "USA"}
    assert sync._born_score("seattle wa usa", p) == 35

    p = {"birthCity": "Seattle", "birthStateProvince": None, "birthCountry": ""}
    assert sync._born_score("seattle wa usa", p) == 8

    p = {"birthCity": "", "birthStateProvince": "", "birthCountry": ""}
    assert sync._born_score("seattle wa usa", p) == 0

    p = {"birthCity": "Seattle", "birthStateProvince": "WA", "birthCountry": "Canada"}
    assert sync._born_score("seattle wa usa", p) == 20

    p = {"birthCity": "Paris", "birthStateProvince": "", "birthCountry": "France"}
    assert sync._born_score("seattle wa usa", p) == 0


def test_body_score():
    sync = player_sync.PlayerSync()

    p = {"height": "6'0", "weight": "200"}
    assert sync._body_score(72, 200, p) == 50

    p = {"height": "6'8", "weight": "260"}
    assert sync._body_score(72, 200, p) < 0

    p = {"height": "6'2", "weight": "210"}
    assert sync._body_score(72, 200, p) == 36

    p = {"height": "6'4", "weight": "218"}
    assert sync._body_score(72, 200, p) == 16


def test_score_candidate_role_filter():
    sync = player_sync.PlayerSync()

    profile = {"expected_is_hitter": True, "two_way_mode": False, "born_norm": "", "card_height_in": None, "card_weight_lb": None}
    pitcher = {"fullName": "Pitcher", "primaryPosition": {"abbreviation": "P"}}
    assert sync._score_candidate("Pitcher", pitcher, profile) == -10_000

    profile = {"expected_is_hitter": False, "two_way_mode": False, "born_norm": "", "card_height_in": None, "card_weight_lb": None}
    hitter = {"fullName": "Hitter", "primaryPosition": {"abbreviation": "CF", "type": "position"}}
    assert sync._score_candidate("Hitter", hitter, profile) == -10_000

    profile = {"expected_is_hitter": True, "two_way_mode": False, "born_norm": "", "card_height_in": None, "card_weight_lb": None}
    batter = {"fullName": "Hitter", "primaryPosition": {"abbreviation": "CF", "type": "position"}}
    assert sync._score_candidate("Hitter", batter, profile) >= 25


def test_score_candidate_full_match_and_parts():
    sync = player_sync.PlayerSync()

    profile = {"expected_is_hitter": None, "two_way_mode": False, "born_norm": "", "card_height_in": None, "card_weight_lb": None}
    person = {"fullName": "John Doe", "firstName": "John", "lastName": "Doe", "active": True}
    assert sync._score_candidate("John Doe", person, profile) >= 145

    person = {"fullName": "John Xavier Doe", "firstName": "John", "lastName": "Doe", "active": False}
    assert sync._score_candidate("John Doe", person, profile) >= 140

    person = {"fullName": "", "firstName": "John", "lastName": "Doe"}
    assert sync._score_candidate("John Doe", person, profile) == -10_000


def test_score_all_candidates():
    sync = player_sync.PlayerSync()
    profile = {"expected_is_hitter": None, "two_way_mode": False, "born_norm": "", "card_height_in": None, "card_weight_lb": None}
    people = [
        {"fullName": "John Doe", "firstName": "John", "lastName": "Doe"},
        {"fullName": "Jane Doe", "firstName": "Jane", "lastName": "Doe"},
    ]
    scored = sync._score_all_candidates("John Doe", people, profile)
    assert len(scored) == 2


def test_pick_best_person_prefers_exact():
    sync = player_sync.PlayerSync()

    profile = {"two_way_mode": False}
    scored = [
        (100, {"fullName": "Someone Else"}),
        (90, {"fullName": "John Doe"}),
    ]

    person = sync._pick_best_person("John Doe", "", profile, scored)
    assert person["fullName"] == "John Doe"

    scored = [
        (100, {"fullName": "Someone Else"}),
        (90, {"fullName": "Johnny Doe"}),
    ]
    person = sync._pick_best_person("John Doe", "", profile, scored)
    assert person["fullName"] == "Someone Else"


def test_pick_best_person_two_way_mode():
    sync = player_sync.PlayerSync()

    profile = {"two_way_mode": True}
    scored = [(-10_000, {"fullName": "Bad"})]
    assert sync._pick_best_person("Bad", "", profile, scored) is None

    scored = [(5, {"fullName": "Good"})]
    assert sync._pick_best_person("Good", "", profile, scored)["fullName"] == "Good"


def test_pick_best_person_role_filtered_empty():
    sync = player_sync.PlayerSync()
    profile = {"two_way_mode": False}
    scored = [(-10_000, {"fullName": "Bad"})]
    assert sync._pick_best_person("Bad", "", profile, scored) is None


def test_load_card_profile_two_way_and_medians():
    sync = player_sync.PlayerSync()

    rows = [
        (True, "6'0", "200", "Seattle"),
        (False, "6'2", "210", "Seattle"),
    ]

    session = DummySession(execute_rows=rows)
    profile = sync._load_card_profile(session, "Name", "Born")

    assert profile["two_way_mode"] is True
    assert profile["expected_is_hitter"] is None
    assert profile["card_height_in"] == 73
    assert profile["card_weight_lb"] == 205

    rows = [(True, "6'0", "200", "Born")]
    session = DummySession(execute_rows=rows)
    profile = sync._load_card_profile(session, "Name", "Born")
    assert profile["expected_is_hitter"] is True

    rows = [(False, "6'0", "200", "Born")]
    session = DummySession(execute_rows=rows)
    profile = sync._load_card_profile(session, "Name", "Born")
    assert profile["expected_is_hitter"] is False


def test_upsert_position_and_unknown():
    sync = player_sync.PlayerSync()

    session = DummySession()
    pos_id = sync._upsert_position(session, {"code": "1", "name": "P", "abbreviation": "P"})
    assert pos_id == 1
    assert session.merged

    session = DummySession()
    assert sync._upsert_position(session, {"code": ""}) == 0
    assert session.merged == []

    session = DummySession()
    assert sync._upsert_position(session, {"code": "0"}) == 0

    session = DummySession(existing=None)
    sync._ensure_unknown_position(session)
    assert session.merged


def test_get_or_create_birth_location_id():
    sync = player_sync.PlayerSync()

    existing = type("Loc", (), {"id": 5})()
    session = DummySession(execute_rows=[existing])
    assert sync._get_or_create_birth_location_id(session, {"birthCity": "City", "birthStateProvince": None, "birthCountry": "Country"}) == 5

    session = DummySession(execute_rows=[])
    new_id = sync._get_or_create_birth_location_id(session, {"birthCity": "City", "birthStateProvince": "ST", "birthCountry": "Country"})
    assert new_id == 1
    assert session.added

    session = DummySession(execute_rows=[])
    assert sync._get_or_create_birth_location_id(session, {"birthCity": "", "birthCountry": ""}) is None


def test_upsert_player_validation():
    sync = player_sync.PlayerSync()

    session = DummySession()
    assert sync._upsert_player(session, {"birthDate": "2020-01-01"}) is False
    assert sync._upsert_player(session, {"id": 1, "birthDate": "bad"}) is False

    person = {
        "id": 1,
        "birthDate": "2020-01-01",
        "primaryPosition": {"code": "1", "name": "P", "abbreviation": "P"},
        "fullName": "A B",
        "firstName": "A",
        "lastName": "B",
    }

    assert sync._upsert_player(session, person) is True
    assert session.merged


def test_parse_date_accepts_date():
    sync = player_sync.PlayerSync()
    d = datetime.date(2026, 1, 1)
    assert sync._parse_date(d) == d


def test_search_people_worker(monkeypatch):
    sync = player_sync.PlayerSync()

    monkeypatch.setattr(player_sync.random, "uniform", lambda *args: 0)
    monkeypatch.setattr(player_sync.time, "sleep", lambda s: None)

    monkeypatch.setattr(player_sync, "APIClient", lambda: DummyClient({"people": [{"id": 1}]}))

    people = sync._search_people_worker("Name")
    assert people == [{"id": 1}]


def test_run_basic_flow(monkeypatch):
    sync = player_sync.PlayerSync(flush_every=1)

    rows = [
        ("", "Born"),
        ("Cached", "Born"),
        ("John Doe", "Seattle"),
        ("No Results", "Nowhere"),
    ]

    session = DummySession()

    def execute(stmt):
        if getattr(stmt, "is_update", False):
            return DummyUpdateResult(rowcount=2)
        return DummySelectResult(rows)

    session.execute = execute

    search_calls = []
    upsert_calls = []

    def fake_search(name):
        search_calls.append(name)
        if name == "No Results":
            return []
        return [{"id": 1, "fullName": name}]

    monkeypatch.setattr(sync, "_search_people_worker", fake_search)
    monkeypatch.setattr(sync, "_load_card_profile", lambda *args, **kwargs: {"two_way_mode": False, "expected_is_hitter": None})
    monkeypatch.setattr(sync, "_score_all_candidates", lambda name, people, profile: [(10, people[0])])
    monkeypatch.setattr(sync, "_pick_best_person", lambda name, born, profile, scored: scored[0][1])
    monkeypatch.setattr(sync, "_upsert_player", lambda session, person: upsert_calls.append(person) or True)

    class DummyFuture:
        def __init__(self, value=None, exc=None):
            self._value = value
            self._exc = exc

        def result(self):
            if self._exc:
                raise self._exc
            return self._value

    class DummyExecutor:
        def __init__(self, max_workers=1):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, *args, **kwargs):
            try:
                return DummyFuture(value=func(*args, **kwargs))
            except Exception as e:
                return DummyFuture(exc=e)

    monkeypatch.setattr(player_sync, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(player_sync, "wait", lambda futures, return_when=None: (set(futures), set()))

    cache_key = (sync._norm_name("Cached"), sync._norm("Born"))
    sync._group_cache[cache_key] = {"id": 9, "_profile": {"two_way_mode": False, "expected_is_hitter": None}}

    sync.run(session)

    assert search_calls == ["John Doe", "No Results"]
    assert len(upsert_calls) == 2


def test_run_branch_paths(monkeypatch):
    sync = player_sync.PlayerSync(flush_every=1)

    monkeypatch.setattr(player_sync, "MAX_WORKERS", 1)

    rows = [
        ("CachedNone", "Born"),
        ("CachedNoId", "Born"),
        ("Boom", "Born"),
        ("EmptyScore", "Born"),
        ("NoPick", "Born"),
        ("NoId", "Born"),
    ]

    session = DummySession()

    def execute(stmt):
        if getattr(stmt, "is_update", False):
            return DummyUpdateResult(rowcount=1)
        return DummySelectResult(rows)

    session.execute = execute

    cache_key_none = (sync._norm_name("CachedNone"), sync._norm("Born"))
    cache_key_noid = (sync._norm_name("CachedNoId"), sync._norm("Born"))
    sync._group_cache[cache_key_none] = None
    sync._group_cache[cache_key_noid] = {"id": None, "_profile": {"two_way_mode": False, "expected_is_hitter": None}}

    monkeypatch.setattr(sync, "_log_top3_misses", lambda *args, **kwargs: None, raising=False)
    monkeypatch.setattr(sync, "_load_card_profile", lambda *args, **kwargs: {"two_way_mode": False, "expected_is_hitter": None})

    def fake_search(name):
        if name == "Boom":
            raise RuntimeError("boom")
        return [{"id": 1, "fullName": name}]

    monkeypatch.setattr(sync, "_search_people_worker", fake_search)
    monkeypatch.setattr(sync, "_score_all_candidates", lambda name, people, profile: [] if name == "EmptyScore" else [(10, people[0])])
    monkeypatch.setattr(sync, "_pick_best_person", lambda name, born, profile, scored: None if name == "NoPick" else ({"id": None} if name == "NoId" else scored[0][1]))
    monkeypatch.setattr(sync, "_upsert_player", lambda session, person: True)

    class DummyFuture:
        def __init__(self, value=None, exc=None):
            self._value = value
            self._exc = exc

        def result(self):
            if self._exc:
                raise self._exc
            return self._value

    class DummyExecutor:
        def __init__(self, max_workers=1):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, func, *args, **kwargs):
            try:
                return DummyFuture(value=func(*args, **kwargs))
            except Exception as e:
                return DummyFuture(exc=e)

    monkeypatch.setattr(player_sync, "ThreadPoolExecutor", DummyExecutor)
    monkeypatch.setattr(player_sync, "wait", lambda futures, return_when=None: (set(futures), set()))

    sync.run(session)
