import sys
import types

import pytest

import apps.jobs.router as router
from shared.queue.queue import Payload


class StopLoop(Exception):
    pass


class FakeThread:
    def __init__(self):
        self.joined = False

    def join(self, timeout=None):
        self.joined = True


class FakeQueue:
    def __init__(self, redis_connector):
        self.redis_connector = redis_connector
        self.sequence = []
        self.ack_calls = []
        self.fail_calls = []
        self.heartbeat_calls = []
        self.fail_raises = False
        self.timeouts = []

    def reserve(self, timeout=0):
        self.timeouts.append(timeout)
        if not self.sequence:
            raise StopLoop()
        return self.sequence.pop(0)

    def ack(self, raw, payload):
        self.ack_calls.append((raw, payload))

    def fail(self, raw, payload, max_attempts=5):
        self.fail_calls.append((raw, payload, max_attempts))
        if self.fail_raises:
            raise RuntimeError("fail boom")

    def heartbeat(self, job_id):
        self.heartbeat_calls.append(job_id)


class FakeRedis:
    def set(self, key, value, ex=None):
        return None


class StubRedisConnector:
    def __init__(self, client):
        self._client = client

    def client(self):
        return self._client


class FakeSession:
    def __init__(self, rollback_raises=False):
        self.rollback_called = False
        self.rollback_raises = rollback_raises

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def rollback(self):
        self.rollback_called = True
        if self.rollback_raises:
            raise RuntimeError("rollback boom")


def _install_stub_job(monkeypatch, module_path, class_name, record, raise_in_run=False):
    class StubJob:
        def __init__(self, **kwargs):
            record["init_kwargs"] = kwargs

        def run(self, session):
            record["run_session"] = session
            if raise_in_run:
                raise RuntimeError("job boom")

    module = types.SimpleNamespace(**{class_name: StubJob})
    monkeypatch.setitem(sys.modules, module_path, module)


def _make_router(monkeypatch, session_factory):
    monkeypatch.setattr(router, "RedisConnector", lambda: StubRedisConnector(FakeRedis()))
    monkeypatch.setattr(router, "Queue", FakeQueue)
    monkeypatch.setattr(router, "SessionLocal", session_factory)

    r = router.Router()
    r.queue.sequence = []

    monkeypatch.setattr(router.Router, "_start_runner_heartbeat", lambda self, stop: FakeThread())
    monkeypatch.setattr(router.Router, "_start_job_heartbeat", lambda self, job_id, stop, interval_s=30: FakeThread())

    return r


def test_run_idle_sets_status_to_idle(monkeypatch):
    r = _make_router(monkeypatch, lambda: FakeSession())
    r.queue.sequence = [None]
    r._status["state"] = "running"

    with pytest.raises(StopLoop):
        r.run()

    assert r._status["state"] == "idle"
    assert r.queue.timeouts[0] == 10


@pytest.mark.parametrize(
    "job_type,args,module_path,class_name,expected_kwargs",
    [
        ("card_sync", {"reload_all_years": True}, "apps.jobs.card_sync", "CardSync", {"reload_all_years": True}),
        ("card_sync", {}, "apps.jobs.card_sync", "CardSync", {"reload_all_years": False}),
        ("roster_update_sync", {"reload_all_years": True}, "apps.jobs.roster_update_sync", "RosterUpdateSync", {"reload_all_years": 1}),
        ("roster_update_sync", {}, "apps.jobs.roster_update_sync", "RosterUpdateSync", {"reload_all_years": 0}),
        ("game_boxscore_sync", {"reload_all_games": True, "season": "2024"}, "apps.jobs.game_boxscore_sync", "GameBoxscoreSync", {"season_year": 2024, "reload_all_games": 1}),
        ("game_boxscore_sync", {}, "apps.jobs.game_boxscore_sync", "GameBoxscoreSync", {"season_year": 2025, "reload_all_games": 0}),
        ("market_sync", {}, "apps.jobs.market_sync", "MarketSync", {}),
        ("market_candle_sync", {}, "apps.jobs.market_candle_sync", "MarketCandleSync", {}),
        ("player_sync", {"reload_all_players": "yes"}, "apps.jobs.player_sync", "PlayerSync", {"reload_all_players": True}),
        ("player_sync", {}, "apps.jobs.player_sync", "PlayerSync", {"reload_all_players": False}),
        ("prediction_sync", {}, "apps.jobs.prediction_sync", "PredictionSync", {}),
        ("roster-update-aggregator", {}, "apps.jobs.roster_update_aggregator", "RosterUpdateAggregator", {}),
        ("your_ovr_sync", {}, "apps.jobs.your_ovr_sync", "YourOvrSync", {}),
        (
            "card_position_overall_sync",
            {},
            "apps.jobs.card_position_overall_sync",
            "CardPositionOverallSync",
            {},
        ),
    ],
)
def test_run_job_routes_and_acks(monkeypatch, job_type, args, module_path, class_name, expected_kwargs):
    record = {}
    _install_stub_job(monkeypatch, module_path, class_name, record)

    session = FakeSession()
    r = _make_router(monkeypatch, lambda: session)

    payload = Payload(job_type=job_type, args=args)
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert record["init_kwargs"] == expected_kwargs
    assert record["run_session"] is session
    assert r.queue.ack_calls == [(raw, payload)]
    assert r.queue.fail_calls == []
    assert r._status["state"] == "idle"


def test_run_unknown_job_type_fails(monkeypatch):
    session = FakeSession()
    r = _make_router(monkeypatch, lambda: session)

    payload = Payload(job_type="unknown_job", args={})
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert r.queue.ack_calls == []
    assert r.queue.fail_calls == [(raw, payload, 5)]
    assert r._status["state"] == "idle"


def test_run_invalid_game_boxscore_season_fails(monkeypatch):
    record = {}
    _install_stub_job(monkeypatch, "apps.jobs.game_boxscore_sync", "GameBoxscoreSync", record)
    session = FakeSession()
    r = _make_router(monkeypatch, lambda: session)

    payload = Payload(job_type="game_boxscore_sync", args={"season": "not-a-number"})
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert session.rollback_called is True
    assert r.queue.ack_calls == []
    assert r.queue.fail_calls == [(raw, payload, 5)]


def test_run_payload_args_none_fails(monkeypatch):
    record = {}
    _install_stub_job(monkeypatch, "apps.jobs.card_sync", "CardSync", record)
    session = FakeSession()
    r = _make_router(monkeypatch, lambda: session)

    payload = Payload(job_type="card_sync", args=None)
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert session.rollback_called is True
    assert r.queue.ack_calls == []
    assert r.queue.fail_calls == [(raw, payload, 5)]


def test_run_job_exception_rolls_back_and_fails(monkeypatch):
    record = {}
    _install_stub_job(monkeypatch, "apps.jobs.card_sync", "CardSync", record, raise_in_run=True)

    session = FakeSession(rollback_raises=True)
    r = _make_router(monkeypatch, lambda: session)

    payload = Payload(job_type="card_sync", args={})
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert session.rollback_called is True
    assert r.queue.ack_calls == []
    assert r.queue.fail_calls == [(raw, payload, 5)]
    assert r._status["state"] == "idle"


def test_run_fail_raises_is_swallowed(monkeypatch):
    record = {}
    _install_stub_job(monkeypatch, "apps.jobs.card_sync", "CardSync", record, raise_in_run=True)

    session = FakeSession()
    r = _make_router(monkeypatch, lambda: session)
    r.queue.fail_raises = True

    payload = Payload(job_type="card_sync", args={})
    raw = payload.to_json()
    r.queue.sequence = [(raw, payload)]

    with pytest.raises(StopLoop):
        r.run()

    assert r.queue.fail_calls == [(raw, payload, 5)]
    assert r._status["state"] == "idle"
