import json
from datetime import datetime, timezone

import apps.jobs.router as router


class FakeRedis:
    def __init__(self):
        self.calls = []

    def set(self, key, value, ex=None):
        self.calls.append({"key": key, "value": value, "ex": ex})


class FailingRedis(FakeRedis):
    def set(self, key, value, ex=None):
        raise RuntimeError("redis boom")


class FakeQueue:
    def __init__(self, redis_connector):
        self.redis_connector = redis_connector
        self.heartbeat_calls = []

    def heartbeat(self, job_id):
        self.heartbeat_calls.append(job_id)


class StubRedisConnector:
    def __init__(self, client):
        self._client = client

    def client(self):
        return self._client


class FakeThread:
    def __init__(self, target=None, daemon=False):
        self.target = target
        self.daemon = daemon
        self.started = False
        self.joined = False

    def start(self):
        self.started = True
        if self.target:
            self.target()

    def join(self, timeout=None):
        self.joined = True


class FakeEvent:
    def __init__(self):
        self._calls = 0
        self.set_called = False

    def is_set(self):
        return self._calls > 0

    def wait(self, timeout=None):
        self._calls += 1
        return True

    def set(self):
        self.set_called = True


def _make_router(monkeypatch, redis_client=None):
    redis_client = redis_client or FakeRedis()

    monkeypatch.setattr(router, "RedisConnector", lambda: StubRedisConnector(redis_client))
    monkeypatch.setattr(router, "Queue", FakeQueue)
    monkeypatch.setattr(router.socket, "gethostname", lambda: "host")
    monkeypatch.delenv("RUNNER_NAME", raising=False)
    monkeypatch.delenv("RUNNER_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("RUNNER_HEARTBEAT_TTL_SECONDS", raising=False)

    return router.Router()


def test_utc_iso_is_parseable():
    value = router._utc_iso()
    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is not None
    assert parsed.tzinfo.utcoffset(parsed) == timezone.utc.utcoffset(parsed)


def test_router_init_defaults(monkeypatch):
    r = _make_router(monkeypatch)

    assert r.runner_name == "host"
    assert r.runner_key == "runner:status:host"
    assert r.hb_interval_s == 15
    assert r.hb_ttl_s == 60
    assert r._status["state"] == "idle"
    assert r._status["current_job_id"] is None
    assert r._status["job_type"] is None
    assert r._status["started_at"] == r.started_at


def test_router_init_env_overrides(monkeypatch):
    redis_client = FakeRedis()
    monkeypatch.setattr(router, "RedisConnector", lambda: StubRedisConnector(redis_client))
    monkeypatch.setattr(router, "Queue", FakeQueue)
    monkeypatch.setenv("RUNNER_NAME", "runner-1")
    monkeypatch.setenv("RUNNER_HEARTBEAT_INTERVAL_SECONDS", "7")
    monkeypatch.setenv("RUNNER_HEARTBEAT_TTL_SECONDS", "20")

    r = router.Router()

    assert r.runner_name == "runner-1"
    assert r.runner_key == "runner:status:runner-1"
    assert r.hb_interval_s == 7
    assert r.hb_ttl_s == 20


def test_set_status_updates_fields(monkeypatch):
    redis_client = FakeRedis()
    monkeypatch.setattr(router, "RedisConnector", lambda: StubRedisConnector(redis_client))
    monkeypatch.setattr(router, "Queue", FakeQueue)
    monkeypatch.setattr(router, "_utc_iso", lambda: "now")

    r = router.Router()
    r._set_status("running", job_id="job-1", job_type="card_sync")

    assert r._status["state"] == "running"
    assert r._status["current_job_id"] == "job-1"
    assert r._status["job_type"] == "card_sync"
    assert r._status["updated_at"] == "now"


def test_start_runner_heartbeat_sets_status(monkeypatch):
    redis_client = FakeRedis()
    r = _make_router(monkeypatch, redis_client=redis_client)
    monkeypatch.setattr(router.threading, "Thread", FakeThread)

    stop = FakeEvent()
    thread = r._start_runner_heartbeat(stop)

    assert thread.started is True
    assert thread.daemon is True
    assert len(redis_client.calls) == 1

    payload = json.loads(redis_client.calls[0]["value"])
    assert redis_client.calls[0]["key"] == r.runner_key
    assert redis_client.calls[0]["ex"] == r.hb_ttl_s
    assert payload["name"] == r.runner_name


def test_start_job_heartbeat_calls_queue(monkeypatch):
    redis_client = FakeRedis()
    r = _make_router(monkeypatch, redis_client=redis_client)
    monkeypatch.setattr(router.threading, "Thread", FakeThread)

    stop = FakeEvent()
    thread = r._start_job_heartbeat("job-123", stop, interval_s=1)

    assert thread.started is True
    assert thread.daemon is True
    assert r.queue.heartbeat_calls == ["job-123"]


def test_start_runner_heartbeat_handles_redis_errors(monkeypatch):
    r = _make_router(monkeypatch, redis_client=FailingRedis())
    monkeypatch.setattr(router.threading, "Thread", FakeThread)

    stop = FakeEvent()
    thread = r._start_runner_heartbeat(stop)

    assert thread.started is True
    assert thread.daemon is True


def test_start_job_heartbeat_handles_queue_errors(monkeypatch):
    redis_client = FakeRedis()
    r = _make_router(monkeypatch, redis_client=redis_client)
    monkeypatch.setattr(router.threading, "Thread", FakeThread)

    def failing_heartbeat(job_id):
        raise RuntimeError("hb boom")

    r.queue.heartbeat = failing_heartbeat

    stop = FakeEvent()
    thread = r._start_job_heartbeat("job-err", stop, interval_s=1)

    assert thread.started is True
    assert thread.daemon is True
