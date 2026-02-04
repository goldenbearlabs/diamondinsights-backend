import json
from datetime import datetime, timezone

import pytest

from shared.queue.queue import Payload, Queue


class FakeRedis:
    def __init__(self):
        self._lists = {}
        self._hashes = {}
        self._zsets = {}

    def lpush(self, key, value):
        self._lists.setdefault(key, [])
        self._lists[key].insert(0, value)
        return len(self._lists[key])

    def brpoplpush(self, source, dest, timeout=0):
        src_list = self._lists.get(source, [])
        if not src_list:
            return None
        value = src_list.pop()
        self._lists.setdefault(dest, []).insert(0, value)
        return value

    def lrem(self, key, count, value):
        lst = self._lists.get(key, [])
        removed = 0

        if count == 0:
            new_list = [v for v in lst if v != value]
            removed = len(lst) - len(new_list)
        elif count > 0:
            new_list = []
            for v in lst:
                if v == value and removed < count:
                    removed += 1
                else:
                    new_list.append(v)
        else:
            new_list = lst[:]
            idx = len(new_list) - 1
            while idx >= 0 and removed < abs(count):
                if new_list[idx] == value:
                    new_list.pop(idx)
                    removed += 1
                idx -= 1

        self._lists[key] = new_list
        return removed

    def hset(self, key, field, value):
        self._hashes.setdefault(key, {})[field] = value
        return 1

    def hdel(self, key, field):
        if key not in self._hashes or field not in self._hashes[key]:
            return 0
        del self._hashes[key][field]
        return 1

    def hget(self, key, field):
        return self._hashes.get(key, {}).get(field)

    def zadd(self, key, mapping):
        zset = self._zsets.setdefault(key, {})
        for member, score in mapping.items():
            zset[member] = score
        return len(mapping)

    def zrem(self, key, member):
        zset = self._zsets.get(key, {})
        if member in zset:
            del zset[member]
            return 1
        return 0

    def zrangebyscore(self, key, min_score, max_score, start=0, num=None):
        zset = self._zsets.get(key, {})
        items = [
            (member, score)
            for member, score in zset.items()
            if float(min_score) <= score <= float(max_score)
        ]
        items.sort(key=lambda item: (item[1], item[0]))
        members = [member for member, _ in items]
        if num is None:
            return members[start:]
        return members[start:start + num]

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.ops = []

    def hset(self, key, field, value):
        self.ops.append(lambda: self.redis_client.hset(key, field, value))
        return self

    def zadd(self, key, mapping):
        self.ops.append(lambda: self.redis_client.zadd(key, mapping))
        return self

    def lrem(self, key, count, value):
        self.ops.append(lambda: self.redis_client.lrem(key, count, value))
        return self

    def hdel(self, key, field):
        self.ops.append(lambda: self.redis_client.hdel(key, field))
        return self

    def zrem(self, key, member):
        self.ops.append(lambda: self.redis_client.zrem(key, member))
        return self

    def execute(self):
        return [op() for op in self.ops]


class FakeRedisConnector:
    def __init__(self, client):
        self._client = client

    def client(self):
        return self._client


def _make_queue():
    client = FakeRedis()
    return Queue(FakeRedisConnector(client)), client


def test_queue_respects_custom_keys():
    client = FakeRedis()
    queue = Queue(
        FakeRedisConnector(client),
        pending_key="pending:custom",
        processing_key="processing:custom",
        dead_key="dead:custom",
        lease_zet="lease:custom",
        raw_hash="raw:custom",
    )

    payload = queue.enqueue("card_sync")
    raw, reserved = queue.reserve()
    queue.ack(raw, reserved)

    assert client._lists["pending:custom"] == []
    assert client._lists["processing:custom"] == []
    assert client._hashes.get("raw:custom", {}) == {}
    assert client._zsets.get("lease:custom", {}) == {}


def test_payload_roundtrip_preserves_fields():
    payload = Payload(job_type="card_sync", args={"x": 1})
    payload.attempts = 2
    payload.enqueued_at = datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    encoded = payload.to_json()
    decoded = Payload.from_json(encoded)

    assert decoded.job_id == payload.job_id
    assert decoded.job_type == payload.job_type
    assert decoded.args == payload.args
    assert decoded.attempts == payload.attempts
    assert decoded.enqueued_at == payload.enqueued_at


def test_payload_from_json_defaults_args_and_attempts():
    now = datetime(2024, 1, 2, tzinfo=timezone.utc)
    data = {
        "job_id": "abc",
        "job_type": "job",
        "args": None,
        "enqueued_at": now.isoformat(),
        "attempts": None,
    }

    payload = Payload.from_json(json.dumps(data))

    assert payload.args == {}
    assert payload.attempts == 0
    assert payload.enqueued_at == now


def test_enqueue_pushes_payload_to_pending():
    queue, client = _make_queue()

    payload = queue.enqueue("card_sync", {"reload_all_years": True})

    assert payload.job_type == "card_sync"
    assert payload.args == {"reload_all_years": True}
    assert client._lists[queue.pending_key] == [payload.to_json()]


def test_enqueue_defaults_args_to_empty_dict():
    queue, client = _make_queue()

    payload = queue.enqueue("market_sync")

    assert payload.args == {}
    assert client._lists[queue.pending_key] == [payload.to_json()]


def test_reserve_returns_none_when_pending_empty():
    queue, _ = _make_queue()

    assert queue.reserve() is None


def test_reserve_moves_payload_to_processing_and_sets_lease(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 1234.5)

    payload = queue.enqueue("card_sync", {"reload_all_years": True})
    raw, reserved = queue.reserve()

    assert raw == payload.to_json()
    assert reserved.job_id == payload.job_id
    assert client._lists[queue.pending_key] == []
    assert client._lists[queue.processing_key] == [raw]
    assert client._hashes[queue.raw_hash][payload.job_id] == raw
    assert client._zsets[queue.lease_zset][payload.job_id] == 1234.5


def test_reserve_defaults_missing_args_and_attempts():
    queue, client = _make_queue()

    raw_payload = json.dumps(
        {
            "job_id": "job-1",
            "job_type": "card_sync",
            "enqueued_at": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
            "attempts": "2",
        }
    )
    client.lpush(queue.pending_key, raw_payload)

    _, reserved = queue.reserve()

    assert reserved.args == {}
    assert reserved.attempts == 2


def test_ack_removes_processing_and_lease_entries():
    queue, client = _make_queue()
    raw, payload = queue.reserve() or (None, None)
    assert raw is None

    payload = queue.enqueue("card_sync")
    raw, reserved = queue.reserve()
    queue.ack(raw, reserved)

    assert client._lists[queue.processing_key] == []
    assert client._hashes.get(queue.raw_hash, {}) == {}
    assert client._zsets.get(queue.lease_zset, {}) == {}


def test_heartbeat_updates_lease_timestamp(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 77.7)

    job_id = "job-123"
    queue.heartbeat(job_id)

    assert client._zsets[queue.lease_zset][job_id] == 77.7


def test_fail_requeues_until_max_attempts(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 1.0)

    payload = queue.enqueue("card_sync")
    raw, reserved = queue.reserve()

    queue.fail(raw, reserved, max_attempts=3)

    assert client._lists[queue.processing_key] == []
    assert client._lists[queue.pending_key]
    requeued = Payload.from_json(client._lists[queue.pending_key][0])
    assert requeued.attempts == 1
    assert requeued.job_id == payload.job_id


def test_fail_moves_to_dead_when_max_attempts_reached():
    queue, client = _make_queue()

    raw, reserved = queue.reserve() or (None, None)
    assert raw is None

    payload = queue.enqueue("card_sync")
    raw, reserved = queue.reserve()

    queue.fail(raw, reserved, max_attempts=1)

    assert client._lists[queue.pending_key] == []
    assert len(client._lists[queue.dead_key]) == 1
    dead = Payload.from_json(client._lists[queue.dead_key][0])
    assert dead.attempts == 1
    assert dead.job_id == payload.job_id


def test_reap_stale_returns_zero_when_no_leases(monkeypatch):
    queue, _ = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    assert queue.reap_stale(lease_seconds=10) == 0


def test_reap_stale_removes_lease_when_raw_missing(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    client.zadd(queue.lease_zset, {"job-1": 1})

    assert queue.reap_stale(lease_seconds=10) == 0
    assert client._zsets[queue.lease_zset] == {}


def test_reap_stale_removes_hash_and_lease_when_processing_missing(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    payload = Payload(job_type="card_sync")
    raw = payload.to_json()
    client.hset(queue.raw_hash, payload.job_id, raw)
    client.zadd(queue.lease_zset, {payload.job_id: 1})

    assert queue.reap_stale(lease_seconds=10) == 0
    assert client._hashes.get(queue.raw_hash, {}) == {}
    assert client._zsets.get(queue.lease_zset, {}) == {}
    assert client._lists.get(queue.pending_key, []) == []


def test_reap_stale_requeues_jobs_and_increments_attempts(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    payload = Payload(job_type="card_sync")
    raw = payload.to_json()

    client.lpush(queue.processing_key, raw)
    client.hset(queue.raw_hash, payload.job_id, raw)
    client.zadd(queue.lease_zset, {payload.job_id: 1})

    requeued = queue.reap_stale(lease_seconds=10)

    assert requeued == 1
    assert client._lists[queue.processing_key] == []
    assert client._hashes.get(queue.raw_hash, {}) == {}
    assert client._zsets.get(queue.lease_zset, {}) == {}

    pending_payload = Payload.from_json(client._lists[queue.pending_key][0])
    assert pending_payload.attempts == 1
    assert pending_payload.job_id == payload.job_id


def test_reap_stale_respects_max_per_run(monkeypatch):
    queue, client = _make_queue()
    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    payload1 = Payload(job_type="card_sync")
    payload2 = Payload(job_type="market_sync")

    raw1 = payload1.to_json()
    raw2 = payload2.to_json()

    client.lpush(queue.processing_key, raw1)
    client.lpush(queue.processing_key, raw2)
    client.hset(queue.raw_hash, payload1.job_id, raw1)
    client.hset(queue.raw_hash, payload2.job_id, raw2)
    client.zadd(queue.lease_zset, {payload1.job_id: 1, payload2.job_id: 2})

    requeued = queue.reap_stale(lease_seconds=10, max_per_run=1)

    assert requeued == 1
    assert len(client._lists[queue.pending_key]) == 1
    remaining = set(client._zsets[queue.lease_zset].keys())
    assert len(remaining) == 1
