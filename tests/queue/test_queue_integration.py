import pytest

from shared.queue.queue import Payload, Queue


fakeredis = pytest.importorskip("fakeredis")


class FakeRedisConnector:
    def __init__(self, client):
        self._client = client

    def client(self):
        return self._client


def test_integration_enqueue_reserve_ack(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    queue = Queue(FakeRedisConnector(client))

    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 999.0)

    payload = queue.enqueue("card_sync", {"reload_all_years": True})
    raw, reserved = queue.reserve()

    assert reserved.job_id == payload.job_id
    assert client.llen(queue.pending_key) == 0
    assert client.llen(queue.processing_key) == 1
    assert client.hget(queue.raw_hash, payload.job_id) == raw
    assert client.zscore(queue.lease_zset, payload.job_id) == 999.0

    queue.ack(raw, reserved)

    assert client.llen(queue.processing_key) == 0
    assert client.hlen(queue.raw_hash) == 0
    assert client.zcard(queue.lease_zset) == 0


def test_integration_fail_to_dead():
    client = fakeredis.FakeRedis(decode_responses=True)
    queue = Queue(FakeRedisConnector(client))

    queue.enqueue("card_sync")
    raw, reserved = queue.reserve()
    queue.fail(raw, reserved, max_attempts=1)

    assert client.llen(queue.pending_key) == 0
    assert client.llen(queue.dead_key) == 1
    dead = Payload.from_json(client.lindex(queue.dead_key, 0))
    assert dead.attempts == 1


def test_integration_reap_stale_requeues(monkeypatch):
    client = fakeredis.FakeRedis(decode_responses=True)
    queue = Queue(FakeRedisConnector(client))

    monkeypatch.setattr("shared.queue.queue.time.time", lambda: 100)

    payload = queue.enqueue("card_sync")
    raw, reserved = queue.reserve()

    client.zadd(queue.lease_zset, {reserved.job_id: 1})

    requeued = queue.reap_stale(lease_seconds=10)

    assert requeued == 1
    assert client.llen(queue.processing_key) == 0
    assert client.llen(queue.pending_key) == 1
    requeued_payload = Payload.from_json(client.lindex(queue.pending_key, 0))
    assert requeued_payload.job_id == payload.job_id
    assert requeued_payload.attempts == 1
