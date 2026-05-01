import json
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Optional
from dataclasses import dataclass, field
from shared.queue.redis_connector import RedisConnector

Priority = str
PRIORITY_HIGH: Priority = "high"
PRIORITY_NORMAL: Priority = "normal"
PRIORITY_LOW: Priority = "low"
VALID_PRIORITIES = {PRIORITY_HIGH, PRIORITY_NORMAL, PRIORITY_LOW}


@dataclass
class Payload:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_type: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    attempts: int = 0
    priority: Priority = PRIORITY_NORMAL

    def to_json(self) -> str:
        return json.dumps({
            "job_id": self.job_id,
            "job_type": self.job_type,
            "args": self.args,
            "enqueued_at": self.enqueued_at.isoformat(),
            "attempts": self.attempts,
            "priority": self.priority,
        })
    
    @staticmethod
    def from_json(s: str) -> "Payload":
        d = json.loads(s)
        p = Payload(
            job_id=d["job_id"],
            job_type=d["job_type"],
            args=d.get("args", {}) or {},
            attempts=int(d.get("attempts", 0) or 0),
            priority=_normalize_priority(d.get("priority")),
        )
        p.enqueued_at = datetime.fromisoformat(d["enqueued_at"])
        return p


def _normalize_priority(priority: Any) -> Priority:
    value = str(priority or PRIORITY_NORMAL).strip().lower()
    return value if value in VALID_PRIORITIES else PRIORITY_NORMAL


class Queue:
    def __init__(self, redis_connector: RedisConnector, 
                 pending_key: str = "jobs:pending", 
                 processing_key: str = "jobs:processing",
                 dead_key: str = "jobs:dead",
                 lease_zet: str = "jobs:processing:lease",
                 raw_hash: str = "jobs:processing:raw"):
        
        self.redis_client = redis_connector.client()
        self.pending_key = pending_key
        self.processing_key = processing_key
        self.dead_key = dead_key
        self.lease_zset = lease_zet
        self.raw_hash = raw_hash
        self.high_pending_key = f"{pending_key}:high"
        self.low_pending_key = f"{pending_key}:low"

    def _pending_key_for_priority(self, priority: Any) -> str:
        normalized = _normalize_priority(priority)
        if normalized == PRIORITY_HIGH:
            return self.high_pending_key
        if normalized == PRIORITY_LOW:
            return self.low_pending_key
        return self.pending_key

    def _pending_keys_in_reserve_order(self) -> tuple[str, ...]:
        return (self.high_pending_key, self.pending_key, self.low_pending_key)

    def enqueue(
        self,
        job_type: str,
        args: Optional[dict[str, Any]] = None,
        *,
        priority: Priority = PRIORITY_NORMAL,
    ) -> Payload:
        payload = Payload(job_type=job_type, args=args or {}, priority=_normalize_priority(priority))
        self.redis_client.lpush(self._pending_key_for_priority(payload.priority), payload.to_json())
        return payload

    def _reserve_once(self) -> Optional[tuple[str, Payload]]:
        for pending_key in self._pending_keys_in_reserve_order():
            raw_payload = self.redis_client.rpoplpush(pending_key, self.processing_key)
            if raw_payload is None:
                continue

            payload = Payload.from_json(raw_payload)
            now = time.time()

            pipe = self.redis_client.pipeline()
            pipe.hset(self.raw_hash, payload.job_id, raw_payload)
            pipe.zadd(self.lease_zset, {payload.job_id: now})
            pipe.execute()

            return raw_payload, payload

        return None

    def reserve(self, timeout: int = 0) -> Optional[tuple[str, Payload]]:
        deadline = None if timeout <= 0 else time.time() + timeout
        while True:
            item = self._reserve_once()
            if item is not None:
                return item
            if deadline is None or time.time() >= deadline:
                return None
            time.sleep(min(0.25, max(0.0, deadline - time.time())))

    def heartbeat(self, job_id: str) -> None:
        self.redis_client.zadd(self.lease_zset, {job_id: time.time()})

    def ack(self, raw_payload: str, payload: Payload) -> None:
        pipe = self.redis_client.pipeline()
        pipe.lrem(self.processing_key, 1, raw_payload)
        pipe.hdel(self.raw_hash, payload.job_id)
        pipe.zrem(self.lease_zset, payload.job_id)
        pipe.execute()

    def fail(self, raw_payload: str, payload: Payload, max_attempts: int = 5) -> None:
        payload.attempts += 1
        self.ack(raw_payload, payload)

        if payload.attempts >= max_attempts:
            self.redis_client.lpush(self.dead_key, payload.to_json())
        else:
            self.redis_client.lpush(self._pending_key_for_priority(payload.priority), payload.to_json())

    def reap_stale(self, lease_seconds: int = 1000, max_per_run: int = 50) -> int:
        cutoff = time.time() - lease_seconds
        job_ids = self.redis_client.zrangebyscore(self.lease_zset, 0, cutoff, start=0, num=max_per_run)
        if not job_ids:
            return 0

        requeued = 0
        for job_id in job_ids:
            raw = self.redis_client.hget(self.raw_hash, job_id)
            if not raw:
                self.redis_client.zrem(self.lease_zset, job_id)
                continue

            removed = self.redis_client.lrem(self.processing_key, 1, raw)
            self.redis_client.hdel(self.raw_hash, job_id)
            self.redis_client.zrem(self.lease_zset, job_id)

            if removed:
                payload = Payload.from_json(raw)
                payload.attempts += 1
                self.redis_client.lpush(self._pending_key_for_priority(payload.priority), payload.to_json())
                requeued += 1

        return requeued
