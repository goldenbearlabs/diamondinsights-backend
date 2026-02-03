

import json
import uuid
import time
from datetime import datetime, timezone
from typing import Any, Optional
from dataclasses import dataclass, field
from shared.queue.redis_connector import RedisConnector


@dataclass
class Payload:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_type: str = ""
    args: dict[str, Any] = field(default_factory=dict)
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    attempts: int = 0

    def to_json(self) -> str:
        return json.dumps({
            "job_id": self.job_id,
            "job_type": self.job_type,
            "args": self.args,
            "enqueued_at": self.enqueued_at.isoformat(),
            "attempts": self.attempts
        })
    
    @staticmethod
    def from_json(s: str) -> "Payload":
        d = json.loads(s)
        p = Payload(
            job_id=d["job_id"],
            job_type=d["job_type"],
            args=d.get("args", {}) or {},
            attempts=int(d.get("attempts", 0) or 0)
        )
        p.enqueued_at = datetime.fromisoformat(d["enqueued_at"])
        return p

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

    def enqueue(self, job_type: str, args: Optional[dict[str, Any]] = None) -> Payload:
        payload = Payload(job_type=job_type, args=args or {})
        self.redis_client.lpush(self.pending_key, payload.to_json())
        return payload
    
    def reserve(self, timeout: int = 0) -> Optional[tuple[str, Payload]]:
        raw_payload = self.redis_client.brpoplpush(self.pending_key, self.processing_key, timeout=timeout)
        if raw_payload is None:
            return None
        
        payload = Payload.from_json(raw_payload)
        now = time.time()

        pipe = self.redis_client.pipeline()
        pipe.hset(self.raw_hash, payload.job_id, raw_payload)
        pipe.zadd(self.lease_zset, {payload.job_id: now})
        pipe.execute()

        return raw_payload, payload
    
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
            self.redis_client.lpush(self.pending_key, payload.to_json())

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
                self.redis_client.lpush(self.pending_key, payload.to_json())
                requeued += 1

        return requeued


        
    

        