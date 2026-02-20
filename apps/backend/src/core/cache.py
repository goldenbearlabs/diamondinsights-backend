import json
import logging
import os
from typing import Any

from fastapi import Request
from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


def _read_default_ttl() -> int:
    raw = os.getenv("CACHE_DEFAULT_TTL_SEC", "60")
    try:
        ttl = int(raw)
    except ValueError:
        logger.warning("Invalid CACHE_DEFAULT_TTL_SEC=%s. Falling back to 60.", raw)
        return 60
    return max(1, ttl)


CACHE_DEFAULT_TTL_SEC = _read_default_ttl()


def init_cache_client() -> Redis | None:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        logger.warning("REDIS_URL is not set. API cache is disabled.")
        return None

    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        client.ping()
        logger.info("Redis cache connected.")
    except RedisError:
        logger.exception("Failed to connect to Redis cache. Caching is disabled.")
        return None
    return client


def close_cache_client(client: Redis | None) -> None:
    if client is None:
        return
    try:
        client.close()
    except RedisError:
        logger.exception("Failed to close Redis cache client cleanly.")


def get_cache_client(request: Request) -> Redis | None:
    return getattr(request.app.state, "cache", None)


def build_cache_key(*parts: object) -> str:
    return "di:api:" + ":".join(str(part) for part in parts)


def get_cached_json(cache: Redis | None, key: str) -> dict[str, Any] | None:
    if cache is None:
        return None

    try:
        raw = cache.get(key)
    except RedisError:
        logger.exception("Redis GET failed for key=%s", key)
        return None

    if not raw:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON in cache key=%s", key)
        return None

    if isinstance(payload, dict):
        return payload
    return None


def set_cached_json(cache: Redis | None, key: str, payload: dict[str, Any], ttl_sec: int) -> None:
    if cache is None:
        return
    try:
        cache.setex(key, ttl_sec, json.dumps(payload))
    except RedisError:
        logger.exception("Redis SETEX failed for key=%s", key)
