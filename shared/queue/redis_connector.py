import os
from redis import Redis

class RedisConnector:
    def __init__(self, redis_url: str | None = None):
        self.redis_url = redis_url or os.getenv("REDIS_URL")

    def client(self) -> Redis:
        return Redis.from_url(self.redis_url, decode_responses=True)