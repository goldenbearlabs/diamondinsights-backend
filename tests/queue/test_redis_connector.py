import os

import shared.queue.redis_connector as redis_connector


def test_redis_connector_uses_explicit_url(monkeypatch):
    captured = {}

    def fake_from_url(cls, url, decode_responses=True):
        captured["url"] = url
        captured["decode_responses"] = decode_responses
        return "client"

    monkeypatch.setattr(redis_connector.Redis, "from_url", classmethod(fake_from_url))

    connector = redis_connector.RedisConnector(redis_url="redis://explicit")
    client = connector.client()

    assert client == "client"
    assert captured["url"] == "redis://explicit"
    assert captured["decode_responses"] is True


def test_redis_connector_uses_env_when_no_url(monkeypatch):
    captured = {}

    def fake_from_url(cls, url, decode_responses=True):
        captured["url"] = url
        captured["decode_responses"] = decode_responses
        return "client"

    monkeypatch.setattr(redis_connector.Redis, "from_url", classmethod(fake_from_url))
    monkeypatch.setenv("REDIS_URL", "redis://from-env")

    connector = redis_connector.RedisConnector()
    client = connector.client()

    assert client == "client"
    assert captured["url"] == "redis://from-env"
    assert captured["decode_responses"] is True


def test_redis_connector_passes_none_when_env_missing(monkeypatch):
    captured = {}

    def fake_from_url(cls, url, decode_responses=True):
        captured["url"] = url
        captured["decode_responses"] = decode_responses
        return "client"

    monkeypatch.setattr(redis_connector.Redis, "from_url", classmethod(fake_from_url))
    monkeypatch.delenv("REDIS_URL", raising=False)

    connector = redis_connector.RedisConnector()
    connector.client()

    assert "REDIS_URL" not in os.environ
    assert captured["url"] is None
    assert captured["decode_responses"] is True
