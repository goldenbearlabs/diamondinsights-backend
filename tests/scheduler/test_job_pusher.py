import pytest

import apps.scheduler.job_pusher as job_pusher


class FakeQueue:
    def __init__(self, redis_connector, pending_key):
        self.redis_connector = redis_connector
        self.pending_key = pending_key
        self.calls = []

    def enqueue(self, job_type, args=None):
        payload = {"job_type": job_type, "args": args or {}}
        self.calls.append(payload)
        return payload


@pytest.mark.parametrize(
    "method,kwargs,expected_args",
    [
        ("card_sync", {}, {"reload_all_years": False}),
        ("card_sync", {"reload_all_years": True}, {"reload_all_years": True}),
        ("roster_update_sync", {}, {"reload_all_years": False}),
        ("roster_update_sync", {"reload_all_years": True}, {"reload_all_years": True}),
        ("market_sync", {}, {}),
        ("market_candle_sync", {}, {}),
        ("player_sync", {}, {"reload_all_players": False}),
        ("player_sync", {"reload_all_players": True}, {"reload_all_players": True}),
        ("game_boxscore_sync", {}, {"reload_all_games": False, "season": 2025}),
        ("game_boxscore_sync", {"reload_all_games": True, "season": 2024}, {"reload_all_games": True, "season": 2024}),
        ("prediction_sync", {}, {}),
        ("card_position_overall_sync", {}, {}),
        (
            "card_position_overall_sync",
            {"weights_path": "apps/backend/src/ml/true_overall_weights.json"},
            {"weights_path": "apps/backend/src/ml/true_overall_weights.json"},
        ),
        ("chat_cleaner", {}, {}),
        ("image_cleaner", {}, {}),
        ("show_profile_stats_updater", {}, {}),
        ("your_ovr_sync", {}, {}),
    ],
)
def test_job_pusher_methods_enqueue(method, kwargs, expected_args, monkeypatch):
    monkeypatch.setattr(job_pusher, "Queue", FakeQueue)

    pusher = job_pusher.JobPusher()
    payload = getattr(pusher, method)(**kwargs)

    assert pusher.queue.calls == [{"job_type": method, "args": expected_args}]
    assert payload["job_type"] == method
    assert payload["args"] == expected_args


def test_job_pusher_uses_provided_connector_and_pending_key(monkeypatch):
    monkeypatch.setattr(job_pusher, "Queue", FakeQueue)

    fake_connector = object()
    pusher = job_pusher.JobPusher(redis_connector=fake_connector, pending_key="custom:pending")

    assert pusher.queue.redis_connector is fake_connector
    assert pusher.queue.pending_key == "custom:pending"


def test_job_pusher_constructs_connector_when_none(monkeypatch):
    class StubRedisConnector:
        created = 0

        def __init__(self):
            StubRedisConnector.created += 1

    monkeypatch.setattr(job_pusher, "RedisConnector", StubRedisConnector)
    monkeypatch.setattr(job_pusher, "Queue", FakeQueue)

    pusher = job_pusher.JobPusher()

    assert StubRedisConnector.created == 1
    assert isinstance(pusher.queue.redis_connector, StubRedisConnector)
