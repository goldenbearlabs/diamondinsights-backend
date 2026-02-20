import runpy
from pathlib import Path

import pytest

import apps.scheduler.main as scheduler_main


class FakeScheduler:
    def __init__(self, timezone=None):
        self.timezone = timezone
        self.jobs = []
        self.started = False

    def add_job(self, func, args=None, **kwargs):
        self.jobs.append({
            "func": func,
            "args": args,
            **kwargs,
        })

    def start(self):
        self.started = True


class StubPusher:
    def card_sync(self, reload_all_years=False):
        return reload_all_years

    def market_sync(self):
        return "market_sync"

    def market_candle_sync(self):
        return "market_candle_sync"

    def player_sync(self, reload_all_players=False):
        return reload_all_players

    def game_boxscore_sync(self, reload_all_games=False, season=2025):
        return (reload_all_games, season)

    def prediction_sync(self):
        return "prediction_sync"

    def roster_update_sync(self, reload_all_years=False):
        return reload_all_years


def _build_with_jobs(monkeypatch, jobs, *, pusher=None, timezone=None, scheduler_cls=FakeScheduler):
    monkeypatch.setattr(scheduler_main, "SCHEDULED_JOBS", jobs)
    return scheduler_main.build_scheduler(
        pusher=pusher,
        timezone=timezone or scheduler_main.DEFAULT_TIMEZONE,
        scheduler_cls=scheduler_cls,
    )


def test_build_scheduler_registers_jobs(monkeypatch):
    pusher = StubPusher()
    jobs = [
        {"func_name": "market_sync", "kwargs": {"trigger": "cron", "minute": "*/10"}},
        {"func_name": "card_sync", "args": [True], "kwargs": {"trigger": "cron", "hour": "0"}},
    ]
    sched = _build_with_jobs(
        monkeypatch=monkeypatch,
        jobs=jobs,
        pusher=pusher,
        scheduler_cls=FakeScheduler,
    )

    assert sched.timezone == scheduler_main.DEFAULT_TIMEZONE
    assert len(sched.jobs) == len(jobs)

    for job, expected in zip(sched.jobs, jobs, strict=True):
        assert job["func"] == getattr(pusher, expected["func_name"])
        assert job["args"] == (expected.get("args") or [])
        for key, value in (expected.get("kwargs") or {}).items():
            assert job[key] == value


def test_build_scheduler_respects_custom_timezone(monkeypatch):
    pusher = StubPusher()
    sched = _build_with_jobs(
        monkeypatch,
        [{"func_name": "card_sync", "kwargs": {"trigger": "cron"}}],
        pusher=pusher,
        timezone="UTC",
    )

    assert sched.timezone == "UTC"


def test_build_scheduler_uses_default_pusher_when_none(monkeypatch):
    class StubJobPusher:
        created = 0

        def __init__(self):
            StubJobPusher.created += 1

        def card_sync(self, reload_all_years=False):
            return reload_all_years

    monkeypatch.setattr(scheduler_main, "JobPusher", StubJobPusher)

    _build_with_jobs(
        monkeypatch,
        [{"func_name": "card_sync", "kwargs": {"trigger": "cron"}}],
        pusher=None,
    )

    assert StubJobPusher.created == 1


def test_build_scheduler_does_not_instantiate_pusher_when_provided(monkeypatch):
    class ShouldNotConstruct:
        def __init__(self):
            raise AssertionError("JobPusher should not be constructed")

    monkeypatch.setattr(scheduler_main, "JobPusher", ShouldNotConstruct)

    pusher = StubPusher()
    _build_with_jobs(
        monkeypatch,
        [{"func_name": "card_sync", "kwargs": {"trigger": "cron"}}],
        pusher=pusher,
    )


def test_build_scheduler_handles_missing_and_none_args_kwargs(monkeypatch):
    pusher = StubPusher()
    sched = _build_with_jobs(
        monkeypatch,
        [
            {"func_name": "card_sync"},
            {"func_name": "card_sync", "args": None, "kwargs": None},
        ],
        pusher=pusher,
    )

    assert sched.jobs[0]["args"] == []
    assert sched.jobs[1]["args"] == []
    assert len(sched.jobs[0].keys()) == 2
    assert len(sched.jobs[1].keys()) == 2


def test_build_scheduler_empty_schedule(monkeypatch):
    pusher = StubPusher()
    sched = _build_with_jobs(monkeypatch, [], pusher=pusher)
    assert sched.jobs == []


def test_build_scheduler_invalid_func_name_raises(monkeypatch):
    pusher = StubPusher()
    jobs = [{"func_name": "missing", "kwargs": {"trigger": "cron"}}]
    monkeypatch.setattr(scheduler_main, "SCHEDULED_JOBS", jobs)

    with pytest.raises(AttributeError):
        scheduler_main.build_scheduler(pusher=pusher, scheduler_cls=FakeScheduler)


def test_build_scheduler_missing_func_name_raises(monkeypatch):
    pusher = StubPusher()
    jobs = [{"kwargs": {"trigger": "cron"}}]
    monkeypatch.setattr(scheduler_main, "SCHEDULED_JOBS", jobs)

    with pytest.raises(KeyError):
        scheduler_main.build_scheduler(pusher=pusher, scheduler_cls=FakeScheduler)


def test_main_starts_scheduler(monkeypatch):
    fake_sched = FakeScheduler()

    def fake_build_scheduler():
        return fake_sched

    monkeypatch.setattr(scheduler_main, "build_scheduler", fake_build_scheduler)

    scheduler_main.main()

    assert fake_sched.started is True


def test_module_entrypoint_runs_main(monkeypatch):
    started = {"value": False}

    class StubJobPusher:
        def card_sync(self, reload_all_years=False):
            return reload_all_years

        def market_sync(self):
            return "market_sync"

        def market_candle_sync(self):
            return "market_candle_sync"

        def player_sync(self, reload_all_players=False):
            return reload_all_players

        def game_boxscore_sync(self, reload_all_games=False, season=2025):
            return (reload_all_games, season)

        def prediction_sync(self):
            return "prediction_sync"

        def roster_update_sync(self, reload_all_years=False):
            return reload_all_years

    class FakeScheduler:
        def __init__(self, timezone=None):
            self.timezone = timezone

        def add_job(self, func, args=None, **kwargs):
            return (func, args, kwargs)

        def start(self):
            started["value"] = True

    import apscheduler.schedulers.blocking as blocking
    import apps.scheduler.job_pusher as job_pusher

    monkeypatch.setattr(blocking, "BlockingScheduler", FakeScheduler)
    monkeypatch.setattr(job_pusher, "JobPusher", StubJobPusher)

    module_path = Path(scheduler_main.__file__)
    runpy.run_path(str(module_path), run_name="__main__")

    assert started["value"] is True
