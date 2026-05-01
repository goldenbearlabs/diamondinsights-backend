import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from apps.scheduler.job_pusher import JobPusher
from shared.core.logging_config import configure_logging

DEFAULT_TIMEZONE = "America/Vancouver"
SCHEDULED_JOBS = [
    {
        "func_name": "market_price_sync",
        "kwargs": {
            "trigger": "cron",
            "minute": "*/3",
            "misfire_grace_time": 120,
            "coalesce": True,
        },
    },
    {
        "func_name": "market_sync_above",
        "kwargs": {
            "trigger": "cron",
            "hour": "*/1",
            "misfire_grace_time": 600,
            "coalesce": True,
        },
    },
    {
        "func_name": "market_sync_below",
        "kwargs": {
            "trigger": "cron",
            "hour": "*/2",
            "minute": "0",
            "misfire_grace_time": 600,
            "coalesce": True,
        },
    },
    {
        "func_name": "market_candle_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "0",
            "minute": "0",
            "misfire_grace_time": 600,
            "coalesce": True,
        },
    },
    {
        "func_name": "show_profile_refresh_enqueue",
        "kwargs": {
            "trigger": "cron",
            "hour": "*/3",
            "minute": "0",
            "misfire_grace_time": 900,
            "coalesce": True,
        },
    },
    {
        "func_name": "show_game_refresh_enqueue",
        "kwargs": {
            "trigger": "cron",
            "hour": "3,19",
            "minute": "0",
            "misfire_grace_time": 1800,
            "coalesce": True,
        },
    },
    {
        "func_name": "show_game_agg_enqueue",
        "kwargs": {
            "trigger": "cron",
            "hour": "6,22",
            "minute": "0",
            "misfire_grace_time": 1800,
            "coalesce": True,
        },
    },
    {
        "func_name": "card_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "*/6",
            "misfire_grace_time": 600,
            "coalesce": True,
        }, 
    },
    {
        "func_name": "card_position_overall_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "1,13",
            "minute": "45",
            "misfire_grace_time": 1200,
            "coalesce": True,
        },
    },
    {
        "func_name": "player_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "0",
            "minute": "30",
            "misfire_grace_time": 1200,
            "coalesce": True,
        },
    },
    {
        "func_name": "game_boxscore_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "1",
            "minute": "0",
            "misfire_grace_time": 1200,
            "coalesce": True,
        },
    },
    {
        "func_name": "prediction_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "1",
            "minute": "10",
            "misfire_grace_time": 1200,
            "coalesce": True,
        },
    },
    {
        "func_name": "roster_update_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "15",
            "minute": "0",
            "misfire_grace_time": 600,
            "coalesce": True,
        },
    },
    {
        "func_name": "revenuecat_entitlements_reconcile",
        "kwargs": {
            "trigger": "cron",
            "hour": "*/1",
            "minute": "17",
            "misfire_grace_time": 900,
        }
    },
  {
        "func_name": "your_ovr_sync",
        "kwargs": {
            "trigger": "cron",
            "hour": "2",
            "minute": "30",
            "misfire_grace_time": 1200,
            "coalesce": True,
        },
    },
]


def build_scheduler(
    pusher: JobPusher | None = None,
    timezone: str = DEFAULT_TIMEZONE,
    scheduler_cls=BlockingScheduler,
):
    pusher = pusher or JobPusher()
    sched = scheduler_cls(timezone=timezone)
    logger = logging.getLogger(__name__)

    for job in SCHEDULED_JOBS:
        func = getattr(pusher, job["func_name"])
        args = job.get("args") or []
        kwargs = job.get("kwargs") or {}
        logger.info(
            "scheduling job func=%s trigger=%s args=%s kwargs=%s",
            job.get("func_name"),
            kwargs.get("trigger"),
            args,
            {k: v for k, v in kwargs.items() if k != "trigger"},
        )
        sched.add_job(func, args=args, **kwargs)

    return sched


def main():
    configure_logging(service_name="scheduler")
    logger = logging.getLogger(__name__)
    logger.info("scheduler starting timezone=%s jobs=%s", DEFAULT_TIMEZONE, len(SCHEDULED_JOBS))
    sched = build_scheduler()
    sched.start()

if __name__ == "__main__":
    main()
