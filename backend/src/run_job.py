from sqlalchemy import text
import argparse

from src.database.database import SessionLocal

from src.jobs.market_sync import MarketSync
from src.jobs.card_sync import CardSync
from src.jobs.market_candle_sync import MarketCandleSync
from src.jobs.roster_update_sync import RosterUpdateSync


def try_lock(session, name: str) -> bool:
    return bool(
        session.execute(
            text("SELECT pg_try_advisory_lock(hashtext(:k), 0)"),
            {"k": name},
        ).scalar()
    )


def unlock(session, name: str) -> None:
    session.execute(
        text("SELECT pg_advisory_unlock(hashtext(:k), 0)"),
        {"k": name},
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "job",
        choices=["market_sync", "card_sync", "market_candle_sync", "roster_update_sync"],
    )
    p.add_argument("--reload-all-years", action="store_true")
    args = p.parse_args()

    job_key = args.job

    with SessionLocal() as session:
        if not try_lock(session, job_key):
            return

        try:
            if job_key == "market_sync":
                MarketSync().execute(session)
            elif job_key == "card_sync":
                CardSync(reload_all_years=args.reload_all_years).execute(session)
            elif job_key == "market_candle_sync":
                MarketCandleSync().execute(session)
            elif job_key == "roster_update_sync":
                RosterUpdateSync(reload_all_years=args.reload_all_years).execute(session)
        finally:
            unlock(session, job_key)


if __name__ == "__main__":
    main()
