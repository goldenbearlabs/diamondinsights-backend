import argparse
from src.database.database import SessionLocal

from src.jobs.market_sync import MarketSync
from src.jobs.card_sync import CardSync
from src.jobs.market_candle_sync import MarketCandleSync
from src.jobs.roster_update_sync import RosterUpdateSync

def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "job",
        choices=["market_sync", "card_sync", "market_candle_sync", "roster_update_sync"]
    )
    p.add_argument("--reload-all-years", action="store_true")
    args = p.parse_args()

    with SessionLocal() as session:
        if args.job == "market_sync":
            MarketSync().execute(session)
        elif args.job == "card_sync":
            CardSync(reload_all_years=args.reload_all_years).execute(session)
        elif args.job == "market_candle_sync":
            MarketCandleSync().execute(session)
        elif args.job == "roster_update_sync":
            RosterUpdateSync(reload_all_years=args.reload_all_years).execute(session)

if __name__ == "__main__":
    main()
