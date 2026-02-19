from fastapi import APIRouter

from src.api.routes import cards, listings, completed_orders, quirks, market_candles, mlb_game_batting_stats, \
                                    players, users, records, show_profiles, search, chat, card_predictions, user_predictions, \
                                    card_comments, portfolios, flipping, mlb_season_stats
from src.core.config import *

api_router = APIRouter()
api_router.include_router(cards.router)
api_router.include_router(listings.router)
api_router.include_router(completed_orders.router)
api_router.include_router(quirks.router)
api_router.include_router(market_candles.router)
api_router.include_router(mlb_game_batting_stats.router)
api_router.include_router(mlb_season_stats.router)
api_router.include_router(players.router)
api_router.include_router(users.router)
api_router.include_router(records.router)
api_router.include_router(show_profiles.router)
api_router.include_router(show_profiles.public_router)
api_router.include_router(search.router)
api_router.include_router(chat.router)
api_router.include_router(card_predictions.router)
api_router.include_router(user_predictions.router)
api_router.include_router(card_comments.router)
api_router.include_router(portfolios.router)
api_router.include_router(flipping.router)
