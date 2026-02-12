from typing import Any, Optional
from shared.queue.redis_connector import RedisConnector
from shared.queue.queue import Queue, Payload

class JobPusher:
    def __init__(self, redis_connector: Optional[RedisConnector] = None, pending_key: str = "jobs:pending"):
        self.queue = Queue(redis_connector or RedisConnector(), pending_key)
    
    def push(self, job_type: str, args: Optional[dict[str, Any]] = None) -> Payload:
        return self.queue.enqueue(job_type, args)
    
    def card_sync(self, reload_all_years: bool = False) -> Payload:
        return self.push("card_sync", {"reload_all_years": reload_all_years})

    def roster_update_sync(self, reload_all_years: bool = False) -> Payload:
        return self.push("roster_update_sync", {"reload_all_years": reload_all_years})
    
    def market_sync(self) -> Payload:
        return self.push("market_sync")

    def market_sync_above(self) -> Payload:
        return self.push("market_sync_above")

    def market_sync_below(self) -> Payload:
        return self.push("market_sync_below")
    
    def market_candle_sync(self) -> Payload:
        return self.push("market_candle_sync")

    def market_price_sync(self) -> Payload:
        return self.push("market_price_sync")
    
    def player_sync(self, reload_all_players: bool = False) -> Payload:
        return self.push("player_sync", {"reload_all_players": reload_all_players})
    
    def game_boxscore_sync(self, reload_all_games: bool = False, season = 2025) -> Payload:
        return self.push("game_boxscore_sync", {"reload_all_games": reload_all_games, "season": season})
    
    def prediction_sync(self) -> Payload:
        return self.push("prediction_sync")

    def card_position_overall_sync(self, weights_path: Optional[str] = None) -> Payload:
        args: dict[str, Any] = {}
        if weights_path:
            args["weights_path"] = weights_path
        return self.push("card_position_overall_sync", args)
    
    def chat_cleaner(self) -> Payload:
        return self.push("chat_cleaner")
    
    def image_cleaner(self) -> Payload:
        return self.push("image_cleaner")
    
    def show_profile_stats_updater(self) -> Payload:
        return self.push("show_profile_stats_updater")
