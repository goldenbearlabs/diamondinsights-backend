from typing import Any, Optional
from shared.core.config import CURRENT_MLB_SEASON
from shared.queue.redis_connector import RedisConnector
from shared.queue.queue import Queue, Payload

class JobPusher:
    def __init__(self, redis_connector: Optional[RedisConnector] = None, pending_key: str = "jobs:pending"):
        self.queue = Queue(redis_connector or RedisConnector(), pending_key)
    
    def push(
        self,
        job_type: str,
        args: Optional[dict[str, Any]] = None,
        *,
        priority: str = "normal",
    ) -> Payload:
        return self.queue.enqueue(job_type, args, priority=priority)
    
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
        return self.push("player_sync", {"rerun_all_cards": reload_all_players})
    
    def game_boxscore_sync(self, reload_all_games: bool = False, season: int = CURRENT_MLB_SEASON) -> Payload:
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

    def show_profile_refresh_enqueue(self) -> Payload:
        return self.push("show_profile_refresh_enqueue", priority="high")

    def show_profile_refresh_username(self, username: str, priority: str = "normal") -> Payload:
        return self.push("show_profile_refresh_username", {"username": username}, priority=priority)

    def show_game_refresh(self) -> Payload:
        return self.push("show_game_refresh")

    def show_game_refresh_enqueue(self) -> Payload:
        return self.push("show_game_refresh_enqueue", priority="high")

    def show_game_refresh_username(self, username: str, priority: str = "normal") -> Payload:
        return self.push("show_game_refresh_username", {"username": username}, priority=priority)

    def show_game_agg(self) -> Payload:
        return self.push("show_game_agg")

    def show_game_agg_enqueue(self) -> Payload:
        return self.push("show_game_agg_enqueue", priority="normal")

    def show_game_agg_batch(self, game_ids: list[str]) -> Payload:
        return self.push("show_game_agg_batch", {"game_ids": game_ids}, priority="low")

    def your_ovr_sync(self) -> Payload:
        return self.push("your_ovr_sync")

    def revenuecat_entitlements_reconcile(
        self,
        *,
        firebase_id: Optional[str] = None,
        user_id: Optional[int] = None,
        batch_limit: Optional[int] = None,
    ) -> Payload:
        args: dict[str, Any] = {}
        if firebase_id:
            args["firebase_id"] = firebase_id
        if user_id is not None:
            args["user_id"] = int(user_id)
        if batch_limit is not None:
            args["batch_limit"] = int(batch_limit)
        return self.push("revenuecat_entitlements_reconcile", args)
