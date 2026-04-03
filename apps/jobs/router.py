import json
import logging
import os
import socket
import threading
from datetime import datetime, timezone

from shared.core.config import CURRENT_MLB_SEASON
from shared.queue.redis_connector import RedisConnector
from shared.queue.queue import Queue
from shared.db.database import SessionLocal


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Router:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.connector = RedisConnector()
        self.redis = self.connector.client()
        self.queue = Queue(redis_connector=self.connector)

        self.runner_name = os.getenv("RUNNER_NAME") or socket.gethostname()
        self.runner_key = f"runner:status:{self.runner_name}"

        self.hb_interval_s = int(os.getenv("RUNNER_HEARTBEAT_INTERVAL_SECONDS", "15"))
        self.hb_ttl_s = int(os.getenv("RUNNER_HEARTBEAT_TTL_SECONDS", "60"))

        self.started_at = _utc_iso()
        self._status = {
            "name": self.runner_name,
            "state": "idle",
            "current_job_id": None,
            "job_type": None,
            "updated_at": _utc_iso(),
            "started_at": self.started_at,
        }

    def _set_status(self, state: str, job_id: str | None = None, job_type: str | None = None) -> None:
        self._status["state"] = state
        self._status["current_job_id"] = job_id
        self._status["job_type"] = job_type
        self._status["updated_at"] = _utc_iso()

    def _start_runner_heartbeat(self, stop: threading.Event) -> threading.Thread:
        def loop():
            while not stop.is_set():
                try:
                    self.redis.set(
                        self.runner_key,
                        json.dumps(self._status),
                        ex=self.hb_ttl_s,
                    )
                except Exception as e:
                    self.logger.error("runner heartbeat error name=%s err=%s", self.runner_name, e)
                stop.wait(self.hb_interval_s)

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        return t

    def _start_job_heartbeat(self, job_id: str, stop: threading.Event, interval_s: int = 30) -> threading.Thread:
        def loop():
            while not stop.is_set():
                try:
                    self.queue.heartbeat(job_id)
                except Exception as e:
                    self.logger.error("job heartbeat error job_id=%s err=%s", job_id, e)
                stop.wait(interval_s)

        t = threading.Thread(target=loop, daemon=True)
        t.start()
        return t

    def run(self):
        stop_runner_hb = threading.Event()
        runner_hb_thread = self._start_runner_heartbeat(stop_runner_hb)
        self.logger.info("jobs router started name=%s", self.runner_name)

        try:
            while True:
                item = self.queue.reserve(timeout=10)
                if item is None:
                    if self._status["state"] != "idle":
                        self._set_status("idle")
                    continue

                raw_payload, payload = item

                stop_job_hb = threading.Event()
                job_hb_thread = self._start_job_heartbeat(payload.job_id, stop_job_hb, interval_s=30)

                self._set_status("running", job_id=payload.job_id, job_type=payload.job_type)
                self.logger.info(
                    "job start job_id=%s job_type=%s runner=%s",
                    payload.job_id,
                    payload.job_type,
                    self.runner_name,
                )

                session = None
                try:
                    with SessionLocal() as session:
                        match payload.job_type:
                            case "card_sync":
                                from apps.jobs.card_sync import CardSync
                                reload_all = bool(payload.args.get("reload_all_years", False))
                                CardSync(reload_all_years=reload_all).run(session)

                            case "roster_update_sync":
                                from apps.jobs.roster_update_sync import RosterUpdateSync
                                reload_all = int(payload.args.get("reload_all_years", False))
                                RosterUpdateSync(reload_all_years=reload_all).run(session)

                            case "game_boxscore_sync":
                                from apps.jobs.game_boxscore_sync import GameBoxscoreSync
                                reload_all = int(payload.args.get("reload_all_games", False))
                                season = int(payload.args.get("season", CURRENT_MLB_SEASON))
                                GameBoxscoreSync(season_year=season,rerun_all_boxscores=reload_all).run(session)

                            case "market_sync":
                                from apps.jobs.market_sync import MarketSync
                                MarketSync().run(session)

                            case "market_sync_above":
                                from apps.jobs.market_sync import MarketSync
                                MarketSync(ovr_min=70).run(session)

                            case "market_sync_below":
                                from apps.jobs.market_sync import MarketSync
                                MarketSync(ovr_max=69).run(session)

                            case "market_candle_sync":
                                from apps.jobs.market_candle_sync import MarketCandleSync
                                MarketCandleSync().run(session)

                            case "market_price_sync":
                                from apps.jobs.market_price_sync import MarketPriceSync
                                MarketPriceSync().run(session)

                            case "player_sync":
                                from apps.jobs.player_sync import PlayerSync
                                reload_all = bool(payload.args.get("rerun_all_cards", False))
                                PlayerSync(rerun_all_cards=reload_all).run(session)

                            case "prediction_sync":
                                from apps.jobs.prediction_sync import PredictionSync
                                PredictionSync().run(session)

                            case "card_position_overall_sync":
                                from apps.jobs.card_position_overall_sync import CardPositionOverallSync
                                weights_path = payload.args.get("weights_path")
                                if weights_path:
                                    CardPositionOverallSync(weights_path=str(weights_path)).run(session)
                                else:
                                    CardPositionOverallSync().run(session)

                            case "show_profile_stats_updater":
                                from apps.jobs.show_profile_refresh import ShowProfileStatsUpdater
                                ShowProfileStatsUpdater().run(session)

                            case "show_game_refresh":
                                from apps.jobs.show_game_refresh import ShowGameRefresh
                                ShowGameRefresh().run(session)

                            case "show_game_agg":
                                from apps.jobs.show_game_agg import ShowGameAgg
                                ShowGameAgg().run(session)

                            case "your_ovr_sync":
                                from apps.jobs.your_ovr_sync import YourOvrSync
                                YourOvrSync().run(session)

                            case "roster-update-aggregator":
                                from apps.jobs.roster_update_aggregator import RosterUpdateAggregator
                                RosterUpdateAggregator().run(session)

                            case "revenuecat_entitlements_reconcile":
                                from apps.jobs.revenuecat_entitlements_reconcile import (
                                    RevenueCatEntitlementsReconcile,
                                )

                                firebase_id_raw = payload.args.get("firebase_id")
                                firebase_id = (
                                    str(firebase_id_raw).strip()
                                    if firebase_id_raw is not None
                                    else None
                                )
                                if firebase_id == "":
                                    firebase_id = None

                                user_id_raw = payload.args.get("user_id")
                                user_id = (
                                    int(user_id_raw)
                                    if user_id_raw is not None and str(user_id_raw).strip() != ""
                                    else None
                                )

                                batch_limit_raw = payload.args.get("batch_limit")
                                batch_limit = (
                                    int(batch_limit_raw)
                                    if batch_limit_raw is not None and str(batch_limit_raw).strip() != ""
                                    else None
                                )

                                RevenueCatEntitlementsReconcile(
                                    firebase_id=firebase_id,
                                    user_id=user_id,
                                    batch_limit=batch_limit,
                                ).run(session)
                            
                            case _:
                                raise ValueError(f"Unknown job type: {payload.job_type}")

                        self.queue.ack(raw_payload, payload)
                        self.logger.info(
                            "job complete job_id=%s job_type=%s runner=%s",
                            payload.job_id,
                            payload.job_type,
                            self.runner_name,
                        )

                except Exception as e:
                    if session is not None:
                        try:
                            session.rollback()
                        except Exception:
                            pass

                    self.logger.exception(
                        "job error job_id=%s job_type=%s runner=%s err=%s",
                        payload.job_id,
                        payload.job_type,
                        self.runner_name,
                        e,
                    )
                    try:
                        self.queue.fail(raw_payload, payload, max_attempts=5)
                    except Exception as e2:
                        self.logger.error(
                            "job fail error job_id=%s job_type=%s err=%s",
                            payload.job_id,
                            payload.job_type,
                            e2,
                        )

                finally:
                    self._set_status("idle")
                    stop_job_hb.set()
                    job_hb_thread.join(timeout=1)

        finally:
            stop_runner_hb.set()
            runner_hb_thread.join(timeout=1)
