import json
import logging
import os
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterable

from redis import Redis


logger = logging.getLogger("heartbeat_exporter")


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")


def _metric_line(name: str, value: float | int, labels: dict[str, str] | None = None) -> str:
    if labels:
        label_pairs = ",".join(f'{k}="{_escape_label(v)}"' for k, v in labels.items())
        return f"{name}{{{label_pairs}}} {value}"
    return f"{name} {value}"


def _parse_iso_age_seconds(ts: str | None, now: float) -> float | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, now - dt.timestamp())


def _redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        raise RuntimeError("REDIS_URL is not set")
    return Redis.from_url(redis_url, decode_responses=True)


def _iter_runner_status(
    client: Redis,
    prefix: str,
) -> Iterable[tuple[str, dict, int]]:
    for key in client.scan_iter(match=f"{prefix}*"):
        raw = client.get(key)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        runner = key[len(prefix):]
        ttl = client.ttl(key)
        yield runner, data, ttl


def _build_metrics() -> str:
    now = time.time()
    lines: list[str] = []

    lines.append("# HELP heartbeat_exporter_up 1 if the exporter collected metrics successfully.")
    lines.append("# TYPE heartbeat_exporter_up gauge")
    lines.append("# HELP heartbeat_exporter_scrape_error 1 if the exporter failed to collect metrics.")
    lines.append("# TYPE heartbeat_exporter_scrape_error gauge")

    scrape_error = 0
    try:
        client = _redis_client()
        client.ping()

        zset_key = os.getenv("JOB_PROCESSING_ZSET", "jobs:processing:lease")
        stale_threshold_s = int(os.getenv("HEARTBEAT_STALE_SECONDS", "90"))
        queue_keys = [k.strip() for k in os.getenv(
            "QUEUE_KEYS",
            "jobs:pending,jobs:processing,jobs:dead",
        ).split(",") if k.strip()]
        max_scan = int(os.getenv("QUEUE_SCAN_MAX", "2000"))

        lines.append("# HELP jobs_processing_active Number of jobs currently in processing.")
        lines.append("# TYPE jobs_processing_active gauge")
        lines.append("# HELP jobs_processing_heartbeat_oldest_age_seconds Age of the oldest job heartbeat.")
        lines.append("# TYPE jobs_processing_heartbeat_oldest_age_seconds gauge")
        lines.append("# HELP jobs_processing_heartbeat_newest_age_seconds Age of the newest job heartbeat.")
        lines.append("# TYPE jobs_processing_heartbeat_newest_age_seconds gauge")
        lines.append("# HELP jobs_processing_heartbeat_stale Number of processing jobs with stale heartbeats.")
        lines.append("# TYPE jobs_processing_heartbeat_stale gauge")

        processing_count = int(client.zcard(zset_key) or 0)
        oldest_age = 0.0
        newest_age = 0.0
        stale_count = 0
        if processing_count:
            min_entry = client.zrange(zset_key, 0, 0, withscores=True)
            max_entry = client.zrange(zset_key, -1, -1, withscores=True)
            if min_entry:
                oldest_age = max(0.0, now - float(min_entry[0][1]))
            if max_entry:
                newest_age = max(0.0, now - float(max_entry[0][1]))
            if stale_threshold_s > 0:
                cutoff = now - stale_threshold_s
                stale_count = int(client.zcount(zset_key, 0, cutoff) or 0)

        lines.append(_metric_line("jobs_processing_active", processing_count))
        lines.append(_metric_line("jobs_processing_heartbeat_oldest_age_seconds", oldest_age))
        lines.append(_metric_line("jobs_processing_heartbeat_newest_age_seconds", newest_age))
        lines.append(_metric_line("jobs_processing_heartbeat_stale", stale_count))

        prefix = os.getenv("RUNNER_STATUS_KEY_PREFIX", "runner:status:")

        lines.append("# HELP jobs_runner_up 1 if the runner heartbeat key exists.")
        lines.append("# TYPE jobs_runner_up gauge")
        lines.append("# HELP jobs_runner_ttl_seconds TTL in seconds for the runner heartbeat key.")
        lines.append("# TYPE jobs_runner_ttl_seconds gauge")
        lines.append("# HELP jobs_runner_last_update_age_seconds Age since the runner status last updated.")
        lines.append("# TYPE jobs_runner_last_update_age_seconds gauge")
        lines.append("# HELP jobs_runner_started_age_seconds Age since the runner started.")
        lines.append("# TYPE jobs_runner_started_age_seconds gauge")
        lines.append("# HELP jobs_runner_state Runner state (1 for current state).")
        lines.append("# TYPE jobs_runner_state gauge")
        lines.append("# HELP jobs_runner_job_type Current job type if running.")
        lines.append("# TYPE jobs_runner_job_type gauge")

        for runner, data, ttl in _iter_runner_status(client, prefix):
            labels = {"runner": runner}
            lines.append(_metric_line("jobs_runner_up", 1, labels))
            lines.append(_metric_line("jobs_runner_ttl_seconds", ttl, labels))

            update_age = _parse_iso_age_seconds(data.get("updated_at"), now)
            if update_age is not None:
                lines.append(_metric_line("jobs_runner_last_update_age_seconds", update_age, labels))

            started_age = _parse_iso_age_seconds(data.get("started_at"), now)
            if started_age is not None:
                lines.append(_metric_line("jobs_runner_started_age_seconds", started_age, labels))

            state = (data.get("state") or "unknown").lower()
            for candidate in ("idle", "running", "unknown"):
                lines.append(
                    _metric_line(
                        "jobs_runner_state",
                        1 if state == candidate else 0,
                        {"runner": runner, "state": candidate},
                    )
                )

            job_type = (data.get("job_type") or "").strip()
            if job_type:
                lines.append(
                    _metric_line(
                        "jobs_runner_job_type",
                        1,
                        {"runner": runner, "job_type": job_type},
                    )
                )

        lines.append("# HELP jobs_queue_depth Number of items in the queue.")
        lines.append("# TYPE jobs_queue_depth gauge")
        lines.append("# HELP jobs_queue_oldest_age_seconds Age of the oldest item in the queue.")
        lines.append("# TYPE jobs_queue_oldest_age_seconds gauge")
        lines.append("# HELP jobs_queue_newest_age_seconds Age of the newest item in the queue.")
        lines.append("# TYPE jobs_queue_newest_age_seconds gauge")
        lines.append("# HELP jobs_queue_job_type_count Count of items by job_type in the queue.")
        lines.append("# TYPE jobs_queue_job_type_count gauge")
        lines.append("# HELP jobs_queue_sample_incomplete 1 if the queue scan was sampled.")
        lines.append("# TYPE jobs_queue_sample_incomplete gauge")

        for key in queue_keys:
            depth = int(client.llen(key) or 0)
            labels = {"queue": key}
            lines.append(_metric_line("jobs_queue_depth", depth, labels))

            if depth == 0:
                continue

            scan_all = max_scan <= 0 or depth <= max_scan
            scan_count = depth if scan_all else max_scan

            items = client.lrange(key, 0, scan_count - 1)

            oldest_age = None
            newest_age = None
            type_counts: dict[str, int] = {}

            for raw in items:
                try:
                    payload = json.loads(raw)
                except Exception:
                    continue
                enqueued_at = payload.get("enqueued_at")
                age = _parse_iso_age_seconds(enqueued_at, now)
                if age is not None:
                    oldest_age = age if oldest_age is None else max(oldest_age, age)
                    newest_age = age if newest_age is None else min(newest_age, age)

                job_type = str(payload.get("job_type") or "").strip()
                if job_type:
                    type_counts[job_type] = type_counts.get(job_type, 0) + 1

            if oldest_age is not None:
                lines.append(_metric_line("jobs_queue_oldest_age_seconds", oldest_age, labels))
            if newest_age is not None:
                lines.append(_metric_line("jobs_queue_newest_age_seconds", newest_age, labels))

            for job_type, count in type_counts.items():
                lines.append(
                    _metric_line(
                        "jobs_queue_job_type_count",
                        count,
                        {"queue": key, "job_type": job_type},
                    )
                )

            if not scan_all:
                lines.append(_metric_line("jobs_queue_sample_incomplete", 1, labels))
            else:
                lines.append(_metric_line("jobs_queue_sample_incomplete", 0, labels))

    except Exception as exc:
        scrape_error = 1
        logger.exception("metrics scrape failed: %s", exc)

    lines.append(_metric_line("heartbeat_exporter_up", 0 if scrape_error else 1))
    lines.append(_metric_line("heartbeat_exporter_scrape_error", scrape_error))
    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/metrics"):
            self.send_response(404)
            self.end_headers()
            return

        body = _build_metrics().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args) -> None:
        logger.info("%s - %s", self.address_string(), fmt % args)


def main() -> None:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    port = int(os.getenv("EXPORTER_PORT", "9188"))
    server = ThreadingHTTPServer(("0.0.0.0", port), MetricsHandler)
    logger.info("heartbeat exporter listening on :%s", port)
    server.serve_forever()


if __name__ == "__main__":
    main()
