#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

COVERAGE_XML = Path("tests/coverage/coverage.xml")
REPORT_PATH = Path("test_coverage.md")

FILES = [
    "apps/scheduler/job_pusher.py",
    "apps/scheduler/main.py",
    "apps/jobs/main.py",
    "apps/jobs/router.py",
    "apps/jobs/job.py",
    "apps/jobs/card_sync.py",
    "apps/jobs/market_sync.py",
    "apps/jobs/market_candle_sync.py",
    "apps/jobs/roster_update_sync.py",
    "apps/jobs/game_boxscore_sync.py",
    "apps/jobs/player_sync.py",
    "apps/jobs/prediction_sync.py",
    "shared/queue/queue.py",
    "shared/queue/redis_connector.py",
]

START_MARKER = "<!-- coverage:start -->"
END_MARKER = "<!-- coverage:end -->"


def _format_percent(rate: str) -> str:
    try:
        return f"{round(float(rate) * 100):d}%"
    except (TypeError, ValueError):
        return "n/a"


def _format_timestamp(ms: str) -> str:
    try:
        dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S %z")
    except (TypeError, ValueError, OSError):
        return "unknown"


def load_coverage(path: Path):
    tree = ET.parse(path)
    root = tree.getroot()

    totals = {
        "lines_valid": root.attrib.get("lines-valid", "0"),
        "lines_covered": root.attrib.get("lines-covered", "0"),
        "line_rate": root.attrib.get("line-rate", "0"),
        "timestamp": root.attrib.get("timestamp", ""),
    }

    file_rates = {}
    for cls in root.findall(".//class"):
        filename = cls.attrib.get("filename")
        if filename:
            file_rates[filename] = cls.attrib.get("line-rate", "0")

    return totals, file_rates


def build_block(totals, file_rates):
    total_pct = _format_percent(totals["line_rate"])
    lines_valid = totals["lines_valid"]
    lines_covered = totals["lines_covered"]
    timestamp = _format_timestamp(totals["timestamp"])

    lines = [
        f"Last run: {timestamp}",
        f"Totals: {total_pct} ({lines_covered}/{lines_valid} lines)",
        "Per-file:",
    ]

    for filename in FILES:
        rate = file_rates.get(filename)
        pct = _format_percent(rate) if rate is not None else "missing"
        lines.append(f"- `{filename}` {pct}")

    return "\n".join(lines)


def update_report(report_path: Path, block_text: str):
    if report_path.exists():
        content = report_path.read_text()
    else:
        content = "# Test Coverage Report\n\n"

    if START_MARKER in content and END_MARKER in content:
        before, rest = content.split(START_MARKER, 1)
        _, after = rest.split(END_MARKER, 1)
        new_content = f"{before}{START_MARKER}\n{block_text}\n{END_MARKER}{after}"
    else:
        new_content = f"{content}\n{START_MARKER}\n{block_text}\n{END_MARKER}\n"

    report_path.write_text(new_content)


def main() -> int:
    if not COVERAGE_XML.exists():
        print(f"Coverage XML not found: {COVERAGE_XML}", file=sys.stderr)
        return 1

    totals, file_rates = load_coverage(COVERAGE_XML)
    block_text = build_block(totals, file_rates)
    update_report(REPORT_PATH, block_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
