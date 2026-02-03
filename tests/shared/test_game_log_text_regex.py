import json
from pathlib import Path
from datetime import datetime

from shared.core.game_log_text_regex import GameLogTextRegexHandler

ROOT = Path(__file__).resolve().parents[2]
SOURCE_LOG_PATH = ROOT / "example-game-logs.json"
SNAPSHOT_DIR = ROOT / "tests" / "snapshots"
SNAPSHOT_PATH = SNAPSHOT_DIR / "game_log_text_regex_output.json"


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _get_game_entries(payload: dict) -> list:
    game_entries = payload.get("game", [])
    if not isinstance(game_entries, list):
        raise AssertionError(f"Expected 'game' to be a list in {SOURCE_LOG_PATH}")
    return game_entries


def _find_entry(payload: dict, key: str):
    for entry in _get_game_entries(payload):
        if isinstance(entry, list) and len(entry) >= 2 and entry[0] == key:
            return entry[1]
    return None


def _get_line_score(payload: dict) -> dict:
    line_score = _find_entry(payload, "line_score")
    if not isinstance(line_score, dict):
        raise AssertionError(f"'line_score' entry not found or not a dict in {SOURCE_LOG_PATH}")
    return line_score


def _get_game_log_text(payload: dict) -> str:
    game_log = _find_entry(payload, "game_log")
    if not isinstance(game_log, str):
        raise AssertionError(f"'game_log' entry not found or not a string in {SOURCE_LOG_PATH}")
    return game_log


def _write_snapshot(data: dict) -> None:
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    with SNAPSHOT_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True)


def test_write_regex_outputs_snapshot() -> None:
    payload = _load_json(SOURCE_LOG_PATH)
    line_score = _get_line_score(payload)
    game_log_text = _get_game_log_text(payload)

    home_full = (line_score.get("home_full_name") or "").strip()
    away_full = (line_score.get("away_full_name") or "").strip()
    if not home_full or not away_full:
        raise AssertionError("Missing home_full_name/away_full_name in line_score")

    home_key = home_full.lower()
    away_key = away_full.lower()

    handler = GameLogTextRegexHandler(game_log_text)

    difficulty = handler.extract_difficulty()
    ball_park_name, ball_park_elev = handler.extract_ball_park()
    weather_deg, weather_desc, weather_wind = handler.extract_weather()
    half_innings = handler.extract_half_innings(home_key, away_key)
    game_events = handler.extract_game_events(home_key, away_key)

    out = {
        "meta": {
            "source_file": str(SOURCE_LOG_PATH),
            "snapshot_file": str(SNAPSHOT_PATH),
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        },
        "keys": {
            "home_full_name": home_full,
            "away_full_name": away_full,
            "home_key_used": home_key,
            "away_key_used": away_key,
        },
        "difficulty": difficulty,
        "ball_park": {"name": ball_park_name, "elevation_ft": ball_park_elev},
        "weather": {"degrees": weather_deg, "description": weather_desc, "wind": weather_wind},
        "half_innings": half_innings,
        "game_events": game_events,
    }

    _write_snapshot(out)
    print(f"\nWrote snapshot: {SNAPSHOT_PATH}")

    # light sanity so pytest shows a failure if something is totally off
    assert isinstance(half_innings, list)
    assert isinstance(game_events, list)
