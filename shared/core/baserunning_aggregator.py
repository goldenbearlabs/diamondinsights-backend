from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()


@dataclass
class BaserunningLine:
    sb: int = 0
    caught_stealing: int = 0

    def to_row(self, game_id: int, player_id: int) -> Dict[str, Any]:
        return {
            "game_id": int(game_id),
            "player_id": int(player_id),
            "sb": self.sb,
            "caught_stealing": self.caught_stealing,
        }


class MLBPlayByPlayBaserunningAggregator:
    def __init__(self):
        self._lines: Dict[Tuple[int, int], BaserunningLine] = {}

    def build_rows(self, game_id: int, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._lines.clear()

        plays = payload.get("allPlays") or []
        for play in plays:
            self._process_play(game_id, play)

        out: List[Dict[str, Any]] = []
        for (g, pid), line in self._lines.items():
            if line.sb > 0 or line.caught_stealing > 0:
                out.append(line.to_row(g, pid))
        return out

    def _line(self, game_id: int, player_id: int) -> BaserunningLine:
        key = (int(game_id), int(player_id))
        if key not in self._lines:
            self._lines[key] = BaserunningLine()
        return self._lines[key]

    def _process_play(self, game_id: int, play: Dict[str, Any]) -> None:
        runners = play.get("runners") or []
        
        for r in runners:
            details = r.get("details") or {}
            mv = r.get("movement") or {}
            
            runner_data = details.get("runner") or {}
            runner_id = runner_data.get("id")
            
            if runner_id is None:
                continue

            event_type = _norm(details.get("eventType"))
            
            if "stolen_base" in event_type:
                line = self._line(game_id, int(runner_id))
                line.sb += 1

            elif "caught_stealing" in event_type:
                line = self._line(game_id, int(runner_id))
                line.caught_stealing += 1