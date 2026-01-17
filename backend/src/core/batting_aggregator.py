from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple


SPLIT_VS_LHP = "vslhp"
SPLIT_VS_RHP = "vsrhp"
SPLIT_RISP = "risp"


def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()

def _pid(x: Any) -> Optional[int]:
    try:
        if not x:
            return None
        v = x.get("id") if isinstance(x, dict) else None
        return int(v) if v is not None else None
    except Exception:
        return None

def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def _base_code(v: Any) -> str:
    s = _norm(v).upper()
    if s in {"1B", "2B", "3B"}:
        return s
    if s in {"HOME", "H"}:
        return "HOME"
    return ""


def _is_pa_play(play: Dict[str, Any]) -> bool:
    res = play.get("result") or {}
    about = play.get("about") or {}
    matchup = play.get("matchup") or {}
    
    if _norm(res.get("type")) != "atbat":
        return False
    if not bool(about.get("isComplete", True)):
        return False
    
    batter = (matchup.get("batter") or {}).get("id")
    pitcher = (matchup.get("pitcher") or {}).get("id")
    if batter is None or pitcher is None:
        return False
    
    event_type = _norm(res.get("eventType"))
    
    non_pa_events = {
        "caught_stealing_2b", "caught_stealing_3b", "caught_stealing_home",
        "pickoff_1b", "pickoff_2b", "pickoff_3b", 
        "pickoff_caught_stealing_2b", "pickoff_caught_stealing_3b", "pickoff_caught_stealing_home",
        "stolen_base_2b", "stolen_base_3b", "stolen_base_home",
        "wild_pitch", "passed_ball", "balk", "other_advance",
        "runner_double_play", "pickoff_error_1b"
    }
    
    if event_type in non_pa_events:
        return False

    return True


def _pitcher_hand_split(play: Dict[str, Any]) -> Optional[str]:
    matchup = play.get("matchup") or {}
    hand = (matchup.get("pitchHand") or {}).get("code")
    hand = _norm(hand).upper()
    if hand == "L":
        return SPLIT_VS_LHP
    if hand == "R":
        return SPLIT_VS_RHP
    return None



def _post_base_state(play: Dict[str, Any], prev: Tuple[Optional[int], Optional[int], Optional[int]]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    matchup = play.get("matchup") or {}

    on1, on2, on3 = prev

    matchup = play.get("matchup") or {}
    on1 = _pid(matchup.get("postOnFirst"))
    on2 = _pid(matchup.get("postOnSecond"))
    on3 = _pid(matchup.get("postOnThird"))

    return on1, on2, on3


def _seed_bases_from_play_start(play: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    on1 = on2 = on3 = None

    matchup = play.get("matchup") or {}
    batter_id = (matchup.get("batter") or {}).get("id")
    runners = play.get("runners") or []
    runner_ids: Set[int] = set()
    for r in runners:
        details = r.get("details") or {}
        rid = _safe_int((details.get("runner") or {}).get("id"))
        if rid == 0:
            continue
        if batter_id is not None and rid == batter_id:
            continue
        runner_ids.add(rid)

        mv = r.get("movement") or {}
        start = _base_code(mv.get("start") or mv.get("originBase"))
        if start == "1B":
            on1 = rid
        elif start == "2B":
            on2 = rid
        elif start == "3B":
            on3 = rid

    # Extra-innings ghost runners can appear on 2B/3B without movement entries.
    post_on_second = _pid(matchup.get("postOnSecond"))
    post_on_third = _pid(matchup.get("postOnThird"))
    if on2 is None and post_on_second and post_on_second != batter_id and post_on_second not in runner_ids:
        on2 = post_on_second
    if on3 is None and post_on_third and post_on_third != batter_id and post_on_third not in runner_ids:
        on3 = post_on_third

    return on1, on2, on3


def _apply_runner_movement(
    bases: Tuple[Optional[int], Optional[int], Optional[int]],
    runner: Dict[str, Any],
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    on1, on2, on3 = bases

    details = runner.get("details") or {}
    rid = _safe_int(_get(details, "runner", "id", default=None), 0)
    if rid == 0:
        return on1, on2, on3

    mv = runner.get("movement") or {}
    end = _base_code(mv.get("end"))
    is_out = bool(mv.get("isOut", False))

    if on1 == rid:
        on1 = None
    if on2 == rid:
        on2 = None
    if on3 == rid:
        on3 = None

    if is_out:
        return on1, on2, on3

    if end == "1B":
        on1 = rid
    elif end == "2B":
        on2 = rid
    elif end == "3B":
        on3 = rid

    return on1, on2, on3


def _risp_on_last_pitch(
    play: Dict[str, Any],
    start_bases: Tuple[Optional[int], Optional[int], Optional[int]],
) -> bool:
    on1, on2, on3 = start_bases

    mv_by_idx: Dict[int, List[Dict[str, Any]]] = {}
    runners = play.get("runners") or []
    for r in runners:
        idx = _safe_int(_get(r, "details", "playIndex", default=None), -1)
        if idx >= 0:
            mv_by_idx.setdefault(idx, []).append(r)

    last_pitch_risp = (on2 is not None) or (on3 is not None)

    events = play.get("playEvents") or []
    for i, ev in enumerate(events):
        is_pitch = ev.get("isPitch")
        if is_pitch is None and ev.get("type") == "pitch":
            is_pitch = True

        if is_pitch:
            last_pitch_risp = (on2 is not None) or (on3 is not None)

        for r in mv_by_idx.get(i, []):
            on1, on2, on3 = _apply_runner_movement((on1, on2, on3), r)

    for idx in sorted(k for k in mv_by_idx.keys() if k >= len(events)):
        for r in mv_by_idx[idx]:
            on1, on2, on3 = _apply_runner_movement((on1, on2, on3), r)

    return bool(last_pitch_risp)


@dataclass
class BatLine:
    pa: int = 0
    r: int = 0
    h: int = 0
    doubles: int = 0
    triples: int = 0
    hr: int = 0
    hbp: int = 0
    tb: int = 0
    rbi: int = 0
    so: int = 0
    bb: int = 0
    intentional_walks: int = 0
    ab: int = 0
    flyOuts: int = 0
    groundOuts: int = 0
    airOuts: int = 0
    gidp: int = 0
    gitp: int = 0
    lob: int = 0
    sac_bunts: int = 0
    sac_flies: int = 0
    pop_outs: int = 0
    line_outs: int = 0

    def to_row(self, game_id: int, player_id: int, split: str) -> Dict[str, Any]:
        return {
            "game_id": int(game_id),
            "player_id": int(player_id),
            "split": split,
            "pa": self.pa,
            "r": self.r,
            "h": self.h,
            "doubles": self.doubles,
            "triples": self.triples,
            "hr": self.hr,
            "hbp": self.hbp,
            "tb": self.tb,
            "rbi": self.rbi,
            "so": self.so,
            "bb": self.bb,
            "intentional_walks": self.intentional_walks,
            "ab": self.ab,
            "flyOuts": self.flyOuts,
            "groundOuts": self.groundOuts,
            "airOuts": self.airOuts,
            "gidp": self.gidp,
            "gitp": self.gitp,
            "lob": self.lob,
            "sac_bunts": self.sac_bunts,
            "sac_flies": self.sac_flies,
            "pop_outs": self.pop_outs,
            "line_outs": self.line_outs,
        }


class MLBPlayByPlayBattingAggregator:
    def __init__(self):
        self._lines: Dict[Tuple[int, int, str], BatLine] = {}

    def build_rows(self, game_id: int, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._lines.clear()

        plays = payload.get("allPlays") or []
        plays = sorted(plays, key=lambda p: _safe_int((p.get("about") or {}).get("atBatIndex"), 0))
        on1: Optional[int] = None
        on2: Optional[int] = None
        on3: Optional[int] = None

        last_half_key: Optional[Tuple[int, bool]] = None

        for play in plays:
            about = play.get("about") or {}
            inning = _safe_int(about.get("inning"), -1)
            is_top = bool(about.get("isTopInning", False))
            half_key = (inning, is_top)

            if last_half_key is None:
                last_half_key = half_key
            elif half_key != last_half_key:
                on1 = on2 = on3 = None
                last_half_key = half_key
            if on1 is None and on2 is None and on3 is None:
                s1, s2, s3 = _seed_bases_from_play_start(play)
                if s1 is not None or s2 is not None or s3 is not None:
                    on1, on2, on3 = s1, s2, s3

            risp_start = (on2 is not None) or (on3 is not None)
            risp_last_pitch = _risp_on_last_pitch(play, (on1, on2, on3))

            if _is_pa_play(play):
                matchup = play.get("matchup") or {}
                batter_id = (matchup.get("batter") or {}).get("id")

                vs_split = _pitcher_hand_split(play)
                if vs_split is None or batter_id is None:
                    pass
                else:
                    self._apply_batter_stats(game_id, int(batter_id), vs_split, play)
                    self._apply_scoring(game_id, vs_split, play)

                    if risp_last_pitch:
                        self._apply_batter_stats(game_id, int(batter_id), SPLIT_RISP, play)
                        self._apply_scoring(game_id, SPLIT_RISP, play)

            if bool(about.get("isComplete", True)):
                on1, on2, on3 = _post_base_state(play, (on1, on2, on3))

        out: List[Dict[str, Any]] = []
        for (g, pid, split), line in self._lines.items():
            out.append(line.to_row(g, pid, split))
        return out

    def _line(self, game_id: int, player_id: int, split: str) -> BatLine:
        key = (int(game_id), int(player_id), split)
        if key not in self._lines:
            self._lines[key] = BatLine()
        return self._lines[key]

    def _apply_batter_stats(self, game_id: int, player_id: int, split: str, play: Dict[str, Any]) -> None:
        line = self._line(game_id, player_id, split)

        res = play.get("result") or {}
        event_type = str(res.get("eventType", "")).strip().lower()
        event_display = str(res.get("event", "")).strip()

        line.pa += 1
        line.rbi += _safe_int(res.get("rbi"), 0)

        if event_type in {"walk", "base_on_balls"}:
            line.bb += 1
        elif event_type in {"intent_walk", "intentional_walk"}:
            line.bb += 1
            line.intentional_walks += 1

        elif event_type in {"hit_by_pitch"}:
            line.hbp += 1

        elif event_type in {"single"}:
            line.h += 1
            line.tb += 1
        elif event_type in {"double"}:
            line.h += 1
            line.doubles += 1
            line.tb += 2
        elif event_type in {"triple"}:
            line.h += 1
            line.triples += 1
            line.tb += 3
        elif event_type in {"home_run", "homerun"}:
            line.h += 1
            line.hr += 1
            line.tb += 4
        
        if "strikeout" in event_type: 
            line.so += 1
        
        if "double_play" in event_type:
            line.gidp += 1
        if "triple_play" in event_type:
            line.gitp += 1

        if "Flyout" in event_display or "Sac Fly" in event_display:
            line.flyOuts += 1
            line.airOuts += 1
        elif "Lineout" in event_display or "Line Out" in event_display:
            line.line_outs += 1
            line.airOuts += 1
        elif "Pop Out" in event_display:
            line.pop_outs += 1
            line.airOuts += 1
        elif "Groundout" in event_display or "Forceout" in event_display or "Grounded Into" in event_display:
            line.groundOuts += 1

        is_sf = ("sac_fly" in event_type) or ("sacrifice_fly" in event_type)
        is_sh = ("sac_bunt" in event_type) or ("sacrifice_bunt" in event_type)

        if is_sf:
            line.sac_flies += 1
        elif is_sh:
            line.sac_bunts += 1

        is_walk = event_type in {"walk", "base_on_balls", "intent_walk", "intentional_walk"}
        is_hbp = event_type == "hit_by_pitch"
        is_ci = "catcher_interf" in event_type

        if not (is_walk or is_hbp or is_sf or is_sh or is_ci):
            line.ab += 1

        is_out = bool(res.get("isOut", False))
        
        if is_out or "fielders_choice" in event_type or "force_out" in event_type:
            bases_stranded = set()
            runners = play.get("runners") or []
            
            for r in runners:
                mv = r.get("movement") or {}
                details = r.get("details") or {}
                runner_id = (details.get("runner") or {}).get("id")

                if runner_id and int(runner_id) == int(player_id):
                    continue

                if bool(mv.get("isOut", False)):
                    continue
                    
                if bool(details.get("isScoringEvent", False)):
                    continue

                end = _norm(mv.get("end")).upper()
                if end in {"1B", "2B", "3B"}:
                    bases_stranded.add(end)

            line.lob += len(bases_stranded)

    def _apply_scoring(self, game_id: int, split: str, play: Dict[str, Any]) -> None:
        """
        Loops through all runners. If they scored, find THEIR stat line 
        (creating it if it doesn't exist) and add a Run.
        """
        runners = play.get("runners") or []
        for r in runners:
            details = r.get("details") or {}
            is_scorer = bool(details.get("isScoringEvent", False))
            
            if is_scorer:
                runner_id = (details.get("runner") or {}).get("id")
                if runner_id:
                    line = self._line(game_id, int(runner_id), split)
                    line.r += 1
