from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

SPLIT_VS_LHB = "vslhb"
SPLIT_VS_RHB = "vsrhb"
SPLIT_RISP = "risp"


def _norm(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip().lower()


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
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


def _ip_from_outs(outs: int) -> float:
    whole = outs // 3
    rem = outs % 3
    return float(whole) + (0.1 * rem)


def _base_code(v: Any) -> str:
    s = _norm(v).upper()
    if s in {"1B", "2B", "3B"}:
        return s
    if s in {"HOME", "H"}:
        return "HOME"
    return ""


def _pitcher_id(play: Dict[str, Any]) -> Optional[int]:
    matchup = play.get("matchup") or {}
    pitcher = matchup.get("pitcher") or {}
    pid = pitcher.get("id")
    try:
        return int(pid) if pid is not None else None
    except Exception:
        return None


def _batter_id(play: Dict[str, Any]) -> Optional[int]:
    matchup = play.get("matchup") or {}
    batter = matchup.get("batter") or {}
    bid = batter.get("id")
    try:
        return int(bid) if bid is not None else None
    except Exception:
        return None


def _batter_hand_split(play: Dict[str, Any]) -> str:
    matchup = play.get("matchup") or {}
    bat_side = matchup.get("batSide") or {}
    code = str(bat_side.get("code") or "").strip().upper()
    if code == "L":
        return SPLIT_VS_LHB
    if code == "R":
        return SPLIT_VS_RHB
    return SPLIT_VS_RHB


def _pid(x: Any) -> Optional[int]:
    try:
        if not x:
            return None
        v = x.get("id") if isinstance(x, dict) else None
        return int(v) if v is not None else None
    except Exception:
        return None


def _post_base_state(
    play: Dict[str, Any], prev: Tuple[Optional[int], Optional[int], Optional[int]]
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    matchup = play.get("matchup") or {}

    on1, on2, on3 = prev

    matchup = play.get("matchup") or {}
    on1 = _pid(matchup.get("postOnFirst"))
    on2 = _pid(matchup.get("postOnSecond"))
    on3 = _pid(matchup.get("postOnThird"))

    return on1, on2, on3


def _seed_bases_from_play_start(play: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    on1 = on2 = on3 = None

    batter_id = _batter_id(play)
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
    matchup = play.get("matchup") or {}
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


def _is_bf_play(play: Dict[str, Any]) -> bool:
    res = play.get("result") or {}
    if _norm(res.get("type")) != "atbat":
        return False
    about = play.get("about") or {}
    if not about.get("isComplete", True):
        return False
    event_type = _norm(res.get("eventType"))
    non_pa_events = {
        "caught_stealing_2b",
        "caught_stealing_3b",
        "caught_stealing_home",
        "pickoff_1b",
        "pickoff_2b",
        "pickoff_3b",
        "pickoff_caught_stealing_2b",
        "pickoff_caught_stealing_3b",
        "pickoff_caught_stealing_home",
        "stolen_base_2b",
        "stolen_base_3b",
        "stolen_base_home",
        "wild_pitch",
        "passed_ball",
        "balk",
        "other_advance",
        "runner_double_play",
        "pickoff_error_1b",
    }
    if event_type in non_pa_events:
        return False
    return _pitcher_id(play) is not None and _batter_id(play) is not None


def _analyze_events(play: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    pitches = 0
    balls = 0
    strikes = 0
    balks = 0
    wild_pitches = 0

    events = play.get("playEvents") or []
    for ev in events:
        details = ev.get("details") or {}

        is_pitch = ev.get("isPitch")
        if is_pitch is None and ev.get("type") == "pitch":
            is_pitch = True

        if is_pitch:
            pitches += 1
            if bool(details.get("isBall")):
                balls += 1
            else:
                strikes += 1

        et = _norm(details.get("eventType"))
        if "wild_pitch" in et:
            wild_pitches += 1
        elif "balk" in et:
            balks += 1

    return pitches, balls, strikes, balks, wild_pitches


@dataclass
class PitchLine:
    outs_pitched: int = 0
    ab: int = 0
    pitches_thrown: int = 0
    h: int = 0
    doubles: int = 0
    triples: int = 0
    hr: int = 0
    bb: int = 0
    k: int = 0
    intentional_walks: int = 0
    wins: int = 0
    losses: int = 0
    saves: int = 0
    save_opportunities: int = 0
    holds: int = 0
    blown_saves: int = 0
    r: int = 0
    er: int = 0
    batters_faced: int = 0
    balls_thrown: int = 0
    strikes_thrown: int = 0
    balks: int = 0
    wild_pitches: int = 0
    inherited_runners: int = 0
    inherited_runners_scored: int = 0

    def to_row(self, game_id: int, player_id: int, split: str) -> Dict[str, Any]:
        return {
            "game_id": int(game_id),
            "player_id": int(player_id),
            "split": split,
            "outs_pitched": int(self.outs_pitched),
            "ip": _ip_from_outs(int(self.outs_pitched)),
            "ab": int(self.ab),
            "pitches_thrown": int(self.pitches_thrown),
            "h": int(self.h),
            "doubles": int(self.doubles),
            "triples": int(self.triples),
            "hr": int(self.hr),
            "bb": int(self.bb),
            "k": int(self.k),
            "intentional_walks": int(self.intentional_walks),
            "wins": int(self.wins),
            "losses": int(self.losses),
            "saves": int(self.saves),
            "save_opportunities": int(self.save_opportunities),
            "holds": int(self.holds),
            "blown_saves": int(self.blown_saves),
            "r": int(self.r),
            "er": int(self.er),
            "batters_faced": int(self.batters_faced),
            "balls_thrown": int(self.balls_thrown),
            "strikes_thrown": int(self.strikes_thrown),
            "balks": int(self.balks),
            "wild_pitches": int(self.wild_pitches),
            "inherited_runners": int(self.inherited_runners),
            "inherited_runners_scored": int(self.inherited_runners_scored),
        }


class MLBPlayByPlayPitchingAggregator:
    def __init__(self):
        self._lines: Dict[Tuple[int, int, str], PitchLine] = {}
        self._runner_splits: Dict[int, str] = {}
        self._current_pitcher_id: Optional[int] = None
        self._runners_on_base_ids: Set[int] = set()

    def build_rows(self, game_id: int, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._lines.clear()
        self._runner_splits.clear()
        self._runners_on_base_ids.clear()
        self._current_pitcher_id = None

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

            pid = _pitcher_id(play)
            if pid is None:
                continue

            if last_half_key is None:
                last_half_key = half_key
            elif half_key != last_half_key:
                on1 = on2 = on3 = None
                self._runners_on_base_ids.clear()
                last_half_key = half_key

            if on1 is None and on2 is None and on3 is None:
                s1, s2, s3 = _seed_bases_from_play_start(play)
                if s1 is not None or s2 is not None or s3 is not None:
                    on1, on2, on3 = s1, s2, s3
                    self._runners_on_base_ids = {x for x in (on1, on2, on3) if x is not None}

            risp_start = (on2 is not None) or (on3 is not None)
            risp_last_pitch = _risp_on_last_pitch(play, (on1, on2, on3))

            if self._current_pitcher_id is None:
                self._current_pitcher_id = pid
            elif pid != self._current_pitcher_id:
                inherited_count = len(self._runners_on_base_ids)
                if inherited_count > 0:
                    split_guess = _batter_hand_split(play)
                    self._line(game_id, pid, split_guess).inherited_runners += inherited_count
                    self._line(game_id, pid, SPLIT_RISP).inherited_runners += inherited_count if risp_start else 0
                self._current_pitcher_id = pid

            if _is_bf_play(play):
                self._process_bf_play(game_id, play, risp_last_pitch)
            else:
                self._process_non_bf_play(game_id, play, risp_last_pitch)

            # Update bases for next play (use postOn keys when present; do not require isComplete)
            on1, on2, on3 = _post_base_state(play, (on1, on2, on3))
            self._runners_on_base_ids = {x for x in (on1, on2, on3) if x is not None}

            # Ensure any new runners (pinch runners/subs) have a split context
            split_guess = _batter_hand_split(play)
            for rid in self._runners_on_base_ids:
                if rid not in self._runner_splits:
                    self._runner_splits[rid] = split_guess

        out: List[Dict[str, Any]] = []
        for (g, p_id, split), line in self._lines.items():
            if (
                line.batters_faced > 0
                or line.outs_pitched > 0
                or line.pitches_thrown > 0
                or line.r > 0
                or line.er > 0
                or line.inherited_runners > 0
                or line.inherited_runners_scored > 0
            ):
                out.append(line.to_row(g, p_id, split))
        return out

    def _line(self, game_id: int, pitcher_id: int, split: str) -> PitchLine:
        key = (int(game_id), int(pitcher_id), split)
        if key not in self._lines:
            self._lines[key] = PitchLine()
        return self._lines[key]

    def _process_bf_play(self, game_id: int, play: Dict[str, Any], risp_start: bool) -> None:
        pitcher_id = _pitcher_id(play)
        batter_id = _batter_id(play)
        if pitcher_id is None or batter_id is None:
            return

        vs_split = _batter_hand_split(play)

        # Batter may become a runner; store split context for later run attribution
        self._runner_splits[batter_id] = vs_split

        self._apply_pa_stats(game_id, pitcher_id, vs_split, play)
        if risp_start:
            self._apply_pa_stats(game_id, pitcher_id, SPLIT_RISP, play)

        self._handle_runners_scoring(game_id, play, risp_start)

    def _process_non_bf_play(self, game_id: int, play: Dict[str, Any], risp_start: bool) -> None:
        pid = _pitcher_id(play)
        if pid is None:
            return

        vs_split = _batter_hand_split(play)

        pitches, balls, strikes, balks, wild_pitches = _analyze_events(play)
        line = self._line(game_id, pid, vs_split)
        line.pitches_thrown += pitches
        line.balls_thrown += balls
        line.strikes_thrown += strikes
        line.balks += balks
        line.wild_pitches += wild_pitches

        if risp_start:
            rline = self._line(game_id, pid, SPLIT_RISP)
            rline.pitches_thrown += pitches
            rline.balls_thrown += balls
            rline.strikes_thrown += strikes
            rline.balks += balks
            rline.wild_pitches += wild_pitches

        self._handle_runners_scoring(game_id, play, risp_start)

    def _apply_pa_stats(self, game_id: int, pitcher_id: int, split: str, play: Dict[str, Any]) -> None:
        line = self._line(game_id, pitcher_id, split)
        line.batters_faced += 1

        pitches, balls, strikes, balks, wild_pitches = _analyze_events(play)
        line.pitches_thrown += pitches
        line.balls_thrown += balls
        line.strikes_thrown += strikes
        line.balks += balks
        line.wild_pitches += wild_pitches

        outs_on_play = 0
        runners = play.get("runners") or []
        for r in runners:
            mv = r.get("movement") or {}
            if bool(mv.get("isOut", False)):
                outs_on_play += 1

        res = play.get("result") or {}
        if outs_on_play == 0 and bool(res.get("isOut", False)):
            outs_on_play = 1

        line.outs_pitched += outs_on_play

        et = _norm(res.get("eventType"))
        is_walk = et in {"walk", "base_on_balls", "intent_walk", "intentional_walk"}
        is_hbp = et == "hit_by_pitch"
        is_sf = "sac_fly" in et or "sacrifice_fly" in et
        is_sh = "sac_bunt" in et or "sacrifice_bunt" in et
        is_ci = "catcher_interf" in et

        if is_walk:
            line.bb += 1
            if "intent" in et:
                line.intentional_walks += 1

        if "strikeout" in et:
            line.k += 1

        if et == "single":
            line.h += 1
        elif et == "double":
            line.h += 1
            line.doubles += 1
        elif et == "triple":
            line.h += 1
            line.triples += 1
        elif "home_run" in et:
            line.h += 1
            line.hr += 1

        if not (is_walk or is_hbp or is_sf or is_sh or is_ci):
            line.ab += 1

    def _handle_runners_scoring(self, game_id: int, play: Dict[str, Any], risp_start: bool) -> None:
        """
        Charges R/ER to the *responsible pitcher* using the API.
        Also mirrors those run charges into the responsible pitcher's RISP split
        iff the PA started with RISP (risp_start=True).
        """
        current_pitcher_id = _pitcher_id(play)
        if current_pitcher_id is None:
            return

        runners = play.get("runners") or []
        for r in runners:
            details = r.get("details") or {}
            if not bool(details.get("isScoringEvent", False)):
                continue

            resp_obj = details.get("responsiblePitcher") or {}
            resp_pid = _safe_int(resp_obj.get("id"))
            if resp_pid == 0:
                resp_pid = current_pitcher_id

            runner_id = _safe_int((details.get("runner") or {}).get("id"))
            split = self._runner_splits.get(runner_id, SPLIT_VS_RHB)

            earned = bool(details.get("earned", False))

            line = self._line(game_id, resp_pid, split)
            line.r += 1
            if earned:
                line.er += 1

            if risp_start:
                rline = self._line(game_id, resp_pid, SPLIT_RISP)
                rline.r += 1
                if earned:
                    rline.er += 1

            if resp_pid != current_pitcher_id:
                curr_split = _batter_hand_split(play)
                self._line(game_id, current_pitcher_id, curr_split).inherited_runners_scored += 1
                if risp_start:
                    self._line(game_id, current_pitcher_id, SPLIT_RISP).inherited_runners_scored += 1
