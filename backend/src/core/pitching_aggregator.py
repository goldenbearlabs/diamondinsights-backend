from __future__ import annotations

from dataclasses import dataclass, field
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
    return SPLIT_VS_RHB  # Default fallback


def _is_risp_start(play: Dict[str, Any]) -> bool:
    """
    Determines if the play started with runners in scoring position.
    Checks runner start positions first to catch HRs clearing the bases.
    """
    runners = play.get("runners") or []
    for r in runners:
        mv = r.get("movement") or {}
        start_pos = str(mv.get("start") or "").strip().upper()
        if start_pos in {"2B", "3B"}:
            return True

    matchup = play.get("matchup") or {}
    splits = matchup.get("splits") or {}
    men_on = _norm(splits.get("menOnBase"))
    
    if men_on in {"risp", "loaded"}:
        return True

    return False


def _event_type(play: Dict[str, Any]) -> str:
    res = play.get("result") or {}
    return _norm(res.get("eventType"))


def _is_bf_play(play: Dict[str, Any]) -> bool:
    res = play.get("result") or {}
    if _norm(res.get("type")) != "atbat":
        return False
    # If the play is complete (result recorded) and has pitcher/batter, it counts as a BF event
    about = play.get("about") or {}
    if not about.get("isComplete", True):
        return False
    return _pitcher_id(play) is not None and _batter_id(play) is not None


def _analyze_events(play: Dict[str, Any]) -> Tuple[int, int, int, int, int]:
    """
    Returns: (pitches, balls, strikes, balks, wild_pitches)
    """
    pitches = 0
    balls = 0
    strikes = 0
    balks = 0
    wild_pitches = 0

    events = play.get("playEvents") or []
    for ev in events:
        details = ev.get("details") or {}
        
        is_pitch = ev.get("isPitch")
        if is_pitch is None:
            if ev.get("type") == "pitch":
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
        # Stores ONLY the split context for a runner. 
        # API handles "Responsible Pitcher", we just need to know "Was this runner put on vsLHB or vsRHB?"
        self._runner_splits: Dict[int, str] = {} 
        self._current_pitcher_id: Optional[int] = None
        
        # Track runners currently on base to calculate "Inherited Runners" count
        self._runners_on_base_ids: Set[int] = set()

    def build_rows(self, game_id: int, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        self._lines.clear()
        self._runner_splits.clear()
        self._runners_on_base_ids.clear()
        self._current_pitcher_id = None

        plays = payload.get("allPlays") or []
        for play in plays:
            pid = _pitcher_id(play)
            if pid is None:
                continue
            
            # --- 1. Handle Pitching Change & Inherited Runners Count ---
            if self._current_pitcher_id is None:
                self._current_pitcher_id = pid
            elif pid != self._current_pitcher_id:
                # Pitcher changed. The new pitcher inherits everyone currently on base.
                inherited_count = len(self._runners_on_base_ids)
                if inherited_count > 0:
                    # We attribute the "Inherited Runners" count to the new pitcher's "Total" context
                    # Since we don't know the batter split yet, we can't perfectly assign it to vslhb/vsrhb.
                    # Ideally, we assign it to the split of the *first batter they face*.
                    # For simplicity, we'll try to guess based on the current batter.
                    split_guess = _batter_hand_split(play)
                    line = self._line(game_id, pid, split_guess)
                    line.inherited_runners += inherited_count
                self._current_pitcher_id = pid

            # --- 2. Process Play Stats ---
            if _is_bf_play(play):
                self._process_bf_play(game_id, play)
            else:
                self._process_non_bf_play(game_id, play)

            # --- 3. Update Runner State for Next Play ---
            # We must look at the "runners" list (which shows the result state) 
            # to know who is left on base for the *next* play.
            self._update_on_base_state(play)

        out: List[Dict[str, Any]] = []
        for (g, p_id, split), line in self._lines.items():
            if (
                line.batters_faced > 0 or line.outs_pitched > 0 or 
                line.pitches_thrown > 0 or line.r > 0 or line.er > 0 or 
                line.inherited_runners > 0 or line.inherited_runners_scored > 0
            ):
                out.append(line.to_row(g, p_id, split))
        return out

    def _line(self, game_id: int, pitcher_id: int, split: str) -> PitchLine:
        key = (int(game_id), int(pitcher_id), split)
        if key not in self._lines:
            self._lines[key] = PitchLine()
        return self._lines[key]

    def _update_on_base_state(self, play: Dict[str, Any]) -> None:
        """
        Updates self._runners_on_base_ids based on who ends up on base after the play.
        """
        self._runners_on_base_ids.clear()
        
        runners = play.get("runners") or []
        for r in runners:
            mv = r.get("movement") or {}
            end = _base_code(mv.get("end"))
            is_out = bool(mv.get("isOut"))
            
            if not is_out and end in {"1B", "2B", "3B"}:
                details = r.get("details") or {}
                runner_obj = details.get("runner") or {}
                rid = runner_obj.get("id")
                if rid:
                    self._runners_on_base_ids.add(int(rid))

    def _process_bf_play(self, game_id: int, play: Dict[str, Any]) -> None:
        pitcher_id = _pitcher_id(play)
        batter_id = _batter_id(play)
        if pitcher_id is None or batter_id is None:
            return

        vs_split = _batter_hand_split(play)
        risp = _is_risp_start(play)

        # Record the split for this batter (who might become a runner)
        self._runner_splits[batter_id] = vs_split

        # Apply Batters Faced stats (K, BB, H, etc)
        self._apply_pa_stats(game_id, pitcher_id, vs_split, play)
        if risp:
            self._apply_pa_stats(game_id, pitcher_id, SPLIT_RISP, play)

        # Handle Runners (Movement & Scoring) - ONLY CALL ONCE to avoid double counting
        self._handle_runners_scoring(game_id, pitcher_id, play)

    def _process_non_bf_play(self, game_id: int, play: Dict[str, Any]) -> None:
        pid = _pitcher_id(play)
        if pid is None:
            return
        
        # Default to RHB if unknown, or try to infer
        vs_split = _batter_hand_split(play) 

        # Add pure pitch counts / balks
        pitches, balls, strikes, balks, wild_pitches = _analyze_events(play)
        line = self._line(game_id, pid, vs_split)
        line.pitches_thrown += pitches
        line.balls_thrown += balls
        line.strikes_thrown += strikes
        line.balks += balks
        line.wild_pitches += wild_pitches

        self._handle_runners_scoring(game_id, pid, play)

    def _apply_pa_stats(self, game_id: int, pitcher_id: int, split: str, play: Dict[str, Any]) -> None:
        line = self._line(game_id, pitcher_id, split)
        line.batters_faced += 1

        pitches, balls, strikes, balks, wild_pitches = _analyze_events(play)
        line.pitches_thrown += pitches
        line.balls_thrown += balls
        line.strikes_thrown += strikes
        line.balks += balks
        line.wild_pitches += wild_pitches

        # Outs
        outs_on_play = 0
        runners = play.get("runners") or []
        for r in runners:
            mv = r.get("movement") or {}
            if bool(mv.get("isOut", False)):
                outs_on_play += 1
        
        res = play.get("result") or {}
        if outs_on_play == 0 and bool(res.get("isOut", False)):
            outs_on_play = 1 # Batter is out
        
        line.outs_pitched += outs_on_play

        # Stats
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

    def _handle_runners_scoring(self, game_id: int, current_pitcher_id: int, play: Dict[str, Any]) -> None:
        """
        Iterates all runners in the play. If they scored, use the API's 'responsiblePitcher' 
        to attribute the run. This completely avoids double-counting or wrong attribution logic.
        """
        runners = play.get("runners") or []
        
        for r in runners:
            details = r.get("details") or {}
            is_scoring = bool(details.get("isScoringEvent", False))
            
            if is_scoring:
                # 1. Who is responsible? (Trust the API)
                resp_obj = details.get("responsiblePitcher") or {}
                resp_pid = _safe_int(resp_obj.get("id"))
                
                # If API doesn't provide it (rare), fall back to current pitcher
                if resp_pid == 0:
                    resp_pid = current_pitcher_id

                # 2. What split? (Use our cache, or default to vsRHB if unknown)
                runner_id = _safe_int((details.get("runner") or {}).get("id"))
                split = self._runner_splits.get(runner_id, SPLIT_VS_RHB)

                # 3. Charge the stats
                line = self._line(game_id, resp_pid, split)
                line.r += 1
                if bool(details.get("earned", False)):
                    line.er += 1
                
                # 4. Handle Inherited Runner Scored
                if resp_pid != current_pitcher_id:
                    # If the responsible pitcher is NOT the current pitcher, 
                    # it means the current pitcher let an inherited runner score.
                    # We charge 'inherited_runners_scored' to the CURRENT pitcher.
                    # But which split? Probably the one matching the current batter context or RISP?
                    # We'll use the current batter's hand to keep it simple.
                    curr_split = _batter_hand_split(play)
                    curr_line = self._line(game_id, current_pitcher_id, curr_split)
                    curr_line.inherited_runners_scored += 1