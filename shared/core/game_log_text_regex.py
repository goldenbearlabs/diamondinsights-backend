import re
from typing import Optional, Tuple
from typing import Optional, List, Dict, Any

_FIELDER_NUM_TO_POS = {
    "1": "P",
    "2": "C",
    "3": "1B",
    "4": "2B",
    "5": "3B",
    "6": "SS",
    "7": "LF",
    "8": "CF",
    "9": "RF",
}

class GameLogTextRegexHandler:

    def __init__(self, text: str):
        self.text = text
        self._pp_cache: Optional[List[Dict[str, Any]]] = None

    def extract_difficulty(self) -> Optional[str]:
        if not self.text:
            return None
        match = re.search(r"Hitting Difficulty is ([^.]+)\.", self.text, re.IGNORECASE)
        if not match:
            match = re.search(r"Pitching Difficulty is ([^.]+)\.", self.text, re.IGNORECASE)
        if not match:
            return None
        return match.group(1).strip() or None 
    
    def extract_ball_park(self) -> Tuple[Optional[str], Optional[int]]:
        '''
            Performs the regex search lookup on the game log string to find ballpark
            params:
                game_log -> the current game_log object
            returns:
                name, elevation
        '''
        if not self.text:
            return None, None

        match = re.search(
            r"\^n\^(?P<name>[^^\n]+?)(?:\^e\^)?\s*\((?P<elev>\d+)\s*ft elevation\)",
            self.text,
            re.IGNORECASE,
        )
        if not match:
            return None, None

        name = match.group("name").strip()
        elevation = int(match.group("elev"))
        return name or None, elevation or None

    def extract_weather(self) -> Tuple[Optional[int], Optional[str], Optional[str]]:
        if not self.text:
            return None, None, None

        degrees = None
        description = None
        weather_match = re.search(r"Weather:\s*(\d+)\s*degrees,\s*([^^\n]+)", self.text, re.IGNORECASE)
        if weather_match:
            degrees = int(weather_match.group(1))
            description = weather_match.group(2).strip()

        wind = None
        wind_match = re.search(r"\^n\^(?P<wind>[^\^\n]*Wind[^\^\n]*)\^n\^", self.text, re.IGNORECASE)
        if wind_match:
            wind = wind_match.group("wind").strip()

        return degrees, description, wind
    
    def extract_half_innings(self, home_key: str, away_key: str) -> list[dict]:
        if not self.text:
            return []

        inning_split = re.split(r"\^c51\^Inning\s+(\d+):\^c50\^", self.text)
        if len(inning_split) < 3:
            return []

        results_by_key: dict[tuple[int, bool], dict] = {}
        for idx in range(1, len(inning_split), 2):
            inning_raw = inning_split[idx]
            inning_text = inning_split[idx + 1] if idx + 1 < len(inning_split) else ""
            inning_num = int(inning_raw)
            if inning_num is None:
                continue

            batting_teams = [
                match.group("team").strip()
                for match in re.finditer(r"\^n\^(?P<team>[^^\n]+?) batting\.", inning_text)
            ]
            summaries = list(
                re.finditer(
                    r"Runs:\s*(\d+)\s*Hits:\s*(\d+)\s*Walks:\s*(\d+)\s*Errors:\s*(\d+)\s*Pitches:\s*(\d+)\s*Runners Left On:\s*(\d+)",
                    inning_text,
                )
            )
            for half_idx, summary in enumerate(summaries):
                is_home = None
                if half_idx < len(batting_teams):
                    team_key = batting_teams[half_idx].lower()
                    if team_key == home_key:
                        is_home = True
                    elif team_key == away_key:
                        is_home = False
                if is_home is None:
                    is_home = half_idx % 2 == 1

                data = {
                    "inning": inning_num,
                    "is_home_batting": is_home,
                    "runs": int(summary.group(1)),
                    "hits": int(summary.group(2)),
                    "walks": int(summary.group(3)),
                    "errors": int(summary.group(4)),
                    "pitches": int(summary.group(5)),
                    "runners_left_on": int(summary.group(6)),
                }
                results_by_key[(inning_num, is_home)] = data

        return list(results_by_key.values())
    
    def extract_game_events(self, home_key: str, away_key: str) -> list[dict]:
        if not self.text:
            return []

        inning_split = re.split(r"\^c51\^Inning\s+(\d+):\^c50\^", self.text)
        if len(inning_split) < 3:
            return []

        events: list[dict] = []
        last_event = None
        last_half_key = None

        for idx in range(1, len(inning_split), 2):
            inning_raw = inning_split[idx]
            inning_text = inning_split[idx + 1] if idx + 1 < len(inning_split) else ""
            inning_num = int(inning_raw)

            half_matches = list(
                re.finditer(
                    r"\^n\^(?P<team>[^^\n]+?) batting\.(?P<rest>.*?)(?=\^n\^Runs:)",
                    inning_text,
                    re.S,
                )
            )

            for half_idx, match in enumerate(half_matches):
                team = match.group("team").strip()
                rest = match.group("rest") or ""

                team_key = team.lower()
                if team_key == home_key:
                    is_home = True
                elif team_key == away_key:
                    is_home = False
                else:
                    is_home = (half_idx % 2 == 1)

                cleaned = self._strip_log_markers(rest)
                half_key = (inning_num, is_home)

                for sentence in self._split_log_sentences(cleaned):
                    if not sentence:
                        continue

                    event_type = self._event_type_from_text(sentence)

                    if last_event is not None and last_half_key == half_key and self._is_followup_out(sentence):
                        last_event["event_text"] = f"{last_event['event_text']} {sentence}"

                        prev_text = (last_event["event_text"] or "").lower()
                        if "double play" not in prev_text and "triple play" not in prev_text:
                            last_event["outs_delta"] += self._outs_delta(sentence)

                        last_event["runs_delta"] += self._runs_delta(sentence)
                        continue

                    if event_type == "advance" and last_event is not None and last_half_key == half_key:
                        last_event["event_text"] = f"{last_event['event_text']} {sentence}"
                        last_event["outs_delta"] += self._outs_delta(sentence)
                        last_event["runs_delta"] += self._runs_delta(sentence)
                        continue

                    new_event = {
                        "inning": inning_num,
                        "is_home_batting": is_home,
                        "event_text": sentence,
                        "event_type": event_type,
                        "outs_delta": self._outs_delta(sentence),
                        "runs_delta": self._runs_delta(sentence),
                    }
                    events.append(new_event)
                    last_event = new_event
                    last_half_key = half_key

        return events
    
    def extract_batter_name(self, event_text: str) -> Optional[str]:
        if not event_text:
            return None
        s = re.sub(r"^\*\s*", "", event_text.strip())
        if not s:
            return None
        # "Happ struck out...", "Smith grounded...", "Aaron grounded into..."
        m = re.match(r"^(?P<name>[A-Za-z][A-Za-z .'\-]*?)\s+(?:was|walked|struck|grounded|flied|lined|popped|fouled|singled|doubled|tripled|homered|reached)\b", s, re.IGNORECASE)
        if m:
            return m.group("name").strip() or None
        # fallback: first token
        first = s.split(" ", 1)[0].strip(" .")
        return first or None
    
    def extract_pitcher_name(self, event_text: str) -> Optional[str]:
        if not event_text:
            return None
        m = re.match(r"^\s*(?P<name>.+?)\s+pitching\.\s*$", event_text.strip(), re.IGNORECASE)
        return m.group("name").strip() if m else None
    
    def _strip_log_markers(self, text: str) -> str:
        if not text:
            return ""
        cleaned = re.sub(r"\^c\d+\^", " ", text)
        cleaned = cleaned.replace("^n^", " ").replace("^e^", " ")
        cleaned = re.sub(r"(?<=\.)\*\s*", " ", cleaned)
        return " ".join(cleaned.split())

    def _split_log_sentences(self, text: str) -> list[str]:
        if not text:
            return []
        safe = text.replace("Jr.", "Jr<dot>").replace("Sr.", "Sr<dot>")
        parts = [p.strip() for p in re.split(r"\.(?:\s+|\*\s*)", safe) if p.strip()]

        out: list[str] = []
        for part in parts:
            part = part.replace("Jr<dot>", "Jr.").replace("Sr<dot>", "Sr.")
            part = re.sub(r"^\*\s*", "", part).strip()
            if not part:
                continue
            out.append(part.rstrip(".") + ".")
        return out
    
    def _outs_delta(self, event_text: str) -> int:
        text = (event_text or "").lower()
        if "triple play" in text:
            return 3
        if "double play" in text:
            return 2
        out_phrases = (
            "struck out",
            "flied out",
            "grounded out",
            "popped out",
            "lined out",
            "fouled out",
            "was called out",
            "out at",
            "out while",
            "thrown out",
            "picked off",
            "caught stealing",
            "out on",
        )
        if any(phrase in text for phrase in out_phrases):
            return 1
        if re.search(r"\bout\.$", text):
            return 1
        return 0

    def _runs_delta(self, event_text: str) -> int:
        text = (event_text or "").lower()
        runs = len(re.findall(r"\bscored\b|\bscores\b", text))
        if "homered" in text:
            runs += 1
        return runs
    
    def _event_type_from_text(self, event_text: str) -> str:
        text = (event_text or "").lower()

        if "pinch hit" in text:
            return "pinch_hit"
        if "pinch ran" in text:
            return "pinch_run"

        if re.search(r"\bpitching\.$", text):
            return "pitching_change"
        if "caught stealing" in text:
            return "caught_stealing"
        if re.search(r"\bstole\b|\bstolen\b|\bsteals\b", text):
            return "steal"

        if self._is_advancement_only(text):
            return "advance"

        if self._is_plate_appearance(text):
            return "pa"

        return "play"
    
    def _is_advancement_only(self, text: str) -> bool:
        if "advances to" in text or "scores" in text or "scored" in text:
            if self._is_plate_appearance(text):
                return False
            if "stole" in text or "caught stealing" in text or "steals" in text:
                return False
            return True
        return False

    def _is_followup_out(self, sentence: str) -> bool:
        s = (sentence or "").strip()
        return bool(re.match(r"^[A-Za-z][A-Za-z .'\-]*\sout\.$", s))
    
    def _is_plate_appearance(self, text: str) -> bool:
        patterns = (
            "struck out",
            "called out on strikes",
            "was called out",
            "walked",
            "walks",
            "intentionally walked",
            "hit by pitch",
            "grounded out",
            "flied out",
            "popped out",
            "lined out",
            "fouled out",
            "singled",
            "doubled",
            "tripled",
            "homered",
            "reached on error",
            "reached on a fielder",
            "reached on fielder",
            "sacrifice",
            "sac flied",
            "sac fly",
            "grounded to",
            "lined to",
            "flied to",
            "popped to",
            "hit a",
            "double play",
            "triple play",
            "grounded into a double play",
            "flied into a double play",
            "lined into a double play",
            "popped into a double play",
        )
        return any(p in text for p in patterns)
    
    def _extract_putout_code(self, text: str) -> tuple[Optional[str], Optional[str]]:
        if not text:
            return None, None

        candidates = [m.group(1).strip() for m in re.finditer(r"\(([^()]+)\)", text)]
        if not candidates:
            return None, None

        def _skip(c: str) -> bool:
            return bool(re.search(r"\bfeet\b|\bmph\b|\bground rule\b", c, re.IGNORECASE))

        def _looks_like_putout(c: str) -> bool:
            if _skip(c):
                return False
            c2 = c.strip()
            if re.fullmatch(r"FC", c2, re.IGNORECASE):
                return True
            if re.fullmatch(r"P[1-9]", c2, re.IGNORECASE):
                return True
            if re.fullmatch(r"F[1-9]", c2, re.IGNORECASE):
                return True
            if re.fullmatch(r"[1-9](?:-[1-9]){1,3}(?:\s*DP)?", c2, re.IGNORECASE):
                return True
            if "DP" in c2.upper():
                return True
            return False

        code = next((c for c in candidates if _looks_like_putout(c)), None)
        if not code:
            return None, None

        m = re.search(r"\bF([1-9])\b", code, re.IGNORECASE)
        if m:
            return code, _FIELDER_NUM_TO_POS.get(m.group(1))

        m = re.search(r"\bP([1-9])\b", code, re.IGNORECASE)
        if m:
            return code, _FIELDER_NUM_TO_POS.get(m.group(1))

        m = re.search(r"\b([1-9])\b", code)
        if m:
            return code, _FIELDER_NUM_TO_POS.get(m.group(1))

        return code, None
    
    def extract_pitcher_game_scores(self) -> list[dict]:
        if not self.text:
            return []

        m = re.search(
            r"Game Scores:\s*(?P<rest>.+?)(?:\^n\^|\n|UMPIRES\b|$)",
            self.text,
            re.IGNORECASE | re.S,
        )
        if not m:
            return []

        rest = self._strip_log_markers(m.group("rest") or "")
        pairs = re.findall(r"(\d+)\s*\(([^)]+)\)", rest)

        out: list[dict] = []
        for i, (score_str, name) in enumerate(pairs):
            if i > 1:
                break
            out.append(
                {
                    "pitcher_name_raw": (name or "").strip(),
                    "game_score": int(score_str),
                    "is_home": (i == 1),
                }
            )
        return out

    def _extract_batted_ball_type(self, text: str) -> Optional[str]:
        t = (text or "").lower()

        if any(x in t for x in ("lined ", "lined out", "lined to")):
            return "line"
        if any(x in t for x in ("grounded ", "grounded out", "grounded to", "chopped ")):
            return "ground"
        if any(x in t for x in ("flied ", "flied out", "flew out", "flied to")):
            return "fly"
        if any(x in t for x in ("popped ", "popped out", "pop out", "popped to")):
            return "popup"
        return None

    def _extract_hit_direction(self, text: str) -> Optional[str]:
        t = (text or "").lower()
        m = re.search(r"\bto\s+(left|center|right)\b", t)
        return m.group(1) if m else None
    
    def _extract_hr_distance_ft(self, text: str) -> Optional[int]:
        if not text:
            return None
        m = re.search(r"\((\d+)\s*feet\)", text, re.IGNORECASE)
        return int(m.group(1)) if m else None
    
    def extract_runs_scored(self, event_text: str) -> int:
        if not event_text:
            return 0
        t = event_text.lower()
        runs = len(re.findall(r"\b(?:scores|scored)\b", t))
        if "homered" in t:
            runs += 1
        return runs

    def extract_rbi(self, event_text: str) -> int:
        if not event_text:
            return 0
        t = event_text.lower()
        rbi = len(re.findall(r"\b(?:scores|scored)\b", t))
        if "homered" in t:
            rbi += 1
        return rbi
    
    def extract_perfect_perfect_hits(self) -> list[dict]:
        if self._pp_cache is not None:
            return self._pp_cache

        if not self.text:
            self._pp_cache = []
            return self._pp_cache

        start = re.search(r"Perfect Contact Hits\s*\(Perfect-Perfect\)", self.text, re.IGNORECASE)
        if not start:
            self._pp_cache = []
            return self._pp_cache

        tail = self.text[start.end():]
        end = re.search(r"\^n\^\s*(?:UMPIRES|Weather:|\w[^\^]*\(\d+\s*ft elevation\))", tail, re.IGNORECASE)
        block = tail[: end.start()] if end else tail

        block = re.sub(r"\^c\d+\^", " ", block)
        block = block.replace("^n^", "\n").replace("^e^", "\n")
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]

        items: list[dict] = []
        for ln in lines:
            m = re.match(r"^(?P<player>.+?):\s*(?P<mph>\d+)\s*mph\s*\((?P<ev>.+?)\)\s*$", ln, re.IGNORECASE)
            if not m:
                continue
            ev_raw = (m.group("ev") or "").strip()
            ev_key = self._pp_event_key(ev_raw)
            if not ev_key:
                continue
            items.append({"event_key": ev_key, "mph": int(m.group("mph")), "used": False})

        self._pp_cache = items
        return self._pp_cache


    def _pp_event_key(self, s: str) -> str:
        if not s:
            return ""
        out = re.sub(r"\s+", " ", s.replace("*", "")).strip()
        out = re.sub(r"\s+\d+\s*RBI(?:s)?\b.*$", "", out, flags=re.IGNORECASE).strip()
        if out and not out.endswith("."):
            out += "."
        return out.lower()
    
    def _parse_pa_outcome_fields(self, event_text: str) -> dict:
        text = (event_text or "").strip()
        t = text.lower()

        putout_code, fielder_pos = self._extract_putout_code(text)

        is_double_play = (" dp" in t) or ("double play" in t)
        is_sac_fly = any(x in t for x in ("sac fly", "sac flied", "sacrifice fly"))
        is_sac_bunt = any(x in t for x in ("sac bunt", "sacrifice bunt"))

        is_error = ("reached on error" in t) or ("reached on an error" in t)
        error_pos = None
        if is_error:
            # crude: try to use putout code's first digit as error position if present
            if putout_code:
                m = re.search(r"\b([1-9])\b", putout_code)
                if m:
                    error_pos = _FIELDER_NUM_TO_POS.get(m.group(1))

        result = None

        if "homered" in t:
            result = "home_run"
        elif "tripled" in t:
            result = "triple"
        elif "doubled" in t:
            result = "double"
        elif "singled" in t:
            result = "single"
        elif "hit by pitch" in t:
            result = "hbp"
        elif "intentionally walked" in t:
            result = "intentional_walk"
        elif re.search(r"\bwalked\b|\bwalks\b", t):
            result = "walk"
        elif "struck out" in t or "was called out on strikes" in t:
            result = "strikeout"
        elif is_sac_fly:
            result = "sac_fly"
        elif is_sac_bunt:
            result = "sac_bunt"
        elif is_double_play:
            result = "double_play"
        elif re.search(r"\bfor a single\b", t):
            result = "single"
        elif re.search(r"\bfor a double\b", t):
            result = "double"
        elif re.search(r"\bfor a triple\b", t):
            result = "triple"
        elif "grounded out" in t or "grounded to" in t:
            result = "groundout"
        elif "flied out" in t or "flied to" in t or "flew out" in t:
            result = "flyout"
        elif "popped out" in t or "popped to" in t or "pop out" in t:
            result = "popout"
        elif "lined out" in t or "lined to" in t:
            result = "lineout"
        elif is_error:
            result = "reached_on_error"
        elif "reached on a fielder" in t or "reached on fielder" in t or "fielder's choice" in t:
            result = "reached_on_fielder_choice"

        out_phrases = (
            "struck out",
            "was called out on strikes",
            "flied out",
            "grounded out",
            "popped out",
            "lined out",
            "fouled out",
            "out at",
            "thrown out",
            "picked off",
            "caught stealing",
            "double play",
        )

        is_out = False
        if any(p in t for p in out_phrases):
            is_out = True
        if is_sac_fly or is_sac_bunt:
            is_out = True
        if is_double_play:
            is_out = True

        batted_ball_type = self._extract_batted_ball_type(text)
        hr_distance_ft = self._extract_hr_distance_ft(text) if result == "home_run" else None

        return {
            "result": result,
            "batted_ball_type": batted_ball_type,
            "fielder_pos": fielder_pos,
            "putout_code": putout_code,
            "is_out": is_out,
            "is_double_play": is_double_play,
            "is_sac_fly": is_sac_fly,
            "is_sac_bunt": is_sac_bunt,
            "is_error": is_error,
            "error_pos": error_pos,
            "hr_distance_ft": hr_distance_ft,
            "hit_direction": self._extract_hit_direction(text),
        }
