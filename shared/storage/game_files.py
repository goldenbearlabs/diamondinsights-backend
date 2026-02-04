from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from shared.storage.spaces_connector import SpacesConnector


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _req_str(v: Any, field: str, max_len: Optional[int] = None) -> str:
    if v is None:
        raise ValueError(f"{field} is required")
    s = str(v)
    if s == "":
        raise ValueError(f"{field} is required")
    if max_len is not None and len(s) > max_len:
        raise ValueError(f"{field} too long ({len(s)} > {max_len})")
    return s


def _opt_str(v: Any, max_len: Optional[int] = None) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if s == "":
        return None
    if max_len is not None and len(s) > max_len:
        return s[:max_len]
    return s


def _opt_int(v: Any, lo: Optional[int] = None, hi: Optional[int] = None) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    if lo is not None and n < lo:
        return None
    if hi is not None and n > hi:
        return None
    return n


def _opt_bool(v: Any) -> Optional[bool]:
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1):
        return bool(v)
    s = str(v).strip().lower()
    if s in ("true", "t", "1", "yes", "y"):
        return True
    if s in ("false", "f", "0", "no", "n"):
        return False
    return None


@dataclass(frozen=True)
class EventRow:
    game_id: str
    seq: int
    inning: Optional[int]
    is_home_batting: Optional[bool]

    outs_before: Optional[int]
    outs_after: Optional[int]

    home_score_before: Optional[int]
    away_score_before: Optional[int]
    home_score_after: Optional[int]
    away_score_after: Optional[int]

    pre_on_1b: Optional[bool]
    pre_on_2b: Optional[bool]
    pre_on_3b: Optional[bool]
    post_on_1b: Optional[bool]
    post_on_2b: Optional[bool]
    post_on_3b: Optional[bool]

    event_type: str
    event_text: str

    event_seq_in_half: Optional[int]
    parser_version: Optional[str]

    @staticmethod
    def validate(x: dict[str, Any]) -> "EventRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        seq = _opt_int(x.get("seq"), lo=1)
        if seq is None:
            raise ValueError("seq is required and must be int >= 1")

        event_type = _req_str(x.get("event_type"), "event_type", 32)
        event_text = _req_str(x.get("event_text"), "event_text")

        return EventRow(
            game_id=game_id,
            seq=seq,
            inning=_opt_int(x.get("inning"), lo=0, hi=99),
            is_home_batting=_opt_bool(x.get("is_home_batting")),
            outs_before=_opt_int(x.get("outs_before"), lo=0, hi=3),
            outs_after=_opt_int(x.get("outs_after"), lo=0, hi=3),
            home_score_before=_opt_int(x.get("home_score_before"), lo=0, hi=99),
            away_score_before=_opt_int(x.get("away_score_before"), lo=0, hi=99),
            home_score_after=_opt_int(x.get("home_score_after"), lo=0, hi=99),
            away_score_after=_opt_int(x.get("away_score_after"), lo=0, hi=99),
            pre_on_1b=_opt_bool(x.get("pre_on_1b")),
            pre_on_2b=_opt_bool(x.get("pre_on_2b")),
            pre_on_3b=_opt_bool(x.get("pre_on_3b")),
            post_on_1b=_opt_bool(x.get("post_on_1b")),
            post_on_2b=_opt_bool(x.get("post_on_2b")),
            post_on_3b=_opt_bool(x.get("post_on_3b")),
            event_type=event_type,
            event_text=event_text,
            event_seq_in_half=_opt_int(x.get("event_seq_in_half"), lo=1, hi=255),
            parser_version=_opt_str(x.get("parser_version"), 64),
        )


@dataclass(frozen=True)
class PlateAppearanceRow:
    game_id: str
    event_seq: int

    batter_name_raw: str
    pitcher_name_raw: str
    batter_mlb_id: Optional[int]
    pitcher_mlb_id: Optional[int]

    result: Optional[str]
    batted_ball_type: Optional[str]
    fielder_pos: Optional[str]
    putout_code: Optional[str]

    is_out: Optional[bool]
    is_double_play: Optional[bool]
    is_sac_fly: Optional[bool]
    is_sac_bunt: Optional[bool]

    runs_scored: Optional[int]
    rbi: Optional[int]

    hr_distance_ft: Optional[int]
    is_perfect_perfect: Optional[bool]
    exit_vel_mph: Optional[int]

    is_strikeout: Optional[bool]
    k_pitch_type: Optional[str]
    k_loc_height: Optional[str]
    k_loc_width: Optional[str]
    k_is_chase: Optional[bool]
    k_is_looking: Optional[bool]
    k_timing: Optional[str]

    batter_side: Optional[str]
    pitcher_throws: Optional[str]

    hit_direction: Optional[str]
    is_error: Optional[bool]
    error_pos: Optional[str]

    @staticmethod
    def validate(x: dict[str, Any]) -> "PlateAppearanceRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        event_seq = _opt_int(x.get("event_seq"), lo=1)
        if event_seq is None:
            raise ValueError("event_seq is required and must be int >= 1")

        batter = _req_str(x.get("batter_name_raw"), "batter_name_raw", 256)
        pitcher = _req_str(x.get("pitcher_name_raw"), "pitcher_name_raw", 256)

        return PlateAppearanceRow(
            game_id=game_id,
            event_seq=event_seq,
            batter_name_raw=batter,
            pitcher_name_raw=pitcher,
            batter_mlb_id=_opt_int(x.get("batter_mlb_id"), lo=1),
            pitcher_mlb_id=_opt_int(x.get("pitcher_mlb_id"), lo=1),
            result=_opt_str(x.get("result"), 64),
            batted_ball_type=_opt_str(x.get("batted_ball_type"), 32),
            fielder_pos=_opt_str(x.get("fielder_pos"), 16),
            putout_code=_opt_str(x.get("putout_code"), 64),
            is_out=_opt_bool(x.get("is_out")),
            is_double_play=_opt_bool(x.get("is_double_play")),
            is_sac_fly=_opt_bool(x.get("is_sac_fly")),
            is_sac_bunt=_opt_bool(x.get("is_sac_bunt")),
            runs_scored=_opt_int(x.get("runs_scored"), lo=0, hi=9),
            rbi=_opt_int(x.get("rbi"), lo=0, hi=9),
            hr_distance_ft=_opt_int(x.get("hr_distance_ft"), lo=0, hi=999),
            is_perfect_perfect=_opt_bool(x.get("is_perfect_perfect")),
            exit_vel_mph=_opt_int(x.get("exit_vel_mph"), lo=0, hi=130),
            is_strikeout=_opt_bool(x.get("is_strikeout")),
            k_pitch_type=_opt_str(x.get("k_pitch_type"), 32),
            k_loc_height=_opt_str(x.get("k_loc_height"), 16),
            k_loc_width=_opt_str(x.get("k_loc_width"), 16),
            k_is_chase=_opt_bool(x.get("k_is_chase")),
            k_is_looking=_opt_bool(x.get("k_is_looking")),
            k_timing=_opt_str(x.get("k_timing"), 32),
            batter_side=_opt_str(x.get("batter_side"), 8),
            pitcher_throws=_opt_str(x.get("pitcher_throws"), 8),
            hit_direction=_opt_str(x.get("hit_direction"), 32),
            is_error=_opt_bool(x.get("is_error")),
            error_pos=_opt_str(x.get("error_pos"), 64),
        )


@dataclass(frozen=True)
class RunnerMoveRow:
    game_id: str
    event_seq: int

    runner_name_raw: str
    runner_mlb_id: Optional[int]
    from_base: Optional[int]
    to_base: Optional[int]
    move_type: str
    note: Optional[str]

    @staticmethod
    def validate(x: dict[str, Any]) -> "RunnerMoveRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        event_seq = _opt_int(x.get("event_seq"), lo=1)
        if event_seq is None:
            raise ValueError("event_seq is required and must be int >= 1")
        move_type = _req_str(x.get("move_type"), "move_type", 32)
        runner = _req_str(x.get("runner_name_raw"), "runner_name_raw", 256)
        return RunnerMoveRow(
            game_id=game_id,
            event_seq=event_seq,
            runner_name_raw=runner,
            runner_mlb_id=_opt_int(x.get("runner_mlb_id"), lo=1),
            from_base=_opt_int(x.get("from_base"), lo=-1, hi=4),
            to_base=_opt_int(x.get("to_base"), lo=-1, hi=4),
            move_type=move_type,
            note=_opt_str(x.get("note"), 256),
        )


@dataclass(frozen=True)
class HalfInningRow:
    game_id: str
    inning: int
    is_home_batting: bool
    runs: int
    hits: int
    walks: int
    errors: int
    pitches: int
    runners_left_on: int

    @staticmethod
    def validate(x: dict[str, Any]) -> "HalfInningRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        inning = _opt_int(x.get("inning"), lo=1, hi=99)
        if inning is None:
            raise ValueError("inning required")
        is_home = _opt_bool(x.get("is_home_batting"))
        if is_home is None:
            raise ValueError("is_home_batting required")
        def _ri(k: str) -> int:
            v = _opt_int(x.get(k), lo=0, hi=255)
            if v is None:
                raise ValueError(f"{k} required")
            return v
        return HalfInningRow(
            game_id=game_id,
            inning=inning,
            is_home_batting=is_home,
            runs=_ri("runs"),
            hits=_ri("hits"),
            walks=_ri("walks"),
            errors=_ri("errors"),
            pitches=_ri("pitches"),
            runners_left_on=_ri("runners_left_on"),
        )


@dataclass(frozen=True)
class BatterBoxscoreRow:
    game_id: str
    is_home: bool
    appearance_idx: int
    replaced_apperance_idx: Optional[int]
    player_name_raw: str
    mlb_id: Optional[int]
    ab: int
    h: int
    r: int
    rbi: int
    bb: int
    so: int
    doubles: int
    triples: int
    hr: int
    sh: int
    sf: int
    gidp: int
    e: int
    pb: int
    hbp: int
    sb: int
    cs: int
    innings: int
    pos: int

    @staticmethod
    def validate(x: dict[str, Any]) -> "BatterBoxscoreRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        is_home = _opt_bool(x.get("is_home"))
        if is_home is None:
            raise ValueError("is_home required")
        appearance_idx = _opt_int(x.get("appearance_idx"), lo=1)
        if appearance_idx is None:
            raise ValueError("appearance_idx required")
        player_name_raw = _req_str(x.get("player_name_raw"), "player_name_raw", 256)

        def _ri(k: str) -> int:
            v = _opt_int(x.get(k), lo=0, hi=999)
            if v is None:
                raise ValueError(f"{k} required")
            return v

        return BatterBoxscoreRow(
            game_id=game_id,
            is_home=is_home,
            appearance_idx=appearance_idx,
            replaced_apperance_idx=_opt_int(x.get("replaced_apperance_idx"), lo=1, hi=99),
            player_name_raw=player_name_raw,
            mlb_id=_opt_int(x.get("mlb_id"), lo=1),
            ab=_ri("ab"),
            h=_ri("h"),
            r=_ri("r"),
            rbi=_ri("rbi"),
            bb=_ri("bb"),
            so=_ri("so"),
            doubles=_ri("doubles"),
            triples=_ri("triples"),
            hr=_ri("hr"),
            sh=_ri("sh"),
            sf=_ri("sf"),
            gidp=_ri("gidp"),
            e=_ri("e"),
            pb=_ri("pb"),
            hbp=_ri("hbp"),
            sb=_ri("sb"),
            cs=_ri("cs"),
            innings=_ri("innings"),
            pos=_ri("pos"),
        )


@dataclass(frozen=True)
class PitcherBoxscoreRow:
    game_id: str
    is_home: bool
    appearance_idx: int
    player_name_raw: str
    mlb_id: Optional[int]
    ip_raw: str
    outs_pitched: int
    r: int
    h: int
    er: int
    bb: int
    so: int
    era: Optional[float]
    wp: int
    win: int
    loss: int
    save: int
    b_save: int
    hold: int
    s_wins: int
    s_losses: int
    s_saves: int
    s_b_saves: int
    s_holds: int

    @staticmethod
    def validate(x: dict[str, Any]) -> "PitcherBoxscoreRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        is_home = _opt_bool(x.get("is_home"))
        if is_home is None:
            raise ValueError("is_home required")
        appearance_idx = _opt_int(x.get("appearance_idx"), lo=1)
        if appearance_idx is None:
            raise ValueError("appearance_idx required")
        player_name_raw = _req_str(x.get("player_name_raw"), "player_name_raw", 256)
        ip_raw = _req_str(x.get("ip_raw"), "ip_raw", 16)
        outs_pitched = _opt_int(x.get("outs_pitched"), lo=0, hi=81)
        if outs_pitched is None:
            raise ValueError("outs_pitched required")

        def _ri(k: str) -> int:
            v = _opt_int(x.get(k), lo=0, hi=999)
            if v is None:
                raise ValueError(f"{k} required")
            return v

        era = x.get("era")
        era_f = None
        if era is not None and era != "":
            try:
                era_f = float(era)
            except (TypeError, ValueError):
                era_f = None

        return PitcherBoxscoreRow(
            game_id=game_id,
            is_home=is_home,
            appearance_idx=appearance_idx,
            player_name_raw=player_name_raw,
            mlb_id=_opt_int(x.get("mlb_id"), lo=1),
            ip_raw=ip_raw,
            outs_pitched=outs_pitched,
            r=_ri("r"),
            h=_ri("h"),
            er=_ri("er"),
            bb=_ri("bb"),
            so=_ri("so"),
            era=era_f,
            wp=_ri("wp"),
            win=_ri("win"),
            loss=_ri("loss"),
            save=_ri("save"),
            b_save=_ri("b_save"),
            hold=_ri("hold"),
            s_wins=_ri("s_wins"),
            s_losses=_ri("s_losses"),
            s_saves=_ri("s_saves"),
            s_b_saves=_ri("s_b_saves"),
            s_holds=_ri("s_holds"),
        )


@dataclass(frozen=True)
class PitcherGameScoreRow:
    game_id: str
    pitcher_name_raw: str
    is_home: bool
    game_score: int
    pitcher_mlb_id: Optional[int]

    @staticmethod
    def validate(x: dict[str, Any]) -> "PitcherGameScoreRow":
        game_id = _req_str(x.get("game_id"), "game_id", 64)
        pitcher = _req_str(x.get("pitcher_name_raw"), "pitcher_name_raw", 256)
        is_home = _opt_bool(x.get("is_home"))
        if is_home is None:
            raise ValueError("is_home required")
        gs = _opt_int(x.get("game_score"), lo=0, hi=200)
        if gs is None:
            raise ValueError("game_score required")
        return PitcherGameScoreRow(
            game_id=game_id,
            pitcher_name_raw=pitcher,
            is_home=is_home,
            game_score=gs,
            pitcher_mlb_id=_opt_int(x.get("pitcher_mlb_id"), lo=1),
        )


def _jsonl_bytes(rows: Iterable[dict[str, Any]]) -> tuple[bytes, int]:
    buf = bytearray()
    n = 0
    for r in rows:
        buf.extend(json.dumps(r, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        buf.extend(b"\n")
        n += 1
    return bytes(buf), n


def write_game_dataset_jsonl(
    *,
    spaces: SpacesConnector,
    game_id: str,
    filename: str,
    rows: Iterable[dict[str, Any]],
    validator,
    schema_version: int = 1,
) -> Optional[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for r in rows:
        obj = validator(dict(r))
        validated.append(asdict(obj))

    if not validated:
        return None

    content, n = _jsonl_bytes(validated)
    key = f"games/{game_id}/{filename}"

    spaces.put_bytes(
        key,
        content,
        content_type="application/x-ndjson",
        cache_control="max-age=31536000",
        metadata={"schema_version": str(schema_version), "rows": str(n)},
    )

    return {"key": key, "rows": n, "bytes": len(content), "sha256": _sha256(content)}


def write_game_manifest(
    *,
    spaces: SpacesConnector,
    game_id: str,
    files: dict[str, dict[str, Any]],
    schema_version: int = 1,
) -> dict[str, Any]:
    manifest = {
        "game_id": game_id,
        "schema_version": schema_version,
        "generated_at": _utc_now_iso(),
        "files": files,
    }
    content = json.dumps(manifest, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    key = f"games/{game_id}/manifest.json"

    spaces.put_bytes(
        key,
        content,
        content_type="application/json",
        cache_control="max-age=31536000",
        metadata={"schema_version": str(schema_version)},
    )
    return {"key": key, "bytes": len(content), "sha256": _sha256(content)}
