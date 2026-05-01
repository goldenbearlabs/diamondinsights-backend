from __future__ import annotations

import json
import math
import re
from itertools import combinations
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]


PITCHER_ATTRS = [
    "stamina",
    "pitching_clutch",
    "hits_per_bf_left",
    "hits_per_bf_right",
    "k_per_bf_left",
    "k_per_bf_right",
    "bb_per_bf",
    "hr_per_bf",
    "pitch_velocity",
    "pitch_control",
    "pitch_movement",
]

HITTER_ATTRS = [
    "contact_left",
    "contact_right",
    "power_left",
    "power_right",
    "plate_vision",
    "plate_discipline",
    "batting_clutch",
    "bunting_ability",
    "drag_bunting_ability",
    "fielding_ability",
    "arm_strength",
    "arm_accuracy",
    "reaction_forward",
    "reaction_back",
    "reaction_left",
    "reaction_right",
    "blocking",
    "speed",
    "base_stealing",
    "pop_time",
]

DISPLAY_POSITION_COL = "display_position"
DEFAULT_TRUE_OVERALL_WEIGHTS_PATH = PROJECT_ROOT / "apps/backend/src/ml/true_overall_weights.json"
HITTER_EVALUATED_POSITIONS = ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH"]
HITTER_POSITION_PENALTY_ATTRS = ("fielding_ability", "arm_accuracy", "reaction_forward", "reaction_back", "reaction_left", "reaction_right")
HITTER_SECONDARY_POSITION_PENALTY_PCT = 0.05
HITTER_OUT_OF_POSITION_BASE_PENALTY_PCT = 0.15
HITTER_OUTFIELD_INFIELD_CROSS_PENALTY_PCT = 0.20
HITTER_NON_CATCHER_TO_CATCHER_PENALTY_PCT = 0.75
OUTFIELD_POSITIONS = {"LF", "CF", "RF"}
INFIELD_POSITIONS = {"C", "1B", "2B", "3B", "SS"}


def _resolve_weights_path(path: Path) -> Path:
    candidates: List[Path] = []

    # Honor explicit absolute paths first.
    if path.is_absolute():
        candidates.append(path)
    else:
        candidates.append(PROJECT_ROOT / path)
        candidates.append(path)

    # Common fallback locations for different runtime layouts.
    candidates.append(PROJECT_ROOT / "apps/backend/src/ml/true_overall_weights.json")
    candidates.append(PROJECT_ROOT / "apps/jobs/true_overall_weights.json")
    candidates.append(Path(__file__).resolve().parent / "true_overall_weights.json")

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate.resolve()) if candidate.exists() else str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate

    # Preserve prior error behavior with the most likely path.
    return candidates[0]


# Easy-to-adjust boost dictionaries. Values are percentage boosts, e.g. 0.05 = +5%.
HITTER_QUIRK_BOOSTS: Dict[str, Dict[str, float]] = {
    "Bad Ball Hitter": {"plate_vision": 0.07},
    "Breaking Ball Hitter": {"power_left": 0.07, "power_right": 0.07},
    "Dead Red": {"power_left": 0.04, "power_right": 0.04},
    "First Pitch Hitter": {"power_left": 0.03, "power_right": 0.03},
    "Unfazed": {"contact_left": 0.02, "contact_right": 0.02},
    "Table Setter": {"contact_left": 0.02, "contact_right": 0.02},
    "Situational Hitter": {"contact_left": 0.02, "contact_right": 0.02},
    "Rally Monkey": {"contact_left": 0.02, "contact_right": 0.02},
}

PITCHER_QUIRK_BOOSTS: Dict[str, Dict[str, float]] = {
    "Break Outlier": {"pitch_movement": 0.03, "stamina": 0.03},
    "Outlier I": {"pitch_velocity": 0.05},
    "Outlier II": {"pitch_velocity": 0.03},
    "Stopper": {"pitching_clutch": 0.03},
}

# Handedness boosts as percentages applied to the predicted meta overall.
HITTER_BAT_HAND_BOOSTS: Dict[str, float] = {
    "L": -0.010,  # -1.0%
    "R": 0.0,     #  0.0%
    "S": 0.07,    # +7.0%
}

PITCHER_HAND_BOOSTS: Dict[str, float] = {
    "L": 0.005,   # +0.5%
    "R": 0.0,     #  0.0%
}

# Extra boost for pitchers with strong opposite-side splits (LHP right splits, RHP left splits).
PITCHER_OPPOSITE_SIDE_BOOST_MAX = 0.02
PITCHER_OPPOSITE_SIDE_ATTR_BASELINE = 70.0

# Tiny hitter-only height scaling (penalty away from 6'0").
# Kept intentionally much smaller than handedness.
HITTER_HEIGHT_SCALING = {
    "target_inches": 72.0,       # 6'0"
    "penalty_per_inch": 0.0004,  # 0.04% per inch from target
    "max_penalty": 0.0020,       # max 0.20% penalty
}

# Pitch quality coefficients per pitch type:
# Q_p = a*speed + b*movement + c*control
PITCH_QUALITY_COEFFICIENTS: Dict[str, Dict[str, float]] = {
    "DEFAULT": {"a": 0.50, "b": 0.30, "c": 0.20},
    "4-Seam Fastball": {"a": 0.62, "b": 0.08, "c": 0.30},
    "2-Seam Fastball": {"a": 0.50, "b": 0.20, "c": 0.30},
    "Sinker": {"a": 0.42, "b": 0.23, "c": 0.35},
    "Cutter": {"a": 0.42, "b": 0.33, "c": 0.25},
    "Slider": {"a": 0.35, "b": 0.35, "c": 0.30},
    "Curveball": {"a": 0.10, "b": 0.60, "c": 0.30},
    "Slurve": {"a": 0.10, "b": 0.60, "c": 0.30},
    "12-6 Curve": {"a": 0.10, "b": 0.60, "c": 0.30},
    "Knuckle-curve": {"a": 0.10, "b": 0.60, "c": 0.30},
    "Sweeping Curve": {"a": 0.15, "b": 0.55, "c": 0.30},
    "Knuckle": {"a": 0.15, "b": 0.55, "c": 0.30},
    "Sweeper": {"a": 0.15, "b": 0.55, "c": 0.30},
    "Changeup": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Circle Change": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Vulcan Change": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Splitter": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Palmball": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Forkball": {"a": 0.20, "b": 0.40, "c": 0.40},
    "Screwball": {"a": 0.20, "b": 0.45, "c": 0.35},
}

FASTBALL_PITCH_NAMES = {
    "4-SEAM FASTBALL",
    "2-SEAM FASTBALL",
    "SINKER",
    "CUTTER",
}
FASTBALL_NAME_HINTS = (
    "FASTBALL",
    "4-SEAM",
    "4SEAM",
    "FOUR-SEAM",
    "FOUR SEAM",
    "2-SEAM",
    "2SEAM",
    "TWO-SEAM",
    "TWO SEAM",
    "SINKER",
    "CUTTER",
)
BREAKING_NAME_HINTS = (
    "SLIDER",
    "SWEEPER",
    "SLURVE",
    "CURVE",
    "SCREW",
)
OFFSPEED_NAME_HINTS = (
    "CHANGE",
    "SPLIT",
    "FORK",
    "PALM",
    "VULCAN",
    "CIRCLE",
    "KNUCKLE",
)

PITCH_MIX_BOOST_CONFIG = {
    "baseline_quality": 70.0,
    "boost_per_quality_point": 0.0006,  # 0.06% per quality point
    "min_boost": -0.01,                 # -1.0%
    "max_boost": 0.03,                  # +3.0%
}

PITCH_SPEED_DIFF_CONFIG = {
    "gap_cap_mph": 30.0,
    "max_gap_boost": 0.018,          # up to +1.8%
    "band_width_mph": 4.0,           # mph bucket width for "distinct bands"
    "baseline_band_count": 2,        # no bonus up to this many bands
    "bonus_per_extra_band": 0.0020,  # +0.20% per extra band
    "max_band_bonus": 0.0055,        # cap bonus at +0.55%
    "max_total_boost": 0.0220,       # overall cap at +2.2%
}

PITCH_TUNNELING_CONFIG = {
    "m_max": 99.0,
    "c_max": 99.0,
    "theta0": 0.55,            # radians (~31.5 deg)
    "m0": 0.22,
    "v0": 6.0,                 # mph
    "vs": 3.0,
    "M0": 0.25,
    "Ms": 0.12,
    "gamma": 0.7,
    "topk_sp": 3,
    "topk_relief": 2,
    "arsenal_bonus_per_pitch": 0.04,
    "arsenal_bonus_cap_multiplier": 1.12,
    "boost_per_score": 0.0160,  # 1.6% at tunnel_final_score == 1.0
    "max_total_boost": 0.0180,  # cap with arsenal bonus
}

# Small bonus for coverage across major pitch families.
PITCH_ARSENAL_COVERAGE_CONFIG = {
    "fastball_family_bonus": 0.0015,  # +0.15%
    "breaking_family_bonus": 0.0015,  # +0.15%
    "offspeed_family_bonus": 0.0015,  # +0.15%
    "full_mix_bonus": 0.0015,         # +0.15% if all 3 are present
    "max_total_boost": 0.0060,        # cap at +0.6%
}

# Archetype bonus from derived pitch traits.
# ArchetypeIndex = 0.6 * StuffIndex + 0.6 * CommandIndex
PITCH_ARCHETYPE_INDEX_CONFIG = {
    "stuff_velocity_weight": 0.55,
    "stuff_movement_weight": 0.45,
    "stuff_top_pitch_weights": [0.65, 0.35],  # emphasize best 2 pitches
    "velocity_cap_mph": 100.0,
    "movement_cap": 99.0,
    "control_cap": 99.0,
    "fastball_control_weight": 1.20,  # slightly emphasize FB command
    "stuff_index_weight": 0.60,
    "command_index_weight": 0.60,
    "boost_per_index_point": 0.0100,  # +1.0% at archetype_index == 1.0
    "max_total_boost": 0.0150,        # cap at +1.5%
}

# RHP baseline tunnel vectors (h, z). LHP flips h.
PITCH_TUNNEL_DIRECTION_VECTORS_RHP: Dict[str, Tuple[float, float]] = {
    "4-SEAM FASTBALL": (0.00, +1.00),
    "2-SEAM FASTBALL": (-0.65, -0.20),
    "SINKER": (-0.70, -0.45),
    "CUTTER": (+0.65, -0.05),
    "SLIDER": (+0.55, -0.70),
    "SWEEPER": (+0.90, -0.25),
    "CURVEBALL": (0.00, -1.00),
    "12-6 CURVE": (0.00, -1.00),
    "KNUCKLE-CURVE": (0.00, -1.00),
    "SWEEPING CURVE": (0.00, -1.00),
    "SLURVE": (+0.35, -0.92),
    "CHANGEUP": (-0.45, -0.85),
    "CIRCLE CHANGE": (-0.45, -0.85),
    "VULCAN CHANGE": (-0.45, -0.85),
    "SPLITTER": (0.00, -0.90),
    "PALMBALL": (0.00, -0.90),
    "FORKBALL": (0.00, -0.90),
    "SCREWBALL": (-0.70, -0.55),
    "KNUCKLE": (0.00, -0.70),
}

# Global hitter overall weighting (separate from positional archetypes).
# Multiplier of 0.0 means "do not value this attribute".
HITTER_OVERALL_WEIGHTS: Dict[str, float] = {
    "contact_left": 2,
    "contact_right": 2,
    "power_left": 2,
    "power_right": 2,
    "plate_vision": 1.10,
    "batting_clutch": 1.50,
    "fielding_ability": 1.50,
    "arm_strength": 1.10,
    "arm_accuracy": 0.75,
    # Base weight; HITTER_POSITION_ATTR_SCALES multiplies these before this step.
    "reaction_forward": 0.80,
    "reaction_back": 0.80,
    "reaction_left": 0.80,
    "reaction_right": 0.80,
    "speed": 1.25,
    "base_stealing": 0.95,
    # Zeroed for non-catchers by HITTER_POSITION_ATTR_SCALES before this step.
    "pop_time": 1.50,
    "plate_discipline": 0.20,
    "bunting_ability": 0.10,
    "drag_bunting_ability": 0.10,
}
# Keep weight distribution centered so overall mass does not drift.
# If configured weights sum to more than N (where N is number of weighted attrs),
# we scale them down; if they sum to less than N, we scale them up.
HITTER_OVERALL_WEIGHT_TARGET_MEAN = 1.0

# Hitter domain caps.
HITTER_CAP_125_ATTRS = {
    "contact_left",
    "contact_right",
    "power_left",
    "power_right",
    "plate_vision",
    "plate_discipline",
    "batting_clutch",
    "bunting_ability",
    "drag_bunting_ability",
}

HITTER_CAP_99_ATTRS = {
    "fielding_ability",
    "arm_strength",
    "arm_accuracy",
    "reaction_forward",
    "reaction_back",
    "reaction_left",
    "reaction_right",
    "blocking",
    "speed",
    "base_stealing",
    "pop_time",
}
PITCHER_ATTR_CAP = 125.0

# Positional archetype scaling profiles.
# Values are multipliers against grouped attributes before prediction.
ARCHETYPE_GROUPS = ["power", "contact", "vision", "fielding", "arm", "speed"]

ATTRIBUTE_GROUP_COLUMNS: Dict[str, List[str]] = {
    "power": ["power_left", "power_right"],
    "contact": ["contact_left", "contact_right", "batting_clutch"],
    "vision": ["plate_vision"],
    "fielding": ["fielding_ability", "reaction_forward", "reaction_back", "reaction_left", "reaction_right", "blocking", "pop_time"],
    "arm": ["arm_strength", "arm_accuracy"],
    "speed": ["speed", "base_stealing"],
}

# Per-position multipliers for reaction direction attrs and pop_time, applied before global weights.
# Directions are from the fielder's perspective (forward = toward home plate, back = toward wall).
# pop_time is zeroed for all non-catchers.
HITTER_POSITION_ATTR_SCALES: Dict[str, Dict[str, float]] = {
    "DEFAULT": {"reaction_forward": 1.0, "reaction_back": 1.0, "reaction_left": 1.0, "reaction_right": 1.0, "pop_time": 0.0},
    "C":  {"reaction_forward": 2.5, "reaction_left": 1.3, "reaction_right": 1.3, "reaction_back": 0.3, "pop_time": 1.0},
    "1B": {"reaction_forward": 2.0, "reaction_left": 1.8, "reaction_right": 0.8, "reaction_back": 0.2, "pop_time": 0.0},
    "2B": {"reaction_forward": 1.0, "reaction_left": 1.6, "reaction_right": 1.6, "reaction_back": 0.8, "pop_time": 0.0},
    "3B": {"reaction_forward": 2.5, "reaction_left": 1.6, "reaction_right": 1.2, "reaction_back": 0.2, "pop_time": 0.0},
    "SS": {"reaction_forward": 1.1, "reaction_left": 2.0, "reaction_right": 1.6, "reaction_back": 1.0, "pop_time": 0.0},
    "LF": {"reaction_forward": 1.8, "reaction_back": 1.5, "reaction_right": 1.2, "reaction_left": 0.7, "pop_time": 0.0},
    "CF": {"reaction_forward": 1.7, "reaction_back": 1.7, "reaction_left": 1.5, "reaction_right": 1.5, "pop_time": 0.0},
    "RF": {"reaction_forward": 1.8, "reaction_back": 1.5, "reaction_left": 1.2, "reaction_right": 0.7, "pop_time": 0.0},
    "DH": {"reaction_forward": 0.2, "reaction_back": 0.1, "reaction_left": 0.2, "reaction_right": 0.2, "pop_time": 0.0},
}

# "DEFAULT" applies when a display_position has no explicit profile.
POSITION_ARCHETYPE_PROFILES: Dict[str, Dict[str, float]] = {
    "DEFAULT": {"power": 1.00, "contact": 1.00, "vision": 1.00, "fielding": 1.00, "arm": 1.00, "speed": 1.00},
    "C":  {"power": 1.05, "contact": 1.00, "vision": 0.95, "fielding": 1.10, "arm": 1.10, "speed": 0.80},
    "1B": {"power": 1.30, "contact": 1.10, "vision": 0.95, "fielding": 1.05, "arm": 0.70, "speed": 0.90},
    "2B": {"power": 0.90, "contact": 1.10, "vision": 1.00, "fielding": 1.20, "arm": 0.80, "speed": 1.00},
    "3B": {"power": 1.10, "contact": 0.95, "vision": 0.85, "fielding": 1.10, "arm": 1.20, "speed": 0.80},
    "SS": {"power": 0.90, "contact": 1.05, "vision": 0.90, "fielding": 1.25, "arm": 1.05, "speed": 0.85},
    "LF": {"power": 1.25, "contact": 1.00, "vision": 0.90, "fielding": 0.85, "arm": 0.90, "speed": 1.10},
    "CF": {"power": 0.85, "contact": 1.00, "vision": 0.90, "fielding": 1.25, "arm": 0.90, "speed": 1.10},
    "RF": {"power": 1.10, "contact": 0.95, "vision": 0.90, "fielding": 0.90, "arm": 1.25, "speed": 0.90},
    "DH": {"power": 1.45, "contact": 1.30, "vision": 1.10, "fielding": 0.50, "arm": 0.50, "speed": 1.05},
    "SP": {"power": 1.00, "contact": 1.00, "vision": 1.00, "fielding": 1.00, "arm": 1.00, "speed": 1.00},
    "RP": {"power": 1.00, "contact": 1.00, "vision": 1.00, "fielding": 1.00, "arm": 1.00, "speed": 1.00},
    "CP": {"power": 1.00, "contact": 1.00, "vision": 1.00, "fielding": 1.00, "arm": 1.00, "speed": 1.00},
    "P": {"power": 1.00, "contact": 1.00, "vision": 1.00, "fielding": 1.00, "arm": 1.00, "speed": 1.00},
}


def _clean_main_position(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip().upper()
    return "" if s in {"NAN", "NONE", "NULL"} else s


def _clean_secondary_positions(v: object) -> str:
    if v is None:
        return ""
    raw = str(v).strip().upper()
    if raw in {"", "NAN", "NONE", "NULL"}:
        return ""
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return ", ".join(parts)


def _split_secondary_positions(v: object) -> List[str]:
    cleaned = _clean_secondary_positions(v)
    if not cleaned:
        return []
    parts = [p.strip() for p in cleaned.split(",") if p.strip()]
    return list(dict.fromkeys(parts))


def _hitter_position_penalty(
    primary_position: object,
    evaluated_position: object,
    secondary_positions: List[str] | None = None,
) -> Tuple[float, str]:
    primary = _clean_main_position(primary_position)
    evaluated = _clean_main_position(evaluated_position)
    secondary_set = {_clean_main_position(p) for p in (secondary_positions or [])}
    secondary_set.discard("")

    if not evaluated:
        return 0.0, "none"
    if evaluated == primary:
        return 0.0, "primary"
    if evaluated in secondary_set:
        return HITTER_SECONDARY_POSITION_PENALTY_PCT, "secondary"

    # Any non-catcher (primary or secondary) playing catcher gets a heavy penalty.
    if evaluated == "C" and primary != "C" and "C" not in secondary_set:
        return HITTER_NON_CATCHER_TO_CATCHER_PENALTY_PCT, "out_of_position_to_catcher"

    outfield_to_infield = primary in OUTFIELD_POSITIONS and evaluated in INFIELD_POSITIONS
    infield_to_outfield = primary in INFIELD_POSITIONS and evaluated in OUTFIELD_POSITIONS
    if outfield_to_infield or infield_to_outfield:
        return HITTER_OUTFIELD_INFIELD_CROSS_PENALTY_PCT, "out_of_position_outfield_infield"

    return HITTER_OUT_OF_POSITION_BASE_PENALTY_PCT, "out_of_position"


def _normalize_common_features(df: pd.DataFrame, numeric_cols: List[str]) -> pd.DataFrame:
    out = df.copy()
    out["height"] = out["height"].fillna("").astype(str).str.strip()
    out["bat_hand"] = out["bat_hand"].astype(str).str.upper().str.strip()
    out["throw_hand"] = out["throw_hand"].astype(str).str.upper().str.strip()
    out["display_position"] = out["display_position"].map(_clean_main_position)
    out["display_secondary_positions"] = out["display_secondary_positions"].map(_clean_secondary_positions)
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).astype(float)
    out["ovr"] = pd.to_numeric(out["ovr"], errors="coerce").fillna(0.0).astype(float)
    return out


def _apply_hitter_position_penalties(
    df: pd.DataFrame,
    penalty_col: str = "position_penalty_pct",
) -> pd.DataFrame:
    out = df.copy()
    if penalty_col not in out.columns or "is_hitter" not in out.columns:
        return out

    penalties = pd.to_numeric(out[penalty_col], errors="coerce").fillna(0.0).clip(lower=0.0, upper=1.0).astype(float)
    hitter_mask = out["is_hitter"] == True  # noqa: E712
    if not bool(hitter_mask.any()):
        return out

    multiplier = (1.0 - penalties).clip(lower=0.0)
    for attr in HITTER_POSITION_PENALTY_ATTRS:
        if attr not in out.columns:
            continue
        base_vals = pd.to_numeric(out.loc[hitter_mask, attr], errors="coerce").fillna(0.0).astype(float)
        out.loc[hitter_mask, attr] = base_vals * multiplier.loc[hitter_mask]

    return out


def _to_finite_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(x):
        return float(default)
    return float(x)


def _load_true_overall_weights(path: Path) -> Mapping[str, Any]:
    resolved = _resolve_weights_path(path)
    if not resolved.exists():
        raise FileNotFoundError(f"True overall weights file not found: {resolved}")
    payload = json.loads(resolved.read_text())
    if "roles" not in payload:
        raise RuntimeError(f"Invalid weights payload (missing roles): {resolved}")
    return payload


def _apply_quirk_boosts(
    df: pd.DataFrame,
    quirk_boosts: Dict[str, Dict[str, float]],
) -> pd.DataFrame:
    out = df.copy()
    if "quirk_names" not in out.columns:
        return out

    for quirk_name, attr_boosts in quirk_boosts.items():
        has_quirk_mask = out["quirk_names"].map(lambda qs, q=quirk_name: q in qs)
        if not has_quirk_mask.any():
            continue
        for attr, pct in attr_boosts.items():
            if attr not in out.columns:
                continue
            out.loc[has_quirk_mask, attr] = out.loc[has_quirk_mask, attr] * (1.0 + float(pct))

    return out


def _score_row_with_role_weights(row: Mapping[str, Any], role_weights: Mapping[str, Any]) -> float:
    score = float(role_weights.get("intercept", 0.0))

    for attr, coef in dict(role_weights.get("numeric_coefficients", {})).items():
        try:
            val = float(row.get(attr, 0.0) or 0.0)
        except Exception:
            val = 0.0
        score += val * float(coef)

    main_pos = _clean_main_position(row.get(DISPLAY_POSITION_COL))

    # v2 schema (individual secondary position coefficients)
    if "display_position_coefficients" in role_weights:
        display_map = dict(role_weights.get("display_position_coefficients", {}))
        sec_map = dict(role_weights.get("secondary_position_coefficients", {}))
        score += float(display_map.get(main_pos, 0.0))
        for sec_pos in set(_split_secondary_positions(row.get("display_secondary_positions"))):
            score += float(sec_map.get(sec_pos, 0.0))
        return score

    # Backward compatibility: v1 schema (full secondary-position string category).
    cat_maps = dict(role_weights.get("categorical_coefficients", {}))
    sec_pos_str = _clean_secondary_positions(row.get("display_secondary_positions"))
    score += float(dict(cat_maps.get("display_position", {})).get(main_pos, 0.0))
    score += float(dict(cat_maps.get("display_secondary_positions", {})).get(sec_pos_str, 0.0))
    return score


def _predict_role_from_weights(role_df: pd.DataFrame, role_weights: Mapping[str, Any]) -> pd.Series:
    preds = role_df.apply(lambda row: _score_row_with_role_weights(row, role_weights), axis=1)
    return pd.to_numeric(preds, errors="coerce").fillna(0.0).astype(float)


def _normalize_archetype_profile(profile: Dict[str, float] | None) -> Dict[str, float]:
    base = POSITION_ARCHETYPE_PROFILES["DEFAULT"]
    p = profile or {}
    return {g: float(p.get(g, base[g])) for g in ARCHETYPE_GROUPS}


def _apply_hitter_position_attr_scales(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    default_scales = HITTER_POSITION_ATTR_SCALES.get("DEFAULT", {})
    all_attrs = {attr for scales in HITTER_POSITION_ATTR_SCALES.values() for attr in scales}

    for attr in all_attrs:
        if attr not in out.columns:
            continue
        default_scale = float(default_scales.get(attr, 1.0))
        scale_map = {
            pos: float(scales[attr])
            for pos, scales in HITTER_POSITION_ATTR_SCALES.items()
            if pos != "DEFAULT" and attr in scales
        }
        multiplier = out[DISPLAY_POSITION_COL].map(lambda p, m=scale_map, d=default_scale: m.get(p, d)).astype(float)
        out[attr] = out[attr] * multiplier

    return out


def _apply_position_archetypes(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    default_profile = _normalize_archetype_profile(POSITION_ARCHETYPE_PROFILES.get("DEFAULT"))

    resolved_profiles = {
        pos: _normalize_archetype_profile(profile)
        for pos, profile in POSITION_ARCHETYPE_PROFILES.items()
    }

    out["applied_archetype"] = out[DISPLAY_POSITION_COL].map(
        lambda p: p if p in resolved_profiles else "DEFAULT"
    )

    for group in ARCHETYPE_GROUPS:
        cols = [c for c in ATTRIBUTE_GROUP_COLUMNS[group] if c in out.columns]
        if not cols:
            continue
        group_multiplier = out["applied_archetype"].map(
            lambda key, g=group: resolved_profiles.get(key, default_profile)[g]
        ).astype(float)
        for col in cols:
            out[col] = out[col] * group_multiplier

    return out


def _normalized_hitter_overall_weights(available_columns: List[str]) -> Tuple[Dict[str, float], float]:
    active_weights = {
        attr: float(weight)
        for attr, weight in HITTER_OVERALL_WEIGHTS.items()
        if attr in available_columns
    }
    if not active_weights:
        return {}, 1.0

    total_weight = float(sum(active_weights.values()))
    target_total = float(len(active_weights)) * float(HITTER_OVERALL_WEIGHT_TARGET_MEAN)

    # If total is zero/non-positive, we cannot normalize safely; keep raw weights.
    if total_weight <= 0.0:
        return active_weights, 1.0

    normalization_factor = target_total / total_weight
    normalized = {attr: weight * normalization_factor for attr, weight in active_weights.items()}
    return normalized, normalization_factor


def _hitter_attribute_cap(attr: str) -> float:
    if attr in HITTER_CAP_99_ATTRS:
        return 99.0
    if attr in HITTER_CAP_125_ATTRS:
        return 125.0
    return 125.0


def _pitcher_attribute_cap(_attr: str) -> float:
    return PITCHER_ATTR_CAP


def _apply_hitter_attribute_caps(df: pd.DataFrame, cap_upper: bool = True) -> pd.DataFrame:
    out = df.copy()
    for attr in HITTER_ATTRS:
        if attr not in out.columns:
            continue
        cap = _hitter_attribute_cap(attr)
        series = pd.to_numeric(out[attr], errors="coerce").fillna(0.0).clip(lower=0.0)
        if cap_upper:
            series = series.clip(upper=cap)
        out[attr] = series.astype(float)
    return out


def _apply_pitcher_attribute_caps(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for attr in PITCHER_ATTRS:
        if attr not in out.columns:
            continue
        out[attr] = (
            pd.to_numeric(out[attr], errors="coerce")
            .fillna(0.0)
            .clip(lower=0.0, upper=_pitcher_attribute_cap(attr))
            .astype(float)
        )
    return out


def _apply_hitter_overall_weights(df: pd.DataFrame) -> Tuple[pd.DataFrame, float]:
    out = df.copy()
    normalized_weights, normalization_factor = _normalized_hitter_overall_weights(list(out.columns))
    for attr, weight in normalized_weights.items():
        if attr in out.columns:
            out[attr] = out[attr] * float(weight)
    return out, normalization_factor


def _normalize_pitch_name(name: Any) -> str:
    return str(name or "").strip().upper()


def _pitch_quality_coeff_for_name(name: Any) -> Dict[str, float]:
    n = _normalize_pitch_name(name)
    for raw_name, coeff in PITCH_QUALITY_COEFFICIENTS.items():
        if _normalize_pitch_name(raw_name) == n:
            return dict(coeff)
    return dict(PITCH_QUALITY_COEFFICIENTS["DEFAULT"])


def _pitch_quality_score(name: Any, speed: float, movement: float, control: float) -> float:
    coeff = _pitch_quality_coeff_for_name(name)
    a = float(coeff.get("a", 0.0))
    b = float(coeff.get("b", 0.0))
    c = float(coeff.get("c", 0.0))
    return (a * float(speed)) + (b * float(movement)) + (c * float(control))


def _pitch_mix_boost_pct(avg_quality: float) -> float:
    baseline = float(PITCH_MIX_BOOST_CONFIG["baseline_quality"])
    per_point = float(PITCH_MIX_BOOST_CONFIG["boost_per_quality_point"])
    min_boost = float(PITCH_MIX_BOOST_CONFIG["min_boost"])
    max_boost = float(PITCH_MIX_BOOST_CONFIG["max_boost"])
    pct = (float(avg_quality) - baseline) * per_point
    return float(max(min_boost, min(max_boost, pct)))


def _is_fastball_pitch(name: Any) -> bool:
    n = _normalize_pitch_name(name)
    if n in FASTBALL_PITCH_NAMES:
        return True
    return any(hint in n for hint in FASTBALL_NAME_HINTS)


def _is_breaking_pitch(name: Any) -> bool:
    n = _normalize_pitch_name(name)
    return any(hint in n for hint in BREAKING_NAME_HINTS)


def _is_offspeed_pitch(name: Any) -> bool:
    n = _normalize_pitch_name(name)
    return any(hint in n for hint in OFFSPEED_NAME_HINTS)


def _primary_fastball_or_max_speed(clean_pitches: List[Dict[str, Any]]) -> float:
    if not clean_pitches:
        return float("nan")
    fb_speeds = [float(p["speed"]) for p in clean_pitches if _is_fastball_pitch(p.get("name"))]
    if fb_speeds:
        return max(fb_speeds)
    return max((float(p["speed"]) for p in clean_pitches), default=float("nan"))


def _pitcher_group_for_position(pos: Any) -> str:
    p = str(pos or "").strip().upper()
    if p in {"RP", "CP"}:
        return "RELIEF"
    return "SP"


def _apply_pitch_mix_boost(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    def avg_quality(pitches: Any) -> float:
        if not isinstance(pitches, list) or not pitches:
            return float(PITCH_MIX_BOOST_CONFIG["baseline_quality"])

        vals = []
        for p in pitches:
            if not isinstance(p, dict):
                continue
            vals.append(
                _pitch_quality_score(
                    name=p.get("name"),
                    speed=_to_finite_float(p.get("speed"), default=0.0),
                    movement=_to_finite_float(p.get("movement"), default=0.0),
                    control=_to_finite_float(p.get("control"), default=0.0),
                )
            )
        if not vals:
            return float(PITCH_MIX_BOOST_CONFIG["baseline_quality"])
        return float(sum(vals) / len(vals))

    avg_q = df["pitch_rows"].map(avg_quality).astype(float)
    boost_pct = avg_q.map(_pitch_mix_boost_pct).astype(float)
    return avg_q, boost_pct


def _apply_pitch_speed_differential_boost(
    df: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    gap_cap = float(PITCH_SPEED_DIFF_CONFIG["gap_cap_mph"])
    max_gap_boost = float(PITCH_SPEED_DIFF_CONFIG["max_gap_boost"])
    band_width = float(PITCH_SPEED_DIFF_CONFIG["band_width_mph"])
    baseline_bands = int(PITCH_SPEED_DIFF_CONFIG["baseline_band_count"])
    bonus_per_extra_band = float(PITCH_SPEED_DIFF_CONFIG["bonus_per_extra_band"])
    max_band_bonus = float(PITCH_SPEED_DIFF_CONFIG["max_band_bonus"])
    max_total_boost = float(PITCH_SPEED_DIFF_CONFIG["max_total_boost"])

    def features(pitches: Any) -> Dict[str, float]:
        if not isinstance(pitches, list) or not pitches:
            return {
                "speed_diff_score": 0.0,
                "speed_band_count": 0.0,
                "primary_fastball_speed": float("nan"),
                "speed_diff_boost_pct": 0.0,
            }

        clean_pitches = []
        for p in pitches:
            if not isinstance(p, dict):
                continue
            clean_pitches.append(
                {
                    "name": p.get("name"),
                    "speed": _to_finite_float(p.get("speed"), default=0.0),
                }
            )

        if not clean_pitches:
            return {
                "speed_diff_score": 0.0,
                "speed_band_count": 0.0,
                "primary_fastball_speed": float("nan"),
                "speed_diff_boost_pct": 0.0,
            }

        primary_fb = _primary_fastball_or_max_speed(clean_pitches)
        non_fb = [p for p in clean_pitches if not _is_fastball_pitch(p["name"])]

        max_gap = 0.0
        if non_fb:
            max_gap = max(max(primary_fb - p["speed"], 0.0) for p in non_fb)
        speed_diff_score = min(max_gap, gap_cap) / gap_cap if gap_cap > 0 else 0.0
        gap_component_boost = speed_diff_score * max_gap_boost

        bands = set(int(p["speed"] // band_width) for p in clean_pitches if band_width > 0)
        band_count = len(bands)
        extra_bands = max(0, band_count - baseline_bands)
        band_bonus = min(extra_bands * bonus_per_extra_band, max_band_bonus)

        total_boost = min(gap_component_boost + band_bonus, max_total_boost)
        return {
            "speed_diff_score": float(speed_diff_score),
            "speed_band_count": float(band_count),
            "primary_fastball_speed": float(primary_fb),
            "speed_diff_boost_pct": float(total_boost),
        }

    feat = df["pitch_rows"].map(features)
    return (
        feat.map(lambda x: float(x["speed_diff_score"])).astype(float),
        feat.map(lambda x: float(x["speed_band_count"])).astype(float),
        feat.map(lambda x: float(x["primary_fastball_speed"])).astype(float),
        feat.map(lambda x: float(x["speed_diff_boost_pct"])).astype(float),
    )


def _clip01(v: float) -> float:
    return float(max(0.0, min(1.0, float(v))))


def _sigmoid(x: float) -> float:
    return float(1.0 / (1.0 + math.exp(-float(x))))


def _pitch_direction_unit(name: Any, throw_hand: Any) -> Tuple[float, float]:
    n = _normalize_pitch_name(name)
    if n in PITCH_TUNNEL_DIRECTION_VECTORS_RHP:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP[n]
    elif "SWEEPER" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SWEEPER"]
    elif "SLIDER" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SLIDER"]
    elif "SLURVE" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SLURVE"]
    elif "CURVE" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["CURVEBALL"]
    elif "CHANGE" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["CHANGEUP"]
    elif "SPLIT" in n or "FORK" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SPLITTER"]
    elif "SINKER" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SINKER"]
    elif "CUTTER" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["CUTTER"]
    elif "2-SEAM" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["2-SEAM FASTBALL"]
    elif "FASTBALL" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["4-SEAM FASTBALL"]
    elif "SCREW" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["SCREWBALL"]
    elif "KNUCKLE" in n:
        h, z = PITCH_TUNNEL_DIRECTION_VECTORS_RHP["KNUCKLE"]
    else:
        h, z = (0.0, -0.70)

    hand = str(throw_hand or "").strip().upper()
    if hand == "L":
        h = -h

    mag = math.sqrt((h * h) + (z * z))
    if mag <= 0.0:
        return (0.0, -1.0)
    return (h / mag, z / mag)


def _tunnel_topk_for_position(pos: Any) -> int:
    if _pitcher_group_for_position(pos) == "SP":
        return int(PITCH_TUNNELING_CONFIG["topk_sp"])
    return int(PITCH_TUNNELING_CONFIG["topk_relief"])


def _apply_pitch_tunneling_boost(
    df: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    m_max = float(PITCH_TUNNELING_CONFIG["m_max"])
    c_max = float(PITCH_TUNNELING_CONFIG["c_max"])
    theta0 = float(PITCH_TUNNELING_CONFIG["theta0"])
    m0 = float(PITCH_TUNNELING_CONFIG["m0"])
    v0 = float(PITCH_TUNNELING_CONFIG["v0"])
    vs = float(PITCH_TUNNELING_CONFIG["vs"])
    M0 = float(PITCH_TUNNELING_CONFIG["M0"])
    Ms = float(PITCH_TUNNELING_CONFIG["Ms"])
    gamma = float(PITCH_TUNNELING_CONFIG["gamma"])
    arsenal_bonus_per_pitch = float(PITCH_TUNNELING_CONFIG["arsenal_bonus_per_pitch"])
    arsenal_bonus_cap_multiplier = float(PITCH_TUNNELING_CONFIG["arsenal_bonus_cap_multiplier"])
    boost_per_score = float(PITCH_TUNNELING_CONFIG["boost_per_score"])
    max_total_boost = float(PITCH_TUNNELING_CONFIG["max_total_boost"])

    def features(row: pd.Series) -> Dict[str, float]:
        pitches = row.get("pitch_rows")
        throw_hand = row.get("throw_hand")
        display_position = row.get("display_position")

        if not isinstance(pitches, list) or len(pitches) < 2:
            return {
                "tunnel_score": 0.0,
                "tunnel_final_score": 0.0,
                "tunnel_boost_pct": 0.0,
            }

        clean_pitches = []
        for p in pitches:
            if not isinstance(p, dict):
                continue
            speed = _to_finite_float(p.get("speed"), default=0.0)
            movement = _to_finite_float(p.get("movement"), default=0.0)
            control = _to_finite_float(p.get("control"), default=0.0)
            m_hat = _clip01(movement / m_max) if m_max > 0 else 0.0
            c_hat = _clip01(control / c_max) if c_max > 0 else 0.0
            u_h, u_z = _pitch_direction_unit(p.get("name"), throw_hand)
            clean_pitches.append(
                {
                    "speed": speed,
                    "m_hat": m_hat,
                    "c_hat": c_hat,
                    "u_h": u_h,
                    "u_z": u_z,
                    "m_vec_h": m_hat * u_h,
                    "m_vec_z": m_hat * u_z,
                }
            )

        if len(clean_pitches) < 2:
            return {
                "tunnel_score": 0.0,
                "tunnel_final_score": 0.0,
                "tunnel_boost_pct": 0.0,
            }

        pair_scores: List[float] = []
        for i, j in combinations(range(len(clean_pitches)), 2):
            p_i = clean_pitches[i]
            p_j = clean_pitches[j]

            dot_u = (p_i["u_h"] * p_j["u_h"]) + (p_i["u_z"] * p_j["u_z"])
            dot_u = max(-1.0, min(1.0, dot_u))
            delta_theta = math.acos(dot_u)
            delta_m = abs(p_i["m_hat"] - p_j["m_hat"])

            if theta0 > 0.0:
                e_theta = math.exp(-((delta_theta / theta0) ** 2))
            else:
                e_theta = 0.0
            if m0 > 0.0:
                e_mag = math.exp(-((delta_m / m0) ** 2))
            else:
                e_mag = 0.0
            early_similarity = e_theta * e_mag

            delta_v = abs(p_i["speed"] - p_j["speed"])
            delta_m_vec = math.sqrt(
                ((p_i["m_vec_h"] - p_j["m_vec_h"]) ** 2)
                + ((p_i["m_vec_z"] - p_j["m_vec_z"]) ** 2)
            )
            s_v = _sigmoid((delta_v - v0) / vs) if vs > 0.0 else 0.0
            s_m = _sigmoid((delta_m_vec - M0) / Ms) if Ms > 0.0 else 0.0
            late_sep = (0.55 * s_v) + (0.45 * s_m)

            reliability = ((p_i["c_hat"] + p_j["c_hat"]) / 2.0) ** gamma
            t_ij = early_similarity * late_sep * reliability
            pair_scores.append(float(max(0.0, t_ij)))

        if not pair_scores:
            tunnel_score = 0.0
        else:
            pair_scores.sort(reverse=True)
            k = min(max(1, _tunnel_topk_for_position(display_position)), len(pair_scores))
            tunnel_score = float(sum(pair_scores[:k]) / k)

        arsenal_mult = 1.0 + (arsenal_bonus_per_pitch * max(0, len(clean_pitches) - 3))
        arsenal_mult = min(arsenal_mult, arsenal_bonus_cap_multiplier)
        tunnel_final_score = float(max(0.0, tunnel_score * arsenal_mult))
        tunnel_boost_pct = min(tunnel_final_score * boost_per_score, max_total_boost)

        return {
            "tunnel_score": float(tunnel_score),
            "tunnel_final_score": float(tunnel_final_score),
            "tunnel_boost_pct": float(max(0.0, tunnel_boost_pct)),
        }

    feat = df.apply(features, axis=1)
    return (
        feat.map(lambda x: float(x["tunnel_score"])).astype(float),
        feat.map(lambda x: float(x["tunnel_final_score"])).astype(float),
        feat.map(lambda x: float(x["tunnel_boost_pct"])).astype(float),
    )


def _apply_pitch_arsenal_profile_boost(
    df: pd.DataFrame,
) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series, pd.Series]:
    coverage_cfg = PITCH_ARSENAL_COVERAGE_CONFIG
    arch_cfg = PITCH_ARCHETYPE_INDEX_CONFIG

    fastball_family_bonus = float(coverage_cfg["fastball_family_bonus"])
    breaking_family_bonus = float(coverage_cfg["breaking_family_bonus"])
    offspeed_family_bonus = float(coverage_cfg["offspeed_family_bonus"])
    full_mix_bonus = float(coverage_cfg["full_mix_bonus"])
    max_coverage_boost = float(coverage_cfg["max_total_boost"])

    stuff_velocity_weight = float(arch_cfg["stuff_velocity_weight"])
    stuff_movement_weight = float(arch_cfg["stuff_movement_weight"])
    stuff_top_pitch_weights = [float(w) for w in arch_cfg["stuff_top_pitch_weights"]]
    velocity_cap = float(arch_cfg["velocity_cap_mph"])
    movement_cap = float(arch_cfg["movement_cap"])
    control_cap = float(arch_cfg["control_cap"])
    fastball_control_weight = float(arch_cfg["fastball_control_weight"])
    stuff_index_weight = float(arch_cfg["stuff_index_weight"])
    command_index_weight = float(arch_cfg["command_index_weight"])
    boost_per_index_point = float(arch_cfg["boost_per_index_point"])
    max_arch_boost = float(arch_cfg["max_total_boost"])

    def features(pitches: Any) -> Dict[str, float]:
        if not isinstance(pitches, list) or not pitches:
            return {
                "arsenal_family_count": 0.0,
                "arsenal_coverage_score": 0.0,
                "arsenal_coverage_boost_pct": 0.0,
                "stuff_index": 0.0,
                "command_index": 0.0,
                "pitch_archetype_index": 0.0,
                "pitch_archetype_boost_pct": 0.0,
            }

        clean_pitches: List[Dict[str, float | str]] = []
        for p in pitches:
            if not isinstance(p, dict):
                continue
            clean_pitches.append(
                {
                    "name": str(p.get("name") or ""),
                    "speed": _to_finite_float(p.get("speed"), default=0.0),
                    "movement": _to_finite_float(p.get("movement"), default=0.0),
                    "control": _to_finite_float(p.get("control"), default=0.0),
                }
            )

        if not clean_pitches:
            return {
                "arsenal_family_count": 0.0,
                "arsenal_coverage_score": 0.0,
                "arsenal_coverage_boost_pct": 0.0,
                "stuff_index": 0.0,
                "command_index": 0.0,
                "pitch_archetype_index": 0.0,
                "pitch_archetype_boost_pct": 0.0,
            }

        has_fastball = any(_is_fastball_pitch(p["name"]) for p in clean_pitches)
        has_breaking = any(_is_breaking_pitch(p["name"]) for p in clean_pitches)
        has_offspeed = any(_is_offspeed_pitch(p["name"]) for p in clean_pitches)
        family_count = int(has_fastball) + int(has_breaking) + int(has_offspeed)
        coverage_score = family_count / 3.0

        coverage_boost = (
            (fastball_family_bonus if has_fastball else 0.0)
            + (breaking_family_bonus if has_breaking else 0.0)
            + (offspeed_family_bonus if has_offspeed else 0.0)
            + (full_mix_bonus if family_count == 3 else 0.0)
        )
        coverage_boost = min(coverage_boost, max_coverage_boost)

        stuff_scores: List[float] = []
        for p in clean_pitches:
            speed_norm = _clip01(float(p["speed"]) / velocity_cap) if velocity_cap > 0 else 0.0
            movement_norm = _clip01(float(p["movement"]) / movement_cap) if movement_cap > 0 else 0.0
            stuff_scores.append((stuff_velocity_weight * speed_norm) + (stuff_movement_weight * movement_norm))

        stuff_scores.sort(reverse=True)
        if stuff_scores:
            take = min(len(stuff_scores), len(stuff_top_pitch_weights))
            numer = sum(stuff_scores[i] * stuff_top_pitch_weights[i] for i in range(take))
            denom = sum(stuff_top_pitch_weights[:take])
            stuff_index = (numer / denom) if denom > 0 else stuff_scores[0]
        else:
            stuff_index = 0.0

        command_weight_sum = 0.0
        command_value_sum = 0.0
        for p in clean_pitches:
            control_norm = _clip01(float(p["control"]) / control_cap) if control_cap > 0 else 0.0
            w = fastball_control_weight if _is_fastball_pitch(p["name"]) else 1.0
            command_value_sum += control_norm * w
            command_weight_sum += w
        command_index = (command_value_sum / command_weight_sum) if command_weight_sum > 0 else 0.0

        pitch_archetype_index = (stuff_index_weight * stuff_index) + (command_index_weight * command_index)
        pitch_archetype_boost_pct = min(pitch_archetype_index * boost_per_index_point, max_arch_boost)

        return {
            "arsenal_family_count": float(family_count),
            "arsenal_coverage_score": float(coverage_score),
            "arsenal_coverage_boost_pct": float(max(0.0, coverage_boost)),
            "stuff_index": float(stuff_index),
            "command_index": float(command_index),
            "pitch_archetype_index": float(pitch_archetype_index),
            "pitch_archetype_boost_pct": float(max(0.0, pitch_archetype_boost_pct)),
        }

    feat = df["pitch_rows"].map(features)
    return (
        feat.map(lambda x: float(x["arsenal_family_count"])).astype(float),
        feat.map(lambda x: float(x["arsenal_coverage_score"])).astype(float),
        feat.map(lambda x: float(x["arsenal_coverage_boost_pct"])).astype(float),
        feat.map(lambda x: float(x["stuff_index"])).astype(float),
        feat.map(lambda x: float(x["command_index"])).astype(float),
        feat.map(lambda x: float(x["pitch_archetype_index"])).astype(float),
        feat.map(lambda x: float(x["pitch_archetype_boost_pct"])).astype(float),
    )


def _build_hitter_position_scale_factors(
    role_weights: Mapping[str, Any],
    applied_positions: List[str],
) -> Dict[str, float]:
    unique_positions = sorted(set(applied_positions))
    if not unique_positions:
        return {}

    scale_map: Dict[str, float] = {}
    numeric_coef_map = dict(role_weights.get("numeric_coefficients", {}))
    sec_coef_map = dict(role_weights.get("secondary_position_coefficients", {}))
    best_secondary_positions = sorted([p for p, coef in sec_coef_map.items() if float(coef) > 0.0])
    best_secondary_str = ", ".join(best_secondary_positions)
    max_handedness_multiplier = 1.0 + max((float(v) for v in HITTER_BAT_HAND_BOOSTS.values()), default=0.0)

    for pos in unique_positions:
        row = {
            DISPLAY_POSITION_COL: pos,
            "display_secondary_positions": best_secondary_str,
            "quirk_names": list(HITTER_QUIRK_BOOSTS.keys()),
        }

        # Theoretical max by coefficient sign: positive -> cap, negative -> 0.
        for attr, coef in numeric_coef_map.items():
            cap = _hitter_attribute_cap(attr)
            row[attr] = cap if float(coef) >= 0.0 else 0.0

        perfect_df = pd.DataFrame([row])
        perfect_df = _apply_position_archetypes(perfect_df)
        perfect_df = _apply_hitter_position_attr_scales(perfect_df)
        perfect_df, _ = _apply_hitter_overall_weights(perfect_df)
        perfect_df = _apply_hitter_attribute_caps(perfect_df, cap_upper=False)

        boosted_df = _apply_quirk_boosts(perfect_df, HITTER_QUIRK_BOOSTS)
        boosted_df = _apply_hitter_attribute_caps(boosted_df, cap_upper=False)

        theoretical_max_meta = float(_predict_role_from_weights(boosted_df, role_weights).iloc[0]) * max_handedness_multiplier
        if theoretical_max_meta <= 0:
            scale_map[pos] = 1.0
        else:
            scale_map[pos] = 125.0 / theoretical_max_meta

    return scale_map


def _build_pitcher_group_scale_factors(
    role_weights: Mapping[str, Any],
    pitcher_groups: List[str],
) -> Dict[str, float]:
    groups = sorted(set(pitcher_groups))
    if not groups:
        return {}

    numeric_coef_map = dict(role_weights.get("numeric_coefficients", {}))
    sec_coef_map = dict(role_weights.get("secondary_position_coefficients", {}))
    best_secondary_positions = sorted([p for p, coef in sec_coef_map.items() if float(coef) > 0.0])
    best_secondary_str = ", ".join(best_secondary_positions)
    max_handedness_multiplier = 1.0 + max((float(v) for v in PITCHER_HAND_BOOSTS.values()), default=0.0)

    # Max pitch-mix quality from defined pitch coefficients at perfect 99 speed/movement/control.
    max_pitch_quality = max(
        (
            _pitch_quality_score(name=n, speed=99.0, movement=99.0, control=99.0)
            for n in PITCH_QUALITY_COEFFICIENTS.keys()
        ),
        default=float(PITCH_MIX_BOOST_CONFIG["baseline_quality"]),
    )
    max_pitch_mix_multiplier = 1.0 + _pitch_mix_boost_pct(max_pitch_quality)
    max_speed_diff_multiplier = 1.0 + float(PITCH_SPEED_DIFF_CONFIG["max_total_boost"])
    max_tunnel_multiplier = 1.0 + float(PITCH_TUNNELING_CONFIG["max_total_boost"])
    coverage_cfg = PITCH_ARSENAL_COVERAGE_CONFIG
    max_coverage_boost = min(
        float(coverage_cfg["fastball_family_bonus"])
        + float(coverage_cfg["breaking_family_bonus"])
        + float(coverage_cfg["offspeed_family_bonus"])
        + float(coverage_cfg["full_mix_bonus"]),
        float(coverage_cfg["max_total_boost"]),
    )
    max_arsenal_coverage_multiplier = 1.0 + max_coverage_boost
    max_pitch_archetype_multiplier = 1.0 + float(PITCH_ARCHETYPE_INDEX_CONFIG["max_total_boost"])

    group_pos_candidates = {
        "SP": ["SP", "P"],
        "RELIEF": ["RP", "CP"],
    }

    scale_map: Dict[str, float] = {}
    for group in groups:
        best_group_meta = 0.0
        for pos in group_pos_candidates.get(group, [group]):
            row = {
                DISPLAY_POSITION_COL: pos,
                "display_secondary_positions": best_secondary_str,
                "quirk_names": list(PITCHER_QUIRK_BOOSTS.keys()),
                "pitch_rows": [
                    {"name": "4-Seam Fastball", "speed": 99.0, "movement": 99.0, "control": 99.0},
                    {"name": "Cutter", "speed": 90.0, "movement": 99.0, "control": 99.0},
                    {"name": "Slider", "speed": 86.0, "movement": 99.0, "control": 99.0},
                    {"name": "Changeup", "speed": 82.0, "movement": 99.0, "control": 99.0},
                ],
                "throw_hand": "L",
            }
            for attr, coef in numeric_coef_map.items():
                cap = _pitcher_attribute_cap(attr)
                row[attr] = cap if float(coef) >= 0.0 else 0.0

            df_one = pd.DataFrame([row])
            df_one = _apply_pitcher_attribute_caps(df_one)
            boosted = _apply_quirk_boosts(df_one, PITCHER_QUIRK_BOOSTS)
            boosted = _apply_pitcher_attribute_caps(boosted)

            base_meta = float(_predict_role_from_weights(boosted, role_weights).iloc[0])
            group_meta = (
                base_meta
                * max_handedness_multiplier
                * max_pitch_mix_multiplier
                * max_speed_diff_multiplier
                * max_tunnel_multiplier
                * max_arsenal_coverage_multiplier
                * max_pitch_archetype_multiplier
            )
            if group_meta > best_group_meta:
                best_group_meta = group_meta

        if best_group_meta <= 0:
            scale_map[group] = 1.0
        else:
            scale_map[group] = 125.0 / best_group_meta

    return scale_map


def _format_quirks_for_print(v: object) -> str:
    if not isinstance(v, list) or not v:
        return "-"
    return ", ".join(v)


def _pitcher_opposite_side_boost(row: "pd.Series") -> float:
    hand = str(row.get("throw_hand", "")).strip().upper()
    if hand == "L":
        hits_attr, k_attr = "hits_per_bf_right", "k_per_bf_right"
    elif hand == "R":
        hits_attr, k_attr = "hits_per_bf_left", "k_per_bf_left"
    else:
        return 0.0
    hits_val = float(row.get(hits_attr, 0.0) or 0.0)
    k_val = float(row.get(k_attr, 0.0) or 0.0)
    avg_opp = (hits_val + k_val) / 2.0
    baseline = PITCHER_OPPOSITE_SIDE_ATTR_BASELINE
    cap = float(PITCHER_ATTR_CAP)
    if avg_opp <= baseline or cap <= baseline:
        return 0.0
    return min(PITCHER_OPPOSITE_SIDE_BOOST_MAX * (avg_opp - baseline) / (cap - baseline), PITCHER_OPPOSITE_SIDE_BOOST_MAX)


def _apply_handedness_boost(
    df: pd.DataFrame,
    is_hitter_value: bool,
) -> Tuple[pd.Series, pd.Series]:
    if is_hitter_value:
        boost_pct = df["bat_hand"].map(lambda v: HITTER_BAT_HAND_BOOSTS.get(str(v).upper(), 0.0)).astype(float)
    else:
        base_boost = df["throw_hand"].map(lambda v: PITCHER_HAND_BOOSTS.get(str(v).upper(), 0.0)).astype(float)
        hand = df["throw_hand"].str.upper().str.strip()
        hits_val = pd.to_numeric(
            np.where(hand == "L", df.get("hits_per_bf_right", 0.0), np.where(hand == "R", df.get("hits_per_bf_left", 0.0), 0.0)),
            errors="coerce",
        ).fillna(0.0)
        k_val = pd.to_numeric(
            np.where(hand == "L", df.get("k_per_bf_right", 0.0), np.where(hand == "R", df.get("k_per_bf_left", 0.0), 0.0)),
            errors="coerce",
        ).fillna(0.0)
        avg_opp = (hits_val + k_val) / 2.0
        raw_opp = PITCHER_OPPOSITE_SIDE_BOOST_MAX * (avg_opp - PITCHER_OPPOSITE_SIDE_ATTR_BASELINE) / (float(PITCHER_ATTR_CAP) - PITCHER_OPPOSITE_SIDE_ATTR_BASELINE)
        opp_boost = pd.Series(
            np.where(avg_opp > PITCHER_OPPOSITE_SIDE_ATTR_BASELINE, np.minimum(raw_opp, PITCHER_OPPOSITE_SIDE_BOOST_MAX), 0.0),
            index=df.index, dtype=float,
        )
        boost_pct = base_boost + opp_boost
    multiplier = 1.0 + boost_pct
    return boost_pct, multiplier


def _height_to_inches(v: Any) -> float | None:
    s = str(v or "").strip()
    if not s:
        return None
    m = re.search(r"(\d+)\s*'\s*(\d+)", s)
    if m:
        return float(int(m.group(1)) * 12 + int(m.group(2)))
    m_alt = re.search(r"(\d+)\s*-\s*(\d+)", s)
    if m_alt:
        return float(int(m_alt.group(1)) * 12 + int(m_alt.group(2)))
    return None


def _apply_hitter_height_adjustment(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series, pd.Series]:
    target = float(HITTER_HEIGHT_SCALING["target_inches"])
    per_inch = float(HITTER_HEIGHT_SCALING["penalty_per_inch"])
    max_penalty = float(HITTER_HEIGHT_SCALING["max_penalty"])

    height_in = df["height"].map(_height_to_inches).astype(float)
    delta = (height_in - target).abs()
    penalty = delta * per_inch
    penalty = penalty.fillna(0.0).clip(lower=0.0, upper=max_penalty)
    # Penalty only; 6'0 is best.
    boost_pct = -penalty
    multiplier = 1.0 + boost_pct
    return boost_pct, multiplier, height_in


def _run_role(
    cards: pd.DataFrame,
    is_hitter_value: bool,
    quirk_boosts: Dict[str, Dict[str, float]],
    role_weights: Mapping[str, Any],
) -> pd.DataFrame:
    role_df = cards[cards["is_hitter"] == is_hitter_value].copy()
    if role_df.empty:
        raise RuntimeError("No cards found for role split.")

    archetyped_base_df = _apply_position_archetypes(role_df)
    position_scale = pd.Series(1.0, index=role_df.index, dtype=float)

    if is_hitter_value:
        reaction_scaled_base_df = _apply_hitter_position_attr_scales(archetyped_base_df)
        weighted_base_df, overall_weight_norm_factor = _apply_hitter_overall_weights(reaction_scaled_base_df)
        weighted_base_df = _apply_hitter_attribute_caps(weighted_base_df, cap_upper=False)
        position_scale_map = _build_hitter_position_scale_factors(
            role_weights=role_weights,
            applied_positions=archetyped_base_df["applied_archetype"].astype(str).tolist(),
        )
        position_scale = archetyped_base_df["applied_archetype"].map(
            lambda p: float(position_scale_map.get(str(p), 1.0))
        ).astype(float)
    else:
        weighted_base_df = _apply_pitcher_attribute_caps(archetyped_base_df.copy())
        overall_weight_norm_factor = 1.0
        pitcher_groups = role_df["display_position"].map(_pitcher_group_for_position).astype(str)
        group_scale_map = _build_pitcher_group_scale_factors(role_weights, pitcher_groups.tolist())
        position_scale = pitcher_groups.map(lambda g: float(group_scale_map.get(str(g), 1.0))).astype(float)

    boosted_df = _apply_quirk_boosts(weighted_base_df, quirk_boosts)
    if is_hitter_value:
        boosted_df = _apply_hitter_attribute_caps(boosted_df, cap_upper=False)
    else:
        boosted_df = _apply_pitcher_attribute_caps(boosted_df)

    raw_true = _predict_role_from_weights(role_df, role_weights)
    archetyped_true = _predict_role_from_weights(archetyped_base_df, role_weights)
    weighted_true = _predict_role_from_weights(weighted_base_df, role_weights)
    role_df["true_overall"] = weighted_true * position_scale
    role_df["archetype_delta"] = archetyped_true - raw_true
    role_df["overall_weight_delta"] = weighted_true - archetyped_true
    role_df["position_group_delta"] = role_df["true_overall"] - weighted_true
    role_df["position_group_scale_factor"] = position_scale
    role_df["meta_overall"] = _predict_role_from_weights(boosted_df, role_weights) * position_scale
    role_df["handedness_boost_pct"], handedness_multiplier = _apply_handedness_boost(role_df, is_hitter_value)
    role_df["meta_overall"] = role_df["meta_overall"] * handedness_multiplier
    if is_hitter_value:
        role_df["pitch_mix_avg_quality"] = pd.NA
        role_df["pitch_mix_boost_pct"] = 0.0
        role_df["speed_diff_score"] = pd.NA
        role_df["speed_band_count"] = pd.NA
        role_df["primary_fastball_speed"] = pd.NA
        role_df["speed_diff_boost_pct"] = 0.0
        role_df["tunnel_score"] = pd.NA
        role_df["tunnel_final_score"] = pd.NA
        role_df["tunnel_boost_pct"] = 0.0
        role_df["arsenal_family_count"] = pd.NA
        role_df["arsenal_coverage_score"] = pd.NA
        role_df["arsenal_coverage_boost_pct"] = 0.0
        role_df["stuff_index"] = pd.NA
        role_df["command_index"] = pd.NA
        role_df["pitch_archetype_index"] = pd.NA
        role_df["pitch_archetype_boost_pct"] = 0.0
    else:
        role_df["pitch_mix_avg_quality"], role_df["pitch_mix_boost_pct"] = _apply_pitch_mix_boost(role_df)
        role_df["meta_overall"] = role_df["meta_overall"] * (1.0 + role_df["pitch_mix_boost_pct"].astype(float))
        (
            role_df["speed_diff_score"],
            role_df["speed_band_count"],
            role_df["primary_fastball_speed"],
            role_df["speed_diff_boost_pct"],
        ) = _apply_pitch_speed_differential_boost(role_df)
        role_df["meta_overall"] = role_df["meta_overall"] * (1.0 + role_df["speed_diff_boost_pct"].astype(float))
        (
            role_df["tunnel_score"],
            role_df["tunnel_final_score"],
            role_df["tunnel_boost_pct"],
        ) = _apply_pitch_tunneling_boost(role_df)
        role_df["meta_overall"] = role_df["meta_overall"] * (1.0 + role_df["tunnel_boost_pct"].astype(float))
        (
            role_df["arsenal_family_count"],
            role_df["arsenal_coverage_score"],
            role_df["arsenal_coverage_boost_pct"],
            role_df["stuff_index"],
            role_df["command_index"],
            role_df["pitch_archetype_index"],
            role_df["pitch_archetype_boost_pct"],
        ) = _apply_pitch_arsenal_profile_boost(role_df)
        role_df["meta_overall"] = role_df["meta_overall"] * (1.0 + role_df["arsenal_coverage_boost_pct"].astype(float))
        role_df["meta_overall"] = role_df["meta_overall"] * (1.0 + role_df["pitch_archetype_boost_pct"].astype(float))
    if is_hitter_value:
        role_df["height_boost_pct"], height_multiplier, role_df["height_inches"] = _apply_hitter_height_adjustment(role_df)
    else:
        role_df["height_boost_pct"] = 0.0
        role_df["height_inches"] = pd.NA
        height_multiplier = pd.Series(1.0, index=role_df.index, dtype=float)
    role_df["meta_overall"] = role_df["meta_overall"] * height_multiplier

    if is_hitter_value:
        role_df["true_overall"] = role_df["true_overall"].clip(lower=0.0, upper=125.0)
        role_df["meta_overall"] = role_df["meta_overall"].clip(lower=0.0, upper=125.0)
    else:
        role_df["true_overall"] = role_df["true_overall"].clip(lower=0.0, upper=125.0)
        role_df["meta_overall"] = role_df["meta_overall"].clip(lower=0.0, upper=125.0)

    role_df["quirk_delta"] = role_df["meta_overall"] - role_df["true_overall"]
    role_df["meta_overall_rounded"] = role_df["meta_overall"].round().astype(int)
    role_df["overall_weight_norm_factor"] = float(overall_weight_norm_factor)
    role_df["applied_archetype"] = archetyped_base_df["applied_archetype"]
    role_df["quirks"] = role_df["quirk_names_display"].map(_format_quirks_for_print)
    return role_df
