#!/usr/bin/env python3
"""
Retrain true_overall_weights.json directly from the cards table.

Usage:
  python train_weights.py [options]

Options:
  --year INT        Card year to train on (default: 26)
  --out PATH        Output JSON path (default: ../ml/true_overall_weights.json)
  --alpha FLOAT     Ridge regularisation for both roles (default: 1.0)
  --hitter-alpha    Override alpha for hitters only
  --pitcher-alpha   Override alpha for pitchers only
  --min-ovr INT     Drop cards with ovr below this value (default: 60)

Example:
  python train_weights.py --year 26 --alpha 0.5
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.metrics import r2_score

# ---------------------------------------------------------------------------
# Bootstrap project imports
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent


def _find_project_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "shared").is_dir():
            return p
    return start


PROJECT_ROOT = _find_project_root(SCRIPT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(PROJECT_ROOT / ".env")
from sqlalchemy import text  # noqa: E402
from shared.db.database import engine  # noqa: E402

# ---------------------------------------------------------------------------
# Feature definitions — must stay in sync with card_overall_logic.py
# ---------------------------------------------------------------------------

HITTER_NUMERIC_FEATURES = [
    "contact_left", "contact_right",
    "power_left", "power_right",
    "plate_vision", "plate_discipline", "batting_clutch",
    "bunting_ability", "drag_bunting_ability",
    "fielding_ability", "arm_strength", "arm_accuracy",
    "reaction_forward", "reaction_back", "reaction_left", "reaction_right",
    "blocking", "speed", "base_stealing", "pop_time",
]

PITCHER_NUMERIC_FEATURES = [
    "stamina", "pitching_clutch",
    "hits_per_bf_left", "hits_per_bf_right",
    "k_per_bf_left", "k_per_bf_right",
    "bb_per_bf", "hr_per_bf",
    "pitch_velocity", "pitch_control", "pitch_movement",
]

HITTER_POSITIONS  = ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH"]
PITCHER_POSITIONS = ["SP", "RP", "CP"]
PITCHER_POS_SET   = {"SP", "RP", "CP", "P"}
SECONDARY_POSITIONS = ["1B", "2B", "3B", "C", "CF", "LF", "P", "RF", "SS"]

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_cards(year: int) -> pd.DataFrame:
    all_features = list(dict.fromkeys(HITTER_NUMERIC_FEATURES + PITCHER_NUMERIC_FEATURES))
    attr_sql = ",\n        ".join(f"c.{col}" for col in all_features)
    q = f"""
        SELECT
            c.id,
            c.ovr,
            c.display_position,
            c.display_secondary_positions,
            c.is_hitter,
            {attr_sql}
        FROM cards c
        WHERE c.year = :year
          AND c.ovr IS NOT NULL
    """
    df = pd.read_sql(text(q), engine, params={"year": year})
    for col in all_features:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    df["ovr"] = pd.to_numeric(df["ovr"], errors="coerce")
    return df

# ---------------------------------------------------------------------------
# Model building
# ---------------------------------------------------------------------------

def _parse_secondary(val) -> set[str]:
    if not val or (isinstance(val, float) and np.isnan(val)):
        return set()
    return {p.strip().upper() for p in str(val).split(",") if p.strip()}


def _build_X(df: pd.DataFrame, numeric_features: list[str], primary_positions: list[str]) -> tuple[np.ndarray, list[str]]:
    n_rows = len(df)
    parts: list[np.ndarray] = []
    names: list[str] = []

    # numeric block
    num = np.zeros((n_rows, len(numeric_features)), dtype=np.float64)
    for i, col in enumerate(numeric_features):
        if col in df.columns:
            num[:, i] = df[col].values
        else:
            print(f"  WARNING: '{col}' missing from DB result — filling with 0", file=sys.stderr)
    parts.append(num)
    names.extend(numeric_features)

    # primary position one-hot
    pos = df["display_position"].fillna("").str.upper().str.strip()
    pos_block = np.zeros((n_rows, len(primary_positions)), dtype=np.float64)
    for j, p in enumerate(primary_positions):
        pos_block[:, j] = (pos == p).astype(float).values
    parts.append(pos_block)
    names.extend(f"pos_{p}" for p in primary_positions)

    # secondary position indicators
    sec = df["display_secondary_positions"].fillna("")
    sec_block = np.zeros((n_rows, len(SECONDARY_POSITIONS)), dtype=np.float64)
    for k, sp in enumerate(SECONDARY_POSITIONS):
        sec_block[:, k] = sec.apply(lambda v, p=sp: 1.0 if p in _parse_secondary(v) else 0.0).values
    parts.append(sec_block)
    names.extend(f"sec_{p}" for p in SECONDARY_POSITIONS)

    return np.hstack(parts), names


def _train(df: pd.DataFrame, numeric_features: list[str], primary_positions: list[str], role: str, alpha: float) -> dict:
    if len(df) < 10:
        print(f"  ERROR: only {len(df)} {role} rows — need more cards in DB.", file=sys.stderr)
        sys.exit(1)

    y = df["ovr"].values.astype(np.float64)
    X, feat_names = _build_X(df, numeric_features, primary_positions)

    model = Ridge(alpha=alpha, fit_intercept=True)
    model.fit(X, y)

    y_hat = model.predict(X)
    r2   = float(r2_score(y, y_hat))
    rmse = float(np.sqrt(np.mean((y - y_hat) ** 2)))

    n_num = len(numeric_features)
    n_pos = len(primary_positions)

    numeric_coefs = {feat_names[i]: float(model.coef_[i]) for i in range(n_num)}
    pos_coefs     = {primary_positions[j]: float(model.coef_[n_num + j]) for j in range(n_pos)}
    sec_coefs     = {
        SECONDARY_POSITIONS[k]: float(model.coef_[n_num + n_pos + k])
        for k in range(len(SECONDARY_POSITIONS))
        if abs(model.coef_[n_num + n_pos + k]) > 1e-10
    }

    print(f"  {role:8s}  n={len(df):5d}  R²={r2:.4f}  RMSE={rmse:.3f}  intercept={model.intercept_:.3f}")

    return {
        "intercept": float(model.intercept_),
        "numeric_features": numeric_features,
        "numeric_coefficients": numeric_coefs,
        "display_position_coefficients": pos_coefs,
        "secondary_position_coefficients": sec_coefs,
        "secondary_positions": SECONDARY_POSITIONS,
        "_train_r2": r2,
        "_train_rmse": rmse,
        "_train_n": len(df),
    }

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Train true_overall_weights.json from the cards table")
    parser.add_argument("--year",         type=int,   default=26,   help="Card year (default: 26)")
    parser.add_argument("--out",          type=Path,  default=SCRIPT_DIR.parent / "ml" / "true_overall_weights.json")
    parser.add_argument("--alpha",        type=float, default=1.0,  help="Ridge regularisation (both roles)")
    parser.add_argument("--hitter-alpha", type=float, default=None)
    parser.add_argument("--pitcher-alpha",type=float, default=None)
    parser.add_argument("--min-ovr",      type=int,   default=60,   help="Drop cards with ovr below this")
    args = parser.parse_args()

    h_alpha = args.hitter_alpha  if args.hitter_alpha  is not None else args.alpha
    p_alpha = args.pitcher_alpha if args.pitcher_alpha is not None else args.alpha

    print(f"Loading year={args.year} cards from DB ...")
    df = _load_cards(args.year)
    print(f"  {len(df):,} cards loaded")

    df = df[df["ovr"] >= args.min_ovr].reset_index(drop=True)
    print(f"  {len(df):,} cards after ovr >= {args.min_ovr} filter")

    pos = df["display_position"].fillna("").str.upper().str.strip()
    if "is_hitter" in df.columns:
        hitter_mask = df["is_hitter"].astype(str).str.lower().isin(["true", "1", "t"])
    else:
        hitter_mask = ~pos.isin(PITCHER_POS_SET)

    hitter_df  = df[hitter_mask].reset_index(drop=True)
    pitcher_df = df[~hitter_mask].reset_index(drop=True)
    print(f"  Hitters: {len(hitter_df):,}   Pitchers: {len(pitcher_df):,}")

    print("\nTraining ...")
    h_weights = _train(hitter_df,  HITTER_NUMERIC_FEATURES,  HITTER_POSITIONS,  "hitter",  h_alpha)
    p_weights = _train(pitcher_df, PITCHER_NUMERIC_FEATURES, PITCHER_POSITIONS, "pitcher", p_alpha)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "target": "ovr",
        "version": 2,
        "roles": {"hitter": h_weights, "pitcher": p_weights},
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2))
    print(f"\nWrote → {args.out}")
    print(f"  Hitter  R²={h_weights['_train_r2']:.4f}  RMSE={h_weights['_train_rmse']:.3f}")
    print(f"  Pitcher R²={p_weights['_train_r2']:.4f}  RMSE={p_weights['_train_rmse']:.3f}")
    print("\nDone. Run card_position_overall_sync to apply the new weights.")


if __name__ == "__main__":
    main()
