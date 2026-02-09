from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from joblib import load as joblib_load
from sqlalchemy import func, select, text

try:
    from sklearn.exceptions import InconsistentVersionWarning
except Exception:
    InconsistentVersionWarning = None

from shared.db.models import Card, CardPrediction, PredictionRun, RosterUpdate
from apps.jobs.job import Job


def height_to_inches(v: Any) -> Optional[int]:
    if not isinstance(v, str):
        return None
    m = re.search(r"(\d+)\s*'\s*(\d+)", v)
    return (int(m.group(1)) * 12 + int(m.group(2))) if m else None


def weight_to_lbs(v: Any) -> Optional[int]:
    if not isinstance(v, str):
        return None
    val = re.sub(r"[^\d]", "", v)
    return int(val) if val else None


def safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d and d != 0 else 0.0


def make_naive(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.tz_localize(None)
    return df


def calc_batting_metrics(s: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    ab = float(s.get("ab", 0) or 0)
    h = float(s.get("h", 0) or 0)
    bb = float(s.get("bb", 0) or 0)
    hbp = float(s.get("hbp", 0) or 0)
    tb = float(s.get("tb", 0) or 0)
    so = float(s.get("so", 0) or 0)

    exclude = {"player_id", "season", "game_id"}
    out = {f"{prefix}{k}": v for k, v in s.items() if k not in exclude}

    out[f"{prefix}avg"] = safe_div(h, ab)
    out[f"{prefix}obp"] = safe_div(h + bb + hbp, ab)
    out[f"{prefix}slug"] = safe_div(tb, ab)
    out[f"{prefix}ops"] = float(out[f"{prefix}obp"]) + float(out[f"{prefix}slug"])
    out[f"{prefix}iso"] = float(out[f"{prefix}slug"]) - float(out[f"{prefix}avg"])
    out[f"{prefix}bb_pct"] = safe_div(bb, ab)
    out[f"{prefix}k_pct"] = safe_div(so, ab)
    return out


def agg_batting(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    def clean_split(s: Any) -> str:
        return str(s).lower().replace(" ", "")

    if not df.empty and "split" in df.columns:
        splits_clean = df["split"].apply(clean_split)
        total_mask = splits_clean.isin(["vslhp", "vsrhp"])
        total_sum = df[total_mask].sum(numeric_only=True).to_dict()
    else:
        total_sum = df.sum(numeric_only=True).to_dict()

    out = calc_batting_metrics(total_sum, prefix)

    if not df.empty and "split" in df.columns:
        for split_name, gdf in df.groupby("split"):
            s_clean = clean_split(split_name)
            split_prefix = f"{prefix}{s_clean}_"
            split_sum = gdf.sum(numeric_only=True).to_dict()
            out.update(calc_batting_metrics(split_sum, split_prefix))

    return out


def calc_pitching_metrics(s: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    outs = float(s.get("outs_pitched", 0) or 0)
    math_ip = outs / 3.0
    display_ip = (outs // 3) + ((outs % 3) / 10.0)

    ab = float(s.get("ab", 0) or 0)
    h = float(s.get("h", 0) or 0)
    bb = float(s.get("bb", 0) or 0)
    hr = float(s.get("hr", 0) or 0)
    er = float(s.get("er", 0) or 0)
    k = float(s.get("k", 0) or 0)

    exclude = {"player_id", "season", "game_id", "outs_pitched", "ip"}
    out = {f"{prefix}p{k}": v for k, v in s.items() if k not in exclude}

    out[f"{prefix}outs_pitched"] = outs
    out[f"{prefix}ip"] = display_ip

    out[f"{prefix}era"] = safe_div(er * 9, math_ip)
    out[f"{prefix}k9"] = safe_div(k * 9, math_ip)
    out[f"{prefix}bb9"] = safe_div(bb * 9, math_ip)
    out[f"{prefix}hr9"] = safe_div(hr * 9, math_ip)
    out[f"{prefix}whip"] = safe_div(bb + h, math_ip)

    out[f"{prefix}avg_against"] = safe_div(h, ab)
    out[f"{prefix}strike_pct"] = safe_div(
        float(s.get("strikes_thrown", 0) or 0), float(s.get("pitches_thrown", 0) or 0)
    )

    return out


def agg_pitching(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    def clean_split(s: Any) -> str:
        return str(s).lower().replace(" ", "")

    if not df.empty and "split" in df.columns:
        splits_clean = df["split"].apply(clean_split)
        total_mask = splits_clean.isin(["vslhb", "vsrhb"])
        total_sum = df[total_mask].sum(numeric_only=True).to_dict()
    else:
        total_sum = df.sum(numeric_only=True).to_dict()

    out = calc_pitching_metrics(total_sum, prefix)

    if not df.empty and "split" in df.columns:
        for split_name, gdf in df.groupby("split"):
            s_clean = clean_split(split_name)
            split_prefix = f"{prefix}{s_clean}_"
            split_sum = gdf.sum(numeric_only=True).to_dict()
            out.update(calc_pitching_metrics(split_sum, split_prefix))

    return out


def agg_baserunning(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    s = df.sum(numeric_only=True).to_dict()
    sb = float(s.get("sb", 0) or 0)
    cs = float(s.get("caught_stealing", 0) or 0)
    return {
        f"{prefix}sb": sb,
        f"{prefix}cs": cs,
        f"{prefix}sb_attempts": sb + cs,
        f"{prefix}sb_pct": safe_div(sb, sb + cs),
    }


def agg_fielding(df: pd.DataFrame, prefix: str) -> Dict[str, Any]:
    s = df.sum(numeric_only=True).to_dict()
    errors = float(s.get("errors", 0) or 0)
    chances = float(s.get("chances", 0) or 0)
    return {
        f"{prefix}errors": errors,
        f"{prefix}chances": chances,
        f"{prefix}put_outs": float(s.get("put_outs", 0) or 0),
        f"{prefix}assists": float(s.get("assists", 0) or 0),
        f"{prefix}field_pct": safe_div(chances - errors, chances),
    }


@dataclass(frozen=True)
class AttrModelSpec:
    role: str
    attr_label: str
    safe_attr: str
    model: Any
    feature_cols: List[str]


@dataclass(frozen=True)
class OvrModelSpec:
    role: str
    field_mode: str
    train_field_updates: bool
    model: Any
    feature_cols: List[str]


class PredictionSync(Job):
    def __init__(self) -> None:
        super().__init__()
        self._model_cache: Dict[Path, Any] = {}
        self._configure_lightgbm_logging()

        self._attr_field_map = {
            "CON L": "contact_left",
            "CON R": "contact_right",
            "POW L": "power_left",
            "POW R": "power_right",
            "VIS": "plate_vision",
            "DISC": "plate_discipline",
            "CLT": "batting_clutch",
            "SPD": "speed",
            "STEAL": "baserunning_ability",
            "FLD": "fielding_ability",
            "ARM": "arm_strength",
            "REAC": "reaction_time",
            "ACC": "arm_accuracy",
            "BLK": "blocking",
            "K/9": "k_per_bf",
            "BB/9": "bb_per_bf",
            "H/9": "hits_per_bf",
            "HR/9": "hr_per_bf",
            "STA": "stamina",
            "VEL": "pitch_velocity",
            "BRK": "pitch_movement",
            "CTRL": "pitch_control",
            "PCLT": "pitching_clutch",
            "BNT": "bunting_ability",
            "DRG BNT": "drag_bunting_ability",
        }

        self._field_run_attrs = {"SPD", "STEAL", "ARM", "ACC", "FLD", "REAC", "BLK"}
        self._attr_safe_to_label = {self._safe_name(k): k for k in self._attr_field_map.keys()}

        self._field_run_update_dates = {
            "2025-08-15",
            "2024-07-26",
            "2023-10-06",
            "2023-07-21",
            "2022-07-29",
            "2021-07-30",
        }

    def _configure_lightgbm_logging(self) -> None:
        try:
            import lightgbm as lgb
        except Exception:
            return
        try:
            lgb.set_config(verbosity=-1)
        except Exception:
            pass

    def run(self, db_session) -> None:
        self._log_start()

        self.logger.info(
            "prediction sync model dirs attr=%s ovr=%s env_attr=%s env_ovr=%s",
            str(self._attr_models_dir()),
            str(self._ovr_models_dir()),
            os.getenv("ATTR_MODELS_DIR"),
            os.getenv("OVR_MODELS_DIR"),
        )

        latest_year = self._get_latest_year(db_session)
        if latest_year is None:
            self.logger.info("prediction sync skipped reason=no_year")
            return

        latest_update = self._get_latest_roster_update(db_session)
        if latest_update is None:
            self.logger.info("prediction sync skipped reason=no_roster_update")
            return

        live_cards = self._load_live_series_cards(db_session, latest_year)
        if not live_cards:
            self.logger.info("prediction sync skipped reason=no_live_cards year=%s", latest_year)
            return

        update_dt = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

        features = self._build_feature_frame(
            db_session=db_session,
            cards=live_cards,
            update_dt=update_dt,
            last_update=latest_update.date,
        )
        if features.empty:
            self.logger.info("prediction sync skipped reason=no_features year=%s", latest_year)
            return
        self.logger.info(
            "prediction sync features loaded year=%s rows=%s",
            latest_year,
            len(features),
        )

        p_df_rates, b_df_rates, _tw_df = self._build_role_feature_frames(features)

        hit_deltas = self._predict_attr_deltas(b_df_rates, role="hit")
        pit_deltas = self._predict_attr_deltas(p_df_rates, role="pit")

        hit_ovr = self._predict_ovr_for_role(b_df_rates, hit_deltas, role="hit")
        pit_ovr = self._predict_ovr_for_role(p_df_rates, pit_deltas, role="pit")

        combined = self._combine_ovr_predictions(hit_ovr, pit_ovr)
        if not combined:
            self.logger.info(
                "prediction sync no combined predictions hit_ovr=%s pit_ovr=%s",
                list(hit_ovr.keys()),
                list(pit_ovr.keys()),
            )
            self.logger.info("prediction sync skipped reason=no_predictions year=%s", latest_year)
            return

        self._persist_predictions(db_session, combined)
        db_session.commit()

        self._log_end(
            year=latest_year,
            live_cards=len(live_cards),
            features=len(features),
            predictions=sum(len(v.get("preds", {})) for v in combined.values()),
        )

    def _get_latest_year(self, db_session) -> Optional[int]:
        latest_year = db_session.execute(select(func.max(Card.year))).scalar_one_or_none()
        return int(latest_year) if latest_year else None

    def _get_latest_roster_update(self, db_session) -> Optional[RosterUpdate]:
        stmt = select(RosterUpdate).order_by(RosterUpdate.date.desc(), RosterUpdate.id.desc())
        return db_session.execute(stmt).scalars().first()

    def _load_live_series_cards(self, db_session, year: int) -> List[Card]:
        stmt = select(Card).where(Card.year == year, Card.is_live_set.is_(True))
        return db_session.execute(stmt).scalars().all()

    def _safe_name(self, attr: str) -> str:
        s = str(attr).strip().upper()
        s = s.replace("/", "_")
        s = re.sub(r"\s+", "_", s)
        s = re.sub(r"[^A-Z0-9_]+", "", s)
        return s

    def _attr_models_dir(self) -> Path:
        env = os.getenv("ATTR_MODELS_DIR")
        if env:
            return Path(env).expanduser().resolve()
        return Path(__file__).resolve().parents[0] / "models" / "attr_models"

    def _ovr_models_dir(self) -> Path:
        env = os.getenv("OVR_MODELS_DIR")
        if env:
            return Path(env).expanduser().resolve()
        return Path(__file__).resolve().parents[0] / "models" / "ovr_models"

    def _joblib_load(self, path: Path) -> Any:
        with warnings.catch_warnings():
            if InconsistentVersionWarning is not None:
                warnings.filterwarnings("ignore", category=InconsistentVersionWarning)
            warnings.filterwarnings("ignore", message=r".*serialized model.*", category=UserWarning)
            return joblib_load(path)

    def _split_by_role(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        main_pos = df["display_position"].astype(str).str.upper().str.strip()
        sec_pos = df["display_secondary_positions"].astype(str).str.upper().str.strip()
        combo = (main_pos + "," + sec_pos).str.replace(r"\s+", " ", regex=True).str.strip(",")

        pitcher_roles = {"SP", "RP", "CP"}
        batter_roles = {"C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH"}

        def roles_regex(roles: set[str]) -> str:
            alts = "|".join(sorted(roles, key=len, reverse=True))
            return rf"(?:^|,\s*)(?:{alts})(?:\s*,|$)"

        mask_p = combo.str.contains(roles_regex(pitcher_roles), regex=True)
        mask_b = combo.str.contains(roles_regex(batter_roles), regex=True)
        tw_mask = mask_p & mask_b

        p_df = df[mask_p | tw_mask].copy()
        b_df = df[mask_b | tw_mask].copy()
        tw_df = df[tw_mask].copy()

        return p_df, b_df, tw_df

    def _drop_by_substring(self, df: pd.DataFrame, substrings: List[str]) -> pd.DataFrame:
        cols = list(df.columns)
        subs = [s.lower() for s in substrings]
        to_drop = [c for c in cols if any(s in c.lower() for s in subs)]
        return df.drop(columns=to_drop, errors="ignore").copy()

    def _add_split_league_shrunk_rates(
        self,
        df: pd.DataFrame,
        windows: List[str],
        split_prefixes: List[str],
        denom_name: str,
        numerators: List[str],
        k_by_window: Dict[str, float],
        split_k_divisors: Dict[str, float],
        group_col: str = "update_date",
        add_raw_rate: bool = True,
    ) -> pd.DataFrame:
        if group_col not in df.columns:
            raise ValueError(f"Missing required column: {group_col}")

        g = df[group_col]
        new_cols: Dict[str, Any] = {}

        def to_num(s: pd.Series) -> pd.Series:
            return pd.to_numeric(s, errors="coerce").fillna(0.0)

        for w in windows:
            base_k = float(k_by_window[w])

            for sp in split_prefixes:
                divisor = float(split_k_divisors.get(sp, 1.0))
                k = base_k / divisor

                dcol = f"{w}_{sp}{denom_name}"
                if dcol not in df.columns:
                    continue

                d = to_num(df[dcol])
                d_sum = d.groupby(g).transform("sum")

                for stat in numerators:
                    ncol = f"{w}_{sp}{stat}"
                    if ncol not in df.columns:
                        continue

                    n = to_num(df[ncol])
                    n_sum = n.groupby(g).transform("sum")

                    r0 = np.where(d_sum > 0, n_sum / d_sum, 0.0)

                    if add_raw_rate:
                        new_cols[f"{w}_{sp}{stat}_per_{denom_name}"] = np.where(d > 0, n / d, 0.0)

                    new_cols[f"{w}_{sp}{stat}_shrunk_per_{denom_name}"] = (n + k * r0) / (d + k)

        return pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1).copy()

    def _build_role_feature_frames(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        p_df, b_df, tw_df = self._split_by_role(df)
        if b_df.empty and not df.empty:
            b_df = df.copy()

        p_df_stage1 = self._drop_by_substring(p_df, ["vslhp", "vsrhp"])
        b_df_stage1 = self._drop_by_substring(b_df, ["vslhb", "vsrhb"])

        b_windows = ["since", "m1", "szn"]
        b_k = {"since": 45.0, "m1": 50.0, "szn": 100.0}
        b_denom = "pa"
        b_splits = ["", "risp_", "vslhp_", "vsrhp_"]
        b_split_div = {"": 1.0, "risp_": 5.0, "vslhp_": 2.0, "vsrhp_": 1.9}
        b_stats = ["r", "h", "doubles", "triples", "hr", "hbp", "tb", "rbi", "so", "bb", "ab", "lob"]

        b_df_rates = self._add_split_league_shrunk_rates(
            b_df_stage1,
            windows=b_windows,
            split_prefixes=b_splits,
            denom_name=b_denom,
            numerators=b_stats,
            k_by_window=b_k,
            split_k_divisors=b_split_div,
            group_col="update_date",
            add_raw_rate=True,
        )

        p_windows = ["since", "m1", "szn"]
        p_k = {"since": 21.0, "m1": 30.0, "szn": 50.0}
        p_denom = "pab"
        p_splits = ["", "risp_", "vslhb_", "vsrhb_"]
        p_split_div = {"": 1.0, "risp_": 5.0, "vslhb_": 2.0, "vsrhb_": 1.9}
        p_stats = ["ph", "pdoubles", "ptriples", "phr", "pbb", "pk", "pr", "per"]

        p_df_rates = self._add_split_league_shrunk_rates(
            p_df_stage1,
            windows=p_windows,
            split_prefixes=p_splits,
            denom_name=p_denom,
            numerators=p_stats,
            k_by_window=p_k,
            split_k_divisors=p_split_div,
            group_col="update_date",
            add_raw_rate=True,
        )

        return p_df_rates, b_df_rates, tw_df

    def _series_or_empty(self, df: pd.DataFrame, col: str) -> pd.Series:
        if col in df.columns:
            return df[col]
        return pd.Series([""] * len(df), index=df.index, dtype="object")

    def _build_position_features(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame(index=df.index)

        main_raw = self._series_or_empty(df, "display_position").astype(str).str.upper().str.strip()
        sec_raw = (
            self._series_or_empty(df, "display_secondary_positions").astype(str).str.upper().str.strip()
        )

        def first_sec(x: str) -> str:
            if not x or x in ("NAN", "NONE", "NULL"):
                return ""
            parts = [p.strip() for p in x.split(",") if p.strip()]
            return parts[0] if parts else ""

        main_filled = main_raw.mask(main_raw.eq(""), sec_raw.map(first_sec))
        out = pd.concat([out, pd.get_dummies(main_filled, prefix="pos_main")], axis=1)

        def split_pos(x: str) -> List[str]:
            if not x or x in ("NAN", "NONE", "NULL"):
                return []
            return [p.strip() for p in x.split(",") if p.strip()]

        sec_lists = sec_raw.map(split_pos)
        all_pos = sorted({p for lst in sec_lists for p in lst})

        for p in all_pos:
            out[f"pos_sec_{p}"] = sec_lists.map(lambda lst, p=p: 1.0 if p in lst else 0.0)

        return out

    def _sanitize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = (
            out.columns.astype(str)
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^A-Za-z0-9_]+", "_", regex=True)
        )
        return out

    def _build_attr_input_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        X = df.copy()

        pos_feats = self._build_position_features(df)
        X = pd.concat([X, pos_feats], axis=1)

        if "height" in df.columns:
            X["height_in"] = df["height"].map(height_to_inches)
        if "weight" in df.columns:
            X["weight_lb"] = df["weight"].map(weight_to_lbs)

        X = X.drop(
            columns=["display_position", "display_secondary_positions", "height", "weight"],
            errors="ignore",
        )

        X_num = X.apply(pd.to_numeric, errors="coerce").fillna(0.0)
        return self._sanitize_columns(X_num)

    def _align_feature_cols(self, X: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        out = X.copy()
        missing = [c for c in feature_cols if c not in out.columns]
        if missing:
            out = pd.concat([out, pd.DataFrame(0.0, index=out.index, columns=missing)], axis=1)
        return out[feature_cols]

    def _load_attr_model(self, model_dir: Path) -> Optional[AttrModelSpec]:
        if model_dir in self._model_cache:
            cached = self._model_cache[model_dir]
            return cached

        best_path = model_dir / "best_params.json"
        model_path = model_dir / "final_model.joblib"
        feat_path = model_dir / "feature_cols.json"

        if not (model_path.exists() and feat_path.exists()):
            self._model_cache[model_dir] = None
            return None

        role = ""
        attr_label = model_dir.name

        if best_path.exists():
            try:
                with best_path.open() as f:
                    best_obj = json.load(f)
                role = str(best_obj.get("role", "") or "").strip().lower()
                attr_label = str(best_obj.get("attr", attr_label) or attr_label).strip()
            except Exception:
                pass

        try:
            with feat_path.open() as f:
                feature_cols = list(json.load(f) or [])
        except Exception:
            self._model_cache[model_dir] = None
            return None

        try:
            model = self._joblib_load(model_path)
        except Exception:
            self._model_cache[model_dir] = None
            return None

        safe_attr = self._safe_name(attr_label)
        spec = AttrModelSpec(role=role, attr_label=attr_label, safe_attr=safe_attr, model=model, feature_cols=feature_cols)
        self._model_cache[model_dir] = spec
        return spec

    def _predict_attr_deltas(self, df_role: pd.DataFrame, role: str) -> pd.DataFrame:
        if df_role.empty:
            self.logger.info("prediction sync attr deltas skipped role=%s reason=empty_df", role)
            return pd.DataFrame()

        model_root = self._attr_models_dir()
        if not model_root.exists():
            self.logger.info(
                "prediction sync attr deltas skipped role=%s reason=missing_attr_models dir=%s",
                role,
                str(model_root),
            )
            return pd.DataFrame()

        X_role = self._build_attr_input_frame(df_role)

        preds = pd.DataFrame(index=df_role.index)
        preds["card_id"] = df_role["card_id"].values

        loaded = 0
        for model_dir in sorted(p for p in model_root.iterdir() if p.is_dir()):
            spec = self._load_attr_model(model_dir)
            if not spec:
                continue
            if spec.role and spec.role != role:
                continue

            col_name = f"pred_{spec.safe_attr}_delta"
            try:
                X = self._align_feature_cols(X_role, spec.feature_cols)
                p = spec.model.predict(X)
                preds[col_name] = pd.to_numeric(pd.Series(p, index=df_role.index), errors="coerce").fillna(0.0).astype(float)
                loaded += 1
            except Exception as exc:
                self.logger.warning(
                    "prediction sync attr delta failed role=%s attr=%s dir=%s err=%r",
                    role,
                    spec.attr_label,
                    str(model_dir),
                    exc,
                )
                continue

        if loaded == 0:
            self.logger.info(
                "prediction sync attr deltas empty role=%s reason=no_models",
                role,
            )
        return preds

    def _load_ovr_model(self, role: str, field_mode: str) -> Optional[OvrModelSpec]:
        model_dir = self._ovr_models_dir() / role / field_mode
        if not model_dir.exists():
            self.logger.info(
                "prediction sync ovr model missing role=%s field_mode=%s dir=%s",
                role,
                field_mode,
                str(model_dir),
            )
            return None

        if model_dir in self._model_cache:
            cached = self._model_cache[model_dir]
            return cached


        model_path = model_dir / "ovr_model.joblib"
        feat_path = model_dir / "feature_cols.json"
        meta_path = model_dir / "meta.json"

        if not (model_path.exists() and feat_path.exists()):
            self.logger.info(
                "prediction sync ovr model files missing role=%s field_mode=%s model=%s features=%s",
                role,
                field_mode,
                str(model_path),
                str(feat_path),
            )
            self._model_cache[model_dir] = None
            return None

        try:
            model = self._joblib_load(model_path)
            with feat_path.open() as f:
                feature_cols = list(json.load(f) or [])
        except Exception:
            self.logger.info(
                "prediction sync ovr model load failed role=%s field_mode=%s model=%s",
                role,
                field_mode,
                str(model_path),
            )
            self._model_cache[model_dir] = None
            return None

        train_field_updates = (field_mode == "field_on")
        if meta_path.exists():
            try:
                with meta_path.open() as f:
                    meta = json.load(f) or {}
                if isinstance(meta, dict) and "train_field_updates" in meta:
                    train_field_updates = bool(meta["train_field_updates"])
            except Exception:
                pass

        spec = OvrModelSpec(
            role=role,
            field_mode=field_mode,
            train_field_updates=train_field_updates,
            model=model,
            feature_cols=feature_cols,
        )
        self._model_cache[model_dir] = spec
        return spec

    def _is_field_run_date(self, role_df: pd.DataFrame) -> pd.Series:
        if "update_date" not in role_df.columns:
            return pd.Series(False, index=role_df.index)
        s = role_df["update_date"]
        if pd.api.types.is_datetime64_any_dtype(s):
            iso = s.dt.date.astype(str)
        else:
            iso = s.astype(str).str.slice(0, 10)
        return iso.isin(self._field_run_update_dates)

    def _build_ovr_input(
        self,
        role_df: pd.DataFrame,
        attr_deltas: pd.DataFrame,
        feature_cols: List[str],
        train_field_updates: bool,
    ) -> pd.DataFrame:
        role_idx = role_df.set_index("card_id", drop=False)
        deltas_idx = attr_deltas.set_index("card_id", drop=False) if not attr_deltas.empty else None

        def num(s: pd.Series) -> pd.Series:
            return pd.to_numeric(s, errors="coerce").fillna(0.0)

        is_field_date = self._is_field_run_date(role_idx)

        def delta_series(safe_attr: str) -> pd.Series:
            col = f"pred_{safe_attr}_delta"
            if deltas_idx is not None and col in deltas_idx.columns:
                return num(deltas_idx[col]).reindex(role_idx.index).fillna(0.0)
            return pd.Series(0.0, index=role_idx.index)

        def old_attr_series(attr_label: Optional[str]) -> pd.Series:
            if not attr_label:
                return pd.Series(0.0, index=role_idx.index)
            old_col = f"{attr_label}_old"
            if old_col in role_idx.columns:
                return num(role_idx[old_col])
            safe_old_col = self._safe_name(old_col)
            if safe_old_col in role_idx.columns:
                return num(role_idx[safe_old_col])
            return pd.Series(0.0, index=role_idx.index)

        col_data: Dict[str, pd.Series] = {}

        for col in feature_cols:
            if col == "old_ovr":
                col_data[col] = num(role_idx["old_ovr"])
                continue

            if col.startswith("pred_") and col.endswith("_delta"):
                safe_attr = col[len("pred_") : -len("_delta")]
                col_data[col] = delta_series(safe_attr)
                continue

            if col.startswith("pred_") and col.endswith("_new"):
                safe_attr = col[len("pred_") : -len("_new")]
                attr_label = self._attr_safe_to_label.get(safe_attr)
                old_vals = old_attr_series(attr_label)
                d = delta_series(safe_attr)
                new_vals = old_vals + d

                if attr_label in self._field_run_attrs:
                    if train_field_updates:
                        new_vals = pd.Series(
                            np.where(is_field_date.values, new_vals.values, old_vals.values),
                            index=role_idx.index,
                        )
                    else:
                        new_vals = old_vals

                col_data[col] = new_vals
                continue

            if col.startswith("old_"):
                safe_attr = col[len("old_") :]
                attr_label = self._attr_safe_to_label.get(safe_attr) or safe_attr
                col_data[col] = old_attr_series(attr_label)
                continue

            if col in role_idx.columns:
                col_data[col] = num(role_idx[col])
            else:
                col_data[col] = pd.Series(0.0, index=role_idx.index)

        X = pd.DataFrame(col_data, index=role_idx.index).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return X[feature_cols]

    def _predict_ovr_for_role(
        self, role_df: pd.DataFrame, attr_deltas: pd.DataFrame, role: str
    ) -> Dict[str, Dict[str, Any]]:
        if role_df.empty:
            self.logger.info("prediction sync ovr skipped role=%s reason=empty_df", role)
            return {}

        out: Dict[str, Dict[str, Any]] = {}

        for field_mode in ["field_off", "field_on"]:
            spec = self._load_ovr_model(role, field_mode)
            if not spec:
                continue

            try:
                X = self._build_ovr_input(role_df, attr_deltas, spec.feature_cols, spec.train_field_updates)
                preds = spec.model.predict(X)
                pred_series = pd.Series(preds, index=role_df["card_id"]).astype(float)

                attrs_cols = [c for c in spec.feature_cols if c != "old_ovr"]
                attrs_df = X[attrs_cols].copy()
                attrs_df.index = role_df["card_id"]

                out[field_mode] = {"preds": pred_series, "attrs": attrs_df}
            except Exception as exc:
                self.logger.warning(
                    "prediction sync ovr failed role=%s field_mode=%s err=%r",
                    role,
                    field_mode,
                    exc,
                )

        if not out:
            self.logger.info("prediction sync ovr empty role=%s reason=no_models_or_failures", role)
        return out

    def _combine_ovr_predictions(
        self, hit_ovr: Dict[str, Dict[str, Any]], pit_ovr: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        combined: Dict[str, Dict[str, Any]] = {}

        def is_valid(v: Any) -> bool:
            if v is None:
                return False
            try:
                if pd.isna(v):
                    return False
            except Exception:
                pass
            return True

        def clean_float(v: Any) -> float:
            if not is_valid(v):
                return 0.0
            try:
                return float(v)
            except Exception:
                return 0.0

        for field_mode in ["field_off", "field_on"]:
            hit = hit_ovr.get(field_mode, {})
            pit = pit_ovr.get(field_mode, {})

            hit_preds: pd.Series = hit.get("preds", pd.Series(dtype=float))
            pit_preds: pd.Series = pit.get("preds", pd.Series(dtype=float))

            if hit_preds.empty and pit_preds.empty:
                continue

            hit_attrs: pd.DataFrame = hit.get("attrs", pd.DataFrame())
            pit_attrs: pd.DataFrame = pit.get("attrs", pd.DataFrame())

            card_ids = set(hit_preds.index).union(set(pit_preds.index))
            rows: Dict[str, float] = {}
            attrs_map: Dict[str, Dict[str, float]] = {}

            for cid in card_ids:
                vals: List[float] = []
                if cid in hit_preds.index and is_valid(hit_preds.loc[cid]):
                    vals.append(float(hit_preds.loc[cid]))
                if cid in pit_preds.index and is_valid(pit_preds.loc[cid]):
                    vals.append(float(pit_preds.loc[cid]))
                if not vals:
                    continue

                rows[cid] = float(np.mean(vals))

                attrs: Dict[str, float] = {}
                if not hit_attrs.empty and cid in hit_attrs.index:
                    for k, v in hit_attrs.loc[cid].to_dict().items():
                        attrs[f"hit_{k}"] = clean_float(v)
                if not pit_attrs.empty and cid in pit_attrs.index:
                    for k, v in pit_attrs.loc[cid].to_dict().items():
                        attrs[f"pit_{k}"] = clean_float(v)

                attrs_map[cid] = attrs

            combined[field_mode] = {"preds": rows, "attrs": attrs_map}

        return combined

    def _persist_predictions(self, db_session, combined: Dict[str, Dict[str, Any]]) -> None:
        base_id = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)
        run_map = {
            "field_off": {"scope": "standard", "run_id": base_id},
            "field_on": {"scope": "fielding", "run_id": base_id + 1},
        }

        for field_mode, payload in combined.items():
            run_info = run_map.get(field_mode)
            if not run_info:
                continue

            run_id = int(run_info["run_id"])
            scope = str(run_info["scope"])

            preds_map: Dict[str, float] = payload.get("preds", {}) or {}
            attrs_map: Dict[str, Dict[str, float]] = payload.get("attrs", {}) or {}

            rows: List[CardPrediction] = []
            for card_id, pred in preds_map.items():
                if pred is None:
                    continue
                try:
                    if np.isnan(pred):
                        continue
                except Exception:
                    pass

                attrs = attrs_map.get(card_id, {}) or {}
                rows.append(
                    CardPrediction(
                        run_id=run_id,
                        card_id=card_id,
                        predicted_ovr=int(round(float(pred))),
                        predicted_attributes=attrs,
                        predicted_rarity=None,
                    )
                )

            if rows:
                run = PredictionRun(
                    id=run_id,
                    run_at=dt.datetime.now(dt.timezone.utc),
                    scope=scope,
                    model_version="ovr_delta_v1",
                    status="success",
                    notes=f"ovr_{field_mode}",
                )
                db_session.add(run)
                db_session.add_all(rows)
                db_session.flush()

    def _load_batting(self, db_session) -> pd.DataFrame:
        q = """
            SELECT b.player_id, g.game_date, g.season, b.split,
                   b.pa, b.r, b.h, b.doubles, b.triples, b.hr,
                   b.hbp, b.tb, b.rbi, b.so, b.bb, b.ab, b.lob
            FROM mlb_game_batting_stats b
            JOIN mlb_games g ON g.id = b.game_id
        """
        df = pd.read_sql(text(q), db_session.get_bind(), parse_dates=["game_date"])
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        df["season"] = pd.to_numeric(df["season"], errors="coerce").fillna(0).astype(int)
        return make_naive(df, "game_date")

    def _load_pitching(self, db_session) -> pd.DataFrame:
        q = """
            SELECT p.player_id, g.game_date, g.season, p.split,
                   p.outs_pitched, p.ip, p.ab, p.pitches_thrown,
                   p.h, p.doubles, p.triples, p.hr, p.bb, p.k,
                   p.r, p.er, p.batters_faced, p.balls_thrown, p.strikes_thrown
            FROM mlb_game_pitching_stats p
            JOIN mlb_games g ON g.id = p.game_id
        """
        df = pd.read_sql(text(q), db_session.get_bind(), parse_dates=["game_date"])
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        df["season"] = pd.to_numeric(df["season"], errors="coerce").fillna(0).astype(int)
        return make_naive(df, "game_date")

    def _load_baserunning(self, db_session) -> pd.DataFrame:
        q = """
            SELECT b.player_id, g.game_date, g.season,
                   b.sb, b.caught_stealing
            FROM mlb_game_baserunning_stats b
            JOIN mlb_games g ON g.id = b.game_id
        """
        df = pd.read_sql(text(q), db_session.get_bind(), parse_dates=["game_date"])
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        df["season"] = pd.to_numeric(df["season"], errors="coerce").fillna(0).astype(int)
        return make_naive(df, "game_date")

    def _load_fielding(self, db_session) -> pd.DataFrame:
        q = """
            SELECT f.player_id, g.game_date, g.season,
                   f.assists, f.put_outs, f.errors, f.chances
            FROM mlb_game_fielding_stats f
            JOIN mlb_games g ON g.id = f.game_id
        """
        df = pd.read_sql(text(q), db_session.get_bind(), parse_dates=["game_date"])
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        df["season"] = pd.to_numeric(df["season"], errors="coerce").fillna(0).astype(int)
        return make_naive(df, "game_date")

    def _build_feature_frame(
        self,
        db_session,
        cards: List[Card],
        update_dt: dt.datetime,
        last_update: dt.date,
    ) -> pd.DataFrame:
        base_rows: List[Dict[str, Any]] = []
        last_update_dt = dt.datetime.combine(last_update, dt.time.min)

        for card in cards:
            row: Dict[str, Any] = {
                "update_id": 0,
                "update_date": update_dt,
                "card_id": card.id,
                "old_ovr": card.ovr or 0,
                "new_ovr": 0,
                "trend_display": "0",
                "mlb_id": card.mlb_id or 0,
                "name": card.name or "",
                "team": card.team or "",
                "display_position": card.display_position or "",
                "display_secondary_positions": card.display_secondary_positions or "",
                "age": card.age or 0,
                "year": card.year or 0,
                "height": card.height or "",
                "weight": card.weight or "",
                "last_update": last_update_dt,
            }

            for attr, field in self._attr_field_map.items():
                val = getattr(card, field, None)
                row[f"{attr}_old"] = 0 if val is None else val

            base_rows.append(row)

        base = pd.DataFrame(base_rows)
        if base.empty:
            return base

        if last_update_dt.year != update_dt.year:
            base["last_update"] = pd.NaT

        batting = self._load_batting(db_session)
        pitching = self._load_pitching(db_session)
        baserunning = self._load_baserunning(db_session)
        fielding = self._load_fielding(db_session)

        relevant_ids = set(base["mlb_id"].unique())
        batting = batting[batting["player_id"].isin(relevant_ids)]
        pitching = pitching[pitching["player_id"].isin(relevant_ids)]
        baserunning = baserunning[baserunning["player_id"].isin(relevant_ids)]
        fielding = fielding[fielding["player_id"].isin(relevant_ids)]

        rows: List[Dict[str, Any]] = []
        for _, u in base.iterrows():
            row = u.to_dict()
            pid = int(u.get("mlb_id", 0) or 0)
            ud = u.get("update_date")
            last = u.get("last_update")

            try:
                raw_year = int(u.get("year") or 0)
                card_year = raw_year + 2000 if raw_year < 100 else raw_year
            except Exception:
                card_year = 0

            season_year = int(ud.year) if pd.notna(ud) else int(card_year or 0)

            if pid == 0:
                continue

            b_p = batting[batting.player_id == pid].copy()
            p_p = pitching[pitching.player_id == pid].copy()
            br_p = baserunning[baserunning.player_id == pid].copy()
            f_p = fielding[fielding.player_id == pid].copy()

            szn_mask_b = (b_p.season == season_year) & (b_p.game_date < ud)
            szn_mask_p = (p_p.season == season_year) & (p_p.game_date < ud)

            m1_start = ud - dt.timedelta(days=30)
            m1_mask_b = (b_p.game_date >= m1_start) & (b_p.game_date < ud)
            m1_mask_p = (p_p.game_date >= m1_start) & (p_p.game_date < ud)

            if pd.notna(last):
                since_mask_b = (b_p.game_date > last) & (b_p.game_date < ud)
                since_mask_p = (p_p.game_date > last) & (p_p.game_date < ud)
            else:
                since_mask_b = szn_mask_b
                since_mask_p = szn_mask_p

            szn_br_df = br_p[(br_p.season == season_year) & (br_p.game_date < ud)]
            szn_f_df = f_p[(f_p.season == season_year) & (f_p.game_date < ud)]

            scopes = {
                "szn_": (b_p[szn_mask_b], p_p[szn_mask_p], szn_br_df, szn_f_df),
                "m1_": (
                    b_p[m1_mask_b],
                    p_p[m1_mask_p],
                    br_p[br_p.game_date.between(m1_start, ud)],
                    f_p[f_p.game_date.between(m1_start, ud)],
                ),
                "since_": (b_p[since_mask_b], p_p[since_mask_p], None, None),
            }

            for prefix, (b_df, p_df, br_df, f_df) in scopes.items():
                row.update(agg_batting(b_df, prefix))
                row.update(agg_pitching(p_df, prefix))
                if br_df is not None:
                    row.update(agg_baserunning(br_df, prefix))
                if f_df is not None:
                    row.update(agg_fielding(f_df, prefix))

            rows.append(row)

        final_df = pd.DataFrame(rows)
        if final_df.empty:
            return final_df

        final_df["height_inches"] = final_df["height"].apply(height_to_inches)
        final_df["weight_lbs"] = final_df["weight"].apply(weight_to_lbs)

        pos = final_df["display_position"].fillna("")
        sec = final_df["display_secondary_positions"].fillna("")

        final_df["is_sp"] = (pos == "SP").astype(int)
        final_df["is_rp"] = (pos == "RP").astype(int)
        final_df["is_if"] = pos.isin(["1B", "2B", "SS", "3B"]).astype(int)
        final_df["is_of"] = pos.isin(["LF", "CF", "RF"]).astype(int)
        final_df["multi_pos"] = sec.ne("").astype(int)

        final_df["age_sq"] = pd.to_numeric(final_df["age"], errors="coerce").fillna(0) ** 2
        age_num = pd.to_numeric(final_df["age"], errors="coerce").fillna(0)
        final_df["age_bucket_young"] = (age_num < 26).astype(int)
        final_df["age_bucket_prime"] = age_num.between(26, 30).astype(int)
        final_df["age_bucket_old"] = (age_num > 30).astype(int)

        final_df = final_df.infer_objects(copy=False)
        num_cols = final_df.select_dtypes(include=["number"]).columns
        if len(num_cols) > 0:
            final_df[num_cols] = final_df[num_cols].fillna(0)
        obj_cols = final_df.columns.difference(num_cols)
        if len(obj_cols) > 0:
            final_df[obj_cols] = final_df[obj_cols].fillna("")
        return final_df.infer_objects(copy=False)
