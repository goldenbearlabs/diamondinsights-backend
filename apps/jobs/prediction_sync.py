from __future__ import annotations

import datetime as dt
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import func, select, text

from apps.jobs.job import Job
from shared.db.models import Card, CardPrediction, PredictionRun, RosterUpdate


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
    # Match training_data.py exactly.
    out[f"{prefix}obp"] = safe_div(h + bb + hbp, ab + bb + hbp)
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
    old_col: str
    new_col: str
    delta_col: str
    model: Any
    feature_cols: List[str]


@dataclass(frozen=True)
class OvrModelSpec:
    role: str
    kind: str
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

        # We never apply fielding-run updates in this job version.
        self._field_run_attrs = {"SPD", "STEAL", "ARM", "ACC", "FLD", "REAC", "BLK"}
        self._pit_attr_labels = {"K/9", "BB/9", "H/9", "HR/9", "STA", "VEL", "BRK", "CTRL", "PCLT"}

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

        p_df, b_df, _tw_df = self._build_role_feature_frames(features)

        hit_attr = self._predict_attr_deltas(b_df, role="hit")
        pit_attr = self._predict_attr_deltas(p_df, role="pit")

        hit_ovr = self._predict_ovr_for_role(b_df, role="hit", attr_preds=hit_attr)
        pit_ovr = self._predict_ovr_for_role(p_df, role="pit", attr_preds=pit_attr)

        combined = self._combine_role_predictions(hit_ovr, pit_ovr)
        if not combined:
            self.logger.info("prediction sync skipped reason=no_predictions year=%s", latest_year)
            return

        self._persist_predictions(db_session, combined)
        db_session.commit()

        self._log_end(
            year=latest_year,
            live_cards=len(live_cards),
            features=len(features),
            predictions=len(combined.get("standard", {}).get("preds", {})),
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

    def _models_root_dir(self) -> Path:
        return Path(__file__).resolve().parents[0] / "models"

    def _attr_models_dir(self) -> Path:
        env = os.getenv("ATTR_MODELS_DIR")
        if env:
            return Path(env).expanduser().resolve()
        return self._models_root_dir()

    def _ovr_models_dir(self) -> Path:
        env = os.getenv("OVR_MODELS_DIR")
        if env:
            return Path(env).expanduser().resolve()
        return self._models_root_dir()

    def _load_lgb_model(self, model_path: Path) -> Any:
        cached = self._model_cache.get(model_path)
        if cached is not None:
            return cached

        import lightgbm as lgb

        model = lgb.Booster(model_file=str(model_path))
        self._model_cache[model_path] = model
        return model

    def _infer_attr_role(self, attr_label: str) -> str:
        return "pit" if attr_label in self._pit_attr_labels else "hit"

    def _iter_attr_model_dirs(self, model_root: Path) -> List[Path]:
        if not model_root.exists():
            return []
        dirs: List[Path] = []
        for model_path in model_root.rglob("model.txt"):
            parent = model_path.parent
            if (parent / "features.json").exists():
                dirs.append(parent)
        return sorted(set(dirs), key=lambda p: str(p))

    def _load_attr_model(self, model_dir: Path) -> Optional[AttrModelSpec]:
        cache_key = model_dir / ".attr_spec"
        if cache_key in self._model_cache:
            return self._model_cache[cache_key]

        model_path = model_dir / "model.txt"
        feat_path = model_dir / "features.json"
        meta_path = model_dir / "meta.json"

        if not (model_path.exists() and feat_path.exists() and meta_path.exists()):
            self._model_cache[cache_key] = None
            return None

        try:
            with meta_path.open() as f:
                meta = json.load(f) or {}
        except Exception:
            self._model_cache[cache_key] = None
            return None

        attr_label = str(meta.get("attr") or "").strip()
        if not attr_label:
            # Skip non-attr bundles (e.g. OVR_*).
            self._model_cache[cache_key] = None
            return None

        old_col = str(meta.get("old_col") or f"{attr_label}_old")
        new_col = str(meta.get("new_col") or f"{attr_label}_new")
        delta_col = str(meta.get("delta_col") or f"{attr_label}_delta")
        safe_attr = self._safe_name(attr_label)

        try:
            with feat_path.open() as f:
                feature_cols = list(json.load(f) or [])
            model = self._load_lgb_model(model_path)
        except Exception:
            self._model_cache[cache_key] = None
            return None

        spec = AttrModelSpec(
            role=self._infer_attr_role(attr_label),
            attr_label=attr_label,
            safe_attr=safe_attr,
            old_col=old_col,
            new_col=new_col,
            delta_col=delta_col,
            model=model,
            feature_cols=feature_cols,
        )
        self._model_cache[cache_key] = spec
        return spec

    def _load_ovr_model(self, role: str) -> Optional[OvrModelSpec]:
        kind = "OVR_HITTER" if role == "hit" else "OVR_PITCHER"
        model_dir = self._ovr_models_dir() / kind
        cache_key = model_dir / ".ovr_spec"

        if cache_key in self._model_cache:
            return self._model_cache[cache_key]

        model_path = model_dir / "model.txt"
        feat_path = model_dir / "features.json"

        if not (model_path.exists() and feat_path.exists()):
            self._model_cache[cache_key] = None
            return None

        try:
            with feat_path.open() as f:
                feature_cols = list(json.load(f) or [])
            model = self._load_lgb_model(model_path)
        except Exception:
            self._model_cache[cache_key] = None
            return None

        spec = OvrModelSpec(role=role, kind=kind, model=model, feature_cols=feature_cols)
        self._model_cache[cache_key] = spec
        return spec

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

    def _build_role_feature_frames(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        p_df, b_df, tw_df = self._split_by_role(df)
        if b_df.empty and not df.empty:
            b_df = df.copy()

        # Match training pre-prune steps.
        p_df = self._drop_by_substring(p_df, ["vslhp", "vsrhp"])
        b_df = self._drop_by_substring(b_df, ["vslhb", "vsrhb"])
        return p_df, b_df, tw_df

    def _align_feature_cols(self, X: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        out = X.copy()
        missing = [c for c in feature_cols if c not in out.columns]
        if missing:
            out = pd.concat([out, pd.DataFrame(0.0, index=out.index, columns=missing)], axis=1)
        return out[feature_cols]

    def _to_numeric(self, X: pd.DataFrame) -> pd.DataFrame:
        return (
            X.apply(pd.to_numeric, errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
            .astype(np.float32)
        )

    def _numeric_series(self, df: pd.DataFrame, col: str) -> pd.Series:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return pd.Series(0.0, index=df.index, dtype="float64")

    def _build_attr_input(self, df: pd.DataFrame, spec: AttrModelSpec) -> pd.DataFrame:
        drop_cols = [
            "update_id",
            "update_date",
            "card_id",
            "old_ovr",
            "new_ovr",
            "trend_display",
            "mlb_id",
            "name",
            "team",
            "year",
            "last_update",
            spec.new_col,
            spec.delta_col,
        ]

        X = df.drop(columns=[c for c in drop_cols if c in df.columns], errors="ignore").copy()

        leak_cols = [c for c in df.columns if (c.endswith("_new") or c.endswith("_delta"))]
        leak_cols = [c for c in leak_cols if c not in {spec.new_col, spec.delta_col}]
        if leak_cols:
            X = X.drop(columns=[c for c in leak_cols if c in X.columns], errors="ignore")

        for c in ["display_position", "display_secondary_positions", "height", "weight"]:
            if c in X.columns:
                X = X.drop(columns=[c])

        if spec.old_col in df.columns and spec.old_col not in X.columns:
            X[spec.old_col] = df[spec.old_col]

        X = self._to_numeric(X)
        return self._align_feature_cols(X, spec.feature_cols)

    def _predict_attr_deltas(self, df_role: pd.DataFrame, role: str) -> pd.DataFrame:
        if df_role.empty:
            return pd.DataFrame()

        model_root = self._attr_models_dir()
        model_dirs = self._iter_attr_model_dirs(model_root)
        if not model_dirs:
            self.logger.info(
                "prediction sync attr deltas skipped role=%s reason=no_attr_models dir=%s",
                role,
                str(model_root),
            )
            return pd.DataFrame()

        preds = pd.DataFrame(index=df_role.index)
        preds["card_id"] = df_role["card_id"].values

        loaded = 0
        for model_dir in model_dirs:
            spec = self._load_attr_model(model_dir)
            if not spec or spec.role != role:
                continue

            try:
                X = self._build_attr_input(df_role, spec)
                delta = pd.Series(spec.model.predict(X), index=df_role.index)
                delta = pd.to_numeric(delta, errors="coerce").fillna(0.0).astype(float)

                old_vals = self._numeric_series(df_role, spec.old_col)

                # Non-fielding-only run: never apply fielding attr deltas.
                if spec.attr_label in self._field_run_attrs:
                    delta = pd.Series(0.0, index=df_role.index)

                new_vals = old_vals + delta

                preds[f"pred_{spec.safe_attr}_delta"] = delta.astype(float)
                preds[f"pred_{spec.safe_attr}_new"] = new_vals.astype(float)
                loaded += 1
            except Exception as exc:
                self.logger.warning(
                    "prediction sync attr delta failed role=%s attr=%s dir=%s err=%r",
                    role,
                    spec.attr_label,
                    str(model_dir),
                    exc,
                )

        if loaded == 0:
            self.logger.info("prediction sync attr deltas empty role=%s reason=no_models", role)

        return preds

    def _build_ovr_input(self, role_df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        drop_cols = [
            "update_id",
            "update_date",
            "card_id",
            "trend_display",
            "mlb_id",
            "name",
            "team",
            "year",
            "last_update",
            "new_ovr",
        ]

        X = role_df.drop(columns=[c for c in drop_cols if c in role_df.columns], errors="ignore").copy()

        leak_cols = [c for c in role_df.columns if (c.endswith("_new") or c.endswith("_delta"))]
        if leak_cols:
            X = X.drop(columns=[c for c in leak_cols if c in X.columns], errors="ignore")

        for c in ["display_position", "display_secondary_positions", "height", "weight"]:
            if c in X.columns:
                X = X.drop(columns=[c])

        if "old_ovr" in role_df.columns and "old_ovr" not in X.columns:
            X["old_ovr"] = role_df["old_ovr"]

        X = self._to_numeric(X)
        return self._align_feature_cols(X, feature_cols)

    def _predict_ovr_for_role(
        self,
        role_df: pd.DataFrame,
        role: str,
        attr_preds: pd.DataFrame,
    ) -> Dict[str, Any]:
        if role_df.empty:
            return {}

        spec = self._load_ovr_model(role)
        if not spec:
            self.logger.info("prediction sync ovr empty role=%s reason=no_model", role)
            return {}

        try:
            role_idx = role_df.set_index("card_id", drop=False)
            X = self._build_ovr_input(role_df, spec.feature_cols)
            X.index = role_idx.index

            delta_pred = pd.Series(spec.model.predict(X), index=role_idx.index)
            delta_pred = pd.to_numeric(delta_pred, errors="coerce").fillna(0.0).astype(float)

            old_ovr = self._numeric_series(role_idx, "old_ovr").astype(float)
            pred_ovr = (old_ovr + delta_pred).clip(lower=1.0, upper=125.0)

            attrs_df = X.drop(columns=["old_ovr"], errors="ignore").copy()
            if not attr_preds.empty:
                attr_cols = [c for c in attr_preds.columns if c != "card_id"]
                if attr_cols:
                    attr_idx = attr_preds.set_index("card_id", drop=False)
                    attrs_df = attrs_df.join(attr_idx[attr_cols], how="left")

            attrs_df = attrs_df.replace([np.inf, -np.inf], np.nan).fillna(0.0)
            return {"preds": pred_ovr.astype(float), "attrs": attrs_df}
        except Exception as exc:
            self.logger.warning("prediction sync ovr failed role=%s err=%r", role, exc)
            return {}

    def _combine_role_predictions(
        self, hit_ovr: Dict[str, Any], pit_ovr: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        hit_preds: pd.Series = hit_ovr.get("preds", pd.Series(dtype=float))
        pit_preds: pd.Series = pit_ovr.get("preds", pd.Series(dtype=float))

        if hit_preds.empty and pit_preds.empty:
            return {}

        hit_attrs: pd.DataFrame = hit_ovr.get("attrs", pd.DataFrame())
        pit_attrs: pd.DataFrame = pit_ovr.get("attrs", pd.DataFrame())

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

        if not rows:
            return {}

        return {"standard": {"preds": rows, "attrs": attrs_map}}

    def _persist_predictions(self, db_session, combined: Dict[str, Dict[str, Any]]) -> None:
        payload = combined.get("standard") or {}
        preds_map: Dict[str, float] = payload.get("preds", {}) or {}
        attrs_map: Dict[str, Dict[str, float]] = payload.get("attrs", {}) or {}

        if not preds_map:
            return

        run_id = int(dt.datetime.now(dt.timezone.utc).timestamp() * 1000)

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

        if not rows:
            return

        run = PredictionRun(
            id=run_id,
            run_at=dt.datetime.now(dt.timezone.utc),
            scope="standard",
            model_version="ovr_delta_v1",
            status="success",
            notes="ovr_field_off",
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
                # Non-fielding-only run.
                "is_fielding": False,
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
                    br_p[(br_p.game_date >= m1_start) & (br_p.game_date < ud)],
                    f_p[(f_p.game_date >= m1_start) & (f_p.game_date < ud)],
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

        age_num = pd.to_numeric(final_df["age"], errors="coerce").fillna(0)
        final_df["age_sq"] = age_num**2
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
