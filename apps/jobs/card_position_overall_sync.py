from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from apps.jobs import card_overall_logic as overall
from shared.db.models import Card, CardPositionOverall, Pitch, card_quirk_association


class CardPositionOverallSync(Job):
    def __init__(self, weights_path: Optional[str] = None):
        super().__init__()
        self.weights_path = Path(weights_path) if weights_path else overall.DEFAULT_TRUE_OVERALL_WEIGHTS_PATH

    def run(self, db_session: Session):
        self._log_start(weights_path=str(self.weights_path))

        cards = self._fetch_cards_df(db_session)
        if cards.empty:
            db_session.execute(delete(CardPositionOverall))
            db_session.commit()
            self._log_end(cards=0, positions=0)
            return

        expanded = self._expand_positions(cards)
        if expanded.empty:
            db_session.execute(delete(CardPositionOverall))
            db_session.commit()
            self._log_end(cards=len(cards), positions=0)
            return

        numeric_cols = sorted(set(overall.PITCHER_ATTRS + overall.HITTER_ATTRS))
        expanded = overall._normalize_common_features(expanded, numeric_cols=numeric_cols)

        weights = overall._load_true_overall_weights(self.weights_path)
        roles = dict(weights.get("roles", {}))
        if "hitter" not in roles or "pitcher" not in roles:
            raise RuntimeError("Weights JSON must include roles.hitter and roles.pitcher")

        scored = self._score_expanded(expanded, roles)
        rows = self._build_rows(scored)
        self._replace_rows(db_session, rows)

        self._log_end(cards=len(cards), positions=len(rows))

    def _fetch_cards_df(self, db_session: Session) -> pd.DataFrame:
        fields = {
            "id": Card.id,
            "name": Card.name,
            "ovr": Card.ovr,
            "height": Card.height,
            "is_hitter": Card.is_hitter,
            "bat_hand": Card.bat_hand,
            "throw_hand": Card.throw_hand,
            "display_position": Card.display_position,
            "display_secondary_positions": Card.display_secondary_positions,
        }
        for attr in sorted(set(overall.PITCHER_ATTRS + overall.HITTER_ATTRS)):
            fields[attr] = getattr(Card, attr)

        stmt = select(*[col.label(name) for name, col in fields.items()])
        quirk_stmt = select(
            card_quirk_association.c.card_id.label("card_id"),
            card_quirk_association.c.quirk_name.label("quirk_name"),
        )
        pitch_stmt = select(
            Pitch.card_id.label("card_id"),
            Pitch.name.label("name"),
            Pitch.speed.label("speed"),
            Pitch.control.label("control"),
            Pitch.movement.label("movement"),
        )

        card_rows = db_session.execute(stmt).mappings().all()
        quirk_rows = db_session.execute(quirk_stmt).mappings().all()
        pitch_rows = db_session.execute(pitch_stmt).mappings().all()

        if not card_rows:
            return pd.DataFrame(columns=list(fields.keys()) + ["quirk_names", "quirk_names_display", "pitch_rows"])

        quirk_map: Dict[str, set[str]] = {}
        quirk_display_map: Dict[str, List[str]] = {}
        for row in quirk_rows:
            card_id = row["card_id"]
            quirk_name = str(row["quirk_name"] or "").strip()
            if not card_id or not quirk_name:
                continue
            quirk_map.setdefault(card_id, set()).add(quirk_name)
            quirk_display_map.setdefault(card_id, []).append(quirk_name)

        pitch_map: Dict[str, List[Dict[str, Any]]] = {}
        for row in pitch_rows:
            card_id = row["card_id"]
            pitch_name = str(row["name"] or "").strip()
            if not card_id or not pitch_name:
                continue
            pitch_map.setdefault(card_id, []).append(
                {
                    "name": pitch_name,
                    "speed": overall._to_finite_float(row["speed"], default=0.0),
                    "control": overall._to_finite_float(row["control"], default=0.0),
                    "movement": overall._to_finite_float(row["movement"], default=0.0),
                }
            )

        df = pd.DataFrame(card_rows)
        df["quirk_names"] = df["id"].map(lambda cid: sorted(quirk_map.get(cid, set())))
        df["quirk_names_display"] = df["id"].map(lambda cid: sorted(set(quirk_display_map.get(cid, []))))
        df["pitch_rows"] = df["id"].map(lambda cid: list(pitch_map.get(cid, [])))
        return df

    def _expand_positions(self, cards_df: pd.DataFrame) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []

        for row in cards_df.to_dict(orient="records"):
            primary = overall._clean_main_position(row.get("display_position"))
            secondary = [p for p in overall._split_secondary_positions(row.get("display_secondary_positions")) if p]
            is_hitter = bool(row.get("is_hitter") == True)  # noqa: E712

            source_playable_positions: List[str] = []
            for p in [primary] + secondary:
                clean = overall._clean_main_position(p)
                if clean and clean not in source_playable_positions:
                    source_playable_positions.append(clean)

            if is_hitter:
                playable_positions = list(overall.HITTER_EVALUATED_POSITIONS)
            else:
                playable_positions = list(source_playable_positions)

            if not playable_positions:
                continue

            source_secondary = ", ".join(secondary)
            secondary_set = set(secondary)
            for pos in playable_positions:
                eval_secondary = [p for p in source_playable_positions if p != pos]
                if is_hitter:
                    position_penalty_pct, position_penalty_tier = overall._hitter_position_penalty(
                        primary_position=primary,
                        evaluated_position=pos,
                        secondary_positions=list(secondary_set),
                    )
                else:
                    position_penalty_pct = 0.0
                    position_penalty_tier = "none"

                out = dict(row)
                out["source_display_position"] = primary
                out["source_secondary_positions"] = source_secondary
                out["evaluated_position"] = pos
                out["is_primary_eval"] = pos == primary
                out["display_position"] = pos
                out["display_secondary_positions"] = ", ".join(eval_secondary)
                out["position_penalty_pct"] = position_penalty_pct
                out["position_penalty_tier"] = position_penalty_tier
                rows.append(out)

        return pd.DataFrame(rows)

    def _score_expanded(self, expanded_df: pd.DataFrame, roles: Dict[str, Any]) -> pd.DataFrame:
        scored_frames: List[pd.DataFrame] = []
        expanded_df = overall._apply_hitter_position_penalties(expanded_df)

        if bool((expanded_df["is_hitter"] == True).any()):  # noqa: E712
            scored_frames.append(
                overall._run_role(
                    cards=expanded_df,
                    is_hitter_value=True,
                    quirk_boosts=overall.HITTER_QUIRK_BOOSTS,
                    role_weights=roles["hitter"],
                )
            )

        if bool((expanded_df["is_hitter"] == False).any()):  # noqa: E712
            scored_frames.append(
                overall._run_role(
                    cards=expanded_df,
                    is_hitter_value=False,
                    quirk_boosts=overall.PITCHER_QUIRK_BOOSTS,
                    role_weights=roles["pitcher"],
                )
            )

        if not scored_frames:
            return pd.DataFrame()

        scored = pd.concat(scored_frames, ignore_index=True)

        # Raw true-overall score directly from exported linear weights (same logic as true_overall inference).
        true_raw = pd.Series(0.0, index=scored.index, dtype=float)
        hitter_mask = scored["is_hitter"] == True  # noqa: E712
        pitcher_mask = scored["is_hitter"] == False  # noqa: E712

        if bool(hitter_mask.any()):
            true_raw.loc[hitter_mask] = overall._predict_role_from_weights(scored.loc[hitter_mask], roles["hitter"])
        if bool(pitcher_mask.any()):
            true_raw.loc[pitcher_mask] = overall._predict_role_from_weights(scored.loc[pitcher_mask], roles["pitcher"])

        scored["true_overall_model"] = true_raw.astype(float)
        scored["true_overall_model_rounded"] = scored["true_overall_model"].round().astype(int)
        return scored

    def _build_rows(self, scored_df: pd.DataFrame) -> List[Dict[str, Any]]:
        if scored_df.empty:
            return []

        computed_at = datetime.now(timezone.utc)
        rows: List[Dict[str, Any]] = []

        for row in scored_df.to_dict(orient="records"):
            position = overall._clean_main_position(row.get("evaluated_position") or row.get("display_position"))
            card_id = str(row.get("id") or "").strip()
            if not card_id or not position:
                continue

            true_overall = float(row.get("true_overall_model", 0.0) or 0.0)
            meta_overall = float(row.get("meta_overall", 0.0) or 0.0)

            rows.append(
                {
                    "card_id": card_id,
                    "position": position,
                    "is_primary": bool(row.get("is_primary_eval", False)),
                    "is_hitter": bool(row.get("is_hitter", False)),
                    "true_overall": true_overall,
                    "true_overall_rounded": int(round(true_overall)),
                    "meta_overall": meta_overall,
                    "meta_overall_rounded": int(round(meta_overall)),
                    "computed_at": computed_at,
                }
            )

        return rows

    def _replace_rows(self, db_session: Session, rows: List[Dict[str, Any]], chunk_size: int = 5000) -> None:
        db_session.execute(text("SET LOCAL synchronous_commit TO OFF"))
        db_session.execute(delete(CardPositionOverall))

        if not rows:
            db_session.commit()
            return

        updatable_cols = {
            "is_primary": insert(CardPositionOverall).excluded.is_primary,
            "is_hitter": insert(CardPositionOverall).excluded.is_hitter,
            "true_overall": insert(CardPositionOverall).excluded.true_overall,
            "true_overall_rounded": insert(CardPositionOverall).excluded.true_overall_rounded,
            "meta_overall": insert(CardPositionOverall).excluded.meta_overall,
            "meta_overall_rounded": insert(CardPositionOverall).excluded.meta_overall_rounded,
            "computed_at": insert(CardPositionOverall).excluded.computed_at,
        }

        for start in range(0, len(rows), chunk_size):
            chunk = rows[start : start + chunk_size]
            stmt = (
                insert(CardPositionOverall)
                .values(chunk)
                .on_conflict_do_update(index_elements=["card_id", "position"], set_=updatable_cols)
            )
            db_session.execute(stmt)

        db_session.commit()
