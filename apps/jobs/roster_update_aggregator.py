from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from apps.jobs.job import Job
from shared.db.models import CardUpdate, RosterUpdate, UserPrediction, UserUpdateScore


@dataclass(frozen=True)
class PredictionComparison:
    user_id: int
    card_id: str
    predicted_ovr: int
    old_ovr: int
    new_ovr: int


@dataclass
class UserScoreSummary:
    votes_count: int = 0
    correct_count: int = 0
    raw_points: float = 0.0


class RosterUpdateAggregator(Job):
    DIAMOND_THRESHOLD = 85

    NEAR_MISS_BASE = 0.40
    DISTANCE_ERROR_CAP = 12
    DISTANCE_PENALTY_PER_OVR = 0.05
    DISTANCE_PENALTY_CAP = 0.60

    HIGH_OVR_BASELINE = 60
    HIGH_OVR_SCALE_STEP = 0.004
    HIGH_OVR_SCALE_CAP = 1.20

    JUMP_BONUS_STEP = 0.10
    JUMP_BONUS_CAP = 0.80
    DIAMOND_BONUS = 0.30

    QUALITY_FACTOR_BASE = 0.60
    QUALITY_FACTOR_WEIGHT = 0.90
    VOLUME_FACTOR_BASE = 0.65
    VOLUME_FACTOR_WEIGHT = 0.85
    VOLUME_TARGET = 200
    STABILITY_BONUS_FROM_VOLUME = 2.0
    STABILITY_BONUS_FROM_ACCURACY = 1.5

    def run(self, db_session: Session) -> None:
        self._log_start()

        latest_update = self._get_latest_roster_update(db_session)
        if latest_update is None:
            self.logger.info("roster-update-aggregator skipped reason=no_roster_update")
            return

        rows = self._load_prediction_comparisons(
            db_session=db_session,
            update_id=latest_update.id,
            update_date=latest_update.date,
        )
        if not rows:
            self.logger.info(
                "roster-update-aggregator skipped reason=no_matching_predictions update_id=%s update_date=%s",
                latest_update.id,
                latest_update.date,
            )
            return

        summaries = self._build_user_summaries(rows)
        now = dt.datetime.now(dt.timezone.utc)

        for user_id, summary in summaries.items():
            points_total = self._final_points_total(summary)
            db_session.merge(
                UserUpdateScore(
                    user_id=user_id,
                    update_id=latest_update.id,
                    update_date=latest_update.date,
                    votes_count=summary.votes_count,
                    correct_count=summary.correct_count,
                    points_total=points_total,
                    computed_at=now,
                )
            )

        try:
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

        self._log_end(
            update_id=latest_update.id,
            update_date=latest_update.date,
            users_scored=len(summaries),
            predictions_scored=len(rows),
        )

    def _get_latest_roster_update(self, db_session: Session) -> Optional[RosterUpdate]:
        return db_session.scalar(
            select(RosterUpdate)
            .order_by(RosterUpdate.date.desc(), RosterUpdate.id.desc())
            .limit(1)
        )

    def _load_prediction_comparisons(
        self,
        db_session: Session,
        *,
        update_id: int,
        update_date: dt.date,
    ) -> List[PredictionComparison]:
        query = (
            select(
                UserPrediction.user_id,
                UserPrediction.card_id,
                UserPrediction.predicted_ovr,
                CardUpdate.old_ovr,
                CardUpdate.new_ovr,
            )
            .join(CardUpdate, CardUpdate.card_id == UserPrediction.card_id)
            .where(
                CardUpdate.update_id == update_id,
                CardUpdate.update_date == update_date,
            )
        )

        rows = db_session.execute(query).all()
        return [
            PredictionComparison(
                user_id=int(row.user_id),
                card_id=str(row.card_id),
                predicted_ovr=int(row.predicted_ovr),
                old_ovr=int(row.old_ovr),
                new_ovr=int(row.new_ovr),
            )
            for row in rows
        ]

    def _build_user_summaries(
        self,
        comparisons: Iterable[PredictionComparison],
    ) -> Dict[int, UserScoreSummary]:
        out: Dict[int, UserScoreSummary] = {}

        for row in comparisons:
            summary = out.setdefault(row.user_id, UserScoreSummary())
            score, is_correct = self._score_prediction(row)

            summary.votes_count += 1
            summary.raw_points += score
            if is_correct:
                summary.correct_count += 1

        return out

    def _score_prediction(self, row: PredictionComparison) -> tuple[float, bool]:
        error = abs(row.predicted_ovr - row.new_ovr)
        is_correct = error == 0

        if is_correct:
            score = 1.0
        else:
            closeness = max(0.0, 1.0 - (float(error) / float(self.DISTANCE_ERROR_CAP)))
            near_miss_points = self.NEAR_MISS_BASE * closeness
            distance_penalty = min(self.DISTANCE_PENALTY_CAP, error * self.DISTANCE_PENALTY_PER_OVR)
            score = max(0.0, near_miss_points - distance_penalty)

        ovr_scale = 1.0 + max(0, row.new_ovr - self.HIGH_OVR_BASELINE) * self.HIGH_OVR_SCALE_STEP
        ovr_scale = min(self.HIGH_OVR_SCALE_CAP, ovr_scale)
        score *= ovr_scale

        if is_correct:
            jump = abs(row.new_ovr - row.old_ovr)
            score += min(self.JUMP_BONUS_CAP, jump * self.JUMP_BONUS_STEP)

        if self._predicted_diamond_transition(old_ovr=row.old_ovr, predicted_ovr=row.predicted_ovr, new_ovr=row.new_ovr):
            score += self.DIAMOND_BONUS

        return score, is_correct

    def _predicted_diamond_transition(self, *, old_ovr: int, predicted_ovr: int, new_ovr: int) -> bool:
        actual_new_diamond = old_ovr < self.DIAMOND_THRESHOLD <= new_ovr
        predicted_new_diamond = old_ovr < self.DIAMOND_THRESHOLD <= predicted_ovr
        if actual_new_diamond and predicted_new_diamond:
            return True

        actual_downgraded_diamond = old_ovr >= self.DIAMOND_THRESHOLD and new_ovr < self.DIAMOND_THRESHOLD
        predicted_downgraded_diamond = old_ovr >= self.DIAMOND_THRESHOLD and predicted_ovr < self.DIAMOND_THRESHOLD
        return actual_downgraded_diamond and predicted_downgraded_diamond

    def _final_points_total(self, summary: UserScoreSummary) -> float:
        if summary.votes_count <= 0:
            return 0.0

        accuracy = summary.correct_count / float(summary.votes_count)
        volume_ratio = min(1.0, math.log1p(summary.votes_count) / math.log1p(self.VOLUME_TARGET))

        quality_factor = self.QUALITY_FACTOR_BASE + (self.QUALITY_FACTOR_WEIGHT * (accuracy ** 1.2))
        volume_factor = self.VOLUME_FACTOR_BASE + (self.VOLUME_FACTOR_WEIGHT * volume_ratio)
        stability_bonus = (
            (self.STABILITY_BONUS_FROM_VOLUME * volume_ratio)
            + (self.STABILITY_BONUS_FROM_ACCURACY * accuracy)
        )

        total = (summary.raw_points * quality_factor * volume_factor) + stability_bonus
        return max(0.0, round(total, 4))
