from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import apps.jobs.roster_update_aggregator as agg


@dataclass
class Row:
    user_id: int
    card_id: str
    predicted_ovr: int
    old_ovr: int
    new_ovr: int


class FakeScalarSession:
    def __init__(self, latest_update):
        self.latest_update = latest_update

    def scalar(self, stmt):
        return self.latest_update


class FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class FakeRunSession:
    def __init__(self):
        self.merged = []
        self.committed = False
        self.rolled_back = False

    def merge(self, obj):
        self.merged.append(obj)
        return obj

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_score_prediction_rewards_exact_high_ovr_and_transitions():
    job = agg.RosterUpdateAggregator()
    row = agg.PredictionComparison(
        user_id=1,
        card_id="25:a",
        predicted_ovr=85,
        old_ovr=84,
        new_ovr=85,
    )

    score, is_correct = job._score_prediction(row)

    assert is_correct is True
    assert score >= 1.5


def test_score_prediction_penalizes_large_miss_more_than_small_miss():
    job = agg.RosterUpdateAggregator()

    near_row = agg.PredictionComparison(
        user_id=1,
        card_id="25:a",
        predicted_ovr=86,
        old_ovr=84,
        new_ovr=85,
    )
    far_row = agg.PredictionComparison(
        user_id=1,
        card_id="25:b",
        predicted_ovr=60,
        old_ovr=84,
        new_ovr=85,
    )

    near_score, near_correct = job._score_prediction(near_row)
    far_score, far_correct = job._score_prediction(far_row)

    assert near_correct is False
    assert far_correct is False
    assert near_score > far_score


def test_final_points_total_scales_with_volume_and_accuracy():
    job = agg.RosterUpdateAggregator()

    one_of_one = agg.UserScoreSummary(votes_count=1, correct_count=1, raw_points=1.8)
    fifty_percent_on_two_hundred = agg.UserScoreSummary(votes_count=200, correct_count=100, raw_points=100.0)

    high_volume_points = job._final_points_total(fifty_percent_on_two_hundred)
    low_volume_points = job._final_points_total(one_of_one)

    assert isinstance(high_volume_points, float)
    assert high_volume_points > low_volume_points


def test_load_prediction_comparisons_maps_rows():
    job = agg.RosterUpdateAggregator()

    class Session:
        def execute(self, stmt):
            return FakeExecuteResult(
                [
                    Row(user_id=1, card_id="25:a", predicted_ovr=83, old_ovr=82, new_ovr=84),
                    Row(user_id=2, card_id="25:b", predicted_ovr=80, old_ovr=82, new_ovr=79),
                ]
            )

    rows = job._load_prediction_comparisons(
        Session(),
        update_id=9,
        update_date=date(2026, 2, 6),
    )

    assert len(rows) == 2
    assert rows[0] == agg.PredictionComparison(user_id=1, card_id="25:a", predicted_ovr=83, old_ovr=82, new_ovr=84)
    assert rows[1] == agg.PredictionComparison(user_id=2, card_id="25:b", predicted_ovr=80, old_ovr=82, new_ovr=79)


def test_get_latest_roster_update_uses_scalar_result():
    latest = object()
    session = FakeScalarSession(latest_update=latest)
    job = agg.RosterUpdateAggregator()

    assert job._get_latest_roster_update(session) is latest


def test_run_writes_user_update_scores(monkeypatch):
    job = agg.RosterUpdateAggregator()
    session = FakeRunSession()

    update = agg.RosterUpdate(id=12, date=date(2026, 2, 6), is_major=True, is_fielding=False)
    comparisons = [
        agg.PredictionComparison(user_id=1, card_id="25:a", predicted_ovr=85, old_ovr=84, new_ovr=85),
        agg.PredictionComparison(user_id=1, card_id="25:b", predicted_ovr=79, old_ovr=80, new_ovr=78),
        agg.PredictionComparison(user_id=2, card_id="25:c", predicted_ovr=90, old_ovr=88, new_ovr=89),
    ]

    monkeypatch.setattr(job, "_get_latest_roster_update", lambda db: update)
    monkeypatch.setattr(job, "_load_prediction_comparisons", lambda db_session, update_id, update_date: comparisons)

    job.run(session)

    assert session.committed is True
    assert session.rolled_back is False
    assert len(session.merged) == 2
    assert {row.user_id for row in session.merged} == {1, 2}
    assert all(row.update_id == 12 for row in session.merged)
    assert all(row.update_date == date(2026, 2, 6) for row in session.merged)
    assert all(row.votes_count >= 1 for row in session.merged)
    assert any(row.points_total != row.correct_count for row in session.merged)


def test_run_skips_when_no_latest_update(monkeypatch):
    job = agg.RosterUpdateAggregator()
    session = FakeRunSession()
    monkeypatch.setattr(job, "_get_latest_roster_update", lambda db: None)

    job.run(session)

    assert session.merged == []
    assert session.committed is False
