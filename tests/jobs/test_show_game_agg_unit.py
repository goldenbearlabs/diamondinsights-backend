from types import SimpleNamespace

import pytest

from apps.jobs.show_game_agg import (
    RECORD_FURTHEST_HR,
    RECORD_FURTHEST_HR_PLUS,
    RECORD_HARDEST_HIT,
    ShowGameAgg,
)


def _agg() -> ShowGameAgg:
    # Avoid Spaces env dependencies for pure helper tests.
    return ShowGameAgg.__new__(ShowGameAgg)


def test_collect_record_candidates_extracts_expected_rows():
    agg = _agg()
    game = SimpleNamespace(
        id="g-1",
        home_profile_username="home_user",
        away_profile_username="away_user",
        difficulty="legend",
    )
    pas = [
        {
            "event_seq": 7,
            "result": "home_run",
            "hr_distance_ft": 440,
            "exit_vel_mph": 110,
            "is_home_batting": True,
            "batter_mlb_id": 101,
            "pitcher_mlb_id": 202,
        },
        {
            "event_seq": 8,
            "result": "lineout",
            "hr_distance_ft": None,
            "exit_vel_mph": 104,
            "is_home_batting": False,
            "batter_mlb_id": 303,
            "pitcher_mlb_id": 404,
        },
        {
            "event_seq": None,
            "result": "home_run",
            "hr_distance_ft": 450,
            "exit_vel_mph": 111,
        },
    ]
    homeruns = []
    hard_hit = []

    agg._collect_record_candidates(
        game=game,
        pas=pas,
        elevation=1200,
        homerun_candidates=homeruns,
        hard_hit_candidates=hard_hit,
    )

    assert len(homeruns) == 1
    assert homeruns[0]["game_id"] == "g-1"
    assert homeruns[0]["event_id"] == 7
    assert homeruns[0]["value"] == 440.0
    assert homeruns[0]["hitter_username"] == "home_user"
    assert homeruns[0]["pitcher_username"] == "away_user"
    assert homeruns[0]["difficulty"] == "legend"
    assert homeruns[0]["elevation"] == 1200.0

    assert len(hard_hit) == 2
    assert hard_hit[0]["event_id"] == 7
    assert hard_hit[0]["hitter_username"] == "home_user"
    assert hard_hit[0]["pitcher_username"] == "away_user"
    assert hard_hit[1]["event_id"] == 8
    assert hard_hit[1]["hitter_username"] == "away_user"
    assert hard_hit[1]["pitcher_username"] == "home_user"


def test_fit_elevation_slope_and_zero_variance_fallback():
    agg = _agg()

    candidates = [
        {"elevation": 0, "value": 300.0},
        {"elevation": 500, "value": 350.0},
        {"elevation": 1000, "value": 400.0},
    ]
    assert agg._fit_elevation_slope(candidates) == pytest.approx(0.1)

    no_variance = [
        {"elevation": 1000, "value": 370.0},
        {"elevation": 1000, "value": 390.0},
    ]
    assert agg._fit_elevation_slope(no_variance) == 0.0


def test_build_records_rows_ranks_by_combo_and_limits_top_1000():
    agg = _agg()

    homeruns = []
    for i in range(1005):
        homeruns.append(
            {
                "game_id": f"g{i}",
                "event_id": i,
                "batter_mlb_id": i,
                "pitcher_mlb_id": i + 1,
                "hitter_username": "h",
                "pitcher_username": "p",
                "difficulty": "legend",
                "value": float(300 + i),
                "elevation": 0.0,
            }
        )
    homeruns.extend(
        [
            {
                "game_id": "g-rookie-1",
                "event_id": 1,
                "batter_mlb_id": 1,
                "pitcher_mlb_id": 2,
                "hitter_username": "h1",
                "pitcher_username": "p1",
                "difficulty": "rookie",
                "value": 380.0,
                "elevation": 0.0,
            },
            {
                "game_id": "g-rookie-2",
                "event_id": 2,
                "batter_mlb_id": 1,
                "pitcher_mlb_id": 2,
                "hitter_username": "h1",
                "pitcher_username": "p1",
                "difficulty": "rookie",
                "value": 390.0,
                "elevation": 0.0,
            },
        ]
    )
    hard_hit = [
        {
            "game_id": "hh-1",
            "event_id": 1,
            "batter_mlb_id": 10,
            "pitcher_mlb_id": 20,
            "hitter_username": "h1",
            "pitcher_username": "p1",
            "difficulty": "legend",
            "value": 108.0,
        },
        {
            "game_id": "hh-2",
            "event_id": 2,
            "batter_mlb_id": 10,
            "pitcher_mlb_id": 20,
            "hitter_username": "h1",
            "pitcher_username": "p1",
            "difficulty": "legend",
            "value": 112.0,
        },
        {
            "game_id": "hh-3",
            "event_id": 3,
            "batter_mlb_id": 10,
            "pitcher_mlb_id": 20,
            "hitter_username": "h1",
            "pitcher_username": "p1",
            "difficulty": "rookie",
            "value": 105.0,
        },
    ]

    records = agg._build_records_rows(homeruns, hard_hit)

    hr_legend = [r for r in records if r["record"] == RECORD_FURTHEST_HR and r["difficulty"] == "legend"]
    assert len(hr_legend) == 1000
    assert [r["record_rank"] for r in hr_legend] == list(range(1, 1001))
    assert hr_legend[0]["value"] == 1304.0
    assert hr_legend[-1]["value"] == 305.0

    hr_plus_legend = [r for r in records if r["record"] == RECORD_FURTHEST_HR_PLUS and r["difficulty"] == "legend"]
    assert len(hr_plus_legend) == 1000
    assert [r["record_rank"] for r in hr_plus_legend] == list(range(1, 1001))
    assert hr_plus_legend[0]["value"] == 1304.0

    hr_rookie = [r for r in records if r["record"] == RECORD_FURTHEST_HR and r["difficulty"] == "rookie"]
    assert len(hr_rookie) == 2
    assert [r["record_rank"] for r in hr_rookie] == [1, 2]
    assert [r["value"] for r in hr_rookie] == [390.0, 380.0]

    hardest_hit_legend = [r for r in records if r["record"] == RECORD_HARDEST_HIT and r["difficulty"] == "legend"]
    assert len(hardest_hit_legend) == 2
    assert [r["record_rank"] for r in hardest_hit_legend] == [1, 2]
    assert [r["value"] for r in hardest_hit_legend] == [112.0, 108.0]


def test_is_pas_sorted_by_event_seq_handles_none_at_end():
    agg = _agg()
    pas = [
        {"event_seq": 1},
        {"event_seq": 2},
        {"event_seq": 2},
        {"event_seq": None},
    ]
    assert agg._is_pas_sorted_by_event_seq(pas) is True

    unsorted_pas = [
        {"event_seq": 2},
        {"event_seq": 1},
        {"event_seq": None},
    ]
    assert agg._is_pas_sorted_by_event_seq(unsorted_pas) is False


def test_run_processes_each_game_once_and_fans_out_to_both_users():
    agg = _agg()

    class FakeSession:
        def scalars(self, _stmt):
            return ["u1", "u2", "u3"]

    games = [
        SimpleNamespace(id="g1", home_profile_username="u1", away_profile_username="u2"),
        SimpleNamespace(id="g2", home_profile_username="u2", away_profile_username="u3"),
    ]
    bundles = {
        "g1": {
            "plate_appearances": [{"event_seq": 1}],
            "batting_boxscores": [{"mlb_id": 10, "ab": 1}],
            "pitching_boxscores": [{"mlb_id": 20, "outs_pitched": 3}],
        },
        "g2": {
            "plate_appearances": [{"event_seq": 2}],
            "batting_boxscores": [{"mlb_id": 11, "ab": 2}],
            "pitching_boxscores": [{"mlb_id": 21, "outs_pitched": 6}],
        },
    }
    load_calls = []
    parquet_writes = {}
    records_write = {}

    agg._fetch_all_games = lambda _db_session: games
    agg._fetch_ballpark_elevations = lambda _db_session: {}
    agg._build_facts_for_games = lambda _game, _bundle: None
    agg._collect_record_candidates = lambda **_kwargs: None

    def fake_load(game_id):
        load_calls.append(game_id)
        return bundles[game_id]

    def fake_put_parquet(key, rows):
        parquet_writes[key] = list(rows)

    def fake_put_records_parquet(key, rows):
        records_write["key"] = key
        records_write["rows"] = list(rows)

    agg._load_game_bundle = fake_load
    agg._put_parquet = fake_put_parquet
    agg._put_records_parquet = fake_put_records_parquet

    agg.run(FakeSession())

    assert load_calls == ["g1", "g2"]
    assert len(parquet_writes["facts/u1/pas.parquet"]) == 1
    assert len(parquet_writes["facts/u2/pas.parquet"]) == 2
    assert len(parquet_writes["facts/u3/pas.parquet"]) == 1
    assert records_write["key"] == "records/records.parquet"
