from types import SimpleNamespace

import pytest

from apps.jobs.show_game_agg import (
    RECORDS_HARDEST_HITS_KEY,
    RECORDS_HOME_RUNS_KEY,
    ShowGameAgg,
)


def _agg() -> ShowGameAgg:
    # Avoid Spaces env dependencies for pure helper tests.
    agg = ShowGameAgg.__new__(ShowGameAgg)
    agg.logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
    )
    return agg


def test_collect_record_candidates_extracts_expected_rows():
    agg = _agg()
    game = SimpleNamespace(
        id="g-1",
        home_profile_username="home_user",
        away_profile_username="away_user",
        difficulty="legend",
        date="2026-02-12T10:00:00Z",
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
    assert homeruns[0]["distance_ft"] == 440.0
    assert homeruns[0]["hitter_username"] == "home_user"
    assert homeruns[0]["pitcher_username"] == "away_user"
    assert homeruns[0]["difficulty"] == "legend"
    assert homeruns[0]["elevation"] == 1200.0
    assert homeruns[0]["home_profile_username"] == "home_user"
    assert homeruns[0]["away_profile_username"] == "away_user"

    assert len(hard_hit) == 2
    assert hard_hit[0]["event_id"] == 7
    assert hard_hit[0]["hitter_username"] == "home_user"
    assert hard_hit[0]["pitcher_username"] == "away_user"
    assert hard_hit[0]["exit_vel_mph"] == 110.0
    assert hard_hit[1]["event_id"] == 8
    assert hard_hit[1]["hitter_username"] == "away_user"
    assert hard_hit[1]["pitcher_username"] == "home_user"
    assert hard_hit[1]["exit_vel_mph"] == 104.0


def test_fit_elevation_slope_and_zero_variance_fallback():
    agg = _agg()

    candidates = [
        {"elevation": 0, "distance_ft": 300.0},
        {"elevation": 500, "distance_ft": 350.0},
        {"elevation": 1000, "distance_ft": 400.0},
    ]
    assert agg._fit_elevation_slope(candidates) == pytest.approx(0.1)

    no_variance = [
        {"elevation": 1000, "distance_ft": 370.0},
        {"elevation": 1000, "distance_ft": 390.0},
    ]
    assert agg._fit_elevation_slope(no_variance) == 0.0


def test_append_and_write_records_merges_and_ranks():
    agg = _agg()

    existing_home_runs = [
        {
            "game_id": "g1",
            "event_id": 1,
            "date": "2026-02-01T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "h1",
            "away_profile_username": "a1",
            "hitter_username": "h1",
            "pitcher_username": "a1",
            "batter_mlb_id": 1,
            "pitcher_mlb_id": 2,
            "is_home_batting": True,
            "elevation": 0.0,
            "distance_ft": 410.0,
            "distance_plus_ft": 410.0,
            "rank": 1,
            "difficulty_rank": 1,
            "rank_plus": 1,
            "difficulty_rank_plus": 1,
        }
    ]
    existing_hard = [
        {
            "game_id": "g1",
            "event_id": 1,
            "date": "2026-02-01T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "h1",
            "away_profile_username": "a1",
            "hitter_username": "h1",
            "pitcher_username": "a1",
            "batter_mlb_id": 1,
            "pitcher_mlb_id": 2,
            "is_home_batting": True,
            "exit_vel_mph": 108.0,
            "rank": 1,
            "difficulty_rank": 1,
        }
    ]

    written = {}
    agg._read_parquet_optional = lambda key: existing_home_runs if key == RECORDS_HOME_RUNS_KEY else existing_hard

    def fake_put_records_parquet(key, rows, schema=None):
        written[key] = [dict(r) for r in rows]

    agg._put_records_parquet = fake_put_records_parquet

    new_hr = [
        {
            "game_id": "g2",
            "event_id": 2,
            "date": "2026-02-02T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "h2",
            "away_profile_username": "a2",
            "hitter_username": "h2",
            "pitcher_username": "a2",
            "batter_mlb_id": 3,
            "pitcher_mlb_id": 4,
            "is_home_batting": False,
            "distance_ft": 430.0,
            "elevation": 1000.0,
        }
    ]
    new_hard = [
        {
            "game_id": "g2",
            "event_id": 2,
            "date": "2026-02-02T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "h2",
            "away_profile_username": "a2",
            "hitter_username": "h2",
            "pitcher_username": "a2",
            "batter_mlb_id": 3,
            "pitcher_mlb_id": 4,
            "is_home_batting": False,
            "exit_vel_mph": 112.0,
        }
    ]

    agg._append_and_write_records(new_hr, new_hard)

    assert RECORDS_HOME_RUNS_KEY in written
    assert RECORDS_HARDEST_HITS_KEY in written

    hr_rows = written[RECORDS_HOME_RUNS_KEY]
    assert len(hr_rows) == 2
    hr_rank_by_distance = {row["distance_ft"]: row["rank"] for row in hr_rows}
    assert hr_rank_by_distance[430.0] == 1
    assert hr_rank_by_distance[410.0] == 2
    assert all("distance_plus_ft" in r for r in hr_rows)
    assert all("rank_plus" in r for r in hr_rows)

    hh_rows = written[RECORDS_HARDEST_HITS_KEY]
    assert len(hh_rows) == 2
    hh_rank_by_ev = {row["exit_vel_mph"]: row["rank"] for row in hh_rows}
    assert hh_rank_by_ev[112.0] == 1
    assert hh_rank_by_ev[108.0] == 2


def test_merge_record_rows_dedupes_when_existing_is_empty():
    agg = _agg()
    new_rows = [
        {"game_id": "g1", "event_id": 1, "distance_ft": 420.0},
        {"game_id": "g1", "event_id": 1, "distance_ft": 420.0},
        {"game_id": "g1", "event_id": 2, "distance_ft": 410.0},
    ]

    merged = agg._merge_record_rows([], new_rows)

    assert len(merged) == 2
    keys = {(str(r.get("game_id")), int(r.get("event_id"))) for r in merged}
    assert keys == {("g1", 1), ("g1", 2)}


def test_merge_pas_rows_dedupes_when_existing_is_empty():
    agg = _agg()
    new_rows = [
        {
            "game_id": "g1",
            "event_seq": 1,
            "batter_mlb_id": 10,
            "pitcher_mlb_id": 20,
            "result": "single",
        },
        {
            "game_id": "g1",
            "event_seq": 1,
            "batter_mlb_id": 10,
            "pitcher_mlb_id": 20,
            "result": "single",
        },
        {
            "game_id": "g1",
            "event_seq": 2,
            "batter_mlb_id": 11,
            "pitcher_mlb_id": 20,
            "result": "home_run",
        },
    ]

    merged = agg._merge_pas_rows([], new_rows)

    assert len(merged) == 2
    keys = {
        (
            str(r.get("game_id")),
            int(r.get("event_seq")),
            int(r.get("batter_mlb_id")),
            int(r.get("pitcher_mlb_id")),
            str(r.get("result")),
        )
        for r in merged
    }
    assert keys == {
        ("g1", 1, 10, 20, "single"),
        ("g1", 2, 11, 20, "home_run"),
    }


def test_append_and_write_records_dedupes_home_runs_by_business_key():
    agg = _agg()
    agg._read_parquet_optional = lambda _key: []
    written = {}

    def fake_put_records_parquet(key, rows, schema=None):
        written[key] = [dict(r) for r in rows]

    agg._put_records_parquet = fake_put_records_parquet

    new_hr = [
        {
            "game_id": "g100",
            "event_id": 1,
            "date": "2026-02-02T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "home_u",
            "away_profile_username": "away_u",
            "hitter_username": "SomeHitter",
            "pitcher_username": "Pitcher1",
            "batter_mlb_id": 77,
            "pitcher_mlb_id": 88,
            "is_home_batting": True,
            "distance_ft": 430.0,
            "elevation": 500.0,
        },
        {
            "game_id": "g100",
            "event_id": 99,
            "date": "2026-02-02T00:00:00Z",
            "difficulty": "legend",
            "home_profile_username": "home_u",
            "away_profile_username": "away_u",
            "hitter_username": "somehitter",
            "pitcher_username": "Pitcher2",
            "batter_mlb_id": 77,
            "pitcher_mlb_id": 89,
            "is_home_batting": True,
            "distance_ft": 430,
            "elevation": 500.0,
        },
    ]

    agg._append_and_write_records(new_hr, [])

    hr_rows = written[RECORDS_HOME_RUNS_KEY]
    assert len(hr_rows) == 1
    assert hr_rows[0]["game_id"] == "g100"
    assert hr_rows[0]["distance_ft"] == 430.0
    assert hr_rows[0]["batter_mlb_id"] == 77


def test_run_processes_only_globally_unprocessed_games_and_fans_out_to_both_users():
    agg = _agg()

    class FakeSession:
        pass

    games = [
        SimpleNamespace(id="g2", home_profile_username="u2", away_profile_username="u3", ball_park_id=None),
        SimpleNamespace(id="g1", home_profile_username="u1", away_profile_username="u2", ball_park_id=None),
        SimpleNamespace(id="g2", home_profile_username="u2", away_profile_username="u3", ball_park_id=None),
    ]
    bundles = {
        "g2": {
            "plate_appearances": [{"game_id": "g2", "event_seq": 3, "result": "single"}],
            "events": [],
            "batting_boxscores": [{"mlb_id": 11, "ab": 2}],
            "pitching_boxscores": [{"mlb_id": 21, "outs_pitched": 6}],
        }
    }

    load_calls = []
    written_parquet = {}
    written_checkpoints = {}
    written_global_checkpoints = []
    record_calls = []

    agg._fetch_all_games = lambda _db_session, _usernames=None: games
    agg._fetch_ballpark_elevations = lambda _db_session: {}
    agg._read_global_checkpoint_game_ids = lambda: {"g1"}
    agg._write_global_checkpoint_game_ids = lambda game_ids: written_global_checkpoints.append(set(game_ids))
    agg._read_checkpoint_game_ids = lambda username: set()
    agg._load_user_state = lambda _username: {
        "pas_existing": [],
        "pas_new": [],
        "batting_box_agg": {},
        "pitching_box_agg": {},
    }
    agg._build_facts_for_games = lambda _game, _bundle: None

    def fake_collect(**kwargs):
        game = kwargs["game"]
        kwargs["homerun_candidates"].append(
            {
                "game_id": game.id,
                "event_id": 1,
                "date": "2026-02-02T00:00:00Z",
                "difficulty": "legend",
                "home_profile_username": game.home_profile_username,
                "away_profile_username": game.away_profile_username,
                "hitter_username": game.home_profile_username,
                "pitcher_username": game.away_profile_username,
                "batter_mlb_id": 1,
                "pitcher_mlb_id": 2,
                "is_home_batting": True,
                "distance_ft": 420.0,
                "elevation": 0.0,
            }
        )
        kwargs["hard_hit_candidates"].append(
            {
                "game_id": game.id,
                "event_id": 1,
                "date": "2026-02-02T00:00:00Z",
                "difficulty": "legend",
                "home_profile_username": game.home_profile_username,
                "away_profile_username": game.away_profile_username,
                "hitter_username": game.home_profile_username,
                "pitcher_username": game.away_profile_username,
                "batter_mlb_id": 1,
                "pitcher_mlb_id": 2,
                "is_home_batting": True,
                "exit_vel_mph": 108.0,
            }
        )

    agg._collect_record_candidates = fake_collect
    agg._append_and_write_records = lambda hr, hh: record_calls.append((list(hr), list(hh)))

    def fake_load(game_id):
        load_calls.append(game_id)
        return bundles[game_id]

    agg._load_game_bundle = fake_load
    agg._put_parquet = lambda key, rows: written_parquet.setdefault(key, list(rows))
    agg._write_checkpoint_game_ids = lambda username, game_ids: written_checkpoints.setdefault(
        username, set(game_ids)
    )

    agg.run(FakeSession())

    assert load_calls == ["g2"]
    assert "facts/u2/pas.parquet" in written_parquet
    assert "facts/u2/batting_boxscores.parquet" in written_parquet
    assert "facts/u2/pitching_boxscores.parquet" in written_parquet
    assert "facts/u3/pas.parquet" in written_parquet
    assert "facts/u3/batting_boxscores.parquet" in written_parquet
    assert "facts/u3/pitching_boxscores.parquet" in written_parquet
    assert written_checkpoints["u2"] == {"g2"}
    assert written_checkpoints["u3"] == {"g2"}
    assert written_global_checkpoints == [{"g1", "g2"}]
    assert len(record_calls) == 1
    assert len(record_calls[0][0]) == 1
    assert len(record_calls[0][1]) == 1


def test_run_skips_user_that_already_has_game_in_pas_when_global_checkpoint_is_stale():
    agg = _agg()

    class FakeSession:
        pass

    games = [
        SimpleNamespace(id="g7", home_profile_username="u1", away_profile_username="u2", ball_park_id=None),
    ]
    bundles = {
        "g7": {
            "plate_appearances": [{"game_id": "g7", "event_seq": 7, "result": "single"}],
            "events": [],
            "batting_boxscores": [{"mlb_id": 12, "ab": 4}],
            "pitching_boxscores": [{"mlb_id": 22, "outs_pitched": 9}],
        }
    }

    written_parquet = {}
    written_checkpoints = {}
    written_global_checkpoints = []

    agg._fetch_all_games = lambda _db_session, _usernames=None: games
    agg._fetch_ballpark_elevations = lambda _db_session: {}
    agg._read_global_checkpoint_game_ids = lambda: set()
    agg._write_global_checkpoint_game_ids = lambda game_ids: written_global_checkpoints.append(set(game_ids))
    agg._read_checkpoint_game_ids = lambda _username: set()

    def fake_load_user_state(username):
        pas_existing = [{"game_id": "g7", "event_seq": 1, "result": "single"}] if username == "u1" else []
        return {
            "pas_existing": pas_existing,
            "pas_new": [],
            "batting_box_agg": {},
            "pitching_box_agg": {},
        }

    agg._load_user_state = fake_load_user_state
    agg._build_facts_for_games = lambda _game, _bundle: None
    agg._collect_record_candidates = lambda **kwargs: None
    agg._append_and_write_records = lambda _hr, _hh: None
    agg._load_game_bundle = lambda game_id: bundles[game_id]
    agg._put_parquet = lambda key, rows: written_parquet.setdefault(key, list(rows))
    agg._write_checkpoint_game_ids = lambda username, game_ids: written_checkpoints.setdefault(
        username, set(game_ids)
    )

    agg.run(FakeSession())

    assert "facts/u1/pas.parquet" not in written_parquet
    assert "facts/u2/pas.parquet" in written_parquet
    assert written_checkpoints["u2"] == {"g7"}
    assert written_global_checkpoints == [{"g7"}]


def test_run_flushes_chunks_periodically_and_reloads_user_state_between_chunks():
    agg = _agg()

    class FakeSession:
        pass

    games = [
        SimpleNamespace(id="g1", home_profile_username="u1", away_profile_username="u2", ball_park_id=None),
        SimpleNamespace(id="g2", home_profile_username="u1", away_profile_username="u3", ball_park_id=None),
    ]
    bundles = {
        "g1": {
            "plate_appearances": [{"game_id": "g1", "event_seq": 1, "batter_mlb_id": 10, "pitcher_mlb_id": 20, "result": "single"}],
            "events": [],
            "batting_boxscores": [{"mlb_id": 11, "ab": 2}],
            "pitching_boxscores": [{"mlb_id": 21, "outs_pitched": 6}],
        },
        "g2": {
            "plate_appearances": [{"game_id": "g2", "event_seq": 2, "batter_mlb_id": 12, "pitcher_mlb_id": 22, "result": "home_run"}],
            "events": [],
            "batting_boxscores": [{"mlb_id": 11, "ab": 3}],
            "pitching_boxscores": [{"mlb_id": 23, "outs_pitched": 9}],
        },
    }

    parquet_store = {}
    checkpoint_store = {}
    global_checkpoint_writes = []
    operation_log = []
    record_calls = []

    def fake_env_int(name, default, minimum=1):
        if name in ("SHOW_GAME_AGG_BUNDLE_FETCH_WORKERS", "SHOW_GAME_AGG_BUNDLE_FETCH_MAX_IN_FLIGHT"):
            return 1
        if name == "SHOW_GAME_AGG_FLUSH_EVERY_GAMES":
            return 1
        return default

    def fake_load_user_state(username):
        base_prefix = f"facts/{username}"
        return {
            "pas_existing": [dict(row) for row in parquet_store.get(f"{base_prefix}/pas.parquet", [])],
            "pas_new": [],
            "batting_box_agg": agg._index_agg_rows(parquet_store.get(f"{base_prefix}/batting_boxscores.parquet", [])),
            "pitching_box_agg": agg._index_agg_rows(parquet_store.get(f"{base_prefix}/pitching_boxscores.parquet", [])),
        }

    agg._env_int = fake_env_int
    agg._fetch_all_games = lambda _db_session, _usernames=None: games
    agg._fetch_ballpark_elevations = lambda _db_session: {}
    agg._read_global_checkpoint_game_ids = lambda: set()
    agg._write_global_checkpoint_game_ids = lambda game_ids: (
        operation_log.append(("global", set(game_ids))),
        global_checkpoint_writes.append(set(game_ids)),
    )
    agg._read_checkpoint_game_ids = lambda username: set(checkpoint_store.get(username, set()))
    agg._load_user_state = fake_load_user_state
    agg._build_facts_for_games = lambda _game, _bundle: None
    agg._collect_record_candidates = lambda **kwargs: None
    agg._append_and_write_records = lambda hr, hh: record_calls.append((list(hr), list(hh)))
    agg._load_game_bundle = lambda game_id: bundles[game_id]

    def fake_put_parquet(key, rows):
        parquet_store[key] = [dict(row) for row in rows]
        operation_log.append(("parquet", key))

    def fake_write_checkpoint(username, game_ids):
        checkpoint_store[username] = set(game_ids)
        operation_log.append(("user_checkpoint", username, set(game_ids)))

    agg._put_parquet = fake_put_parquet
    agg._write_checkpoint_game_ids = fake_write_checkpoint

    agg.run(FakeSession())

    assert global_checkpoint_writes == [{"g1"}, {"g1", "g2"}]
    assert checkpoint_store["u1"] == {"g1", "g2"}
    assert checkpoint_store["u2"] == {"g1"}
    assert checkpoint_store["u3"] == {"g2"}

    u1_pas = parquet_store["facts/u1/pas.parquet"]
    assert {row["game_id"] for row in u1_pas} == {"g1", "g2"}
    assert parquet_store["facts/u2/pas.parquet"][0]["game_id"] == "g1"
    assert parquet_store["facts/u3/pas.parquet"][0]["game_id"] == "g2"

    first_global_index = next(i for i, item in enumerate(operation_log) if item[0] == "global" and item[1] == {"g1"})
    assert ("user_checkpoint", "u1", {"g1"}) in operation_log[:first_global_index]
    assert ("user_checkpoint", "u2", {"g1"}) in operation_log[:first_global_index]
    assert len(record_calls) == 1
    assert record_calls[0] == ([], [])
