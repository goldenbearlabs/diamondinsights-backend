from types import SimpleNamespace

import pandas as pd

from apps.jobs.your_ovr_sync import YourOvrSync


def _sync() -> YourOvrSync:
    sync = YourOvrSync.__new__(YourOvrSync)
    sync.logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
    )
    sync.min_pa_per_mlb_id = 2
    sync.weight_smoothing_pa = 20.0
    sync.weight_min = 0.75
    sync.weight_max = 1.25
    return sync


def test_build_user_rows_only_includes_user_hitting_and_pitching_ids():
    sync = _sync()

    pas_df = pd.DataFrame(
        [
            {
                "result": "home_run",
                "is_strikeout": False,
                "is_sac_fly": False,
                "is_sac_bunt": False,
                "runs_scored": 1,
                "rbi": 1,
                "is_double_play": False,
                "is_out": False,
                "is_home_batting": True,
                "batter_mlb_id": 10,
                "pitcher_mlb_id": 900,
                "home_profile_username": "user1",
                "away_profile_username": "opp1",
            },
            {
                "result": "single",
                "is_strikeout": False,
                "is_sac_fly": False,
                "is_sac_bunt": False,
                "runs_scored": 0,
                "rbi": 0,
                "is_double_play": False,
                "is_out": False,
                "is_home_batting": True,
                "batter_mlb_id": 10,
                "pitcher_mlb_id": 901,
                "home_profile_username": "user1",
                "away_profile_username": "opp1",
            },
            {
                "result": "strikeout",
                "is_strikeout": True,
                "is_sac_fly": False,
                "is_sac_bunt": False,
                "runs_scored": 0,
                "rbi": 0,
                "is_double_play": False,
                "is_out": True,
                "is_home_batting": False,
                "batter_mlb_id": 902,
                "pitcher_mlb_id": 20,
                "home_profile_username": "user1",
                "away_profile_username": "opp1",
            },
            {
                "result": "home_run",
                "is_strikeout": False,
                "is_sac_fly": False,
                "is_sac_bunt": False,
                "runs_scored": 1,
                "rbi": 1,
                "is_double_play": False,
                "is_out": False,
                "is_home_batting": False,
                "batter_mlb_id": 903,
                "pitcher_mlb_id": 21,
                "home_profile_username": "user1",
                "away_profile_username": "opp1",
            },
        ]
    )

    rows = sync._build_user_rows("user1", pas_df)

    by_role = {(r["role"], r["mlb_id"]): r for r in rows}
    assert ("hitting", 10) in by_role
    assert ("pitching", 20) in by_role
    assert ("pitching", 21) in by_role

    # Opponent batter IDs should never become user's hitter rows.
    assert ("hitting", 902) not in by_role
    assert ("hitting", 903) not in by_role

    assert by_role[("hitting", 10)]["pa"] == 2
    assert by_role[("pitching", 20)]["pa"] == 1
    assert by_role[("pitching", 21)]["pa"] == 1


def test_append_global_rankings_assigns_rank_only_for_eligible_samples():
    sync = _sync()
    sync.min_pa_per_mlb_id = 10

    ranked = sync._append_global_rankings(
        [
            {"username": "u1", "role": "hitting", "mlb_id": 100, "pa": 25, "weight": 1.20},
            {"username": "u2", "role": "hitting", "mlb_id": 100, "pa": 15, "weight": 1.05},
            {"username": "u3", "role": "hitting", "mlb_id": 100, "pa": 5, "weight": 1.40},
            {"username": "u4", "role": "pitching", "mlb_id": 200, "pa": 30, "weight": 0.90},
        ]
    )

    lookup = {(r["role"], r["mlb_id"], r["username"]): r for r in ranked}

    top = lookup[("hitting", 100, "u1")]
    second = lookup[("hitting", 100, "u2")]
    ineligible = lookup[("hitting", 100, "u3")]
    solo = lookup[("pitching", 200, "u4")]

    assert top["global_rank"] == 1
    assert second["global_rank"] == 2
    assert top["global_cohort_size"] == 2
    assert second["global_cohort_size"] == 2
    assert top["is_top_weight_for_player"] is True
    assert second["is_top_weight_for_player"] is False

    assert ineligible["global_rank"] is None
    assert ineligible["global_cohort_size"] == 2

    assert solo["global_rank"] == 1
    assert solo["global_percentile"] == 1.0


def test_discover_usernames_only_returns_pas_paths():
    sync = _sync()
    sync.facts_scan_limit = 100
    sync.spaces = SimpleNamespace(
        list_keys=lambda prefix, limit=1000: [
            "facts/alice/pas.parquet",
            "facts/alice/your_ovr.parquet",
            "facts/bob/pas.parquet",
            "facts/your_ovr_all.parquet",
            "facts/tmp/file.txt",
        ]
    )

    usernames = sync._discover_usernames_from_facts()

    assert usernames == ["alice", "bob"]


def test_validate_user_rows_does_not_warn_when_pa_matches():
    messages = []
    sync = _sync()
    sync.logger = SimpleNamespace(
        info=lambda *args, **kwargs: None,
        warning=lambda msg, *args, **kwargs: messages.append(msg % args if args else msg),
        error=lambda *args, **kwargs: None,
    )

    hitting_df = pd.DataFrame([{"x": 1}, {"x": 2}])
    pitching_df = pd.DataFrame([{"x": 1}])
    rows = [
        {"role": "hitting", "pa": 1},
        {"role": "hitting", "pa": 1},
        {"role": "pitching", "pa": 1},
    ]

    sync._validate_user_rows(
        username="user1",
        hitting_df=hitting_df,
        pitching_df=pitching_df,
        rows=rows,
    )

    assert messages == []


def test_discover_usernames_falls_back_to_db_when_listing_fails():
    sync = _sync()
    sync.db_fallback_verify_pas = False
    sync.spaces = SimpleNamespace(
        list_keys=lambda prefix, limit=1000: (_ for _ in ()).throw(RuntimeError("NoSuchKey")),
        exists=lambda key: key in {
            "facts/alice/pas.parquet",
            "di-storage/facts/bob/pas.parquet",
        },
    )

    class StubSession:
        def scalars(self, _stmt):
            return ["alice", "bob", "charlie", None, "  "]

    usernames = sync._discover_usernames_from_facts(StubSession())

    assert usernames == ["alice", "bob", "charlie"]


def test_discover_usernames_db_verify_filters_missing_pas():
    sync = _sync()
    sync.db_fallback_verify_pas = True
    sync.spaces = SimpleNamespace(
        list_keys=lambda prefix, limit=1000: (_ for _ in ()).throw(RuntimeError("NoSuchKey")),
        exists=lambda key: key in {
            "facts/alice/pas.parquet",
            "di-storage/facts/bob/pas.parquet",
        },
        get_bytes=lambda key, byte_range=None: (_ for _ in ()).throw(RuntimeError("missing")),
    )

    class StubSession:
        def scalars(self, _stmt):
            return ["alice", "bob", "charlie"]

    usernames = sync._discover_usernames_from_facts(StubSession())

    assert usernames == ["alice", "bob"]
