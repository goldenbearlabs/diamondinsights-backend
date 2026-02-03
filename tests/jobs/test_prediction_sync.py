import builtins
import datetime as dt
import json
from pathlib import Path
from decimal import Decimal
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import apps.jobs.prediction_sync as pred


class DummyModel:
    def __init__(self, output=1.0):
        self.output = output

    def predict(self, X):
        return np.full(len(X), self.output)


class DummySession:
    def __init__(self):
        self.added = []
        self.added_all = []
        self.flushed = 0

    def add(self, obj):
        self.added.append(obj)

    def add_all(self, objs):
        self.added_all.extend(objs)

    def flush(self):
        self.flushed += 1


class DummyExecResult:
    def __init__(self, value=None, items=None):
        self._value = value
        self._items = items or []

    def scalar_one_or_none(self):
        return self._value

    def scalars(self):
        return self

    def first(self):
        return self._items[0] if self._items else None

    def all(self):
        return list(self._items)


def test_basic_helpers():
    assert pred.height_to_inches("6'2") == 74
    assert pred.height_to_inches(123) is None
    assert pred.weight_to_lbs("200 lbs") == 200
    assert pred.weight_to_lbs(200) is None
    assert pred.safe_div(10, 0) == 0.0
    assert pred.safe_div(10, 2) == 5


def test_make_naive():
    df = pd.DataFrame({"ts": [pd.Timestamp("2026-01-01", tz="UTC")]})
    out = pred.make_naive(df, "ts")
    assert out["ts"].dt.tz is None


def test_calc_batting_and_agg_batting():
    s = {"ab": 10, "h": 4, "bb": 2, "hbp": 1, "tb": 8, "so": 3}
    out = pred.calc_batting_metrics(s, "p_")
    assert out["p_avg"] == 0.4
    assert out["p_obp"] == 0.7
    assert out["p_ops"] == out["p_obp"] + out["p_slug"]

    df = pd.DataFrame([
        {"split": "vslhp", "ab": 10, "h": 3, "bb": 1, "hbp": 0, "tb": 5, "so": 2},
        {"split": "vsrhp", "ab": 10, "h": 5, "bb": 1, "hbp": 1, "tb": 9, "so": 1},
    ])
    agg = pred.agg_batting(df, "b_")
    assert "b_avg" in agg
    assert "b_vslhp_avg" in agg


def test_calc_pitching_and_agg_pitching():
    s = {"outs_pitched": 6, "ab": 10, "h": 3, "bb": 1, "hr": 1, "er": 2, "k": 4, "strikes_thrown": 20, "pitches_thrown": 40}
    out = pred.calc_pitching_metrics(s, "p_")
    assert out["p_ip"] == 2.0
    assert out["p_era"] == 9.0
    assert out["p_strike_pct"] == 0.5

    df = pd.DataFrame([
        {"split": "vslhb", "outs_pitched": 3, "ab": 5, "h": 1, "bb": 0, "hr": 0, "er": 0, "k": 1},
        {"split": "vsrhb", "outs_pitched": 3, "ab": 5, "h": 2, "bb": 1, "hr": 1, "er": 1, "k": 2},
    ])
    agg = pred.agg_pitching(df, "p_")
    assert "p_ip" in agg
    assert "p_vslhb_ip" in agg


def test_agg_baserunning_and_fielding():
    df = pd.DataFrame([{"sb": 2, "caught_stealing": 1}])
    out = pred.agg_baserunning(df, "br_")
    assert out["br_sb_attempts"] == 3
    assert out["br_sb_pct"] == 2 / 3

    df = pd.DataFrame([{"errors": 1, "chances": 4, "put_outs": 2, "assists": 1}])
    out = pred.agg_fielding(df, "f_")
    assert out["f_field_pct"] == 0.75


def test_safe_name_and_split_by_role():
    sync = pred.PredictionSync()
    assert sync._safe_name("DRG BNT") == "DRG_BNT"

    df = pd.DataFrame([
        {"display_position": "SP", "display_secondary_positions": ""},
        {"display_position": "C", "display_secondary_positions": ""},
        {"display_position": "SP", "display_secondary_positions": "C"},
    ])

    p_df, b_df, tw_df = sync._split_by_role(df)
    assert len(p_df) == 2
    assert len(b_df) == 2
    assert len(tw_df) == 1


def test_build_position_features():
    sync = pred.PredictionSync()
    df = pd.DataFrame([
        {"display_position": "", "display_secondary_positions": "LF, CF"},
        {"display_position": "SP", "display_secondary_positions": ""},
    ])

    feats = sync._build_position_features(df)
    assert "pos_main_SP" in feats.columns
    assert "pos_sec_LF" in feats.columns
    assert "pos_sec_CF" in feats.columns


def test_build_attr_input_frame_and_align():
    sync = pred.PredictionSync()
    df = pd.DataFrame([
        {
            "card_id": "c1",
            "display_position": "SP",
            "display_secondary_positions": "",
            "height": "6'2",
            "weight": "200",
            "old_ovr": 50,
        }
    ])

    X = sync._build_attr_input_frame(df)
    assert "height_in" in X.columns
    assert "weight_lb" in X.columns

    aligned = sync._align_feature_cols(X, ["old_ovr", "missing"])
    assert list(aligned.columns) == ["old_ovr", "missing"]
    assert aligned["missing"].iloc[0] == 0.0


def test_load_attr_model_cache(tmp_path, monkeypatch):
    sync = pred.PredictionSync()

    model_dir = tmp_path / "modelA"
    model_dir.mkdir()
    (model_dir / "best_model.json").write_text(json.dumps({"best_model": "x", "role": "hit", "attr": "CON L"}))
    (model_dir / "feature_cols.json").write_text(json.dumps(["old_ovr"]))
    (model_dir / "final_model.joblib").write_text("dummy")

    monkeypatch.setattr(sync, "_joblib_load", lambda path: DummyModel(output=2.0))

    spec = sync._load_attr_model(model_dir)
    assert spec[0] == "x"

    cached = sync._load_attr_model(model_dir)
    assert cached is spec


def test_predict_attr_models(tmp_path, monkeypatch):
    sync = pred.PredictionSync()

    model_root = tmp_path / "attr_models"
    model_root.mkdir()
    model_dir = model_root / "m1"
    model_dir.mkdir()

    def fake_load_attr(_dir):
        return ("key", DummyModel(output=3.0), ["old_ovr"], {"role": "hit", "attr": "CON L"})

    monkeypatch.setattr(sync, "_attr_models_dir", lambda: model_root)
    monkeypatch.setattr(sync, "_load_attr_model", fake_load_attr)

    df = pd.DataFrame([{"card_id": "c1", "old_ovr": 50}])
    preds = sync._predict_attr_models(df, role="hit")

    assert "pred_CON_L" in preds.columns
    assert preds["pred_CON_L"].iloc[0] == 3.0


def test_load_ovr_model(tmp_path, monkeypatch):
    sync = pred.PredictionSync()

    model_dir = tmp_path / "ovr" / "hit" / "field_on"
    model_dir.mkdir(parents=True)
    (model_dir / "feature_cols.json").write_text(json.dumps(["old_ovr", "pred_CON_L_new"]))
    (model_dir / "ovr_model.joblib").write_text("dummy")

    monkeypatch.setattr(sync, "_joblib_load", lambda path: DummyModel(output=5.0))
    monkeypatch.setattr(sync, "_ovr_models_dir", lambda: tmp_path / "ovr")

    model, cols = sync._load_ovr_model("hit", "field_on")
    assert cols == ["old_ovr", "pred_CON_L_new"]


def test_build_ovr_input_and_predict():
    sync = pred.PredictionSync()

    role_df = pd.DataFrame([
        {"card_id": "c1", "old_ovr": 50, "CON L_old": 10, "display_position": "", "display_secondary_positions": ""},
    ])
    attr_preds = pd.DataFrame([
        {"card_id": "c1", "pred_CON_L": 20}
    ])

    feature_cols = ["old_ovr", "pred_CON_L_new"]

    X = sync._build_ovr_input(role_df, attr_preds, feature_cols, field_mode="field_on")
    assert X["pred_CON_L_new"].iloc[0] == 20

    X_off = sync._build_ovr_input(role_df, attr_preds, feature_cols, field_mode="field_off")
    assert X_off["pred_CON_L_new"].iloc[0] == 20

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(sync, "_load_ovr_model", lambda role, field_mode: (DummyModel(output=7.0), feature_cols))
    out = sync._predict_ovr_for_role(role_df, attr_preds, role="hit")
    assert "field_on" in out
    monkeypatch.undo()


def test_combine_ovr_predictions():
    sync = pred.PredictionSync()

    hit = {"field_on": {"preds": pd.Series({"c1": 10.0}), "attrs": pd.DataFrame({"old_ovr": [1]}, index=["c1"])}}
    pit = {"field_on": {"preds": pd.Series({"c1": np.nan, "c2": 20.0}), "attrs": pd.DataFrame({"old_ovr": [2, 3]}, index=["c1", "c2"])}}

    combined = sync._combine_ovr_predictions(hit, pit)
    assert combined["field_on"]["preds"]["c1"] == 10.0
    assert combined["field_on"]["preds"]["c2"] == 20.0


def test_persist_predictions():
    sync = pred.PredictionSync()
    session = DummySession()

    combined = {
        "field_on": {"preds": {"c1": 50.5, "c2": np.nan}, "attrs": {"c1": {"x": 1}}},
        "field_off": {"preds": {"c1": 60.0}, "attrs": {}},
    }

    sync._persist_predictions(session, combined)

    assert len(session.added) == 2
    assert len(session.added_all) == 2
    assert session.flushed == 2


def test_sparsity_drop_and_rates():
    sync = pred.PredictionSync()
    df = pd.DataFrame({"a": ["", "0"], "b": [1, 2], "keep_BRK": ["", "1"]})

    rates = sync._sparsity_rates(df)
    assert rates["a"] == 1.0

    pruned, sr, dropped, protected = sync._drop_sparse_cols(df, thresh=0.9, keep_if_contains=["BRK"])
    assert "a" in dropped
    assert "keep_BRK" in protected

    pruned2, removed = sync._drop_by_substring(df, ["keep"])
    assert "keep_BRK" in removed


def test_add_split_league_shrunk_rates():
    sync = pred.PredictionSync()
    df = pd.DataFrame({
        "update_date": [1, 1],
        "since_pa": [10, 20],
        "since_r": [5, 3],
        "since_vslhp_pa": [5, 5],
        "since_vslhp_r": [1, 2],
    })

    out = sync._add_split_league_shrunk_rates(
        df,
        windows=["since"],
        split_prefixes=["", "vslhp_"],
        denom_name="pa",
        numerators=["r"],
        k_by_window={"since": 10.0},
        split_k_divisors={"": 1.0, "vslhp_": 2.0},
        group_col="update_date",
        add_raw_rate=True,
    )

    assert "since_r_per_pa" in out.columns
    assert "since_vslhp_r_shrunk_per_pa" in out.columns

    with pytest.raises(ValueError):
        sync._add_split_league_shrunk_rates(df, ["since"], [""], "pa", ["r"], {}, {}, group_col="update_date")

    with pytest.raises(ValueError):
        sync._add_split_league_shrunk_rates(df, ["since"], [""], "pa", ["r"], {"since": 1.0}, {}, group_col="missing")


def test_build_role_feature_frames_calls(monkeypatch):
    sync = pred.PredictionSync()
    df = pd.DataFrame({
        "display_position": ["SP"],
        "display_secondary_positions": [""],
        "update_date": [1],
        "since_pa": [1],
        "since_r": [1],
        "since_pab": [1],
        "since_ph": [1],
    })

    def fake_add(df_in, **kwargs):
        return df_in

    monkeypatch.setattr(sync, "_add_split_league_shrunk_rates", fake_add)

    p_df, b_df, tw_df = sync._build_role_feature_frames(df)
    assert not p_df.empty
    assert b_df.empty is False
    assert tw_df.empty


def test_agg_helpers_without_split():
    df_b = pd.DataFrame([{"ab": 10, "h": 4, "bb": 1, "hbp": 0, "tb": 6, "so": 2}])
    out_b = pred.agg_batting(df_b, "x_")
    assert out_b["x_avg"] == 0.4

    df_p = pd.DataFrame([{"outs_pitched": 3, "ab": 3, "h": 1, "bb": 0, "hr": 0, "er": 1, "k": 1}])
    out_p = pred.agg_pitching(df_p, "y_")
    assert out_p["y_ip"] == 1.0


def test_configure_lightgbm_logging_import_error(monkeypatch):
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "lightgbm":
            raise ImportError("no lightgbm")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    pred.PredictionSync()


def test_run_paths(monkeypatch):
    sync = pred.PredictionSync()
    session = object()

    monkeypatch.setattr(sync, "_get_latest_year", lambda db: None)
    sync.run(session)

    monkeypatch.setattr(sync, "_get_latest_year", lambda db: 2026)
    monkeypatch.setattr(sync, "_get_latest_roster_update", lambda db: None)
    sync.run(session)

    monkeypatch.setattr(sync, "_get_latest_roster_update", lambda db: SimpleNamespace(date=dt.date(2026, 1, 1)))
    monkeypatch.setattr(sync, "_load_live_series_cards", lambda db, year: [])
    sync.run(session)

    monkeypatch.setattr(sync, "_load_live_series_cards", lambda db, year: [object()])
    monkeypatch.setattr(sync, "_build_feature_frame", lambda **kwargs: pd.DataFrame())
    sync.run(session)

    monkeypatch.setattr(sync, "_build_feature_frame", lambda **kwargs: pd.DataFrame([{"card_id": 1}]))
    monkeypatch.setattr(
        sync,
        "_build_role_feature_frames",
        lambda features: (pd.DataFrame([{"card_id": 1}]), pd.DataFrame([{"card_id": 1}]), pd.DataFrame()),
    )
    monkeypatch.setattr(sync, "_predict_attr_models", lambda df, role: pd.DataFrame())
    monkeypatch.setattr(sync, "_predict_ovr_for_role", lambda df, preds, role: {})
    monkeypatch.setattr(sync, "_combine_ovr_predictions", lambda hit, pit: {})
    sync.run(session)

    called = {}
    monkeypatch.setattr(
        sync,
        "_combine_ovr_predictions",
        lambda hit, pit: {"field_on": {"preds": {"c1": 1.0}, "attrs": {}}},
    )
    monkeypatch.setattr(sync, "_persist_predictions", lambda db, combined: called.setdefault("combined", combined))
    sync.run(session)
    assert "combined" in called


def test_latest_year_roster_and_cards_queries():
    sync = pred.PredictionSync()

    class DummySession:
        def __init__(self, result):
            self._result = result

        def execute(self, stmt):
            return self._result

    assert sync._get_latest_year(DummySession(DummyExecResult(value=2024))) == 2024
    assert sync._get_latest_year(DummySession(DummyExecResult(value=None))) is None

    roster_obj = object()
    roster_session = DummySession(DummyExecResult(items=[roster_obj]))
    assert sync._get_latest_roster_update(roster_session) is roster_obj

    cards = [object(), object()]
    cards_session = DummySession(DummyExecResult(items=cards))
    assert sync._load_live_series_cards(cards_session, 2024) == cards


def test_model_dirs_and_joblib_load(tmp_path, monkeypatch):
    sync = pred.PredictionSync()
    assert sync._attr_models_dir().name == "attr_models"
    assert sync._ovr_models_dir().name == "ovr_models"

    sentinel = object()
    monkeypatch.setattr(pred, "InconsistentVersionWarning", UserWarning)
    monkeypatch.setattr(pred, "joblib_load", lambda path: sentinel)
    assert sync._joblib_load(tmp_path / "model.joblib") is sentinel


def test_load_attr_model_missing_files(tmp_path):
    sync = pred.PredictionSync()
    model_dir = tmp_path / "missing"
    model_dir.mkdir()
    assert sync._load_attr_model(model_dir) is None


def test_predict_attr_models_skips_and_exceptions(tmp_path, monkeypatch):
    sync = pred.PredictionSync()

    model_root = tmp_path / "attr_models"
    (model_root / "a").mkdir(parents=True)
    (model_root / "b").mkdir()
    (model_root / "c").mkdir()

    def fake_load_attr(model_dir):
        if model_dir.name == "a":
            return None
        if model_dir.name == "b":
            return ("key", DummyModel(output=1.0), ["old_ovr"], {"role": "pit", "attr": "CON L"})
        if model_dir.name == "c":
            class BadModel:
                def predict(self, X):
                    raise RuntimeError("boom")

            return ("key", BadModel(), ["old_ovr"], {"role": "hit", "attr": "CON L"})
        return None

    monkeypatch.setattr(sync, "_attr_models_dir", lambda: model_root)
    monkeypatch.setattr(sync, "_load_attr_model", fake_load_attr)

    df = pd.DataFrame([{"card_id": "c1", "old_ovr": 50}])
    preds = sync._predict_attr_models(df, role="hit")
    assert "card_id" in preds.columns

    empty_preds = sync._predict_attr_models(pd.DataFrame(), role="hit")
    assert empty_preds.empty

    monkeypatch.setattr(sync, "_attr_models_dir", lambda: tmp_path / "missing_root")
    missing_root_preds = sync._predict_attr_models(df, role="hit")
    assert missing_root_preds.empty


def test_load_ovr_model_missing_paths(tmp_path, monkeypatch):
    sync = pred.PredictionSync()
    monkeypatch.setattr(sync, "_ovr_models_dir", lambda: tmp_path)
    assert sync._load_ovr_model("hit", "field_on") is None

    (tmp_path / "hit" / "field_on").mkdir(parents=True)
    assert sync._load_ovr_model("hit", "field_on") is None


def test_build_ovr_input_field_off_uses_old_attr():
    sync = pred.PredictionSync()
    role_df = pd.DataFrame([
        {"card_id": "c1", "old_ovr": 50, "SPD_old": 12, "display_position": "", "display_secondary_positions": ""},
    ])
    attr_preds = pd.DataFrame([{"card_id": "c1", "pred_SPD": 99}])
    feature_cols = ["old_ovr", "pred_SPD_new"]

    x_off = sync._build_ovr_input(role_df, attr_preds, feature_cols, field_mode="field_off")
    assert x_off["pred_SPD_new"].iloc[0] == 12

    x_on = sync._build_ovr_input(role_df, pd.DataFrame(), feature_cols, field_mode="field_on")
    assert x_on["pred_SPD_new"].iloc[0] == 12


def test_build_ovr_input_skips_non_pred_cols():
    sync = pred.PredictionSync()
    role_df = pd.DataFrame([
        {"card_id": "c1", "old_ovr": 50, "display_position": "", "display_secondary_positions": ""},
    ])
    feature_cols = ["old_ovr", "not_pred"]
    with pytest.raises(KeyError):
        sync._build_ovr_input(role_df, pd.DataFrame(), feature_cols, field_mode="field_on")


def test_predict_ovr_for_role_empty_and_exceptions(monkeypatch):
    sync = pred.PredictionSync()
    assert sync._predict_ovr_for_role(pd.DataFrame(), pd.DataFrame(), role="hit") == {}

    role_df = pd.DataFrame([
        {"card_id": "c1", "old_ovr": 1, "SPD_old": 1, "display_position": "", "display_secondary_positions": ""},
    ])
    attr_preds = pd.DataFrame([{"card_id": "c1", "pred_SPD": 1}])
    feature_cols = ["old_ovr", "pred_SPD_new"]

    class BadModel:
        def predict(self, X):
            raise RuntimeError("boom")

    monkeypatch.setattr(sync, "_load_ovr_model", lambda role, field_mode: (BadModel(), feature_cols))
    out = sync._predict_ovr_for_role(role_df, attr_preds, role="hit")
    assert out == {}

    monkeypatch.setattr(sync, "_load_ovr_model", lambda role, field_mode: None)
    out_none = sync._predict_ovr_for_role(role_df, attr_preds, role="hit")
    assert out_none == {}


def test_combine_ovr_predictions_empty_and_invalid(monkeypatch):
    sync = pred.PredictionSync()
    assert sync._combine_ovr_predictions({}, {}) == {}

    class IsNaErrorFloat:
        def __float__(self):
            return 7.0

    class BadFloat:
        def __float__(self):
            raise ValueError("bad")

    sentinel = IsNaErrorFloat()
    orig_isna = pd.isna

    def fake_isna(v):
        if v is sentinel:
            raise TypeError("boom")
        return orig_isna(v)

    monkeypatch.setattr(pred.pd, "isna", fake_isna)

    hit = {
        "field_on": {
            "preds": pd.Series({"c1": sentinel, "c2": None, "c3": np.nan}),
            "attrs": pd.DataFrame({"x": [BadFloat(), 2, 3], "z": [None, 1, 2]}, index=["c1", "c2", "c3"]),
        }
    }
    pit = {
        "field_on": {
            "preds": pd.Series({"c1": np.nan, "c2": 3.0, "c3": None}),
            "attrs": pd.DataFrame({"y": [1, BadFloat(), 4]}, index=["c1", "c2", "c3"]),
        }
    }

    combined = sync._combine_ovr_predictions(hit, pit)
    assert combined["field_on"]["preds"]["c1"] == 7.0
    assert combined["field_on"]["preds"]["c2"] == 3.0
    assert combined["field_on"]["attrs"]["c1"]["hit_x"] == 0.0
    assert combined["field_on"]["attrs"]["c1"]["hit_z"] == 0.0


def test_persist_predictions_skips_invalid():
    sync = pred.PredictionSync()
    session = DummySession()

    combined = {
        "unknown": {"preds": {"c1": 1.0}, "attrs": {}},
        "field_on": {"preds": {"c1": None, "c2": Decimal("1.2")}, "attrs": {}},
    }

    sync._persist_predictions(session, combined)

    assert len(session.added) == 1
    assert len(session.added_all) == 1
    assert session.flushed == 1


def test_add_split_league_shrunk_rates_missing_columns():
    sync = pred.PredictionSync()
    df = pd.DataFrame({
        "update_date": [1],
        "since_pa": [10],
    })

    out = sync._add_split_league_shrunk_rates(
        df,
        windows=["since"],
        split_prefixes=["", "vslhp_"],
        denom_name="pa",
        numerators=["r"],
        k_by_window={"since": 10.0},
        split_k_divisors={"": 1.0, "vslhp_": 2.0},
        group_col="update_date",
        add_raw_rate=False,
    )

    assert out.shape[0] == 1


def test_load_stat_frames(monkeypatch):
    sync = pred.PredictionSync()

    batting_df = pd.DataFrame({
        "player_id": [None],
        "game_date": [pd.Timestamp("2026-01-01", tz="UTC")],
        "season": [None],
        "split": [""],
        "pa": [1],
        "r": [0],
        "h": [0],
        "doubles": [0],
        "triples": [0],
        "hr": [0],
        "hbp": [0],
        "tb": [0],
        "rbi": [0],
        "so": [0],
        "bb": [0],
        "ab": [0],
        "lob": [0],
    })
    pitching_df = pd.DataFrame({
        "player_id": [None],
        "game_date": [pd.Timestamp("2026-01-01", tz="UTC")],
        "season": [None],
        "split": [""],
        "outs_pitched": [0],
        "ip": [0],
        "ab": [0],
        "pitches_thrown": [0],
        "h": [0],
        "doubles": [0],
        "triples": [0],
        "hr": [0],
        "bb": [0],
        "k": [0],
        "r": [0],
        "er": [0],
        "batters_faced": [0],
        "balls_thrown": [0],
        "strikes_thrown": [0],
    })
    baserunning_df = pd.DataFrame({
        "player_id": [None],
        "game_date": [pd.Timestamp("2026-01-01", tz="UTC")],
        "season": [None],
        "sb": [0],
        "caught_stealing": [0],
    })
    fielding_df = pd.DataFrame({
        "player_id": [None],
        "game_date": [pd.Timestamp("2026-01-01", tz="UTC")],
        "season": [None],
        "assists": [0],
        "put_outs": [0],
        "errors": [0],
        "chances": [0],
    })

    def fake_read_sql(query, bind, parse_dates=None):
        if "mlb_game_batting_stats" in str(query):
            return batting_df.copy()
        if "mlb_game_pitching_stats" in str(query):
            return pitching_df.copy()
        if "mlb_game_baserunning_stats" in str(query):
            return baserunning_df.copy()
        if "mlb_game_fielding_stats" in str(query):
            return fielding_df.copy()
        raise AssertionError("unexpected query")

    class DummyDB:
        def get_bind(self):
            return object()

    monkeypatch.setattr(pred.pd, "read_sql", fake_read_sql)

    b_df = sync._load_batting(DummyDB())
    p_df = sync._load_pitching(DummyDB())
    br_df = sync._load_baserunning(DummyDB())
    f_df = sync._load_fielding(DummyDB())

    assert b_df["player_id"].iloc[0] == 0
    assert p_df["season"].iloc[0] == 0
    assert br_df["game_date"].dt.tz is None
    assert f_df["game_date"].dt.tz is None


def test_build_feature_frame_empty_and_nan_dates(monkeypatch):
    sync = pred.PredictionSync()

    class FakeDateTime:
        def __init__(self, year):
            self.year = year

    fake_dt = FakeDateTime(2026)
    orig_notna = pd.notna

    def fake_notna(val):
        if isinstance(val, FakeDateTime):
            return False
        return orig_notna(val)

    monkeypatch.setattr(pred.pd, "notna", fake_notna)

    card = SimpleNamespace(
        id=1,
        ovr=50,
        mlb_id=0,
        name="A",
        team="T",
        display_position="SP",
        display_secondary_positions="",
        age=25,
        year="bad",
        height="6'0",
        weight="180",
    )

    empty_batting = pd.DataFrame(columns=["player_id", "game_date", "season"])
    empty_pitching = pd.DataFrame(columns=["player_id", "game_date", "season"])
    empty_baserunning = pd.DataFrame(columns=["player_id", "game_date", "season"])
    empty_fielding = pd.DataFrame(columns=["player_id", "game_date", "season"])

    monkeypatch.setattr(sync, "_load_batting", lambda db: empty_batting)
    monkeypatch.setattr(sync, "_load_pitching", lambda db: empty_pitching)
    monkeypatch.setattr(sync, "_load_baserunning", lambda db: empty_baserunning)
    monkeypatch.setattr(sync, "_load_fielding", lambda db: empty_fielding)

    out = sync._build_feature_frame(
        db_session=object(),
        cards=[card],
        update_dt=fake_dt,
        last_update=dt.date(2026, 1, 1),
    )
    assert out.empty


def test_build_feature_frame_no_cards():
    sync = pred.PredictionSync()
    out = sync._build_feature_frame(
        db_session=object(),
        cards=[],
        update_dt=dt.datetime(2026, 1, 1),
        last_update=dt.date(2026, 1, 1),
    )
    assert out.empty


def test_build_feature_frame_full(monkeypatch):
    sync = pred.PredictionSync()
    update_dt = dt.datetime(2026, 1, 15)
    last_update = dt.date(2025, 12, 31)

    card = SimpleNamespace(
        id=1,
        ovr=50,
        mlb_id=1,
        name="A",
        team="T",
        display_position="SP",
        display_secondary_positions="LF",
        age=27,
        year=24,
        height="6'2",
        weight="200",
    )

    batting_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-10"),
        "season": 2026,
        "split": "vslhp",
        "pa": 4,
        "r": 1,
        "h": 2,
        "doubles": 0,
        "triples": 0,
        "hr": 0,
        "hbp": 0,
        "tb": 2,
        "rbi": 1,
        "so": 1,
        "bb": 0,
        "ab": 4,
        "lob": 1,
    }])
    pitching_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-09"),
        "season": 2026,
        "split": "vslhb",
        "outs_pitched": 3,
        "ip": 1.0,
        "ab": 3,
        "pitches_thrown": 10,
        "h": 1,
        "doubles": 0,
        "triples": 0,
        "hr": 0,
        "bb": 0,
        "k": 1,
        "r": 0,
        "er": 0,
        "batters_faced": 3,
        "balls_thrown": 5,
        "strikes_thrown": 5,
    }])
    baserunning_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-08"),
        "season": 2026,
        "sb": 1,
        "caught_stealing": 0,
    }])
    fielding_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-07"),
        "season": 2026,
        "assists": 1,
        "put_outs": 2,
        "errors": 0,
        "chances": 3,
    }])

    monkeypatch.setattr(sync, "_load_batting", lambda db: batting_df)
    monkeypatch.setattr(sync, "_load_pitching", lambda db: pitching_df)
    monkeypatch.setattr(sync, "_load_baserunning", lambda db: baserunning_df)
    monkeypatch.setattr(sync, "_load_fielding", lambda db: fielding_df)

    out = sync._build_feature_frame(
        db_session=object(),
        cards=[card],
        update_dt=update_dt,
        last_update=last_update,
    )

    assert out["height_inches"].iloc[0] == 74
    assert out["weight_lbs"].iloc[0] == 200
    assert out["is_sp"].iloc[0] == 1
    assert out["multi_pos"].iloc[0] == 1
    assert out["age_bucket_prime"].iloc[0] == 1


def test_build_feature_frame_since_mask_with_last_update(monkeypatch):
    sync = pred.PredictionSync()
    update_dt = dt.datetime(2026, 1, 15)
    last_update = dt.date(2026, 1, 5)

    card = SimpleNamespace(
        id=1,
        ovr=50,
        mlb_id=1,
        name="A",
        team="T",
        display_position="SP",
        display_secondary_positions="",
        age=25,
        year=2026,
        height="6'0",
        weight="180",
    )

    batting_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-10"),
        "season": 2026,
        "split": "vsrhp",
        "pa": 4,
        "r": 1,
        "h": 2,
        "doubles": 0,
        "triples": 0,
        "hr": 0,
        "hbp": 0,
        "tb": 2,
        "rbi": 1,
        "so": 1,
        "bb": 0,
        "ab": 4,
        "lob": 1,
    }])
    pitching_df = pd.DataFrame([{
        "player_id": 1,
        "game_date": pd.Timestamp("2026-01-09"),
        "season": 2026,
        "split": "vsrhb",
        "outs_pitched": 3,
        "ip": 1.0,
        "ab": 3,
        "pitches_thrown": 10,
        "h": 1,
        "doubles": 0,
        "triples": 0,
        "hr": 0,
        "bb": 0,
        "k": 1,
        "r": 0,
        "er": 0,
        "batters_faced": 3,
        "balls_thrown": 5,
        "strikes_thrown": 5,
    }])

    monkeypatch.setattr(sync, "_load_batting", lambda db: batting_df)
    monkeypatch.setattr(sync, "_load_pitching", lambda db: pitching_df)
    empty_baserunning = pd.DataFrame(columns=["player_id", "game_date", "season", "sb", "caught_stealing"])
    empty_fielding = pd.DataFrame(columns=["player_id", "game_date", "season", "assists", "put_outs", "errors", "chances"])
    monkeypatch.setattr(sync, "_load_baserunning", lambda db: empty_baserunning)
    monkeypatch.setattr(sync, "_load_fielding", lambda db: empty_fielding)

    out = sync._build_feature_frame(
        db_session=object(),
        cards=[card],
        update_dt=update_dt,
        last_update=last_update,
    )
    assert not out.empty
