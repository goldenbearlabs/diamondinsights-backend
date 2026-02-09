import re
import sys
from pathlib import Path
from datetime import timedelta

import pandas as pd
from sqlalchemy import text



SCRIPT_DIR = Path(__file__).resolve().parent

def _find_project_root(start: Path) -> Path:
    for p in [start, *start.parents]:
        if (p / "shared").is_dir():
            return p
    return start

PROJECT_ROOT = _find_project_root(SCRIPT_DIR)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")
from shared.db.database import engine  # noqa: E402


OUTPUT_PATH = SCRIPT_DIR.parent / "training_data.csv"

def height_to_inches(v):
    if not isinstance(v, str):
        return None
    m = re.match(r"(\d+)'\s*(\d+)", v)
    return int(m.group(1)) * 12 + int(m.group(2)) if m else None

def weight_to_lbs(v):
    if not isinstance(v, str):
        return None
    val = re.sub(r"[^\d]", "", v)
    return int(val) if val else None

def safe_div(n, d):
    return n / d if d and d != 0 else 0.0

def make_naive(df, col):
    if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
        df[col] = df[col].dt.tz_localize(None)
    return df

def load_base():
    q = """
        SELECT 
            cu.update_id, cu.update_date, cu.card_id, 
            cu.old_ovr, cu.new_ovr, cu.trend_display,
            c.mlb_id, c.name, c.team, c.display_position, 
            c.display_secondary_positions, c.age, c.year,
            c.height, c.weight
        FROM card_updates cu
        JOIN cards c ON c.id = cu.card_id
        ORDER BY cu.card_id, cu.update_date
    """
    df = pd.read_sql(text(q), engine, parse_dates=["update_date"])

    df["last_update"] = df.groupby("card_id")["update_date"].shift(1)
    df.loc[df["last_update"].dt.year != df["update_date"].dt.year, "last_update"] = pd.NaT

    df["mlb_id"] = df["mlb_id"].fillna(0).astype(int)
    df = make_naive(df, "update_date")
    df = make_naive(df, "last_update")
    return df

def load_attribute_changes():
    q = """
        SELECT update_id, update_date, card_id, name, new_value, old_value, delta
        FROM card_attribute_changes
    """
    df = pd.read_sql(text(q), engine, parse_dates=["update_date"])
    df = make_naive(df, "update_date")

    df["delta"] = df["delta"].astype(str).str.replace("+", "", regex=False)
    df["delta"] = pd.to_numeric(df["delta"], errors="coerce")

    df["old_value"] = pd.to_numeric(df["old_value"], errors="coerce")
    df["new_value"] = pd.to_numeric(df["new_value"], errors="coerce")

    return df

def load_batting():
    df = pd.read_sql(
        text("""
            SELECT b.player_id, g.game_date, g.season, b.split,
                   b.pa, b.r, b.h, b.doubles, b.triples, b.hr, 
                   b.hbp, b.tb, b.rbi, b.so, b.bb, b.ab, b.lob
            FROM mlb_game_batting_stats b
            JOIN mlb_games g ON g.id = b.game_id
        """),
        engine,
        parse_dates=["game_date"],
    )
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_pitching():
    df = pd.read_sql(
        text("""
            SELECT p.player_id, g.game_date, g.season, p.split,
                   p.outs_pitched, p.ip, p.ab, p.pitches_thrown,
                   p.h, p.doubles, p.triples, p.hr, p.bb, p.k, 
                   p.r, p.er, p.batters_faced, p.balls_thrown, p.strikes_thrown
            FROM mlb_game_pitching_stats p
            JOIN mlb_games g ON g.id = p.game_id
        """),
        engine,
        parse_dates=["game_date"],
    )
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_baserunning():
    df = pd.read_sql(
        text("""
            SELECT b.player_id, g.game_date, g.season, 
                   b.sb, b.caught_stealing
            FROM mlb_game_baserunning_stats b
            JOIN mlb_games g ON g.id = b.game_id
        """),
        engine,
        parse_dates=["game_date"],
    )
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_fielding():
    df = pd.read_sql(
        text("""
            SELECT f.player_id, g.game_date, g.season,
                   f.assists, f.put_outs, f.errors, f.chances
            FROM mlb_game_fielding_stats f
            JOIN mlb_games g ON g.id = f.game_id
        """),
        engine,
        parse_dates=["game_date"],
    )
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def calc_batting_metrics(s, prefix):
    ab = s.get("ab", 0)
    h = s.get("h", 0)
    bb = s.get("bb", 0)
    hbp = s.get("hbp", 0)
    tb = s.get("tb", 0)
    so = s.get("so", 0)

    exclude = {"player_id", "season", "game_id"}
    out = {f"{prefix}{k}": v for k, v in s.items() if k not in exclude}

    out[f"{prefix}avg"] = safe_div(h, ab)
    out[f"{prefix}obp"] = safe_div(h + bb + hbp, ab + bb + hbp)
    out[f"{prefix}slug"] = safe_div(tb, ab)
    out[f"{prefix}ops"] = out[f"{prefix}obp"] + out[f"{prefix}slug"]
    out[f"{prefix}iso"] = out[f"{prefix}slug"] - out[f"{prefix}avg"]
    out[f"{prefix}bb_pct"] = safe_div(bb, ab)
    out[f"{prefix}k_pct"] = safe_div(so, ab)
    return out

def agg_batting(df, prefix):
    def clean_split(s):
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

def calc_pitching_metrics(s, prefix):
    outs = s.get("outs_pitched", 0)

    math_ip = outs / 3.0
    display_ip = (outs // 3) + ((outs % 3) / 10.0)

    ab = s.get("ab", 0)
    h = s.get("h", 0)
    bb = s.get("bb", 0)
    hr = s.get("hr", 0)
    er = s.get("er", 0)
    k = s.get("k", 0)

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
    out[f"{prefix}strike_pct"] = safe_div(s.get("strikes_thrown", 0), s.get("pitches_thrown", 0))
    return out

def agg_pitching(df, prefix):
    def clean_split(s):
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

def agg_baserunning(df, prefix):
    s = df.sum(numeric_only=True).to_dict()
    sb = s.get("sb", 0)
    cs = s.get("caught_stealing", 0)
    return {
        f"{prefix}sb": sb,
        f"{prefix}cs": cs,
        f"{prefix}sb_attempts": sb + cs,
        f"{prefix}sb_pct": safe_div(sb, sb + cs),
    }

def agg_fielding(df, prefix):
    s = df.sum(numeric_only=True).to_dict()
    errors = s.get("errors", 0)
    chances = s.get("chances", 0)
    return {
        f"{prefix}errors": errors,
        f"{prefix}chances": chances,
        f"{prefix}put_outs": s.get("put_outs", 0),
        f"{prefix}assists": s.get("assists", 0),
        f"{prefix}field_pct": safe_div(chances - errors, chances),
    }

def _merge_attr_changes(base: pd.DataFrame, attr_changes: pd.DataFrame) -> pd.DataFrame:
    if attr_changes.empty:
        return base

    attr_pivot = attr_changes.pivot_table(
        index=["update_id", "update_date", "card_id"],
        columns="name",
        values=["new_value", "old_value", "delta"],
        aggfunc="first",
    )

    val_map = {"new_value": "new", "old_value": "old", "delta": "delta"}
    attr_pivot.columns = [f"{col[1]}_{val_map[col[0]]}" for col in attr_pivot.columns]
    attr_pivot = attr_pivot.reset_index()

    out = pd.merge(base, attr_pivot, on=["update_id", "update_date", "card_id"], how="left")

    all_attr_cols = [c for c in attr_pivot.columns if c not in ["update_id", "update_date", "card_id"]]
    delta_cols = [c for c in all_attr_cols if c.endswith("_delta")]

    if delta_cols:
        out[delta_cols] = out[delta_cols].fillna(0)

    return out

def main():
    print("Loading Data...")
    base = load_base()
    attr_changes = load_attribute_changes()
    batting = load_batting()
    pitching = load_pitching()
    baserunning = load_baserunning()
    fielding = load_fielding()
    print("Data Loaded.")

    base = _merge_attr_changes(base, attr_changes)

    rows = []

    relevant_ids = set(base["mlb_id"].unique())
    batting = batting[batting["player_id"].isin(relevant_ids)]
    pitching = pitching[pitching["player_id"].isin(relevant_ids)]
    baserunning = baserunning[baserunning["player_id"].isin(relevant_ids)]
    fielding = fielding[fielding["player_id"].isin(relevant_ids)]

    print(f"Processing {len(base)} updates...")

    for _, u in base.iterrows():
        pid = int(u["mlb_id"] or 0)
        ud = u["update_date"]
        last = u["last_update"]
        if pid == 0 or pd.isna(ud):
            continue

        row = u.to_dict()

        try:
            raw_year = int(u["year"])
            card_year = raw_year + 2000 if raw_year < 100 else raw_year
        except (ValueError, TypeError):
            card_year = 0

        season_year = int(ud.year) if pd.notna(ud) else card_year

        b_p = batting[batting.player_id == pid]
        p_p = pitching[pitching.player_id == pid]
        br_p = baserunning[baserunning.player_id == pid]
        f_p = fielding[fielding.player_id == pid]

        szn_mask_b = (b_p.season == season_year) & (b_p.game_date < ud)
        szn_mask_p = (p_p.season == season_year) & (p_p.game_date < ud)

        m1_start = ud - timedelta(days=30)
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

    final_df["height_inches"] = final_df["height"].apply(height_to_inches)
    final_df["weight_lbs"] = final_df["weight"].apply(weight_to_lbs)

    pos = final_df["display_position"].fillna("")
    sec = final_df["display_secondary_positions"].fillna("")

    final_df["is_sp"] = (pos == "SP").astype(int)
    final_df["is_rp"] = (pos == "RP").astype(int)
    final_df["is_if"] = pos.isin(["1B", "2B", "SS", "3B"]).astype(int)
    final_df["is_of"] = pos.isin(["LF", "CF", "RF"]).astype(int)
    final_df["multi_pos"] = sec.ne("").astype(int)

    final_df["age_sq"] = final_df["age"] ** 2
    final_df["age_bucket_young"] = (final_df["age"] < 26).astype(int)
    final_df["age_bucket_prime"] = final_df["age"].between(26, 30).astype(int)
    final_df["age_bucket_old"] = (final_df["age"] > 30).astype(int)

    attr_keep_na_cols = [c for c in final_df.columns if c.endswith("_old") or c.endswith("_new")]
    fill_cols = [c for c in final_df.columns if c not in attr_keep_na_cols]
    final_df[fill_cols] = final_df[fill_cols].fillna(0)

    print(f"Done. Generated {len(final_df)} rows and {len(final_df.columns)} columns.")
    final_df.to_csv(OUTPUT_PATH, index=False)

if __name__ == "__main__":
    main()
