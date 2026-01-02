import re
import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from src.database.database import engine

OUTPUT_PATH = "src/training_data.csv"

def height_to_inches(v):
    if not isinstance(v, str): return None
    m = re.match(r"(\d+)'\s*(\d+)", v)
    return int(m.group(1)) * 12 + int(m.group(2)) if m else None

def weight_to_lbs(v):
    if not isinstance(v, str): return None
    val = re.sub(r"[^\d]", "", v)
    return int(val) if val else None

def safe_div(n, d):
    return n / d if d and d != 0 else 0.0

def make_naive(df, col):
    """Removes timezone info from a datetime column to prevent comparison errors."""
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
    df.loc[
        df["last_update"].dt.year != df["update_date"].dt.year, 
        "last_update"
    ] = pd.NaT

    df["mlb_id"] = df["mlb_id"].fillna(0).astype(int)
    df = make_naive(df, "update_date")
    df = make_naive(df, "last_update")
    
    return df

def load_batting():
    df = pd.read_sql(text("""
        SELECT b.player_id, g.game_date, g.season, b.split,
               b.pa, b.r, b.h, b.doubles, b.triples, b.hr, 
               b.hbp, b.tb, b.rbi, b.so, b.bb, b.ab, b.lob
        FROM mlb_game_batting_stats b
        JOIN mlb_games g ON g.id = b.game_id
    """), engine, parse_dates=["game_date"])
    
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_pitching():
    df = pd.read_sql(text("""
        SELECT p.player_id, g.game_date, g.season, p.split,
               p.outs_pitched, p.ip, p.ab, p.pitches_thrown,
               p.h, p.doubles, p.triples, p.hr, p.bb, p.k, 
               p.r, p.er, p.batters_faced, p.balls_thrown, p.strikes_thrown
        FROM mlb_game_pitching_stats p
        JOIN mlb_games g ON g.id = p.game_id
    """), engine, parse_dates=["game_date"])
    
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_baserunning():
    df = pd.read_sql(text("""
        SELECT b.player_id, g.game_date, g.season, 
               b.sb, b.caught_stealing
        FROM mlb_game_baserunning_stats b
        JOIN mlb_games g ON g.id = b.game_id
    """), engine, parse_dates=["game_date"])
    
    df["player_id"] = df["player_id"].fillna(0).astype(int)
    df["season"] = df["season"].fillna(0).astype(int)
    df = make_naive(df, "game_date")
    return df

def load_fielding():
    df = pd.read_sql(text("""
        SELECT f.player_id, g.game_date, g.season,
               f.assists, f.put_outs, f.errors, f.chances
        FROM mlb_game_fielding_stats f
        JOIN mlb_games g ON g.id = f.game_id
    """), engine, parse_dates=["game_date"])
    
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
    total_sum = df.sum(numeric_only=True).to_dict()
    out = calc_batting_metrics(total_sum, prefix)

    if not df.empty and "split" in df.columns:
        for split_name, gdf in df.groupby("split"):
            s_clean = str(split_name).lower().replace(" ", "")
            split_prefix = f"{prefix}{s_clean}_"
            split_sum = gdf.sum(numeric_only=True).to_dict()
            out.update(calc_batting_metrics(split_sum, split_prefix))
    return out

def calc_pitching_metrics(s, prefix):
    ip = s.get("ip", 0)
    ab = s.get("ab", 0)
    h = s.get("h", 0)
    bb = s.get("bb", 0)
    hr = s.get("hr", 0)
    er = s.get("er", 0)
    k = s.get("k", 0)

    exclude = {"player_id", "season", "game_id"}
    out = {f"{prefix}{k}": v for k, v in s.items() if k not in exclude}

    out[f"{prefix}era"] = safe_div(er * 9, ip)
    out[f"{prefix}k9"] = safe_div(k * 9, ip)
    out[f"{prefix}bb9"] = safe_div(bb * 9, ip)
    out[f"{prefix}hr9"] = safe_div(hr * 9, ip)
    out[f"{prefix}whip"] = safe_div(bb + h, ip)
    out[f"{prefix}avg_against"] = safe_div(h, ab)
    out[f"{prefix}strike_pct"] = safe_div(s.get("strikes_thrown", 0), s.get("pitches_thrown", 0))
    return out

def agg_pitching(df, prefix):
    total_sum = df.sum(numeric_only=True).to_dict()
    out = calc_pitching_metrics(total_sum, prefix)

    if not df.empty and "split" in df.columns:
        for split_name, gdf in df.groupby("split"):
            s_clean = str(split_name).lower().replace(" ", "")
            split_prefix = f"{prefix}{s_clean}_"
            split_sum = gdf.sum(numeric_only=True).to_dict()
            out.update(calc_pitching_metrics(split_sum, split_prefix))
    return out

def agg_baserunning(df, prefix):
    s = df.sum(numeric_only=True).to_dict()
    sb = s.get("sb", 0)
    cs = s.get("caught_stealing", 0)
    out = {
        f"{prefix}sb": sb,
        f"{prefix}cs": cs,
        f"{prefix}sb_attempts": sb + cs,
        f"{prefix}sb_pct": safe_div(sb, sb + cs)
    }
    return out

def agg_fielding(df, prefix):
    s = df.sum(numeric_only=True).to_dict()
    errors = s.get("errors", 0)
    chances = s.get("chances", 0)
    out = {
        f"{prefix}errors": errors,
        f"{prefix}chances": chances,
        f"{prefix}put_outs": s.get("put_outs", 0),
        f"{prefix}assists": s.get("assists", 0),
        f"{prefix}field_pct": safe_div(chances - errors, chances)
    }
    return out


def main():
    print("Loading Data...")
    base = load_base()
    batting = load_batting()
    pitching = load_pitching()
    baserunning = load_baserunning()
    fielding = load_fielding()
    print("Data Loaded.")

    rows = []
    
    relevant_ids = set(base["mlb_id"].unique())
    batting = batting[batting["player_id"].isin(relevant_ids)]
    pitching = pitching[pitching["player_id"].isin(relevant_ids)]

    print(f"Processing {len(base)} updates...")
    
    for i, u in base.iterrows():
        row = u.to_dict()
        pid = u["mlb_id"]
        ud = u["update_date"]
        last = u["last_update"]
        
        try:
            raw_year = int(u["year"])
            year = raw_year + 2000 if raw_year < 100 else raw_year
        except (ValueError, TypeError):
            year = 0

        if pid == 0: continue

        b_p = batting[batting.player_id == pid].copy()
        p_p = pitching[pitching.player_id == pid].copy()
        br_p = baserunning[baserunning.player_id == pid].copy()
        f_p = fielding[fielding.player_id == pid].copy()

        szn_mask_b = (b_p.season == year)
        szn_mask_p = (p_p.season == year)
        
        m1_start = ud - timedelta(days=30)
        m1_mask_b = (b_p.game_date >= m1_start) & (b_p.game_date <= ud)
        m1_mask_p = (p_p.game_date >= m1_start) & (p_p.game_date <= ud)

        if pd.notna(last):
            since_mask_b = (b_p.game_date > last) & (b_p.game_date <= ud)
            since_mask_p = (p_p.game_date > last) & (p_p.game_date <= ud)
        else:
            since_mask_b = szn_mask_b
            since_mask_p = szn_mask_p

        scopes = {
            "szn_": (b_p[szn_mask_b], p_p[szn_mask_p], br_p[br_p.season == year], f_p[f_p.season == year]),
            "m1_": (b_p[m1_mask_b], p_p[m1_mask_p], br_p[br_p.game_date.between(m1_start, ud)], f_p[f_p.game_date.between(m1_start, ud)]),
            "since_": (b_p[since_mask_b], p_p[since_mask_p], None, None) 
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

    final_df = final_df.fillna(0)

    print(f"Done. Generated {len(final_df)} rows and {len(final_df.columns)} columns.")
    final_df.to_csv(OUTPUT_PATH, index=False)

if __name__ == "__main__":
    main()