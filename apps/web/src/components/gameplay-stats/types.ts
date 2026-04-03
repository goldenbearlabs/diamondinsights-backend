export type ShowProfile = {
  username: string;
};

export type ShowUserSearchResult = {
  user_id?: number | null;
  username: string;
  display_name?: string | null;
  profile_img_url?: string | null;
};

export type ShowGameSummary = {
  games_played: number;
  record: string;
  last_game_date?: string | null;
  last_game_difficulty?: string | null;
};

export type PlateAppearanceStats = {
  avg?: number | null;
  obp?: number | null;
  slg?: number | null;
  ops?: number | null;
  kbb?: number | null;
};

export type ShowSkills = {
  hitting: PlateAppearanceStats;
  pitching: PlateAppearanceStats;
};

export type BattingArchetype = {
  overall: number;
  power: number;
  timing: number;
  location: number;
};

export type PitchingArchetype = {
  overall: number;
  consistency: number;
  strikeout: number;
  location: number;
};

export type CombinedArchetype = {
  batting: BattingArchetype;
  pitching: PitchingArchetype;
};

export type ShowAggregateStats = {
  pa: number;
  ab: number;
  r: number;
  h: number;
  rbi: number;
  singles: number;
  doubles: number;
  triples: number;
  hr: number;
  bb: number;
  so: number;
  avg: number;
  obp: number;
  slg: number;
  ops: number;
  lob: number;
  gidp: number;
  gidp_pct?: number | null;
  woba: number;
  iso: number;
  babip: number;
  k_pct: number;
  bb_pct: number;
  hr_pct: number;
  xbh_pct: number;
  rs_pct: number;
  chase_pct: number;
  freeze_pct: number;
  timing_pct: number;
  timing_k_pct: number;
  eye_k_pct: number;
  location_k_pct: number;
  sweet_spot_pct: number;
  popup_rate: number;
  flyball_rate: number;
  gb_air_ratio: number;
  pulled_air_rate: number;
  oppo_air_rate: number;
  perfect_perfect_pct: number;
};

export type StrikeoutOutsideKey =
  | "top_left"
  | "top"
  | "top_right"
  | "right"
  | "bottom_right"
  | "bottom"
  | "bottom_left"
  | "left";

export type StrikeoutStats = {
  k_pct: number;
  chase_pct: number;
  freeze_pct: number;
  timing_pct: number;
  timing_k_pct: number;
  eye_k_pct: number;
  location_k_pct: number;
  heart_miss_k_pct?: number;
  inzone_swing_k_pct?: number;
};

export type StrikeoutCounts = {
  k: number;
  chase: number;
  look: number;
  eye: number;
  early: number;
  late: number;
};

export type StrikeoutSelection =
  | { kind: "zone"; row: number; col: number }
  | { kind: "outside"; key: StrikeoutOutsideKey };

export type StrikeoutMapData = {
  zones: number[][];
  outside: Record<StrikeoutOutsideKey, number>;
  total: number;
  pa: number;
  pitch_type_options: string[];
  stats: StrikeoutStats;
  stats_by_zone: StrikeoutStats[][];
  stats_by_outside: Record<StrikeoutOutsideKey, StrikeoutStats>;
  counts_by_zone: StrikeoutCounts[][];
  counts_by_outside: Record<StrikeoutOutsideKey, StrikeoutCounts>;
};

export type HitDataStat = "count" | "share" | "babip" | "woba" | "slug";

export type HitZoneKey =
  | "infield_left"
  | "infield_right"
  | "outfield_left"
  | "outfield_center"
  | "outfield_right"
  | "homerun_left"
  | "homerun_center"
  | "homerun_right";

export type HitDataMap = {
  zones: Record<HitZoneKey, number>;
  total: number;
  pa: number;
  stat: HitDataStat;
  stats: {
    sweet_spot_pct: number;
    popup_rate: number;
    flyball_rate: number;
    groundball_rate?: number;
    gb_air_ratio: number;
    pulled_air_rate: number;
    oppo_air_rate: number;
    perfect_perfect_pct: number;
    extreme_contact_nopp_pct?: number;
  };
};

export type ShowGameLogItem = {
  game_id: string;
  date: string;
  difficulty?: string | null;
  is_online?: boolean | null;
  ball_park_name?: string | null;
  home_profile_username: string;
  away_profile_username: string;
  home_full_name: string;
  away_full_name: string;
  home_result: string;
  away_result: string;
  home_runs: number;
  away_runs: number;
  home_hits: number;
  away_hits: number;
  home_errors: number;
  away_errors: number;
  innings: number;
  summary?: string | null;
};

export type ShowGameEvent = {
  game_id: string;
  seq: number;
  inning?: number | null;
  is_home_batting?: boolean | null;
  outs_before?: number | null;
  outs_after?: number | null;
  home_score_before?: number | null;
  away_score_before?: number | null;
  home_score_after?: number | null;
  away_score_after?: number | null;
  pre_on_1b?: boolean | null;
  pre_on_2b?: boolean | null;
  pre_on_3b?: boolean | null;
  post_on_1b?: boolean | null;
  post_on_2b?: boolean | null;
  post_on_3b?: boolean | null;
  event_type: string;
  event_text: string;
  event_seq_in_half?: number | null;
  parser_version?: string | null;
};

export type ShowHalfInningSummary = {
  game_id: string;
  inning: number;
  is_home_batting: boolean;
  runs: number;
  hits: number;
  walks: number;
  errors: number;
  pitches: number;
  runners_left_on: number;
};

export type ShowPlateAppearance = {
  game_id: string;
  event_seq: number;
  batter_name_raw: string;
  pitcher_name_raw: string;
  batter_mlb_id?: number | null;
  pitcher_mlb_id?: number | null;
  result?: string | null;
  batted_ball_type?: string | null;
  fielder_pos?: string | null;
  putout_code?: string | null;
  is_out?: boolean | null;
  is_double_play?: boolean | null;
  is_sac_fly?: boolean | null;
  is_sac_bunt?: boolean | null;
  runs_scored?: number | null;
  rbi?: number | null;
  hr_distance_ft?: number | null;
  is_perfect_perfect?: boolean | null;
  exit_vel_mph?: number | null;
  is_strikeout?: boolean | null;
  k_pitch_type?: string | null;
  k_loc_height?: string | null;
  k_loc_width?: string | null;
  k_is_chase?: boolean | null;
  k_is_looking?: boolean | null;
  k_timing?: string | null;
  batter_side?: string | null;
  pitcher_throws?: string | null;
  hit_direction?: string | null;
  is_error?: boolean | null;
  error_pos?: string | null;
};

export type ShowBatterBoxscore = {
  game_id: string;
  is_home: boolean;
  appearance_idx: number;
  replaced_apperance_idx?: number | null;
  player_name_raw: string;
  mlb_id?: number | null;
  ab: number;
  h: number;
  r: number;
  rbi: number;
  bb: number;
  so: number;
  doubles: number;
  triples: number;
  hr: number;
  sh: number;
  sf: number;
  gidp: number;
  e: number;
  pb: number;
  hbp: number;
  sb: number;
  cs: number;
  innings: number;
  pos: number;
};

export type ShowPitcherBoxscore = {
  game_id: string;
  is_home: boolean;
  appearance_idx: number;
  player_name_raw: string;
  mlb_id?: number | null;
  ip_raw: string;
  outs_pitched: number;
  r: number;
  h: number;
  er: number;
  bb: number;
  so: number;
  era?: number | null;
  wp: number;
  win: number;
  loss: number;
  save: number;
  b_save: number;
  hold: number;
  s_wins: number;
  s_losses: number;
  s_saves: number;
  s_b_saves: number;
  s_holds: number;
};

export type ShowGameBundle = {
  events: ShowGameEvent[];
  half_innings: ShowHalfInningSummary[];
  plate_appearances: ShowPlateAppearance[];
  batter_boxscores: ShowBatterBoxscore[];
  pitcher_boxscores: ShowPitcherBoxscore[];
};

export type ShowCardStats = {
  mlb_id: number;
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  pa: number;
  avg: number;
  obp: number;
  slg: number;
  ops: number;
  k_pct: number;
  bb_pct: number;
  hr: number;
};

export type ShowCardPitchingStats = {
  mlb_id: number;
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  pa: number;
  era?: number | null;
  whip?: number | null;
  k_pct: number;
  bb_pct: number;
  hr_pct: number;
};

export type PitchTypeRank = {
  pitchType: string;
  kPct: number | null;
};

export type HitterSide = "left" | "right" | "all";
export type PitcherHand = "left" | "right" | "all";
export type TimingType = "all" | "late" | "early";
export type OutType = "all" | "looking" | "chasing";

export type ShowPitcherSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  pitch_hand_code?: string | null;
};

export type ShowHitterSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  bat_side_code?: string | null;
};

export type SectionTab = "Analytics" | "Game Log" | "Cards" | "Coaching";
