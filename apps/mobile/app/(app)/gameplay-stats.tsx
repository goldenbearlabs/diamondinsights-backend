import { useEffect, useMemo, useRef, useState, type Dispatch, type SetStateAction } from "react";
import {
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useWindowDimensions,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { Ionicons } from "@expo/vector-icons";
import { useLocalSearchParams, useRouter } from "expo-router";

import { ApiError, apiGet, apiGetAuth } from "../../src/lib/api";
import { auth } from "../../src/lib/firebase";
import { useProfileImageUri, resolveAvatarUrl } from "../../src/lib/profileImage";
import { Avatar } from "../../src/components/Avatar";
import {
  HitDataSection,
  type HitDataMap,
  type HitDataStat,
  type HitZoneKey,
} from "../../src/components/HitDataSection";
import { StrikeoutMap } from "../../src/components/StrikeoutMap";
import { theme } from "../../src/theme/colors";

type ShowProfile = {
  username: string;
};

type ShowGameSummary = {
  games_played: number;
  wins: number;
  losses: number;
  record: string;
  last_game_date?: string | null;
  last_game_difficulty?: string | null;
};

type ShowGameLogItem = {
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

type PlateAppearanceStats = {
  plate_appearances: number;
  hits: number;
  walks: number;
  strikeouts: number;
  avg?: number | null;
  obp?: number | null;
  slg?: number | null;
  ops?: number | null;
  kbb?: number | null;
};

type ShowSkills = {
  hitting: PlateAppearanceStats;
  pitching: PlateAppearanceStats;
};

type ShowAggregateStats = {
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

type BattingArchetype = {
  overall: number;
  power: number;
  timing: number;
  location: number;
  pa: number;
};

type PitchingArchetype = {
  overall: number;
  consistency: number;
  strikeout: number;
  location: number;
  pa: number;
};

type StrikeoutMapData = {
  zones: number[][];
  outside: {
    top_left: number;
    top: number;
    top_right: number;
    right: number;
    bottom_right: number;
    bottom: number;
    bottom_left: number;
    left: number;
  };
  total: number;
  pa: number;
  pitch_type_options: string[];
  stats: StrikeoutStats;
  stats_by_zone: StrikeoutStats[][];
  stats_by_outside: Record<StrikeoutOutsideKey, StrikeoutStats>;
  counts_by_zone: StrikeoutCounts[][];
  counts_by_outside: Record<StrikeoutOutsideKey, StrikeoutCounts>;
};

type PitchTypeRank = {
  pitchType: string;
  kPct: number | null;
};

type StrikeoutStats = {
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

type StrikeoutCounts = {
  k: number;
  chase: number;
  look: number;
  eye: number;
  early: number;
  late: number;
};

type StrikeoutOutsideKey =
  | "top_left"
  | "top"
  | "top_right"
  | "right"
  | "bottom_right"
  | "bottom"
  | "bottom_left"
  | "left";

type StrikeoutSelection =
  | { kind: "zone"; row: number; col: number }
  | { kind: "outside"; key: StrikeoutOutsideKey }
  | null;

type HitterSide = "left" | "right" | "all";
type PitcherHand = "left" | "right" | "all";
type PitchType = string;
type TimingType = "all" | "late" | "early";
type OutType = "all" | "looking" | "chasing";
type HitBaseState = "all" | "runner_on" | "risp" | "loaded";
type HitOutState = "all" | "1" | "2";
type HitAbState = "all" | "1" | "2" | "3plus";
type HitPitcherState = "all" | "1" | "2" | "3plus";
type GameLogResultFilter = "all" | "wins" | "losses";
type StatHelp = {
  title: string;
  description: string;
  formula: string;
};

const HITTER_SIDE_OPTIONS: { label: string; value: HitterSide }[] = [
  { label: "All hitters", value: "all" },
  { label: "Left hitters", value: "left" },
  { label: "Right hitters", value: "right" },
];

const PITCHER_HAND_OPTIONS: { label: string; value: PitcherHand }[] = [
  { label: "All pitchers", value: "all" },
  { label: "Left pitchers", value: "left" },
  { label: "Right pitchers", value: "right" },
];


const TIMING_OPTIONS: { label: string; value: TimingType }[] = [
  { label: "All", value: "all" },
  { label: "Late", value: "late" },
  { label: "Early", value: "early" },
];

const OUT_TYPE_OPTIONS: { label: string; value: OutType }[] = [
  { label: "All", value: "all" },
  { label: "Looking", value: "looking" },
  { label: "Chasing", value: "chasing" },
];

const HIT_BASE_OPTIONS: { label: string; value: HitBaseState }[] = [
  { label: "All", value: "all" },
  { label: "Runner On", value: "runner_on" },
  { label: "RISP", value: "risp" },
  { label: "Loaded", value: "loaded" },
];

const HIT_OUT_OPTIONS: { label: string; value: HitOutState }[] = [
  { label: "All", value: "all" },
  { label: "0 out", value: "0" },
  { label: "1 out", value: "1" },
  { label: "2 out", value: "2" },
];

const HIT_AB_OPTIONS: { label: string; value: HitAbState }[] = [
  { label: "All", value: "all" },
  { label: "1st AB", value: "1" },
  { label: "2nd AB", value: "2" },
  { label: "3rd+ AB", value: "3plus" },
];

const HIT_PITCHER_COUNT_OPTIONS: { label: string; value: HitPitcherState }[] = [
  { label: "All", value: "all" },
  { label: "1st Pitcher", value: "1" },
  { label: "2nd Pitcher", value: "2" },
  { label: "3+ Pitchers", value: "3plus" },
];

const HIT_STAT_OPTIONS: { label: string; value: HitDataStat }[] = [
  { label: "Count", value: "count" },
  { label: "Share", value: "share" },
  { label: "BABIP", value: "babip" },
  { label: "wOBA", value: "woba" },
  { label: "SLG", value: "slug" },
];

const GAME_LOG_RESULT_OPTIONS: { label: string; value: GameLogResultFilter }[] = [
  { label: "All", value: "all" },
  { label: "Wins", value: "wins" },
  { label: "Losses", value: "losses" },
];

const defaultHitAdvancedFilters = {
  baseState: "all" as HitBaseState,
  outs: "all" as HitOutState,
  abCount: "all" as HitAbState,
  minSeen: "",
  maxSeen: "",
  pitcherCount: "all" as HitPitcherState,
};

const STRIKEOUT_STAT_HELP: Record<string, StatHelp> = {
  "K%": {
    title: "K%",
    description: "Share of plate appearances that end in a strikeout.",
    formula: "K% = strikeouts / plate appearances × 100",
  },
  "Chase %": {
    title: "Chase %",
    description: "Among strikeouts, the share where the batter chased out of the zone.",
    formula: "Chase% = chase strikeouts / strikeouts × 100",
  },
  "Freeze %": {
    title: "Freeze %",
    description: "Among strikeouts, the share where the batter took a called strike.",
    formula: "Freeze% = looking strikeouts / strikeouts × 100",
  },
  "Timing Bias": {
    title: "Timing Bias",
    description:
      "Bias toward early versus late timing on strikeout swings. Positive is more early, negative is more late.",
    formula: "Timing Bias = 100 × (E − L) / (E + L), where E=early Ks, L=late Ks",
  },
  "Mistime K%": {
    title: "Mistime K%",
    description: "Share of strikeouts that came on mistimed swings (early or late).",
    formula: "Mistime K% = (early + late) / strikeouts × 100",
  },
  "Eye K%": {
    title: "Eye K%",
    description: "Share of strikeouts that were either chase or looking.",
    formula: "Eye K% = (chase + looking) / strikeouts × 100",
  },
  "Location K%": {
    title: "Location K%",
    description: "Remainder after mistime and eye. Interpreted as location-driven Ks.",
    formula: "Location K% = 100 − (Mistime K% + Eye K%)",
  },
};

const HIT_DATA_STAT_HELP: Record<string, StatHelp> = {
  "Sweet Spot%": {
    title: "Sweet Spot%",
    description:
      "Share of balls in play that were line drives, deep fly balls, or perfect-perfects (no double counting).",
    formula: "Sweet Spot% = (LD + Deep FB + PP) / BIP × 100",
  },
  "Popup%": {
    title: "Popup%",
    description: "Share of balls in play that were popups.",
    formula: "Popup% = popups / BIP × 100",
  },
  "Flyball%": {
    title: "Flyball%",
    description: "Share of balls in play that were fly balls.",
    formula: "Flyball% = fly balls / BIP × 100",
  },
  "GB/Air%": {
    title: "GB/Air%",
    description:
      "Ground balls divided by air balls (fly balls + line drives). Over 100% is more ground-heavy.",
    formula: "GB/Air% = ground balls / (fly balls + line drives) × 100",
  },
  "Pulled Air%": {
    title: "Pulled Air%",
    description:
      "Share of air balls (fly balls + line drives + HR) hit to the pull side.",
    formula: "Pulled Air% = pulled air / air balls × 100",
  },
  "Oppo Air%": {
    title: "Oppo Air%",
    description:
      "Share of air balls (fly balls + line drives + HR) hit to the opposite field.",
    formula: "Oppo Air% = opposite air / air balls × 100",
  },
  "Perfect Perfect%": {
    title: "Perfect Perfect%",
    description: "Share of balls in play that were perfect-perfect.",
    formula: "Perfect Perfect% = perfect-perfect / BIP × 100",
  },
};

type ShowUserSearchResult = {
  user_id?: number | null;
  username: string;
  display_name?: string | null;
  profile_img_url?: string | null;
};

type ShowPitcherSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  pitch_hand_code?: string | null;
};

type ShowHitterSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  bat_side_code?: string | null;
};

type ShowCardStats = {
  mlb_id: number;
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
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

type ShowCardPitchingStats = {
  mlb_id: number;
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  pa: number;
  outs_pitched: number;
  h: number;
  r: number;
  hr: number;
  bb: number;
  so: number;
  hbp: number;
  avg: number;
  obp: number;
  slg: number;
  ops: number;
  woba: number;
  babip: number;
  k_pct: number;
  bb_pct: number;
  hr_pct: number;
  xbh_pct: number;
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
  era?: number | null;
  whip?: number | null;
  kbb?: number | null;
};

type CardSortDirection = "asc" | "desc";

const CARD_COLUMNS = [
  {
    key: "player",
    label: "Player",
    width: 120,
    align: "left",
    format: (row: ShowCardStats) => formatCardName(row),
    sortValue: (row: ShowCardStats) => formatCardName(row).toLowerCase(),
  },
  { key: "pa", label: "PA", width: 44, align: "right", format: (row: ShowCardStats) => formatCount(row.pa) },
  { key: "ab", label: "AB", width: 60, align: "right", format: (row: ShowCardStats) => formatCount(row.ab) },
  { key: "r", label: "R", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.r) },
  { key: "h", label: "H", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.h) },
  { key: "rbi", label: "RBI", width: 60, align: "right", format: (row: ShowCardStats) => formatCount(row.rbi) },
  { key: "singles", label: "1B", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.singles) },
  { key: "doubles", label: "2B", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.doubles) },
  { key: "triples", label: "3B", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.triples) },
  { key: "hr", label: "HR", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.hr) },
  { key: "bb", label: "BB", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.bb) },
  { key: "so", label: "SO", width: 50, align: "right", format: (row: ShowCardStats) => formatCount(row.so) },
  { key: "avg", label: "AVG", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.avg) },
  { key: "obp", label: "OBP", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.obp) },
  { key: "slg", label: "SLG", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.slg) },
  { key: "ops", label: "OPS", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.ops) },
  { key: "lob", label: "LOB", width: 60, align: "right", format: (row: ShowCardStats) => formatCount(row.lob) },
  { key: "gidp", label: "GIDP", width: 60, align: "right", format: (row: ShowCardStats) => formatCount(row.gidp) },
  {
    key: "gidp_pct",
    label: "GIDP%",
    width: 70,
    align: "right",
    format: (row: ShowCardStats) => formatPercent(row.gidp_pct ?? null),
  },
  { key: "woba", label: "wOBA", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.woba) },
  { key: "iso", label: "ISO", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.iso) },
  { key: "babip", label: "BABIP", width: 70, align: "right", format: (row: ShowCardStats) => formatRate(row.babip) },
  { key: "k_pct", label: "K%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.k_pct) },
  { key: "bb_pct", label: "BB%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.bb_pct) },
  { key: "hr_pct", label: "HR%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.hr_pct) },
  { key: "xbh_pct", label: "XBH%", width: 70, align: "right", format: (row: ShowCardStats) => formatPercent(row.xbh_pct) },
  { key: "rs_pct", label: "RS%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.rs_pct) },
  { key: "chase_pct", label: "Chase%", width: 70, align: "right", format: (row: ShowCardStats) => formatPercent(row.chase_pct) },
  { key: "freeze_pct", label: "Freeze%", width: 70, align: "right", format: (row: ShowCardStats) => formatPercent(row.freeze_pct) },
  { key: "timing_pct", label: "Timing Bias", width: 86, align: "right", format: (row: ShowCardStats) => formatTimingBias(row.timing_pct) },
  { key: "timing_k_pct", label: "Mistime%", width: 72, align: "right", format: (row: ShowCardStats) => formatPercent(row.timing_k_pct) },
  { key: "eye_k_pct", label: "Eye%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.eye_k_pct) },
  { key: "location_k_pct", label: "Loc%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.location_k_pct) },
  { key: "sweet_spot_pct", label: "Sweet Spot%", width: 86, align: "right", format: (row: ShowCardStats) => formatPercent(row.sweet_spot_pct) },
  { key: "popup_rate", label: "Popup%", width: 70, align: "right", format: (row: ShowCardStats) => formatPercent(row.popup_rate) },
  { key: "flyball_rate", label: "Fly%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.flyball_rate) },
  { key: "gb_air_ratio", label: "GB/Air%", width: 72, align: "right", format: (row: ShowCardStats) => formatPercent(row.gb_air_ratio) },
  { key: "pulled_air_rate", label: "Pulled Air%", width: 86, align: "right", format: (row: ShowCardStats) => formatPercent(row.pulled_air_rate) },
  { key: "oppo_air_rate", label: "Oppo Air%", width: 78, align: "right", format: (row: ShowCardStats) => formatPercent(row.oppo_air_rate) },
  { key: "perfect_perfect_pct", label: "PP%", width: 60, align: "right", format: (row: ShowCardStats) => formatPercent(row.perfect_perfect_pct) },
] as const;

type CardColumn = (typeof CARD_COLUMNS)[number];
type CardSortKey = CardColumn["key"];
const CARD_FROZEN_KEYS: CardSortKey[] = ["player", "pa"];

const PITCHING_CARD_COLUMNS = [
  {
    key: "player",
    label: "Player",
    width: 120,
    align: "left",
    format: (row: ShowCardPitchingStats) => formatCardName(row),
    sortValue: (row: ShowCardPitchingStats) => formatCardName(row).toLowerCase(),
  },
  {
    key: "pa",
    label: "BF",
    width: 44,
    align: "right",
    format: (row: ShowCardPitchingStats) => formatCount(row.pa),
  },
  {
    key: "outs_pitched",
    label: "IP",
    width: 58,
    align: "right",
    format: (row: ShowCardPitchingStats) => formatInnings(row.outs_pitched),
  },
  { key: "h", label: "H", width: 50, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.h) },
  { key: "r", label: "R", width: 50, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.r) },
  { key: "hr", label: "HR", width: 50, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.hr) },
  { key: "bb", label: "BB", width: 50, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.bb) },
  { key: "so", label: "SO", width: 50, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.so) },
  { key: "hbp", label: "HBP", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatCount(row.hbp) },
  { key: "avg", label: "AVG", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.avg) },
  { key: "obp", label: "OBP", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.obp) },
  { key: "slg", label: "SLG", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.slg) },
  { key: "ops", label: "OPS", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.ops) },
  { key: "woba", label: "wOBA", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.woba) },
  { key: "babip", label: "BABIP", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRate(row.babip) },
  { key: "k_pct", label: "K%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.k_pct) },
  { key: "bb_pct", label: "BB%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.bb_pct) },
  { key: "hr_pct", label: "HR%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.hr_pct) },
  { key: "xbh_pct", label: "XBH%", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.xbh_pct) },
  { key: "era", label: "ERA", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatRatio(row.era) },
  { key: "whip", label: "WHIP", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatRatio(row.whip) },
  { key: "kbb", label: "K/BB", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatRatio(row.kbb) },
  { key: "chase_pct", label: "Chase%", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.chase_pct) },
  { key: "freeze_pct", label: "Freeze%", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.freeze_pct) },
  { key: "timing_pct", label: "Timing Bias", width: 86, align: "right", format: (row: ShowCardPitchingStats) => formatTimingBias(row.timing_pct) },
  { key: "timing_k_pct", label: "Mistime%", width: 72, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.timing_k_pct) },
  { key: "eye_k_pct", label: "Eye%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.eye_k_pct) },
  { key: "location_k_pct", label: "Loc%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.location_k_pct) },
  { key: "sweet_spot_pct", label: "Sweet Spot%", width: 86, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.sweet_spot_pct) },
  { key: "popup_rate", label: "Popup%", width: 70, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.popup_rate) },
  { key: "flyball_rate", label: "Fly%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.flyball_rate) },
  { key: "gb_air_ratio", label: "GB/Air%", width: 72, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.gb_air_ratio) },
  { key: "pulled_air_rate", label: "Pulled Air%", width: 86, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.pulled_air_rate) },
  { key: "oppo_air_rate", label: "Oppo Air%", width: 78, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.oppo_air_rate) },
  { key: "perfect_perfect_pct", label: "PP%", width: 60, align: "right", format: (row: ShowCardPitchingStats) => formatPercent(row.perfect_perfect_pct) },
] as const;

type PitchingCardColumn = (typeof PITCHING_CARD_COLUMNS)[number];
type PitchingCardSortKey = PitchingCardColumn["key"];

export default function GameplayStatsScreen() {
  const { width } = useWindowDimensions();
  useProfileImageUri(); // keep subscription active for cache warming
  const router = useRouter();
  const localParams = useLocalSearchParams<{
    viewUsername?: string | string[];
    viewUserId?: string | string[];
  }>();
  const requestRef = useRef(0);
  const pitcherRequestRef = useRef(0);
  const hitterRequestRef = useRef(0);
  const hitPitcherRequestRef = useRef(0);
  const hitHitterRequestRef = useRef(0);
  const [showProfile, setShowProfile] = useState<ShowProfile | null>(null);
  const [gameSummary, setGameSummary] = useState<ShowGameSummary | null>(null);
  const [gameLog, setGameLog] = useState<ShowGameLogItem[]>([]);
  const [gameLogLoading, setGameLogLoading] = useState(false);
  const [gameLogError, setGameLogError] = useState<string | null>(null);
  const [gameLogDifficulty, setGameLogDifficulty] = useState<string>("all");
  const [gameLogResult, setGameLogResult] = useState<GameLogResultFilter>("all");
  const [gameLogBallpark, setGameLogBallpark] = useState<string>("");
  const [skills, setSkills] = useState<ShowSkills | null>(null);
  const [battingArchetype, setBattingArchetype] = useState<BattingArchetype | null>(null);
  const [pitchingArchetype, setPitchingArchetype] = useState<PitchingArchetype | null>(null);
  const [strikeoutMap, setStrikeoutMap] = useState<StrikeoutMapData | null>(null);
  const [pitchTypeRanks, setPitchTypeRanks] = useState<PitchTypeRank[]>([]);
  const [pitchTypeRanksLoading, setPitchTypeRanksLoading] = useState(false);
  const [pitchTypeRanksError, setPitchTypeRanksError] = useState<string | null>(null);
  const [hitDataMap, setHitDataMap] = useState<HitDataMap | null>(null);
  const [aggregateStats, setAggregateStats] = useState<ShowAggregateStats | null>(null);
  const [aggregateStatsLoading, setAggregateStatsLoading] = useState(false);
  const [aggregateStatsError, setAggregateStatsError] = useState<string | null>(null);
  const [cardStats, setCardStats] = useState<ShowCardStats[]>([]);
  const [cardStatsLoading, setCardStatsLoading] = useState(false);
  const [cardStatsError, setCardStatsError] = useState<string | null>(null);
  const [cardPitchingStats, setCardPitchingStats] = useState<ShowCardPitchingStats[]>([]);
  const [cardPitchingStatsLoading, setCardPitchingStatsLoading] = useState(false);
  const [cardPitchingStatsError, setCardPitchingStatsError] = useState<string | null>(null);
  const [cardHittingFilter, setCardHittingFilter] = useState("");
  const [cardHittingMinPa, setCardHittingMinPa] = useState("");
  const [cardPitchingFilter, setCardPitchingFilter] = useState("");
  const [cardPitchingMinBf, setCardPitchingMinBf] = useState("");
  const [cardSortKey, setCardSortKey] = useState<CardSortKey>("pa");
  const [cardSortDirection, setCardSortDirection] = useState<CardSortDirection>("desc");
  const [cardPitchingSortKey, setCardPitchingSortKey] = useState<PitchingCardSortKey>("pa");
  const [cardPitchingSortDirection, setCardPitchingSortDirection] = useState<CardSortDirection>("desc");
  const [loading, setLoading] = useState(true);
  const [viewUserId, setViewUserId] = useState<number | null>(null);
  const [viewUsername, setViewUsername] = useState<string | null>(null);
  const [viewProfileImage, setViewProfileImage] = useState<string | null>(null);
  const [skillMode, setSkillMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [strikeoutMode, setStrikeoutMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [hitMode, setHitMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [statsMode, setStatsMode] = useState<"Hitting" | "Pitching">("Hitting");
  const [hitStatMode, setHitStatMode] = useState<HitDataStat>("count");
  const [hitFilterHitterSide, setHitFilterHitterSide] = useState<HitterSide>("all");
  const [hitFilterPitcherHand, setHitFilterPitcherHand] = useState<PitcherHand>("all");
  const [hitFocusZone, setHitFocusZone] = useState<HitZoneKey | null>(null);
  const [hitAdvancedFiltersOpen, setHitAdvancedFiltersOpen] = useState(false);
  const [hitAdvancedFilters, setHitAdvancedFilters] = useState(defaultHitAdvancedFilters);
  const [sectionTab, setSectionTab] = useState<
    "Analytics" | "Game Log" | "Cards" | "Coaching"
  >("Analytics");
  const [strikeoutSelections, setStrikeoutSelections] = useState<StrikeoutSelection[]>([]);
  const [filterHitterSide, setFilterHitterSide] = useState<{ side: HitterSide }>({
    side: "all",
  });
  const [filterPitcherHand, setFilterPitcherHand] = useState<{ hand: PitcherHand }>({
    hand: "all",
  });
  const [filterPitchTypes, setFilterPitchTypes] = useState<PitchType[]>([]);
  const [activeFilterMenu, setActiveFilterMenu] = useState<null | "hitter" | "pitcher">(null);
  const [advancedFiltersOpen, setAdvancedFiltersOpen] = useState(false);
  const [statHelpOpen, setStatHelpOpen] = useState(false);
  const [statHelp, setStatHelp] = useState<StatHelp | null>(null);
  const defaultAdvancedFilters = {
    minSpeed: "",
    maxSpeed: "",
    timing: "all" as TimingType,
    outType: "all" as OutType,
  };
  const [advancedFilters, setAdvancedFilters] = useState<{
    minSpeed: string;
    maxSpeed: string;
    timing: TimingType;
    outType: OutType;
  }>(defaultAdvancedFilters);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<ShowUserSearchResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [searchImages, setSearchImages] = useState<Record<string, string>>({});
  const [pitcherSearchQuery, setPitcherSearchQuery] = useState("");
  const [pitcherSearchResults, setPitcherSearchResults] = useState<
    ShowPitcherSearchResult[]
  >([]);
  const [pitcherSearchLoading, setPitcherSearchLoading] = useState(false);
  const [pitcherSearchError, setPitcherSearchError] = useState<string | null>(null);
  const [selectedPitcher, setSelectedPitcher] = useState<ShowPitcherSearchResult | null>(
    null
  );
  const [hitterSearchQuery, setHitterSearchQuery] = useState("");
  const [hitterSearchResults, setHitterSearchResults] = useState<ShowHitterSearchResult[]>(
    []
  );
  const [hitterSearchLoading, setHitterSearchLoading] = useState(false);
  const [hitterSearchError, setHitterSearchError] = useState<string | null>(null);
  const [selectedHitter, setSelectedHitter] = useState<ShowHitterSearchResult | null>(null);
  const [hitPitcherSearchQuery, setHitPitcherSearchQuery] = useState("");
  const [hitPitcherSearchResults, setHitPitcherSearchResults] = useState<
    ShowPitcherSearchResult[]
  >([]);
  const [hitPitcherSearchLoading, setHitPitcherSearchLoading] = useState(false);
  const [hitPitcherSearchError, setHitPitcherSearchError] = useState<string | null>(null);
  const [hitHitterSearchQuery, setHitHitterSearchQuery] = useState("");
  const [hitHitterSearchResults, setHitHitterSearchResults] = useState<
    ShowHitterSearchResult[]
  >([]);
  const [hitHitterSearchLoading, setHitHitterSearchLoading] = useState(false);
  const [hitHitterSearchError, setHitHitterSearchError] = useState<string | null>(null);
  const isSelfView = !viewUserId && !viewUsername;

  useEffect(() => {
    const rawUsername = localParams.viewUsername;
    const rawUserId = localParams.viewUserId;

    const nextUsername = Array.isArray(rawUsername) ? rawUsername[0] : rawUsername;
    const nextUserIdRaw = Array.isArray(rawUserId) ? rawUserId[0] : rawUserId;
    const nextUserId = nextUserIdRaw ? Number(nextUserIdRaw) : NaN;

    if (nextUsername && nextUsername.trim()) {
      setViewUsername(nextUsername.trim());
      setViewUserId(null);
      return;
    }

    if (Number.isFinite(nextUserId) && nextUserId > 0) {
      setViewUserId(nextUserId);
      setViewUsername(null);
    }
  }, [localParams.viewUserId, localParams.viewUsername]);

  useEffect(() => {
    setShowProfile(null);
    setGameSummary(null);
    setGameLog([]);
    setGameLogLoading(false);
    setGameLogError(null);
    setGameLogDifficulty("all");
    setGameLogResult("all");
    setGameLogBallpark("");
    setSkills(null);
    setBattingArchetype(null);
    setPitchingArchetype(null);
    setStrikeoutMap(null);
    setAggregateStats(null);
    setAggregateStatsLoading(false);
    setAggregateStatsError(null);
    setCardStats([]);
    setCardStatsLoading(false);
    setCardStatsError(null);
    setCardPitchingStats([]);
    setCardPitchingStatsLoading(false);
    setCardPitchingStatsError(null);
    setCardHittingFilter("");
    setCardHittingMinPa("");
    setCardPitchingFilter("");
    setCardPitchingMinBf("");
    setCardSortKey("pa");
    setCardSortDirection("desc");
    setCardPitchingSortKey("pa");
    setCardPitchingSortDirection("desc");
    setSelectedPitcher(null);
    setPitcherSearchQuery("");
    setPitcherSearchResults([]);
    setPitcherSearchLoading(false);
    setPitcherSearchError(null);
    setSelectedHitter(null);
    setHitterSearchQuery("");
    setHitterSearchResults([]);
    setHitterSearchLoading(false);
    setHitterSearchError(null);
    setHitPitcherSearchQuery("");
    setHitPitcherSearchResults([]);
    setHitPitcherSearchLoading(false);
    setHitPitcherSearchError(null);
    setHitHitterSearchQuery("");
    setHitHitterSearchResults([]);
    setHitHitterSearchLoading(false);
    setHitHitterSearchError(null);
  }, [viewUserId, viewUsername]);

  useEffect(() => {
    let active = true;

    const loadShowProfile = async () => {
      setLoading(true);
      try {
        const path = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}`
          : viewUserId
            ? `/users/${viewUserId}/show`
            : "/users/me/show";
        const data = isSelfView
          ? await apiGetAuth<ShowProfile>(path)
          : await apiGet<ShowProfile>(path);
        if (!active) return;
        setShowProfile(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setShowProfile(null);
        } else {
          setShowProfile(null);
        }
      } finally {
        if (active) setLoading(false);
      }
    };

    loadShowProfile();

    return () => {
      active = false;
    };
  }, [
    viewUserId,
    viewUsername,
    isSelfView,
  ]);

  useEffect(() => {
    const options = strikeoutMap?.pitch_type_options ?? [];
    if (options.length === 0) {
      setFilterPitchTypes([]);
      return;
    }
    setFilterPitchTypes((prev) => prev.filter((item) => options.includes(item)));
  }, [strikeoutMap?.pitch_type_options?.join("|")]);

  useEffect(() => {
    if (!searchOpen) {
      setSearchQuery("");
      setSearchResults([]);
      setSearchLoading(false);
      setSearchError(null);
      return;
    }
    const trimmed = searchQuery.trim();
    if (!trimmed) {
      setSearchResults([]);
      setSearchLoading(false);
      setSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = requestRef.current + 1;
      requestRef.current = requestId;
      setSearchLoading(true);
      setSearchError(null);

      try {
        const params = new URLSearchParams({ q: trimmed, limit: "12" });
        const data = await apiGet<ShowUserSearchResult[]>(
          `/users/show/search?${params.toString()}`
        );
        if (requestRef.current !== requestId) return;
        setSearchResults(data);
        await resolveSearchImages(data);
      } catch (err: any) {
        if (requestRef.current !== requestId) return;
        setSearchError(err?.message ?? "Search failed.");
      } finally {
        if (requestRef.current === requestId) setSearchLoading(false);
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [searchOpen, searchQuery]);

  useEffect(() => {
    if (activeFilterMenu !== "pitcher") {
      setPitcherSearchQuery("");
      setPitcherSearchResults([]);
      setPitcherSearchLoading(false);
      setPitcherSearchError(null);
      return;
    }

    const trimmed = pitcherSearchQuery.trim();
    if (!trimmed) {
      setPitcherSearchResults([]);
      setPitcherSearchLoading(false);
      setPitcherSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = pitcherRequestRef.current + 1;
      pitcherRequestRef.current = requestId;
      setPitcherSearchLoading(true);
      setPitcherSearchError(null);

      try {
        const params = new URLSearchParams({ q: trimmed, limit: "12" });
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/pitchers`
          : viewUserId
            ? `/users/${viewUserId}/show/pitchers`
            : "/users/me/show/pitchers";
        const path = `${basePath}?${params.toString()}`;
        const data = isSelfView
          ? await apiGetAuth<ShowPitcherSearchResult[]>(path)
          : await apiGet<ShowPitcherSearchResult[]>(path);
        if (pitcherRequestRef.current !== requestId) return;
        setPitcherSearchResults(data);
      } catch (err: any) {
        if (pitcherRequestRef.current !== requestId) return;
        setPitcherSearchError(err?.message ?? "Search failed.");
      } finally {
        if (pitcherRequestRef.current === requestId) setPitcherSearchLoading(false);
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [activeFilterMenu, pitcherSearchQuery, viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    if (activeFilterMenu !== "hitter") {
      setHitterSearchQuery("");
      setHitterSearchResults([]);
      setHitterSearchLoading(false);
      setHitterSearchError(null);
      return;
    }

    const trimmed = hitterSearchQuery.trim();
    if (!trimmed) {
      setHitterSearchResults([]);
      setHitterSearchLoading(false);
      setHitterSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = hitterRequestRef.current + 1;
      hitterRequestRef.current = requestId;
      setHitterSearchLoading(true);
      setHitterSearchError(null);

      try {
        const params = new URLSearchParams({
          q: trimmed,
          limit: "12",
          view: strikeoutMode.toLowerCase(),
        });
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/hitters`
          : viewUserId
            ? `/users/${viewUserId}/show/hitters`
            : "/users/me/show/hitters";
        const path = `${basePath}?${params.toString()}`;
        const data = isSelfView
          ? await apiGetAuth<ShowHitterSearchResult[]>(path)
          : await apiGet<ShowHitterSearchResult[]>(path);
        if (hitterRequestRef.current !== requestId) return;
        setHitterSearchResults(data);
      } catch (err: any) {
        if (hitterRequestRef.current !== requestId) return;
        setHitterSearchError(err?.message ?? "Search failed.");
      } finally {
        if (hitterRequestRef.current === requestId) setHitterSearchLoading(false);
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [
    activeFilterMenu,
    hitterSearchQuery,
    viewUserId,
    viewUsername,
    isSelfView,
    strikeoutMode,
  ]);

  useEffect(() => {
    const trimmed = hitPitcherSearchQuery.trim();
    if (!trimmed) {
      setHitPitcherSearchResults([]);
      setHitPitcherSearchLoading(false);
      setHitPitcherSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = hitPitcherRequestRef.current + 1;
      hitPitcherRequestRef.current = requestId;
      setHitPitcherSearchLoading(true);
      setHitPitcherSearchError(null);

      try {
        const params = new URLSearchParams({ q: trimmed, limit: "12" });
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/pitchers`
          : viewUserId
            ? `/users/${viewUserId}/show/pitchers`
            : "/users/me/show/pitchers";
        const path = `${basePath}?${params.toString()}`;
        const data = isSelfView
          ? await apiGetAuth<ShowPitcherSearchResult[]>(path)
          : await apiGet<ShowPitcherSearchResult[]>(path);
        if (hitPitcherRequestRef.current !== requestId) return;
        setHitPitcherSearchResults(data);
      } catch (err: any) {
        if (hitPitcherRequestRef.current !== requestId) return;
        setHitPitcherSearchError(err?.message ?? "Search failed.");
      } finally {
        if (hitPitcherRequestRef.current === requestId) {
          setHitPitcherSearchLoading(false);
        }
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [hitPitcherSearchQuery, viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    const trimmed = hitHitterSearchQuery.trim();
    if (!trimmed) {
      setHitHitterSearchResults([]);
      setHitHitterSearchLoading(false);
      setHitHitterSearchError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = hitHitterRequestRef.current + 1;
      hitHitterRequestRef.current = requestId;
      setHitHitterSearchLoading(true);
      setHitHitterSearchError(null);

      try {
        const params = new URLSearchParams({
          q: trimmed,
          limit: "12",
          view: "hitting",
        });
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/hitters`
          : viewUserId
            ? `/users/${viewUserId}/show/hitters`
            : "/users/me/show/hitters";
        const path = `${basePath}?${params.toString()}`;
        const data = isSelfView
          ? await apiGetAuth<ShowHitterSearchResult[]>(path)
          : await apiGet<ShowHitterSearchResult[]>(path);
        if (hitHitterRequestRef.current !== requestId) return;
        setHitHitterSearchResults(data);
      } catch (err: any) {
        if (hitHitterRequestRef.current !== requestId) return;
        setHitHitterSearchError(err?.message ?? "Search failed.");
      } finally {
        if (hitHitterRequestRef.current === requestId) {
          setHitHitterSearchLoading(false);
        }
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [hitHitterSearchQuery, viewUserId, viewUsername, isSelfView]);

  const resolveSearchImages = async (users: ShowUserSearchResult[]) => {
    const pending = users.filter((user) => {
      if (!user.profile_img_url) return false;
      const key = getSearchKey(user);
      return !!key && !searchImages[key];
    });
    if (!pending.length) return;

    const entries = await Promise.all(
      pending.map(async (user) => {
        try {
          const url = await resolveAvatarUrl(user.profile_img_url ?? "");
          const key = getSearchKey(user);
          return key && url ? ([key, url] as const) : null;
        } catch {
          return null;
        }
      })
    );

    if (!entries.length) return;
    setSearchImages((prev) => {
      const next = { ...prev };
      entries.forEach((entry) => {
        if (!entry) return;
        next[entry[0]] = entry[1];
      });
      return next;
    });
  };

  useEffect(() => {
    let active = true;

    const loadPitchingArchetype = async () => {
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/archetype/pitching`
          : viewUserId
            ? `/users/${viewUserId}/show/archetype/pitching`
            : "/users/me/show/archetype/pitching";
        const params = new URLSearchParams();
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView
          ? await apiGetAuth<PitchingArchetype>(path)
          : await apiGet<PitchingArchetype>(path);
        if (!active) return;
        setPitchingArchetype(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setPitchingArchetype(null);
        } else {
          setPitchingArchetype(null);
        }
      }
    };

    loadPitchingArchetype();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView, selectedPitcher?.mlb_id, selectedHitter?.mlb_id]);

  useEffect(() => {
    let active = true;

    const loadBattingArchetype = async () => {
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/archetype/batting`
          : viewUserId
            ? `/users/${viewUserId}/show/archetype/batting`
            : "/users/me/show/archetype/batting";
        const params = new URLSearchParams();
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView
          ? await apiGetAuth<BattingArchetype>(path)
          : await apiGet<BattingArchetype>(path);
        if (!active) return;
        setBattingArchetype(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setBattingArchetype(null);
        } else {
          setBattingArchetype(null);
        }
      }
    };

    loadBattingArchetype();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView, selectedPitcher?.mlb_id, selectedHitter?.mlb_id]);

  useEffect(() => {
    let active = true;

    const loadGameSummary = async () => {
      try {
        const path = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/summary`
          : viewUserId
            ? `/users/${viewUserId}/show/summary`
            : "/users/me/show/summary";
        const data = isSelfView
          ? await apiGetAuth<ShowGameSummary>(path)
          : await apiGet<ShowGameSummary>(path);
        if (!active) return;
        setGameSummary(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setGameSummary(null);
        } else {
          setGameSummary(null);
        }
      }
    };

    loadGameSummary();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    let active = true;

    const loadCardPitchingStats = async () => {
      setCardPitchingStatsLoading(true);
      setCardPitchingStatsError(null);
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/cards/pitching`
          : viewUserId
            ? `/users/${viewUserId}/show/cards/pitching`
            : "/users/me/show/cards/pitching";
        const data = isSelfView
          ? await apiGetAuth<ShowCardPitchingStats[]>(basePath)
          : await apiGet<ShowCardPitchingStats[]>(basePath);
        if (!active) return;
        setCardPitchingStats(Array.isArray(data) ? data : []);
      } catch (err: any) {
        if (!active) return;
        setCardPitchingStats([]);
        if (err instanceof ApiError && err.status === 404) {
          setCardPitchingStatsError("No pitching card stats available.");
        } else {
          setCardPitchingStatsError("Unable to load pitching card stats.");
        }
      } finally {
        if (active) setCardPitchingStatsLoading(false);
      }
    };

    loadCardPitchingStats();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    let active = true;

    const loadGameLog = async () => {
      setGameLogLoading(true);
      setGameLogError(null);
      try {
        const path = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/game-log?limit=200`
          : viewUserId
            ? `/users/${viewUserId}/show/game-log?limit=200`
            : "/users/me/show/game-log?limit=200";
        const data = isSelfView
          ? await apiGetAuth<ShowGameLogItem[]>(path)
          : await apiGet<ShowGameLogItem[]>(path);
        if (!active) return;
        setGameLog(Array.isArray(data) ? data : []);
      } catch (err: any) {
        if (!active) return;
        setGameLog([]);
        if (err instanceof ApiError && err.status === 404) {
          setGameLogError("No game log found.");
        } else {
          setGameLogError("Unable to load game log.");
        }
      } finally {
        if (active) setGameLogLoading(false);
      }
    };

    loadGameLog();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    let active = true;

    const loadSkills = async () => {
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/skills`
          : viewUserId
            ? `/users/${viewUserId}/show/skills`
            : "/users/me/show/skills";
        const params = new URLSearchParams();
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView ? await apiGetAuth<ShowSkills>(path) : await apiGet<ShowSkills>(path);
        if (!active) return;
        setSkills(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setSkills(null);
        } else {
          setSkills(null);
        }
      }
    };

    loadSkills();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView, selectedPitcher?.mlb_id, selectedHitter?.mlb_id]);

  useEffect(() => {
    let active = true;

    const loadAggregateStats = async () => {
      setAggregateStatsLoading(true);
      setAggregateStatsError(null);
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/stats`
          : viewUserId
            ? `/users/${viewUserId}/show/stats`
            : "/users/me/show/stats";
        const params = new URLSearchParams();
        params.set("view", statsMode.toLowerCase());
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView
          ? await apiGetAuth<ShowAggregateStats>(path)
          : await apiGet<ShowAggregateStats>(path);
        if (!active) return;
        setAggregateStats(data);
      } catch (err: any) {
        if (!active) return;
        setAggregateStats(null);
        if (err instanceof ApiError && err.status === 404) {
          setAggregateStatsError("No stats available.");
        } else {
          setAggregateStatsError("Unable to load stats.");
        }
      } finally {
        if (active) setAggregateStatsLoading(false);
      }
    };

    loadAggregateStats();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView, statsMode]);

  useEffect(() => {
    let active = true;

    const loadCardStats = async () => {
      setCardStatsLoading(true);
      setCardStatsError(null);
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/cards`
          : viewUserId
            ? `/users/${viewUserId}/show/cards`
            : "/users/me/show/cards";
        const data = isSelfView
          ? await apiGetAuth<ShowCardStats[]>(basePath)
          : await apiGet<ShowCardStats[]>(basePath);
        if (!active) return;
        setCardStats(Array.isArray(data) ? data : []);
      } catch (err: any) {
        if (!active) return;
        setCardStats([]);
        if (err instanceof ApiError && err.status === 404) {
          setCardStatsError("No card stats available.");
        } else {
          setCardStatsError("Unable to load card stats.");
        }
      } finally {
        if (active) setCardStatsLoading(false);
      }
    };

    loadCardStats();

    return () => {
      active = false;
    };
  }, [viewUserId, viewUsername, isSelfView]);

  useEffect(() => {
    let active = true;

    const loadStrikeoutMap = async () => {
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/strikeout-map`
          : viewUserId
            ? `/users/${viewUserId}/show/strikeout-map`
            : "/users/me/show/strikeout-map";
        const params = new URLSearchParams();
        if (filterHitterSide.side !== "all") {
          params.set("hitter_side", filterHitterSide.side === "left" ? "L" : "R");
        }
        if (filterPitcherHand.hand !== "all") {
          params.set("pitcher_hand", filterPitcherHand.hand === "left" ? "L" : "R");
        }
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        if (filterPitchTypes.length > 0) {
          params.set(
            "pitch_types",
            filterPitchTypes.map((pt) => pt.toLowerCase()).join(",")
          );
        }
        params.set("view", strikeoutMode.toLowerCase());
        if (advancedFilters.minSpeed) {
          params.set("min_speed", advancedFilters.minSpeed);
        }
        if (advancedFilters.maxSpeed) {
          params.set("max_speed", advancedFilters.maxSpeed);
        }
        if (advancedFilters.timing !== "all") {
          params.set("timing", advancedFilters.timing);
        }
        if (advancedFilters.outType !== "all") {
          params.set("out_type", advancedFilters.outType);
        }
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView
          ? await apiGetAuth<StrikeoutMapData>(path)
          : await apiGet<StrikeoutMapData>(path);
        if (!active) return;
        setStrikeoutMap(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setStrikeoutMap(null);
        } else {
          setStrikeoutMap(null);
        }
      }
    };

    loadStrikeoutMap();

    return () => {
      active = false;
    };
  }, [
    viewUserId,
    viewUsername,
    isSelfView,
    filterHitterSide.side,
    filterPitcherHand.hand,
    filterPitchTypes.join(","),
    strikeoutMode,
    advancedFilters.minSpeed,
    advancedFilters.maxSpeed,
    advancedFilters.timing,
    advancedFilters.outType,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  useEffect(() => {
    let active = true;

    const loadPitchTypeRanks = async () => {
      setPitchTypeRanksLoading(true);
      setPitchTypeRanksError(null);
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/strikeout-map`
          : viewUserId
            ? `/users/${viewUserId}/show/strikeout-map`
            : "/users/me/show/strikeout-map";
        const baseParams = new URLSearchParams();
        if (filterHitterSide.side !== "all") {
          baseParams.set("hitter_side", filterHitterSide.side === "left" ? "L" : "R");
        }
        if (filterPitcherHand.hand !== "all") {
          baseParams.set("pitcher_hand", filterPitcherHand.hand === "left" ? "L" : "R");
        }
        if (selectedPitcher?.mlb_id) {
          baseParams.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          baseParams.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        baseParams.set("view", "hitting");
        if (advancedFilters.minSpeed) {
          baseParams.set("min_speed", advancedFilters.minSpeed);
        }
        if (advancedFilters.maxSpeed) {
          baseParams.set("max_speed", advancedFilters.maxSpeed);
        }
        if (advancedFilters.timing !== "all") {
          baseParams.set("timing", advancedFilters.timing);
        }
        if (advancedFilters.outType !== "all") {
          baseParams.set("out_type", advancedFilters.outType);
        }
        const basePathWithParams = baseParams.toString()
          ? `${basePath}?${baseParams.toString()}`
          : basePath;
        const baseData = isSelfView
          ? await apiGetAuth<StrikeoutMapData>(basePathWithParams)
          : await apiGet<StrikeoutMapData>(basePathWithParams);
        if (!active) return;
        const pitchTypes = baseData?.pitch_type_options ?? [];
        if (pitchTypes.length === 0) {
          setPitchTypeRanks([]);
          return;
        }

        const requests = pitchTypes.map(async (pitchType) => {
          const params = new URLSearchParams(baseParams);
          params.set("pitch_types", pitchType.toLowerCase());
          const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
          const data = isSelfView
            ? await apiGetAuth<StrikeoutMapData>(path)
            : await apiGet<StrikeoutMapData>(path);
          return {
            pitchType,
            kPct: isFiniteNumber(data?.stats?.k_pct) ? data.stats.k_pct : null,
          };
        });
        const results = await Promise.all(requests);
        if (!active) return;
        setPitchTypeRanks(
          results
            .filter((row) => row.pitchType)
            .map((row) => ({
              pitchType: row.pitchType,
              kPct: row.kPct,
            }))
        );
      } catch (err: any) {
        if (!active) return;
        setPitchTypeRanks([]);
        setPitchTypeRanksError("Unable to load pitch rankings.");
      } finally {
        if (active) setPitchTypeRanksLoading(false);
      }
    };

    loadPitchTypeRanks();

    return () => {
      active = false;
    };
  }, [
    viewUserId,
    viewUsername,
    isSelfView,
    filterHitterSide.side,
    filterPitcherHand.hand,
    advancedFilters.minSpeed,
    advancedFilters.maxSpeed,
    advancedFilters.timing,
    advancedFilters.outType,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  useEffect(() => {
    let active = true;

    const loadHitDataMap = async () => {
      try {
        const basePath = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/hit-map`
          : viewUserId
            ? `/users/${viewUserId}/show/hit-map`
            : "/users/me/show/hit-map";
        const params = new URLSearchParams();
        params.set("view", hitMode.toLowerCase());
        params.set("stat", hitStatMode);
        if (hitFilterHitterSide !== "all") {
          params.set("hitter_side", hitFilterHitterSide === "left" ? "L" : "R");
        }
        if (hitFilterPitcherHand !== "all") {
          params.set("pitcher_hand", hitFilterPitcherHand === "left" ? "L" : "R");
        }
        if (selectedPitcher?.mlb_id) {
          params.set("pitcher_mlb_id", String(selectedPitcher.mlb_id));
        }
        if (selectedHitter?.mlb_id) {
          params.set("hitter_mlb_id", String(selectedHitter.mlb_id));
        }
        if (hitAdvancedFilters.baseState !== "all") {
          params.set("base_state", hitAdvancedFilters.baseState);
        }
        if (hitAdvancedFilters.outs !== "all") {
          params.set("outs", hitAdvancedFilters.outs);
        }
        if (hitAdvancedFilters.abCount !== "all") {
          params.set("ab_count", hitAdvancedFilters.abCount);
        }
        if (hitAdvancedFilters.minSeen) {
          params.set("min_seen", hitAdvancedFilters.minSeen);
        }
        if (hitAdvancedFilters.maxSeen) {
          params.set("max_seen", hitAdvancedFilters.maxSeen);
        }
        if (hitAdvancedFilters.pitcherCount !== "all") {
          params.set("pitcher_count", hitAdvancedFilters.pitcherCount);
        }
        if (hitFocusZone) {
          params.set("focus_zone", hitFocusZone);
        }
        const path = params.toString() ? `${basePath}?${params.toString()}` : basePath;
        const data = isSelfView
          ? await apiGetAuth<HitDataMap>(path)
          : await apiGet<HitDataMap>(path);
        if (!active) return;
        setHitDataMap(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setHitDataMap(null);
        } else {
          setHitDataMap(null);
        }
      }
    };

    loadHitDataMap();

    return () => {
      active = false;
    };
  }, [
    viewUserId,
    viewUsername,
    isSelfView,
    hitMode,
    hitStatMode,
    hitFilterHitterSide,
    hitFilterPitcherHand,
    hitAdvancedFilters.baseState,
    hitAdvancedFilters.outs,
    hitAdvancedFilters.abCount,
    hitAdvancedFilters.minSeen,
    hitAdvancedFilters.maxSeen,
    hitAdvancedFilters.pitcherCount,
    hitFocusZone,
    selectedPitcher?.mlb_id,
    selectedHitter?.mlb_id,
  ]);

  const currentSkills = skillMode === "Hitting" ? skills?.hitting : skills?.pitching;
  const skillRows = useMemo(
    () => [
      { label: "AVG", value: formatRate(currentSkills?.avg) },
      { label: "OBP", value: formatRate(currentSkills?.obp) },
      { label: "SLUG", value: formatRate(currentSkills?.slg) },
      { label: "OPS", value: formatRate(currentSkills?.ops) },
      { label: "K/BB", value: formatRatio(currentSkills?.kbb) },
    ],
    [currentSkills]
  );
  const resolvedUsername = showProfile?.username ?? viewUsername ?? null;
  const username =
    resolvedUsername ??
    (loading ? "Loading..." : "Not linked");
  const detailRows = useMemo(
    () => [
      { label: "Games Played", value: String(gameSummary?.games_played ?? 0) },
      { label: "Difficulty", value: gameSummary?.last_game_difficulty ?? "—" },
      {
        label: "Last Game Played",
        value: formatDate(gameSummary?.last_game_date),
      },
      { label: "Record", value: gameSummary?.record ?? "0-0" },
    ],
    [gameSummary]
  );
  const gameLogDifficultyOptions = useMemo(() => {
    const values = new Set<string>();
    gameLog.forEach((game) => {
      if (game.difficulty) values.add(game.difficulty);
    });
    return ["All", ...Array.from(values).sort((a, b) => a.localeCompare(b))];
  }, [gameLog]);
  const filteredGameLog = useMemo(() => {
    const query = gameLogBallpark.trim().toLowerCase();
    const filtered = gameLog.filter((game) => {
      if (gameLogDifficulty !== "all") {
        const difficulty = (game.difficulty ?? "").toLowerCase();
        if (difficulty !== gameLogDifficulty.toLowerCase()) return false;
      }
      if (gameLogResult !== "all") {
        const result = gameLogResult === "wins" ? "W" : "L";
        const perspective = getGameLogPerspective(game, resolvedUsername);
        if (perspective.userResult !== result) return false;
      }
      if (query) {
        const park = (game.ball_park_name ?? "").toLowerCase();
        if (!park.includes(query)) return false;
      }
      return true;
    });
    const sorted = filtered.sort((a, b) => {
      const aTime = new Date(a.date).getTime();
      const bTime = new Date(b.date).getTime();
      if (Number.isNaN(aTime) || Number.isNaN(bTime)) return 0;
      return bTime - aTime;
    });
    const seen = new Set<string>();
    const deduped: ShowGameLogItem[] = [];
    for (const game of sorted) {
      const key = getGameLogDedupeKey(game, resolvedUsername);
      if (seen.has(key)) continue;
      seen.add(key);
      deduped.push(game);
    }
    return deduped;
  }, [gameLog, gameLogBallpark, gameLogDifficulty, gameLogResult, resolvedUsername]);
  const handleOpenGame = (game: ShowGameLogItem) => {
    const perspective = getGameLogPerspective(game, resolvedUsername);
    router.push({
      pathname: "/game/[gameId]",
      params: {
        gameId: game.game_id,
        date: game.date ?? "",
        opponent: perspective.opponentName ?? "",
        location: perspective.locationLabel ?? "",
        result: perspective.userResult ?? "",
        scoreFor: String(perspective.scoreFor ?? ""),
        scoreAgainst: String(perspective.scoreAgainst ?? ""),
        difficulty: game.difficulty ?? "",
        ballpark: game.ball_park_name ?? "",
        homeTeam: game.home_full_name ?? "",
        awayTeam: game.away_full_name ?? "",
        viewUsername: viewUsername ?? "",
        viewUserId: viewUserId ? String(viewUserId) : "",
      },
    });
  };
  const strikeoutZones = strikeoutMap?.zones ?? [
    [0, 0, 0],
    [0, 0, 0],
    [0, 0, 0],
  ];
  const strikeoutOutside = strikeoutMap?.outside ?? {
    top_left: 0,
    top: 0,
    top_right: 0,
    right: 0,
    bottom_right: 0,
    bottom: 0,
    bottom_left: 0,
    left: 0,
  };
  const strikeoutStats = strikeoutMap?.stats;
  const pitchTypeOptions = strikeoutMap?.pitch_type_options ?? [];
  const strikeoutCountsByZone = strikeoutMap?.counts_by_zone;
  const strikeoutCountsByOutside = strikeoutMap?.counts_by_outside;
  const strikeoutPa = strikeoutMap?.pa ?? 0;
  const activeStrikeoutStats = useMemo(() => {
    if (!strikeoutSelections.length) return strikeoutStats;
    const aggregate: StrikeoutCounts = {
      k: 0,
      chase: 0,
      look: 0,
      eye: 0,
      early: 0,
      late: 0,
    };
    strikeoutSelections.forEach((selection) => {
      const counts =
        selection.kind === "zone"
          ? strikeoutCountsByZone?.[selection.row]?.[selection.col]
          : strikeoutCountsByOutside?.[selection.key];
      if (!counts) return;
      aggregate.k += counts.k;
      aggregate.chase += counts.chase;
      aggregate.look += counts.look;
      aggregate.eye += counts.eye;
      aggregate.early += counts.early;
      aggregate.late += counts.late;
    });
    return statsFromCounts(aggregate, strikeoutPa) ?? strikeoutStats;
  }, [
    strikeoutSelections,
    strikeoutStats,
    strikeoutCountsByZone,
    strikeoutCountsByOutside,
    strikeoutPa,
  ]);
  const summaryStats = [
    { label: "K%", value: formatPercent(activeStrikeoutStats?.k_pct) },
    { label: "Chase %", value: formatPercent(activeStrikeoutStats?.chase_pct) },
    { label: "Freeze %", value: formatPercent(activeStrikeoutStats?.freeze_pct) },
    { label: "Timing Bias", value: formatTimingBias(activeStrikeoutStats?.timing_pct) },
    { label: "Mistime K%", value: formatPercent(activeStrikeoutStats?.timing_k_pct) },
    { label: "Eye K%", value: formatPercent(activeStrikeoutStats?.eye_k_pct) },
    { label: "Location K%", value: formatPercent(activeStrikeoutStats?.location_k_pct) },
  ];
  const coachingMetrics = useMemo(() => {
    const hitStats = hitDataMap?.stats;
    const strikeStats = strikeoutStats;
    const flyRate = hitStats?.flyball_rate;
    const popupRate = hitStats?.popup_rate;
    const groundRate = hitStats?.groundball_rate;
    const perfectRate = hitStats?.perfect_perfect_pct;
    let launchTilt: number | null = null;
    if (
      isFiniteNumber(flyRate)
      && isFiniteNumber(popupRate)
      && isFiniteNumber(groundRate)
      && isFiniteNumber(perfectRate)
    ) {
      const denom = 100 - perfectRate;
      if (denom > 0) {
        launchTilt = clampNumber((flyRate + popupRate - groundRate) / denom, -1, 1);
      }
    }
    let launchTiltStatus: "low" | "ok" | "high" | "unknown" = "unknown";
    if (launchTilt !== null) {
      if (launchTilt < -0.2) {
        launchTiltStatus = "low";
      } else if (launchTilt > 0.2) {
        launchTiltStatus = "high";
      } else {
        launchTiltStatus = "ok";
      }
    }
    const heartMissK = strikeStats?.heart_miss_k_pct;
    const inzoneSwingK = strikeStats?.inzone_swing_k_pct;
    const extremeContact = hitStats?.extreme_contact_nopp_pct;
    let slamScore: number | null = null;
    if (
      isFiniteNumber(heartMissK)
      && isFiniteNumber(inzoneSwingK)
      && isFiniteNumber(extremeContact)
    ) {
      slamScore = clampNumber(
        0.45 * heartMissK + 0.3 * inzoneSwingK + 0.25 * extremeContact,
        0,
        100
      );
    }
    const launchTiltAdvice =
      launchTiltStatus === "low"
        ? "Start higher in your swing (PCI anchoring)."
        : launchTiltStatus === "high"
          ? "Start lower in your swing (PCI anchoring)."
          : launchTiltStatus === "ok"
            ? "All good."
            : "Not enough data yet.";
    return {
      launchTilt,
      launchTiltStatus,
      launchTiltAdvice,
      heartMissK: isFiniteNumber(heartMissK) ? heartMissK : null,
      inzoneSwingK: isFiniteNumber(inzoneSwingK) ? inzoneSwingK : null,
      extremeContact: isFiniteNumber(extremeContact) ? extremeContact : null,
      slamScore,
    };
  }, [hitDataMap, strikeoutStats]);
  const selectedHitterLabel = selectedHitter ? formatHitterName(selectedHitter) : null;
  const selectedPitcherLabel = selectedPitcher
    ? formatPitcherName(selectedPitcher)
    : null;
  const hitterSideLabel =
    selectedHitterLabel ??
    HITTER_SIDE_OPTIONS.find((option) => option.value === filterHitterSide.side)?.label ??
    "All hitters";
  const pitcherHandLabel =
    selectedPitcherLabel ??
    PITCHER_HAND_OPTIONS.find((option) => option.value === filterPitcherHand.hand)?.label ??
    "All pitchers";
  const pitchTypeLabel =
    filterPitchTypes.length === 0
      ? "All pitches"
      : filterPitchTypes.length === 1
        ? formatPitchTypeLabel(filterPitchTypes[0])
        : `${filterPitchTypes.length} pitches`;
  const hitStatLabel =
    HIT_STAT_OPTIONS.find((option) => option.value === hitStatMode)?.label ?? "Count";
  const hitHitterLabel =
    selectedHitterLabel ??
    HITTER_SIDE_OPTIONS.find((option) => option.value === hitFilterHitterSide)?.label ??
    "All hitters";
  const hitPitcherLabel =
    selectedPitcherLabel ??
    PITCHER_HAND_OPTIONS.find((option) => option.value === hitFilterPitcherHand)?.label ??
    "All pitchers";
  const pitchTypeMenuOptions = useMemo(
    () => ["all", ...pitchTypeOptions],
    [pitchTypeOptions]
  );
  const statsRows = useMemo(() => {
    if (!aggregateStats) {
      return {
        boxscorePrimary: [] as StatsTableRow[],
        boxscoreSecondary: [] as StatsTableRow[],
        advanced: [] as StatsTableRow[],
      };
    }
    const boxscore = [
      { label: "PA", value: formatCount(aggregateStats.pa) },
      { label: "AB", value: formatCount(aggregateStats.ab) },
      { label: "R", value: formatCount(aggregateStats.r) },
      { label: "H", value: formatCount(aggregateStats.h) },
      { label: "RBI", value: formatCount(aggregateStats.rbi) },
      { label: "1B", value: formatCount(aggregateStats.singles) },
      { label: "2B", value: formatCount(aggregateStats.doubles) },
      { label: "3B", value: formatCount(aggregateStats.triples) },
      { label: "HR", value: formatCount(aggregateStats.hr) },
      { label: "BB", value: formatCount(aggregateStats.bb) },
      { label: "SO", value: formatCount(aggregateStats.so) },
      { label: "AVG", value: formatRate(aggregateStats.avg) },
      { label: "OBP", value: formatRate(aggregateStats.obp) },
      { label: "SLG", value: formatRate(aggregateStats.slg) },
      { label: "OPS", value: formatRate(aggregateStats.ops) },
      { label: "LOB", value: formatCount(aggregateStats.lob) },
      { label: "GIDP%", value: formatPercent(aggregateStats.gidp_pct ?? null) },
    ];
    const advanced = [
      { label: "wOBA", value: formatRate(aggregateStats.woba) },
      { label: "ISO", value: formatRate(aggregateStats.iso) },
      { label: "BABIP", value: formatRate(aggregateStats.babip) },
      { label: "K%", value: formatPercent(aggregateStats.k_pct) },
      { label: "BB%", value: formatPercent(aggregateStats.bb_pct) },
      { label: "HR%", value: formatPercent(aggregateStats.hr_pct) },
      { label: "XBH%", value: formatPercent(aggregateStats.xbh_pct) },
      { label: "RS%", value: formatPercent(aggregateStats.rs_pct) },
    ];
    return {
      boxscorePrimary: boxscore.slice(0, 9),
      boxscoreSecondary: boxscore.slice(9),
      advanced,
    };
  }, [aggregateStats]);
  const cardFrozenDivider = 8;
  const filteredCardStats = useMemo(() => {
    const needle = cardHittingFilter.trim().toLowerCase();
    const minPa = cardHittingMinPa.trim()
      ? Number.parseInt(cardHittingMinPa, 10)
      : null;
    return cardStats.filter((row) => {
      if (minPa !== null && !Number.isNaN(minPa) && row.pa < minPa) return false;
      if (!needle) return true;
      const name = formatCardName(row).toLowerCase();
      return name.includes(needle) || String(row.mlb_id).includes(needle);
    });
  }, [cardStats, cardHittingFilter, cardHittingMinPa]);
  const filteredCardPitchingStats = useMemo(() => {
    const needle = cardPitchingFilter.trim().toLowerCase();
    const minBf = cardPitchingMinBf.trim()
      ? Number.parseInt(cardPitchingMinBf, 10)
      : null;
    return cardPitchingStats.filter((row) => {
      if (minBf !== null && !Number.isNaN(minBf) && row.pa < minBf) return false;
      if (!needle) return true;
      const name = formatCardName(row).toLowerCase();
      return name.includes(needle) || String(row.mlb_id).includes(needle);
    });
  }, [cardPitchingStats, cardPitchingFilter, cardPitchingMinBf]);
  const sortedCardStats = useMemo(
    () =>
      sortCardRows(
        filteredCardStats,
        CARD_COLUMNS,
        cardSortKey,
        cardSortDirection,
        (row) => formatCardName(row)
      ),
    [filteredCardStats, cardSortKey, cardSortDirection]
  );
  const sortedCardPitchingStats = useMemo(
    () =>
      sortCardRows(
        filteredCardPitchingStats,
        PITCHING_CARD_COLUMNS,
        cardPitchingSortKey,
        cardPitchingSortDirection,
        (row) => formatCardName(row)
      ),
    [filteredCardPitchingStats, cardPitchingSortKey, cardPitchingSortDirection]
  );
  const horizontalPadding = 24;
  const gap = 8;
  const available = Math.max(0, width - horizontalPadding - gap);
  const cardWidth = Math.floor(available / 2);
  const clampSpeedInput = (value: string) => {
    const digits = value.replace(/[^0-9]/g, "");
    if (!digits) return "";
    const next = Math.max(0, Math.min(99, Number.parseInt(digits, 10)));
    return Number.isNaN(next) ? "" : String(next);
  };
  const clampSeenInput = (value: string) => {
    const digits = value.replace(/[^0-9]/g, "");
    if (!digits) return "";
    const next = Math.max(0, Math.min(999, Number.parseInt(digits, 10)));
    return Number.isNaN(next) ? "" : String(next);
  };
  const togglePitchType = (pitch: string) => {
    if (pitch === "all") {
      setFilterPitchTypes([]);
      return;
    }
    setFilterPitchTypes((prev) => {
      if (prev.includes(pitch)) {
        return prev.filter((item) => item !== pitch);
      }
      return [...prev, pitch];
    });
  };
  const handleCardSort = (key: CardSortKey) => {
    setCardSortKey((prev) => {
      if (prev === key) {
        setCardSortDirection((dir) => (dir === "asc" ? "desc" : "asc"));
        return prev;
      }
      setCardSortDirection("desc");
      return key;
    });
  };
  const handleCardPitchingSort = (key: PitchingCardSortKey) => {
    setCardPitchingSortKey((prev) => {
      if (prev === key) {
        setCardPitchingSortDirection((dir) => (dir === "asc" ? "desc" : "asc"));
        return prev;
      }
      setCardPitchingSortDirection("desc");
      return key;
    });
  };
  const handleSelectHitStat = (value: HitDataStat) => {
    setHitStatMode(value);
  };
  const handleSelectHitHitterSide = (value: HitterSide) => {
    setHitFilterHitterSide(value);
    setSelectedHitter(null);
  };
  const handleSelectHitPitcherHand = (value: PitcherHand) => {
    setHitFilterPitcherHand(value);
    setSelectedPitcher(null);
  };
  const toggleStrikeoutSelection = (next: StrikeoutSelection) => {
    if (!next) {
      setStrikeoutSelections([]);
      return;
    }
    setStrikeoutSelections((prev) => {
      const key = strikeoutSelectionKey(next);
      const exists = prev.some((sel) => strikeoutSelectionKey(sel) === key);
      if (exists) {
        return prev.filter((sel) => strikeoutSelectionKey(sel) !== key);
      }
      return [...prev, next];
    });
  };
  const handleResetFilters = () => {
    setFilterHitterSide({ side: "all" });
    setFilterPitcherHand({ hand: "all" });
    setFilterPitchTypes([]);
    setStrikeoutSelections([]);
    setActiveFilterMenu(null);
    setAdvancedFiltersOpen(false);
    setAdvancedFilters(defaultAdvancedFilters);
    setSelectedPitcher(null);
    setSelectedHitter(null);
  };
  const handleResetHitFilters = () => {
    setHitStatMode("count");
    setHitFilterHitterSide("all");
    setHitFilterPitcherHand("all");
    setHitFocusZone(null);
    setHitAdvancedFilters(defaultHitAdvancedFilters);
    setHitAdvancedFiltersOpen(false);
    setSelectedPitcher(null);
    setSelectedHitter(null);
  };
  const hasAdvancedFilters =
    filterPitchTypes.length > 0 ||
    advancedFilters.minSpeed !== "" ||
    advancedFilters.maxSpeed !== "" ||
    advancedFilters.timing !== "all" ||
    advancedFilters.outType !== "all";
  const hasHitAdvancedFilters =
    hitAdvancedFilters.baseState !== "all" ||
    hitAdvancedFilters.outs !== "all" ||
    hitAdvancedFilters.abCount !== "all" ||
    hitAdvancedFilters.pitcherCount !== "all" ||
    hitAdvancedFilters.minSeen !== "" ||
    hitAdvancedFilters.maxSeen !== "";
  const handleOpenStatHelp = (label: string) => {
    const help = STRIKEOUT_STAT_HELP[label] ?? HIT_DATA_STAT_HELP[label];
    if (!help) return;
    setStatHelp(help);
    setStatHelpOpen(true);
  };

  const handleSelectPitcher = (
    pitcher: ShowPitcherSearchResult | null,
    opts?: { closeMenu?: boolean }
  ) => {
    const closeMenu = opts?.closeMenu ?? true;
    setSelectedPitcher(pitcher);
    setPitcherSearchQuery("");
    setPitcherSearchResults([]);
    setPitcherSearchLoading(false);
    setPitcherSearchError(null);
    setHitPitcherSearchQuery("");
    setHitPitcherSearchResults([]);
    setHitPitcherSearchLoading(false);
    setHitPitcherSearchError(null);
    if (pitcher) {
      setFilterPitcherHand({ hand: "all" });
      setHitFilterPitcherHand("all");
    }
    if (closeMenu) setActiveFilterMenu(null);
  };

  const handleSelectHitter = (
    hitter: ShowHitterSearchResult | null,
    opts?: { closeMenu?: boolean }
  ) => {
    const closeMenu = opts?.closeMenu ?? true;
    setSelectedHitter(hitter);
    setHitterSearchQuery("");
    setHitterSearchResults([]);
    setHitterSearchLoading(false);
    setHitterSearchError(null);
    setHitHitterSearchQuery("");
    setHitHitterSearchResults([]);
    setHitHitterSearchLoading(false);
    setHitHitterSearchError(null);
    if (hitter) {
      setFilterHitterSide({ side: "all" });
      setHitFilterHitterSide("all");
    }
    if (closeMenu) setActiveFilterMenu(null);
  };

  const handleSelectUser = (user: ShowUserSearchResult | null) => {
    if (!user) {
      setViewUserId(null);
      setViewUsername(null);
      setViewProfileImage(null);
      setSearchOpen(false);
      return;
    }
    const key = getSearchKey(user);
    const cached = key ? searchImages[key] ?? null : null;
    const raw = user.profile_img_url ?? null;
    const fallback = raw && raw.startsWith("http") ? raw : null;
    setViewUserId(user.user_id ?? null);
    setViewUsername(user.username);
    setViewProfileImage(cached ?? fallback);
    setShowProfile({ username: user.username });
    setSearchOpen(false);
  };

  useEffect(() => {
    if (isSelfView) {
      setViewProfileImage(null);
      return;
    }
    const key =
      viewUserId != null
        ? `id:${viewUserId}`
        : viewUsername
          ? `name:${viewUsername.toLowerCase()}`
          : null;
    if (key && searchImages[key]) {
      setViewProfileImage(searchImages[key]);
    }
  }, [viewUserId, viewUsername, searchImages, isSelfView]);

  return (
    <SafeAreaView style={styles.safe} edges={["left", "right"]}>
      <ScrollView
        style={styles.container}
        contentContainerStyle={styles.containerContent}
        showsVerticalScrollIndicator={false}
      >
        <View style={styles.header}>
          <Text style={styles.headerTitle}>Gameplay Engine</Text>
          <Text style={styles.headerCaption}>
            Data may be up to 12 hours behind
          </Text>
        </View>

        <TouchableOpacity style={styles.profileTab} onPress={() => setSearchOpen(true)}>
          <View style={styles.profileImageWrap}>
            <Avatar
              firebasePath={isSelfView
                ? (auth.currentUser?.uid ? `users/${auth.currentUser.uid}/profile.jpg` : null)
                : (viewProfileImage || null)}
              size={32}
            />
          </View>
          <View style={styles.profileText}>
            <Text style={styles.profileLabel}>MLB The Show</Text>
            <Text style={styles.profileName} numberOfLines={1}>
              {username}
            </Text>
          </View>
        </TouchableOpacity>

        <View style={styles.cardsRow}>
          <View style={[styles.card, { width: cardWidth }]}>
            <Text style={styles.cardTitle}>Details</Text>
            <View style={styles.cardBody}>
              {detailRows.map((row) => (
                <View key={row.label} style={styles.detailRow}>
                  <Text style={styles.detailLabel}>{row.label}</Text>
                  <Text style={styles.detailValue}>{row.value}</Text>
                </View>
              ))}
            </View>
            <View style={styles.cardDivider} />
            <Text style={[styles.cardTitle, styles.statsHeader]}>Stats</Text>
            <View style={styles.toggleRow}>
              <View style={styles.toggle}>
                {(["Hitting", "Pitching"] as const).map((mode) => (
                  <TouchableOpacity
                    key={mode}
                    style={[
                      styles.toggleButton,
                      skillMode === mode && styles.toggleButtonActive,
                    ]}
                    onPress={() => setSkillMode(mode)}
                  >
                    <Text
                      style={[
                        styles.toggleText,
                        skillMode === mode && styles.toggleTextActive,
                      ]}
                    >
                      {mode}
                    </Text>
                  </TouchableOpacity>
                ))}
              </View>
            </View>
            <View style={styles.cardBody}>
              {skillRows.map((row) => (
                <View key={row.label} style={styles.skillRow}>
                  <Text style={styles.detailLabel}>{row.label}</Text>
                  <Text style={styles.detailValue}>{row.value}</Text>
                </View>
              ))}
            </View>
          </View>

          <View style={[styles.card, { width: cardWidth }]}>
            <Text style={styles.cardTitle}>Skills</Text>
            <View style={styles.cardBody}>
              {renderPercentRow("Batting", battingArchetype?.overall ?? null, {
                emphasis: true,
              })}
              {renderPercentRow("Timing", battingArchetype?.timing ?? null)}
              {renderPercentRow("Location", battingArchetype?.location ?? null)}
              {renderPercentRow("Power", battingArchetype?.power ?? null)}
              <View style={styles.skillDivider} />
              {renderPercentRow("Pitching", pitchingArchetype?.overall ?? null, {
                emphasis: true,
              })}
              {renderPercentRow("Consistency", pitchingArchetype?.consistency ?? null)}
              {renderPercentRow("Strikeout", pitchingArchetype?.strikeout ?? null)}
              {renderPercentRow("Location", pitchingArchetype?.location ?? null)}
            </View>
          </View>
        </View>

        <View style={styles.sectionTabs}>
          {(["Analytics", "Game Log", "Cards", "Coaching"] as const).map((tab) => (
            <TouchableOpacity
              key={tab}
              style={[styles.sectionTab, sectionTab === tab && styles.sectionTabActive]}
              onPress={() => setSectionTab(tab)}
            >
              <Text
                style={[
                  styles.sectionTabText,
                  sectionTab === tab && styles.sectionTabTextActive,
                ]}
              >
                {tab}
              </Text>
            </TouchableOpacity>
          ))}
        </View>

        {sectionTab === "Analytics" ? (
          <>
            <StrikeoutsSection
              strikeoutMode={strikeoutMode}
              setStrikeoutMode={setStrikeoutMode}
              hitterSideLabel={hitterSideLabel}
              pitcherHandLabel={pitcherHandLabel}
              selectedPitcher={selectedPitcher}
              onSelectPitcher={handleSelectPitcher}
              pitcherSearchQuery={pitcherSearchQuery}
              setPitcherSearchQuery={setPitcherSearchQuery}
              pitcherSearchResults={pitcherSearchResults}
              pitcherSearchLoading={pitcherSearchLoading}
              pitcherSearchError={pitcherSearchError}
              selectedHitter={selectedHitter}
              onSelectHitter={handleSelectHitter}
              hitterSearchQuery={hitterSearchQuery}
              setHitterSearchQuery={setHitterSearchQuery}
              hitterSearchResults={hitterSearchResults}
              hitterSearchLoading={hitterSearchLoading}
              hitterSearchError={hitterSearchError}
              pitchTypeLabel={pitchTypeLabel}
              pitchTypeMenuOptions={pitchTypeMenuOptions}
              filterHitterSide={filterHitterSide}
              setFilterHitterSide={setFilterHitterSide}
              filterPitcherHand={filterPitcherHand}
              setFilterPitcherHand={setFilterPitcherHand}
              filterPitchTypes={filterPitchTypes}
              togglePitchType={togglePitchType}
              activeFilterMenu={activeFilterMenu}
              setActiveFilterMenu={setActiveFilterMenu}
              handleResetFilters={handleResetFilters}
              advancedFiltersOpen={advancedFiltersOpen}
              setAdvancedFiltersOpen={setAdvancedFiltersOpen}
              advancedFilters={advancedFilters}
              setAdvancedFilters={setAdvancedFilters}
              clampSpeedInput={clampSpeedInput}
              hasAdvancedFilters={hasAdvancedFilters}
              strikeoutZones={strikeoutZones}
              strikeoutOutside={strikeoutOutside}
              strikeoutSelections={strikeoutSelections}
              onSelectionChange={toggleStrikeoutSelection}
              summaryStats={summaryStats}
              onStatPress={handleOpenStatHelp}
              statHelpOpen={statHelpOpen}
              setStatHelpOpen={setStatHelpOpen}
              statHelp={statHelp}
            />

            <HitDataSection
              data={hitDataMap}
              hitMode={hitMode}
              statKey={hitStatMode}
              statOptions={HIT_STAT_OPTIONS}
              hitterOptions={HITTER_SIDE_OPTIONS}
              pitcherOptions={PITCHER_HAND_OPTIONS}
              hitterValue={hitFilterHitterSide}
              pitcherValue={hitFilterPitcherHand}
              statLabel={hitStatLabel}
              hitterLabel={hitHitterLabel}
              pitcherLabel={hitPitcherLabel}
              selectedHitter={selectedHitter}
              selectedPitcher={selectedPitcher}
              onSelectMode={setHitMode}
              onSelectStat={handleSelectHitStat}
              onSelectHitter={handleSelectHitHitterSide}
              onSelectPitcher={handleSelectHitPitcherHand}
              onSelectHitterPlayer={handleSelectHitter}
              onSelectPitcherPlayer={handleSelectPitcher}
              hitterSearchQuery={hitHitterSearchQuery}
              setHitterSearchQuery={setHitHitterSearchQuery}
              hitterSearchResults={hitHitterSearchResults}
              hitterSearchLoading={hitHitterSearchLoading}
              hitterSearchError={hitHitterSearchError}
              pitcherSearchQuery={hitPitcherSearchQuery}
              setPitcherSearchQuery={setHitPitcherSearchQuery}
              pitcherSearchResults={hitPitcherSearchResults}
              pitcherSearchLoading={hitPitcherSearchLoading}
              pitcherSearchError={hitPitcherSearchError}
              onStatPress={handleOpenStatHelp}
              onResetFilters={handleResetHitFilters}
              onOpenAdvancedFilters={() => setHitAdvancedFiltersOpen(true)}
              hasAdvancedFilters={hasHitAdvancedFilters}
              selectedZone={hitFocusZone}
              onSelectZone={(zone) =>
                setHitFocusZone((prev) => (prev === zone ? null : zone))
              }
            />

            <StatsTableSection
              statsMode={statsMode}
              setStatsMode={setStatsMode}
              boxscorePrimary={statsRows.boxscorePrimary}
              boxscoreSecondary={statsRows.boxscoreSecondary}
              advanced={statsRows.advanced}
              loading={aggregateStatsLoading}
              error={aggregateStatsError}
            />
          </>
        ) : null}

        {sectionTab === "Game Log" ? (
          <GameLogSection
            games={filteredGameLog}
            totalGames={gameLog.length}
            loading={gameLogLoading}
            error={gameLogError}
            difficulty={gameLogDifficulty}
            setDifficulty={setGameLogDifficulty}
            difficultyOptions={gameLogDifficultyOptions}
            resultFilter={gameLogResult}
            setResultFilter={setGameLogResult}
            ballparkQuery={gameLogBallpark}
            setBallparkQuery={setGameLogBallpark}
            username={resolvedUsername}
            onSelectGame={handleOpenGame}
          />
        ) : null}

        {sectionTab === "Cards" ? (
          <>
            <CardsTableSection
              title="Hitting Cards"
              subtitle="Boxscore, contact, and strikeout rates by MLB ID."
              rows={sortedCardStats}
              totalRows={cardStats.length}
              loading={cardStatsLoading}
              error={cardStatsError}
              filter={cardHittingFilter}
              setFilter={setCardHittingFilter}
              minLabel="Min PA"
              minValue={cardHittingMinPa}
              setMinValue={setCardHittingMinPa}
              filterPlaceholder="Filter by batter or ID..."
              sortKey={cardSortKey}
              sortDirection={cardSortDirection}
              onSortChange={handleCardSort}
              columns={CARD_COLUMNS}
              frozenKeys={CARD_FROZEN_KEYS}
              frozenDivider={cardFrozenDivider}
            />

            <CardsTableSection
              title="Pitching Cards"
              subtitle="Pitching outcomes with contact quality and strikeout rates."
              rows={sortedCardPitchingStats}
              totalRows={cardPitchingStats.length}
              loading={cardPitchingStatsLoading}
              error={cardPitchingStatsError}
              filter={cardPitchingFilter}
              setFilter={setCardPitchingFilter}
              minLabel="Min BF"
              minValue={cardPitchingMinBf}
              setMinValue={setCardPitchingMinBf}
              filterPlaceholder="Filter by pitcher or ID..."
              sortKey={cardPitchingSortKey}
              sortDirection={cardPitchingSortDirection}
              onSortChange={handleCardPitchingSort}
              columns={PITCHING_CARD_COLUMNS}
              frozenKeys={CARD_FROZEN_KEYS}
              frozenDivider={cardFrozenDivider}
            />
          </>
        ) : null}

        {sectionTab === "Coaching" ? (
          <CoachingSection
            launchTilt={coachingMetrics.launchTilt}
            launchTiltAdvice={coachingMetrics.launchTiltAdvice}
            heartMissK={coachingMetrics.heartMissK}
            inzoneSwingK={coachingMetrics.inzoneSwingK}
            extremeContact={coachingMetrics.extremeContact}
            slamScore={coachingMetrics.slamScore}
            pitchTypeRanks={pitchTypeRanks}
            pitchTypeRanksLoading={pitchTypeRanksLoading}
            pitchTypeRanksError={pitchTypeRanksError}
          />
        ) : null}

        <Modal
          transparent
          visible={hitAdvancedFiltersOpen}
          animationType="fade"
          onRequestClose={() => setHitAdvancedFiltersOpen(false)}
        >
          <View style={styles.filterOverlay}>
            <Pressable
              style={StyleSheet.absoluteFill}
              onPress={() => setHitAdvancedFiltersOpen(false)}
            />
            <View style={styles.filterPanel}>
              <View style={styles.filterHeaderRow}>
                <Text style={styles.filterTitle}>Hit Filters</Text>
                <TouchableOpacity
                  style={styles.filterCloseButton}
                  onPress={() => setHitAdvancedFiltersOpen(false)}
                >
                  <Ionicons name="close" size={16} color={theme.colors.text} />
                </TouchableOpacity>
              </View>

              <View style={styles.filterField}>
                <Text style={styles.filterLabel}>Base State</Text>
                <View style={styles.filterToggle}>
                  {HIT_BASE_OPTIONS.map((option) => {
                    const active = option.value === hitAdvancedFilters.baseState;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                        onPress={() =>
                          setHitAdvancedFilters((prev) => ({ ...prev, baseState: option.value }))
                        }
                      >
                        <Text
                          style={[
                            styles.filterToggleText,
                            active && styles.filterToggleTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              </View>

              <View style={styles.filterField}>
                <Text style={styles.filterLabel}>Outs</Text>
                <View style={styles.filterToggle}>
                  {HIT_OUT_OPTIONS.map((option) => {
                    const active = option.value === hitAdvancedFilters.outs;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                        onPress={() =>
                          setHitAdvancedFilters((prev) => ({ ...prev, outs: option.value }))
                        }
                      >
                        <Text
                          style={[
                            styles.filterToggleText,
                            active && styles.filterToggleTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              </View>

              <View style={styles.filterField}>
                <Text style={styles.filterLabel}>AB with Hitter</Text>
                <View style={styles.filterToggle}>
                  {HIT_AB_OPTIONS.map((option) => {
                    const active = option.value === hitAdvancedFilters.abCount;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                        onPress={() =>
                          setHitAdvancedFilters((prev) => ({ ...prev, abCount: option.value }))
                        }
                      >
                        <Text
                          style={[
                            styles.filterToggleText,
                            active && styles.filterToggleTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              </View>

              <View style={styles.filterField}>
                <Text style={styles.filterLabel}>Times Seen Pitcher</Text>
                <View style={styles.filterInputRow}>
                  <View style={styles.filterInputWrap}>
                    <Text style={styles.filterInputLabel}>Min</Text>
                    <TextInput
                      value={hitAdvancedFilters.minSeen}
                      onChangeText={(value) =>
                        setHitAdvancedFilters((prev) => ({
                          ...prev,
                          minSeen: clampSeenInput(value),
                        }))
                      }
                      placeholder="0"
                      placeholderTextColor={theme.colors.muted}
                      keyboardType="number-pad"
                      maxLength={3}
                      style={styles.filterInput}
                    />
                  </View>
                  <View style={styles.filterInputWrap}>
                    <Text style={styles.filterInputLabel}>Max</Text>
                    <TextInput
                      value={hitAdvancedFilters.maxSeen}
                      onChangeText={(value) =>
                        setHitAdvancedFilters((prev) => ({
                          ...prev,
                          maxSeen: clampSeenInput(value),
                        }))
                      }
                      placeholder="99"
                      placeholderTextColor={theme.colors.muted}
                      keyboardType="number-pad"
                      maxLength={3}
                      style={styles.filterInput}
                    />
                  </View>
                </View>
              </View>

              <View style={styles.filterField}>
                <Text style={styles.filterLabel}>Pitcher Order</Text>
                <View style={styles.filterToggle}>
                  {HIT_PITCHER_COUNT_OPTIONS.map((option) => {
                    const active = option.value === hitAdvancedFilters.pitcherCount;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                        onPress={() =>
                          setHitAdvancedFilters((prev) => ({
                            ...prev,
                            pitcherCount: option.value,
                          }))
                        }
                      >
                        <Text
                          style={[
                            styles.filterToggleText,
                            active && styles.filterToggleTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              </View>
            </View>
          </View>
        </Modal>

        <Modal
          transparent
          visible={searchOpen}
          animationType="fade"
          onRequestClose={() => setSearchOpen(false)}
        >
          <View style={styles.searchOverlay}>
            <Pressable style={StyleSheet.absoluteFill} onPress={() => setSearchOpen(false)} />
            <View style={styles.searchPanel}>
              <Text style={styles.searchTitle}>Find MLB The Show User</Text>
              <TextInput
                value={searchQuery}
                onChangeText={setSearchQuery}
                placeholder="Search show username..."
                placeholderTextColor={theme.colors.muted}
                autoCapitalize="none"
                autoCorrect={false}
                style={styles.searchInput}
              />
              <ScrollView
                style={styles.searchResults}
                contentContainerStyle={styles.searchResultsContent}
                showsVerticalScrollIndicator={false}
              >
                <TouchableOpacity
                  style={styles.searchRow}
                  onPress={() => handleSelectUser(null)}
                >
                  <View style={styles.searchAvatarWrap}>
                    <Avatar
                      firebasePath={auth.currentUser?.uid ? `users/${auth.currentUser.uid}/profile.jpg` : null}
                      size={30}
                      borderColor="rgba(255, 255, 255, 0.12)"
                      borderWidth={1}
                    />
                  </View>
                  <View style={styles.searchText}>
                    <Text style={styles.searchName}>My Stats</Text>
                    <Text style={styles.searchMeta}>View your profile</Text>
                  </View>
                </TouchableOpacity>

                {searchLoading ? (
                  <Text style={styles.searchStatus}>Searching...</Text>
                ) : searchError ? (
                  <Text style={styles.searchStatus}>{searchError}</Text>
                ) : searchResults.length === 0 && searchQuery.trim() ? (
                  <Text style={styles.searchStatus}>No matches.</Text>
                ) : (
                  searchResults.map((user) => {
                    const key = getSearchKey(user) ?? user.username;
                    const imageKey = getSearchKey(user);
                    const image =
                      (imageKey && searchImages[imageKey]) ||
                      (user.profile_img_url && user.profile_img_url.startsWith("http")
                        ? user.profile_img_url
                        : null);
                    return (
                    <TouchableOpacity
                      key={key}
                      style={styles.searchRow}
                      onPress={() => handleSelectUser(user)}
                    >
                      <View style={styles.searchAvatarWrap}>
                        <Avatar
                          firebasePath={user.profile_img_url}
                          size={30}
                          borderColor="rgba(255, 255, 255, 0.12)"
                          borderWidth={1}
                        />
                      </View>
                      <View style={styles.searchText}>
                        <Text style={styles.searchName}>{user.username}</Text>
                        <Text style={styles.searchMeta}>
                          {user.display_name ?? "User"}
                        </Text>
                      </View>
                    </TouchableOpacity>
                  );
                  })
                )}
              </ScrollView>
            </View>
          </View>
        </Modal>

      </ScrollView>
    </SafeAreaView>
  );
}

type SummaryStatItem = { label: string; value: string };

type SectionPlaceholderProps = {
  title: string;
  description: string;
};

type GameLogSectionProps = {
  games: ShowGameLogItem[];
  totalGames: number;
  loading: boolean;
  error: string | null;
  difficulty: string;
  setDifficulty: Dispatch<SetStateAction<string>>;
  difficultyOptions: string[];
  resultFilter: GameLogResultFilter;
  setResultFilter: Dispatch<SetStateAction<GameLogResultFilter>>;
  ballparkQuery: string;
  setBallparkQuery: Dispatch<SetStateAction<string>>;
  username: string | null;
  onSelectGame: (game: ShowGameLogItem) => void;
};

type CoachingSectionProps = {
  launchTilt: number | null;
  launchTiltAdvice: string;
  heartMissK: number | null;
  inzoneSwingK: number | null;
  extremeContact: number | null;
  slamScore: number | null;
  pitchTypeRanks: PitchTypeRank[];
  pitchTypeRanksLoading: boolean;
  pitchTypeRanksError: string | null;
};

const CoachingSection = ({
  launchTilt,
  launchTiltAdvice,
  heartMissK,
  inzoneSwingK,
  extremeContact,
  slamScore,
  pitchTypeRanks,
  pitchTypeRanksLoading,
  pitchTypeRanksError,
}: CoachingSectionProps) => {
  const [launchExpanded, setLaunchExpanded] = useState(false);
  const [slamExpanded, setSlamExpanded] = useState(false);
  const [pitchesExpanded, setPitchesExpanded] = useState(false);
  const tiltScore = launchTilt !== null ? ((launchTilt + 1) / 2) * 100 : null;
  const tiltColor = tiltScore !== null ? gradientColor(tiltScore) : "rgba(226, 232, 240, 0.6)";
  const tiltPosition = tiltScore !== null ? clampNumber(tiltScore, 0, 100) : 50;
  const graphHeight = 96;
  const graphHalf = graphHeight / 2;
  const tiltMagnitude = launchTilt !== null ? Math.min(1, Math.abs(launchTilt)) : 0;
  const tiltBarHeight = Math.max(6, Math.round(graphHalf * tiltMagnitude));
  const tiltBarStyle =
    launchTilt !== null
      ? {
          height: tiltBarHeight,
          backgroundColor: tiltColor,
          top: launchTilt < 0 ? graphHalf : undefined,
          bottom: launchTilt >= 0 ? graphHalf : undefined,
        }
      : null;

  const slamColor = slamScore !== null ? gradientColor(slamScore) : "rgba(226, 232, 240, 0.6)";
  const slamWidth = slamScore !== null ? clampNumber(slamScore, 0, 100) : 0;
  const launchStatus =
    launchTilt === null
      ? "unknown"
      : Math.abs(launchTilt) <= 0.2
        ? "good"
        : Math.abs(launchTilt) <= 0.35
          ? "average"
          : "needs-work";
  const slamStatus =
    slamScore === null
      ? "unknown"
      : slamScore < 40
        ? "good"
        : slamScore < 60
          ? "average"
          : "needs-work";
  const pitchRanks = useMemo(() => {
    const rows = pitchTypeRanks.filter((row) => row.kPct !== null);
    rows.sort((a, b) => {
      if (a.kPct === null && b.kPct === null) return 0;
      if (a.kPct === null) return 1;
      if (b.kPct === null) return -1;
      return a.kPct - b.kPct;
    });
    return rows;
  }, [pitchTypeRanks]);
  const bestPitch = pitchRanks[0] ?? null;
  const worstPitch = pitchRanks.length ? pitchRanks[pitchRanks.length - 1] : null;
  const pitchStatus =
    worstPitch?.kPct === null || worstPitch?.kPct === undefined
      ? "unknown"
      : worstPitch.kPct <= 20
        ? "good"
        : worstPitch.kPct <= 30
          ? "average"
          : "needs-work";
  const pitchStatusColor =
    pitchStatus === "good"
      ? "#22c55e"
      : pitchStatus === "average"
        ? "#facc15"
        : pitchStatus === "needs-work"
          ? "#f87171"
          : "rgba(226, 232, 240, 0.6)";
  const launchStatusStyle =
    launchStatus === "good"
      ? styles.coachingAdviceOk
      : launchStatus === "average"
        ? styles.coachingAdviceHigh
        : launchStatus === "needs-work"
          ? styles.coachingAdviceLow
          : styles.coachingAdviceUnknown;
  const slamStatusStyle =
    slamStatus === "good"
      ? styles.coachingAdviceOk
      : slamStatus === "average"
        ? styles.coachingAdviceHigh
        : slamStatus === "needs-work"
          ? styles.coachingAdviceLow
          : styles.coachingAdviceUnknown;
  const pitchStatusStyle =
    pitchStatus === "good"
      ? styles.coachingAdviceOk
      : pitchStatus === "average"
        ? styles.coachingAdviceHigh
        : pitchStatus === "needs-work"
          ? styles.coachingAdviceLow
          : styles.coachingAdviceUnknown;
  const formatStatusLabel = (status: string) =>
    status === "good"
      ? "Good"
      : status === "average"
        ? "Average"
        : status === "needs-work"
          ? "Needs work"
          : "Awaiting data";
  const bestPitchText = bestPitch
    ? `${formatPitchTypeLabel(bestPitch.pitchType)} · ${formatPercent(bestPitch.kPct)}`
    : pitchTypeRanksLoading
      ? "Loading..."
      : pitchTypeRanksError
        ? "Unavailable"
        : "Not enough data yet.";
  const worstPitchText = worstPitch
    ? `${formatPitchTypeLabel(worstPitch.pitchType)} · ${formatPercent(worstPitch.kPct)}`
    : pitchTypeRanksLoading
      ? "Loading..."
      : pitchTypeRanksError
        ? "Unavailable"
        : "Not enough data yet.";

  return (
    <View style={styles.coachingStack}>
      <View style={styles.coachingCard}>
        <Pressable
          style={styles.coachingCardHeader}
          onPress={() => setLaunchExpanded((prev) => !prev)}
        >
          <View style={styles.coachingHeaderText}>
            <Text style={styles.cardTitle}>Launch Tilt</Text>
            <Text style={styles.coachingSummaryText}>
              Lift vs. ground contact on non-perfect balls in play.
            </Text>
          </View>
          <View style={styles.coachingHeaderRight}>
            <Text style={[styles.coachingMetricValue, { color: tiltColor }]}>
              {formatSignedDecimal(launchTilt, 2)}
            </Text>
            <Ionicons
              name={launchExpanded ? "chevron-up" : "chevron-down"}
              size={16}
              color="rgba(226, 232, 240, 0.7)"
            />
          </View>
        </Pressable>

        <View style={styles.coachingSummaryRow}>
          <View style={styles.coachingSummaryBlock}>
            <Text style={styles.detailLabel}>Recommendation</Text>
            <Text style={styles.detailValue}>{launchTiltAdvice}</Text>
          </View>
          <View style={[styles.launchTiltAdvicePill, launchStatusStyle]}>
            <Text style={styles.launchTiltAdviceLabel}>Status</Text>
            <Text style={styles.launchTiltAdviceText}>
              {formatStatusLabel(launchStatus)}
            </Text>
          </View>
        </View>

        {launchExpanded ? (
          <View style={styles.coachingDetails}>
            <Text style={styles.coachingDescription}>
              Balanced launch tilt sits between -0.2 and +0.2. Low tilt means you are
              driving too many balls into the ground; high tilt means you are getting under
              too many balls.
            </Text>
            <View style={styles.launchTiltRow}>
              <View style={[styles.launchTiltGraph, { height: graphHeight }]}>
                <View style={styles.launchTiltGraphMidline} />
                {launchTilt !== null ? (
                  <View style={[styles.launchTiltGraphBar, tiltBarStyle]} />
                ) : (
                  <Text style={styles.launchTiltGraphEmpty}>—</Text>
                )}
              </View>
              <View style={styles.launchTiltInfo}>
                <Text style={[styles.launchTiltValue, { color: tiltColor }]}>
                  {formatSignedDecimal(launchTilt, 2)}
                </Text>
                <Text style={styles.launchTiltRange}>Target range: -0.2 to +0.2</Text>
              </View>
            </View>

            <View style={styles.launchTiltSlider}>
              <View style={styles.launchTiltTrack}>
                <View style={[styles.launchTiltSegment, styles.launchTiltSegmentNeg]} />
                <View style={[styles.launchTiltSegment, styles.launchTiltSegmentMid]} />
                <View style={[styles.launchTiltSegment, styles.launchTiltSegmentPos]} />
                {launchTilt !== null ? (
                  <View
                    style={[
                      styles.launchTiltThumb,
                      { left: `${tiltPosition}%`, backgroundColor: tiltColor },
                    ]}
                  />
                ) : null}
              </View>
              <View style={styles.launchTiltTicks}>
                <Text style={styles.launchTiltTickText}>-1.0</Text>
                <Text style={styles.launchTiltTickText}>0</Text>
                <Text style={styles.launchTiltTickText}>+1.0</Text>
              </View>
            </View>
          </View>
        ) : null}
      </View>

      <View style={styles.coachingCard}>
        <Pressable
          style={styles.coachingCardHeader}
          onPress={() => setSlamExpanded((prev) => !prev)}
        >
          <View style={styles.coachingHeaderText}>
            <Text style={styles.cardTitle}>PCI Slamming</Text>
            <Text style={styles.coachingSummaryText}>
              Weighted strikeout + contact pressure score.
            </Text>
          </View>
          <View style={styles.coachingHeaderRight}>
            <Text style={[styles.coachingMetricValue, { color: slamColor }]}>
              {formatPercent(slamScore)}
            </Text>
            <Ionicons
              name={slamExpanded ? "chevron-up" : "chevron-down"}
              size={16}
              color="rgba(226, 232, 240, 0.7)"
            />
          </View>
        </Pressable>

        <View style={styles.coachingSummaryRow}>
          <View style={styles.coachingSummaryBlock}>
            <Text style={styles.detailLabel}>Quick Take</Text>
            <Text style={styles.detailValue}>
              {slamScore === null
                ? "Not enough data yet."
                : slamScore >= 60
                  ? "High slamming pressure."
                  : slamScore >= 40
                    ? "Moderate slamming pressure."
                    : "Low slamming pressure."}
            </Text>
          </View>
          <View style={[styles.launchTiltAdvicePill, slamStatusStyle]}>
            <Text style={styles.launchTiltAdviceLabel}>Status</Text>
            <Text style={styles.launchTiltAdviceText}>
              {formatStatusLabel(slamStatus)}
            </Text>
          </View>
        </View>

        {slamExpanded ? (
          <View style={styles.coachingDetails}>
            <Text style={styles.coachingDescription}>
              PCI Slamming blends swing-and-miss in the heart, in-zone swing strikeouts,
              and non-perfect ground/popup contact into one weighted score.
            </Text>

            <View style={styles.slamScoreRow}>
              <Text style={styles.slamScoreLabel}>Slam Score</Text>
              <Text style={[styles.slamScoreValue, { color: slamColor }]}>
                {formatPercent(slamScore)}
              </Text>
            </View>
            <View style={styles.slamScoreTrack}>
              <View
                style={[
                  styles.slamScoreFill,
                  { width: `${slamWidth}%`, backgroundColor: slamColor },
                ]}
              />
            </View>

            <View style={styles.slamBreakdown}>
              <View style={styles.slamBreakdownRow}>
                <Text style={styles.detailLabel}>Heart Miss K</Text>
                <Text style={styles.detailValue}>{formatPercent(heartMissK)}</Text>
              </View>
              <View style={styles.slamBreakdownRow}>
                <Text style={styles.detailLabel}>In-Zone Swing K</Text>
                <Text style={styles.detailValue}>{formatPercent(inzoneSwingK)}</Text>
              </View>
              <View style={styles.slamBreakdownRow}>
                <Text style={styles.detailLabel}>Extreme Contact (No PP)</Text>
                <Text style={styles.detailValue}>{formatPercent(extremeContact)}</Text>
              </View>
            </View>

            <Text style={styles.slamFormula}>
              SlamScore = 0.45 × Heart Miss K + 0.30 × In-Zone Swing K + 0.25 × Extreme Contact (No PP)
            </Text>
          </View>
        ) : null}
      </View>

      <View style={styles.coachingCard}>
        <Pressable
          style={styles.coachingCardHeader}
          onPress={() => setPitchesExpanded((prev) => !prev)}
        >
          <View style={styles.coachingHeaderText}>
            <Text style={styles.cardTitle}>Worst & Best Pitches</Text>
            <Text style={styles.coachingSummaryText}>
              Strikeout rates by pitch type.
            </Text>
          </View>
          <View style={styles.coachingHeaderRight}>
            <Text style={[styles.coachingMetricValue, { color: pitchStatusColor }]}>
              {worstPitch ? formatPercent(worstPitch.kPct) : pitchTypeRanksLoading ? "..." : "—"}
            </Text>
            <Ionicons
              name={pitchesExpanded ? "chevron-up" : "chevron-down"}
              size={16}
              color="rgba(226, 232, 240, 0.7)"
            />
          </View>
        </Pressable>

        <View style={styles.coachingSummaryRow}>
          <View style={styles.coachingSummaryBlock}>
            <Text style={styles.detailLabel}>Best Pitch</Text>
            <Text style={styles.detailValue}>{bestPitchText}</Text>
            <Text style={styles.detailLabel}>Worst Pitch</Text>
            <Text style={styles.detailValue}>{worstPitchText}</Text>
          </View>
          <View style={[styles.launchTiltAdvicePill, pitchStatusStyle]}>
            <Text style={styles.launchTiltAdviceLabel}>Status</Text>
            <Text style={styles.launchTiltAdviceText}>
              {formatStatusLabel(pitchStatus)}
            </Text>
          </View>
        </View>

        {pitchesExpanded ? (
          <View style={styles.coachingDetails}>
            <Text style={styles.coachingDescription}>
              Ranks pitch types by your strikeout rate when hitting (lower is better).
            </Text>
            {pitchTypeRanksLoading ? (
              <Text style={styles.pitchRankStatus}>Loading pitch rankings...</Text>
            ) : pitchTypeRanksError ? (
              <Text style={styles.pitchRankStatus}>{pitchTypeRanksError}</Text>
            ) : pitchRanks.length ? (
              <View style={styles.pitchRankList}>
                {pitchRanks.map((pitch, index) => (
                  <View key={pitch.pitchType} style={styles.pitchRankRow}>
                    <Text style={styles.pitchRankIndex}>{index + 1}</Text>
                    <Text style={styles.pitchRankLabel}>
                      {formatPitchTypeLabel(pitch.pitchType)}
                    </Text>
                    <Text style={styles.pitchRankValue}>
                      {formatPercent(pitch.kPct)}
                    </Text>
                  </View>
                ))}
              </View>
            ) : (
              <Text style={styles.pitchRankStatus}>Not enough data yet.</Text>
            )}
          </View>
        ) : null}
      </View>
    </View>
  );
};

const SectionPlaceholder = ({ title, description }: SectionPlaceholderProps) => {
  return (
    <View style={styles.analyticsSection}>
      <Text style={styles.cardTitle}>{title}</Text>
      <Text style={styles.placeholderText}>{description}</Text>
    </View>
  );
};

type CardsTableSectionProps<
  RowT,
  ColumnT extends { key: string; width: number; align: string; label: string; format?: (row: RowT) => string; }
> = {
  title: string;
  subtitle: string;
  rows: RowT[];
  totalRows: number;
  loading: boolean;
  error: string | null;
  filter: string;
  setFilter: Dispatch<SetStateAction<string>>;
  minLabel: string;
  minValue: string;
  setMinValue: Dispatch<SetStateAction<string>>;
  filterPlaceholder: string;
  sortKey: string;
  sortDirection: CardSortDirection;
  onSortChange: (key: any) => void;
  columns: ReadonlyArray<ColumnT>;
  frozenKeys: ReadonlyArray<string>;
  frozenDivider: number;
};

function CardsTableSection<
  RowT extends { mlb_id: number },
  ColumnT extends { key: string; width: number; align: string; label: string; format?: (row: RowT) => string; }
>({
  title,
  subtitle,
  rows,
  totalRows,
  loading,
  error,
  filter,
  setFilter,
  minLabel,
  minValue,
  setMinValue,
  filterPlaceholder,
  sortKey,
  sortDirection,
  onSortChange,
  columns,
  frozenKeys,
  frozenDivider,
}: CardsTableSectionProps<RowT, ColumnT>) {
  const handleMinChange = (value: string) => {
    const digits = value.replace(/[^0-9]/g, "");
    if (!digits) {
      setMinValue("");
      return;
    }
    const next = Math.max(0, Math.min(9999, Number.parseInt(digits, 10)));
    setMinValue(Number.isNaN(next) ? "" : String(next));
  };
  const statusLabel = `${rows.length} of ${totalRows} cards`;
  const frozenColumns = useMemo(
    () => columns.filter((column) => frozenKeys.includes(column.key)),
    [columns, frozenKeys]
  );
  const scrollColumns = useMemo(
    () => columns.filter((column) => !frozenKeys.includes(column.key)),
    [columns, frozenKeys]
  );
  const frozenWidth = useMemo(() => {
    const base = frozenColumns.reduce((sum, column) => sum + column.width, 0);
    return base + frozenDivider;
  }, [frozenColumns, frozenDivider]);
  const scrollWidth = useMemo(
    () => scrollColumns.reduce((sum, column) => sum + column.width, 0),
    [scrollColumns]
  );

  return (
    <View style={styles.analyticsSection}>
      <View style={styles.sectionHeaderRow}>
        <View style={styles.sectionHeader}>
          <Text style={styles.cardTitle}>{title}</Text>
          <Text style={styles.sectionSubheader}>{subtitle}</Text>
        </View>
      </View>

      <View style={styles.cardsFilters}>
        <View style={styles.cardsFilterRow}>
          <View style={styles.cardsFilterInput}>
            <Ionicons name="search" size={14} color="rgba(226, 232, 240, 0.6)" />
            <TextInput
              value={filter}
              onChangeText={setFilter}
              placeholder={filterPlaceholder}
              placeholderTextColor={theme.colors.muted}
              autoCapitalize="words"
              style={styles.cardsFilterInputField}
            />
          </View>
          <View style={styles.cardsMinPaInput}>
            <Text style={styles.cardsMinPaLabel}>{minLabel}</Text>
            <TextInput
              value={minValue}
              onChangeText={handleMinChange}
              placeholder="0"
              placeholderTextColor={theme.colors.muted}
              keyboardType="number-pad"
              maxLength={4}
              style={styles.cardsMinPaField}
            />
          </View>
        </View>
        <View style={styles.cardsMetaRow}>
          <Text style={styles.cardsMetaText}>{statusLabel}</Text>
          <Text style={styles.cardsHintText}>Tap a column to sort</Text>
        </View>
      </View>

      <CardsStatsTable
        rows={rows}
        loading={loading}
        error={error}
        sortKey={sortKey}
        sortDirection={sortDirection}
        onSortChange={onSortChange}
        frozenColumns={frozenColumns}
        scrollColumns={scrollColumns}
        frozenWidth={frozenWidth}
        scrollWidth={scrollWidth}
        frozenDivider={frozenDivider}
      />
    </View>
  );
}

type CardsStatsTableProps<
  RowT,
  ColumnT extends { key: string; width: number; align: string; label: string; format?: (row: RowT) => string; }
> = {
  rows: RowT[];
  loading: boolean;
  error: string | null;
  sortKey: string;
  sortDirection: CardSortDirection;
  onSortChange: (key: any) => void;
  frozenColumns: ReadonlyArray<ColumnT>;
  scrollColumns: ReadonlyArray<ColumnT>;
  frozenWidth: number;
  scrollWidth: number;
  frozenDivider: number;
};

function CardsStatsTable<
  RowT extends { mlb_id: number },
  ColumnT extends { key: string; width: number; align: string; label: string; format?: (row: RowT) => string; }
>({
  rows,
  loading,
  error,
  sortKey,
  sortDirection,
  onSortChange,
  frozenColumns,
  scrollColumns,
  frozenWidth,
  scrollWidth,
  frozenDivider,
}: CardsStatsTableProps<RowT, ColumnT>) {
  const headerScrollRef = useRef<ScrollView | null>(null);
  const bodyScrollRef = useRef<ScrollView | null>(null);
  const syncingRef = useRef(false);
  const showStatus = loading || !!error || rows.length === 0;
  const statusText = loading
    ? "Loading cards..."
    : error
      ? error
      : "No cards match these filters.";

  const handleBodyScroll = (event: any) => {
    if (syncingRef.current) return;
    const offsetX = event?.nativeEvent?.contentOffset?.x ?? 0;
    if (!headerScrollRef.current) return;
    syncingRef.current = true;
    headerScrollRef.current.scrollTo({ x: offsetX, animated: false });
    syncingRef.current = false;
  };

  return (
    <>
      <View style={styles.cardsTableWrap}>
        <View
          style={[
            styles.cardsHeaderRow,
            styles.cardsHeaderFrozen,
            { width: frozenWidth, paddingRight: frozenDivider },
          ]}
        >
          {frozenColumns.map((column) => {
            const active = column.key === sortKey;
            return (
              <Pressable
                key={column.key}
                style={[
                  styles.cardsHeaderCell,
                  { width: column.width },
                  column.align === "right" && styles.cardsCellRight,
                ]}
                onPress={() => onSortChange(column.key)}
              >
                <View style={styles.cardsHeaderCellInner}>
                  <Text
                    style={[
                      styles.cardsHeaderCellText,
                      active && styles.cardsHeaderCellTextActive,
                    ]}
                    numberOfLines={1}
                  >
                    {column.label}
                  </Text>
                  {active ? (
                    <Ionicons
                      name={sortDirection === "asc" ? "chevron-up" : "chevron-down"}
                      size={10}
                      color={theme.colors.primary}
                    />
                  ) : null}
                </View>
              </Pressable>
            );
          })}
        </View>

        <ScrollView
          horizontal
          scrollEnabled={false}
          ref={headerScrollRef}
          showsHorizontalScrollIndicator={false}
        >
          <View style={[styles.cardsHeaderRow, styles.cardsHeaderScroll, { width: scrollWidth }]}>
            {scrollColumns.map((column) => {
              const active = column.key === sortKey;
              return (
                <Pressable
                  key={column.key}
                  style={[
                    styles.cardsHeaderCell,
                    { width: column.width },
                    column.align === "right" && styles.cardsCellRight,
                  ]}
                  onPress={() => onSortChange(column.key)}
                >
                  <View style={styles.cardsHeaderCellInner}>
                    <Text
                      style={[
                        styles.cardsHeaderCellText,
                        active && styles.cardsHeaderCellTextActive,
                      ]}
                      numberOfLines={1}
                    >
                      {column.label}
                    </Text>
                    {active ? (
                      <Ionicons
                        name={sortDirection === "asc" ? "chevron-up" : "chevron-down"}
                        size={10}
                        color={theme.colors.primary}
                      />
                    ) : null}
                  </View>
                </Pressable>
              );
            })}
          </View>
        </ScrollView>
      </View>

      {showStatus ? (
        <View style={styles.cardsStatusRow}>
          <Text style={styles.cardsStatusText}>{statusText}</Text>
        </View>
      ) : (
        <ScrollView style={styles.cardsBody} nestedScrollEnabled>
          <View style={styles.cardsBodyRowWrap}>
            <View
              style={[
                styles.cardsFrozenColumn,
                { width: frozenWidth, paddingRight: frozenDivider },
              ]}
            >
              {rows.map((row, index) => (
                <View
                  key={row.mlb_id}
                  style={[styles.cardsRow, index % 2 === 1 && styles.cardsRowAlt]}
                >
                  {frozenColumns.map((column) => {
                    const value = column.format ? column.format(row) : "—";
                    return (
                      <View
                        key={column.key}
                        style={[
                          styles.cardsCell,
                          { width: column.width },
                          column.align === "right" && styles.cardsCellRight,
                        ]}
                      >
                        <Text
                          style={[
                            styles.cardsCellText,
                            column.align === "right" && styles.cardsCellTextRight,
                          ]}
                          numberOfLines={1}
                        >
                          {value}
                        </Text>
                      </View>
                    );
                  })}
                </View>
              ))}
            </View>

            <ScrollView
              horizontal
              ref={bodyScrollRef}
              onScroll={handleBodyScroll}
              scrollEventThrottle={16}
              showsHorizontalScrollIndicator={false}
            >
              <View style={{ width: scrollWidth }}>
                {rows.map((row, index) => (
                  <View
                    key={row.mlb_id}
                    style={[styles.cardsRow, index % 2 === 1 && styles.cardsRowAlt]}
                  >
                    {scrollColumns.map((column) => {
                      const value = column.format ? column.format(row) : "—";
                      return (
                        <View
                          key={column.key}
                          style={[
                            styles.cardsCell,
                            { width: column.width },
                            column.align === "right" && styles.cardsCellRight,
                          ]}
                        >
                          <Text
                            style={[
                              styles.cardsCellText,
                              column.align === "right" && styles.cardsCellTextRight,
                            ]}
                            numberOfLines={1}
                          >
                            {value}
                          </Text>
                        </View>
                      );
                    })}
                  </View>
                ))}
              </View>
            </ScrollView>
          </View>
        </ScrollView>
      )}
    </>
  );
}

const GameLogSection = ({
  games,
  totalGames,
  loading,
  error,
  difficulty,
  setDifficulty,
  difficultyOptions,
  resultFilter,
  setResultFilter,
  ballparkQuery,
  setBallparkQuery,
  username,
  onSelectGame,
}: GameLogSectionProps) => {
  const statusLabel = loading
    ? "Loading games..."
    : error
      ? error
      : `${games.length} of ${totalGames} games`;

  return (
    <View style={styles.analyticsSection}>
      <View style={styles.sectionHeaderRow}>
        <View style={styles.sectionHeader}>
          <Text style={styles.cardTitle}>Game Log</Text>
          <Text style={styles.sectionSubheader}>
            Recent games with quick filters.
          </Text>
        </View>
      </View>

      <View style={styles.gameLogFilters}>
        <View style={styles.gameLogFilterRow}>
          <Text style={styles.filterLabel}>Result</Text>
          <View style={styles.filterToggle}>
            {GAME_LOG_RESULT_OPTIONS.map((option) => {
              const active = option.value === resultFilter;
              return (
                <TouchableOpacity
                  key={option.value}
                  style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                  onPress={() => setResultFilter(option.value)}
                >
                  <Text
                    style={[
                      styles.filterToggleText,
                      active && styles.filterToggleTextActive,
                    ]}
                  >
                    {option.label}
                  </Text>
                </TouchableOpacity>
              );
            })}
          </View>
        </View>

        <View style={styles.gameLogFilterRow}>
          <Text style={styles.filterLabel}>Difficulty</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false}>
            <View style={[styles.filterToggle, styles.gameLogDifficultyRow]}>
              {difficultyOptions.map((option) => {
                const value = option === "All" ? "all" : option;
                const active = value === difficulty;
                return (
                  <TouchableOpacity
                    key={option}
                    style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                    onPress={() => setDifficulty(value)}
                  >
                    <Text
                      style={[
                        styles.filterToggleText,
                        active && styles.filterToggleTextActive,
                      ]}
                    >
                      {option}
                    </Text>
                  </TouchableOpacity>
                );
              })}
            </View>
          </ScrollView>
        </View>

        <View style={styles.gameLogFilterRow}>
          <Text style={styles.filterLabel}>Ball Park</Text>
          <TextInput
            value={ballparkQuery}
            onChangeText={setBallparkQuery}
            placeholder="Filter by park name..."
            placeholderTextColor={theme.colors.muted}
            autoCapitalize="words"
            style={[styles.filterInput, styles.gameLogBallparkInput]}
          />
        </View>
      </View>

      <View style={styles.gameLogMetaRow}>
        <Text style={styles.gameLogMetaText}>{statusLabel}</Text>
      </View>

      <ScrollView horizontal showsHorizontalScrollIndicator={false}>
        <View style={styles.gameLogTable}>
          <View style={styles.gameLogHeaderRow}>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellDate]}>Date</Text>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellResult]}>Result</Text>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellScore]}>Score</Text>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellOpponent]}>Opponent</Text>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellDifficulty]}>
              Difficulty
            </Text>
            <Text style={[styles.gameLogHeaderCell, styles.gameLogCellPark]}>Ballpark</Text>
          </View>

          <ScrollView style={styles.gameLogBody} nestedScrollEnabled>
            {loading ? (
              <View style={styles.gameLogStatusRow}>
                <Text style={styles.gameLogStatusText}>Loading games...</Text>
              </View>
            ) : error ? (
              <View style={styles.gameLogStatusRow}>
                <Text style={styles.gameLogStatusText}>{error}</Text>
              </View>
            ) : games.length === 0 ? (
              <View style={styles.gameLogStatusRow}>
                <Text style={styles.gameLogStatusText}>No games match these filters.</Text>
              </View>
            ) : (
              games.map((game) => {
                const perspective = getGameLogPerspective(game, username);
                const result = perspective.userResult ?? "—";
                const resultStyle =
                  result === "W"
                    ? styles.gameLogResultWin
                    : result === "L"
                      ? styles.gameLogResultLoss
                      : styles.gameLogResultNeutral;
                return (
                  <TouchableOpacity
                    key={game.game_id}
                    style={styles.gameLogRow}
                    activeOpacity={0.8}
                    onPress={() => onSelectGame(game)}
                  >
                    <View style={styles.gameLogRowMain}>
                      <Text style={[styles.gameLogCell, styles.gameLogCellDate]}>
                        {formatDate(game.date)}
                      </Text>
                      <View style={[styles.gameLogCell, styles.gameLogCellResult]}>
                        <Text style={[styles.gameLogResultPill, resultStyle]}>{result}</Text>
                      </View>
                      <Text style={[styles.gameLogCell, styles.gameLogCellScore]}>
                        {perspective.scoreFor}-{perspective.scoreAgainst}
                      </Text>
                      <Text
                        style={[styles.gameLogCell, styles.gameLogCellOpponent]}
                        numberOfLines={1}
                      >
                        {perspective.locationLabel} {perspective.opponentName}
                      </Text>
                      <Text style={[styles.gameLogCell, styles.gameLogCellDifficulty]}>
                        {game.difficulty ?? "—"}
                      </Text>
                      <Text
                        style={[styles.gameLogCell, styles.gameLogCellPark]}
                        numberOfLines={1}
                      >
                        {game.ball_park_name ?? "—"}
                      </Text>
                    </View>
                    {game.summary ? (
                      <Text style={styles.gameLogRowSummary} numberOfLines={2}>
                        {game.summary}
                      </Text>
                    ) : null}
                  </TouchableOpacity>
                );
              })
            )}
          </ScrollView>
        </View>
      </ScrollView>
    </View>
  );
};

type StrikeoutsSectionProps = {
  strikeoutMode: "Hitting" | "Pitching";
  setStrikeoutMode: Dispatch<SetStateAction<"Hitting" | "Pitching">>;
  hitterSideLabel: string;
  pitcherHandLabel: string;
  selectedPitcher: ShowPitcherSearchResult | null;
  onSelectPitcher: (
    pitcher: ShowPitcherSearchResult | null,
    opts?: { closeMenu?: boolean }
  ) => void;
  pitcherSearchQuery: string;
  setPitcherSearchQuery: Dispatch<SetStateAction<string>>;
  pitcherSearchResults: ShowPitcherSearchResult[];
  pitcherSearchLoading: boolean;
  pitcherSearchError: string | null;
  selectedHitter: ShowHitterSearchResult | null;
  onSelectHitter: (
    hitter: ShowHitterSearchResult | null,
    opts?: { closeMenu?: boolean }
  ) => void;
  hitterSearchQuery: string;
  setHitterSearchQuery: Dispatch<SetStateAction<string>>;
  hitterSearchResults: ShowHitterSearchResult[];
  hitterSearchLoading: boolean;
  hitterSearchError: string | null;
  pitchTypeLabel: string;
  pitchTypeMenuOptions: string[];
  filterHitterSide: { side: HitterSide };
  setFilterHitterSide: Dispatch<SetStateAction<{ side: HitterSide }>>;
  filterPitcherHand: { hand: PitcherHand };
  setFilterPitcherHand: Dispatch<SetStateAction<{ hand: PitcherHand }>>;
  filterPitchTypes: PitchType[];
  togglePitchType: (pitch: string) => void;
  activeFilterMenu: null | "hitter" | "pitcher";
  setActiveFilterMenu: Dispatch<SetStateAction<null | "hitter" | "pitcher">>;
  handleResetFilters: () => void;
  advancedFiltersOpen: boolean;
  setAdvancedFiltersOpen: Dispatch<SetStateAction<boolean>>;
  advancedFilters: {
    minSpeed: string;
    maxSpeed: string;
    timing: TimingType;
    outType: OutType;
  };
  setAdvancedFilters: Dispatch<
    SetStateAction<{
      minSpeed: string;
      maxSpeed: string;
      timing: TimingType;
      outType: OutType;
    }>
  >;
  clampSpeedInput: (value: string) => string;
  hasAdvancedFilters: boolean;
  strikeoutZones: number[][];
  strikeoutOutside: StrikeoutMapData["outside"];
  strikeoutSelections: StrikeoutSelection[];
  onSelectionChange: (selection: StrikeoutSelection) => void;
  summaryStats: SummaryStatItem[];
  onStatPress: (label: string) => void;
  statHelpOpen: boolean;
  setStatHelpOpen: Dispatch<SetStateAction<boolean>>;
  statHelp: StatHelp | null;
};

const StrikeoutsSection = ({
  strikeoutMode,
  setStrikeoutMode,
  hitterSideLabel,
  pitcherHandLabel,
  selectedPitcher,
  onSelectPitcher,
  pitcherSearchQuery,
  setPitcherSearchQuery,
  pitcherSearchResults,
  pitcherSearchLoading,
  pitcherSearchError,
  selectedHitter,
  onSelectHitter,
  hitterSearchQuery,
  setHitterSearchQuery,
  hitterSearchResults,
  hitterSearchLoading,
  hitterSearchError,
  pitchTypeLabel,
  pitchTypeMenuOptions,
  filterHitterSide,
  setFilterHitterSide,
  filterPitcherHand,
  setFilterPitcherHand,
  filterPitchTypes,
  togglePitchType,
  activeFilterMenu,
  setActiveFilterMenu,
  handleResetFilters,
  advancedFiltersOpen,
  setAdvancedFiltersOpen,
  advancedFilters,
  setAdvancedFilters,
  clampSpeedInput,
  hasAdvancedFilters,
  strikeoutZones,
  strikeoutOutside,
  strikeoutSelections,
  onSelectionChange,
  summaryStats,
  onStatPress,
  statHelpOpen,
  setStatHelpOpen,
  statHelp,
}: StrikeoutsSectionProps) => {
  return (
    <>
      <View style={styles.analyticsSection}>
        <View style={styles.sectionHeaderRow}>
          <View style={styles.sectionHeader}>
            <Text style={styles.cardTitle}>Strikeout Data</Text>
            <Text style={styles.sectionSubheader}>
              Filtered strikeout outcomes and zone patterns.
            </Text>
          </View>
          <View style={styles.sectionToggle}>
            {(["Hitting", "Pitching"] as const).map((mode) => (
              <TouchableOpacity
                key={mode}
                style={[
                  styles.sectionToggleButton,
                  strikeoutMode === mode && styles.sectionToggleButtonActive,
                ]}
                onPress={() => setStrikeoutMode(mode)}
              >
                <Text
                  style={[
                    styles.sectionToggleText,
                    strikeoutMode === mode && styles.sectionToggleTextActive,
                  ]}
                >
                  {mode}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>
        <View style={styles.filtersHeaderRow}>
          <View style={styles.filtersLeft}>
            <View style={styles.hitterFilterColumn}>
              <TouchableOpacity
                style={styles.hitterFilterToggle}
                activeOpacity={0.8}
                onPress={() =>
                  setActiveFilterMenu((prev) => (prev === "hitter" ? null : "hitter"))
                }
              >
                <Text
                  style={styles.hitterFilterToggleText}
                  numberOfLines={1}
                  ellipsizeMode="tail"
                >
                  {hitterSideLabel}
                </Text>
                <Text style={styles.hitterFilterToggleIcon}>
                  {activeFilterMenu === "hitter" ? "^" : "v"}
                </Text>
              </TouchableOpacity>
              {activeFilterMenu === "hitter" ? (
                <View style={[styles.hitterFilterMenu, styles.wideFilterMenuLeft]}>
                  {selectedHitter ? (
                    <View style={styles.pitcherActiveRow}>
                      <View style={styles.pitcherActiveText}>
                        <Text style={styles.pitcherActiveLabel}>Selected hitter</Text>
                        <Text style={styles.pitcherActiveName} numberOfLines={1}>
                          {formatHitterName(selectedHitter)}
                        </Text>
                      </View>
                      <TouchableOpacity
                        style={styles.pitcherClearButton}
                        onPress={() => onSelectHitter(null)}
                      >
                        <Text style={styles.pitcherClearText}>Clear</Text>
                      </TouchableOpacity>
                    </View>
                  ) : null}

                  <View style={styles.pitcherSearchBlock}>
                    <TextInput
                      value={hitterSearchQuery}
                      onChangeText={setHitterSearchQuery}
                      placeholder="Search hitter..."
                      placeholderTextColor={theme.colors.muted}
                      autoCapitalize="words"
                      autoCorrect={false}
                      style={styles.pitcherSearchInput}
                    />
                    <View style={styles.pitcherSearchResults}>
                      {hitterSearchLoading ? (
                        <Text style={styles.pitcherSearchStatus}>Searching...</Text>
                      ) : hitterSearchError ? (
                        <Text style={styles.pitcherSearchStatus}>{hitterSearchError}</Text>
                      ) : hitterSearchResults.length === 0 && hitterSearchQuery.trim() ? (
                        <Text style={styles.pitcherSearchStatus}>No matches.</Text>
                      ) : (
                        hitterSearchResults.map((hitter) => (
                          <TouchableOpacity
                            key={hitter.mlb_id}
                            style={styles.pitcherSearchRow}
                            onPress={() => onSelectHitter(hitter)}
                          >
                            <View style={styles.pitcherSearchText}>
                              <Text style={styles.pitcherSearchName} numberOfLines={2}>
                                {formatHitterName(hitter)}
                              </Text>
                              <Text style={styles.pitcherSearchMeta}>
                                {formatHitterSideLabel(hitter)}
                              </Text>
                            </View>
                          </TouchableOpacity>
                        ))
                      )}
                    </View>
                  </View>

                  <View style={styles.pitcherSearchDivider} />

                  {HITTER_SIDE_OPTIONS.map((option) => {
                    const active = option.value === filterHitterSide.side;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.hitterFilterItem, active && styles.hitterFilterItemActive]}
                        onPress={() => {
                          setFilterHitterSide({ side: option.value });
                          onSelectHitter(null, { closeMenu: false });
                        }}
                      >
                        <Text
                          style={[
                            styles.hitterFilterItemText,
                            active && styles.hitterFilterItemTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              ) : null}
            </View>

            <View style={styles.hitterFilterColumn}>
              <TouchableOpacity
                style={styles.hitterFilterToggle}
                activeOpacity={0.8}
                onPress={() =>
                  setActiveFilterMenu((prev) => (prev === "pitcher" ? null : "pitcher"))
                }
              >
                <Text
                  style={styles.hitterFilterToggleText}
                  numberOfLines={1}
                  ellipsizeMode="tail"
                >
                  {pitcherHandLabel}
                </Text>
                <Text style={styles.hitterFilterToggleIcon}>
                  {activeFilterMenu === "pitcher" ? "^" : "v"}
                </Text>
              </TouchableOpacity>
              {activeFilterMenu === "pitcher" ? (
                <View style={[styles.hitterFilterMenu, styles.wideFilterMenuRight]}>
                  {selectedPitcher ? (
                    <View style={styles.pitcherActiveRow}>
                      <View style={styles.pitcherActiveText}>
                        <Text style={styles.pitcherActiveLabel}>Selected pitcher</Text>
                        <Text style={styles.pitcherActiveName} numberOfLines={1}>
                          {formatPitcherName(selectedPitcher)}
                        </Text>
                      </View>
                      <TouchableOpacity
                        style={styles.pitcherClearButton}
                        onPress={() => onSelectPitcher(null)}
                      >
                        <Text style={styles.pitcherClearText}>Clear</Text>
                      </TouchableOpacity>
                    </View>
                  ) : null}

                  <View style={styles.pitcherSearchBlock}>
                    <TextInput
                      value={pitcherSearchQuery}
                      onChangeText={setPitcherSearchQuery}
                      placeholder="Search pitcher..."
                      placeholderTextColor={theme.colors.muted}
                      autoCapitalize="words"
                      autoCorrect={false}
                      style={styles.pitcherSearchInput}
                    />
                    <View style={styles.pitcherSearchResults}>
                      {pitcherSearchLoading ? (
                        <Text style={styles.pitcherSearchStatus}>Searching...</Text>
                      ) : pitcherSearchError ? (
                        <Text style={styles.pitcherSearchStatus}>{pitcherSearchError}</Text>
                      ) : pitcherSearchResults.length === 0 && pitcherSearchQuery.trim() ? (
                        <Text style={styles.pitcherSearchStatus}>No matches.</Text>
                      ) : (
                        pitcherSearchResults.map((pitcher) => (
                          <TouchableOpacity
                            key={pitcher.mlb_id}
                            style={styles.pitcherSearchRow}
                            onPress={() => onSelectPitcher(pitcher)}
                          >
                            <View style={styles.pitcherSearchText}>
                              <Text style={styles.pitcherSearchName} numberOfLines={2}>
                                {formatPitcherName(pitcher)}
                              </Text>
                              <Text style={styles.pitcherSearchMeta}>
                                {formatPitcherHandLabel(pitcher)}
                              </Text>
                            </View>
                          </TouchableOpacity>
                        ))
                      )}
                    </View>
                  </View>

                  <View style={styles.pitcherSearchDivider} />

                  {PITCHER_HAND_OPTIONS.map((option) => {
                    const active = option.value === filterPitcherHand.hand;
                    return (
                      <TouchableOpacity
                        key={option.value}
                        style={[styles.hitterFilterItem, active && styles.hitterFilterItemActive]}
                        onPress={() => {
                          setFilterPitcherHand({ hand: option.value });
                          onSelectPitcher(null, { closeMenu: false });
                          setActiveFilterMenu(null);
                        }}
                      >
                        <Text
                          style={[
                            styles.hitterFilterItemText,
                            active && styles.hitterFilterItemTextActive,
                          ]}
                        >
                          {option.label}
                        </Text>
                      </TouchableOpacity>
                    );
                  })}
                </View>
              ) : null}
            </View>
          </View>
          <View style={styles.filtersActions}>
            <TouchableOpacity style={styles.resetFiltersButton} onPress={handleResetFilters}>
              <Text style={styles.resetFiltersText}>Reset</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[
                styles.advancedFilterButton,
                (advancedFiltersOpen || hasAdvancedFilters) &&
                  styles.advancedFilterButtonActive,
              ]}
              onPress={() => {
                setActiveFilterMenu(null);
                setAdvancedFiltersOpen(true);
              }}
            >
              <Ionicons name="options" size={16} color={theme.colors.text} />
            </TouchableOpacity>
          </View>
        </View>
        <View style={styles.analyticsContentRow}>
          <StrikeoutMap
            zones={strikeoutZones}
            outside={strikeoutOutside}
            filterHitterSide={filterHitterSide}
            selections={strikeoutSelections}
            onSelectionChange={onSelectionChange}
          />
          <View style={styles.analyticsSummary}>
            {summaryStats.map((item) => (
              <Pressable
                key={item.label}
                style={({ pressed }) => [
                  styles.summaryRow,
                  pressed && styles.summaryRowPressed,
                ]}
                onPress={() => onStatPress(item.label)}
              >
                <Text style={styles.summaryLabel} numberOfLines={1}>
                  {item.label}
                </Text>
                <Text style={styles.summaryValue} numberOfLines={1}>
                  {item.value}
                </Text>
              </Pressable>
            ))}
          </View>
        </View>
      </View>

      <Modal
        transparent
        visible={advancedFiltersOpen}
        animationType="fade"
        onRequestClose={() => setAdvancedFiltersOpen(false)}
      >
        <View style={styles.filterOverlay}>
          <Pressable
            style={StyleSheet.absoluteFill}
            onPress={() => setAdvancedFiltersOpen(false)}
          />
          <View style={styles.filterPanel}>
            <View style={styles.filterHeaderRow}>
              <Text style={styles.filterTitle}>Advanced Filters</Text>
              <TouchableOpacity
                style={styles.filterCloseButton}
                onPress={() => setAdvancedFiltersOpen(false)}
              >
                <Ionicons name="close" size={16} color={theme.colors.text} />
              </TouchableOpacity>
            </View>

            <View style={styles.filterField}>
              <Text style={styles.filterLabel}>Pitch Speed (mph)</Text>
              <View style={styles.filterInputRow}>
                <View style={styles.filterInputWrap}>
                  <Text style={styles.filterInputLabel}>Min</Text>
                  <TextInput
                    value={advancedFilters.minSpeed}
                    onChangeText={(value) =>
                      setAdvancedFilters((prev) => ({
                        ...prev,
                        minSpeed: clampSpeedInput(value),
                      }))
                    }
                    placeholder="0"
                    placeholderTextColor={theme.colors.muted}
                    keyboardType="number-pad"
                    maxLength={3}
                    style={styles.filterInput}
                  />
                </View>
                <View style={styles.filterInputWrap}>
                  <Text style={styles.filterInputLabel}>Max</Text>
                  <TextInput
                    value={advancedFilters.maxSpeed}
                    onChangeText={(value) =>
                      setAdvancedFilters((prev) => ({
                        ...prev,
                        maxSpeed: clampSpeedInput(value),
                      }))
                    }
                    placeholder="99"
                    placeholderTextColor={theme.colors.muted}
                    keyboardType="number-pad"
                    maxLength={3}
                    style={styles.filterInput}
                  />
                </View>
              </View>
            </View>

            <View style={styles.filterField}>
              <Text style={styles.filterLabel}>Timing</Text>
              <View style={styles.filterToggle}>
                {TIMING_OPTIONS.map((option) => {
                  const active = option.value === advancedFilters.timing;
                  return (
                    <TouchableOpacity
                      key={option.value}
                      style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                      onPress={() =>
                        setAdvancedFilters((prev) => ({ ...prev, timing: option.value }))
                      }
                    >
                      <Text
                        style={[
                          styles.filterToggleText,
                          active && styles.filterToggleTextActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            </View>

            <View style={styles.filterField}>
              <Text style={styles.filterLabel}>Out Type</Text>
              <View style={styles.filterToggle}>
                {OUT_TYPE_OPTIONS.map((option) => {
                  const active = option.value === advancedFilters.outType;
                  return (
                    <TouchableOpacity
                      key={option.value}
                      style={[styles.filterToggleButton, active && styles.filterToggleButtonActive]}
                      onPress={() =>
                        setAdvancedFilters((prev) => ({ ...prev, outType: option.value }))
                      }
                    >
                      <Text
                        style={[
                          styles.filterToggleText,
                          active && styles.filterToggleTextActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            </View>

            <View style={styles.filterField}>
              <Text style={styles.filterLabel}>Pitch Types</Text>
              <Text style={styles.filterHint}>{pitchTypeLabel}</Text>
              <View style={styles.pitchTypeRow}>
                {pitchTypeMenuOptions.map((option) => {
                  const isAll = option === "all";
                  const active = isAll
                    ? filterPitchTypes.length === 0
                    : filterPitchTypes.includes(option);
                  return (
                    <TouchableOpacity
                      key={option}
                      style={[
                        styles.pitchTypeChip,
                        active && styles.pitchTypeChipActive,
                      ]}
                      onPress={() => togglePitchType(option)}
                    >
                      <Text
                        style={[
                          styles.pitchTypeChipText,
                          active && styles.pitchTypeChipTextActive,
                        ]}
                      >
                        {isAll ? "All pitches" : formatPitchTypeLabel(option)}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            </View>
          </View>
        </View>
      </Modal>

      <Modal
        transparent
        visible={statHelpOpen}
        animationType="fade"
        onRequestClose={() => setStatHelpOpen(false)}
      >
        <View style={styles.helpOverlay}>
          <Pressable
            style={StyleSheet.absoluteFill}
            onPress={() => setStatHelpOpen(false)}
          />
          <View style={styles.helpPanel}>
            <View style={styles.helpHeaderRow}>
              <Text style={styles.helpTitle}>{statHelp?.title ?? "Stat Detail"}</Text>
              <TouchableOpacity
                style={styles.helpCloseButton}
                onPress={() => setStatHelpOpen(false)}
              >
                <Ionicons name="close" size={16} color={theme.colors.text} />
              </TouchableOpacity>
            </View>
            <Text style={styles.helpDescription}>
              {statHelp?.description ?? "No description available."}
            </Text>
            <Text style={styles.helpSectionLabel}>Math</Text>
            <Text style={styles.helpFormula}>{statHelp?.formula ?? "—"}</Text>
          </View>
        </View>
      </Modal>
    </>
  );
};

type StatsTableRow = { label: string; value: string };

type StatsTableSectionProps = {
  statsMode: "Hitting" | "Pitching";
  setStatsMode: Dispatch<SetStateAction<"Hitting" | "Pitching">>;
  boxscorePrimary: StatsTableRow[];
  boxscoreSecondary: StatsTableRow[];
  advanced: StatsTableRow[];
  loading: boolean;
  error: string | null;
};

const StatsTableSection = ({
  statsMode,
  setStatsMode,
  boxscorePrimary,
  boxscoreSecondary,
  advanced,
  loading,
  error,
}: StatsTableSectionProps) => {
  return (
    <View style={styles.analyticsSection}>
      <View style={styles.sectionHeaderRow}>
        <View style={styles.sectionHeader}>
          <Text style={styles.cardTitle}>Stats Table</Text>
          <Text style={styles.sectionSubheader}>
            Aggregated boxscore and advanced rates.
          </Text>
        </View>
        <View style={styles.statsHeaderActions}>
          <View style={styles.sectionToggle}>
            {(["Hitting", "Pitching"] as const).map((mode) => (
              <TouchableOpacity
                key={mode}
                style={[
                  styles.sectionToggleButton,
                  statsMode === mode && styles.sectionToggleButtonActive,
                ]}
                onPress={() => setStatsMode(mode)}
              >
                <Text
                  style={[
                    styles.sectionToggleText,
                    statsMode === mode && styles.sectionToggleTextActive,
                  ]}
                >
                  {mode}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>
      </View>

      {loading ? (
        <Text style={styles.placeholderText}>Loading stats...</Text>
      ) : error ? (
        <Text style={styles.placeholderText}>{error}</Text>
      ) : boxscorePrimary.length === 0 && advanced.length === 0 ? (
        <Text style={styles.placeholderText}>No stats available yet.</Text>
      ) : (
        <View style={styles.statsRows}>
          <View style={styles.statsRowBlock}>
            <Text style={styles.statsRowTitle}>Boxscore</Text>
            <ScrollView
              horizontal
              showsHorizontalScrollIndicator={false}
              contentContainerStyle={styles.statsRowScroll}
            >
              {boxscorePrimary.map((row) => (
                <View key={row.label} style={styles.statsChip}>
                  <Text style={styles.statsChipLabel}>{row.label}</Text>
                  <Text style={styles.statsChipValue}>{row.value}</Text>
                </View>
              ))}
            </ScrollView>
            {boxscoreSecondary.length ? (
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={styles.statsRowScroll}
              >
                {boxscoreSecondary.map((row) => (
                  <View key={row.label} style={styles.statsChip}>
                    <Text style={styles.statsChipLabel}>{row.label}</Text>
                    <Text style={styles.statsChipValue}>{row.value}</Text>
                  </View>
                ))}
              </ScrollView>
            ) : null}
          </View>
          {advanced.length ? (
            <View style={styles.statsRowBlock}>
              <Text style={styles.statsRowTitle}>Advanced</Text>
              <ScrollView
                horizontal
                showsHorizontalScrollIndicator={false}
                contentContainerStyle={styles.statsRowScroll}
              >
                {advanced.map((row) => (
                  <View key={row.label} style={styles.statsChip}>
                    <Text style={styles.statsChipLabel}>{row.label}</Text>
                    <Text style={styles.statsChipValue}>{row.value}</Text>
                  </View>
                ))}
              </ScrollView>
            </View>
          ) : null}
        </View>
      )}
    </View>
  );
};

function gradientColor(value: number) {
  const v = spreadScore(value);
  if (v <= 50) {
    const t = v / 50;
    return lerpColor("#ef4444", "#facc15", t);
  }
  const t = (v - 50) / 50;
  return lerpColor("#facc15", "#22c55e", t);
}

function spreadScore(value: number) {
  const v = Math.max(0, Math.min(100, value));
  const x = (v - 50) / 50;
  const y = Math.sign(x) * Math.pow(Math.abs(x), 0.2);
  return 50 + 50 * y;
}

function getSearchKey(user: ShowUserSearchResult) {
  if (user.user_id != null) return `id:${user.user_id}`;
  if (user.username) return `name:${user.username.toLowerCase()}`;
  return null;
}

function formatPitcherName(pitcher: ShowPitcherSearchResult) {
  if (!pitcher) return "All pitchers";
  const full = (pitcher.full_name || "").trim();
  if (full) return full;
  const assembled = [pitcher.first_name, pitcher.last_name].filter(Boolean).join(" ").trim();
  if (assembled) return assembled;
  return `Pitcher ${pitcher.mlb_id}`;
}

function formatPitcherHandLabel(pitcher: ShowPitcherSearchResult) {
  const code = (pitcher.pitch_hand_code || "").toUpperCase();
  if (!code) return "Pitcher";
  if (code === "L") return "LHP";
  if (code === "R") return "RHP";
  if (code === "S") return "SHP";
  return `${code}HP`;
}

function formatHitterName(hitter: ShowHitterSearchResult) {
  if (!hitter) return "All hitters";
  const full = (hitter.full_name || "").trim();
  if (full) return full;
  const assembled = [hitter.first_name, hitter.last_name].filter(Boolean).join(" ").trim();
  if (assembled) return assembled;
  return `Hitter ${hitter.mlb_id}`;
}

type CardNameLike = {
  mlb_id: number;
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
};

function formatCardName(card: CardNameLike) {
  if (!card) return "Player";
  const full = (card.full_name || "").trim();
  if (full) return full;
  const assembled = [card.first_name, card.last_name].filter(Boolean).join(" ").trim();
  if (assembled) return assembled;
  return `Player ${card.mlb_id}`;
}

function formatHitterSideLabel(hitter: ShowHitterSearchResult) {
  const code = (hitter.bat_side_code || "").toUpperCase();
  if (!code) return "Hitter";
  if (code === "L") return "LHB";
  if (code === "R") return "RHB";
  if (code === "S") return "Switch";
  return `${code}HB`;
}

function lerpColor(a: string, b: string, t: number) {
  const ar = parseInt(a.slice(1, 3), 16);
  const ag = parseInt(a.slice(3, 5), 16);
  const ab = parseInt(a.slice(5, 7), 16);
  const br = parseInt(b.slice(1, 3), 16);
  const bg = parseInt(b.slice(3, 5), 16);
  const bb = parseInt(b.slice(5, 7), 16);
  const rr = Math.round(ar + (br - ar) * t);
  const rg = Math.round(ag + (bg - ag) * t);
  const rb = Math.round(ab + (bb - ab) * t);
  return `#${rr.toString(16).padStart(2, "0")}${rg
    .toString(16)
    .padStart(2, "0")}${rb.toString(16).padStart(2, "0")}`;
}

function renderPercentRow(
  label: string,
  value: number | null,
  opts?: { emphasis?: boolean }
) {
  const emphasis = opts?.emphasis ?? false;

  if (value === null || Number.isNaN(value)) {
    return (
      <View style={[styles.percentRow, emphasis && styles.percentRowEmphasis]}>
        <View style={styles.percentRowHeader}>
          <Text style={[styles.detailLabel, emphasis && styles.detailLabelEmphasis]}>
            {label}
          </Text>
          <Text
            style={[
              styles.percentValue,
              styles.percentValueMuted,
              emphasis && styles.percentValueEmphasis,
            ]}
          >
            —
          </Text>
        </View>
        <View style={[styles.percentTrack, emphasis && styles.percentTrackEmphasis]}>
          <View style={[styles.percentFill, { width: "0%", backgroundColor: "transparent" }]} />
        </View>
      </View>
    );
  }

  const color = gradientColor(value);
  return (
    <View style={[styles.percentRow, emphasis && styles.percentRowEmphasis]}>
      <View style={styles.percentRowHeader}>
        <Text style={[styles.detailLabel, emphasis && styles.detailLabelEmphasis]}>
          {label}
        </Text>
        <Text style={[styles.percentValue, { color }, emphasis && styles.percentValueEmphasis]}>
          {value}
        </Text>
      </View>
      <View style={[styles.percentTrack, emphasis && styles.percentTrackEmphasis]}>
        <View style={[styles.percentFill, { width: `${value}%`, backgroundColor: color }]} />
      </View>
    </View>
  );
}

function formatDate(value?: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function getGameLogPerspective(game: ShowGameLogItem, username: string | null) {
  const normalized = username ? username.toLowerCase() : null;
  const isHome =
    normalized != null &&
    game.home_profile_username?.toLowerCase() === normalized;
  const isAway =
    normalized != null &&
    game.away_profile_username?.toLowerCase() === normalized;
  const opponentName = isHome
    ? game.away_full_name || game.away_profile_username
    : isAway
      ? game.home_full_name || game.home_profile_username
      : game.away_full_name || game.away_profile_username;
  const locationLabel = isHome ? "vs" : isAway ? "@" : "vs";
  const scoreFor = isHome ? game.home_runs : isAway ? game.away_runs : game.home_runs;
  const scoreAgainst = isHome ? game.away_runs : isAway ? game.home_runs : game.away_runs;
  const userResult = isHome ? game.home_result : isAway ? game.away_result : null;

  return {
    isHome,
    isAway,
    opponentName,
    locationLabel,
    scoreFor,
    scoreAgainst,
    userResult,
  };
}

function getGameLogDedupeKey(game: ShowGameLogItem, username: string | null) {
  const perspective = getGameLogPerspective(game, username);
  const date = new Date(game.date);
  const dateKey = Number.isNaN(date.getTime())
    ? (game.date ?? "").slice(0, 10)
    : date.toISOString().slice(0, 10);
  const opponent = (perspective.opponentName || "").trim().toLowerCase();
  return `${dateKey}|${opponent}`;
}

function formatRate(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const fixed = value.toFixed(3);
  return value < 1 ? fixed.replace(/^0/, "") : fixed;
}

function formatCount(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return Math.round(value).toString();
}

function formatRatio(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(2);
}

function formatInnings(outs?: number | null) {
  if (outs === null || outs === undefined || Number.isNaN(outs)) return "—";
  const whole = Math.floor(outs / 3);
  const rem = Math.max(0, outs % 3);
  return `${whole}.${rem}`;
}

function strikeoutSelectionKey(selection: StrikeoutSelection) {
  if (selection.kind === "zone") return `zone:${selection.row}:${selection.col}`;
  return `outside:${selection.key}`;
}

function formatPitchTypeLabel(value: string) {
  if (!value) return "Unknown";
  return value
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function statsFromCounts(counts: StrikeoutCounts, basePa: number): StrikeoutStats | null {
  if (!basePa || basePa <= 0) return null;
  const kPct = (counts.k / basePa) * 100;
  if (counts.k === 0) {
    return {
      k_pct: kPct,
      chase_pct: 0,
      freeze_pct: 0,
      timing_pct: 0,
      timing_k_pct: 0,
      eye_k_pct: 0,
      location_k_pct: 0,
    };
  }

  const timingTotal = counts.early + counts.late;
  const timingPct = timingTotal
    ? ((counts.early - counts.late) / timingTotal) * 100
    : 0;
  const timingKPct = (timingTotal / counts.k) * 100;
  const eyeKPct = (counts.eye / counts.k) * 100;
  const locationKPct = Math.max(0, Math.min(100, 100 - (timingKPct + eyeKPct)));

  return {
    k_pct: kPct,
    chase_pct: (counts.chase / counts.k) * 100,
    freeze_pct: (counts.look / counts.k) * 100,
    timing_pct: timingPct,
    timing_k_pct: timingKPct,
    eye_k_pct: eyeKPct,
    location_k_pct: locationKPct,
  };
}

function formatPercent(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return `${Math.round(value)}%`;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function clampNumber(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

function formatSignedDecimal(value?: number | null, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const fixed = value.toFixed(digits);
  if (value > 0) return `+${fixed}`;
  return fixed;
}

function sortCardRows<RowT extends { mlb_id: number }>(
  rows: RowT[],
  columns: Array<{ key: string; sortValue?: (row: RowT) => string | number }>,
  sortKey: string,
  sortDirection: CardSortDirection,
  nameGetter: (row: RowT) => string
) {
  const output = [...rows];
  const column = columns.find((col) => col.key === sortKey) ?? columns[0];
  const direction = sortDirection === "asc" ? 1 : -1;
  output.sort((a, b) => {
    const aValue = column.sortValue ? column.sortValue(a) : (a as any)[sortKey];
    const bValue = column.sortValue ? column.sortValue(b) : (b as any)[sortKey];
    if (typeof aValue === "string" || typeof bValue === "string") {
      const result = String(aValue ?? "").localeCompare(String(bValue ?? ""));
      return direction * result;
    }
    const aNum = Number(aValue ?? 0);
    const bNum = Number(bValue ?? 0);
    if (Number.isNaN(aNum) && Number.isNaN(bNum)) return 0;
    if (Number.isNaN(aNum)) return 1;
    if (Number.isNaN(bNum)) return -1;
    if (aNum === bNum) {
      return nameGetter(a).localeCompare(nameGetter(b));
    }
    return direction * (aNum - bNum);
  });
  return output;
}

function formatTimingBias(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const rounded = Math.round(value);
  if (rounded === 0) return "0";
  return rounded > 0 ? `+${rounded}` : `${rounded}`;
}

const styles = StyleSheet.create({
  safe: {
    flex: 1,
    backgroundColor: "#020617",
  },
  container: {
    flex: 1,
    paddingHorizontal: 12,
    paddingTop: 0,
  },
  containerContent: {
    paddingBottom: 24,
    alignItems: "flex-start",
  },
  header: {
    width: "100%",
  },
  headerTitle: {
    color: "white",
    fontSize: 28,
    fontWeight: "800",
    letterSpacing: 0.2,
  },
  headerCaption: {
    marginTop: 4,
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 12,
    fontWeight: "500",
  },
  profileTab: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
    marginTop: 12,
    shadowColor: "#000",
    shadowOpacity: 0.2,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 6 },
    elevation: 6,
  },
  profileImageWrap: {
  },
  profileText: {
    maxWidth: 200,
  },
  profileLabel: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "600",
    letterSpacing: 0.4,
    textTransform: "uppercase",
  },
  profileName: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
    marginTop: 2,
  },
  cardsRow: {
    flexDirection: "row",
    paddingTop: 14,
    paddingBottom: 8,
    gap: 8,
  },
  card: {
    width: 170,
    padding: 14,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  cardTitle: {
    color: theme.colors.primary,
    fontSize: 14,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
  toggleRow: {
    marginTop: 8,
  },
  skillDivider: {
    height: 1,
    backgroundColor: "rgba(148, 163, 184, 0.18)",
    marginVertical: 10,
  },
  cardDivider: {
    height: 1,
    backgroundColor: "rgba(148, 163, 184, 0.18)",
    marginTop: 10,
  },
  cardBody: {
    marginTop: 10,
    gap: 10,
  },
  detailRow: {
    gap: 4,
  },
  detailLabel: {
    color: "rgba(226, 232, 240, 0.55)",
    fontSize: 11,
    fontWeight: "600",
  },
  detailLabelEmphasis: {
    color: "rgba(226, 232, 240, 0.95)",
    fontSize: 14,
    fontWeight: "800",
    letterSpacing: 0.2,
  },
  detailValue: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  percentRow: {
    gap: 6,
  },
  percentRowEmphasis: {
    gap: 10,
    marginBottom: 2,
  },
  percentRowHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  percentValue: {
    fontSize: 12,
    fontWeight: "700",
  },
  percentValueEmphasis: {
    fontSize: 18,
    fontWeight: "900",
  },
  percentValueMuted: {
    color: "rgba(226, 232, 240, 0.45)",
  },
  percentTrack: {
    width: "100%",
    height: 6,
    borderRadius: 999,
    backgroundColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
    position: "relative",
  },
  percentTrackEmphasis: {
    height: 10,
  },
  percentFill: {
    height: "100%",
    borderRadius: 999,
  },
  toggle: {
    flexDirection: "row",
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderRadius: 999,
    padding: 2,
    gap: 2,
    alignSelf: "flex-start",
  },
  toggleButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },
  toggleButtonActive: {
    backgroundColor: "#fbbf24",
  },
  toggleText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 11,
    fontWeight: "600",
  },
  toggleTextActive: {
    color: "#0f172a",
  },
  statsHeader: {
    marginTop: 6,
    color: theme.colors.primary,
  },
  skillRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  sectionTabs: {
    marginTop: 8,
    width: "100%",
    flexDirection: "row",
    gap: 8,
  },
  analyticsSection: {
    width: "100%",
    marginTop: 12,
    padding: 14,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    overflow: "visible",
  },
  coachingStack: {
    width: "100%",
    marginTop: 12,
    gap: 12,
  },
  coachingCard: {
    padding: 14,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  coachingCardHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  coachingHeaderText: {
    flex: 1,
    gap: 4,
  },
  coachingHeaderRight: {
    alignItems: "flex-end",
    gap: 6,
  },
  coachingMetricValue: {
    fontSize: 16,
    fontWeight: "800",
  },
  coachingSummaryText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "600",
  },
  coachingSummaryRow: {
    marginTop: 10,
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 10,
  },
  coachingSummaryBlock: {
    flex: 1,
    gap: 4,
  },
  coachingDetails: {
    marginTop: 12,
    gap: 10,
  },
  coachingDescription: {
    color: "rgba(226, 232, 240, 0.75)",
    fontSize: 12,
    lineHeight: 18,
  },
  launchTiltRow: {
    marginTop: 12,
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
  },
  launchTiltGraph: {
    width: 56,
    borderRadius: 12,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    alignItems: "center",
    justifyContent: "center",
    position: "relative",
    overflow: "hidden",
  },
  launchTiltGraphMidline: {
    position: "absolute",
    left: 0,
    right: 0,
    height: 1,
    backgroundColor: "rgba(148, 163, 184, 0.35)",
    top: "50%",
  },
  launchTiltGraphBar: {
    position: "absolute",
    width: 12,
    borderRadius: 999,
    left: "50%",
    transform: [{ translateX: -6 }],
  },
  launchTiltGraphEmpty: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 16,
    fontWeight: "700",
  },
  launchTiltInfo: {
    flex: 1,
    gap: 6,
  },
  launchTiltValue: {
    fontSize: 20,
    fontWeight: "800",
  },
  launchTiltRange: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "600",
  },
  launchTiltAdvicePill: {
    padding: 10,
    borderRadius: 12,
    borderWidth: 1,
    gap: 4,
  },
  launchTiltAdviceLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  launchTiltAdviceText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  coachingAdviceLow: {
    backgroundColor: "rgba(248, 113, 113, 0.16)",
    borderColor: "rgba(248, 113, 113, 0.4)",
  },
  coachingAdviceHigh: {
    backgroundColor: "rgba(251, 191, 36, 0.15)",
    borderColor: "rgba(251, 191, 36, 0.5)",
  },
  coachingAdviceOk: {
    backgroundColor: "rgba(34, 197, 94, 0.16)",
    borderColor: "rgba(34, 197, 94, 0.45)",
  },
  coachingAdviceUnknown: {
    backgroundColor: "rgba(148, 163, 184, 0.15)",
    borderColor: "rgba(148, 163, 184, 0.35)",
  },
  launchTiltSlider: {
    marginTop: 12,
    gap: 8,
  },
  launchTiltTrack: {
    height: 10,
    borderRadius: 999,
    overflow: "hidden",
    flexDirection: "row",
    position: "relative",
    backgroundColor: "rgba(148, 163, 184, 0.2)",
  },
  launchTiltSegment: {
    height: "100%",
  },
  launchTiltSegmentNeg: {
    flex: 4,
    backgroundColor: "rgba(248, 113, 113, 0.55)",
  },
  launchTiltSegmentMid: {
    flex: 2,
    backgroundColor: "rgba(34, 197, 94, 0.65)",
  },
  launchTiltSegmentPos: {
    flex: 4,
    backgroundColor: "rgba(251, 191, 36, 0.6)",
  },
  launchTiltThumb: {
    position: "absolute",
    top: -3,
    width: 14,
    height: 14,
    borderRadius: 7,
    borderWidth: 2,
    borderColor: "#0f172a",
    transform: [{ translateX: -7 }],
  },
  launchTiltTicks: {
    flexDirection: "row",
    justifyContent: "space-between",
  },
  launchTiltTickText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "600",
  },
  slamScoreRow: {
    marginTop: 12,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  slamScoreLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  slamScoreValue: {
    fontSize: 18,
    fontWeight: "800",
  },
  slamScoreTrack: {
    marginTop: 8,
    height: 8,
    borderRadius: 999,
    backgroundColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
  },
  slamScoreFill: {
    height: "100%",
    borderRadius: 999,
  },
  slamBreakdown: {
    marginTop: 10,
    gap: 6,
  },
  slamBreakdownRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  slamFormula: {
    marginTop: 10,
    color: "rgba(226, 232, 240, 0.55)",
    fontSize: 10,
    lineHeight: 14,
  },
  pitchRankStatus: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "600",
  },
  pitchRankList: {
    marginTop: 6,
    gap: 8,
  },
  pitchRankRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  pitchRankIndex: {
    width: 22,
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "700",
  },
  pitchRankLabel: {
    flex: 1,
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
  },
  pitchRankValue: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  placeholderText: {
    marginTop: 6,
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 12,
    fontWeight: "600",
  },
  gameLogFilters: {
    marginTop: 12,
    gap: 12,
  },
  gameLogFilterRow: {
    gap: 8,
  },
  gameLogDifficultyRow: {
    flexDirection: "row",
    gap: 6,
    paddingVertical: 2,
  },
  gameLogBallparkInput: {
    height: 40,
  },
  gameLogMetaRow: {
    marginTop: 10,
  },
  gameLogMetaText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "600",
  },
  gameLogTable: {
    minWidth: 620,
  },
  gameLogHeaderRow: {
    flexDirection: "row",
    paddingHorizontal: 10,
    paddingVertical: 8,
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    borderTopLeftRadius: 12,
    borderTopRightRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  gameLogHeaderCell: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  gameLogBody: {
    maxHeight: 320,
    borderWidth: 1,
    borderTopWidth: 0,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderBottomLeftRadius: 12,
    borderBottomRightRadius: 12,
    backgroundColor: "rgba(2, 6, 23, 0.55)",
  },
  gameLogRow: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.15)",
  },
  gameLogRowMain: {
    flexDirection: "row",
    alignItems: "center",
  },
  gameLogCell: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "600",
    paddingRight: 6,
  },
  gameLogCellDate: {
    width: 90,
  },
  gameLogCellResult: {
    width: 60,
  },
  gameLogCellScore: {
    width: 70,
  },
  gameLogCellOpponent: {
    width: 150,
  },
  gameLogCellDifficulty: {
    width: 110,
  },
  gameLogCellPark: {
    width: 140,
  },
  gameLogResultPill: {
    alignSelf: "flex-start",
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 999,
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  gameLogResultWin: {
    backgroundColor: "rgba(34, 197, 94, 0.16)",
    color: "#22c55e",
  },
  gameLogResultLoss: {
    backgroundColor: "rgba(248, 113, 113, 0.16)",
    color: "#f87171",
  },
  gameLogResultNeutral: {
    backgroundColor: "rgba(148, 163, 184, 0.2)",
    color: "rgba(226, 232, 240, 0.65)",
  },
  gameLogRowSummary: {
    marginTop: 6,
    color: "rgba(226, 232, 240, 0.65)",
    fontSize: 10,
    lineHeight: 14,
  },
  gameLogStatusRow: {
    paddingVertical: 16,
    paddingHorizontal: 10,
  },
  gameLogStatusText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 12,
    fontWeight: "600",
    textAlign: "center",
  },
  cardsFilters: {
    marginTop: 12,
    gap: 8,
  },
  cardsFilterRow: {
    flexDirection: "row",
    alignItems: "flex-end",
    gap: 8,
  },
  cardsFilterInput: {
    flex: 1,
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  cardsFilterInputField: {
    flex: 1,
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
  },
  cardsMinPaInput: {
    width: 86,
    gap: 6,
  },
  cardsMinPaLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.3,
  },
  cardsMinPaField: {
    height: 36,
    borderRadius: 10,
    paddingHorizontal: 8,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
    textAlign: "right",
  },
  cardsMetaRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  cardsMetaText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "600",
  },
  cardsHintText: {
    color: "rgba(226, 232, 240, 0.45)",
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  cardsTableWrap: {
    flexDirection: "row",
    width: "100%",
  },
  cardsHeaderRow: {
    flexDirection: "row",
    paddingHorizontal: 2,
    paddingVertical: 8,
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    borderTopLeftRadius: 12,
    borderTopRightRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  cardsHeaderFrozen: {
    borderRightWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderTopRightRadius: 0,
  },
  cardsHeaderScroll: {
    borderTopLeftRadius: 0,
    borderLeftWidth: 0,
  },
  cardsHeaderCell: {
    paddingRight: 0,
  },
  cardsHeaderCellInner: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 0,
  },
  cardsHeaderCellText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  cardsHeaderCellTextActive: {
    color: theme.colors.text,
  },
  cardsBody: {
    maxHeight: 360,
    borderWidth: 1,
    borderTopWidth: 0,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderBottomLeftRadius: 12,
    borderBottomRightRadius: 12,
    backgroundColor: "rgba(2, 6, 23, 0.55)",
  },
  cardsBodyRowWrap: {
    flexDirection: "row",
  },
  cardsFrozenColumn: {
    borderRightWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  cardsRow: {
    flexDirection: "row",
    paddingHorizontal: 2,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.15)",
  },
  cardsRowAlt: {
    backgroundColor: "rgba(15, 23, 42, 0.35)",
  },
  cardsCell: {
    paddingRight: 0,
    justifyContent: "center",
  },
  cardsCellRight: {
    alignItems: "flex-end",
  },
  cardsCellText: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "600",
  },
  cardsCellTextRight: {
    textAlign: "right",
  },
  cardsStatusRow: {
    paddingVertical: 16,
    paddingHorizontal: 10,
  },
  cardsStatusText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 12,
    fontWeight: "600",
    textAlign: "center",
  },
  sectionHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  sectionHeader: {
    gap: 4,
    flex: 1,
  },
  sectionSubheader: {
    color: "rgba(226, 232, 240, 0.55)",
    fontSize: 11,
    fontWeight: "600",
  },
  sectionToggle: {
    flexDirection: "row",
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderRadius: 999,
    padding: 2,
    gap: 2,
    alignSelf: "flex-start",
  },
  sectionToggleButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },
  sectionToggleButtonActive: {
    backgroundColor: "#fbbf24",
  },
  sectionToggleText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
  },
  sectionToggleTextActive: {
    color: "#0f172a",
  },
  statsHeaderActions: {
    flexDirection: "column",
    alignItems: "flex-end",
    gap: 6,
  },
  filtersHeaderRow: {
    marginTop: 10,
    marginBottom: 6,
    width: "100%",
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    overflow: "visible",
  },
  filtersLeft: {
    flexDirection: "row",
    alignItems: "flex-start",
    flexWrap: "nowrap",
    gap: 8,
    flex: 1,
    minWidth: 0,
    overflow: "visible",
  },
  filtersActions: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  resetFiltersButton: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 999,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
  },
  resetFiltersText: {
    color: "rgba(226, 232, 240, 0.85)",
    fontSize: 11,
    fontWeight: "700",
  },
  hitterFilterColumn: {
    flex: 1,
    minWidth: 0,
    position: "relative",
    zIndex: 5,
  },
  hitterFilterToggle: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
    justifyContent: "space-between",
  },
  hitterFilterToggleText: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
    flexShrink: 1,
  },
  hitterFilterToggleIcon: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
  },
  hitterFilterMenu: {
    position: "absolute",
    top: 34,
    left: 0,
    right: 0,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
    zIndex: 20,
    elevation: 10,
  },
  wideFilterMenuLeft: {
    left: 0,
    right: -80,
    minWidth: 240,
    maxWidth: 360,
  },
  wideFilterMenuRight: {
    left: -80,
    right: 0,
    minWidth: 240,
    maxWidth: 360,
  },
  hitterFilterItem: {
    paddingHorizontal: 12,
    paddingVertical: 8,
  },
  hitterFilterItemActive: {
    backgroundColor: "rgba(251, 191, 36, 0.15)",
  },
  hitterFilterItemText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 11,
    fontWeight: "600",
  },
  hitterFilterItemTextActive: {
    color: theme.colors.text,
    fontWeight: "700",
  },
  pitcherActiveRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.2)",
    backgroundColor: "rgba(15, 23, 42, 0.7)",
  },
  pitcherActiveText: {
    flex: 1,
    marginRight: 8,
  },
  pitcherActiveLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "600",
  },
  pitcherActiveName: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
    marginTop: 2,
  },
  pitcherClearButton: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
  },
  pitcherClearText: {
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 10,
    fontWeight: "700",
  },
  pitcherSearchBlock: {
    paddingHorizontal: 10,
    paddingTop: 10,
    paddingBottom: 12,
  },
  pitcherSearchInput: {
    height: 36,
    borderRadius: 10,
    paddingHorizontal: 10,
    backgroundColor: "rgba(30, 41, 59, 0.75)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
    color: theme.colors.text,
    fontSize: 12,
  },
  pitcherSearchResults: {
    marginTop: 8,
    gap: 6,
  },
  pitcherSearchRow: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 10,
    backgroundColor: "rgba(30, 41, 59, 0.65)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.16)",
  },
  pitcherSearchText: {
    flex: 1,
    gap: 2,
  },
  pitcherSearchName: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  pitcherSearchMeta: {
    color: theme.colors.muted,
    fontSize: 10,
  },
  pitcherSearchStatus: {
    color: theme.colors.muted,
    fontSize: 11,
    paddingVertical: 4,
    textAlign: "center",
  },
  pitcherSearchDivider: {
    height: 1,
    backgroundColor: "rgba(148, 163, 184, 0.2)",
  },
  advancedFilterButton: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
  },
  advancedFilterButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.8)",
  },
  analyticsContentRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    justifyContent: "space-between",
    gap: 12,
  },
  analyticsSummary: {
    flex: 1,
    alignSelf: "stretch",
    justifyContent: "center",
    gap: 4,
  },
  summaryRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 6,
    paddingVertical: 4,
    borderRadius: 6,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
    backgroundColor: "rgba(15, 23, 42, 0.55)",
  },
  summaryRowPressed: {
    borderColor: "rgba(251, 191, 36, 0.8)",
    backgroundColor: "rgba(251, 191, 36, 0.08)",
  },
  summaryLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "600",
  },
  summaryValue: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  statsRows: {
    marginTop: 10,
    gap: 10,
  },
  statsRowBlock: {
    gap: 6,
  },
  statsRowTitle: {
    color: "rgba(226, 232, 240, 0.65)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  statsRowScroll: {
    gap: 8,
    paddingRight: 6,
  },
  statsChip: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 10,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    minWidth: 70,
  },
  statsChipLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 9,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  statsChipValue: {
    marginTop: 4,
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  filterOverlay: {
    flex: 1,
    backgroundColor: "rgba(2, 6, 23, 0.45)",
    justifyContent: "flex-start",
    paddingTop: 140,
    paddingHorizontal: 16,
  },
  helpOverlay: {
    flex: 1,
    backgroundColor: "rgba(2, 6, 23, 0.5)",
    justifyContent: "center",
    paddingHorizontal: 16,
  },
  helpPanel: {
    backgroundColor: "rgba(2, 6, 23, 0.96)",
    borderRadius: 18,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    padding: 16,
  },
  helpHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  helpTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  helpCloseButton: {
    padding: 4,
  },
  helpDescription: {
    marginTop: 10,
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 12,
    lineHeight: 18,
  },
  helpSectionLabel: {
    marginTop: 12,
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  helpFormula: {
    marginTop: 6,
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  filterPanel: {
    backgroundColor: "rgba(2, 6, 23, 0.96)",
    borderRadius: 18,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    padding: 16,
  },
  filterHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  filterTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  filterCloseButton: {
    padding: 4,
  },
  filterField: {
    marginTop: 14,
    gap: 8,
  },
  filterLabel: {
    color: "rgba(226, 232, 240, 0.75)",
    fontSize: 12,
    fontWeight: "700",
  },
  filterHint: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 11,
  },
  pitchTypeRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  pitchTypeChip: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
  },
  pitchTypeChipActive: {
    borderColor: "rgba(251, 191, 36, 0.8)",
    backgroundColor: "rgba(251, 191, 36, 0.15)",
  },
  pitchTypeChipText: {
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 11,
    fontWeight: "600",
  },
  pitchTypeChipTextActive: {
    color: theme.colors.text,
    fontWeight: "700",
  },
  filterInputRow: {
    flexDirection: "row",
    gap: 10,
  },
  filterInputWrap: {
    flex: 1,
    gap: 6,
  },
  filterInputLabel: {
    color: "rgba(226, 232, 240, 0.55)",
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  filterInput: {
    height: 40,
    borderRadius: 10,
    paddingHorizontal: 10,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "600",
  },
  filterToggle: {
    flexDirection: "row",
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderRadius: 999,
    padding: 2,
    gap: 2,
    alignSelf: "flex-start",
  },
  filterToggleButton: {
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 999,
  },
  filterToggleButtonActive: {
    backgroundColor: "#fbbf24",
  },
  filterToggleText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 11,
    fontWeight: "600",
  },
  filterToggleTextActive: {
    color: "#0f172a",
  },
  sectionTab: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    alignItems: "center",
  },
  sectionTabActive: {
    backgroundColor: "#fbbf24",
    borderColor: "rgba(251, 191, 36, 0.8)",
  },
  sectionTabText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 12,
    fontWeight: "700",
  },
  sectionTabTextActive: {
    color: "#0f172a",
  },
  searchOverlay: {
    flex: 1,
    backgroundColor: "rgba(2, 6, 23, 0.45)",
    justifyContent: "flex-start",
    paddingTop: 120,
    paddingHorizontal: 16,
  },
  searchPanel: {
    backgroundColor: "rgba(2, 6, 23, 0.96)",
    borderRadius: 20,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    padding: 16,
    maxHeight: "70%",
  },
  searchTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
    marginBottom: 10,
  },
  searchInput: {
    height: 44,
    borderRadius: 12,
    paddingHorizontal: 12,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    color: theme.colors.text,
    fontSize: 14,
  },
  searchResults: {
    marginTop: 12,
  },
  searchResultsContent: {
    paddingBottom: 12,
    gap: 10,
  },
  searchRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 14,
    backgroundColor: "rgba(15, 23, 42, 0.85)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.14)",
  },
  searchAvatarWrap: {
    marginRight: 12,
  },
  searchText: {
    flex: 1,
  },
  searchName: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  searchMeta: {
    color: theme.colors.muted,
    fontSize: 11,
    marginTop: 2,
  },
  searchStatus: {
    color: theme.colors.muted,
    fontSize: 13,
    paddingVertical: 8,
    textAlign: "center",
  },
});
