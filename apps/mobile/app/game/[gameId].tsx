import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useLocalSearchParams, useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import { ApiError, apiGet, apiGetAuth } from "../../src/lib/api";
import { theme } from "../../src/theme/colors";

type GameEvent = {
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

type HalfInningSummary = {
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

type PlateAppearance = {
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

type BatterBoxscore = {
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

type PitcherBoxscore = {
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

type GameBundle = {
  events: GameEvent[];
  half_innings: HalfInningSummary[];
  plate_appearances: PlateAppearance[];
  batter_boxscores: BatterBoxscore[];
  pitcher_boxscores: PitcherBoxscore[];
};

type HalfInningSection = {
  key: string;
  inning: number;
  isHomeBatting: boolean;
  summary: HalfInningSummary | null;
  data: GameEvent[];
};

const STICKY_HEADER_HEIGHT = 56;

const getParamString = (value: string | string[] | undefined) => {
  if (!value) return "";
  return Array.isArray(value) ? value[0] ?? "" : value;
};

export default function GameLogScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const gameId = getParamString(params.gameId);
  const date = getParamString(params.date);
  const opponent = getParamString(params.opponent);
  const location = getParamString(params.location) || "vs";
  const result = getParamString(params.result);
  const scoreFor = getParamString(params.scoreFor);
  const scoreAgainst = getParamString(params.scoreAgainst);
  const difficulty = getParamString(params.difficulty);
  const ballpark = getParamString(params.ballpark);
  const homeTeam = getParamString(params.homeTeam);
  const awayTeam = getParamString(params.awayTeam);
  const viewUsername = getParamString(params.viewUsername);
  const viewUserId = getParamString(params.viewUserId);

  const [events, setEvents] = useState<GameEvent[]>([]);
  const [halfInnings, setHalfInnings] = useState<HalfInningSummary[]>([]);
  const [plateAppearances, setPlateAppearances] = useState<PlateAppearance[]>([]);
  const [batterBoxscores, setBatterBoxscores] = useState<BatterBoxscore[]>([]);
  const [pitcherBoxscores, setPitcherBoxscores] = useState<PitcherBoxscore[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [boxscoreTeam, setBoxscoreTeam] = useState<"home" | "away">("home");
  const [stickyKey, setStickyKey] = useState<string | null>(null);
  const sectionOffsetsRef = useRef<Record<string, number>>({});
  const lastStickyKeyRef = useRef<string | null>(null);

  useEffect(() => {
    let active = true;

    const loadEvents = async () => {
      if (!gameId) return;
      setLoading(true);
      setError(null);
      try {
        const path = viewUsername
          ? `/users/show/${encodeURIComponent(viewUsername)}/game-bundle/${gameId}`
          : viewUserId
            ? `/users/${viewUserId}/show/game-bundle/${gameId}`
            : `/users/me/show/game-bundle/${gameId}`;
        const data = viewUsername || viewUserId
          ? await apiGet<GameBundle>(path)
          : await apiGetAuth<GameBundle>(path);
        const parsedEvents = Array.isArray(data?.events) ? data.events : [];
        const parsedHalfInnings = Array.isArray(data?.half_innings)
          ? data.half_innings
          : [];
        const parsedPas = Array.isArray(data?.plate_appearances)
          ? data.plate_appearances
          : [];
        const parsedBatters = Array.isArray(data?.batter_boxscores)
          ? data.batter_boxscores
          : [];
        const parsedPitchers = Array.isArray(data?.pitcher_boxscores)
          ? data.pitcher_boxscores
          : [];
        parsedEvents.sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
        if (!active) return;
        setEvents(parsedEvents);
        setHalfInnings(parsedHalfInnings);
        setPlateAppearances(parsedPas);
        setBatterBoxscores(parsedBatters);
        setPitcherBoxscores(parsedPitchers);
      } catch (err) {
        if (!active) return;
        setEvents([]);
        setHalfInnings([]);
        setPlateAppearances([]);
        setBatterBoxscores([]);
        setPitcherBoxscores([]);
        if (err instanceof ApiError && err.status === 404) {
          setError("Game events not available.");
        } else {
          setError("Unable to load game events.");
        }
      } finally {
        if (active) setLoading(false);
      }
    };

    loadEvents();

    return () => {
      active = false;
    };
  }, [gameId, viewUsername, viewUserId]);

  const matchup = opponent ? `${location} ${opponent}` : "Game";
  const scoreLine =
    scoreFor && scoreAgainst ? `${scoreFor}-${scoreAgainst}` : "—";
  const dateLabel = formatDateLong(date);
  const paBySeq = useMemo(() => {
    const map = new Map<number, PlateAppearance>();
    plateAppearances.forEach((pa) => {
      if (typeof pa.event_seq === "number") {
        map.set(pa.event_seq, pa);
      }
    });
    return map;
  }, [plateAppearances]);
  const summaryByHalf = useMemo(() => {
    const map = new Map<string, HalfInningSummary>();
    halfInnings.forEach((half) => {
      map.set(halfKey(half.inning, half.is_home_batting), half);
    });
    return map;
  }, [halfInnings]);
  const sections = useMemo<HalfInningSection[]>(() => {
    const sorted = [...events].sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
    const output: HalfInningSection[] = [];
    let currentKey: string | null = null;
    let currentSection: HalfInningSection | null = null;

    sorted.forEach((event) => {
      const inning = event.inning ?? 0;
      const isHome = Boolean(event.is_home_batting);
      const key = halfKey(inning, isHome);
      if (key !== currentKey) {
        currentKey = key;
        currentSection = {
          key,
          inning,
          isHomeBatting: isHome,
          summary: summaryByHalf.get(key) ?? null,
          data: [],
        };
        output.push(currentSection);
      }
      currentSection?.data.push(event);
    });

    return output;
  }, [events, summaryByHalf]);
  const stickySection = useMemo(() => {
    if (!sections.length) return null;
    if (!stickyKey) return sections[0];
    return sections.find((section) => section.key === stickyKey) ?? sections[0];
  }, [sections, stickyKey]);
  const homeBatters = useMemo(
    () =>
      batterBoxscores
        .filter((row) => row.is_home)
        .sort((a, b) => a.appearance_idx - b.appearance_idx),
    [batterBoxscores]
  );
  const awayBatters = useMemo(
    () =>
      batterBoxscores
        .filter((row) => !row.is_home)
        .sort((a, b) => a.appearance_idx - b.appearance_idx),
    [batterBoxscores]
  );
  const homePitchers = useMemo(
    () =>
      pitcherBoxscores
        .filter((row) => row.is_home)
        .sort((a, b) => a.appearance_idx - b.appearance_idx),
    [pitcherBoxscores]
  );
  const awayPitchers = useMemo(
    () =>
      pitcherBoxscores
        .filter((row) => !row.is_home)
        .sort((a, b) => a.appearance_idx - b.appearance_idx),
    [pitcherBoxscores]
  );
  const activeBatters = boxscoreTeam === "home" ? homeBatters : awayBatters;
  const activePitchers = boxscoreTeam === "home" ? homePitchers : awayPitchers;

  useEffect(() => {
    if (boxscoreTeam === "home" && homeBatters.length === 0 && awayBatters.length > 0) {
      setBoxscoreTeam("away");
    }
  }, [boxscoreTeam, homeBatters.length, awayBatters.length]);

  const header = useMemo(() => {
    return (
      <View style={styles.header}>
        <View style={styles.headerTopRow}>
          <Text style={styles.headerTitle}>Game Log</Text>
          <View style={styles.resultPill}>
            <Text style={styles.resultPillText}>{result || "—"}</Text>
          </View>
        </View>
        <Text style={styles.headerMatchup}>{matchup}</Text>
        <View style={styles.headerMetaRow}>
          <View style={styles.metaItem}>
            <Text style={styles.metaLabel}>Date</Text>
            <Text style={styles.metaValue}>{dateLabel}</Text>
          </View>
          <View style={styles.metaItem}>
            <Text style={styles.metaLabel}>Score</Text>
            <Text style={styles.metaValue}>{scoreLine}</Text>
          </View>
        </View>
        <View style={styles.headerMetaRow}>
          <View style={styles.metaItem}>
            <Text style={styles.metaLabel}>Difficulty</Text>
            <Text style={styles.metaValue}>{difficulty || "—"}</Text>
          </View>
          <View style={styles.metaItem}>
            <Text style={styles.metaLabel}>Ballpark</Text>
            <Text style={styles.metaValue} numberOfLines={1}>
              {ballpark || "—"}
            </Text>
          </View>
        </View>
        {loading ? (
          <View style={styles.loadingRow}>
            <ActivityIndicator size="small" color={theme.colors.text} />
            <Text style={styles.loadingText}>Loading events...</Text>
          </View>
        ) : null}
        {error ? <Text style={styles.errorText}>{error}</Text> : null}
      </View>
    );
  }, [
    matchup,
    result,
    dateLabel,
    scoreLine,
    difficulty,
    ballpark,
    loading,
    error,
  ]);

  return (
    <View style={styles.container}>
      <SafeAreaView style={styles.safeArea} edges={["top"]}>
        <View style={styles.navBar}>
          <TouchableOpacity onPress={() => router.back()} style={styles.backBtn}>
            <Ionicons name="arrow-back" size={22} color={theme.colors.text} />
            <Text style={styles.backText}>Back to Stats</Text>
          </TouchableOpacity>
        </View>

        <ScrollView
          style={styles.pageScroll}
          contentContainerStyle={styles.pageContent}
          showsVerticalScrollIndicator={false}
        >
          {header}

          <View style={styles.eventsContainer}>
            <View style={styles.sectionHeaderRow}>
              <Text style={styles.sectionTitle}>Event Log</Text>
            </View>
            {stickySection ? (
              <View style={styles.stickyHeader}>
                <View style={styles.halfHeaderTop}>
                  <Text style={styles.halfHeaderTitle}>
                    {formatHalfInning(stickySection.inning, stickySection.isHomeBatting)}
                  </Text>
                  <Text style={styles.halfHeaderTeam}>
                    {stickySection.isHomeBatting ? homeTeam || "Home" : awayTeam || "Away"}
                  </Text>
                </View>
                {stickySection.summary ? (
                  <Text style={styles.halfHeaderMeta}>
                    R {stickySection.summary.runs} • H {stickySection.summary.hits} • BB{" "}
                    {stickySection.summary.walks} • E {stickySection.summary.errors} • P{" "}
                    {stickySection.summary.pitches} • LOB{" "}
                    {stickySection.summary.runners_left_on}
                  </Text>
                ) : null}
              </View>
            ) : null}
            <ScrollView
              style={styles.eventsList}
              contentContainerStyle={styles.eventsListContent}
              showsVerticalScrollIndicator={false}
              nestedScrollEnabled
              scrollEventThrottle={16}
              onScroll={(event) => {
                const y = event.nativeEvent.contentOffset.y;
                const threshold = y + STICKY_HEADER_HEIGHT;
                const offsets = Object.entries(sectionOffsetsRef.current)
                  .map(([key, value]) => ({ key, value }))
                  .sort((a, b) => a.value - b.value);
                let nextKey: string | null = null;
                for (const offset of offsets) {
                  if (offset.value <= threshold) {
                    nextKey = offset.key;
                  } else {
                    break;
                  }
                }
                if (nextKey && nextKey !== lastStickyKeyRef.current) {
                  lastStickyKeyRef.current = nextKey;
                  setStickyKey(nextKey);
                }
              }}
            >
              {sections.length === 0 && !loading && !error ? (
                <View style={styles.emptyState}>
                  <Text style={styles.emptyText}>No events found.</Text>
                </View>
              ) : null}
              {sections.map((section) => (
                <View
                  key={section.key}
                  onLayout={(e) => {
                    sectionOffsetsRef.current[section.key] = e.nativeEvent.layout.y;
                    if (!stickyKey && !lastStickyKeyRef.current) {
                      lastStickyKeyRef.current = section.key;
                      setStickyKey(section.key);
                    }
                  }}
                >
                  <View style={styles.halfHeader}>
                    <View style={styles.halfHeaderTop}>
                      <Text style={styles.halfHeaderTitle}>
                        {formatHalfInning(section.inning, section.isHomeBatting)}
                      </Text>
                      <Text style={styles.halfHeaderTeam}>
                        {section.isHomeBatting ? homeTeam || "Home" : awayTeam || "Away"}
                      </Text>
                    </View>
                    {section.summary ? (
                      <Text style={styles.halfHeaderMeta}>
                        R {section.summary.runs} • H {section.summary.hits} • BB{" "}
                        {section.summary.walks} • E {section.summary.errors} • P{" "}
                        {section.summary.pitches} • LOB {section.summary.runners_left_on}
                      </Text>
                    ) : null}
                  </View>
                  {section.data.map((item) => {
                    const pa = item.seq ? paBySeq.get(item.seq) : undefined;
                    const battingTeam = item.is_home_batting
                      ? homeTeam || "Home"
                      : awayTeam || "Away";
                    const resultLabel = pa?.result
                      ? formatResultLabel(pa.result)
                      : formatEventType(item.event_type);
                    const resultTone = getResultTone(pa);
                    const resultStyle =
                      resultTone === "resultPillHit"
                        ? styles.resultPillHit
                        : resultTone === "resultPillOut"
                          ? styles.resultPillOut
                          : styles.resultPillNeutral;
                    return (
                      <View key={item.seq} style={styles.eventRow}>
                        <View style={styles.eventRowTop}>
                          <Text style={styles.eventBattingText}>{battingTeam} batting</Text>
                          <Text style={styles.eventMetaText}>
                            {formatScore(item.away_score_after, item.home_score_after)}
                          </Text>
                          <Text style={styles.eventMetaText}>
                            {formatOuts(item.outs_after)}
                          </Text>
                          <View style={[styles.resultPillBase, resultStyle]}>
                            <Text style={styles.eventResultText}>{resultLabel}</Text>
                          </View>
                        </View>
                        <View style={styles.eventPlayersRow}>
                          <Text style={styles.eventPlayerText}>
                            Batter: {pa?.batter_name_raw || "—"}
                          </Text>
                          <Text style={styles.eventPlayerText}>
                            Pitcher: {pa?.pitcher_name_raw || "—"}
                          </Text>
                        </View>
                        <View style={styles.eventBasesRow}>
                          <BaseIndicator label="1B" active={Boolean(item.pre_on_1b)} />
                          <BaseIndicator label="2B" active={Boolean(item.pre_on_2b)} />
                          <BaseIndicator label="3B" active={Boolean(item.pre_on_3b)} />
                        </View>
                        <Text style={styles.eventText}>
                          {item.event_text || "Event recorded."}
                        </Text>
                      </View>
                    );
                  })}
                </View>
              ))}
            </ScrollView>
          </View>

          <View style={styles.boxscoreContainer}>
            <View style={styles.boxscoreHeaderRow}>
              <Text style={styles.sectionTitle}>Boxscore</Text>
              <View style={styles.boxscoreToggle}>
                {([
                  { key: "home", label: homeTeam || "Home" },
                  { key: "away", label: awayTeam || "Away" },
                ] as const).map((option) => {
                  const active = boxscoreTeam === option.key;
                  return (
                    <TouchableOpacity
                      key={option.key}
                      style={[styles.boxscoreToggleButton, active && styles.boxscoreToggleActive]}
                      onPress={() => setBoxscoreTeam(option.key)}
                    >
                      <Text
                        style={[
                          styles.boxscoreToggleText,
                          active && styles.boxscoreToggleTextActive,
                        ]}
                        numberOfLines={1}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            </View>

            <Text style={styles.boxscoreSectionTitle}>Batters</Text>
            <ScrollView horizontal showsHorizontalScrollIndicator={false}>
              <View style={styles.boxscoreTable}>
                <View style={styles.boxscoreRowHeader}>
                  <Text style={[styles.boxscoreHeaderCell, styles.boxscoreCellName]}>
                    Player
                  </Text>
                  {[
                    "AB",
                    "R",
                    "H",
                    "RBI",
                    "BB",
                    "SO",
                    "2B",
                    "3B",
                    "HR",
                    "AVG",
                    "OBP",
                    "SLG",
                    "OPS",
                    "SB",
                    "CS",
                    "SB%",
                    "E",
                    "POS",
                    "INN",
                  ].map((label) => (
                    <Text key={label} style={styles.boxscoreHeaderCell}>
                      {label}
                    </Text>
                  ))}
                </View>
                {activeBatters.map((row) => {
                  const stats = computeBattingStats(row);
                  return (
                    <View key={`${row.game_id}:${row.appearance_idx}`} style={styles.boxscoreRow}>
                      <Text
                        style={[styles.boxscoreCell, styles.boxscoreCellName]}
                        numberOfLines={1}
                      >
                        {row.player_name_raw}
                      </Text>
                      <Text style={styles.boxscoreCell}>{row.ab}</Text>
                      <Text style={styles.boxscoreCell}>{row.r}</Text>
                      <Text style={styles.boxscoreCell}>{row.h}</Text>
                      <Text style={styles.boxscoreCell}>{row.rbi}</Text>
                      <Text style={styles.boxscoreCell}>{row.bb}</Text>
                      <Text style={styles.boxscoreCell}>{row.so}</Text>
                      <Text style={styles.boxscoreCell}>{row.doubles}</Text>
                      <Text style={styles.boxscoreCell}>{row.triples}</Text>
                      <Text style={styles.boxscoreCell}>{row.hr}</Text>
                      <Text style={styles.boxscoreCell}>{formatRate(stats.avg)}</Text>
                      <Text style={styles.boxscoreCell}>{formatRate(stats.obp)}</Text>
                      <Text style={styles.boxscoreCell}>{formatRate(stats.slg)}</Text>
                      <Text style={styles.boxscoreCell}>{formatRate(stats.ops)}</Text>
                      <Text style={styles.boxscoreCell}>{row.sb}</Text>
                      <Text style={styles.boxscoreCell}>{row.cs}</Text>
                      <Text style={styles.boxscoreCell}>{formatRate(stats.sbPct)}</Text>
                      <Text style={styles.boxscoreCell}>{row.e}</Text>
                      <Text style={styles.boxscoreCell}>{row.pos}</Text>
                      <Text style={styles.boxscoreCell}>{row.innings}</Text>
                    </View>
                  );
                })}
              </View>
            </ScrollView>

            <Text style={styles.boxscoreSectionTitle}>Pitchers</Text>
            <ScrollView horizontal showsHorizontalScrollIndicator={false}>
              <View style={styles.boxscoreTable}>
                <View style={styles.boxscoreRowHeader}>
                  <Text style={[styles.boxscoreHeaderCell, styles.boxscoreCellName]}>
                    Player
                  </Text>
                  {[
                    "IP",
                    "H",
                    "R",
                    "ER",
                    "BB",
                    "SO",
                    "ERA",
                    "WP",
                    "W",
                    "L",
                    "SV",
                    "HLD",
                  ].map((label) => (
                    <Text key={label} style={styles.boxscoreHeaderCell}>
                      {label}
                    </Text>
                  ))}
                </View>
                {activePitchers.map((row) => (
                  <View key={`${row.game_id}:${row.appearance_idx}`} style={styles.boxscoreRow}>
                    <Text
                      style={[styles.boxscoreCell, styles.boxscoreCellName]}
                      numberOfLines={1}
                    >
                      {row.player_name_raw}
                    </Text>
                    <Text style={styles.boxscoreCell}>{row.ip_raw}</Text>
                    <Text style={styles.boxscoreCell}>{row.h}</Text>
                    <Text style={styles.boxscoreCell}>{row.r}</Text>
                    <Text style={styles.boxscoreCell}>{row.er}</Text>
                    <Text style={styles.boxscoreCell}>{row.bb}</Text>
                    <Text style={styles.boxscoreCell}>{row.so}</Text>
                    <Text style={styles.boxscoreCell}>{formatEra(row.era)}</Text>
                    <Text style={styles.boxscoreCell}>{row.wp}</Text>
                    <Text style={styles.boxscoreCell}>{row.win}</Text>
                    <Text style={styles.boxscoreCell}>{row.loss}</Text>
                    <Text style={styles.boxscoreCell}>{row.save}</Text>
                    <Text style={styles.boxscoreCell}>{row.hold}</Text>
                  </View>
                ))}
              </View>
            </ScrollView>
          </View>
        </ScrollView>
      </SafeAreaView>
    </View>
  );
}

function formatHalfInning(inning?: number, isHomeBatting?: boolean) {
  if (!inning) return "—";
  return `${isHomeBatting ? "Bot" : "Top"} ${inning}`;
}

function formatScore(away?: number, home?: number) {
  if (away === undefined || home === undefined) return "—";
  return `${away}-${home}`;
}

function formatOuts(outs?: number) {
  if (outs === undefined || outs === null) return "—";
  return `${outs} out${outs === 1 ? "" : "s"}`;
}

function formatEventType(value?: string) {
  if (!value) return "Event";
  return value
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatDateLong(value?: string) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatResultLabel(value: string) {
  return value
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getResultTone(pa?: PlateAppearance) {
  if (!pa) return "resultPillNeutral";
  if (pa.is_out) return "resultPillOut";
  if (pa.result) return "resultPillHit";
  return "resultPillNeutral";
}

function halfKey(inning?: number, isHomeBatting?: boolean) {
  if (!inning) return "unknown";
  return `${inning}:${isHomeBatting ? "H" : "A"}`;
}

function computeBattingStats(row: BatterBoxscore) {
  const singles = Math.max(0, row.h - row.doubles - row.triples - row.hr);
  const totalBases = singles + row.doubles * 2 + row.triples * 3 + row.hr * 4;
  const avg = row.ab > 0 ? row.h / row.ab : null;
  const obpDen = row.ab + row.bb + row.hbp + row.sf;
  const obp = obpDen > 0 ? (row.h + row.bb + row.hbp) / obpDen : null;
  const slg = row.ab > 0 ? totalBases / row.ab : null;
  const ops = obp !== null && slg !== null ? obp + slg : null;
  const sbDen = row.sb + row.cs;
  const sbPct = sbDen > 0 ? row.sb / sbDen : null;
  return { avg, obp, slg, ops, sbPct };
}

function formatRate(value: number | null) {
  if (value === null || Number.isNaN(value)) return "—";
  const fixed = value.toFixed(3);
  return value < 1 ? fixed.replace(/^0/, "") : fixed;
}

function formatEra(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(2);
}

const BaseIndicator = ({ label, active }: { label: string; active: boolean }) => {
  return (
    <View style={[styles.baseIndicator, active && styles.baseIndicatorActive]}>
      <Text style={[styles.baseIndicatorText, active && styles.baseIndicatorTextActive]}>
        {label}
      </Text>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  safeArea: {
    flex: 1,
  },
  navBar: {
    paddingHorizontal: 16,
    paddingBottom: 8,
  },
  backBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  backText: {
    color: theme.colors.text,
    fontSize: 15,
    fontWeight: "600",
  },
  pageContent: {
    paddingHorizontal: 16,
    paddingBottom: 32,
    gap: 16,
  },
  pageScroll: {
    flex: 1,
  },
  sectionHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 12,
    paddingTop: 12,
    paddingBottom: 6,
  },
  sectionTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  eventsContainer: {
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    overflow: "hidden",
    height: 420,
  },
  eventsList: {
    flex: 1,
  },
  eventsListContent: {
    paddingBottom: 0,
    paddingHorizontal: 0,
    paddingTop: STICKY_HEADER_HEIGHT,
  },
  stickyHeader: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    zIndex: 10,
    paddingHorizontal: 12,
    paddingVertical: 10,
    backgroundColor: "rgba(2, 6, 23, 0.95)",
    borderBottomWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
  },
  header: {
    padding: 16,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    marginBottom: 16,
  },
  headerTopRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  headerTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  resultPill: {
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
    backgroundColor: "rgba(251, 191, 36, 0.2)",
  },
  resultPillText: {
    color: "#fbbf24",
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  headerMatchup: {
    marginTop: 6,
    color: "rgba(226, 232, 240, 0.85)",
    fontSize: 14,
    fontWeight: "700",
  },
  headerMetaRow: {
    marginTop: 10,
    flexDirection: "row",
    gap: 16,
  },
  metaItem: {
    flex: 1,
    gap: 4,
  },
  metaLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  metaValue: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  loadingRow: {
    marginTop: 12,
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  loadingText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 12,
    fontWeight: "600",
  },
  errorText: {
    marginTop: 10,
    color: theme.colors.error,
    fontSize: 12,
    fontWeight: "600",
  },
  boxscoreContainer: {
    padding: 14,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    backgroundColor: "rgba(15, 23, 42, 0.92)",
  },
  boxscoreHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "flex-start",
    gap: 12,
  },
  boxscoreToggle: {
    flexDirection: "row",
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderRadius: 999,
    padding: 2,
    gap: 4,
    alignSelf: "flex-start",
  },
  boxscoreToggleButton: {
    paddingHorizontal: 12,
    paddingVertical: 4,
    borderRadius: 999,
  },
  boxscoreToggleActive: {
    backgroundColor: "#fbbf24",
  },
  boxscoreToggleText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 11,
    fontWeight: "700",
  },
  boxscoreToggleTextActive: {
    color: "#0f172a",
  },
  boxscoreSectionTitle: {
    marginTop: 14,
    marginBottom: 8,
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 12,
    fontWeight: "800",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  boxscoreTable: {
    minWidth: 760,
  },
  boxscoreRowHeader: {
    flexDirection: "row",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.25)",
    paddingBottom: 6,
    marginBottom: 6,
  },
  boxscoreHeaderCell: {
    width: 48,
    textAlign: "center",
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
  },
  boxscoreRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 6,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.12)",
  },
  boxscoreCell: {
    width: 48,
    textAlign: "center",
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "600",
  },
  boxscoreCellName: {
    width: 110,
    textAlign: "left",
    paddingRight: 4,
  },
  halfHeader: {
    paddingHorizontal: 12,
    paddingVertical: 10,
    backgroundColor: "rgba(2, 6, 23, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
  },
  halfHeaderTop: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  halfHeaderTitle: {
    color: "#fbbf24",
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  halfHeaderTeam: {
    color: "rgba(226, 232, 240, 0.75)",
    fontSize: 11,
    fontWeight: "700",
  },
  halfHeaderMeta: {
    marginTop: 4,
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "600",
  },
  eventRow: {
    paddingHorizontal: 12,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.16)",
    backgroundColor: "rgba(15, 23, 42, 0.78)",
  },
  eventRowTop: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  eventBattingText: {
    color: "rgba(226, 232, 240, 0.9)",
    fontSize: 11,
    fontWeight: "700",
  },
  eventMetaText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "600",
  },
  resultPillBase: {
    marginLeft: "auto",
    paddingHorizontal: 10,
    paddingVertical: 4,
    borderRadius: 999,
  },
  resultPillHit: {
    backgroundColor: "rgba(34, 197, 94, 0.2)",
  },
  resultPillOut: {
    backgroundColor: "rgba(248, 113, 113, 0.2)",
  },
  resultPillNeutral: {
    backgroundColor: "rgba(148, 163, 184, 0.2)",
  },
  eventPlayersRow: {
    marginTop: 8,
    gap: 2,
  },
  eventPlayerText: {
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 11,
    fontWeight: "600",
  },
  eventBasesRow: {
    marginTop: 8,
    flexDirection: "row",
    gap: 6,
  },
  baseIndicator: {
    minWidth: 36,
    alignItems: "center",
    paddingVertical: 4,
    borderRadius: 6,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
    backgroundColor: "rgba(2, 6, 23, 0.4)",
  },
  baseIndicatorActive: {
    backgroundColor: "rgba(251, 191, 36, 0.2)",
    borderColor: "rgba(251, 191, 36, 0.6)",
  },
  baseIndicatorText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 10,
    fontWeight: "700",
  },
  baseIndicatorTextActive: {
    color: "#fbbf24",
  },
  eventResultText: {
    color: theme.colors.text,
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.3,
  },
  eventText: {
    marginTop: 8,
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
    lineHeight: 18,
  },
  emptyState: {
    paddingVertical: 24,
    alignItems: "center",
  },
  emptyText: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 12,
    fontWeight: "600",
  },
});
