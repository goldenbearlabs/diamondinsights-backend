import { Ionicons } from "@expo/vector-icons";
import { useRouter } from "expo-router";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { ApiError, apiGet, apiGetAuth } from "../../src/lib/api";
import { theme } from "../../src/theme/colors";

type RecordMode = "normal" | "plus";
type LeaderboardType = "bombs" | "hardest";
type TimeRange = "24h" | "1w" | "1m" | "all";

type HomeRunRecord = {
  game_id: string;
  event_id: number | null;
  date: string | null;
  difficulty: string | null;
  hitter_username: string | null;
  pitcher_username: string | null;
  batter_mlb_id: number | null;
  pitcher_mlb_id: number | null;
  hitter_name: string | null;
  pitcher_name: string | null;
  ball_park_name: string | null;
  elevation: number | null;
  selected_distance_ft: number | null;
  exit_vel_mph?: number | null;
  selected_exit_vel_mph?: number | null;
  filtered_rank: number | null;
  selected_rank: number | null;
};

type HomeRunRecordsResponse = {
  items: HomeRunRecord[];
  available_difficulties: string[];
  my_top_hr_ovr_rank: number | null;
  total: number;
  limit: number;
  offset: number;
  mode: RecordMode;
};

type HardHitRecordsResponse = {
  items: HomeRunRecord[];
  available_difficulties: string[];
  my_top_hit_ovr_rank: number | null;
  total: number;
  limit: number;
  offset: number;
};

type PlayerOption = {
  mlb_id: number;
  full_name: string;
};

type ShowUsernameOption = {
  username: string;
  display_name: string | null;
};

const PAGE_SIZE = 100;
const ALL_DIFFICULTIES = "ALL";
const DIFFICULTY_ORDER = ["goat", "legend", "hall of fame", "all star", "veteran", "rookie"];
const TIME_RANGE_OPTIONS: { value: TimeRange; label: string }[] = [
  { value: "24h", label: "24H" },
  { value: "1w", label: "1W" },
  { value: "1m", label: "1M" },
  { value: "all", label: "All" },
];

export default function RecordsScreen() {
  const router = useRouter();
  const [leaderboardType, setLeaderboardType] = useState<LeaderboardType>("bombs");

  const [mode, setMode] = useState<RecordMode>("normal");
  const [difficulty, setDifficulty] = useState(ALL_DIFFICULTIES);
  const [timeRange, setTimeRange] = useState<TimeRange>("all");
  const [hitterUsername, setHitterUsername] = useState("");
  const [pitcherUsername, setPitcherUsername] = useState("");
  const [hitterPlayerQuery, setHitterPlayerQuery] = useState("");
  const [pitcherPlayerQuery, setPitcherPlayerQuery] = useState("");
  const [hitterPlayerSelected, setHitterPlayerSelected] = useState<PlayerOption | null>(null);
  const [pitcherPlayerSelected, setPitcherPlayerSelected] = useState<PlayerOption | null>(null);

  const [appliedMode, setAppliedMode] = useState<RecordMode>("normal");
  const [appliedDifficulty, setAppliedDifficulty] = useState(ALL_DIFFICULTIES);
  const [appliedTimeRange, setAppliedTimeRange] = useState<TimeRange>("all");
  const [appliedHitterUsername, setAppliedHitterUsername] = useState("");
  const [appliedPitcherUsername, setAppliedPitcherUsername] = useState("");
  const [appliedHitterMlbId, setAppliedHitterMlbId] = useState<number | null>(null);
  const [appliedPitcherMlbId, setAppliedPitcherMlbId] = useState<number | null>(null);

  const [filtersOpen, setFiltersOpen] = useState(false);
  const [difficultyOpen, setDifficultyOpen] = useState(false);
  const [expandedKey, setExpandedKey] = useState<string | null>(null);
  const [page, setPage] = useState(1);

  const [hitterPlayerOptions, setHitterPlayerOptions] = useState<PlayerOption[]>([]);
  const [pitcherPlayerOptions, setPitcherPlayerOptions] = useState<PlayerOption[]>([]);
  const [hitterUsernameOptions, setHitterUsernameOptions] = useState<ShowUsernameOption[]>([]);
  const [pitcherUsernameOptions, setPitcherUsernameOptions] = useState<ShowUsernameOption[]>([]);
  const [hitterPlayerLoading, setHitterPlayerLoading] = useState(false);
  const [pitcherPlayerLoading, setPitcherPlayerLoading] = useState(false);
  const [hitterUsernameLoading, setHitterUsernameLoading] = useState(false);
  const [pitcherUsernameLoading, setPitcherUsernameLoading] = useState(false);

  const [difficultyOptions, setDifficultyOptions] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [records, setRecords] = useState<HomeRunRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [myTopHrOvrRank, setMyTopHrOvrRank] = useState<number | null>(null);
  const [myTopHitOvrRank, setMyTopHitOvrRank] = useState<number | null>(null);

  const hitterReqRef = useRef(0);
  const pitcherReqRef = useRef(0);
  const hitterUsernameReqRef = useRef(0);
  const pitcherUsernameReqRef = useRef(0);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / PAGE_SIZE)), [total]);

  const orderedDifficultyOptions = useMemo(
    () => orderDifficulties(difficultyOptions),
    [difficultyOptions]
  );

  const appliedFilters = useMemo(() => {
    const labels: string[] = [];
    if (appliedDifficulty !== ALL_DIFFICULTIES) labels.push(appliedDifficulty);
    if (appliedTimeRange !== "all") labels.push(timeRangeLabel(appliedTimeRange));
    if (appliedHitterUsername.trim()) labels.push(`Hitter @${appliedHitterUsername.trim()}`);
    if (appliedPitcherUsername.trim()) labels.push(`Pitcher @${appliedPitcherUsername.trim()}`);
    if (appliedHitterMlbId != null) labels.push(`Hitter MLB #${appliedHitterMlbId}`);
    if (appliedPitcherMlbId != null) labels.push(`Pitcher MLB #${appliedPitcherMlbId}`);
    return labels;
  }, [
    appliedDifficulty,
    appliedHitterMlbId,
    appliedHitterUsername,
    appliedPitcherMlbId,
    appliedPitcherUsername,
    appliedTimeRange,
  ]);

  const fetchRecords = useCallback(async <T,>(path: string): Promise<T> => {
    try {
      return await apiGetAuth<T>(path);
    } catch (err) {
      const unauthenticated =
        (err instanceof Error && err.message === "Not authenticated")
        || (err instanceof ApiError && err.status === 401);
      if (unauthenticated) return apiGet<T>(path);
      throw err;
    }
  }, []);

  const loadRecords = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("time_range", appliedTimeRange);
      params.set("limit", String(PAGE_SIZE));
      params.set("offset", String((page - 1) * PAGE_SIZE));

      if (appliedDifficulty !== ALL_DIFFICULTIES) params.set("difficulty", appliedDifficulty);
      if (appliedHitterUsername.trim()) params.set("hitter_username", appliedHitterUsername.trim());
      if (appliedPitcherUsername.trim()) params.set("pitcher_username", appliedPitcherUsername.trim());
      if (appliedHitterMlbId != null) params.set("hitter_mlb_id", String(appliedHitterMlbId));
      if (appliedPitcherMlbId != null) params.set("pitcher_mlb_id", String(appliedPitcherMlbId));

      if (leaderboardType === "bombs") {
        params.set("mode", appliedMode);
        const data = await fetchRecords<HomeRunRecordsResponse>(`/records/home-runs?${params.toString()}`);
        setRecords(Array.isArray(data.items) ? data.items : []);
        setDifficultyOptions(Array.isArray(data.available_difficulties) ? data.available_difficulties : []);
        setTotal(Number.isFinite(data.total) ? data.total : 0);
        setMyTopHrOvrRank(
          typeof data.my_top_hr_ovr_rank === "number" ? data.my_top_hr_ovr_rank : null
        );
        setMyTopHitOvrRank(null);
      } else {
        const data = await fetchRecords<HardHitRecordsResponse>(`/records/hardest-hits?${params.toString()}`);
        setRecords(Array.isArray(data.items) ? data.items : []);
        setDifficultyOptions(Array.isArray(data.available_difficulties) ? data.available_difficulties : []);
        setTotal(Number.isFinite(data.total) ? data.total : 0);
        setMyTopHitOvrRank(
          typeof data.my_top_hit_ovr_rank === "number" ? data.my_top_hit_ovr_rank : null
        );
        setMyTopHrOvrRank(null);
      }
      setExpandedKey(null);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unable to load records.";
      setError(message);
      setRecords([]);
      setTotal(0);
      setMyTopHrOvrRank(null);
      setMyTopHitOvrRank(null);
      setExpandedKey(null);
    } finally {
      setLoading(false);
    }
  }, [
    appliedDifficulty,
    appliedHitterMlbId,
    appliedHitterUsername,
    appliedMode,
    appliedPitcherMlbId,
    appliedPitcherUsername,
    appliedTimeRange,
    fetchRecords,
    leaderboardType,
    page,
  ]);

  useEffect(() => {
    loadRecords();
  }, [loadRecords]);

  useEffect(() => {
    const query = hitterPlayerQuery.trim();
    const selectedName = hitterPlayerSelected?.full_name?.trim().toLowerCase() ?? "";
    if (!filtersOpen || query.length < 2 || query.toLowerCase() === selectedName) {
      setHitterPlayerOptions([]);
      setHitterPlayerLoading(false);
      return;
    }

    const requestId = hitterReqRef.current + 1;
    hitterReqRef.current = requestId;

    const timer = setTimeout(async () => {
      setHitterPlayerLoading(true);
      try {
        const params = new URLSearchParams({ name: query, limit: "8" });
        const players = await apiGet<PlayerOption[]>(`/players/?${params.toString()}`);
        if (hitterReqRef.current !== requestId) return;
        setHitterPlayerOptions(Array.isArray(players) ? players.slice(0, 8) : []);
      } catch {
        if (hitterReqRef.current === requestId) setHitterPlayerOptions([]);
      } finally {
        if (hitterReqRef.current === requestId) setHitterPlayerLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [filtersOpen, hitterPlayerQuery, hitterPlayerSelected]);

  useEffect(() => {
    const query = hitterUsername.trim();
    if (!filtersOpen || query.length < 2) {
      setHitterUsernameOptions([]);
      setHitterUsernameLoading(false);
      return;
    }

    const requestId = hitterUsernameReqRef.current + 1;
    hitterUsernameReqRef.current = requestId;

    const timer = setTimeout(async () => {
      setHitterUsernameLoading(true);
      try {
        const params = new URLSearchParams({ q: query, limit: "8" });
        const profiles = await apiGet<ShowUsernameOption[]>(`/users/show/search?${params.toString()}`);
        if (hitterUsernameReqRef.current !== requestId) return;
        setHitterUsernameOptions(Array.isArray(profiles) ? profiles.slice(0, 8) : []);
      } catch {
        if (hitterUsernameReqRef.current === requestId) setHitterUsernameOptions([]);
      } finally {
        if (hitterUsernameReqRef.current === requestId) setHitterUsernameLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [filtersOpen, hitterUsername]);

  useEffect(() => {
    const query = pitcherUsername.trim();
    if (!filtersOpen || query.length < 2) {
      setPitcherUsernameOptions([]);
      setPitcherUsernameLoading(false);
      return;
    }

    const requestId = pitcherUsernameReqRef.current + 1;
    pitcherUsernameReqRef.current = requestId;

    const timer = setTimeout(async () => {
      setPitcherUsernameLoading(true);
      try {
        const params = new URLSearchParams({ q: query, limit: "8" });
        const profiles = await apiGet<ShowUsernameOption[]>(`/users/show/search?${params.toString()}`);
        if (pitcherUsernameReqRef.current !== requestId) return;
        setPitcherUsernameOptions(Array.isArray(profiles) ? profiles.slice(0, 8) : []);
      } catch {
        if (pitcherUsernameReqRef.current === requestId) setPitcherUsernameOptions([]);
      } finally {
        if (pitcherUsernameReqRef.current === requestId) setPitcherUsernameLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [filtersOpen, pitcherUsername]);

  useEffect(() => {
    const query = pitcherPlayerQuery.trim();
    const selectedName = pitcherPlayerSelected?.full_name?.trim().toLowerCase() ?? "";
    if (!filtersOpen || query.length < 2 || query.toLowerCase() === selectedName) {
      setPitcherPlayerOptions([]);
      setPitcherPlayerLoading(false);
      return;
    }

    const requestId = pitcherReqRef.current + 1;
    pitcherReqRef.current = requestId;

    const timer = setTimeout(async () => {
      setPitcherPlayerLoading(true);
      try {
        const params = new URLSearchParams({ name: query, limit: "8" });
        const players = await apiGet<PlayerOption[]>(`/players/?${params.toString()}`);
        if (pitcherReqRef.current !== requestId) return;
        setPitcherPlayerOptions(Array.isArray(players) ? players.slice(0, 8) : []);
      } catch {
        if (pitcherReqRef.current === requestId) setPitcherPlayerOptions([]);
      } finally {
        if (pitcherReqRef.current === requestId) setPitcherPlayerLoading(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [filtersOpen, pitcherPlayerQuery, pitcherPlayerSelected]);

  const clearFilters = useCallback(() => {
    setMode("normal");
    setDifficulty(ALL_DIFFICULTIES);
    setTimeRange("all");
    setHitterUsername("");
    setPitcherUsername("");
    setHitterPlayerQuery("");
    setPitcherPlayerQuery("");
    setHitterPlayerSelected(null);
    setPitcherPlayerSelected(null);
    setHitterPlayerOptions([]);
    setPitcherPlayerOptions([]);
    setHitterUsernameOptions([]);
    setPitcherUsernameOptions([]);
    setDifficultyOpen(false);

    setAppliedMode("normal");
    setAppliedDifficulty(ALL_DIFFICULTIES);
    setAppliedTimeRange("all");
    setAppliedHitterUsername("");
    setAppliedPitcherUsername("");
    setAppliedHitterMlbId(null);
    setAppliedPitcherMlbId(null);

    setPage(1);
    setExpandedKey(null);
  }, []);

  const applyFilters = useCallback(() => {
    setAppliedMode(mode);
    setAppliedDifficulty(difficulty);
    setAppliedTimeRange(timeRange);
    setAppliedHitterUsername(hitterUsername);
    setAppliedPitcherUsername(pitcherUsername);
    setAppliedHitterMlbId(hitterPlayerSelected?.mlb_id ?? null);
    setAppliedPitcherMlbId(pitcherPlayerSelected?.mlb_id ?? null);
    setPage(1);
    setExpandedKey(null);
    setDifficultyOpen(false);
    setHitterPlayerOptions([]);
    setPitcherPlayerOptions([]);
    setHitterUsernameOptions([]);
    setPitcherUsernameOptions([]);
  }, [
    difficulty,
    hitterPlayerSelected,
    hitterUsername,
    mode,
    pitcherPlayerSelected,
    pitcherUsername,
    timeRange,
  ]);

  const startIndex = records.length > 0 ? (page - 1) * PAGE_SIZE + 1 : 0;
  const endIndex = records.length > 0 ? startIndex + records.length - 1 : 0;
  const isHardestLeaderboard = leaderboardType === "hardest";
  const leaderboardTitle = isHardestLeaderboard
    ? "Hardest Hits Leaderboard"
    : "Biggest Bombs Leaderboard";
  const leaderboardSubtitle = isHardestLeaderboard
    ? `Sorted by Exit Velocity · ${timeRangeLabel(appliedTimeRange)}`
    : `Sorted by ${appliedMode === "plus" ? "Home Runs+" : "Home Runs"} · ${timeRangeLabel(appliedTimeRange)}`;
  const myTopOvrRank = isHardestLeaderboard ? myTopHitOvrRank : myTopHrOvrRank;
  const myTopLabel = isHardestLeaderboard ? "Your top hit is" : "Your top HR is";

  const navigateToUserStats = useCallback(
    (username: string | null) => {
      const next = (username || "").trim();
      if (!next) return;
      router.push({
        pathname: "/(app)/gameplay-stats",
        params: { viewUsername: next },
      });
    },
    [router]
  );

  return (
    <View style={styles.container}>
      <ScrollView contentContainerStyle={styles.content} keyboardShouldPersistTaps="handled">
        <Text style={styles.title}>Records</Text>

        <View style={styles.topToggleRow}>
          <Pressable
            style={[
              styles.topToggleButton,
              leaderboardType === "bombs" && styles.topToggleButtonActive,
            ]}
            onPress={() => {
              setLeaderboardType("bombs");
              setPage(1);
              setExpandedKey(null);
            }}
          >
            <Text
              style={[
                styles.topToggleText,
                leaderboardType === "bombs" && styles.topToggleTextActive,
              ]}
            >
              Biggest Bombs
            </Text>
          </Pressable>
          <Pressable
            style={[
              styles.topToggleButton,
              leaderboardType === "hardest" && styles.topToggleButtonActive,
            ]}
            onPress={() => {
              setLeaderboardType("hardest");
              setPage(1);
              setExpandedKey(null);
            }}
          >
            <Text
              style={[
                styles.topToggleText,
                leaderboardType === "hardest" && styles.topToggleTextActive,
              ]}
            >
              Hardest Hits
            </Text>
          </Pressable>
        </View>

        <>
            <View style={styles.filterCard}>
              <Pressable
                style={styles.filterHeaderRow}
                onPress={() => {
                  setFiltersOpen((prev) => !prev);
                  setDifficultyOpen(false);
                }}
              >
                <View>
                  <Text style={styles.sectionTitle}>Filters</Text>
                  <Text style={styles.filterSummaryText} numberOfLines={1}>
                    {appliedFilters.length === 0
                      ? "No filters applied"
                      : appliedFilters.join(" · ")}
                  </Text>
                </View>
                <Ionicons
                  name={filtersOpen ? "chevron-up" : "chevron-down"}
                  size={16}
                  color={theme.colors.text}
                />
              </Pressable>

              {filtersOpen ? (
                <View style={styles.filterBody}>
                  {!isHardestLeaderboard ? (
                    <View style={styles.modeRow}>
                      <Pressable
                        style={[styles.modeButton, mode === "normal" && styles.modeButtonActive]}
                        onPress={() => setMode("normal")}
                      >
                        <Text
                          style={[
                            styles.modeButtonText,
                            mode === "normal" && styles.modeButtonTextActive,
                          ]}
                        >
                          Normal
                        </Text>
                      </Pressable>
                      <Pressable
                        style={[styles.modeButton, mode === "plus" && styles.modeButtonActive]}
                        onPress={() => setMode("plus")}
                      >
                        <Text
                          style={[
                            styles.modeButtonText,
                            mode === "plus" && styles.modeButtonTextActive,
                          ]}
                        >
                          Home Runs+
                        </Text>
                      </Pressable>
                    </View>
                  ) : null}

                  <View>
                    <Text style={styles.fieldLabel}>Time Range</Text>
                    <View style={styles.timeRangeRow}>
                      {TIME_RANGE_OPTIONS.map((option) => {
                        const active = timeRange === option.value;
                        return (
                          <Pressable
                            key={option.value}
                            style={[styles.timeRangeButton, active && styles.timeRangeButtonActive]}
                            onPress={() => setTimeRange(option.value)}
                          >
                            <Text
                              style={[
                                styles.timeRangeButtonText,
                                active && styles.timeRangeButtonTextActive,
                              ]}
                            >
                              {option.label}
                            </Text>
                          </Pressable>
                        );
                      })}
                    </View>
                  </View>

                  <View>
                    <Text style={styles.fieldLabel}>Difficulty</Text>
                    <Pressable
                      style={styles.dropdownTrigger}
                      onPress={() => setDifficultyOpen((prev) => !prev)}
                    >
                      <Text style={styles.dropdownValueText}>
                        {difficulty === ALL_DIFFICULTIES ? "All difficulties" : difficulty}
                      </Text>
                      <Ionicons
                        name={difficultyOpen ? "chevron-up" : "chevron-down"}
                        size={14}
                        color={theme.colors.muted}
                      />
                    </Pressable>
                    {difficultyOpen ? (
                      <View style={styles.dropdownMenu}>
                        {[ALL_DIFFICULTIES, ...orderedDifficultyOptions].map((option) => {
                          const active = difficulty === option;
                          return (
                            <Pressable
                              key={option}
                              style={[styles.dropdownOption, active && styles.dropdownOptionActive]}
                              onPress={() => {
                                setDifficulty(option);
                                setDifficultyOpen(false);
                              }}
                            >
                              <Text
                                style={[
                                  styles.dropdownOptionText,
                                  active && styles.dropdownOptionTextActive,
                                ]}
                              >
                                {option === ALL_DIFFICULTIES ? "All difficulties" : option}
                              </Text>
                            </Pressable>
                          );
                        })}
                      </View>
                    ) : null}
                  </View>

                  <TextInput
                    value={hitterUsername}
                    onChangeText={(value) => {
                      setHitterUsername(value);
                    }}
                    placeholder="Hitter username"
                    placeholderTextColor={theme.colors.muted}
                    style={styles.input}
                    autoCapitalize="none"
                    autoCorrect={false}
                  />
                  {hitterUsernameLoading ? (
                    <Text style={styles.lookupHint}>Searching hitter usernames...</Text>
                  ) : null}
                  {hitterUsernameOptions.length > 0 ? (
                    <View style={styles.suggestionWrap}>
                      {hitterUsernameOptions.map((profile) => (
                        <Pressable
                          key={`hu-${profile.username}`}
                          style={styles.suggestionItem}
                          onPress={() => {
                            setHitterUsername(profile.username);
                            setHitterUsernameOptions([]);
                          }}
                        >
                          <Text style={styles.suggestionName}>@{profile.username}</Text>
                          <Text style={styles.suggestionMeta}>
                            {profile.display_name || "Show user"}
                          </Text>
                        </Pressable>
                      ))}
                    </View>
                  ) : null}

                  <TextInput
                    value={pitcherUsername}
                    onChangeText={(value) => {
                      setPitcherUsername(value);
                    }}
                    placeholder="Pitcher username"
                    placeholderTextColor={theme.colors.muted}
                    style={styles.input}
                    autoCapitalize="none"
                    autoCorrect={false}
                  />
                  {pitcherUsernameLoading ? (
                    <Text style={styles.lookupHint}>Searching pitcher usernames...</Text>
                  ) : null}
                  {pitcherUsernameOptions.length > 0 ? (
                    <View style={styles.suggestionWrap}>
                      {pitcherUsernameOptions.map((profile) => (
                        <Pressable
                          key={`pu-${profile.username}`}
                          style={styles.suggestionItem}
                          onPress={() => {
                            setPitcherUsername(profile.username);
                            setPitcherUsernameOptions([]);
                          }}
                        >
                          <Text style={styles.suggestionName}>@{profile.username}</Text>
                          <Text style={styles.suggestionMeta}>
                            {profile.display_name || "Show user"}
                          </Text>
                        </Pressable>
                      ))}
                    </View>
                  ) : null}

                  <View>
                    <Text style={styles.fieldLabel}>Hitter MLB player</Text>
                    <TextInput
                      value={hitterPlayerQuery}
                      onChangeText={(value) => {
                        setHitterPlayerQuery(value);
                        setHitterPlayerSelected(null);
                      }}
                      placeholder="Type player name..."
                      placeholderTextColor={theme.colors.muted}
                      style={styles.input}
                      autoCapitalize="words"
                      autoCorrect={false}
                    />
                    {hitterPlayerLoading ? (
                      <Text style={styles.lookupHint}>Searching hitters...</Text>
                    ) : null}
                    {hitterPlayerOptions.length > 0 ? (
                      <View style={styles.suggestionWrap}>
                        {hitterPlayerOptions.map((player) => (
                          <Pressable
                            key={`h-${player.mlb_id}`}
                            style={styles.suggestionItem}
                            onPress={() => {
                              setHitterPlayerSelected(player);
                              setHitterPlayerQuery(player.full_name);
                              setHitterPlayerOptions([]);
                            }}
                          >
                            <Text style={styles.suggestionName}>{player.full_name}</Text>
                            <Text style={styles.suggestionMeta}>#{player.mlb_id}</Text>
                          </Pressable>
                        ))}
                      </View>
                    ) : null}
                  </View>

                  <View>
                    <Text style={styles.fieldLabel}>Pitcher MLB player</Text>
                    <TextInput
                      value={pitcherPlayerQuery}
                      onChangeText={(value) => {
                        setPitcherPlayerQuery(value);
                        setPitcherPlayerSelected(null);
                      }}
                      placeholder="Type player name..."
                      placeholderTextColor={theme.colors.muted}
                      style={styles.input}
                      autoCapitalize="words"
                      autoCorrect={false}
                    />
                    {pitcherPlayerLoading ? (
                      <Text style={styles.lookupHint}>Searching pitchers...</Text>
                    ) : null}
                    {pitcherPlayerOptions.length > 0 ? (
                      <View style={styles.suggestionWrap}>
                        {pitcherPlayerOptions.map((player) => (
                          <Pressable
                            key={`p-${player.mlb_id}`}
                            style={styles.suggestionItem}
                            onPress={() => {
                              setPitcherPlayerSelected(player);
                              setPitcherPlayerQuery(player.full_name);
                              setPitcherPlayerOptions([]);
                            }}
                          >
                            <Text style={styles.suggestionName}>{player.full_name}</Text>
                            <Text style={styles.suggestionMeta}>#{player.mlb_id}</Text>
                          </Pressable>
                        ))}
                      </View>
                    ) : null}
                  </View>

                  <View style={styles.actionsRow}>
                    <Pressable style={styles.applyButton} onPress={applyFilters}>
                      <Text style={styles.applyButtonText}>Apply</Text>
                    </Pressable>
                    <Pressable style={styles.clearButton} onPress={clearFilters}>
                      <Text style={styles.clearButtonText}>Clear</Text>
                    </Pressable>
                  </View>
                </View>
              ) : null}
            </View>

            <Text style={styles.leaderboardTitle}>{leaderboardTitle}</Text>
            <Text style={styles.leaderboardSubtitle}>{leaderboardSubtitle}</Text>
            {myTopOvrRank != null ? (
              <Text style={styles.topRankNote}>
                {myTopLabel} {formatOrdinal(myTopOvrRank)} Ovr
              </Text>
            ) : null}

            <View style={styles.listHeaderRow}>
              <Text style={styles.totalText}>
                {startIndex}-{endIndex} of {total}
              </Text>
            </View>

            <View style={styles.paginationRow}>
              <Pressable
                style={[styles.pageButton, (page <= 1 || loading) && styles.pageButtonDisabled]}
                disabled={page <= 1 || loading}
                onPress={() => setPage((p) => Math.max(1, p - 1))}
              >
                <Text style={styles.pageButtonText}>Prev</Text>
              </Pressable>
              <Text style={styles.pageMetaText}>
                Page {page} / {totalPages}
              </Text>
              <Pressable
                style={[
                  styles.pageButton,
                  (page >= totalPages || loading) && styles.pageButtonDisabled,
                ]}
                disabled={page >= totalPages || loading}
                onPress={() => setPage((p) => Math.min(totalPages, p + 1))}
              >
                <Text style={styles.pageButtonText}>Next</Text>
              </Pressable>
            </View>

            {loading ? (
              <View style={styles.loadingWrap}>
                <ActivityIndicator color={theme.colors.primary} />
              </View>
            ) : error ? (
              <Text style={styles.errorText}>{error}</Text>
            ) : records.length === 0 ? (
              <Text style={styles.emptyText}>No records found.</Text>
            ) : (
              records.map((item, index) => {
                const key = `${item.game_id}-${item.event_id ?? index}`;
                const isExpanded = expandedKey === key;
                const value = isHardestLeaderboard
                  ? (item.selected_exit_vel_mph ?? item.exit_vel_mph ?? null)
                  : item.selected_distance_ft;
                const valueText = isHardestLeaderboard
                  ? formatExitVelocity(value)
                  : formatDistance(value);
                const filteredRank = item.filtered_rank ?? (page - 1) * PAGE_SIZE + index + 1;
                const overallRank = item.selected_rank;
                const hitterUsernameText = item.hitter_username ? `@${item.hitter_username}` : "@unknown";

                return (
                  <View key={key} style={styles.recordCard}>
                    <Pressable
                      onPress={() => setExpandedKey((current) => (current === key ? null : key))}
                      style={styles.condensedRow}
                    >
                      <View style={styles.rankBadge}>
                        <Text style={styles.rankBadgeText}>#{filteredRank}</Text>
                        {overallRank != null ? (
                          <Text style={styles.rankOverallText}>#{overallRank} Ovr</Text>
                        ) : null}
                      </View>
                      <View style={styles.condensedCenter}>
                        <Text style={styles.compactHitter} numberOfLines={1}>
                          <Text
                            style={styles.profileLinkText}
                            onPress={() => navigateToUserStats(item.hitter_username)}
                          >
                            {hitterUsernameText}
                          </Text>
                        </Text>
                        <Text style={styles.compactMeta} numberOfLines={1}>
                          {item.hitter_name || "Unknown hitter"}
                        </Text>
                      </View>
                      <View style={styles.condensedRight}>
                        <Text style={styles.distanceText}>{valueText}</Text>
                        <Ionicons
                          name={isExpanded ? "chevron-up" : "chevron-down"}
                          size={14}
                          color={theme.colors.muted}
                        />
                      </View>
                    </Pressable>

                    {isExpanded ? (
                      <View style={styles.detailWrap}>
                        <View style={styles.chipRow}>
                          <View style={styles.detailChip}>
                            <Text style={styles.detailChipText}>
                              {item.difficulty || "Unknown Difficulty"}
                            </Text>
                          </View>
                          <View style={styles.detailChip}>
                            <Text style={styles.detailChipText}>{formatDate(item.date)}</Text>
                          </View>
                        </View>

                        <View style={styles.profilesRow}>
                          <View style={styles.profileBlock}>
                            <Text style={styles.detailLabel}>Hitting Profile</Text>
                            <Text
                              style={[styles.detailProfileValue, styles.profileLinkText]}
                              numberOfLines={1}
                              onPress={() => navigateToUserStats(item.hitter_username)}
                            >
                              @{item.hitter_username || "unknown"}
                            </Text>
                          </View>
                          <View style={styles.profileBlock}>
                            <Text style={styles.detailLabel}>Pitching Profile</Text>
                            <Text
                              style={[styles.detailProfileValue, styles.profileLinkText]}
                              numberOfLines={1}
                              onPress={() => navigateToUserStats(item.pitcher_username)}
                            >
                              @{item.pitcher_username || "unknown"}
                            </Text>
                          </View>
                        </View>

                        <View style={styles.playerRow}>
                          <Text style={styles.detailLabel}>Hitter</Text>
                          <Text style={styles.detailValueRight}>
                            {item.hitter_name || formatMlbId(item.batter_mlb_id)}
                          </Text>
                        </View>
                        <View style={styles.playerRow}>
                          <Text style={styles.detailLabel}>Pitcher</Text>
                          <Text style={styles.detailValueRight}>
                            {item.pitcher_name || formatMlbId(item.pitcher_mlb_id)}
                          </Text>
                        </View>
                        <View style={styles.playerRow}>
                          <Text style={styles.detailLabel}>
                            {isHardestLeaderboard
                              ? "Exit Velocity"
                              : appliedMode === "plus"
                                ? "Distance+"
                                : "Distance"}
                          </Text>
                          <Text style={styles.detailValueRight}>{valueText}</Text>
                        </View>
                        <View style={styles.playerRow}>
                          <Text style={styles.detailLabel}>Ball Park</Text>
                          <Text style={styles.detailValueRight}>{item.ball_park_name || "Unknown"}</Text>
                        </View>
                        {!isHardestLeaderboard ? (
                          <View style={styles.playerRow}>
                            <Text style={styles.detailLabel}>Elevation</Text>
                            <Text style={styles.detailValueRight}>{formatElevation(item.elevation)}</Text>
                          </View>
                        ) : null}
                      </View>
                    ) : null}
                  </View>
                );
              })
            )}
          </>
      </ScrollView>
    </View>
  );
}

function normalizeDifficulty(value: string): string {
  return value.trim().toLowerCase().replace(/-/g, " ");
}

function orderDifficulties(values: string[]): string[] {
  const unique = Array.from(new Set(values.filter((value) => value.trim().length > 0)));
  return unique.sort((a, b) => {
    const left = normalizeDifficulty(a);
    const right = normalizeDifficulty(b);
    const leftIdx = DIFFICULTY_ORDER.indexOf(left);
    const rightIdx = DIFFICULTY_ORDER.indexOf(right);

    if (leftIdx >= 0 && rightIdx >= 0) return leftIdx - rightIdx;
    if (leftIdx >= 0) return -1;
    if (rightIdx >= 0) return 1;
    return left.localeCompare(right);
  });
}

function timeRangeLabel(value: TimeRange): string {
  if (value === "24h") return "24H";
  if (value === "1w") return "1W";
  if (value === "1m") return "1M";
  return "All Time";
}

function formatDate(value: string | null): string {
  if (!value) return "No date";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleDateString();
}

function formatDistance(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(1)} ft`;
}

function formatExitVelocity(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(1)} mph`;
}

function formatOrdinal(value: number): string {
  const abs = Math.abs(value);
  const mod100 = abs % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${value}th`;
  const mod10 = abs % 10;
  if (mod10 === 1) return `${value}st`;
  if (mod10 === 2) return `${value}nd`;
  if (mod10 === 3) return `${value}rd`;
  return `${value}th`;
}

function formatElevation(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(0)} ft`;
}

function formatMlbId(value: number | null): string {
  if (value == null) return "Unknown";
  return `#${value}`;
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  content: {
    padding: 16,
    paddingBottom: 110,
    gap: 12,
  },
  title: {
    color: theme.colors.text,
    fontSize: 26,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
  topToggleRow: {
    flexDirection: "row",
    gap: 8,
  },
  topToggleButton: {
    flex: 1,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: theme.colors.border,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    paddingVertical: 10,
    alignItems: "center",
  },
  topToggleButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.7)",
    backgroundColor: "rgba(64, 40, 8, 0.65)",
  },
  topToggleText: {
    color: theme.colors.muted,
    fontSize: 13,
    fontWeight: "700",
  },
  topToggleTextActive: {
    color: "#fbbf24",
  },
  placeholderCard: {
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    backgroundColor: "rgba(15, 23, 42, 0.82)",
    padding: 16,
    gap: 8,
  },
  placeholderTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  placeholderText: {
    color: theme.colors.muted,
    fontSize: 13,
    lineHeight: 18,
  },
  filterCard: {
    backgroundColor: "rgba(15, 23, 42, 0.82)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    borderRadius: 14,
    paddingHorizontal: 12,
    paddingVertical: 10,
  },
  filterHeaderRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 8,
  },
  filterSummaryText: {
    color: theme.colors.muted,
    fontSize: 12,
    marginTop: 3,
    maxWidth: 270,
  },
  filterBody: {
    marginTop: 12,
    gap: 10,
  },
  sectionTitle: {
    color: theme.colors.text,
    fontSize: 15,
    fontWeight: "700",
  },
  modeRow: {
    flexDirection: "row",
    gap: 8,
  },
  modeButton: {
    flex: 1,
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    paddingVertical: 10,
    alignItems: "center",
    backgroundColor: "rgba(15, 23, 42, 0.6)",
  },
  modeButtonActive: {
    borderColor: theme.colors.primary,
    backgroundColor: "rgba(59, 130, 246, 0.22)",
  },
  modeButtonText: {
    color: theme.colors.muted,
    fontWeight: "600",
    fontSize: 13,
  },
  modeButtonTextActive: {
    color: theme.colors.text,
  },
  fieldLabel: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
    marginBottom: 5,
  },
  timeRangeRow: {
    flexDirection: "row",
    gap: 8,
  },
  timeRangeButton: {
    flex: 1,
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    paddingVertical: 9,
    alignItems: "center",
    backgroundColor: "rgba(15, 23, 42, 0.55)",
  },
  timeRangeButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.65)",
    backgroundColor: "rgba(64, 40, 8, 0.6)",
  },
  timeRangeButtonText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "700",
  },
  timeRangeButtonTextActive: {
    color: "#fbbf24",
  },
  dropdownTrigger: {
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 10,
    backgroundColor: "rgba(15, 23, 42, 0.55)",
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  dropdownValueText: {
    color: theme.colors.text,
    fontSize: 13,
  },
  dropdownMenu: {
    marginTop: 6,
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    overflow: "hidden",
    backgroundColor: "rgba(2, 6, 23, 0.96)",
  },
  dropdownOption: {
    paddingHorizontal: 10,
    paddingVertical: 10,
    borderTopWidth: 1,
    borderTopColor: "rgba(148, 163, 184, 0.2)",
  },
  dropdownOptionActive: {
    backgroundColor: "rgba(59, 130, 246, 0.18)",
  },
  dropdownOptionText: {
    color: theme.colors.muted,
    fontSize: 13,
  },
  dropdownOptionTextActive: {
    color: theme.colors.text,
    fontWeight: "700",
  },
  input: {
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 10,
    color: theme.colors.text,
    backgroundColor: "rgba(15, 23, 42, 0.55)",
  },
  lookupHint: {
    marginTop: 4,
    color: theme.colors.muted,
    fontSize: 11,
  },
  suggestionWrap: {
    marginTop: 6,
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    overflow: "hidden",
    backgroundColor: "rgba(2, 6, 23, 0.94)",
  },
  suggestionItem: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 10,
    borderTopWidth: 1,
    borderTopColor: "rgba(148, 163, 184, 0.2)",
  },
  suggestionName: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "600",
    flex: 1,
    marginRight: 8,
  },
  suggestionMeta: {
    color: theme.colors.muted,
    fontSize: 11,
  },
  actionsRow: {
    flexDirection: "row",
    gap: 8,
  },
  applyButton: {
    flex: 1,
    borderRadius: 8,
    backgroundColor: theme.colors.primary,
    paddingVertical: 11,
    alignItems: "center",
  },
  applyButtonText: {
    color: "#fff",
    fontWeight: "700",
    fontSize: 14,
  },
  clearButton: {
    flex: 1,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: theme.colors.border,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    paddingVertical: 11,
    alignItems: "center",
  },
  clearButtonText: {
    color: theme.colors.text,
    fontWeight: "700",
    fontSize: 14,
  },
  leaderboardTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "800",
  },
  leaderboardSubtitle: {
    color: theme.colors.muted,
    fontSize: 12,
    marginTop: -2,
  },
  topRankNote: {
    color: "#fbbf24",
    fontSize: 12,
    fontWeight: "700",
    marginTop: -4,
  },
  listHeaderRow: {
    flexDirection: "row",
    justifyContent: "flex-end",
    alignItems: "center",
  },
  totalText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  paginationRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  pageButton: {
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: 8,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    paddingVertical: 8,
    paddingHorizontal: 14,
  },
  pageButtonDisabled: {
    opacity: 0.45,
  },
  pageButtonText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  pageMetaText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  loadingWrap: {
    paddingTop: 22,
    alignItems: "center",
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
    paddingTop: 6,
  },
  emptyText: {
    color: theme.colors.muted,
    fontSize: 13,
    paddingTop: 6,
  },
  recordCard: {
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    backgroundColor: "rgba(15, 23, 42, 0.82)",
    overflow: "hidden",
  },
  condensedRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 10,
    gap: 10,
  },
  rankBadge: {
    minWidth: 62,
    borderRadius: 8,
    paddingVertical: 5,
    paddingHorizontal: 8,
    alignItems: "center",
    backgroundColor: "rgba(251, 191, 36, 0.16)",
    borderWidth: 1,
    borderColor: "rgba(251, 191, 36, 0.4)",
  },
  rankBadgeText: {
    color: "#fbbf24",
    fontSize: 12,
    fontWeight: "800",
  },
  rankOverallText: {
    color: "#fde68a",
    fontSize: 9,
    fontWeight: "700",
    marginTop: 1,
  },
  condensedCenter: {
    flex: 1,
    minWidth: 0,
  },
  compactHitter: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  compactMeta: {
    color: theme.colors.muted,
    fontSize: 11,
    marginTop: 2,
  },
  condensedRight: {
    alignItems: "flex-end",
    gap: 2,
  },
  distanceText: {
    color: theme.colors.text,
    fontWeight: "800",
    fontSize: 14,
  },
  detailWrap: {
    borderTopWidth: 1,
    borderTopColor: "rgba(148, 163, 184, 0.22)",
    paddingHorizontal: 12,
    paddingVertical: 12,
    gap: 10,
    backgroundColor: "rgba(2, 6, 23, 0.35)",
  },
  chipRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
  },
  detailChip: {
    borderRadius: 999,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "rgba(59, 130, 246, 0.16)",
    borderWidth: 1,
    borderColor: "rgba(59, 130, 246, 0.35)",
  },
  detailChipText: {
    color: "#bfdbfe",
    fontSize: 11,
    fontWeight: "700",
  },
  profilesRow: {
    flexDirection: "row",
    gap: 10,
  },
  profileBlock: {
    flex: 1,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    paddingVertical: 8,
    paddingHorizontal: 10,
    backgroundColor: "rgba(15, 23, 42, 0.52)",
  },
  playerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: 8,
  },
  detailLabel: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.4,
    textTransform: "uppercase",
  },
  detailProfileValue: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
    marginTop: 4,
  },
  profileLinkText: {
    color: "#93c5fd",
  },
  detailValueRight: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "700",
    flexShrink: 1,
    textAlign: "right",
  },
});
