"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import styles from "./page.module.css";
import { ApiError, apiGet, apiGetAuth } from "@/lib/api";

type RecordMode = "normal" | "plus";
type LeaderboardType = "bombs" | "hardest";
type TimeRange = "24h" | "1w" | "1m" | "all";

type RecordItem = {
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
  selected_distance_ft?: number | null;
  exit_vel_mph?: number | null;
  selected_exit_vel_mph?: number | null;
  filtered_rank: number | null;
  selected_rank: number | null;
};

type HomeRunRecordsResponse = {
  items: RecordItem[];
  available_difficulties: string[];
  my_top_hr_ovr_rank: number | null;
  total: number;
  limit: number;
  offset: number;
  mode: RecordMode;
};

type HardHitRecordsResponse = {
  items: RecordItem[];
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
const TIME_RANGE_OPTIONS: Array<{ value: TimeRange; label: string }> = [
  { value: "24h", label: "24H" },
  { value: "1w", label: "1W" },
  { value: "1m", label: "1M" },
  { value: "all", label: "All" },
];

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

function formatDistance(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "--";
  return `${value.toFixed(1)} ft`;
}

function formatExitVelocity(value: number | null | undefined): string {
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

export default function RecordsPage() {
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
  const [records, setRecords] = useState<RecordItem[]>([]);
  const [total, setTotal] = useState(0);
  const [myTopHrOvrRank, setMyTopHrOvrRank] = useState<number | null>(null);
  const [myTopHitOvrRank, setMyTopHitOvrRank] = useState<number | null>(null);

  const hitterReqRef = useRef(0);
  const pitcherReqRef = useRef(0);
  const hitterUsernameReqRef = useRef(0);
  const pitcherUsernameReqRef = useRef(0);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / PAGE_SIZE)), [total]);
  const orderedDifficultyOptions = useMemo(() => orderDifficulties(difficultyOptions), [difficultyOptions]);

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
        (err instanceof Error && err.message === "Not authenticated") ||
        (err instanceof ApiError && err.status === 401);
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
        setMyTopHrOvrRank(typeof data.my_top_hr_ovr_rank === "number" ? data.my_top_hr_ovr_rank : null);
        setMyTopHitOvrRank(null);
      } else {
        const data = await fetchRecords<HardHitRecordsResponse>(`/records/hardest-hits?${params.toString()}`);
        setRecords(Array.isArray(data.items) ? data.items : []);
        setDifficultyOptions(Array.isArray(data.available_difficulties) ? data.available_difficulties : []);
        setTotal(Number.isFinite(data.total) ? data.total : 0);
        setMyTopHitOvrRank(typeof data.my_top_hit_ovr_rank === "number" ? data.my_top_hit_ovr_rank : null);
        setMyTopHrOvrRank(null);
      }

      setExpandedKey(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to load records.");
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
    void loadRecords();
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
  }, [difficulty, hitterPlayerSelected, hitterUsername, mode, pitcherPlayerSelected, pitcherUsername, timeRange]);

  const startIndex = records.length > 0 ? (page - 1) * PAGE_SIZE + 1 : 0;
  const endIndex = records.length > 0 ? startIndex + records.length - 1 : 0;
  const isHardestLeaderboard = leaderboardType === "hardest";
  const leaderboardTitle = isHardestLeaderboard ? "Hardest Hits Leaderboard" : "Biggest Bombs Leaderboard";
  const leaderboardSubtitle = isHardestLeaderboard
    ? `Sorted by Exit Velocity · ${timeRangeLabel(appliedTimeRange)}`
    : `Sorted by ${appliedMode === "plus" ? "Home Runs+" : "Home Runs"} · ${timeRangeLabel(appliedTimeRange)}`;
  const myTopOvrRank = isHardestLeaderboard ? myTopHitOvrRank : myTopHrOvrRank;
  const myTopLabel = isHardestLeaderboard ? "Your top hit is" : "Your top HR is";

  const navigateToUserStats = useCallback(
    (username: string | null) => {
      const next = (username || "").trim();
      if (!next) return;
      router.push(`/gameplay-stats?user=${encodeURIComponent(next)}`);
    },
    [router],
  );

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <header className={styles.header}>
          <h1>Records</h1>
        </header>

        <div className={styles.topToggleRow}>
          <button
            type="button"
            className={leaderboardType === "bombs" ? styles.topToggleButtonActive : styles.topToggleButton}
            onClick={() => {
              setLeaderboardType("bombs");
              setPage(1);
              setExpandedKey(null);
            }}
          >
            Biggest Bombs
          </button>
          <button
            type="button"
            className={leaderboardType === "hardest" ? styles.topToggleButtonActive : styles.topToggleButton}
            onClick={() => {
              setLeaderboardType("hardest");
              setPage(1);
              setExpandedKey(null);
            }}
          >
            Hardest Hits
          </button>
        </div>

        <section className={styles.filterCard}>
          <button
            type="button"
            className={styles.filterHeaderRow}
            onClick={() => {
              setFiltersOpen((prev) => !prev);
              setDifficultyOpen(false);
            }}
          >
            <span className={styles.filterHeaderText}>
              <strong>Filters</strong>
              <small>{appliedFilters.length === 0 ? "No filters applied" : appliedFilters.join(" · ")}</small>
            </span>
            <span className={styles.chevron}>{filtersOpen ? "▲" : "▼"}</span>
          </button>

          {filtersOpen ? (
            <div className={styles.filterBody}>
              {!isHardestLeaderboard ? (
                <div className={styles.modeRow}>
                  <button
                    type="button"
                    className={mode === "normal" ? styles.modeButtonActive : styles.modeButton}
                    onClick={() => setMode("normal")}
                  >
                    Normal
                  </button>
                  <button
                    type="button"
                    className={mode === "plus" ? styles.modeButtonActive : styles.modeButton}
                    onClick={() => setMode("plus")}
                  >
                    Home Runs+
                  </button>
                </div>
              ) : null}

              <div className={styles.fieldGroup}>
                <span className={styles.fieldLabel}>Time Range</span>
                <div className={styles.timeRangeRow}>
                  {TIME_RANGE_OPTIONS.map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      className={timeRange === option.value ? styles.timeRangeButtonActive : styles.timeRangeButton}
                      onClick={() => setTimeRange(option.value)}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className={styles.fieldGroup}>
                <span className={styles.fieldLabel}>Difficulty</span>
                <button type="button" className={styles.dropdownTrigger} onClick={() => setDifficultyOpen((prev) => !prev)}>
                  <span>{difficulty === ALL_DIFFICULTIES ? "All difficulties" : difficulty}</span>
                  <span>{difficultyOpen ? "▲" : "▼"}</span>
                </button>
                {difficultyOpen ? (
                  <div className={styles.dropdownMenu}>
                    {[ALL_DIFFICULTIES, ...orderedDifficultyOptions].map((option) => (
                      <button
                        key={option}
                        type="button"
                        className={difficulty === option ? styles.dropdownOptionActive : styles.dropdownOption}
                        onClick={() => {
                          setDifficulty(option);
                          setDifficultyOpen(false);
                        }}
                      >
                        {option === ALL_DIFFICULTIES ? "All difficulties" : option}
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.fieldGroup}>
                <input
                  className={styles.input}
                  value={hitterUsername}
                  onChange={(event) => setHitterUsername(event.target.value)}
                  placeholder="Hitter username"
                  autoCapitalize="none"
                  autoCorrect="off"
                  spellCheck={false}
                />
                {hitterUsernameLoading ? <p className={styles.lookupHint}>Searching hitter usernames...</p> : null}
                {hitterUsernameOptions.length > 0 ? (
                  <div className={styles.suggestionWrap}>
                    {hitterUsernameOptions.map((profile) => (
                      <button
                        key={`hu-${profile.username}`}
                        type="button"
                        className={styles.suggestionItem}
                        onClick={() => {
                          setHitterUsername(profile.username);
                          setHitterUsernameOptions([]);
                        }}
                      >
                        <strong>@{profile.username}</strong>
                        <span>{profile.display_name || "Show user"}</span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.fieldGroup}>
                <input
                  className={styles.input}
                  value={pitcherUsername}
                  onChange={(event) => setPitcherUsername(event.target.value)}
                  placeholder="Pitcher username"
                  autoCapitalize="none"
                  autoCorrect="off"
                  spellCheck={false}
                />
                {pitcherUsernameLoading ? <p className={styles.lookupHint}>Searching pitcher usernames...</p> : null}
                {pitcherUsernameOptions.length > 0 ? (
                  <div className={styles.suggestionWrap}>
                    {pitcherUsernameOptions.map((profile) => (
                      <button
                        key={`pu-${profile.username}`}
                        type="button"
                        className={styles.suggestionItem}
                        onClick={() => {
                          setPitcherUsername(profile.username);
                          setPitcherUsernameOptions([]);
                        }}
                      >
                        <strong>@{profile.username}</strong>
                        <span>{profile.display_name || "Show user"}</span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.fieldGroup}>
                <span className={styles.fieldLabel}>Hitter MLB player</span>
                <input
                  className={styles.input}
                  value={hitterPlayerQuery}
                  onChange={(event) => {
                    setHitterPlayerQuery(event.target.value);
                    setHitterPlayerSelected(null);
                  }}
                  placeholder="Type player name..."
                  autoCapitalize="words"
                  autoCorrect="off"
                  spellCheck={false}
                />
                {hitterPlayerLoading ? <p className={styles.lookupHint}>Searching hitters...</p> : null}
                {hitterPlayerOptions.length > 0 ? (
                  <div className={styles.suggestionWrap}>
                    {hitterPlayerOptions.map((player) => (
                      <button
                        key={`h-${player.mlb_id}`}
                        type="button"
                        className={styles.suggestionItem}
                        onClick={() => {
                          setHitterPlayerSelected(player);
                          setHitterPlayerQuery(player.full_name);
                          setHitterPlayerOptions([]);
                        }}
                      >
                        <strong>{player.full_name}</strong>
                        <span>#{player.mlb_id}</span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.fieldGroup}>
                <span className={styles.fieldLabel}>Pitcher MLB player</span>
                <input
                  className={styles.input}
                  value={pitcherPlayerQuery}
                  onChange={(event) => {
                    setPitcherPlayerQuery(event.target.value);
                    setPitcherPlayerSelected(null);
                  }}
                  placeholder="Type player name..."
                  autoCapitalize="words"
                  autoCorrect="off"
                  spellCheck={false}
                />
                {pitcherPlayerLoading ? <p className={styles.lookupHint}>Searching pitchers...</p> : null}
                {pitcherPlayerOptions.length > 0 ? (
                  <div className={styles.suggestionWrap}>
                    {pitcherPlayerOptions.map((player) => (
                      <button
                        key={`p-${player.mlb_id}`}
                        type="button"
                        className={styles.suggestionItem}
                        onClick={() => {
                          setPitcherPlayerSelected(player);
                          setPitcherPlayerQuery(player.full_name);
                          setPitcherPlayerOptions([]);
                        }}
                      >
                        <strong>{player.full_name}</strong>
                        <span>#{player.mlb_id}</span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.actionsRow}>
                <button type="button" className={styles.applyButton} onClick={applyFilters}>
                  Apply
                </button>
                <button type="button" className={styles.clearButton} onClick={clearFilters}>
                  Clear
                </button>
              </div>
            </div>
          ) : null}
        </section>

        <section className={styles.leaderboardHeader}>
          <h2>{leaderboardTitle}</h2>
          <p>{leaderboardSubtitle}</p>
          {myTopOvrRank != null ? (
            <strong>
              {myTopLabel} {formatOrdinal(myTopOvrRank)} Ovr
            </strong>
          ) : null}
        </section>

        <section className={styles.metaBar}>
          <span>
            {startIndex}-{endIndex} of {total}
          </span>
          <div className={styles.paginationRow}>
            <button
              type="button"
              className={styles.pageButton}
              disabled={page <= 1 || loading}
              onClick={() => setPage((current) => Math.max(1, current - 1))}
            >
              Prev
            </button>
            <span className={styles.pageMetaText}>
              Page {page} / {totalPages}
            </span>
            <button
              type="button"
              className={styles.pageButton}
              disabled={page >= totalPages || loading}
              onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
            >
              Next
            </button>
          </div>
        </section>

        {loading ? <section className={styles.statusCard}>Loading records...</section> : null}
        {error ? <section className={styles.statusCardError}>{error}</section> : null}
        {!loading && !error && records.length === 0 ? <section className={styles.statusCard}>No records found.</section> : null}

        {!loading && !error && records.length > 0 ? (
          <section className={styles.recordsList}>
            {records.map((item, index) => {
              const key = `${item.game_id}-${item.event_id ?? index}`;
              const isExpanded = expandedKey === key;
              const value = isHardestLeaderboard
                ? (item.selected_exit_vel_mph ?? item.exit_vel_mph ?? null)
                : (item.selected_distance_ft ?? null);
              const valueText = isHardestLeaderboard ? formatExitVelocity(value) : formatDistance(value);
              const filteredRank = item.filtered_rank ?? (page - 1) * PAGE_SIZE + index + 1;
              const overallRank = item.selected_rank;
              const hitterUsernameText = item.hitter_username ? `@${item.hitter_username}` : "@unknown";

              return (
                <article key={key} className={styles.recordCard}>
                  <button
                    type="button"
                    className={styles.condensedRow}
                    onClick={() => setExpandedKey((current) => (current === key ? null : key))}
                  >
                    <span className={styles.rankBadge}>
                      <strong>#{filteredRank}</strong>
                      {overallRank != null ? <small>#{overallRank} Ovr</small> : null}
                    </span>
                    <span className={styles.condensedCenter}>
                      <strong
                        className={styles.profileLinkText}
                        onClick={(event) => {
                          event.stopPropagation();
                          navigateToUserStats(item.hitter_username);
                        }}
                      >
                        {hitterUsernameText}
                      </strong>
                      <small>{item.hitter_name || "Unknown hitter"}</small>
                    </span>
                    <span className={styles.condensedRight}>
                      <strong>{valueText}</strong>
                      <small>{isExpanded ? "▲" : "▼"}</small>
                    </span>
                  </button>

                  {isExpanded ? (
                    <div className={styles.detailWrap}>
                      <div className={styles.chipRow}>
                        <span className={styles.detailChip}>{item.difficulty || "Unknown Difficulty"}</span>
                        <span className={styles.detailChip}>{formatDate(item.date)}</span>
                      </div>

                      <div className={styles.profilesRow}>
                        <div className={styles.profileBlock}>
                          <span className={styles.detailLabel}>Hitting Profile</span>
                          <button
                            type="button"
                            className={styles.detailProfileButton}
                            onClick={() => navigateToUserStats(item.hitter_username)}
                          >
                            @{item.hitter_username || "unknown"}
                          </button>
                        </div>
                        <div className={styles.profileBlock}>
                          <span className={styles.detailLabel}>Pitching Profile</span>
                          <button
                            type="button"
                            className={styles.detailProfileButton}
                            onClick={() => navigateToUserStats(item.pitcher_username)}
                          >
                            @{item.pitcher_username || "unknown"}
                          </button>
                        </div>
                      </div>

                      <div className={styles.playerRow}>
                        <span className={styles.detailLabel}>Hitter</span>
                        <strong>{item.hitter_name || formatMlbId(item.batter_mlb_id)}</strong>
                      </div>
                      <div className={styles.playerRow}>
                        <span className={styles.detailLabel}>Pitcher</span>
                        <strong>{item.pitcher_name || formatMlbId(item.pitcher_mlb_id)}</strong>
                      </div>
                      <div className={styles.playerRow}>
                        <span className={styles.detailLabel}>
                          {isHardestLeaderboard ? "Exit Velocity" : appliedMode === "plus" ? "Distance+" : "Distance"}
                        </span>
                        <strong>{valueText}</strong>
                      </div>
                      <div className={styles.playerRow}>
                        <span className={styles.detailLabel}>Ball Park</span>
                        <strong>{item.ball_park_name || "Unknown"}</strong>
                      </div>
                      {!isHardestLeaderboard ? (
                        <div className={styles.playerRow}>
                          <span className={styles.detailLabel}>Elevation</span>
                          <strong>{formatElevation(item.elevation)}</strong>
                        </div>
                      ) : null}
                    </div>
                  ) : null}
                </article>
              );
            })}
          </section>
        ) : null}
      </div>
    </main>
  );
}
