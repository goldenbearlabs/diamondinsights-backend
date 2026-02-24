"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { formatDate } from "./format";
import type {
  ShowBatterBoxscore,
  ShowGameBundle,
  ShowGameEvent,
  ShowGameLogItem,
  ShowHalfInningSummary,
  ShowPitcherBoxscore,
  ShowPlateAppearance,
} from "./types";
import styles from "./styles.module.css";
import { ApiError, apiGetAuth } from "@/lib/api";
import { toReadableAuthError } from "@/lib/auth-errors";

type Props = {
  game: ShowGameLogItem;
  username: string | null;
  onBack: () => void;
};

type HalfInningSection = {
  key: string;
  inning: number;
  isHomeBatting: boolean;
  summary: ShowHalfInningSummary | null;
  data: ShowGameEvent[];
};

const STICKY_HALF_HEADER_HEIGHT = 56;

function perspective(game: ShowGameLogItem, username: string | null) {
  const normalized = username ? username.toLowerCase() : null;
  const isHome = normalized !== null && (game.home_profile_username || "").toLowerCase() === normalized;
  const isAway = normalized !== null && (game.away_profile_username || "").toLowerCase() === normalized;

  const userResult = isHome ? game.home_result : isAway ? game.away_result : null;
  const scoreFor = isHome ? game.home_runs : isAway ? game.away_runs : game.home_runs;
  const scoreAgainst = isHome ? game.away_runs : isAway ? game.home_runs : game.away_runs;
  const opponentName = isHome
    ? game.away_full_name || game.away_profile_username
    : isAway
      ? game.home_full_name || game.home_profile_username
      : game.away_full_name || game.away_profile_username;
  const location = isHome ? "vs" : isAway ? "@" : "vs";

  return {
    userResult,
    scoreFor,
    scoreAgainst,
    opponentName,
    location,
  };
}

function halfKey(inning?: number, isHomeBatting?: boolean) {
  if (!inning) {
    return "unknown";
  }
  return `${inning}:${isHomeBatting ? "H" : "A"}`;
}

function formatHalfInning(inning?: number, isHomeBatting?: boolean) {
  if (!inning) {
    return "-";
  }
  return `${isHomeBatting ? "Bot" : "Top"} ${inning}`;
}

function formatScore(away?: number | null, home?: number | null) {
  if (away === null || away === undefined || home === null || home === undefined) {
    return "-";
  }
  return `${away}-${home}`;
}

function formatOuts(outs?: number | null) {
  if (outs === null || outs === undefined) {
    return "-";
  }
  return `${outs} out${outs === 1 ? "" : "s"}`;
}

function formatEventType(value?: string | null) {
  if (!value) {
    return "Event";
  }
  return value
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function getResultTone(pa?: ShowPlateAppearance) {
  if (!pa) {
    return "neutral";
  }
  if (pa.is_out) {
    return "out";
  }
  if (pa.result) {
    return "hit";
  }
  return "neutral";
}

function formatRate(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  const fixed = value.toFixed(3);
  return value < 1 ? fixed.replace(/^0/, "") : fixed;
}

function formatEra(value?: number | null) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(2);
}

function computeBattingStats(row: ShowBatterBoxscore) {
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

export function GameDetailSection({ game, username, onBack }: Props) {
  const [events, setEvents] = useState<ShowGameEvent[]>([]);
  const [halfInnings, setHalfInnings] = useState<ShowHalfInningSummary[]>([]);
  const [plateAppearances, setPlateAppearances] = useState<ShowPlateAppearance[]>([]);
  const [batterBoxscores, setBatterBoxscores] = useState<ShowBatterBoxscore[]>([]);
  const [pitcherBoxscores, setPitcherBoxscores] = useState<ShowPitcherBoxscore[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [boxscoreTeam, setBoxscoreTeam] = useState<"home" | "away">("home");
  const [stickyKey, setStickyKey] = useState<string | null>(null);
  const eventsListRef = useRef<HTMLDivElement | null>(null);
  const sectionRefs = useRef<Record<string, HTMLDivElement | null>>({});

  useEffect(() => {
    setBoxscoreTeam("home");
  }, [game.game_id]);

  useEffect(() => {
    let active = true;

    const loadGameBundle = async () => {
      setLoading(true);
      setError(null);

      try {
        const data = await apiGetAuth<ShowGameBundle>(`/users/me/show/game-bundle/${encodeURIComponent(game.game_id)}`);
        if (!active) {
          return;
        }

        const parsedEvents = Array.isArray(data?.events) ? data.events : [];
        parsedEvents.sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));

        setEvents(parsedEvents);
        setHalfInnings(Array.isArray(data?.half_innings) ? data.half_innings : []);
        setPlateAppearances(Array.isArray(data?.plate_appearances) ? data.plate_appearances : []);
        setBatterBoxscores(Array.isArray(data?.batter_boxscores) ? data.batter_boxscores : []);
        setPitcherBoxscores(Array.isArray(data?.pitcher_boxscores) ? data.pitcher_boxscores : []);
      } catch (loadError: unknown) {
        if (!active) {
          return;
        }

        setEvents([]);
        setHalfInnings([]);
        setPlateAppearances([]);
        setBatterBoxscores([]);
        setPitcherBoxscores([]);
        if (loadError instanceof ApiError && loadError.status === 404) {
          setError("Game events not available.");
        } else {
          setError(toReadableAuthError(loadError, "Unable to load game events"));
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    void loadGameBundle();

    return () => {
      active = false;
    };
  }, [game.game_id]);

  const view = useMemo(() => perspective(game, username), [game, username]);
  const matchup = `${view.location} ${view.opponentName}`;

  const paBySeq = useMemo(() => {
    const map = new Map<number, ShowPlateAppearance>();
    plateAppearances.forEach((pa) => {
      if (typeof pa.event_seq === "number") {
        map.set(pa.event_seq, pa);
      }
    });
    return map;
  }, [plateAppearances]);

  const summaryByHalf = useMemo(() => {
    const map = new Map<string, ShowHalfInningSummary>();
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
      const isHomeBatting = Boolean(event.is_home_batting);
      const key = halfKey(inning, isHomeBatting);
      if (key !== currentKey) {
        currentKey = key;
        currentSection = {
          key,
          inning,
          isHomeBatting,
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
    if (!sections.length) {
      return null;
    }
    if (!stickyKey) {
      return sections[0];
    }
    return sections.find((section) => section.key === stickyKey) ?? sections[0];
  }, [sections, stickyKey]);

  const homeBatters = useMemo(
    () => batterBoxscores.filter((row) => row.is_home).sort((a, b) => a.appearance_idx - b.appearance_idx),
    [batterBoxscores],
  );
  const awayBatters = useMemo(
    () => batterBoxscores.filter((row) => !row.is_home).sort((a, b) => a.appearance_idx - b.appearance_idx),
    [batterBoxscores],
  );
  const homePitchers = useMemo(
    () => pitcherBoxscores.filter((row) => row.is_home).sort((a, b) => a.appearance_idx - b.appearance_idx),
    [pitcherBoxscores],
  );
  const awayPitchers = useMemo(
    () => pitcherBoxscores.filter((row) => !row.is_home).sort((a, b) => a.appearance_idx - b.appearance_idx),
    [pitcherBoxscores],
  );
  const activeBatters = boxscoreTeam === "home" ? homeBatters : awayBatters;
  const activePitchers = boxscoreTeam === "home" ? homePitchers : awayPitchers;

  useEffect(() => {
    if (boxscoreTeam === "home" && homeBatters.length === 0 && awayBatters.length > 0) {
      setBoxscoreTeam("away");
    }
  }, [awayBatters.length, boxscoreTeam, homeBatters.length]);

  useEffect(() => {
    setStickyKey(sections[0]?.key ?? null);
  }, [sections]);

  useEffect(() => {
    const listEl = eventsListRef.current;
    if (!listEl || sections.length === 0) {
      return;
    }

    const onScroll = () => {
      const threshold = listEl.scrollTop + STICKY_HALF_HEADER_HEIGHT;
      let nextKey = sections[0]?.key ?? null;
      for (const section of sections) {
        const node = sectionRefs.current[section.key];
        if (!node) {
          continue;
        }
        if (node.offsetTop <= threshold) {
          nextKey = section.key;
        } else {
          break;
        }
      }
      setStickyKey((prev) => (prev === nextKey ? prev : nextKey));
    };

    onScroll();
    listEl.addEventListener("scroll", onScroll, { passive: true });
    return () => {
      listEl.removeEventListener("scroll", onScroll);
    };
  }, [sections]);

  useEffect(() => {
    const listEl = eventsListRef.current;
    if (listEl) {
      listEl.scrollTop = 0;
    }
  }, [game.game_id]);

  return (
    <section className={styles.gameLogSection}>
      <div className={styles.sectionHeaderRow}>
        <button type="button" className={styles.gameDetailBackButton} onClick={onBack}>
          Back to Game Log
        </button>
      </div>

      <div className={styles.gameDetailHeader}>
        <div className={styles.gameDetailHeaderTop}>
          <h3 className={styles.sectionTitle}>Game</h3>
          <span
            className={`${styles.gameDetailResultPill} ${
              view.userResult === "W"
                ? styles.gameDetailResultPillWin
                : view.userResult === "L"
                  ? styles.gameDetailResultPillLoss
                  : styles.gameDetailResultPillNeutral
            }`}
          >
            {view.userResult || "-"}
          </span>
        </div>
        <p className={styles.gameDetailMatchup}>{matchup}</p>
        <div className={styles.gameDetailMetaGrid}>
          <div className={styles.gameDetailMetaItem}>
            <span>Date</span>
            <strong>{formatDate(game.date)}</strong>
          </div>
          <div className={styles.gameDetailMetaItem}>
            <span>Score</span>
            <strong>
              {view.scoreFor}-{view.scoreAgainst}
            </strong>
          </div>
          <div className={styles.gameDetailMetaItem}>
            <span>Difficulty</span>
            <strong>{game.difficulty || "-"}</strong>
          </div>
          <div className={styles.gameDetailMetaItem}>
            <span>Ballpark</span>
            <strong>{game.ball_park_name || "-"}</strong>
          </div>
        </div>
      </div>

      {loading ? <p className={styles.muted}>Loading game events...</p> : null}
      {error ? <p className={styles.error}>{error}</p> : null}

      <div className={styles.gameDetailGrid}>
        <section className={styles.gameDetailCard}>
          <div className={`${styles.sectionHeaderRow} ${styles.gameDetailSectionHeader}`}>
            <h4 className={styles.sectionTitle}>Event Log</h4>
          </div>
          <div className={styles.gameEventsList} ref={eventsListRef}>
            {stickySection ? (
              <div className={`${styles.gameHalfHeader} ${styles.gameHalfHeaderSticky}`}>
                <div className={styles.gameHalfHeaderTop}>
                  <p className={styles.gameHalfTitle}>{formatHalfInning(stickySection.inning, stickySection.isHomeBatting)}</p>
                  <p className={styles.gameHalfTeam}>
                    {stickySection.isHomeBatting ? game.home_full_name : game.away_full_name}
                  </p>
                </div>
                {stickySection.summary ? (
                  <p className={styles.gameHalfMeta}>
                    R {stickySection.summary.runs} • H {stickySection.summary.hits} • BB {stickySection.summary.walks} • E{" "}
                    {stickySection.summary.errors} • P {stickySection.summary.pitches} • LOB{" "}
                    {stickySection.summary.runners_left_on}
                  </p>
                ) : null}
              </div>
            ) : null}
            {!loading && !error && sections.length === 0 ? <p className={styles.muted}>No events found.</p> : null}
            {sections.map((section) => (
              <div
                key={section.key}
                className={styles.gameHalfSection}
                ref={(node) => {
                  sectionRefs.current[section.key] = node;
                }}
              >
                <div className={styles.gameHalfHeader}>
                  <div className={styles.gameHalfHeaderTop}>
                    <p className={styles.gameHalfTitle}>{formatHalfInning(section.inning, section.isHomeBatting)}</p>
                    <p className={styles.gameHalfTeam}>
                      {section.isHomeBatting ? game.home_full_name : game.away_full_name}
                    </p>
                  </div>
                  {section.summary ? (
                    <p className={styles.gameHalfMeta}>
                      R {section.summary.runs} • H {section.summary.hits} • BB {section.summary.walks} • E{" "}
                      {section.summary.errors} • P {section.summary.pitches} • LOB {section.summary.runners_left_on}
                    </p>
                  ) : null}
                </div>
                {section.data.map((item) => {
                  const pa = typeof item.seq === "number" ? paBySeq.get(item.seq) : undefined;
                  const resultLabel = pa?.result ? formatEventType(pa.result) : formatEventType(item.event_type);
                  const tone = getResultTone(pa);
                  return (
                    <article key={`${item.game_id}-${item.seq}`} className={styles.gameEventRow}>
                      <div className={styles.gameEventTop}>
                        <p className={styles.gameEventMeta}>
                          {item.is_home_batting ? game.home_full_name : game.away_full_name} batting
                        </p>
                        <p className={styles.gameEventMeta}>{formatScore(item.away_score_after, item.home_score_after)}</p>
                        <p className={styles.gameEventMeta}>{formatOuts(item.outs_after)}</p>
                        <span
                          className={`${styles.gameEventResultPill} ${
                            tone === "hit"
                              ? styles.gameEventResultHit
                              : tone === "out"
                                ? styles.gameEventResultOut
                                : styles.gameEventResultNeutral
                          }`}
                        >
                          {resultLabel}
                        </span>
                      </div>
                      <div className={styles.gameEventPlayers}>
                        <p>Batter: {pa?.batter_name_raw || "-"}</p>
                        <p>Pitcher: {pa?.pitcher_name_raw || "-"}</p>
                      </div>
                      <div className={styles.gameEventBases}>
                        <span className={`${styles.baseIndicator} ${item.pre_on_1b ? styles.baseIndicatorActive : ""}`}>1B</span>
                        <span className={`${styles.baseIndicator} ${item.pre_on_2b ? styles.baseIndicatorActive : ""}`}>2B</span>
                        <span className={`${styles.baseIndicator} ${item.pre_on_3b ? styles.baseIndicatorActive : ""}`}>3B</span>
                      </div>
                      <p className={styles.gameEventText}>{item.event_text || "Event recorded."}</p>
                    </article>
                  );
                })}
              </div>
            ))}
          </div>
        </section>

        <section className={styles.gameDetailCard}>
          <div className={`${styles.sectionHeaderRow} ${styles.gameDetailSectionHeader}`}>
            <h4 className={styles.sectionTitle}>Boxscore</h4>
            <div className={styles.toggle}>
              <button
                type="button"
                className={boxscoreTeam === "home" ? styles.toggleButtonActive : styles.toggleButton}
                onClick={() => setBoxscoreTeam("home")}
              >
                {game.home_full_name || "Home"}
              </button>
              <button
                type="button"
                className={boxscoreTeam === "away" ? styles.toggleButtonActive : styles.toggleButton}
                onClick={() => setBoxscoreTeam("away")}
              >
                {game.away_full_name || "Away"}
              </button>
            </div>
          </div>

          <h5 className={styles.tableSubtitle}>Batters</h5>
          <div className={styles.tableShell}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Player</th>
                  <th>AB</th>
                  <th>R</th>
                  <th>H</th>
                  <th>RBI</th>
                  <th>BB</th>
                  <th>SO</th>
                  <th>2B</th>
                  <th>3B</th>
                  <th>HR</th>
                  <th>AVG</th>
                  <th>OBP</th>
                  <th>SLG</th>
                  <th>OPS</th>
                  <th>SB</th>
                  <th>CS</th>
                  <th>SB%</th>
                  <th>E</th>
                  <th>POS</th>
                  <th>INN</th>
                </tr>
              </thead>
              <tbody>
                {activeBatters.map((row) => {
                  const stats = computeBattingStats(row);
                  return (
                    <tr key={`${row.game_id}:${row.appearance_idx}`}>
                      <td>{row.player_name_raw}</td>
                      <td>{row.ab}</td>
                      <td>{row.r}</td>
                      <td>{row.h}</td>
                      <td>{row.rbi}</td>
                      <td>{row.bb}</td>
                      <td>{row.so}</td>
                      <td>{row.doubles}</td>
                      <td>{row.triples}</td>
                      <td>{row.hr}</td>
                      <td>{formatRate(stats.avg)}</td>
                      <td>{formatRate(stats.obp)}</td>
                      <td>{formatRate(stats.slg)}</td>
                      <td>{formatRate(stats.ops)}</td>
                      <td>{row.sb}</td>
                      <td>{row.cs}</td>
                      <td>{formatRate(stats.sbPct)}</td>
                      <td>{row.e}</td>
                      <td>{row.pos}</td>
                      <td>{row.innings}</td>
                    </tr>
                  );
                })}
                {!loading && !error && activeBatters.length === 0 ? (
                  <tr>
                    <td colSpan={20} className={styles.tableEmptyCell}>
                      No batter boxscore rows.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>

          <h5 className={styles.tableSubtitle}>Pitchers</h5>
          <div className={styles.tableShell}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th>Player</th>
                  <th>IP</th>
                  <th>H</th>
                  <th>R</th>
                  <th>ER</th>
                  <th>BB</th>
                  <th>SO</th>
                  <th>ERA</th>
                  <th>WP</th>
                  <th>W</th>
                  <th>L</th>
                  <th>SV</th>
                  <th>HLD</th>
                </tr>
              </thead>
              <tbody>
                {activePitchers.map((row) => (
                  <tr key={`${row.game_id}:${row.appearance_idx}`}>
                    <td>{row.player_name_raw}</td>
                    <td>{row.ip_raw}</td>
                    <td>{row.h}</td>
                    <td>{row.r}</td>
                    <td>{row.er}</td>
                    <td>{row.bb}</td>
                    <td>{row.so}</td>
                    <td>{formatEra(row.era)}</td>
                    <td>{row.wp}</td>
                    <td>{row.win}</td>
                    <td>{row.loss}</td>
                    <td>{row.save}</td>
                    <td>{row.hold}</td>
                  </tr>
                ))}
                {!loading && !error && activePitchers.length === 0 ? (
                  <tr>
                    <td colSpan={13} className={styles.tableEmptyCell}>
                      No pitcher boxscore rows.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </section>
  );
}
