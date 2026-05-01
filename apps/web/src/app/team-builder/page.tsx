"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { GripVertical, Lock, Search, Sparkles, Wand2, X } from "lucide-react";
import { useRouter } from "next/navigation";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { apiGetAuth, getMyEntitlements } from "@/lib/api";
import { toReadableAuthError } from "@/lib/auth-errors";
import { CURRENT_CARD_YEAR } from "@/lib/config";
import { getFirebaseAuth } from "@/lib/firebase";
import {
  ALL_SLOT_KEYS,
  BATTING_LINEUP_SLOT_KEYS,
  BENCH_SLOT_KEYS,
  BULLPEN_SLOT_KEYS,
  EMPTY_ROSTER,
  MODE_OPTIONS,
  POSITION_FILTER_OPTIONS,
  ROTATION_SLOT_KEYS,
  SLOT_META,
  VALUE_METRIC_OPTIONS,
  cardMatchesSlot,
  compareCardsForSlot,
  formatMetric,
  generateBatterRoster,
  generatePitcherRoster,
  getAttributesForMode,
  getHandText,
  getMetaOverallValue,
  getModeSlotKeys,
  getPlayerIdentityKey,
  getPrimaryPosition,
  getSlotMetricValue,
  getTrueOverallValue,
  getYourOverallValue,
  isSlotRequiringPositionFilter,
  moveItem,
  type AnySlotKey,
  type BattingLineupSlotKey,
  type PositionFilterMode,
  type RosterMode,
  type TeamCard,
  type ValueMetric,
} from "@/lib/team-builder";

import styles from "./page.module.css";

const BUILDER_YEAR = CURRENT_CARD_YEAR;

async function fetchCardsForBuilder(path: string): Promise<TeamCard[]> {
  return apiGetAuth<TeamCard[]>(path);
}

function buildFilterDescription(
  targetPosition: string,
  activeSlotLabel: string,
  positionFilterMode: PositionFilterMode,
): string {
  if (targetPosition === "DH") {
    return "DH can use any hitter card. Duplicate players are excluded automatically.";
  }
  if (targetPosition === "BENCH") {
    return "Bench can use any hitter card. Duplicate players are excluded automatically.";
  }
  if (targetPosition === "BP") {
    if (positionFilterMode === "all") {
      return "Showing all pitcher cards.";
    }
    if (positionFilterMode === "secondary") {
      return "Showing bullpen cards by primary and secondary positions (RP/CP).";
    }
    return "Showing primary bullpen cards only (RP/CP).";
  }
  if (targetPosition === "SP") {
    if (positionFilterMode === "all") {
      return "Showing all pitcher cards.";
    }
    if (positionFilterMode === "secondary") {
      return "Showing SP cards by primary and secondary positions.";
    }
    return "Showing SP primary-position cards only.";
  }
  if (positionFilterMode === "all") {
    return "Showing all cards for this roster mode.";
  }
  if (positionFilterMode === "secondary") {
    return `Showing ${activeSlotLabel} cards by primary and secondary position.`;
  }
  return `Showing ${activeSlotLabel} primary-position cards only.`;
}

export default function TeamBuilderPage() {
  const router = useRouter();

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  const [isPro, setIsPro] = useState<boolean | null>(null);

  const [mode, setMode] = useState<RosterMode>("batters");
  const [roster, setRoster] = useState<Record<AnySlotKey, TeamCard | null>>(() => ({ ...EMPTY_ROSTER }));
  const [battingOrder, setBattingOrder] = useState<BattingLineupSlotKey[]>([...BATTING_LINEUP_SLOT_KEYS]);

  const [activeSlot, setActiveSlot] = useState<AnySlotKey | null>(null);
  const [searchText, setSearchText] = useState("");
  const [results, setResults] = useState<TeamCard[]>([]);
  const [loading, setLoading] = useState(false);
  const [pickerError, setPickerError] = useState<string | null>(null);
  const [pageError, setPageError] = useState<string | null>(null);

  const [positionFilterMode, setPositionFilterMode] = useState<PositionFilterMode>("primary");
  const [teamMetric, setTeamMetric] = useState<ValueMetric>("meta");
  const [sortMetric, setSortMetric] = useState<ValueMetric>("meta");

  const [allHitterCardsCache, setAllHitterCardsCache] = useState<TeamCard[] | null>(null);
  const [allPitcherCardsCache, setAllPitcherCardsCache] = useState<TeamCard[] | null>(null);
  const [generating, setGenerating] = useState(false);

  const [draggingSlot, setDraggingSlot] = useState<BattingLineupSlotKey | null>(null);
  const [dragOverSlot, setDragOverSlot] = useState<BattingLineupSlotKey | null>(null);

  const requestIdRef = useRef(0);

  const hasProAccess = isPro === true;
  const isMetricLocked = (metric: ValueMetric) => metric === "your" && !hasProAccess;

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;

    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        setFirebaseUser(user);
        setAuthReady(true);
        if (!user) {
          router.replace("/signin");
        }
      });
    } catch (error: unknown) {
      queueMicrotask(() => {
        setAuthError(toReadableAuthError(error, "Firebase auth is not configured for web."));
        setAuthReady(true);
      });
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!firebaseUser) {
      setIsPro(null);
      return;
    }

    let active = true;
    void getMyEntitlements()
      .then((payload) => {
        if (active) {
          setIsPro(Boolean(payload.has_pro));
        }
      })
      .catch(() => {
        if (active) {
          setIsPro(false);
        }
      });

    return () => {
      active = false;
    };
  }, [firebaseUser]);

  useEffect(() => {
    setActiveSlot(null);
  }, [mode]);

  useEffect(() => {
    if (hasProAccess) {
      return;
    }
    if (teamMetric === "your") {
      setTeamMetric("meta");
    }
    if (sortMetric === "your") {
      setSortMetric("meta");
    }
  }, [hasProAccess, sortMetric, teamMetric]);

  const currentModeSlotKeys = useMemo(() => getModeSlotKeys(mode), [mode]);

  const selectedCardsForMode = useMemo(
    () =>
      currentModeSlotKeys
        .map((slotKey) => roster[slotKey])
        .filter((card): card is TeamCard => card !== null),
    [currentModeSlotKeys, roster],
  );

  const selectedCount = selectedCardsForMode.length;
  const totalCount = currentModeSlotKeys.length;
  const activeSlotMeta = useMemo(() => (activeSlot ? SLOT_META[activeSlot] : null), [activeSlot]);
  const activeSlotLabel = activeSlotMeta?.label ?? "";

  const metricLabel = useMemo(
    () => VALUE_METRIC_OPTIONS.find((option) => option.key === teamMetric)?.label ?? "Overall",
    [teamMetric],
  );

  const averageOverall = useMemo(() => {
    const metricValues = currentModeSlotKeys
      .map((slotKey) => {
        const card = roster[slotKey];
        if (!card) {
          return null;
        }
        return getSlotMetricValue(card, teamMetric, SLOT_META[slotKey]);
      })
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));

    if (metricValues.length === 0) {
      return null;
    }

    const total = metricValues.reduce((sum, value) => sum + value, 0);
    return total / metricValues.length;
  }, [currentModeSlotKeys, roster, teamMetric]);

  const filteredResults = useMemo(() => {
    if (!activeSlotMeta) {
      return [];
    }

    const blockedPlayers = new Set<string>();
    for (const slotKey of ALL_SLOT_KEYS) {
      if (slotKey === activeSlotMeta.key) {
        continue;
      }
      const selected = roster[slotKey];
      if (!selected) {
        continue;
      }
      blockedPlayers.add(getPlayerIdentityKey(selected));
    }

    return results.filter((card) => {
      if (!cardMatchesSlot(card, activeSlotMeta, positionFilterMode)) {
        return false;
      }
      if (blockedPlayers.has(getPlayerIdentityKey(card))) {
        return false;
      }
      return true;
    });
  }, [activeSlotMeta, positionFilterMode, results, roster]);

  const sortedFilteredResults = useMemo(() => {
    if (!activeSlotMeta) {
      return filteredResults;
    }
    return [...filteredResults].sort((a, b) => compareCardsForSlot(a, b, sortMetric, activeSlotMeta));
  }, [activeSlotMeta, filteredResults, sortMetric]);

  const filterDescription = useMemo(() => {
    if (!activeSlotMeta) {
      return "";
    }
    return buildFilterDescription(activeSlotMeta.targetPosition, activeSlotLabel, positionFilterMode);
  }, [activeSlotLabel, activeSlotMeta, positionFilterMode]);

  useEffect(() => {
    if (!activeSlotMeta) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      void loadCards(searchText, activeSlotMeta.mode);
    }, 250);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [activeSlotMeta, searchText]);

  useEffect(() => {
    if (activeSlotMeta) {
      return;
    }
    setSearchText("");
    setResults([]);
    setPickerError(null);
    setPositionFilterMode("primary");
  }, [activeSlotMeta]);

  const loadCards = async (query: string, rosterMode: RosterMode) => {
    const requestId = ++requestIdRef.current;
    setLoading(true);
    setPickerError(null);

    try {
      const trimmed = query.trim();
      const buildUrl = (offset: number) => {
        const params = new URLSearchParams({
          year: BUILDER_YEAR,
          is_hitter: rosterMode === "batters" ? "true" : "false",
          limit: "100",
          offset: String(offset),
        });
        if (trimmed) {
          params.set("name", trimmed);
        }
        return `/cards/?${params.toString()}`;
      };

      const firstBatch = await fetchCardsForBuilder(buildUrl(0));
      let cards = firstBatch;

      if (!trimmed && firstBatch.length === 100) {
        const secondBatch = await fetchCardsForBuilder(buildUrl(100));
        cards = [...firstBatch, ...secondBatch];
      }

      if (requestId !== requestIdRef.current) {
        return;
      }

      const unique = new Map<string, TeamCard>();
      for (const card of cards) {
        unique.set(card.id, card);
      }
      setResults(Array.from(unique.values()).filter((card) => card.year === Number(BUILDER_YEAR)));
    } catch (error: unknown) {
      if (requestId !== requestIdRef.current) {
        return;
      }
      if (error instanceof Error && error.message === "Not authenticated") {
        setPickerError("Sign in to load personalized cards.");
      } else {
        setPickerError("Could not load cards. Try again.");
      }
    } finally {
      if (requestId === requestIdRef.current) {
        setLoading(false);
      }
    }
  };

  const assignCardToSlot = (card: TeamCard) => {
    if (!activeSlotMeta) {
      return;
    }
    const nextPlayerKey = getPlayerIdentityKey(card);

    setRoster((previous) => {
      const next = { ...previous };
      for (const slotKey of ALL_SLOT_KEYS) {
        const existing = next[slotKey];
        if (!existing) {
          continue;
        }
        if (existing.id === card.id || getPlayerIdentityKey(existing) === nextPlayerKey) {
          next[slotKey] = null;
        }
      }
      next[activeSlotMeta.key] = card;
      return next;
    });

    setActiveSlot(null);
  };

  const clearSlot = (slotKey: AnySlotKey) => {
    setRoster((previous) => ({ ...previous, [slotKey]: null }));
  };

  const openCardPicker = (slotKey: AnySlotKey) => {
    setSortMetric(isMetricLocked(teamMetric) ? "meta" : teamMetric);
    setActiveSlot(slotKey);
  };

  const loadAllCardsForMode = async (rosterMode: RosterMode) => {
    if (rosterMode === "batters" && allHitterCardsCache) {
      return allHitterCardsCache;
    }
    if (rosterMode === "pitchers" && allPitcherCardsCache) {
      return allPitcherCardsCache;
    }

    const pageSize = 100;
    const maxPages = 40;
    const collected: TeamCard[] = [];

    for (let page = 0; page < maxPages; page += 1) {
      const offset = page * pageSize;
      const params = new URLSearchParams({
        year: BUILDER_YEAR,
        is_hitter: rosterMode === "batters" ? "true" : "false",
        limit: String(pageSize),
        offset: String(offset),
      });

      const batch = await fetchCardsForBuilder(`/cards/?${params.toString()}`);
      collected.push(...batch);
      if (batch.length < pageSize) {
        break;
      }
    }

    const unique = new Map<string, TeamCard>();
    for (const card of collected) {
      unique.set(card.id, card);
    }

    const cached = Array.from(unique.values()).filter((card) => card.year === Number(BUILDER_YEAR));
    if (rosterMode === "batters") {
      setAllHitterCardsCache(cached);
    } else {
      setAllPitcherCardsCache(cached);
    }
    return cached;
  };

  const handleGenerateGreedy = async () => {
    if (generating) {
      return;
    }

    setGenerating(true);
    setPageError(null);

    try {
      const [hitterPool, pitcherPool] = await Promise.all([loadAllCardsForMode("batters"), loadAllCardsForMode("pitchers")]);
      const usedPlayers = new Set<string>();
      const generatedBatters = generateBatterRoster(hitterPool, usedPlayers, teamMetric);
      const generatedPitchers = generatePitcherRoster(pitcherPool, usedPlayers, teamMetric);

      setBattingOrder(generatedBatters.optimizedOrder);
      setRoster((previous) => ({ ...previous, ...generatedBatters.generated, ...generatedPitchers }));
    } catch (error: unknown) {
      if (error instanceof Error && error.message === "Not authenticated") {
        setPageError("Sign in to generate a personalized team.");
      } else {
        setPageError("Could not generate a team right now.");
      }
    } finally {
      setGenerating(false);
    }
  };

  const onLineupDragStart = (slotKey: BattingLineupSlotKey) => {
    setDraggingSlot(slotKey);
    setDragOverSlot(slotKey);
  };

  const onLineupDragOver = (event: React.DragEvent<HTMLElement>, slotKey: BattingLineupSlotKey) => {
    if (!draggingSlot) {
      return;
    }
    event.preventDefault();
    if (draggingSlot === slotKey) {
      return;
    }

    setBattingOrder((previous) => {
      const fromIndex = previous.indexOf(draggingSlot);
      const toIndex = previous.indexOf(slotKey);
      if (fromIndex < 0 || toIndex < 0 || fromIndex === toIndex) {
        return previous;
      }
      return moveItem(previous, fromIndex, toIndex);
    });
    setDragOverSlot(slotKey);
  };

  const endLineupDrag = () => {
    setDraggingSlot(null);
    setDragOverSlot(null);
  };

  const renderRosterRow = (slotKey: AnySlotKey, orderIndex?: number) => {
    const slotMeta = SLOT_META[slotKey];
    const card = roster[slotKey];
    const isActive = activeSlotMeta?.key === slotKey;
    const isDraggable = slotMeta.draggable && mode === "batters";
    const isLineupSlot = typeof orderIndex === "number";
    const lineupKey = slotKey as BattingLineupSlotKey;
    const isDragging = draggingSlot === lineupKey;
    const isDragTarget = dragOverSlot === lineupKey && draggingSlot !== null && draggingSlot !== lineupKey;

    return (
      <article
        key={slotKey}
        className={`${styles.rowCard} ${isActive ? styles.rowCardActive : ""} ${isDragging ? styles.rowCardDragging : ""} ${
          isDragTarget ? styles.rowCardDragTarget : ""
        }`}
        draggable={isDraggable}
        onDragStart={() => {
          if (isDraggable) {
            onLineupDragStart(lineupKey);
          }
        }}
        onDragOver={(event) => {
          if (isDraggable) {
            onLineupDragOver(event, lineupKey);
          }
        }}
        onDrop={(event) => {
          event.preventDefault();
          endLineupDrag();
        }}
        onDragEnd={endLineupDrag}
        onClick={() => openCardPicker(slotKey)}
      >
        <div className={styles.rowHeader}>
          <div className={styles.orderPill}>{isLineupSlot ? orderIndex + 1 : slotMeta.label}</div>
          <div className={styles.rowTitleBlock}>
            <span className={styles.slotLabel}>{slotMeta.label}</span>
            <span className={styles.slotSubtitle}>
              {slotMeta.section === "lineup" ? "Lineup slot" : slotMeta.section === "bench" ? "Bench slot" : slotMeta.section === "rotation" ? "Rotation slot" : "Bullpen slot"}
            </span>
          </div>

          {isDraggable ? (
            <div className={styles.dragHandle} aria-hidden>
              <GripVertical size={16} />
            </div>
          ) : null}

          <button
            type="button"
            className={styles.clearButton}
            onClick={(event) => {
              event.stopPropagation();
              clearSlot(slotKey);
            }}
            aria-label={card ? `Clear ${slotMeta.label}` : `Select ${slotMeta.label}`}
          >
            {card ? <X size={16} /> : "+"}
          </button>
        </div>

        {card ? (
          <div className={styles.cardBody}>
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img src={card.baked_img} alt={card.name} className={styles.cardImage} />

            <div className={styles.cardMeta}>
              <p className={styles.cardName}>
                {card.name} <span>· {getHandText(card, slotMeta.mode)}</span>
              </p>
              <p className={styles.cardDetail}>
                {card.team_short_name} · {getPrimaryPosition(card)} · Year {card.year}
              </p>
              <p className={styles.attrText}>{getAttributesForMode(card, slotMeta.mode).join("  ·  ")}</p>

              <div className={styles.metricRow}>
                <span className={`${styles.metricPill} ${styles.metricCurrent}`}>OVR {formatMetric(card.ovr)}</span>
                <span className={`${styles.metricPill} ${styles.metricTrue}`}>TRUE {formatMetric(getSlotMetricValue(card, "true", slotMeta))}</span>
                <span className={`${styles.metricPill} ${styles.metricMeta}`}>META {formatMetric(getSlotMetricValue(card, "meta", slotMeta))}</span>
                {hasProAccess ? (
                  <span className={`${styles.metricPill} ${styles.metricYour}`}>YOUR {formatMetric(getSlotMetricValue(card, "your", slotMeta))}</span>
                ) : null}
              </div>
            </div>
          </div>
        ) : (
          <div className={styles.emptySlot}>
            <p>{`Add a Year ${BUILDER_YEAR} card for this slot.`}</p>
            <span>Click to search the same card pool used by the app.</span>
          </div>
        )}
      </article>
    );
  };

  if (!authReady) {
    return (
      <div className={styles.page}>
        <FloatingShieldsBackground />
        <Navbar />
        <main className={styles.loadingState}>
          <div className={styles.spinner} />
          <p>Loading team builder...</p>
        </main>
      </div>
    );
  }

  if (!firebaseUser) {
    return null;
  }

  return (
    <div className={styles.page}>
      <FloatingShieldsBackground />
      <div className={styles.texture} />
      <Navbar />

      <main className={styles.content}>
        <section className={styles.hero}>
          <div className={styles.heroCopy}>
            <span className={styles.kicker}>Gameplay Lab</span>
            <h1>Team Builder</h1>
            <p>
              Same contracts and roster-generation logic as the app, rebuilt for the web with a full drafting board, drag-to-reorder lineup control, and a wider search workflow.
            </p>
          </div>
        </section>

        {authError ? <div className={styles.errorBanner}>{authError}</div> : null}
        {pageError ? <div className={styles.errorBanner}>{pageError}</div> : null}

        <section className={styles.layout}>
          <aside className={styles.sidebar}>
            <div className={styles.sidebarCard}>
              <div className={styles.summaryHeader}>
                <div>
                  <p className={styles.eyebrow}>Team Avg</p>
                  <h2>{averageOverall === null ? "--" : averageOverall.toFixed(1)}</h2>
                </div>
                <div className={styles.summaryBadge}>
                  <Sparkles size={14} />
                  <span>{metricLabel}</span>
                </div>
              </div>

              <p className={styles.summarySubtext}>
                {metricLabel} average across {selectedCount}/{totalCount} slots in the current view.
              </p>

              <div className={styles.metricChipRow}>
                {VALUE_METRIC_OPTIONS.map((option) => {
                  const selected = option.key === teamMetric;
                  const locked = isMetricLocked(option.key);
                  return (
                    <button
                      key={option.key}
                      type="button"
                      className={`${styles.metricChip} ${selected && !locked ? styles.metricChipActive : ""} ${
                        locked ? styles.metricChipLocked : ""
                      }`}
                      onClick={() => {
                        if (!locked) {
                          setTeamMetric(option.key);
                        }
                      }}
                      disabled={locked}
                    >
                      <span>{option.label}</span>
                      {locked ? <Lock size={13} /> : null}
                    </button>
                  );
                })}
              </div>

              <button type="button" className={styles.generateButton} onClick={() => void handleGenerateGreedy()} disabled={generating}>
                <Wand2 size={16} />
                <span>{generating ? "Generating..." : "Generate Best Team"}</span>
              </button>

              {!hasProAccess ? (
                <p className={styles.lockNote}>Pro unlocks the `Your Overall` team metric and sort mode.</p>
              ) : null}
            </div>

            <div className={styles.sidebarCard}>
              <p className={styles.eyebrow}>Roster Mode</p>
              <div className={styles.modeToggle}>
                {MODE_OPTIONS.map((option) => {
                  const selected = option.key === mode;
                  return (
                    <button
                      key={option.key}
                      type="button"
                      className={`${styles.modeChip} ${selected ? styles.modeChipActive : ""}`}
                      onClick={() => setMode(option.key)}
                    >
                      {option.label}
                    </button>
                  );
                })}
              </div>

              <div className={styles.noteBlock}>
                <p>
                  {mode === "batters"
                    ? "Drag lineup rows to reorder the batting order after you assign cards."
                    : "Rotation and bullpen keep the app’s fixed-slot behavior."}
                </p>
              </div>
            </div>
          </aside>

          <section className={styles.board}>
            {mode === "batters" ? (
              <>
                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Batting Lineup</h2>
                    <p>Click any slot to choose a card. Drag rows by the grip to set order.</p>
                  </div>
                </div>
                <div className={styles.rosterGrid}>{battingOrder.map((slotKey, index) => renderRosterRow(slotKey, index))}</div>

                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Bench</h2>
                    <p>{`Depth bats and utility coverage pulled from the same Year ${BUILDER_YEAR} card set.`}</p>
                  </div>
                </div>
                <div className={styles.rosterGrid}>{BENCH_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}</div>
              </>
            ) : (
              <>
                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Starting Rotation</h2>
                    <p>Top five starters by the active metric, matching the app’s generation rules.</p>
                  </div>
                </div>
                <div className={styles.rosterGrid}>{ROTATION_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}</div>

                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Bullpen</h2>
                    <p>Relievers sorted against RP/CP slot fit, with duplicate players automatically blocked.</p>
                  </div>
                </div>
                <div className={styles.rosterGrid}>{BULLPEN_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}</div>
              </>
            )}
          </section>
        </section>
      </main>

      {activeSlotMeta ? (
        <div className={styles.modalBackdrop} role="presentation" onClick={() => setActiveSlot(null)}>
          <div className={styles.modalCard} role="dialog" aria-modal="true" aria-labelledby="team-builder-modal-title" onClick={(event) => event.stopPropagation()}>
            <div className={styles.modalHeader}>
              <div>
                <p className={styles.eyebrow}>Choose Card</p>
                <h2 id="team-builder-modal-title">{activeSlotLabel}</h2>
              </div>
              <button type="button" className={styles.iconButton} onClick={() => setActiveSlot(null)} aria-label="Close card picker">
                <X size={18} />
              </button>
            </div>

            <div className={styles.searchBox}>
              <Search size={16} />
              <input
                className={styles.searchInput}
                type="search"
                placeholder={`Search Year ${BUILDER_YEAR} cards...`}
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
              />
            </div>

            {isSlotRequiringPositionFilter(activeSlotMeta) ? (
              <div className={styles.filterChipRow}>
                {POSITION_FILTER_OPTIONS.map((option) => {
                  const selected = positionFilterMode === option.key;
                  return (
                    <button
                      key={option.key}
                      type="button"
                      className={`${styles.filterChip} ${selected ? styles.filterChipActive : ""}`}
                      onClick={() => setPositionFilterMode(option.key)}
                    >
                      {option.label}
                    </button>
                  );
                })}
              </div>
            ) : null}

            <p className={styles.filterDescription}>{filterDescription}</p>

            <div className={styles.sortRow}>
              <span className={styles.sortLabel}>Sort by</span>
              <div className={styles.sortChipRow}>
                {VALUE_METRIC_OPTIONS.map((option) => {
                  const selected = option.key === sortMetric;
                  const locked = isMetricLocked(option.key);
                  return (
                    <button
                      key={option.key}
                      type="button"
                      className={`${styles.sortChip} ${selected && !locked ? styles.sortChipActive : ""} ${
                        locked ? styles.sortChipLocked : ""
                      }`}
                      onClick={() => {
                        if (!locked) {
                          setSortMetric(option.key);
                        }
                      }}
                      disabled={locked}
                    >
                      <span>{option.chip}</span>
                      {locked ? <Lock size={12} /> : null}
                    </button>
                  );
                })}
              </div>
            </div>

            <div className={styles.resultsMeta}>
              <span>{sortedFilteredResults.length} cards</span>
              <span>Year {BUILDER_YEAR} only</span>
            </div>

            <div className={styles.resultsPanel}>
              {loading ? (
                <div className={styles.statusState}>
                  <div className={styles.spinner} />
                  <p>Loading cards...</p>
                </div>
              ) : pickerError ? (
                <div className={styles.statusState}>
                  <p>{pickerError}</p>
                </div>
              ) : sortedFilteredResults.length === 0 ? (
                <div className={styles.statusState}>
                  <p>No cards found for this slot.</p>
                </div>
              ) : (
                sortedFilteredResults.map((card) => (
                  <button key={card.id} type="button" className={styles.resultRow} onClick={() => assignCardToSlot(card)}>
                    {/* eslint-disable-next-line @next/next/no-img-element */}
                    <img src={card.baked_img} alt={card.name} className={styles.resultImage} />
                    <div className={styles.resultMeta}>
                      <p className={styles.resultName}>
                        {card.name} <span>· {getHandText(card, activeSlotMeta.mode)}</span>
                      </p>
                      <p className={styles.resultSubtext}>
                        {card.team_short_name} · {getPrimaryPosition(card)} · Year {card.year}
                      </p>
                      <p className={styles.resultAttr}>{getAttributesForMode(card, activeSlotMeta.mode).join("  ·  ")}</p>
                      <div className={styles.resultMetricRow}>
                        <span>OVR {formatMetric(card.ovr)}</span>
                        <span>TRUE {formatMetric(getSlotMetricValue(card, "true", activeSlotMeta) ?? getTrueOverallValue(card))}</span>
                        <span>META {formatMetric(getSlotMetricValue(card, "meta", activeSlotMeta) ?? getMetaOverallValue(card))}</span>
                        {hasProAccess ? (
                          <span>YOUR {formatMetric(getSlotMetricValue(card, "your", activeSlotMeta) ?? getYourOverallValue(card))}</span>
                        ) : null}
                      </div>
                    </div>
                  </button>
                ))
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
