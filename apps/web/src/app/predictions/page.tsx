"use client";

import Link from "next/link";
import Image from "next/image";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged } from "firebase/auth";
import { ChevronDown, Lock } from "lucide-react";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { ApiError, apiGet, apiGetAuth, getMyEntitlements } from "@/lib/api";
import { getFirebaseAuth } from "@/lib/firebase";

import styles from "./page.module.css";

type CardData = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  series: string;
  display_position: string;
  age: number;
  is_hitter: boolean;
  rarity: string;
  comment_count: number;
  user_prediction_count: number;
  predicted_ovr: number | null;
  community_predicted_ovr: number | null;
  best_buy_price: number | null;
  best_sell_price: number | null;
  quicksell_value: number | null;
  buy_now_uses_quicksell: boolean | null;
  buy_now_above_quicksell_pct: number | null;
  predicted_attributes: Record<string, number> | null;
  user_prediction: number | null;
};

type PlayerTypeFilter = "all" | "hitter" | "pitcher";
type PopularityFilter = "none" | "most" | "least";
type DeltaFilter = "none" | "high" | "low";
type FilterDropdownOption = {
  value: string;
  label: string;
};

const NON_PRO_ALLOWED_RARITIES = ["common", "bronze"] as const;
const ALL_RARITIES = ["common", "bronze", "silver", "gold", "diamond"] as const;
const PLAYER_TYPE_OPTIONS: FilterDropdownOption[] = [
  { value: "all", label: "All" },
  { value: "hitter", label: "Hitters" },
  { value: "pitcher", label: "Pitchers" },
];
const POPULARITY_OPTIONS: FilterDropdownOption[] = [
  { value: "none", label: "None" },
  { value: "most", label: "Most" },
  { value: "least", label: "Least" },
];
const DELTA_OPTIONS: FilterDropdownOption[] = [
  { value: "none", label: "None" },
  { value: "high", label: "Highest Increase" },
  { value: "low", label: "Highest Decrease" },
];

function toTitle(value: string): string {
  if (!value) {
    return "";
  }
  return `${value.charAt(0).toUpperCase()}${value.slice(1)}`;
}

function formatStubs(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toLocaleString("en-US");
}

function formatPercent(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  const rounded = Math.round(value * 10) / 10;
  if (Number.isInteger(rounded)) {
    return `${rounded}%`;
  }
  return `${rounded.toFixed(1)}%`;
}

function FilterDropdown({
  label,
  value,
  options,
  onSelect,
}: {
  label: string;
  value: string;
  options: FilterDropdownOption[];
  onSelect: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const rootRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onMouseDown = (event: MouseEvent) => {
      if (!rootRef.current) {
        return;
      }
      if (!rootRef.current.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
    };
  }, []);

  const selectedOption = options.find((option) => option.value === value);
  const selectedLabel = selectedOption ? selectedOption.label : options[0]?.label ?? "";

  return (
    <div className={styles.filterControl} ref={rootRef}>
      <span>{label}</span>
      <button type="button" className={styles.filterDropdownTrigger} onClick={() => setOpen((prev) => !prev)} aria-expanded={open}>
        <span className={styles.filterDropdownValue}>{selectedLabel}</span>
        <span className={styles.chevronIcon} aria-hidden>
          <ChevronDown size={14} strokeWidth={2.2} />
        </span>
      </button>
      {open ? (
        <div className={styles.filterDropdownMenu}>
          <ul className={styles.filterDropdownList}>
            {options.map((option) => (
              <li key={option.value}>
                <button
                  type="button"
                  className={`${styles.filterDropdownOption} ${option.value === value ? styles.filterDropdownOptionActive : ""}`}
                  onClick={() => {
                    onSelect(option.value);
                    setOpen(false);
                  }}
                >
                  {option.label}
                </button>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

export default function PredictionsPage() {
  const router = useRouter();

  const [cards, setCards] = useState<CardData[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [searchText, setSearchText] = useState("");
  const [debouncedSearch, setDebouncedSearch] = useState("");

  const [selectedRarities, setSelectedRarities] = useState<string[]>([]);
  const [selectedPlayerType, setSelectedPlayerType] = useState<PlayerTypeFilter>("all");
  const [selectedPopularity, setSelectedPopularity] = useState<PopularityFilter>("none");
  const [selectedDelta, setSelectedDelta] = useState<DeltaFilter>("none");
  const [showMyPredictions, setShowMyPredictions] = useState(false);

  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(25);
  const [hasMore, setHasMore] = useState(true);

  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isPro, setIsPro] = useState<boolean | null>(null);
  const [nonProRarityOpen, setNonProRarityOpen] = useState(false);
  const nonProRarityRef = useRef<HTMLDivElement | null>(null);

  const enforceNonProRarity = isPro === false;
  const restrictRarityUntilProResolved = isPro !== true;
  const effectiveSelectedRarities = useMemo(() => {
    if (!restrictRarityUntilProResolved) {
      return selectedRarities;
    }
    const allowedSelections = selectedRarities.filter((rarity) =>
      NON_PRO_ALLOWED_RARITIES.includes(rarity as (typeof NON_PRO_ALLOWED_RARITIES)[number]),
    );
    if (allowedSelections.length > 0) {
      return allowedSelections;
    }
    return [...NON_PRO_ALLOWED_RARITIES];
  }, [restrictRarityUntilProResolved, selectedRarities]);

  const selectedRarityValue = useMemo(() => {
    if (enforceNonProRarity) {
      return "non-pro";
    }
    if (selectedRarities.length === 1) {
      return selectedRarities[0];
    }
    return "all";
  }, [enforceNonProRarity, selectedRarities]);

  const nonProRaritySummary = useMemo(() => {
    const count = effectiveSelectedRarities.length;
    if (count === 1) {
      return "1 Filter Applied";
    }
    return `${count} Filters Applied`;
  }, [effectiveSelectedRarities]);

  const activeFilterCount = useMemo(() => {
    const rarityCount = !enforceNonProRarity && selectedRarities.length > 0 ? 1 : 0;
    return rarityCount + (selectedPlayerType !== "all" ? 1 : 0) + (selectedPopularity !== "none" ? 1 : 0) + (selectedDelta !== "none" ? 1 : 0);
  }, [enforceNonProRarity, selectedRarities.length, selectedPlayerType, selectedPopularity, selectedDelta]);

  const hasFiltering = activeFilterCount > 0 || showMyPredictions;

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;

    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        const signedIn = Boolean(user);
        setIsAuthenticated(signedIn);

        if (!signedIn) {
          setIsPro(false);
          setShowMyPredictions(false);
          return;
        }

        void getMyEntitlements()
          .then((payload) => {
            setIsPro(Boolean(payload.has_pro));
          })
          .catch(() => {
            setIsPro(false);
          });
      });
    } catch {
      // If Firebase bootstrap fails, page stays in signed-out mode.
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setDebouncedSearch(searchText.trim());
      setPage(1);
    }, 500);
    return () => {
      window.clearTimeout(timer);
    };
  }, [searchText]);

  useEffect(() => {
    if (!enforceNonProRarity) {
      return;
    }

    setSelectedRarities((current) => {
      const allowed = current.filter((rarity) => NON_PRO_ALLOWED_RARITIES.includes(rarity as (typeof NON_PRO_ALLOWED_RARITIES)[number]));
      if (allowed.length > 0) {
        return allowed;
      }
      return [...NON_PRO_ALLOWED_RARITIES];
    });
  }, [enforceNonProRarity]);

  useEffect(() => {
    if (!enforceNonProRarity) {
      setNonProRarityOpen(false);
      return;
    }

    const onMouseDown = (event: MouseEvent) => {
      if (!nonProRarityRef.current) {
        return;
      }
      if (!nonProRarityRef.current.contains(event.target as Node)) {
        setNonProRarityOpen(false);
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
    };
  }, [enforceNonProRarity]);

  const loadCards = useCallback(
    async (targetPage: number, targetLimit: number, query: string) => {
      setLoading(true);
      setError(null);

      try {
        const offset = (targetPage - 1) * targetLimit;
        const params = new URLSearchParams({
          series: "live",
          year: "25",
          offset: String(offset),
          limit: String(targetLimit),
        });

        if (query) {
          params.set("name", query);
        }
        if (showMyPredictions) {
          params.set("my_predictions", "true");
        }
        if (effectiveSelectedRarities.length > 0) {
          params.set("rarity", effectiveSelectedRarities.join(","));
        }

        if (selectedPlayerType === "hitter") {
          params.set("is_hitter", "true");
        } else if (selectedPlayerType === "pitcher") {
          params.set("is_hitter", "false");
        }

        if (selectedPopularity !== "none") {
          params.set("sort_by", "popularity");
          params.set("desc", selectedPopularity === "most" ? "true" : "false");
        } else if (selectedDelta !== "none") {
          params.set("sort_by", "predicted_ovr_delta");
          params.set("desc", selectedDelta === "high" ? "true" : "false");
        }

        const path = `/cards?${params.toString()}`;
        const useAuthedRequest = showMyPredictions || isAuthenticated;
        const nextCards = useAuthedRequest ? await apiGetAuth<CardData[]>(path) : await apiGet<CardData[]>(path);
        setCards(nextCards);
        setHasMore(nextCards.length === targetLimit);
      } catch (err: unknown) {
        if (err instanceof ApiError && err.status === 401) {
          setError("Sign in to access this filter.");
        } else if (err instanceof Error && err.message) {
          setError(err.message);
        } else {
          setError("Failed to load predictions.");
        }
        setCards([]);
        setHasMore(false);
      } finally {
        setLoading(false);
      }
    },
    [
      effectiveSelectedRarities,
      isAuthenticated,
      selectedDelta,
      selectedPlayerType,
      selectedPopularity,
      showMyPredictions,
    ],
  );

  useEffect(() => {
    void loadCards(page, limit, debouncedSearch);
  }, [debouncedSearch, limit, loadCards, page]);

  const clearAllFilters = () => {
    setSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : []);
    setSelectedPlayerType("all");
    setSelectedPopularity("none");
    setSelectedDelta("none");
    setShowMyPredictions(false);
    setPage(1);
  };

  const onToggleMyPredictions = () => {
    if (!isAuthenticated) {
      router.push("/signin");
      return;
    }
    setShowMyPredictions((prev) => !prev);
    setPage(1);
  };

  const renderPredictionBadge = (item: CardData) => {
    const communityPrediction = item.community_predicted_ovr ?? item.predicted_ovr;

    let trendClass = styles.neutral;
    if (item.predicted_ovr != null && item.predicted_ovr > item.ovr) {
      trendClass = styles.up;
    } else if (item.predicted_ovr != null && item.predicted_ovr < item.ovr) {
      trendClass = styles.down;
    }

    let communityTrendClass = styles.neutral;
    if (communityPrediction != null && communityPrediction > item.ovr) {
      communityTrendClass = styles.up;
    } else if (communityPrediction != null && communityPrediction < item.ovr) {
      communityTrendClass = styles.down;
    }

    const yourBadgeClass = item.user_prediction == null ? styles.noPrediction : "";

    return (
      <>
        <span className={`${styles.arrow} ${trendClass}`}>-&gt;</span>
        <div className={`${styles.ratingBadge} ${styles.predBadge} ${trendClass}`}>
          <span className={styles.badgeLabel}>Our</span>
          <strong>{item.predicted_ovr ?? "--"}</strong>
        </div>
        <div className={`${styles.ratingBadge} ${styles.commBadge} ${communityTrendClass}`}>
          <span className={styles.badgeLabel}>Comm</span>
          <strong>{communityPrediction ?? "--"}</strong>
        </div>
        <div className={`${styles.ratingBadge} ${styles.yourBadge} ${yourBadgeClass}`}>
          <span className={styles.badgeLabel}>Your</span>
          <strong>{item.user_prediction ?? "--"}</strong>
        </div>
      </>
    );
  };

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <section className={styles.content}>
        <header className={styles.header}>
          <div>
            <h1>Market Predictions</h1>
            <p>Track projected OVR moves and community sentiment in real time.</p>
          </div>
        </header>

        <div className={styles.toolbarRow}>
          <label className={styles.searchInputContainer}>
            <input
              type="search"
              placeholder="Search players..."
              value={searchText}
              onChange={(event) => setSearchText(event.target.value)}
              aria-label="Search players"
            />
          </label>

          <FilterDropdown
            label="Type"
            value={selectedPlayerType}
            options={PLAYER_TYPE_OPTIONS}
            onSelect={(value) => {
              setSelectedPlayerType(value as PlayerTypeFilter);
              setPage(1);
            }}
          />

          <FilterDropdown
            label="Popularity"
            value={selectedPopularity}
            options={POPULARITY_OPTIONS}
            onSelect={(value) => {
              const nextValue = value as PopularityFilter;
              setSelectedPopularity(nextValue);
              if (nextValue !== "none") {
                setSelectedDelta("none");
              }
              setPage(1);
            }}
          />

          <FilterDropdown
            label="Predicted OVR"
            value={selectedDelta}
            options={DELTA_OPTIONS}
            onSelect={(value) => {
              const nextValue = value as DeltaFilter;
              setSelectedDelta(nextValue);
              if (nextValue !== "none") {
                setSelectedPopularity("none");
              }
              setPage(1);
            }}
          />

          {enforceNonProRarity ? (
            <div className={styles.filterControl} ref={nonProRarityRef}>
              <span className={styles.filterTitleWithTag}>
                Rarity
                <span className={styles.proTag}>
                  <Lock size={11} strokeWidth={2.2} />
                  Pro
                </span>
              </span>
              <button
                type="button"
                className={styles.lockedRarityTrigger}
                onClick={() => setNonProRarityOpen((prev) => !prev)}
                aria-expanded={nonProRarityOpen}
              >
                <span className={styles.lockedRarityValue}>{nonProRaritySummary}</span>
                <span className={styles.chevronIcon} aria-hidden>
                  <ChevronDown size={14} strokeWidth={2.2} />
                </span>
              </button>
              {nonProRarityOpen ? (
                <div className={styles.lockedRarityMenu}>
                  <p className={styles.lockedUpgradeText}>Common and Bronze are available on Free. Upgrade to unlock all rarities.</p>
                  <ul className={styles.lockedRarityList}>
                    {ALL_RARITIES.map((rarity) => (
                      NON_PRO_ALLOWED_RARITIES.includes(rarity as (typeof NON_PRO_ALLOWED_RARITIES)[number]) ? (
                        <li key={rarity} className={styles.freeRarityItem}>
                          <label className={styles.freeRarityLabel}>
                            <span>{toTitle(rarity)}</span>
                            <input
                              type="checkbox"
                              checked={selectedRarities.includes(rarity)}
                              onChange={() => {
                                setSelectedRarities((current) => {
                                  const hasValue = current.includes(rarity);
                                  if (hasValue && current.length === 1) {
                                    return current;
                                  }
                                  return hasValue ? current.filter((entry) => entry !== rarity) : [...current, rarity];
                                });
                                setPage(1);
                              }}
                            />
                          </label>
                        </li>
                      ) : (
                        <li key={rarity} className={styles.lockedRarityItem}>
                          <span>{toTitle(rarity)}</span>
                          <span className={styles.lockIcon} aria-hidden>
                            <Lock size={12} strokeWidth={2.2} />
                          </span>
                        </li>
                      )
                    ))}
                  </ul>
                  <Link href="/account" className={styles.upgradeLink}>
                    Upgrade to Pro
                  </Link>
                </div>
              ) : null}
            </div>
          ) : (
            <label className={styles.filterControl}>
              <span>Rarity</span>
              <select
                value={selectedRarityValue}
                onChange={(event) => {
                  const value = event.target.value;
                  if (value === "all") {
                    setSelectedRarities([]);
                  } else {
                    setSelectedRarities([value]);
                  }
                  setPage(1);
                }}
              >
                <option value="all">All</option>
                {ALL_RARITIES.map((rarity) => (
                  <option key={rarity} value={rarity}>
                    {toTitle(rarity)}
                  </option>
                ))}
              </select>
            </label>
          )}

          <button
            type="button"
            className={`${styles.predictionsToggle} ${showMyPredictions ? styles.predictionsToggleActive : ""}`}
            onClick={onToggleMyPredictions}
            aria-pressed={showMyPredictions}
          >
            <span className={styles.predictionsToggleLabel}>View My Predictions</span>
            <span className={styles.predictionsToggleTrack} aria-hidden>
              <span className={styles.predictionsToggleThumb} />
            </span>
          </button>

          {!isAuthenticated ? (
            <Link href="/signin" className={styles.quickLink}>
              Sign in
            </Link>
          ) : null}
        </div>

        {error ? <p className={styles.errorMessage}>{error}</p> : null}

        {loading ? (
          <div className={styles.loadingState}>
            <div className={styles.spinner} />
            <p>Loading predictions...</p>
          </div>
        ) : cards.length === 0 ? (
          <div className={styles.emptyState}>
            <p>{hasFiltering ? "No cards match your filters." : "No players found."}</p>
            {hasFiltering ? (
              <button type="button" className={styles.clearFiltersButton} onClick={clearAllFilters}>
                Clear Filters
              </button>
            ) : null}
          </div>
        ) : (
          <>
            <div className={styles.resultsScroll}>
              <div className={styles.cardList}>
                {cards.map((card) => (
                  <Link key={card.id} href={`/cards/${encodeURIComponent(card.id)}`} className={styles.cardRowLink}>
                    <article className={styles.cardRow}>
                      <div className={styles.cardImageWrap}>
                        {card.baked_img ? (
                          <Image
                            src={card.baked_img}
                            alt={card.name}
                            width={56}
                            height={78}
                            className={styles.cardImage}
                            unoptimized
                          />
                        ) : (
                          <div className={styles.cardImageFallback}>No Image</div>
                        )}
                      </div>

                      <div className={styles.cardInfo}>
                        <h2>{card.name}</h2>

                        <div className={styles.teamRow}>
                          <span className={styles.teamName}>{card.team_short_name || "N/A"}</span>
                          <span className={styles.verticalDivider} />
                          <span className={styles.statPill}>Predictions {card.user_prediction_count ?? 0}</span>
                          <span className={styles.verticalDivider} />
                          <span className={styles.statPill}>Comments {card.comment_count ?? 0}</span>
                          <span className={styles.verticalDivider} />
                          <span className={`${styles.statPill} ${styles.marketStat}`}>
                            <span className={styles.marketLabel}>
                              <Image src="/images/stub.png" alt="" width={11} height={11} className={styles.stubIcon} />
                              Buy:
                            </span>
                            <span>{formatStubs(card.best_buy_price)}</span>
                          </span>
                          <span className={styles.verticalDivider} />
                          <span className={`${styles.statPill} ${styles.marketStat}`}>
                            <span className={styles.marketLabel}>
                              <Image src="/images/stub.png" alt="" width={11} height={11} className={styles.stubIcon} />
                              Quicksell:
                            </span>
                            <span>{formatStubs(card.quicksell_value)}</span>
                          </span>
                          <span className={styles.verticalDivider} />
                          <span
                            className={`${styles.statPill} ${styles.quicksellStatus} ${
                              card.buy_now_uses_quicksell ? styles.quicksellStatusGood : styles.quicksellStatusBad
                            }`}
                          >
                            <span className={styles.quicksellStatusIcon} aria-hidden>
                              {card.buy_now_uses_quicksell ? "✓" : "✕"}
                            </span>
                            <span>
                              {card.buy_now_uses_quicksell ? "At Quicksell" : `${formatPercent(card.buy_now_above_quicksell_pct)} above QS`}
                            </span>
                          </span>
                        </div>

                        <div className={styles.ratingRow}>
                          <div className={styles.ratingBadge}>
                            <span className={styles.badgeLabel}>CUR</span>
                            <strong>{card.ovr}</strong>
                          </div>
                          {renderPredictionBadge(card)}
                        </div>
                      </div>
                    </article>
                  </Link>
                ))}
              </div>
            </div>

            <footer className={styles.footer}>
              <div className={styles.footerRow}>
                <label className={styles.inlineLimitControl}>
                  <span>View:</span>
                  <select
                    className={styles.inlineLimitSelect}
                    value={String(limit)}
                    onChange={(event) => {
                      const nextLimit = Number.parseInt(event.target.value, 10);
                      if (!Number.isFinite(nextLimit) || nextLimit === limit) {
                        return;
                      }
                      setLimit(nextLimit);
                      setPage(1);
                    }}
                  >
                    <option value="10">10</option>
                    <option value="25">25</option>
                    <option value="50">50</option>
                  </select>
                </label>

                <div className={styles.navRow}>
                  <button
                    type="button"
                    className={styles.navButton}
                    disabled={page === 1}
                    onClick={() => setPage((currentPage) => Math.max(1, currentPage - 1))}
                  >
                    Prev
                  </button>
                  <span className={styles.pageNumber}>Page {page}</span>
                  <button
                    type="button"
                    className={styles.navButton}
                    disabled={!hasMore}
                    onClick={() => setPage((currentPage) => currentPage + 1)}
                  >
                    Next
                  </button>
                </div>
              </div>
            </footer>
          </>
        )}
      </section>
    </main>
  );
}
