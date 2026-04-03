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
import { CURRENT_CARD_YEAR } from "@/lib/config";
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
type PredictionsSortKey = "popularity" | "predicted_ovr_delta" | "profit";
type SortDirection = "asc" | "desc";
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
const SORT_OPTIONS: FilterDropdownOption[] = [
  { value: "popularity", label: "Popularity" },
  { value: "predicted_ovr_delta", label: "Predicted OVR" },
  { value: "profit", label: "Profitable" },
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

function getQuicksellValue(ovr: number | null | undefined): number | null {
  if (ovr == null || Number.isNaN(ovr)) {
    return null;
  }
  if (ovr < 65) {
    return 5;
  }
  if (ovr <= 74) {
    return 25;
  }
  if (ovr <= 79) {
    return 50 + ((ovr - 75) * 25);
  }

  const quicksellByOvr: Record<number, number> = {
    80: 400,
    81: 600,
    82: 900,
    83: 1200,
    84: 1500,
    85: 3000,
    86: 3750,
    87: 4500,
    88: 5500,
    89: 7000,
    90: 8000,
    91: 9000,
  };
  if (ovr >= 92) {
    return 10000;
  }
  return quicksellByOvr[ovr] ?? null;
}

function getPredictedProfit(card: CardData): number | null {
  const predictedQuicksell = getQuicksellValue(card.predicted_ovr);
  if (predictedQuicksell == null || card.best_buy_price == null) {
    return null;
  }
  return predictedQuicksell - card.best_buy_price;
}

function formatSignedStubs(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatStubs(value)}`;
}

function normalizeNumericInput(value: string): string {
  return value.replace(/[^\d]/g, "");
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
  const [selectedSort, setSelectedSort] = useState<PredictionsSortKey>("predicted_ovr_delta");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [atQuicksellOnly, setAtQuicksellOnly] = useState(false);
  const [showMyPredictions, setShowMyPredictions] = useState(false);
  const [minBuyPrice, setMinBuyPrice] = useState("");
  const [maxBuyPrice, setMaxBuyPrice] = useState("");

  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(25);
  const [hasMore, setHasMore] = useState(true);

  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isPro, setIsPro] = useState<boolean | null>(null);
  const [rarityMenuOpen, setRarityMenuOpen] = useState(false);
  const rarityMenuRef = useRef<HTMLDivElement | null>(null);

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

  const raritySummary = useMemo(() => {
    if (!enforceNonProRarity && effectiveSelectedRarities.length === 0) {
      return "All";
    }
    if (effectiveSelectedRarities.length === 1) {
      return toTitle(effectiveSelectedRarities[0]);
    }
    if (effectiveSelectedRarities.length === 0) {
      return "None";
    }
    return `${effectiveSelectedRarities.length} Selected`;
  }, [effectiveSelectedRarities, enforceNonProRarity]);

  const parsedMinBuyPrice = useMemo(() => {
    if (!minBuyPrice.trim()) {
      return null;
    }
    const parsed = Number.parseInt(minBuyPrice, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }, [minBuyPrice]);

  const parsedMaxBuyPrice = useMemo(() => {
    if (!maxBuyPrice.trim()) {
      return null;
    }
    const parsed = Number.parseInt(maxBuyPrice, 10);
    return Number.isFinite(parsed) ? parsed : null;
  }, [maxBuyPrice]);

  const activeFilterCount = useMemo(() => {
    const freeRarityFiltered =
      enforceNonProRarity &&
      effectiveSelectedRarities.length > 0 &&
      effectiveSelectedRarities.length < NON_PRO_ALLOWED_RARITIES.length;
    const rarityCount = (!enforceNonProRarity && effectiveSelectedRarities.length > 0) || freeRarityFiltered ? 1 : 0;
    return (
      rarityCount +
      (selectedPlayerType !== "all" ? 1 : 0) +
      (selectedSort !== "predicted_ovr_delta" ? 1 : 0) +
      (sortDirection !== "desc" ? 1 : 0) +
      (atQuicksellOnly ? 1 : 0) +
      (parsedMinBuyPrice != null ? 1 : 0) +
      (parsedMaxBuyPrice != null ? 1 : 0)
    );
  }, [
    atQuicksellOnly,
    effectiveSelectedRarities.length,
    enforceNonProRarity,
    selectedPlayerType,
    parsedMaxBuyPrice,
    parsedMinBuyPrice,
    selectedSort,
    sortDirection,
  ]);

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
    if (!rarityMenuOpen) {
      return;
    }

    const onMouseDown = (event: MouseEvent) => {
      if (!rarityMenuRef.current) {
        return;
      }
      if (!rarityMenuRef.current.contains(event.target as Node)) {
        setRarityMenuOpen(false);
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
    };
  }, [rarityMenuOpen]);

  const toggleRarity = useCallback(
    (rarity: string) => {
      setSelectedRarities((current) => {
        const hasValue = current.includes(rarity);
        if (enforceNonProRarity) {
          if (hasValue && current.length === 1) {
            return current;
          }
          return hasValue ? current.filter((entry) => entry !== rarity) : [...current, rarity];
        }
        return hasValue ? current.filter((entry) => entry !== rarity) : [...current, rarity];
      });
      setPage(1);
    },
    [enforceNonProRarity],
  );

  const loadCards = useCallback(
    async (targetPage: number, targetLimit: number, query: string) => {
      setLoading(true);
      setError(null);

      try {
        const offset = (targetPage - 1) * targetLimit;
        const params = new URLSearchParams({
          series: "live",
          year: CURRENT_CARD_YEAR,
          offset: String(offset),
          limit: String(targetLimit),
        });

        if (query) {
          params.set("name", query);
        }
        if (showMyPredictions) {
          params.set("my_predictions", "true");
        }
        if (atQuicksellOnly) {
          params.set("at_quicksell", "true");
        }
        if (parsedMinBuyPrice != null) {
          params.set("min_buy_price", String(parsedMinBuyPrice));
        }
        if (parsedMaxBuyPrice != null) {
          params.set("max_buy_price", String(parsedMaxBuyPrice));
        }
        if (effectiveSelectedRarities.length > 0) {
          params.set("rarity", effectiveSelectedRarities.join(","));
        }

        if (selectedPlayerType === "hitter") {
          params.set("is_hitter", "true");
        } else if (selectedPlayerType === "pitcher") {
          params.set("is_hitter", "false");
        }

        params.set("sort_by", selectedSort);
        params.set("sort_dir", sortDirection);

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
      atQuicksellOnly,
      effectiveSelectedRarities,
      isAuthenticated,
      parsedMaxBuyPrice,
      parsedMinBuyPrice,
      selectedPlayerType,
      selectedSort,
      sortDirection,
      showMyPredictions,
    ],
  );

  useEffect(() => {
    void loadCards(page, limit, debouncedSearch);
  }, [debouncedSearch, limit, loadCards, page]);

  const clearAllFilters = () => {
    setSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : []);
    setSelectedPlayerType("all");
    setSelectedSort("predicted_ovr_delta");
    setSortDirection("desc");
    setAtQuicksellOnly(false);
    setMinBuyPrice("");
    setMaxBuyPrice("");
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
            label="Sort By"
            value={selectedSort}
            options={SORT_OPTIONS}
            onSelect={(value) => {
              setSelectedSort(value as PredictionsSortKey);
              setPage(1);
            }}
          />

          <button
            type="button"
            className={styles.sortDirectionButton}
            onClick={() => {
              setSortDirection((current) => (current === "desc" ? "asc" : "desc"));
              setPage(1);
            }}
          >
            <span>{sortDirection === "desc" ? "Desc" : "Asc"}</span>
          </button>

          <div className={styles.filterControl} ref={rarityMenuRef}>
            <span className={styles.filterTitleWithTag}>
              Rarity
              {enforceNonProRarity ? (
                <span className={styles.proTag}>
                  <Lock size={11} strokeWidth={2.2} />
                  Pro
                </span>
              ) : null}
            </span>
            <button
              type="button"
              className={styles.lockedRarityTrigger}
              onClick={() => setRarityMenuOpen((prev) => !prev)}
              aria-expanded={rarityMenuOpen}
            >
              <span className={styles.lockedRarityValue}>{raritySummary}</span>
              <span className={styles.chevronIcon} aria-hidden>
                <ChevronDown size={14} strokeWidth={2.2} />
              </span>
            </button>
            {rarityMenuOpen ? (
              <div className={styles.lockedRarityMenu}>
                {enforceNonProRarity ? (
                  <p className={styles.lockedUpgradeText}>Common and Bronze are available on Free. Upgrade to unlock all rarities.</p>
                ) : (
                  <p className={styles.lockedUpgradeText}>Select one or more rarities to narrow the board.</p>
                )}
                <ul className={styles.lockedRarityList}>
                  {ALL_RARITIES.map((rarity) => (
                    NON_PRO_ALLOWED_RARITIES.includes(rarity as (typeof NON_PRO_ALLOWED_RARITIES)[number]) || !enforceNonProRarity ? (
                      <li key={rarity} className={styles.freeRarityItem}>
                        <label className={styles.freeRarityLabel}>
                          <span>{toTitle(rarity)}</span>
                          <input
                            type="checkbox"
                            checked={effectiveSelectedRarities.includes(rarity)}
                            onChange={() => toggleRarity(rarity)}
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
                {enforceNonProRarity ? (
                  <Link href="/account" className={styles.upgradeLink}>
                    Upgrade to Pro
                  </Link>
                ) : null}
              </div>
            ) : null}
          </div>

          <label className={styles.filterControl}>
            <span>Buy Price</span>
            <div className={styles.priceRangeGroup}>
              <input
                type="text"
                inputMode="numeric"
                placeholder="Min"
                value={minBuyPrice}
                onChange={(event) => {
                  setMinBuyPrice(normalizeNumericInput(event.target.value));
                  setPage(1);
                }}
              />
              <span className={styles.priceRangeDash}>-</span>
              <input
                type="text"
                inputMode="numeric"
                placeholder="Max"
                value={maxBuyPrice}
                onChange={(event) => {
                  setMaxBuyPrice(normalizeNumericInput(event.target.value));
                  setPage(1);
                }}
              />
            </div>
          </label>

          <button
            type="button"
            className={`${styles.predictionsToggle} ${atQuicksellOnly ? styles.predictionsToggleActive : ""}`}
            onClick={() => {
              setAtQuicksellOnly((prev) => !prev);
              setPage(1);
            }}
            aria-pressed={atQuicksellOnly}
          >
            <span className={styles.predictionsToggleLabel}>At Quicksell</span>
            <span className={styles.predictionsToggleTrack} aria-hidden>
              <span className={styles.predictionsToggleThumb} />
            </span>
          </button>

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
                {cards.map((card) => {
                  const predictedProfit = getPredictedProfit(card);
                  const profitClass =
                    predictedProfit == null
                      ? ""
                      : predictedProfit > 0
                        ? styles.profitPositive
                        : predictedProfit < 0
                          ? styles.profitNegative
                          : styles.profitNeutral;

                  return (
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
                            <span className={`${styles.statPill} ${styles.marketStat} ${profitClass}`}>
                              <span className={styles.marketLabel}>
                                <Image src="/images/stub.png" alt="" width={11} height={11} className={styles.stubIcon} />
                                Profit:
                              </span>
                              <span>{formatSignedStubs(predictedProfit)}</span>
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
                  );
                })}
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
