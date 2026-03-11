"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { ChevronDown, ChevronUp, Globe, Lock, Pencil, Plus, RefreshCcw, Trash2, X } from "lucide-react";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import {
  ApiError,
  apiDeleteAuth,
  apiGet,
  apiGetAuth,
  apiPatchAuth,
  apiPostAuth,
  apiPutAuth,
  getMyEntitlements,
} from "@/lib/api";
import { getFirebaseAuth } from "@/lib/firebase";

import styles from "./page.module.css";

type CardSearchResult = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  rarity: string;
  predicted_ovr: number | null;
};

type HoldingCard = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  rarity: string;
  predicted_ovr: number | null;
};

type Holding = {
  card_id: string;
  quantity: number;
  avg_price: number | null;
  user_predicted_ovr: number | null;
  notes: string | null;
  card: HoldingCard;
};

type PortfolioData = {
  id: number;
  name: string;
  is_public: boolean;
  holdings: Holding[];
};

type UserPredictionResponse = {
  predicted_ovr: number;
};

const QUICKSELL_TABLE: Record<number, number> = {
  91: 9000,
  90: 8000,
  89: 7000,
  88: 5500,
  87: 4500,
  86: 3750,
  85: 3000,
  84: 1500,
  83: 1200,
  82: 900,
  81: 600,
  80: 400,
  79: 150,
  78: 125,
  77: 100,
  76: 75,
  75: 50,
};

function getQuicksellValue(ovr: number): number {
  if (ovr >= 92) {
    return 10000;
  }
  if (QUICKSELL_TABLE[ovr] !== undefined) {
    return QUICKSELL_TABLE[ovr];
  }
  if (ovr >= 65) {
    return 25;
  }
  return 5;
}

function formatStubs(value: number): string {
  if (Math.abs(value) >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return value.toLocaleString();
}

function parseInteger(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
}

function signedStubs(value: number): string {
  const prefix = value > 0 ? "+" : "";
  return `${prefix}${formatStubs(value)}`;
}

function plClassName(value: number): string {
  if (value > 0) {
    return styles.valuePositive;
  }
  if (value < 0) {
    return styles.valueNegative;
  }
  return "";
}

function SummaryCard({
  title,
  value,
  signed,
  isPro,
}: {
  title: string;
  value: number;
  signed?: boolean;
  isPro?: boolean;
}) {
  return (
    <div className={styles.summaryCard}>
      <div className={styles.summaryTitleRow}>
        {isPro ? <span className={styles.proBadge}>PRO</span> : null}
        <span className={styles.summaryTitle}>{title}</span>
      </div>
      <div className={styles.summaryValueRow}>
        <Image src="/images/stub.png" alt="" width={12} height={12} className={styles.stubIcon} />
        <strong className={`${styles.summaryValue} ${signed ? plClassName(value) : ""}`}>{signed ? signedStubs(value) : formatStubs(value)}</strong>
      </div>
    </div>
  );
}

export default function PortfolioPage() {
  const router = useRouter();

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);

  const [isPro, setIsPro] = useState(false);
  const [portfolio, setPortfolio] = useState<PortfolioData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const [isAddOpen, setIsAddOpen] = useState(true);
  const [searchText, setSearchText] = useState("");
  const [searching, setSearching] = useState(false);
  const [searchResults, setSearchResults] = useState<CardSearchResult[]>([]);
  const [selectedCard, setSelectedCard] = useState<CardSearchResult | null>(null);
  const [quantity, setQuantity] = useState("");
  const [avgBuyPrice, setAvgBuyPrice] = useState("");
  const [projectedOvr, setProjectedOvr] = useState("");
  const [notes, setNotes] = useState("");
  const [addError, setAddError] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);

  const [editingHolding, setEditingHolding] = useState<Holding | null>(null);
  const [editQuantity, setEditQuantity] = useState("");
  const [editAvgPrice, setEditAvgPrice] = useState("");
  const [editProjectedOvr, setEditProjectedOvr] = useState("");
  const [editNotes, setEditNotes] = useState("");
  const [editError, setEditError] = useState<string | null>(null);
  const [editSubmitting, setEditSubmitting] = useState(false);

  const [privacyUpdating, setPrivacyUpdating] = useState(false);

  const holdings = useMemo(() => portfolio?.holdings ?? [], [portfolio]);

  const totals = useMemo(() => {
    return holdings.reduce(
      (acc, holding) => {
        const avgPrice = holding.avg_price ?? 0;
        const invested = holding.quantity * avgPrice;

        const yourOvr = holding.user_predicted_ovr ?? holding.card.ovr;
        const yourValue = holding.quantity * getQuicksellValue(yourOvr);

        const modelOvr = holding.card.predicted_ovr ?? holding.card.ovr;
        const modelValue = holding.quantity * getQuicksellValue(modelOvr);

        return {
          invested: acc.invested + invested,
          yourValue: acc.yourValue + yourValue,
          yourPl: acc.yourPl + (yourValue - invested),
          modelValue: acc.modelValue + modelValue,
          modelPl: acc.modelPl + (modelValue - invested),
        };
      },
      { invested: 0, yourValue: 0, yourPl: 0, modelValue: 0, modelPl: 0 },
    );
  }, [holdings]);

  const resetAddForm = useCallback(() => {
    setSelectedCard(null);
    setSearchText("");
    setSearchResults([]);
    setQuantity("");
    setAvgBuyPrice("");
    setProjectedOvr("");
    setNotes("");
    setAddError(null);
  }, []);

  const closeEditModal = useCallback(() => {
    setEditingHolding(null);
    setEditQuantity("");
    setEditAvgPrice("");
    setEditProjectedOvr("");
    setEditNotes("");
    setEditError(null);
  }, []);

  const fetchPortfolio = useCallback(
    async (opts?: { showLoading?: boolean }) => {
      if (!firebaseUser) {
        return;
      }

      const showLoading = opts?.showLoading ?? true;
      if (showLoading) {
        setLoading(true);
      }

      try {
        const data = await apiGetAuth<PortfolioData>("/portfolios/me");
        setPortfolio(data);
        setPageError(null);
      } catch (error: unknown) {
        if (error instanceof ApiError && error.status === 404) {
          setPageError("No portfolio found for this account yet.");
        } else {
          setPageError("Failed to load portfolio.");
        }
      } finally {
        if (showLoading) {
          setLoading(false);
        }
        setRefreshing(false);
      }
    },
    [firebaseUser],
  );

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
    } catch {
      setAuthReady(true);
      setPageError("Firebase auth is not configured.");
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!authReady || !firebaseUser) {
      return;
    }
    void fetchPortfolio();
  }, [authReady, firebaseUser, fetchPortfolio]);

  useEffect(() => {
    if (!firebaseUser) {
      setIsPro(false);
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
    if (searchText.trim().length < 2 || selectedCard) {
      setSearchResults([]);
      return;
    }

    let active = true;
    const timer = window.setTimeout(() => {
      setSearching(true);
      void apiGet<CardSearchResult[]>(
        `/cards?series=live&year=25&name=${encodeURIComponent(searchText.trim())}&limit=8`,
      )
        .then((results) => {
          if (active) {
            setSearchResults(results);
          }
        })
        .catch(() => {
          if (active) {
            setSearchResults([]);
          }
        })
        .finally(() => {
          if (active) {
            setSearching(false);
          }
        });
    }, 350);

    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [searchText, selectedCard]);

  useEffect(() => {
    if (!selectedCard) {
      return;
    }

    let active = true;
    void apiGetAuth<UserPredictionResponse>(`/user-predictions/${selectedCard.id}`)
      .then((prediction) => {
        if (active && prediction?.predicted_ovr != null) {
          setProjectedOvr(String(prediction.predicted_ovr));
        }
      })
      .catch(() => {
        // No existing prediction is expected for many cards.
      });

    return () => {
      active = false;
    };
  }, [selectedCard]);

  useEffect(() => {
    if (!successMessage) {
      return;
    }

    const timer = window.setTimeout(() => setSuccessMessage(null), 2500);
    return () => {
      window.clearTimeout(timer);
    };
  }, [successMessage]);

  const onRefresh = () => {
    setRefreshing(true);
    void fetchPortfolio({ showLoading: false });
  };

  const onTogglePrivacy = async () => {
    if (!portfolio || privacyUpdating) {
      return;
    }

    const nextIsPublic = !portfolio.is_public;
    setPortfolio({ ...portfolio, is_public: nextIsPublic });
    setPrivacyUpdating(true);

    try {
      await apiPatchAuth<void>("/portfolios/me", { is_public: nextIsPublic });
      setSuccessMessage(`Portfolio is now ${nextIsPublic ? "public" : "private"}.`);
    } catch {
      await fetchPortfolio({ showLoading: false });
      setPageError("Failed to update portfolio privacy.");
    } finally {
      setPrivacyUpdating(false);
    }
  };

  const onAddHolding = async () => {
    if (!selectedCard) {
      setAddError("Select a card before adding an investment.");
      return;
    }

    const parsedQty = parseInteger(quantity);
    if (parsedQty == null || parsedQty < 1) {
      setAddError("Quantity must be at least 1.");
      return;
    }

    const parsedAvgPrice = parseInteger(avgBuyPrice);
    if (parsedAvgPrice == null || parsedAvgPrice < 1) {
      setAddError("Avg buy price must be at least 1.");
      return;
    }

    const parsedPredictedOvr = parseInteger(projectedOvr);
    if (parsedPredictedOvr == null || parsedPredictedOvr < 0 || parsedPredictedOvr > 99) {
      setAddError("Predicted OVR must be between 0 and 99.");
      return;
    }

    if (notes.length > 500) {
      setAddError("Notes must be 500 characters or less.");
      return;
    }

    setAddError(null);
    setAdding(true);

    try {
      await Promise.all([
        apiPostAuth<void>("/portfolios/me/holdings", {
          card_id: selectedCard.id,
          quantity: parsedQty,
          avg_price: parsedAvgPrice,
          user_predicted_ovr: parsedPredictedOvr,
          notes: notes.trim() || null,
        }),
        apiPostAuth<UserPredictionResponse>("/user-predictions", {
          card_id: selectedCard.id,
          predicted_ovr: parsedPredictedOvr,
        }).catch(() => {
          // Keep portfolio write successful even if prediction sync fails.
        }),
      ]);

      resetAddForm();
      await fetchPortfolio({ showLoading: false });
      setSuccessMessage("Investment added.");
    } catch {
      setAddError("Failed to add investment. Try again.");
    } finally {
      setAdding(false);
    }
  };

  const onRemoveHolding = async (holding: Holding) => {
    const confirmed = window.confirm(`Remove ${holding.card.name} from your portfolio?`);
    if (!confirmed) {
      return;
    }

    try {
      await apiDeleteAuth<void>(`/portfolios/me/holdings/${holding.card_id}`);
      await fetchPortfolio({ showLoading: false });
      setSuccessMessage("Investment removed.");
    } catch {
      setPageError("Failed to remove investment.");
    }
  };

  const openEditModal = (holding: Holding) => {
    setEditingHolding(holding);
    setEditQuantity(String(holding.quantity));
    setEditAvgPrice(holding.avg_price == null ? "" : String(holding.avg_price));
    setEditProjectedOvr(holding.user_predicted_ovr == null ? "" : String(holding.user_predicted_ovr));
    setEditNotes(holding.notes ?? "");
    setEditError(null);
  };

  const onSaveEdit = async () => {
    if (!editingHolding) {
      return;
    }

    const parsedQty = parseInteger(editQuantity);
    if (parsedQty == null || parsedQty < 1) {
      setEditError("Quantity must be at least 1.");
      return;
    }

    const parsedAvgPrice = editAvgPrice.trim() ? parseInteger(editAvgPrice) : null;
    if (editAvgPrice.trim() && (parsedAvgPrice == null || parsedAvgPrice < 0)) {
      setEditError("Avg buy price must be 0 or higher.");
      return;
    }

    const parsedPredictedOvr = editProjectedOvr.trim() ? parseInteger(editProjectedOvr) : null;
    if (editProjectedOvr.trim() && (parsedPredictedOvr == null || parsedPredictedOvr < 0 || parsedPredictedOvr > 99)) {
      setEditError("Predicted OVR must be between 0 and 99.");
      return;
    }

    if (editNotes.length > 500) {
      setEditError("Notes must be 500 characters or less.");
      return;
    }

    setEditSubmitting(true);
    setEditError(null);

    try {
      await Promise.all([
        apiPutAuth<void>(`/portfolios/me/holdings/${editingHolding.card_id}`, {
          quantity: parsedQty,
          avg_price: parsedAvgPrice,
          user_predicted_ovr: parsedPredictedOvr,
          notes: editNotes.trim() || null,
        }),
        parsedPredictedOvr != null
          ? apiPostAuth<UserPredictionResponse>("/user-predictions", {
              card_id: editingHolding.card_id,
              predicted_ovr: parsedPredictedOvr,
            }).catch(() => {
                // Keep holding updates independent from prediction sync.
              })
          : Promise.resolve(),
      ]);

      closeEditModal();
      await fetchPortfolio({ showLoading: false });
      setSuccessMessage("Investment updated.");
    } catch {
      setEditError("Failed to update investment. Try again.");
    } finally {
      setEditSubmitting(false);
    }
  };

  if (!authReady) {
    return (
      <div className={styles.page}>
        <FloatingShieldsBackground />
        <Navbar />
        <main className={styles.content}>
          <p className={styles.loadingText}>Loading portfolio...</p>
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
        <section className={styles.header}>
          <h1>Portfolio</h1>
          <p>Track your card investments with the same flow as mobile, optimized for web.</p>
        </section>

        {successMessage ? <div className={styles.successCard}>{successMessage}</div> : null}
        {pageError ? <div className={styles.errorCard}>{pageError}</div> : null}

        <section className={styles.panel}>
          <div className={styles.panelHeader}>
            <div>
              <h2>Summary</h2>
              <p>{holdings.length} holdings</p>
            </div>
            <div className={styles.headerActions}>
              <button
                type="button"
                className={`${styles.privacyToggle} ${portfolio?.is_public ? styles.privacyPublic : styles.privacyPrivate}`}
                onClick={onTogglePrivacy}
                disabled={!portfolio || privacyUpdating}
              >
                {portfolio?.is_public ? <Globe size={14} /> : <Lock size={14} />}
                {portfolio?.is_public ? "Public" : "Private"}
              </button>
              <button type="button" className={styles.refreshButton} onClick={onRefresh} disabled={refreshing || loading}>
                <RefreshCcw size={14} />
                {refreshing ? "Refreshing" : "Refresh"}
              </button>
            </div>
          </div>

          {loading ? (
            <p className={styles.loadingText}>Loading your holdings...</p>
          ) : (
            <>
              {isPro ? (
                <>
                  <div className={styles.summaryGrid}>
                    <SummaryCard title="Total Invested" value={totals.invested} />
                    <SummaryCard title="Model Value" value={totals.modelValue} isPro />
                    <SummaryCard title="Model P/L" value={totals.modelPl} signed isPro />
                  </div>
                  <div className={styles.summaryGridSecondary}>
                    <SummaryCard title="Your Value" value={totals.yourValue} />
                    <SummaryCard title="Your P/L" value={totals.yourPl} signed />
                  </div>
                </>
              ) : (
                <div className={styles.summaryGrid}>
                  <SummaryCard title="Total Invested" value={totals.invested} />
                  <SummaryCard title="Your Value" value={totals.yourValue} />
                  <SummaryCard title="Your P/L" value={totals.yourPl} signed />
                </div>
              )}
            </>
          )}
        </section>

        <section className={styles.panel}>
          <button type="button" className={styles.addHeader} onClick={() => setIsAddOpen((prev) => !prev)}>
            <span>Add New Investment</span>
            {isAddOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </button>

          {isAddOpen ? (
            <div className={styles.addForm}>
              <div className={styles.searchField}>
                <label htmlFor="portfolio-card-search">Search Card</label>
                {selectedCard ? (
                  <div className={styles.selectedCardChip}>
                    <Image src={selectedCard.baked_img} alt={selectedCard.name} width={24} height={32} className={styles.cardThumb} />
                    <span>{selectedCard.name}</span>
                    <button type="button" className={styles.clearChipButton} onClick={resetAddForm} aria-label="Clear selected card">
                      <X size={14} />
                    </button>
                  </div>
                ) : (
                  <input
                    id="portfolio-card-search"
                    value={searchText}
                    onChange={(event) => setSearchText(event.target.value)}
                    placeholder="Start typing a player name"
                    autoComplete="off"
                  />
                )}
                {searching ? <span className={styles.searchStatus}>Searching...</span> : null}
                {!selectedCard && searchResults.length > 0 ? (
                  <div className={styles.searchResults}>
                    {searchResults.map((card) => (
                      <button
                        key={card.id}
                        type="button"
                        className={styles.searchResultRow}
                        onClick={() => {
                          setSelectedCard(card);
                          setSearchText(card.name);
                          setSearchResults([]);
                          setAddError(null);
                        }}
                      >
                        <Image src={card.baked_img} alt={card.name} width={30} height={40} className={styles.cardThumb} />
                        <span className={styles.resultCopy}>
                          <span className={styles.resultName}>{card.name}</span>
                          <span className={styles.resultMeta}>
                            {card.team_short_name} · {card.display_position} · {card.ovr} OVR
                          </span>
                        </span>
                      </button>
                    ))}
                  </div>
                ) : null}
              </div>

              <div className={styles.inputGrid}>
                <label>
                  Quantity
                  <input type="number" min={1} value={quantity} onChange={(event) => setQuantity(event.target.value)} placeholder="0" />
                </label>
                <label>
                  Avg Buy Price
                  <input type="number" min={1} value={avgBuyPrice} onChange={(event) => setAvgBuyPrice(event.target.value)} placeholder="0" />
                </label>
                <label>
                  Predicted OVR
                  <input
                    type="number"
                    min={0}
                    max={99}
                    value={projectedOvr}
                    onChange={(event) => setProjectedOvr(event.target.value)}
                    placeholder="0"
                  />
                </label>
              </div>

              <label className={styles.notesLabel}>
                Notes (Optional)
                <textarea
                  value={notes}
                  onChange={(event) => setNotes(event.target.value.slice(0, 500))}
                  placeholder="Add context for this investment"
                  rows={3}
                />
                <span className={styles.charCount}>{notes.length}/500</span>
              </label>

              {addError ? <div className={styles.inlineError}>{addError}</div> : null}

              <button
                type="button"
                className={styles.addButton}
                onClick={onAddHolding}
                disabled={!selectedCard || adding || !quantity || !avgBuyPrice || !projectedOvr}
              >
                <Plus size={15} />
                {adding ? "Adding..." : "Add Investment"}
              </button>
            </div>
          ) : null}
        </section>

        <section className={styles.panel}>
          <div className={styles.panelHeaderCompact}>
            <h2>Your Investments</h2>
            <span className={styles.countBadge}>{holdings.length}</span>
          </div>

          {loading ? (
            <p className={styles.loadingText}>Loading your investments...</p>
          ) : holdings.length === 0 ? (
            <div className={styles.emptyState}>No investments yet. Add your first card above.</div>
          ) : (
            <div className={styles.tableWrap}>
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th>Card</th>
                    <th>Qty</th>
                    <th>Avg Buy</th>
                    <th>Invested</th>
                    <th>Your OVR</th>
                    <th>Your Value</th>
                    <th>Your P/L</th>
                    {isPro ? <th>Model OVR</th> : null}
                    {isPro ? <th>Model Value</th> : null}
                    {isPro ? <th>Model P/L</th> : null}
                    <th>Notes</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {holdings.map((holding) => {
                    const avgPrice = holding.avg_price ?? 0;
                    const invested = holding.quantity * avgPrice;
                    const yourOvr = holding.user_predicted_ovr ?? holding.card.ovr;
                    const yourValue = holding.quantity * getQuicksellValue(yourOvr);
                    const yourPl = yourValue - invested;

                    const modelOvr = holding.card.predicted_ovr ?? holding.card.ovr;
                    const modelValue = holding.quantity * getQuicksellValue(modelOvr);
                    const modelPl = modelValue - invested;

                    return (
                      <tr key={holding.card_id}>
                        <td>
                          <Link href={`/cards/${holding.card.id}`} className={styles.playerLink}>
                            <Image
                              src={holding.card.baked_img}
                              alt={holding.card.name}
                              width={42}
                              height={56}
                              className={styles.cardThumbLarge}
                            />
                            <span>
                              <strong>{holding.card.name}</strong>
                              <small>
                                {holding.card.team_short_name} · {holding.card.display_position} · {holding.card.ovr} OVR
                              </small>
                            </span>
                          </Link>
                        </td>
                        <td>{holding.quantity}</td>
                        <td>
                          <span className={styles.moneyCell}>
                            <Image src="/images/stub.png" alt="" width={10} height={10} className={styles.stubIcon} />
                            {formatStubs(avgPrice)}
                          </span>
                        </td>
                        <td>
                          <span className={styles.moneyCell}>
                            <Image src="/images/stub.png" alt="" width={10} height={10} className={styles.stubIcon} />
                            {formatStubs(invested)}
                          </span>
                        </td>
                        <td>{yourOvr}</td>
                        <td>
                          <span className={styles.moneyCell}>
                            <Image src="/images/stub.png" alt="" width={10} height={10} className={styles.stubIcon} />
                            {formatStubs(yourValue)}
                          </span>
                        </td>
                        <td className={plClassName(yourPl)}>{signedStubs(yourPl)}</td>
                        {isPro ? <td>{modelOvr}</td> : null}
                        {isPro ? (
                          <td>
                            <span className={styles.moneyCell}>
                              <Image src="/images/stub.png" alt="" width={10} height={10} className={styles.stubIcon} />
                              {formatStubs(modelValue)}
                            </span>
                          </td>
                        ) : null}
                        {isPro ? <td className={plClassName(modelPl)}>{signedStubs(modelPl)}</td> : null}
                        <td className={styles.notesCell}>{holding.notes?.trim() ? holding.notes : "—"}</td>
                        <td>
                          <div className={styles.rowActions}>
                            <button type="button" className={styles.rowActionButton} onClick={() => openEditModal(holding)}>
                              <Pencil size={14} />
                            </button>
                            <button
                              type="button"
                              className={`${styles.rowActionButton} ${styles.rowActionDanger}`}
                              onClick={() => onRemoveHolding(holding)}
                            >
                              <Trash2 size={14} />
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </main>

      {editingHolding ? (
        <div className={styles.modalOverlay} onClick={closeEditModal}>
          <div className={styles.modalCard} onClick={(event) => event.stopPropagation()}>
            <div className={styles.modalHeader}>
              <h3>Edit Investment</h3>
              <button type="button" className={styles.modalClose} onClick={closeEditModal} aria-label="Close edit modal">
                <X size={16} />
              </button>
            </div>

            <div className={styles.modalPlayer}>
              <Image src={editingHolding.card.baked_img} alt={editingHolding.card.name} width={40} height={54} className={styles.cardThumbLarge} />
              <div>
                <strong>{editingHolding.card.name}</strong>
                <small>
                  {editingHolding.card.team_short_name} · {editingHolding.card.display_position} · {editingHolding.card.ovr} OVR
                </small>
              </div>
            </div>

            <div className={styles.inputGrid}>
              <label>
                Quantity
                <input type="number" min={1} value={editQuantity} onChange={(event) => setEditQuantity(event.target.value)} />
              </label>
              <label>
                Avg Buy Price
                <input type="number" min={0} value={editAvgPrice} onChange={(event) => setEditAvgPrice(event.target.value)} />
              </label>
              <label>
                Predicted OVR
                <input
                  type="number"
                  min={0}
                  max={99}
                  value={editProjectedOvr}
                  onChange={(event) => setEditProjectedOvr(event.target.value)}
                />
              </label>
            </div>

            <label className={styles.notesLabel}>
              Notes
              <textarea
                value={editNotes}
                onChange={(event) => setEditNotes(event.target.value.slice(0, 500))}
                rows={4}
                placeholder="Optional note"
              />
              <span className={styles.charCount}>{editNotes.length}/500</span>
            </label>

            {editError ? <div className={styles.inlineError}>{editError}</div> : null}

            <div className={styles.modalActions}>
              <button type="button" className={styles.cancelButton} onClick={closeEditModal} disabled={editSubmitting}>
                Cancel
              </button>
              <button type="button" className={styles.saveButton} onClick={onSaveEdit} disabled={editSubmitting}>
                {editSubmitting ? "Saving..." : "Save Changes"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
