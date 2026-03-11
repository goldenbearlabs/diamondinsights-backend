"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { ApiError, apiGet } from "@/lib/api";

import styles from "./page.module.css";

type FlippingSortBy =
  | "profit"
  | "spread"
  | "profit_per_min"
  | "margin"
  | "orders"
  | "buys"
  | "sells"
  | "buys_sells"
  | "buy"
  | "sell"
  | "ovr"
  | "name";

type FlippingRow = {
  card_id: string;
  name: string | null;
  team: string | null;
  ovr: number;
  series: string | null;
  year: number | null;
  baked_img: string | null;
  best_sell_price: number;
  best_buy_price: number;
  effective_buy_price: number;
  quicksell_price: number;
  uses_quicksell_buy: boolean;
  after_tax_sell_price: number;
  spread: number;
  profit: number;
  profit_margin_pct: number | null;
  orders_1h: number;
  buys_1h: number;
  sells_1h: number;
  avg_completed_price_1h: number | null;
  latest_completed_order_at: string | null;
};

type SortDirection = "asc" | "desc";
type AppliedFilters = {
  min_buy?: number;
  max_buy?: number;
  min_sell?: number;
  max_sell?: number;
  min_ovr?: number;
  max_ovr?: number;
};

type FilterDraft = {
  minBuy: string;
  maxBuy: string;
  minSell: string;
  maxSell: string;
  minOvr: string;
  maxOvr: string;
};

const PAGE_SIZE = 30;
const DEFAULT_SORT: FlippingSortBy = "profit_per_min";
const TEXT_SORT_KEYS: FlippingSortBy[] = ["name"];

const EMPTY_FILTER_DRAFT: FilterDraft = {
  minBuy: "",
  maxBuy: "",
  minSell: "",
  maxSell: "",
  minOvr: "",
  maxOvr: "",
};

function formatStubs(value: number): string {
  return `$${Math.round(value).toLocaleString()}`;
}

function formatSignedStubs(value: number): string {
  if (value > 0) {
    return `+$${Math.round(value).toLocaleString()}`;
  }
  if (value < 0) {
    return `-$${Math.abs(Math.round(value)).toLocaleString()}`;
  }
  return "$0";
}

function formatRateStubs(value: number): string {
  const rounded = Math.round(value);
  if (rounded > 0) {
    return `+$${rounded.toLocaleString()}`;
  }
  if (rounded < 0) {
    return `-$${Math.abs(rounded).toLocaleString()}`;
  }
  return "$0";
}

function formatPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) {
    return "-";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function parseOptionalInt(value: string, max: number | null = null): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) {
    return undefined;
  }
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return undefined;
  }
  if (max != null) {
    return Math.min(parsed, max);
  }
  return parsed;
}

function computeProfitPerMin(item: FlippingRow): number {
  return ((item.orders_1h / 2) * item.profit) / 60;
}

function rangeSummary(min?: number, max?: number): string {
  if (min == null && max == null) {
    return "Any";
  }
  if (min != null && max != null) {
    return `${min}-${max}`;
  }
  if (min != null) {
    return `${min}+`;
  }
  return `<=${max}`;
}

function SortHeader({
  label,
  sortKey,
  activeSortKey,
  direction,
  onPress,
}: {
  label: string;
  sortKey: FlippingSortBy;
  activeSortKey: FlippingSortBy;
  direction: SortDirection;
  onPress: (key: FlippingSortBy) => void;
}) {
  const isActive = activeSortKey === sortKey;
  return (
    <button
      type="button"
      className={`${styles.sortHeader} ${isActive ? styles.sortHeaderActive : ""}`}
      onClick={() => onPress(sortKey)}
    >
      {label}
      {isActive ? (direction === "desc" ? " ↓" : " ↑") : ""}
    </button>
  );
}

export default function FlippingPage() {
  const router = useRouter();

  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [hasNext, setHasNext] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<FlippingRow[]>([]);

  const [sortKey, setSortKey] = useState<FlippingSortBy>(DEFAULT_SORT);
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [profitableOnly, setProfitableOnly] = useState(false);

  const [searchInput, setSearchInput] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [filterDraft, setFilterDraft] = useState<FilterDraft>(EMPTY_FILTER_DRAFT);
  const [appliedFilters, setAppliedFilters] = useState<AppliedFilters>({});

  const offset = useMemo(() => (page - 1) * PAGE_SIZE, [page]);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSearchQuery(searchInput.trim());
      setPage(1);
    }, 250);
    return () => {
      window.clearTimeout(timer);
    };
  }, [searchInput]);

  const fetchRows = useCallback(async () => {
    setLoading(true);
    setError(null);

    const query = new URLSearchParams();
    query.set("limit", String(PAGE_SIZE));
    query.set("offset", String(offset));
    query.set("sort_by", sortKey);
    query.set("sort_dir", sortDirection);
    query.set("profitable_only", profitableOnly ? "true" : "false");

    if (searchQuery) {
      query.set("name", searchQuery);
    }
    if (appliedFilters.min_buy != null) {
      query.set("min_buy", String(appliedFilters.min_buy));
    }
    if (appliedFilters.max_buy != null) {
      query.set("max_buy", String(appliedFilters.max_buy));
    }
    if (appliedFilters.min_sell != null) {
      query.set("min_sell", String(appliedFilters.min_sell));
    }
    if (appliedFilters.max_sell != null) {
      query.set("max_sell", String(appliedFilters.max_sell));
    }
    if (appliedFilters.min_ovr != null) {
      query.set("min_ovr", String(appliedFilters.min_ovr));
    }
    if (appliedFilters.max_ovr != null) {
      query.set("max_ovr", String(appliedFilters.max_ovr));
    }

    try {
      const data = await apiGet<FlippingRow[]>(`/flipping?${query.toString()}`);
      const nextRows = Array.isArray(data) ? data : [];
      setRows(nextRows);
      setHasNext(nextRows.length === PAGE_SIZE);
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        setError(err.body || `Error ${err.status}`);
      } else {
        setError("Failed to load flipping data.");
      }
      setRows([]);
      setHasNext(false);
    } finally {
      setLoading(false);
    }
  }, [offset, sortKey, sortDirection, profitableOnly, searchQuery, appliedFilters]);

  useEffect(() => {
    void fetchRows();
  }, [fetchRows]);

  const positiveCount = useMemo(() => rows.filter((item) => item.profit > 0).length, [rows]);

  const onSortChange = (nextKey: FlippingSortBy) => {
    if (sortKey === nextKey) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      setPage(1);
      return;
    }
    setSortKey(nextKey);
    setSortDirection(TEXT_SORT_KEYS.includes(nextKey) ? "asc" : "desc");
    setPage(1);
  };

  const setNumericDraft = (key: keyof FilterDraft, value: string) => {
    const sanitized = value.replace(/[^0-9]/g, "");
    setFilterDraft((current) => ({ ...current, [key]: sanitized }));
  };

  const applyFilters = () => {
    setAppliedFilters({
      min_buy: parseOptionalInt(filterDraft.minBuy),
      max_buy: parseOptionalInt(filterDraft.maxBuy),
      min_sell: parseOptionalInt(filterDraft.minSell),
      max_sell: parseOptionalInt(filterDraft.maxSell),
      min_ovr: parseOptionalInt(filterDraft.minOvr, 99),
      max_ovr: parseOptionalInt(filterDraft.maxOvr, 99),
    });
    setPage(1);
  };

  const clearFilters = () => {
    setFilterDraft(EMPTY_FILTER_DRAFT);
    setAppliedFilters({});
    setPage(1);
  };

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <section className={styles.content}>
        <header className={styles.header}>
          <h1>Flipping</h1>
          <p>Same flipping model as mobile: sortable by profitability, speed, and spread using live market conditions.</p>
        </header>

        <div className={styles.filtersCard}>
          <div className={styles.filtersTopRow}>
            <label className={styles.searchInputWrap}>
              <span>Search Card</span>
              <input
                value={searchInput}
                onChange={(event) => setSearchInput(event.target.value)}
                placeholder="Start typing card name..."
              />
            </label>
            <button
              type="button"
              className={`${styles.profitableToggle} ${profitableOnly ? styles.profitableToggleOn : ""}`}
              onClick={() => {
                setProfitableOnly((current) => !current);
                setPage(1);
              }}
            >
              {profitableOnly ? "Profitable: On" : "Profitable: Off"}
            </button>
            <button type="button" className={styles.applyButton} onClick={applyFilters}>
              Apply Filters
            </button>
            <button type="button" className={styles.clearButton} onClick={clearFilters}>
              Clear
            </button>
          </div>

          <div className={styles.rangeGrid}>
            <div className={styles.rangeGroup}>
              <span>Buy Range</span>
              <div className={styles.rangeInputs}>
                <input value={filterDraft.minBuy} onChange={(event) => setNumericDraft("minBuy", event.target.value)} placeholder="Min" />
                <input value={filterDraft.maxBuy} onChange={(event) => setNumericDraft("maxBuy", event.target.value)} placeholder="Max" />
              </div>
            </div>
            <div className={styles.rangeGroup}>
              <span>Sell Range</span>
              <div className={styles.rangeInputs}>
                <input value={filterDraft.minSell} onChange={(event) => setNumericDraft("minSell", event.target.value)} placeholder="Min" />
                <input value={filterDraft.maxSell} onChange={(event) => setNumericDraft("maxSell", event.target.value)} placeholder="Max" />
              </div>
            </div>
            <div className={styles.rangeGroup}>
              <span>OVR Range</span>
              <div className={styles.rangeInputs}>
                <input value={filterDraft.minOvr} onChange={(event) => setNumericDraft("minOvr", event.target.value)} placeholder="Min" />
                <input value={filterDraft.maxOvr} onChange={(event) => setNumericDraft("maxOvr", event.target.value)} placeholder="Max" />
              </div>
            </div>
          </div>

          <p className={styles.filterSummary}>
            Showing {rows.length} cards. {positiveCount} currently profitable. Buy: {rangeSummary(appliedFilters.min_buy, appliedFilters.max_buy)} | Sell:{" "}
            {rangeSummary(appliedFilters.min_sell, appliedFilters.max_sell)} | OVR: {rangeSummary(appliedFilters.min_ovr, appliedFilters.max_ovr)}
          </p>
        </div>

        {error ? <div className={styles.errorCard}>{error}</div> : null}

        <div className={styles.tableCard}>
          <div className={styles.tableScroll}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th className={styles.imageCol}>Img</th>
                  <th className={styles.cardCol}>
                    <SortHeader label="Card" sortKey="name" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.ovrCol}>
                    <SortHeader label="OVR" sortKey="ovr" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.moneyCol}>
                    <SortHeader label="Profit/Min" sortKey="profit_per_min" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.moneyCol}>
                    <SortHeader label="Profit" sortKey="profit" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.moneyCol}>
                    <SortHeader label="Buy" sortKey="buy" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.moneyCol}>
                    <SortHeader label="Sell" sortKey="sell" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.marginCol}>
                    <SortHeader label="Profit %" sortKey="margin" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                  <th className={styles.buysSellsCol}>
                    <SortHeader label="Buys/Sells" sortKey="buys_sells" activeSortKey={sortKey} direction={sortDirection} onPress={onSortChange} />
                  </th>
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 && !loading ? (
                  <tr>
                    <td colSpan={9} className={styles.emptyRow}>
                      No flipping rows found.
                    </td>
                  </tr>
                ) : (
                  rows.map((item, index) => (
                    <tr key={item.card_id} className={index % 2 === 0 ? styles.evenRow : styles.oddRow}>
                      <td className={styles.imageCol}>
                        <button
                          type="button"
                          className={styles.imageButton}
                          onClick={() => router.push(`/cards/${encodeURIComponent(item.card_id)}`)}
                        >
                          {item.baked_img ? (
                            <Image src={item.baked_img} alt={item.name || "Card"} width={38} height={50} className={styles.cardImage} unoptimized />
                          ) : (
                            <span className={styles.imageFallback}>N/A</span>
                          )}
                        </button>
                      </td>
                      <td className={`${styles.cardCol} ${sortKey === "name" ? styles.activeColumn : ""}`}>
                        <Link href={`/cards/${encodeURIComponent(item.card_id)}`} className={styles.cardNameLink}>
                          {item.name || "Unknown Card"}
                        </Link>
                        <span className={styles.cardSubline}>
                          {item.team || "-"} | {item.series || "-"}
                          {item.year ? ` | ${item.year}` : ""}
                        </span>
                      </td>
                      <td className={`${styles.ovrCol} ${styles.centerCell} ${sortKey === "ovr" ? styles.activeColumn : ""}`}>{item.ovr}</td>
                      <td className={`${styles.moneyCol} ${styles.centerCell} ${sortKey === "profit_per_min" ? styles.activeColumn : ""}`}>
                        <span className={computeProfitPerMin(item) >= 0 ? styles.positiveText : styles.negativeText}>
                          {formatRateStubs(computeProfitPerMin(item))}
                        </span>
                      </td>
                      <td className={`${styles.moneyCol} ${styles.centerCell} ${sortKey === "profit" ? styles.activeColumn : ""}`}>
                        <span className={item.profit >= 0 ? styles.positiveText : styles.negativeText}>{formatSignedStubs(item.profit)}</span>
                      </td>
                      <td className={`${styles.moneyCol} ${styles.centerCell} ${sortKey === "buy" ? styles.activeColumn : ""}`}>
                        {formatStubs(item.effective_buy_price)}
                        {item.uses_quicksell_buy ? "*" : ""}
                      </td>
                      <td className={`${styles.moneyCol} ${styles.centerCell} ${sortKey === "sell" ? styles.activeColumn : ""}`}>
                        {formatStubs(item.best_sell_price)}
                      </td>
                      <td className={`${styles.marginCol} ${styles.centerCell} ${sortKey === "margin" ? styles.activeColumn : ""}`}>
                        {formatPct(item.profit_margin_pct)}
                      </td>
                      <td className={`${styles.buysSellsCol} ${styles.centerCell} ${sortKey === "buys_sells" ? styles.activeColumn : ""}`}>
                        {item.buys_1h}/{item.sells_1h}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
          {loading ? <div className={styles.loadingOverlay}>Loading...</div> : null}
        </div>

        <p className={styles.tableNote}>* Buy price uses quicksell fallback when no buy orders are present.</p>

        <div className={styles.pagination}>
          <button
            type="button"
            onClick={() => setPage((current) => Math.max(1, current - 1))}
            disabled={loading || page === 1}
            className={styles.pageButton}
          >
            Prev
          </button>
          <span className={styles.pageText}>Page {page}</span>
          <button
            type="button"
            onClick={() => setPage((current) => current + 1)}
            disabled={loading || !hasNext}
            className={styles.pageButton}
          >
            Next
          </button>
        </div>
      </section>
    </main>
  );
}
