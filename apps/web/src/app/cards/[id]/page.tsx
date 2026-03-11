"use client";

import Image from "next/image";
import Link from "next/link";
import { useParams, useRouter } from "next/navigation";
import { type ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import { onAuthStateChanged } from "firebase/auth";
import { ArrowLeft, ChevronDown, ChevronUp, Crown, Lock, MessageSquare } from "lucide-react";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { ApiError, apiGet, apiGetAuth, apiPostAuth, getMyEntitlements } from "@/lib/api";
import { getFirebaseAuth } from "@/lib/firebase";

import styles from "./page.module.css";

type CardQuirk = {
  card_id: string;
  name: string;
  description: string;
  img: string;
};

type PitchData = {
  card_id: string;
  name: string;
  speed: number;
  control: number;
  movement: number;
};

type CompletedOrder = {
  card_id: string;
  date: string;
  price: number;
  is_buy: boolean | null;
};

type PriceHistoryRow = {
  card_id: string;
  date: string;
  best_buy_price: number | null;
  best_sell_price: number | null;
  volume: number | null;
};

type PriceChartTick = {
  y: number;
  label: string;
};

type PriceChartXAxisTick = {
  x: number;
  label: string;
};

type PriceHistoryChart =
  | {
      hasChart: false;
      tableRows: PriceHistoryRow[];
    }
  | {
      hasChart: true;
      tableRows: PriceHistoryRow[];
      width: number;
      height: number;
      padLeft: number;
      padRight: number;
      buyPath: string;
      sellPath: string;
      yTicks: PriceChartTick[];
      xTicks: PriceChartXAxisTick[];
    };

type CardDetail = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  rarity: string;
  display_position: string;
  display_primary_position?: string | null;
  display_secondary_positions?: string | null;
  age: number;
  bat_hand: string;
  throw_hand: string;
  is_hitter: boolean;
  baked_img: string;
  comment_count: number;
  user_prediction_count: number;
  predicted_ovr: number | null;
  community_predicted_ovr: number | null;
  user_prediction: number | null;
  true_overall_rounded?: number | null;
  meta_overall_rounded?: number | null;
  your_overall_rounded?: number | null;
  true_overall_by_position?: Record<string, number> | null;
  meta_overall_by_position?: Record<string, number> | null;
  your_overall_by_position?: Record<string, number> | null;
  predicted_attributes: Record<string, number> | null;
  best_buy_price: number | null;
  best_sell_price: number | null;
  quicksell_value: number | null;
  buy_now_uses_quicksell: boolean | null;
  buy_now_above_quicksell_pct: number | null;
  stamina: number;
  pitching_clutch: number;
  hits_per_bf: number;
  k_per_bf: number;
  bb_per_bf: number;
  hr_per_bf: number;
  contact_left: number;
  contact_right: number;
  power_left: number;
  power_right: number;
  plate_vision: number;
  batting_clutch: number;
  fielding_ability: number;
  arm_strength: number;
  arm_accuracy: number;
  reaction_time: number;
  blocking: number;
  speed: number;
  baserunning_ability: number;
  baserunning_aggression: number;
  quirks: CardQuirk[] | null;
};

type UserPredictionResponse = {
  predicted_ovr: number;
};

type CardComment = {
  id: number;
  created_at: string;
  updated_at: string | null;
  edited_at: string | null;
  content: string;
  is_deleted: boolean;
  user_id: number;
  user_firebase_id: string;
  user_display_name: string;
  user_profile_img: string | null;
  likes_count: number;
  is_liked_by_me: boolean;
  parent_id?: number | null;
};

type BattingSplitStats = {
  split: string;
  pa: number;
  ab: number;
  r: number;
  h: number;
  doubles: number;
  triples: number;
  hr: number;
  rbi: number;
  bb: number;
  so: number;
  hbp: number;
  tb: number;
  sac_flies: number;
  avg: number;
  obp: number;
  slg: number;
  ops: number;
};

type PitchingSplitStats = {
  split: string;
  ip: number;
  h: number;
  r: number;
  er: number;
  hr: number;
  bb: number;
  k: number;
  batters_faced: number;
  strike_pct: number;
  era: number;
  whip: number;
  k9: number;
};

type SeasonStats = {
  is_hitter: boolean;
  season: number;
  batting?: {
    overall: BattingSplitStats;
    splits: BattingSplitStats[];
  } | null;
  pitching?: {
    overall: PitchingSplitStats;
    splits: PitchingSplitStats[];
  } | null;
};

type StatsWindow = "season" | "7d" | "14d" | "last_update";

type DetailTab = "attributes" | "market" | "stats" | "pro";

const BATTING_PREDICTION_KEYS = [
  { key: "CON_R", label: "Contact R" },
  { key: "CON_L", label: "Contact L" },
  { key: "POW_R", label: "Power R" },
  { key: "POW_L", label: "Power L" },
  { key: "VIS", label: "Vision" },
  { key: "CLT", label: "Clutch" },
] as const;

const PITCHING_PREDICTION_KEYS = [
  { key: "STA", label: "Stamina" },
  { key: "PCLT", label: "Clutch" },
  { key: "H_9", label: "H/9" },
  { key: "K_9", label: "K/9" },
  { key: "BB_9", label: "BB/9" },
] as const;

const TWO_WAY_PLAYERS = ["Shohei Ohtani"];
const STATS_WINDOW_OPTIONS: Array<{ key: Exclude<StatsWindow, "last_update">; label: string }> = [
  { key: "season", label: "Season" },
  { key: "7d", label: "Last 7 Days" },
  { key: "14d", label: "Last 14 Days" },
];

const SPLIT_LABELS: Record<string, string> = {
  vslhp: "vs LHP",
  vsrhp: "vs RHP",
  vslhb: "vs LHB",
  vsrhb: "vs RHB",
  risp: "RISP",
  overall: "Overall",
};

const VOLUME_RANGE_OPTIONS = [
  { key: "48h", label: "48H", minutes: 48 * 60 },
  { key: "24h", label: "24H", minutes: 24 * 60 },
  { key: "12h", label: "12H", minutes: 12 * 60 },
  { key: "6h", label: "6H", minutes: 6 * 60 },
  { key: "1h", label: "1H", minutes: 60 },
  { key: "30m", label: "30M", minutes: 30 },
  { key: "10m", label: "10M", minutes: 10 },
] as const;

type VolumeRangeKey = (typeof VOLUME_RANGE_OPTIONS)[number]["key"];

function formatStubs(value: number | null | undefined): string {
  if (value == null || Number.isNaN(value)) {
    return "--";
  }
  return value.toLocaleString("en-US");
}

function formatPositionSummary(primary: string, secondary: string | null | undefined): string {
  const normalizedPrimary = (primary || "").trim();
  const normalizedSecondary = (secondary || "").trim();
  if (!normalizedSecondary) {
    return normalizedPrimary;
  }
  return `${normalizedPrimary} • ${normalizedSecondary}`;
}

function formatCommentTimestamp(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "";
  }
  return parsed.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function normalizePositionValue(value: string | null | undefined): string {
  return (value || "").trim().toUpperCase();
}

function parseSecondaryPositions(value: string | null | undefined): string[] {
  if (!value) {
    return [];
  }
  return value
    .replaceAll("/", ",")
    .split(",")
    .map((token) => normalizePositionValue(token))
    .filter((token) => token.length > 0);
}

function resolveOverallForPosition(
  map: Record<string, number> | null | undefined,
  position: string,
  fallback: number | null | undefined,
): number | null {
  if (position && map) {
    const value = map[position];
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.round(value);
    }
  }
  if (typeof fallback === "number" && Number.isFinite(fallback)) {
    return Math.round(fallback);
  }
  return null;
}

function formatPredictionChange(prediction: number | null | undefined, current: number | null | undefined): { text: string; trend: "up" | "down" | "flat" } {
  if (prediction == null || !Number.isFinite(prediction)) {
    return { text: "--", trend: "flat" };
  }
  const roundedPrediction = Math.round(prediction);
  if (current == null || !Number.isFinite(current)) {
    return { text: `${roundedPrediction}`, trend: "flat" };
  }
  const roundedCurrent = Math.round(current);
  const delta = roundedPrediction - roundedCurrent;
  if (delta > 0) {
    return { text: `${roundedPrediction} (+${delta})`, trend: "up" };
  }
  if (delta < 0) {
    return { text: `${roundedPrediction} (${delta})`, trend: "down" };
  }
  return { text: `${roundedPrediction} (0)`, trend: "flat" };
}

function StatBar({ label, value, color }: { label: string; value: number; color: string }) {
  const normalized = Math.max(0, Math.min(value ?? 0, 99));
  return (
    <div className={styles.statBarRow}>
      <div className={styles.statBarHead}>
        <span>{label}</span>
        <strong>{normalized}</strong>
      </div>
      <div className={styles.statTrack}>
        <div className={styles.statFill} style={{ width: `${normalized}%`, background: color }} />
      </div>
    </div>
  );
}

function PitchGauge({ value, color, label }: { value: number; color: string; label: string }) {
  const radius = 30;
  const strokeWidth = 8;
  const padding = Math.ceil(strokeWidth / 2) + 2;
  const cx = radius + padding;
  const cy = radius + padding;
  const width = cx * 2;
  const height = cy + 4;
  const max = 99;
  const normalized = Math.max(0, Math.min(value ?? 0, max));
  const pct = normalized / max;
  const arcLen = Math.PI * radius;
  const path = `M ${cx - radius} ${cy} A ${radius} ${radius} 0 0 1 ${cx + radius} ${cy}`;

  return (
    <div className={styles.pitchGauge}>
      <svg width={width} height={height} className={styles.pitchGaugeSvg} role="img" aria-label={`${label} ${normalized}`}>
        <path d={path} stroke="rgba(148, 163, 184, 0.28)" strokeWidth={strokeWidth} fill="none" strokeLinecap="round" />
        <path
          d={path}
          stroke={color}
          strokeWidth={strokeWidth}
          fill="none"
          strokeLinecap="round"
          strokeDasharray={`${arcLen} ${arcLen}`}
          strokeDashoffset={arcLen * (1 - pct)}
        />
      </svg>
      <span className={styles.pitchGaugeLabel}>{label}</span>
      <strong className={styles.pitchGaugeValue}>{normalized}</strong>
    </div>
  );
}

function formatCompactNumber(value: number): string {
  if (!Number.isFinite(value)) {
    return "--";
  }
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1)}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1)}k`;
  }
  return `${Math.round(value)}`;
}

function formatStat(value: number | null | undefined, decimals = 3): string {
  if (value == null || !Number.isFinite(value)) {
    return "-";
  }
  const fixed = value.toFixed(decimals);
  if (decimals === 3) {
    return fixed.replace(/^0/, "");
  }
  return fixed;
}

function getStatsWindowLabel(value: StatsWindow): string {
  if (value === "7d") {
    return "Last 7 Days";
  }
  if (value === "14d") {
    return "Last 14 Days";
  }
  if (value === "last_update") {
    return "Since Last Roster Update";
  }
  return "Season Totals";
}

function parseMarketDate(value: string | null | undefined): Date | null {
  if (!value) {
    return null;
  }
  const normalized = value.includes("T")
    ? value
    : /^\d{4}-\d{2}-\d{2}$/.test(value)
      ? `${value}T00:00:00`
      : value.replace(" ", "T");
  const parsed = new Date(normalized);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function formatClockLabel(value: string | null | undefined): string {
  const parsed = parseMarketDate(value);
  if (!parsed) {
    return "--";
  }
  return parsed.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function formatDayLabel(value: string | null | undefined): string {
  const parsed = parseMarketDate(value);
  if (!parsed) {
    return "--";
  }
  return parsed.toLocaleDateString([], { month: "short", day: "numeric" });
}

function formatDayTableLabel(value: string | null | undefined): string {
  const parsed = parseMarketDate(value);
  if (!parsed) {
    return "--";
  }
  return parsed.toLocaleDateString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
  });
}

function buildLinePath(points: Array<{ x: number; y: number }>): string {
  if (points.length === 0) {
    return "";
  }
  return points.map((point, index) => `${index === 0 ? "M" : "L"} ${point.x} ${point.y}`).join(" ");
}

function PriceHistoryChart({
  history,
  loading,
}: {
  history: PriceHistoryRow[];
  loading: boolean;
}) {
  const chart = useMemo<PriceHistoryChart | null>(() => {
    const orderedRows = [...(history || [])].sort((a, b) => (parseMarketDate(a.date)?.getTime() ?? 0) - (parseMarketDate(b.date)?.getTime() ?? 0));
    if (orderedRows.length === 0) {
      return null;
    }

    type PricePoint = {
      ts: number;
      labelDate: string;
      buy: number;
      sell: number;
    };

    const points: PricePoint[] = [];
    let lastBuy =
      orderedRows.find((row) => typeof row.best_buy_price === "number" && row.best_buy_price > 0)?.best_buy_price ?? 0;
    let lastSell =
      orderedRows.find((row) => typeof row.best_sell_price === "number" && row.best_sell_price > 0)?.best_sell_price ?? 0;

    orderedRows.forEach((row) => {
      const parsed = parseMarketDate(row.date);
      if (!parsed) {
        return;
      }
      if (typeof row.best_buy_price === "number" && row.best_buy_price > 0) {
        lastBuy = row.best_buy_price;
      }
      if (typeof row.best_sell_price === "number" && row.best_sell_price > 0) {
        lastSell = row.best_sell_price;
      }
      if (lastBuy <= 0 && lastSell <= 0) {
        return;
      }
      points.push({
        ts: parsed.getTime(),
        labelDate: row.date,
        buy: lastBuy,
        sell: lastSell,
      });
    });

    const tableRows = [...orderedRows].reverse();
    if (points.length === 0) {
      return {
        hasChart: false,
        tableRows,
      };
    }

    const dedupedPoints: PricePoint[] = [];
    points.forEach((point) => {
      const last = dedupedPoints[dedupedPoints.length - 1];
      if (last && last.ts === point.ts) {
        dedupedPoints[dedupedPoints.length - 1] = point;
      } else {
        dedupedPoints.push(point);
      }
    });

    const buySeries = dedupedPoints.map((point) => point.buy);
    const sellSeries = dedupedPoints.map((point) => point.sell);
    const allValues = [...buySeries, ...sellSeries].filter((value) => value > 0);
    if (allValues.length === 0) {
      return null;
    }

    const minRaw = Math.min(...allValues);
    const maxRaw = Math.max(...allValues);
    const range = maxRaw - minRaw || 1;
    const paddedMin = Math.max(0, minRaw - range * 0.15);
    const paddedMax = maxRaw + range * 0.15;
    const yMin = Math.floor(paddedMin / 50) * 50;
    let yMax = Math.ceil(paddedMax / 50) * 50;
    if (yMax <= yMin) {
      yMax = yMin + 50;
    }

    const height = 240;
    const padLeft = 52;
    const padRight = 16;
    const padTop = 14;
    const padBottom = 34;
    const pointSpacing = 40;
    const plotWidth = Math.max(860, dedupedPoints.length * pointSpacing);
    const width = padLeft + padRight + plotWidth;
    const plotHeight = height - padTop - padBottom;
    const scaleX = (index: number) => padLeft + (dedupedPoints.length <= 1 ? plotWidth / 2 : (index / (dedupedPoints.length - 1)) * plotWidth);
    const scaleY = (value: number) => padTop + (1 - (value - yMin) / (yMax - yMin)) * plotHeight;

    const buyPoints = buySeries.map((value, index) => ({ x: scaleX(index), y: scaleY(value) }));
    const sellPoints = sellSeries.map((value, index) => ({ x: scaleX(index), y: scaleY(value) }));
    const yTicks = Array.from({ length: 5 }, (_, i) => {
      const value = yMax - ((yMax - yMin) * i) / 4;
      const y = padTop + (plotHeight * i) / 4;
      return { y, label: formatCompactNumber(value) };
    });
    const tickCount = Math.min(6, dedupedPoints.length);
    const xIndexes = Array.from({ length: tickCount }, (_, i) =>
      Math.round((i * (dedupedPoints.length - 1)) / Math.max(1, tickCount - 1)),
    ).filter((value, index, arr) => arr.indexOf(value) === index);
    const xTicks = xIndexes.map((index) => ({ x: scaleX(index), label: formatDayLabel(dedupedPoints[index]?.labelDate) }));

    return {
      hasChart: true,
      tableRows,
      width,
      height,
      padLeft,
      padRight,
      buyPath: buildLinePath(buyPoints),
      sellPath: buildLinePath(sellPoints),
      yTicks,
      xTicks,
    };
  }, [history]);

  if (loading) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>Loading price history...</p>
      </div>
    );
  }

  if (!chart) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>No market history available.</p>
      </div>
    );
  }

  if (!chart.hasChart) {
    return (
      <div className={styles.marketChartCard}>
        <div className={styles.marketChartHeader}>
          <h3>Daily Price History</h3>
          <div className={styles.marketLegend}>
            <span className={styles.marketLegendItem}>
              <span className={`${styles.marketLegendDot} ${styles.marketLegendSell}`} />
              Sell
            </span>
            <span className={styles.marketLegendItem}>
              <span className={`${styles.marketLegendDot} ${styles.marketLegendBuy}`} />
              Buy
            </span>
          </div>
        </div>
        <p className={styles.marketChartSubtitle}>Best buy and best sell by day (from `price_history`).</p>
        <p className={styles.marketChartLoading}>No chartable daily values yet.</p>
        <div className={styles.priceHistoryTableWrap}>
          <table className={styles.priceHistoryTable}>
            <thead>
              <tr>
                <th>Date</th>
                <th>Best Sell</th>
                <th>Best Buy</th>
                <th>Spread</th>
                <th>Volume</th>
              </tr>
            </thead>
            <tbody>
              {chart.tableRows.map((row) => {
                const spread =
                  typeof row.best_sell_price === "number" && typeof row.best_buy_price === "number"
                    ? row.best_sell_price - row.best_buy_price
                    : null;
                return (
                  <tr key={`${row.card_id}-${row.date}`}>
                    <td>{formatDayTableLabel(row.date)}</td>
                    <td>{formatStubs(row.best_sell_price)}</td>
                    <td>{formatStubs(row.best_buy_price)}</td>
                    <td>{formatStubs(spread)}</td>
                    <td>{row.volume != null ? formatCompactNumber(row.volume) : "--"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.marketChartCard}>
      <div className={styles.marketChartHeader}>
        <h3>Daily Price History</h3>
        <div className={styles.marketLegend}>
          <span className={styles.marketLegendItem}>
            <span className={`${styles.marketLegendDot} ${styles.marketLegendSell}`} />
            Sell
          </span>
          <span className={styles.marketLegendItem}>
            <span className={`${styles.marketLegendDot} ${styles.marketLegendBuy}`} />
            Buy
          </span>
        </div>
      </div>
      <p className={styles.marketChartSubtitle}>Best buy and best sell by day (from `price_history`).</p>
      <div className={styles.marketChartScroll}>
        <svg
          width={chart.width}
          height={chart.height}
          viewBox={`0 0 ${chart.width} ${chart.height}`}
          className={styles.marketSpreadSvg}
          role="img"
          aria-label="Daily price history chart"
        >
          {chart.yTicks.map((tick) => (
            <g key={tick.y}>
              <line x1={chart.padLeft} y1={tick.y} x2={chart.width - chart.padRight} y2={tick.y} className={styles.marketGridLine} />
              <text x={chart.padLeft - 8} y={tick.y + 3} textAnchor="end" className={styles.marketAxisLabel}>
                {tick.label}
              </text>
            </g>
          ))}
          <path d={chart.sellPath} className={styles.marketSellLine} />
          <path d={chart.buyPath} className={styles.marketBuyLine} />
          {chart.xTicks.map((tick, index) => (
            <text key={`${tick.x}-${tick.label}-${index}`} x={tick.x} y={chart.height - 8} textAnchor="middle" className={styles.marketAxisLabel}>
              {tick.label}
            </text>
          ))}
        </svg>
      </div>
      <div className={styles.priceHistoryTableWrap}>
        <table className={styles.priceHistoryTable}>
          <thead>
            <tr>
              <th>Date</th>
              <th>Best Sell</th>
              <th>Best Buy</th>
              <th>Spread</th>
              <th>Volume</th>
            </tr>
          </thead>
          <tbody>
            {chart.tableRows.map((row) => {
              const spread =
                typeof row.best_sell_price === "number" && typeof row.best_buy_price === "number"
                  ? row.best_sell_price - row.best_buy_price
                  : null;
              return (
                <tr key={`${row.card_id}-${row.date}`}>
                  <td>{formatDayTableLabel(row.date)}</td>
                  <td>{formatStubs(row.best_sell_price)}</td>
                  <td>{formatStubs(row.best_buy_price)}</td>
                  <td>{formatStubs(spread)}</td>
                  <td>{row.volume != null ? formatCompactNumber(row.volume) : "--"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CompletedOrdersChart({
  orders,
  loading,
}: {
  orders: CompletedOrder[];
  loading: boolean;
}) {
  const chart = useMemo(() => {
    const normalizedOrders = [...(orders || [])]
      .map((order) => ({
        ...order,
        parsedTime: parseMarketDate(order.date)?.getTime() ?? 0,
      }))
      .filter((order) => order.parsedTime > 0);
    const latestTs = normalizedOrders.reduce((maxTs, order) => Math.max(maxTs, order.parsedTime), 0);
    const cutoff = latestTs - 48 * 60 * 60 * 1000;
    const orderedOrders = normalizedOrders.filter((order) => order.parsedTime >= cutoff).sort((a, b) => a.parsedTime - b.parsedTime);

    if (orderedOrders.length === 0) {
      return null;
    }

    type PricePoint = {
      ts: number;
      labelDate: string;
      buy: number;
      sell: number;
    };

    let lastBuy = orderedOrders.find((order) => order.is_buy === true)?.price ?? 0;
    let lastSell = orderedOrders.find((order) => order.is_buy === false)?.price ?? 0;
    const points: PricePoint[] = [];

    orderedOrders.forEach((order) => {
      if (order.is_buy === true) {
        lastBuy = order.price;
      } else if (order.is_buy === false) {
        lastSell = order.price;
      }
      if (lastBuy <= 0 && lastSell <= 0) {
        return;
      }
      points.push({
        ts: order.parsedTime,
        labelDate: order.date,
        buy: lastBuy,
        sell: lastSell,
      });
    });

    if (points.length === 0) {
      return null;
    }

    const dedupedPoints: PricePoint[] = [];
    points.forEach((point) => {
      const last = dedupedPoints[dedupedPoints.length - 1];
      if (last && last.ts === point.ts) {
        dedupedPoints[dedupedPoints.length - 1] = point;
      } else {
        dedupedPoints.push(point);
      }
    });

    const buySeries = dedupedPoints.map((point) => point.buy);
    const sellSeries = dedupedPoints.map((point) => point.sell);
    const allValues = [...buySeries, ...sellSeries].filter((value) => value > 0);
    if (allValues.length === 0) {
      return null;
    }

    const minRaw = Math.min(...allValues);
    const maxRaw = Math.max(...allValues);
    const range = maxRaw - minRaw || 1;
    const paddedMin = Math.max(0, minRaw - range * 0.15);
    const paddedMax = maxRaw + range * 0.15;
    const yMin = Math.floor(paddedMin / 50) * 50;
    let yMax = Math.ceil(paddedMax / 50) * 50;
    if (yMax <= yMin) {
      yMax = yMin + 50;
    }

    const height = 240;
    const padLeft = 52;
    const padRight = 16;
    const padTop = 14;
    const padBottom = 34;
    const pointSpacing = 8;
    const plotWidth = Math.max(620, dedupedPoints.length * pointSpacing);
    const width = padLeft + padRight + plotWidth;
    const plotHeight = height - padTop - padBottom;
    const scaleX = (index: number) => padLeft + (dedupedPoints.length <= 1 ? plotWidth / 2 : (index / (dedupedPoints.length - 1)) * plotWidth);
    const scaleY = (value: number) => padTop + (1 - (value - yMin) / (yMax - yMin)) * plotHeight;

    const buyPoints = buySeries.map((value, index) => ({ x: scaleX(index), y: scaleY(value) }));
    const sellPoints = sellSeries.map((value, index) => ({ x: scaleX(index), y: scaleY(value) }));
    const yTicks = Array.from({ length: 5 }, (_, i) => {
      const value = yMax - ((yMax - yMin) * i) / 4;
      const y = padTop + (plotHeight * i) / 4;
      return { y, label: formatCompactNumber(value) };
    });
    const tickCount = Math.min(6, dedupedPoints.length);
    const xIndexes = Array.from({ length: tickCount }, (_, i) =>
      Math.round((i * (dedupedPoints.length - 1)) / Math.max(1, tickCount - 1)),
    ).filter((value, index, arr) => arr.indexOf(value) === index);
    const xTicks = xIndexes.map((index) => ({ x: scaleX(index), label: formatClockLabel(dedupedPoints[index]?.labelDate) }));

    return {
      width,
      height,
      padLeft,
      padRight,
      buyPath: buildLinePath(buyPoints),
      sellPath: buildLinePath(sellPoints),
      yTicks,
      xTicks,
    };
  }, [orders]);

  if (loading) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>Loading completed orders...</p>
      </div>
    );
  }

  if (!chart) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>No completed orders in the last 48 hours.</p>
      </div>
    );
  }

  return (
    <div className={styles.marketChartCard}>
      <div className={styles.marketChartHeader}>
        <h3>Completed Orders (48H)</h3>
        <div className={styles.marketLegend}>
          <span className={styles.marketLegendItem}>
            <span className={`${styles.marketLegendDot} ${styles.marketLegendSell}`} />
            Sell
          </span>
          <span className={styles.marketLegendItem}>
            <span className={`${styles.marketLegendDot} ${styles.marketLegendBuy}`} />
            Buy
          </span>
        </div>
      </div>
      <p className={styles.marketChartSubtitle}>Every completed order in the past 48 hours.</p>
      <div className={styles.marketChartScroll}>
        <svg
          width={chart.width}
          height={chart.height}
          viewBox={`0 0 ${chart.width} ${chart.height}`}
          className={styles.marketSpreadSvg}
          role="img"
          aria-label="Completed orders chart"
        >
          {chart.yTicks.map((tick) => (
            <g key={tick.y}>
              <line x1={chart.padLeft} y1={tick.y} x2={chart.width - chart.padRight} y2={tick.y} className={styles.marketGridLine} />
              <text x={chart.padLeft - 8} y={tick.y + 3} textAnchor="end" className={styles.marketAxisLabel}>
                {tick.label}
              </text>
            </g>
          ))}
          <path d={chart.sellPath} className={styles.marketSellLine} />
          <path d={chart.buyPath} className={styles.marketBuyLine} />
          {chart.xTicks.map((tick, index) => (
            <text key={`${tick.x}-${tick.label}-${index}`} x={tick.x} y={chart.height - 8} textAnchor="middle" className={styles.marketAxisLabel}>
              {tick.label}
            </text>
          ))}
        </svg>
      </div>
    </div>
  );
}

function MarketVolumeChart({
  orders,
  loading,
}: {
  orders: CompletedOrder[];
  loading: boolean;
}) {
  const [selectedRange, setSelectedRange] = useState<VolumeRangeKey>("48h");

  const stats = useMemo(() => {
    const minutes = VOLUME_RANGE_OPTIONS.find((option) => option.key === selectedRange)?.minutes ?? 48 * 60;
    const normalizedOrders = orders
      .map((order) => ({
        ...order,
        parsedTime: parseMarketDate(order.date)?.getTime() ?? 0,
      }))
      .filter((order) => order.parsedTime > 0);
    const latestTs = normalizedOrders.reduce((maxTs, order) => Math.max(maxTs, order.parsedTime), 0);
    const cutoff = latestTs - minutes * 60 * 1000;

    let buyCount = 0;
    let sellCount = 0;
    let buyStubs = 0;
    let sellStubs = 0;

    normalizedOrders.forEach((order) => {
      if (order.parsedTime < cutoff) {
        return;
      }
      if (order.is_buy === true) {
        buyCount += 1;
        buyStubs += order.price;
      } else if (order.is_buy === false) {
        sellCount += 1;
        sellStubs += order.price;
      }
    });

    return {
      minutes,
      buyCount,
      sellCount,
      buyStubs,
      sellStubs,
      totalCount: buyCount + sellCount,
    };
  }, [orders, selectedRange]);

  if (loading) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>Loading volume...</p>
      </div>
    );
  }

  if (!orders || orders.length === 0) {
    return (
      <div className={styles.marketChartCard}>
        <p className={styles.marketChartLoading}>No completed orders available.</p>
      </div>
    );
  }

  const buy = stats.buyCount;
  const sell = stats.sellCount;
  const max = Math.max(buy, sell, 1);
  const sellHeight = Math.max(0, (sell / max) * 100);
  const buyHeight = Math.max(0, (buy / max) * 100);

  return (
    <div className={styles.marketChartCard}>
      <div className={styles.marketChartHeader}>
        <h3>Buy/Sell Volume</h3>
      </div>
      <p className={styles.marketChartSubtitle}>Filter from 48 hours down to 10 minutes.</p>
      <div className={styles.volumeRangeFilters}>
        {VOLUME_RANGE_OPTIONS.map((option) => (
          <button
            key={option.key}
            type="button"
            onClick={() => setSelectedRange(option.key)}
            className={selectedRange === option.key ? styles.volumeRangeButtonActive : styles.volumeRangeButton}
          >
            {option.label}
          </button>
        ))}
      </div>
      <p className={styles.marketVolumeSummary}>
        {stats.totalCount} completed order{stats.totalCount === 1 ? "" : "s"} in last {Math.round(stats.minutes / 60) >= 1 ? optionLabelFromMinutes(stats.minutes) : `${stats.minutes}m`}
      </p>
      <div className={styles.marketVolumeBars}>
        <div className={styles.marketVolumeBarWrap}>
          <span className={`${styles.marketVolumeValue} ${styles.marketLegendSell}`}>{formatCompactNumber(sell)}</span>
          <div className={styles.marketVolumeTrack}>
            <div className={`${styles.marketVolumeFill} ${styles.marketVolumeSell}`} style={{ height: `${sellHeight}%` }} />
          </div>
          <span className={styles.marketVolumeLabel}>Sell</span>
          <span className={styles.marketVolumeMeta}>{formatCompactNumber(stats.sellStubs)} stubs</span>
        </div>
        <div className={styles.marketVolumeBarWrap}>
          <span className={`${styles.marketVolumeValue} ${styles.marketLegendBuy}`}>{formatCompactNumber(buy)}</span>
          <div className={styles.marketVolumeTrack}>
            <div className={`${styles.marketVolumeFill} ${styles.marketVolumeBuy}`} style={{ height: `${buyHeight}%` }} />
          </div>
          <span className={styles.marketVolumeLabel}>Buy</span>
          <span className={styles.marketVolumeMeta}>{formatCompactNumber(stats.buyStubs)} stubs</span>
        </div>
      </div>
    </div>
  );
}

function optionLabelFromMinutes(minutes: number): string {
  if (minutes >= 60) {
    const hours = minutes / 60;
    return `${Number.isInteger(hours) ? hours : hours.toFixed(1)}h`;
  }
  return `${minutes}m`;
}

export default function CardDetailPage() {
  const params = useParams<{ id: string }>();
  const router = useRouter();
  const cardId = decodeURIComponent(params.id ?? "");

  const [card, setCard] = useState<CardDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [isPro, setIsPro] = useState<boolean | null>(null);
  const [activeTab, setActiveTab] = useState<DetailTab>("attributes");
  const [userPredictionInput, setUserPredictionInput] = useState("");
  const [predictionSaving, setPredictionSaving] = useState(false);
  const [predictionError, setPredictionError] = useState<string | null>(null);
  const [pitches, setPitches] = useState<PitchData[]>([]);
  const [loadingPitches, setLoadingPitches] = useState(false);
  const [comments, setComments] = useState<CardComment[]>([]);
  const [loadingComments, setLoadingComments] = useState(false);
  const [commentsExpanded, setCommentsExpanded] = useState(false);
  const [commentsLoaded, setCommentsLoaded] = useState(false);
  const [commentInput, setCommentInput] = useState("");
  const [commentSubmitting, setCommentSubmitting] = useState(false);
  const [commentError, setCommentError] = useState<string | null>(null);
  const [isPredictionEditing, setIsPredictionEditing] = useState(false);
  const [selectedPosition, setSelectedPosition] = useState("");
  const [loadingMarket, setLoadingMarket] = useState(false);
  const [marketBuyPrice, setMarketBuyPrice] = useState<number | null>(null);
  const [marketSellPrice, setMarketSellPrice] = useState<number | null>(null);
  const [marketHistory, setMarketHistory] = useState<CompletedOrder[]>([]);
  const [priceHistoryRows, setPriceHistoryRows] = useState<PriceHistoryRow[]>([]);
  const [seasonStats, setSeasonStats] = useState<SeasonStats | null>(null);
  const [loadingStats, setLoadingStats] = useState(false);
  const [activeStatsWindow, setActiveStatsWindow] = useState<StatsWindow>("season");

  const communityPrediction = card?.community_predicted_ovr ?? card?.predicted_ovr ?? null;
  const isTwoWay = card ? TWO_WAY_PLAYERS.includes(card.name) : false;
  const showBatting = Boolean(card && (card.is_hitter || isTwoWay));
  const showPitching = Boolean(card && (!card.is_hitter || isTwoWay));
  const positionSummary = card ? formatPositionSummary(card.display_position, card.display_secondary_positions) : "";
  const showYourOverall = isPro === true;
  const canAccessLastUpdateWindow = isPro === true;

  const positionOptions = useMemo(() => {
    if (!card) {
      return [];
    }

    const primary = normalizePositionValue(card.display_primary_position || card.display_position);
    const optionSet = new Set<string>();
    if (primary) {
      optionSet.add(primary);
    }
    parseSecondaryPositions(card.display_secondary_positions).forEach((position) => optionSet.add(position));
    Object.keys(card.true_overall_by_position || {}).forEach((position) => optionSet.add(normalizePositionValue(position)));
    Object.keys(card.meta_overall_by_position || {}).forEach((position) => optionSet.add(normalizePositionValue(position)));
    Object.keys(card.your_overall_by_position || {}).forEach((position) => optionSet.add(normalizePositionValue(position)));

    const rest = Array.from(optionSet).filter((position) => position !== primary).sort((a, b) => a.localeCompare(b));
    return primary ? [primary, ...rest] : rest;
  }, [card]);

  useEffect(() => {
    if (!positionOptions.length) {
      setSelectedPosition("");
      return;
    }
    setSelectedPosition((current) => (current && positionOptions.includes(current) ? current : positionOptions[0]));
  }, [positionOptions]);

  const selectedTrueOverall = useMemo(
    () => resolveOverallForPosition(card?.true_overall_by_position, selectedPosition, card?.true_overall_rounded ?? card?.ovr ?? null),
    [card?.true_overall_by_position, card?.true_overall_rounded, card?.ovr, selectedPosition],
  );
  const selectedMetaOverall = useMemo(
    () => resolveOverallForPosition(card?.meta_overall_by_position, selectedPosition, card?.meta_overall_rounded ?? card?.predicted_ovr ?? null),
    [card?.meta_overall_by_position, card?.meta_overall_rounded, card?.predicted_ovr, selectedPosition],
  );
  const selectedYourOverall = useMemo(
    () => resolveOverallForPosition(card?.your_overall_by_position, selectedPosition, card?.your_overall_rounded ?? null),
    [card?.your_overall_by_position, card?.your_overall_rounded, selectedPosition],
  );
  const ourPredictionChange = useMemo(() => formatPredictionChange(card?.predicted_ovr, card?.ovr), [card?.predicted_ovr, card?.ovr]);
  const communityPredictionChange = useMemo(() => formatPredictionChange(communityPrediction, card?.ovr), [communityPrediction, card?.ovr]);
  const yourPredictionChange = useMemo(() => formatPredictionChange(card?.user_prediction, card?.ovr), [card?.user_prediction, card?.ovr]);

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;

    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        const signedIn = Boolean(user);
        setIsAuthenticated(signedIn);
        if (!signedIn) {
          setIsPro(false);
          return;
        }

        void getMyEntitlements()
          .then((payload) => setIsPro(Boolean(payload.has_pro)))
          .catch(() => setIsPro(false));
      });
    } catch {
      setIsAuthenticated(false);
      setIsPro(false);
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, []);

  useEffect(() => {
    if (!cardId) {
      setError("Invalid card id.");
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    setError(null);
    setPredictionError(null);

    const load = async () => {
      try {
        const path = `/cards/${encodeURIComponent(cardId)}`;
        const payload = isAuthenticated ? await apiGetAuth<CardDetail>(path) : await apiGet<CardDetail>(path);
        if (cancelled) {
          return;
        }

        setCard(payload);

        if (isAuthenticated) {
          try {
            const userPrediction = await apiGetAuth<UserPredictionResponse>(`/user-predictions/${encodeURIComponent(cardId)}`);
            if (cancelled) {
              return;
            }
            setUserPredictionInput(String(userPrediction.predicted_ovr));
            setCard((current) => (current ? { ...current, user_prediction: userPrediction.predicted_ovr } : current));
          } catch {
            if (cancelled) {
              return;
            }
            setUserPredictionInput("");
            setCard((current) => (current ? { ...current, user_prediction: null } : current));
          }
        } else {
          setUserPredictionInput("");
          setCard((current) => (current ? { ...current, user_prediction: null } : current));
        }
      } catch (err: unknown) {
        if (cancelled) {
          return;
        }
        if (err instanceof ApiError && err.status === 404) {
          setError("Card not found.");
        } else if (err instanceof Error && err.message) {
          setError(err.message);
        } else {
          setError("Failed to load card details.");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, [cardId, isAuthenticated]);

  useEffect(() => {
    if (!card?.id || !showPitching) {
      setPitches([]);
      return;
    }

    let cancelled = false;
    setLoadingPitches(true);

    void apiGet<PitchData[]>(`/pitches/${encodeURIComponent(card.id)}`)
      .then((payload) => {
        if (!cancelled) {
          setPitches(payload);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setPitches([]);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingPitches(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [card?.id, showPitching]);

  useEffect(() => {
    if (!card?.id) {
      setMarketBuyPrice(null);
      setMarketSellPrice(null);
      setMarketHistory([]);
      setPriceHistoryRows([]);
      return;
    }

    let cancelled = false;
    setLoadingMarket(true);

    void Promise.all([
      apiGet<CompletedOrder[]>(`/completed_orders/latest?card_id=${encodeURIComponent(card.id)}&is_buy=true&limit=1`),
      apiGet<CompletedOrder[]>(`/completed_orders/latest?card_id=${encodeURIComponent(card.id)}&is_buy=false&limit=1`),
      apiGet<CompletedOrder[]>(`/completed_orders/${encodeURIComponent(card.id)}/history?limit=1000`),
      apiGet<PriceHistoryRow[]>(`/price_history/${encodeURIComponent(card.id)}/history?limit=730`),
    ])
      .then(([buyRes, sellRes, historyRes, priceHistoryRes]) => {
        if (cancelled) {
          return;
        }
        setMarketBuyPrice(buyRes?.[0]?.price ?? null);
        setMarketSellPrice(sellRes?.[0]?.price ?? null);
        setMarketHistory(Array.isArray(historyRes) ? historyRes : []);
        setPriceHistoryRows(Array.isArray(priceHistoryRes) ? priceHistoryRes : []);
      })
      .catch(() => {
        if (cancelled) {
          return;
        }
        setMarketBuyPrice(null);
        setMarketSellPrice(null);
        setMarketHistory([]);
        setPriceHistoryRows([]);
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingMarket(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [card?.id]);

  useEffect(() => {
    if (!card?.id) {
      setSeasonStats(null);
      return;
    }

    let cancelled = false;
    setLoadingStats(true);

    const effectiveWindow = activeStatsWindow === "last_update" && !canAccessLastUpdateWindow ? "season" : activeStatsWindow;
    const windowParam = effectiveWindow !== "season" ? `&window=${effectiveWindow}` : "";

    void apiGet<SeasonStats>(`/mlb_stats/season/${encodeURIComponent(card.id)}?season=2025${windowParam}`)
      .then((payload) => {
        if (!cancelled) {
          setSeasonStats(payload);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSeasonStats(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingStats(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [card?.id, activeStatsWindow, canAccessLastUpdateWindow]);

  useEffect(() => {
    if (activeStatsWindow === "last_update" && !canAccessLastUpdateWindow) {
      setActiveStatsWindow("season");
    }
  }, [activeStatsWindow, canAccessLastUpdateWindow]);

  useEffect(() => {
    setComments([]);
    setCommentsLoaded(false);
    setCommentsExpanded(false);
    setLoadingComments(false);
    setCommentInput("");
    setCommentError(null);
    setIsPredictionEditing(false);
    setActiveStatsWindow("season");
    setSeasonStats(null);
    setLoadingStats(false);
  }, [cardId]);

  const fetchComments = useCallback(async () => {
    if (!card?.id) {
      return;
    }

    setLoadingComments(true);
    setCommentError(null);
    try {
      const path = `/comments/card/${encodeURIComponent(card.id)}`;
      const payload = isAuthenticated ? await apiGetAuth<CardComment[]>(path) : await apiGet<CardComment[]>(path);
      setComments(payload);
      setCommentsLoaded(true);
      setCard((current) => (current ? { ...current, comment_count: payload.length } : current));
    } catch (err: unknown) {
      if (err instanceof Error && err.message) {
        setCommentError(err.message);
      } else {
        setCommentError("Failed to load comments.");
      }
    } finally {
      setLoadingComments(false);
    }
  }, [card?.id, isAuthenticated]);

  const toggleComments = () => {
    setCommentsExpanded((current) => {
      const next = !current;
      if (next && !commentsLoaded && !loadingComments) {
        void fetchComments();
      }
      return next;
    });
  };

  const handlePostComment = async () => {
    if (!card?.id) {
      return;
    }
    const content = commentInput.trim();
    if (!content) {
      return;
    }
    if (!isAuthenticated) {
      router.push("/signin");
      return;
    }

    setCommentSubmitting(true);
    setCommentError(null);
    try {
      await apiPostAuth(`/comments/card/${encodeURIComponent(card.id)}`, { content });
      setCommentInput("");
      await fetchComments();
    } catch (err: unknown) {
      if (err instanceof Error && err.message) {
        setCommentError(err.message);
      } else {
        setCommentError("Failed to post comment.");
      }
    } finally {
      setCommentSubmitting(false);
    }
  };

  const handleSavePrediction = async () => {
    if (!card) {
      return;
    }
    if (!isAuthenticated) {
      router.push("/signin");
      return;
    }

    const parsed = Number.parseInt(userPredictionInput.trim(), 10);
    if (!Number.isFinite(parsed) || parsed < 0 || parsed > 99) {
      setPredictionError("Enter a valid overall from 0 to 99.");
      return;
    }

    setPredictionSaving(true);
    setPredictionError(null);
    try {
      await apiPostAuth<UserPredictionResponse>("/user-predictions/", {
        card_id: card.id,
        predicted_ovr: parsed,
      });
      setUserPredictionInput(String(parsed));
      setIsPredictionEditing(false);
      setCard((current) => {
        if (!current) {
          return current;
        }
        const hadExistingPrediction = current.user_prediction != null;
        return {
          ...current,
          user_prediction: parsed,
          user_prediction_count: hadExistingPrediction ? current.user_prediction_count : current.user_prediction_count + 1,
        };
      });
    } catch (err: unknown) {
      if (err instanceof Error && err.message) {
        setPredictionError(err.message);
      } else {
        setPredictionError("Failed to save prediction.");
      }
    } finally {
      setPredictionSaving(false);
    }
  };

  const handleStartPredictionEdit = () => {
    if (!isAuthenticated) {
      router.push("/signin");
      return;
    }
    setPredictionError(null);
    setUserPredictionInput(card?.user_prediction != null ? String(card.user_prediction) : "");
    setIsPredictionEditing(true);
  };

  const handleCancelPredictionEdit = () => {
    setPredictionError(null);
    setUserPredictionInput(card?.user_prediction != null ? String(card.user_prediction) : "");
    setIsPredictionEditing(false);
  };

  const showProStatusPending = isAuthenticated && isPro == null;
  const showProLock = !showProStatusPending && isPro !== true;

  const hasPitchingPredictions = useMemo(() => {
    if (!card?.predicted_attributes) {
      return false;
    }
    return PITCHING_PREDICTION_KEYS.some(({ key }) => card.predicted_attributes?.[`pit_pred_${key}_new`] != null);
  }, [card?.predicted_attributes]);

  const hasBattingPredictions = useMemo(() => {
    if (!card?.predicted_attributes) {
      return false;
    }
    return BATTING_PREDICTION_KEYS.some(({ key }) => card.predicted_attributes?.[`hit_pred_${key}_new`] != null);
  }, [card?.predicted_attributes]);

  const handleGoPro = () => {
    if (isAuthenticated) {
      router.push("/account");
      return;
    }
    router.push("/signin");
  };

  const renderPitchingBlock = () => (
    <div className={styles.attributeBlock} key="pitching">
      <h3>Pitching</h3>
      <StatBar label="Stamina" value={card?.stamina ?? 0} color="#fbbf24" />
      <StatBar label="Pitching Clutch" value={card?.pitching_clutch ?? 0} color="#fbbf24" />
      <StatBar label="H/9" value={card?.hits_per_bf ?? 0} color="#fbbf24" />
      <StatBar label="K/9" value={card?.k_per_bf ?? 0} color="#fbbf24" />
      <StatBar label="BB/9" value={card?.bb_per_bf ?? 0} color="#fbbf24" />
      <StatBar label="HR/9" value={card?.hr_per_bf ?? 0} color="#fbbf24" />
    </div>
  );

  const renderBattingBlock = () => (
    <div className={styles.attributeBlock} key="batting">
      <h3>Batting</h3>
      <StatBar label="Contact R" value={card?.contact_right ?? 0} color="#3b82f6" />
      <StatBar label="Contact L" value={card?.contact_left ?? 0} color="#3b82f6" />
      <StatBar label="Power R" value={card?.power_right ?? 0} color="#3b82f6" />
      <StatBar label="Power L" value={card?.power_left ?? 0} color="#3b82f6" />
      <StatBar label="Vision" value={card?.plate_vision ?? 0} color="#3b82f6" />
      <StatBar label="Clutch" value={card?.batting_clutch ?? 0} color="#3b82f6" />
    </div>
  );

  const renderFieldingBlock = () => (
    <div className={styles.attributeBlock} key="fielding">
      <h3>Fielding</h3>
      <StatBar label="Fielding" value={card?.fielding_ability ?? 0} color="#22c55e" />
      <StatBar label="Arm Strength" value={card?.arm_strength ?? 0} color="#22c55e" />
      <StatBar label="Arm Accuracy" value={card?.arm_accuracy ?? 0} color="#22c55e" />
      <StatBar label="Reaction" value={card?.reaction_time ?? 0} color="#22c55e" />
      <StatBar label="Blocking" value={card?.blocking ?? 0} color="#22c55e" />
    </div>
  );

  const renderRunningBlock = () => (
    <div className={styles.attributeBlock} key="running">
      <h3>Running</h3>
      <StatBar label="Speed" value={card?.speed ?? 0} color="#a78bfa" />
      <StatBar label="Baserunning Ability" value={card?.baserunning_ability ?? 0} color="#a78bfa" />
      <StatBar label="Baserunning Aggression" value={card?.baserunning_aggression ?? 0} color="#a78bfa" />
    </div>
  );

  const renderPitchesBlock = () => (
    <div className={styles.attributeBlock} key="pitches">
      <h3>Pitches</h3>
      {loadingPitches ? <p className={styles.emptyText}>Loading pitches...</p> : null}
      {!loadingPitches && pitches.length === 0 ? <p className={styles.emptyText}>No pitch data available.</p> : null}
      {!loadingPitches && pitches.length > 0 ? (
        <div className={styles.pitchList}>
          {pitches.map((pitch) => (
            <div key={pitch.name} className={styles.pitchCard}>
              <div className={styles.pitchName}>{pitch.name}</div>
              <div className={styles.pitchGaugeRow}>
                <PitchGauge label="Velocity" value={pitch.speed} color="#ef4444" />
                <PitchGauge label="Control" value={pitch.control} color="#60a5fa" />
                <PitchGauge label="Movement" value={pitch.movement} color="#a855f7" />
              </div>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );

  const renderQuirksCompactBlock = () => (
    <div className={styles.attributeBlock} key="quirks">
      <h3>Quirks</h3>
      {card?.quirks && card.quirks.length > 0 ? (
        <div className={styles.quirkCompactList}>
          {card.quirks.map((quirk) => (
            <div key={quirk.name} className={styles.quirkCompactItem}>
              {quirk.img ? <Image src={quirk.img} alt={quirk.name} width={24} height={24} className={styles.quirkCompactImage} unoptimized /> : null}
              <span className={styles.quirkCompactName}>{quirk.name}</span>
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );

  const leftColumnBlocks: ReactNode[] = [];
  const middleColumnBlocks: ReactNode[] = [];
  const rightColumnBlocks: ReactNode[] = [renderQuirksCompactBlock()];
  const fullWidthBlocks: ReactNode[] = [];

  if (showBatting && showPitching) {
    leftColumnBlocks.push(renderBattingBlock(), renderPitchingBlock());
    middleColumnBlocks.push(renderFieldingBlock(), renderRunningBlock());
    fullWidthBlocks.push(renderPitchesBlock());
  } else if (showPitching) {
    leftColumnBlocks.push(renderPitchingBlock());
    middleColumnBlocks.push(renderFieldingBlock());
    fullWidthBlocks.push(renderPitchesBlock());
  } else {
    leftColumnBlocks.push(renderBattingBlock());
    middleColumnBlocks.push(renderFieldingBlock(), renderRunningBlock());
  }

  const commentCountLabel = commentsLoaded ? comments.length : (card?.comment_count ?? 0);
  const statsWindowLabel = getStatsWindowLabel(activeStatsWindow);

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <section className={styles.content}>
        <div className={styles.backRow}>
          <Link href="/predictions" className={styles.backLink}>
            <ArrowLeft size={16} />
            Back To Predictions
          </Link>
        </div>

        {loading ? (
          <div className={styles.loadingState}>Loading card details...</div>
        ) : error || !card ? (
          <div className={styles.errorState}>
            <p>{error || "Card not found."}</p>
          </div>
        ) : (
          <>
            <div className={styles.heroCard}>
              <div className={styles.heroTop}>
                <div className={styles.cardImageWrap}>
                  {card.baked_img ? <Image src={card.baked_img} alt={card.name} width={120} height={168} className={styles.cardImage} unoptimized /> : null}
                </div>
                <div className={styles.bioColumn}>
                  <h1>{card.name}</h1>
                  <p>
                    {card.team_short_name} • {positionSummary} • Age {card.age}
                  </p>
                  <p>
                    Throws {card.throw_hand} • Bats {card.bat_hand}
                  </p>
                  <div className={styles.positionSelectorRow}>
                    <label htmlFor="card-position-select">Position</label>
                    <div className={styles.positionSelectWrap}>
                      <select id="card-position-select" value={selectedPosition} onChange={(event) => setSelectedPosition(event.target.value)}>
                        {positionOptions.map((position) => (
                          <option key={position} value={position}>
                            {position}
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>
                  <div className={styles.badgeRow}>
                    <div className={styles.ratingBadge}>
                      <span>OVR</span>
                      <strong>{card.ovr}</strong>
                    </div>
                    <div className={styles.ratingBadge}>
                      <span>TRUE</span>
                      <strong>{selectedTrueOverall ?? "--"}</strong>
                    </div>
                    <div className={styles.ratingBadge}>
                      <span>META</span>
                      <strong>{selectedMetaOverall ?? "--"}</strong>
                    </div>
                    <div className={styles.ratingBadge}>
                      <span>YOUR</span>
                      {showYourOverall ? (
                        <strong>{selectedYourOverall ?? "--"}</strong>
                      ) : (
                        <strong className={styles.proLockedBadgeValue}>
                          <Lock size={12} />
                          PRO
                        </strong>
                      )}
                    </div>
                  </div>
                </div>

                <div className={styles.headerSideGrid}>
                  <div className={styles.headerPredictionColumn}>
                    <p className={styles.sideColumnHeading}>Roster Update Predictions</p>
                    <div className={styles.headerPredictionItem}>
                      <span className={styles.headerPredictionLabel}>Our Prediction</span>
                      <strong
                        className={`${styles.headerPredictionValue} ${
                          ourPredictionChange.trend === "up"
                            ? styles.predictionUp
                            : ourPredictionChange.trend === "down"
                              ? styles.predictionDown
                              : styles.predictionFlat
                        }`}
                      >
                        {ourPredictionChange.text}
                      </strong>
                    </div>
                    <div className={styles.headerPredictionItem}>
                      <span className={styles.headerPredictionLabel}>Community Prediction</span>
                      <strong
                        className={`${styles.headerPredictionValue} ${
                          communityPredictionChange.trend === "up"
                            ? styles.predictionUp
                            : communityPredictionChange.trend === "down"
                              ? styles.predictionDown
                              : styles.predictionFlat
                        }`}
                      >
                        {communityPredictionChange.text}
                      </strong>
                    </div>
                    <div className={`${styles.headerPredictionItem} ${isPredictionEditing ? styles.headerPredictionItemEditing : ""}`}>
                      <span className={styles.headerPredictionLabel}>Your Prediction</span>
                      {isPredictionEditing ? (
                        <div className={styles.inlinePredictionEditor}>
                          <input
                            type="number"
                            min={0}
                            max={99}
                            placeholder="0-99"
                            value={userPredictionInput}
                            onChange={(event) => setUserPredictionInput(event.target.value)}
                          />
                          <div className={styles.inlinePredictionActions}>
                            <button type="button" onClick={handleSavePrediction} disabled={predictionSaving}>
                              {predictionSaving ? "Saving..." : "Save"}
                            </button>
                            <button type="button" onClick={handleCancelPredictionEdit} disabled={predictionSaving}>
                              Cancel
                            </button>
                          </div>
                          {predictionError ? <p className={styles.inlinePredictionError}>{predictionError}</p> : null}
                        </div>
                      ) : (
                        <button
                          type="button"
                          onClick={handleStartPredictionEdit}
                          aria-label="Edit your prediction"
                          className={`${styles.headerPredictionValueButton} ${
                            yourPredictionChange.trend === "up"
                              ? styles.predictionUp
                              : yourPredictionChange.trend === "down"
                                ? styles.predictionDown
                                : styles.predictionFlat
                          }`}
                        >
                          <span>{yourPredictionChange.text}</span>
                          <ChevronDown size={12} className={styles.inlineEditCaret} />
                        </button>
                      )}
                    </div>
                  </div>

                  <div className={styles.headerMarketColumn}>
                    <p className={styles.sideColumnHeading}>Market Data</p>
                    <div className={styles.headerMarketRow}>
                      <div className={styles.headerMarketItem}>
                        <span className={styles.headerMarketLabel}>Sell Now</span>
                        <span className={styles.headerMarketValue}>
                          <Image src="/images/stub.png" alt="Stubs" width={13} height={13} />
                          {formatStubs(card.best_sell_price)}
                        </span>
                      </div>
                      <div className={styles.headerMarketItem}>
                        <span className={styles.headerMarketLabel}>Buy Now</span>
                        <span className={styles.headerMarketValue}>
                          <Image src="/images/stub.png" alt="Stubs" width={13} height={13} />
                          {formatStubs(card.best_buy_price)}
                        </span>
                      </div>
                      <div className={styles.headerMarketItem}>
                        <span className={styles.headerMarketLabel}>Quicksell</span>
                        <span className={styles.headerMarketValue}>
                          <Image src="/images/stub.png" alt="Stubs" width={13} height={13} />
                          {formatStubs(card.quicksell_value)}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div className={styles.interactionRow}>
              <div className={styles.commentsCard}>
                <button type="button" className={styles.commentsHeader} onClick={toggleComments}>
                  <span className={styles.commentsHeaderTitle}>
                    <MessageSquare size={14} />
                    Comments ({commentCountLabel})
                  </span>
                  {commentsExpanded ? <ChevronUp size={15} /> : <ChevronDown size={15} />}
                </button>

                {commentsExpanded ? (
                  <div className={styles.commentsBody}>
                    {loadingComments ? <p className={styles.emptyText}>Loading comments...</p> : null}
                    {!loadingComments && comments.length === 0 ? <p className={styles.emptyText}>No comments yet.</p> : null}
                    {!loadingComments && comments.length > 0 ? (
                      <div className={styles.commentsList}>
                        {comments.map((comment) => (
                          <div key={comment.id} className={styles.commentRow}>
                            <div className={styles.commentMeta}>
                              <strong>{comment.user_display_name || "User"}</strong>
                              <span>{formatCommentTimestamp(comment.created_at)}</span>
                            </div>
                            <p className={styles.commentContent}>{comment.content}</p>
                          </div>
                        ))}
                      </div>
                    ) : null}

                    <div className={styles.commentComposer}>
                      <textarea
                        placeholder={isAuthenticated ? "Add a comment..." : "Sign in to add a comment"}
                        value={commentInput}
                        onChange={(event) => setCommentInput(event.target.value)}
                        disabled={commentSubmitting}
                      />
                      <button type="button" onClick={handlePostComment} disabled={commentSubmitting || commentInput.trim().length === 0}>
                        {commentSubmitting ? "Posting..." : "Post Comment"}
                      </button>
                    </div>
                  {commentError ? <p className={styles.predictionError}>{commentError}</p> : null}
                </div>
              ) : null}
            </div>
            </div>

            <div className={styles.tabRow}>
              <button type="button" className={activeTab === "attributes" ? styles.tabActive : styles.tab} onClick={() => setActiveTab("attributes")}>
                Attributes
              </button>
              <button type="button" className={activeTab === "market" ? styles.tabActive : styles.tab} onClick={() => setActiveTab("market")}>
                Market
              </button>
              <button type="button" className={activeTab === "stats" ? styles.tabActive : styles.tab} onClick={() => setActiveTab("stats")}>
                MLB Stats
              </button>
              <button type="button" className={activeTab === "pro" ? styles.proTabActive : styles.proTab} onClick={() => setActiveTab("pro")}>
                <Crown size={12} />
                Pro
              </button>
            </div>

            {activeTab === "attributes" ? (
              <div className={styles.sectionCard}>
                <div className={styles.attributesGrid}>
                  <div className={styles.attributesColumn}>{leftColumnBlocks}</div>
                  <div className={styles.attributesColumn}>{middleColumnBlocks}</div>
                  <div className={styles.attributesColumn}>{rightColumnBlocks}</div>
                </div>
                {fullWidthBlocks.length > 0 ? <div className={styles.attributesFullWidth}>{fullWidthBlocks}</div> : null}
              </div>
            ) : null}

            {activeTab === "market" ? (
              <div className={styles.sectionCard}>
                <div className={styles.marketGrid}>
                  <div className={styles.marketColumn}>
                    <p className={styles.marketLabel}>Buy Order</p>
                    {loadingMarket ? (
                      <p className={`${styles.marketValue} ${styles.marketValueText}`}>Loading...</p>
                    ) : marketBuyPrice != null ? (
                      <div className={styles.marketValueRow}>
                        <Image src="/images/stub.png" alt="Stubs" width={16} height={16} className={styles.marketIcon} />
                        <p className={styles.marketValue}>{formatStubs(marketBuyPrice)}</p>
                      </div>
                    ) : (
                      <p className={`${styles.marketValue} ${styles.marketValueText}`}>N/A</p>
                    )}
                  </div>
                  <div className={styles.marketColumn}>
                    <p className={styles.marketLabel}>Quicksell</p>
                    <div className={styles.marketValueRow}>
                      <Image src="/images/stub.png" alt="Stubs" width={16} height={16} className={styles.marketIcon} />
                      <p className={styles.marketValue}>{formatStubs(card.quicksell_value)}</p>
                    </div>
                  </div>
                  <div className={styles.marketColumn}>
                    <p className={styles.marketLabel}>Sell Order</p>
                    {loadingMarket ? (
                      <p className={`${styles.marketValue} ${styles.marketValueText}`}>Loading...</p>
                    ) : marketSellPrice != null ? (
                      <div className={styles.marketValueRow}>
                        <Image src="/images/stub.png" alt="Stubs" width={16} height={16} className={styles.marketIcon} />
                        <p className={styles.marketValue}>{formatStubs(marketSellPrice)}</p>
                      </div>
                    ) : (
                      <p className={`${styles.marketValue} ${styles.marketValueText}`}>N/A</p>
                    )}
                  </div>
                </div>
                <PriceHistoryChart history={priceHistoryRows} loading={loadingMarket} />
                <div className={styles.marketBottomRow}>
                  <div className={styles.marketBottomMain}>
                    <CompletedOrdersChart orders={marketHistory} loading={loadingMarket} />
                  </div>
                  <div className={styles.marketBottomSide}>
                    <MarketVolumeChart orders={marketHistory} loading={loadingMarket} />
                  </div>
                </div>
              </div>
            ) : null}

            {activeTab === "stats" ? (
              <div className={styles.sectionCard}>
                <div className={styles.statsWindowGrid}>
                  <div className={styles.statsWindowRow}>
                    {STATS_WINDOW_OPTIONS.map(({ key, label }) => (
                      <button
                        key={key}
                        type="button"
                        className={activeStatsWindow === key ? styles.statsWindowPillActive : styles.statsWindowPill}
                        onClick={() => setActiveStatsWindow(key)}
                      >
                        {label}
                      </button>
                    ))}
                  </div>
                  <button
                    type="button"
                    className={`${activeStatsWindow === "last_update" ? styles.statsWindowPillActive : styles.statsWindowPill} ${
                      !canAccessLastUpdateWindow ? styles.statsWindowPillLocked : ""
                    }`}
                    onClick={() => {
                      if (canAccessLastUpdateWindow) {
                        setActiveStatsWindow("last_update");
                      }
                    }}
                  >
                    <span className={styles.statsWindowPillContent}>
                      Since Last Roster Update
                      {!canAccessLastUpdateWindow ? <Crown size={11} /> : null}
                    </span>
                  </button>
                </div>

                {loadingStats ? (
                  <div className={styles.statsEmptyState}>Loading MLB stats...</div>
                ) : !seasonStats || (!seasonStats.batting && !seasonStats.pitching) ? (
                  <div className={styles.statsEmptyState}>N/A</div>
                ) : (
                  <div className={styles.statsStack}>
                    {seasonStats.batting ? (
                      <div className={styles.statsSection}>
                        {isTwoWay && seasonStats.pitching ? <h3 className={styles.statsSectionTitle}>Batting</h3> : null}
                        <div className={styles.statsBadgeRow}>
                          {(["avg", "obp", "slg", "ops"] as const).map((key) => (
                            <div key={key} className={styles.statsBadge}>
                              <span className={styles.statsBadgeLabel}>{key.toUpperCase()}</span>
                              <strong className={styles.statsBadgeValue}>{formatStat(seasonStats.batting?.overall[key])}</strong>
                            </div>
                          ))}
                        </div>

                        <div className={styles.statsSubCard}>
                          <h4 className={styles.statsSubCardTitle}>{statsWindowLabel}</h4>
                          <div className={styles.statsGrid}>
                            {(["pa", "ab", "h", "hr", "rbi", "r", "bb", "so", "doubles", "triples", "hbp", "tb"] as const).map((key) => (
                              <div key={key} className={styles.statsGridItem}>
                                <span className={styles.statsGridLabel}>
                                  {key === "doubles" ? "2B" : key === "triples" ? "3B" : key.toUpperCase()}
                                </span>
                                <strong className={styles.statsGridValue}>{seasonStats.batting?.overall[key] ?? 0}</strong>
                              </div>
                            ))}
                          </div>
                        </div>

                        <div className={styles.statsSubCard}>
                          <h4 className={styles.statsSubCardTitle}>Splits</h4>
                          <div className={styles.splitTableWrap}>
                            <table className={styles.splitTable}>
                              <thead>
                                <tr>
                                  <th className={styles.splitNameColumn}></th>
                                  {["AVG", "OBP", "SLG", "AB", "H", "HR", "BB", "K"].map((header) => (
                                    <th key={header}>{header}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {seasonStats.batting.splits.map((split, index) => (
                                  <tr key={`${split.split}-${index}`}>
                                    <td className={styles.splitNameCell}>{SPLIT_LABELS[split.split] || split.split}</td>
                                    <td>{formatStat(split.avg)}</td>
                                    <td>{formatStat(split.obp)}</td>
                                    <td>{formatStat(split.slg)}</td>
                                    <td>{split.ab}</td>
                                    <td>{split.h}</td>
                                    <td>{split.hr}</td>
                                    <td>{split.bb}</td>
                                    <td>{split.so}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    ) : null}

                    {seasonStats.batting && seasonStats.pitching ? <div className={styles.statsSectionDivider} /> : null}

                    {seasonStats.pitching ? (
                      <div className={styles.statsSection}>
                        {isTwoWay && seasonStats.batting ? <h3 className={styles.statsSectionTitle}>Pitching</h3> : null}
                        <div className={styles.statsBadgeRow}>
                          {(["ip", "era", "whip", "k9"] as const).map((key) => (
                            <div key={key} className={styles.statsBadge}>
                              <span className={styles.statsBadgeLabel}>{key === "k9" ? "K/9" : key.toUpperCase()}</span>
                              <strong className={styles.statsBadgeValue}>
                                {key === "era" || key === "whip" || key === "k9"
                                  ? formatStat(seasonStats.pitching?.overall[key], 2)
                                  : seasonStats.pitching?.overall[key]}
                              </strong>
                            </div>
                          ))}
                        </div>

                        <div className={styles.statsSubCard}>
                          <h4 className={styles.statsSubCardTitle}>{statsWindowLabel}</h4>
                          <div className={styles.statsGrid}>
                            {(["ip", "h", "er", "hr", "bb", "k", "batters_faced", "strike_pct"] as const).map((key) => (
                              <div key={key} className={styles.statsGridItem}>
                                <span className={styles.statsGridLabel}>
                                  {key === "batters_faced" ? "BF" : key === "strike_pct" ? "STR%" : key.toUpperCase()}
                                </span>
                                <strong className={styles.statsGridValue}>
                                  {key === "strike_pct"
                                    ? `${(((seasonStats.pitching?.overall[key] as number) ?? 0) * 100).toFixed(1)}%`
                                    : seasonStats.pitching?.overall[key] ?? 0}
                                </strong>
                              </div>
                            ))}
                          </div>
                        </div>

                        <div className={styles.statsSubCard}>
                          <h4 className={styles.statsSubCardTitle}>Splits</h4>
                          <div className={styles.splitTableWrap}>
                            <table className={styles.splitTable}>
                              <thead>
                                <tr>
                                  <th className={styles.splitNameColumn}></th>
                                  {["IP", "ERA", "WHIP", "H", "ER", "K", "BB", "HR"].map((header) => (
                                    <th key={header}>{header}</th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody>
                                {seasonStats.pitching.splits.map((split, index) => (
                                  <tr key={`${split.split}-${index}`}>
                                    <td className={styles.splitNameCell}>{SPLIT_LABELS[split.split] || split.split}</td>
                                    <td>{split.ip}</td>
                                    <td>{formatStat(split.era, 2)}</td>
                                    <td>{formatStat(split.whip, 2)}</td>
                                    <td>{split.h}</td>
                                    <td>{split.er}</td>
                                    <td>{split.k}</td>
                                    <td>{split.bb}</td>
                                    <td>{split.hr}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      </div>
                    ) : null}
                  </div>
                )}
              </div>
            ) : null}

            {activeTab === "pro" ? (
              <div className={`${styles.sectionCard} ${styles.proCard}`}>
                <div className={styles.proHeader}>
                  <div className={styles.proBadge}>
                    <Crown size={10} />
                    <span>PRO</span>
                  </div>
                  <h3 className={styles.proTitle}>Predicted Attributes</h3>
                </div>
                <p className={styles.proSectionSubheader}>
                  {showProLock || showProStatusPending
                    ? "See projected individual batting and pitching attribute changes for this card. Sign up for Pro to unlock full access."
                    : "Projected individual batting and pitching attribute changes from our latest model run."}
                </p>

                <div className={showProLock || showProStatusPending ? styles.proContentObscured : undefined}>
                  {hasPitchingPredictions ? (
                    <div className={styles.attributeBlock}>
                      <h3>Projected Pitching Attributes</h3>
                      {PITCHING_PREDICTION_KEYS.map(({ key, label }) => {
                        const newValue = card.predicted_attributes?.[`pit_pred_${key}_new`];
                        const delta = card.predicted_attributes?.[`pit_pred_${key}_delta`];
                        if (newValue == null || delta == null) {
                          return null;
                        }
                        return (
                          <div key={key} className={styles.predictedRow}>
                            <span>{label}</span>
                            <strong>{Math.round(newValue)}</strong>
                            <em className={delta >= 0 ? styles.deltaUp : styles.deltaDown}>{delta >= 0 ? `+${delta}` : delta}</em>
                          </div>
                        );
                      })}
                    </div>
                  ) : null}

                  {hasBattingPredictions ? (
                    <div className={styles.attributeBlock}>
                      <h3>Projected Batting Attributes</h3>
                      {BATTING_PREDICTION_KEYS.map(({ key, label }) => {
                        const newValue = card.predicted_attributes?.[`hit_pred_${key}_new`];
                        const delta = card.predicted_attributes?.[`hit_pred_${key}_delta`];
                        if (newValue == null || delta == null) {
                          return null;
                        }
                        return (
                          <div key={key} className={styles.predictedRow}>
                            <span>{label}</span>
                            <strong>{Math.round(newValue)}</strong>
                            <em className={delta >= 0 ? styles.deltaUp : styles.deltaDown}>{delta >= 0 ? `+${delta}` : delta}</em>
                          </div>
                        );
                      })}
                    </div>
                  ) : null}

                  {!hasPitchingPredictions && !hasBattingPredictions ? <p className={styles.emptyText}>No predicted attributes available.</p> : null}
                </div>

                {showProStatusPending ? (
                  <div className={styles.proPendingOverlay}>
                    <p className={styles.proPendingText}>Checking Pro access...</p>
                  </div>
                ) : null}

                {showProLock ? (
                  <div className={styles.proLockOverlay}>
                    <p className={styles.proLockTitle}>Sign up for Pro to see predicted attributes</p>
                    <p className={styles.proLockDescription}>
                      Unlock projected increases and decreases for key batting and pitching ratings before roster updates.
                    </p>
                    <button type="button" className={styles.proLockButton} onClick={handleGoPro}>
                      <Crown size={12} />
                      <span>Go Pro</span>
                    </button>
                  </div>
                ) : null}
              </div>
            ) : null}

          </>
        )}
      </section>
    </main>
  );
}
