import type { ShowCardPitchingStats, ShowCardStats } from "./types";

export function formatDate(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }
  return parsed.toLocaleDateString();
}

export function formatRate(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(3);
}

export function formatRatio(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(2);
}

export function formatPercent(value?: number | null, digits = 0): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${value.toFixed(digits)}%`;
}

export function formatCount(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return Math.round(value).toLocaleString();
}

export function clampPercent(value: number | null): number {
  if (value === null || Number.isNaN(value)) {
    return 0;
  }
  return Math.max(0, Math.min(100, value));
}

export function barColor(percent: number | null): string {
  if (percent === null) {
    return "rgba(148, 163, 184, 0.35)";
  }
  if (percent >= 80) {
    return "#22c55e";
  }
  if (percent >= 60) {
    return "#fbbf24";
  }
  return "#60a5fa";
}

export function formatCardName(card: {
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  mlb_id: number;
}): string {
  const full = (card.full_name || "").trim();
  if (full) {
    return full;
  }
  const assembled = [card.first_name, card.last_name].filter(Boolean).join(" ").trim();
  if (assembled) {
    return assembled;
  }
  return `Player ${card.mlb_id}`;
}

export function sortHittingCards(
  cards: ShowCardStats[],
  key: keyof ShowCardStats | "player",
  direction: "asc" | "desc",
): ShowCardStats[] {
  const factor = direction === "asc" ? 1 : -1;
  return [...cards].sort((a, b) => {
    const av = key === "player" ? formatCardName(a).toLowerCase() : a[key];
    const bv = key === "player" ? formatCardName(b).toLowerCase() : b[key];

    if (typeof av === "string" && typeof bv === "string") {
      return av.localeCompare(bv) * factor;
    }
    const an = typeof av === "number" ? av : -Infinity;
    const bn = typeof bv === "number" ? bv : -Infinity;
    return (an - bn) * factor;
  });
}

export function sortPitchingCards(
  cards: ShowCardPitchingStats[],
  key: keyof ShowCardPitchingStats | "player",
  direction: "asc" | "desc",
): ShowCardPitchingStats[] {
  const factor = direction === "asc" ? 1 : -1;
  return [...cards].sort((a, b) => {
    const av = key === "player" ? formatCardName(a).toLowerCase() : a[key];
    const bv = key === "player" ? formatCardName(b).toLowerCase() : b[key];

    if (typeof av === "string" && typeof bv === "string") {
      return av.localeCompare(bv) * factor;
    }
    const an = typeof av === "number" ? av : -Infinity;
    const bn = typeof bv === "number" ? bv : -Infinity;
    return (an - bn) * factor;
  });
}
