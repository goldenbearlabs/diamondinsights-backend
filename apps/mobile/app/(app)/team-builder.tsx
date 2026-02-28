import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  FlatList,
  Image,
  Modal,
  PanResponder,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
} from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { apiGetAuth } from "../../src/lib/api";
import { FloatingBackground } from "../../src/homescreencomponents/FloatingBackground";
import { useBackendProStatus } from "../../src/lib/proStatus";
import { theme } from "../../src/theme/colors";

type TeamCard = {
  id: string;
  name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  display_primary_position?: string | null;
  display_secondary_positions?: string | null;
  display_seconday_position?: string | null;
  true_overall?: number | null;
  true_overall_rounded?: number | null;
  meta_overall?: number | null;
  meta_overall_rounded?: number | null;
  your_overall?: number | null;
  your_overall_rounded?: number | null;
  true_overall_by_position?: Record<string, number> | null;
  meta_overall_by_position?: Record<string, number> | null;
  your_overall_by_position?: Record<string, number> | null;
  contact_left?: number | null;
  contact_right?: number | null;
  power_left?: number | null;
  power_right?: number | null;
  plate_vision?: number | null;
  batting_clutch?: number | null;
  speed?: number | null;
  fielding_ability?: number | null;
  stamina?: number | null;
  hits_per_bf?: number | null;
  k_per_bf?: number | null;
  pitch_velocity?: number | null;
  pitch_control?: number | null;
  pitch_movement?: number | null;
  bat_hand?: string | null;
  throw_hand?: string | null;
  team_short_name: string;
  year: number;
  is_hitter?: boolean;
  mlb_id?: number | null;
};

type RosterMode = "batters" | "pitchers";
type PositionFilterMode = "primary" | "secondary" | "all";
type ValueMetric = "ovr" | "true" | "meta" | "your";

type SlotSection = "lineup" | "bench" | "rotation" | "bullpen";

const BATTING_LINEUP_SLOT_KEYS = ["c", "1b", "2b", "3b", "ss", "lf", "cf", "rf", "dh"] as const;
const BENCH_SLOT_KEYS = ["bn1", "bn2", "bn3", "bn4"] as const;
const ROTATION_SLOT_KEYS = ["sp1", "sp2", "sp3", "sp4", "sp5"] as const;
const BULLPEN_SLOT_KEYS = ["bp1", "bp2", "bp3", "bp4", "bp5", "bp6", "bp7", "bp8"] as const;

const ALL_SLOT_KEYS = [
  ...BATTING_LINEUP_SLOT_KEYS,
  ...BENCH_SLOT_KEYS,
  ...ROTATION_SLOT_KEYS,
  ...BULLPEN_SLOT_KEYS,
] as const;

type BattingLineupSlotKey = (typeof BATTING_LINEUP_SLOT_KEYS)[number];
type AnySlotKey = (typeof ALL_SLOT_KEYS)[number];

type SlotMeta = {
  key: AnySlotKey;
  label: string;
  mode: RosterMode;
  section: SlotSection;
  targetPosition: string;
  draggable: boolean;
};

const SLOT_META: Record<AnySlotKey, SlotMeta> = {
  c: { key: "c", label: "C", mode: "batters", section: "lineup", targetPosition: "C", draggable: true },
  "1b": {
    key: "1b",
    label: "1B",
    mode: "batters",
    section: "lineup",
    targetPosition: "1B",
    draggable: true,
  },
  "2b": {
    key: "2b",
    label: "2B",
    mode: "batters",
    section: "lineup",
    targetPosition: "2B",
    draggable: true,
  },
  "3b": {
    key: "3b",
    label: "3B",
    mode: "batters",
    section: "lineup",
    targetPosition: "3B",
    draggable: true,
  },
  ss: {
    key: "ss",
    label: "SS",
    mode: "batters",
    section: "lineup",
    targetPosition: "SS",
    draggable: true,
  },
  lf: {
    key: "lf",
    label: "LF",
    mode: "batters",
    section: "lineup",
    targetPosition: "LF",
    draggable: true,
  },
  cf: {
    key: "cf",
    label: "CF",
    mode: "batters",
    section: "lineup",
    targetPosition: "CF",
    draggable: true,
  },
  rf: {
    key: "rf",
    label: "RF",
    mode: "batters",
    section: "lineup",
    targetPosition: "RF",
    draggable: true,
  },
  dh: {
    key: "dh",
    label: "DH",
    mode: "batters",
    section: "lineup",
    targetPosition: "DH",
    draggable: true,
  },
  bn1: {
    key: "bn1",
    label: "BN1",
    mode: "batters",
    section: "bench",
    targetPosition: "BENCH",
    draggable: false,
  },
  bn2: {
    key: "bn2",
    label: "BN2",
    mode: "batters",
    section: "bench",
    targetPosition: "BENCH",
    draggable: false,
  },
  bn3: {
    key: "bn3",
    label: "BN3",
    mode: "batters",
    section: "bench",
    targetPosition: "BENCH",
    draggable: false,
  },
  bn4: {
    key: "bn4",
    label: "BN4",
    mode: "batters",
    section: "bench",
    targetPosition: "BENCH",
    draggable: false,
  },
  sp1: {
    key: "sp1",
    label: "SP1",
    mode: "pitchers",
    section: "rotation",
    targetPosition: "SP",
    draggable: false,
  },
  sp2: {
    key: "sp2",
    label: "SP2",
    mode: "pitchers",
    section: "rotation",
    targetPosition: "SP",
    draggable: false,
  },
  sp3: {
    key: "sp3",
    label: "SP3",
    mode: "pitchers",
    section: "rotation",
    targetPosition: "SP",
    draggable: false,
  },
  sp4: {
    key: "sp4",
    label: "SP4",
    mode: "pitchers",
    section: "rotation",
    targetPosition: "SP",
    draggable: false,
  },
  sp5: {
    key: "sp5",
    label: "SP5",
    mode: "pitchers",
    section: "rotation",
    targetPosition: "SP",
    draggable: false,
  },
  bp1: {
    key: "bp1",
    label: "BP1",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp2: {
    key: "bp2",
    label: "BP2",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp3: {
    key: "bp3",
    label: "BP3",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp4: {
    key: "bp4",
    label: "BP4",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp5: {
    key: "bp5",
    label: "BP5",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp6: {
    key: "bp6",
    label: "BP6",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp7: {
    key: "bp7",
    label: "BP7",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
  bp8: {
    key: "bp8",
    label: "BP8",
    mode: "pitchers",
    section: "bullpen",
    targetPosition: "BP",
    draggable: false,
  },
};

const POSITION_FILTER_OPTIONS = [
  { key: "primary", label: "Primary Only" },
  { key: "secondary", label: "Include Secondary" },
  { key: "all", label: "All Cards" },
] as const;

const VALUE_METRIC_OPTIONS = [
  { key: "ovr", label: "Overall", chip: "OVR" },
  { key: "true", label: "True Overall", chip: "TRUE" },
  { key: "meta", label: "Meta Overall", chip: "META" },
  { key: "your", label: "Your Overall", chip: "YOUR" },
] as const;

const MODE_OPTIONS: { key: RosterMode; label: string }[] = [
  { key: "batters", label: "Batters" },
  { key: "pitchers", label: "Pitchers" },
];

const EMPTY_ROSTER = ALL_SLOT_KEYS.reduce((acc, key) => {
  acc[key] = null;
  return acc;
}, {} as Record<AnySlotKey, TeamCard | null>);

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const normalizePosition = (value: string | null | undefined) =>
  (value ?? "").trim().toUpperCase();

const normalizePlayerName = (value: string | null | undefined) =>
  (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

const normalizeHand = (value: string | null | undefined) => {
  const hand = (value ?? "").trim().toUpperCase();
  if (hand.startsWith("L")) return "L";
  if (hand.startsWith("R")) return "R";
  if (hand.startsWith("S")) return "S";
  return hand || "--";
};

const getPlayerIdentityKey = (card: TeamCard) => {
  if (typeof card.mlb_id === "number" && Number.isFinite(card.mlb_id)) {
    return `mlb:${card.mlb_id}`;
  }
  return `name:${normalizePlayerName(card.name)}`;
};

const splitPositions = (value: string | null | undefined) =>
  (value ?? "")
    .split(",")
    .flatMap((part) => part.split("/"))
    .map((part) => normalizePosition(part))
    .filter((part) => part.length > 0);

const moveItem = <T,>(items: T[], fromIndex: number, toIndex: number) => {
  if (fromIndex === toIndex) return items;
  const next = [...items];
  const [item] = next.splice(fromIndex, 1);
  if (item === undefined) return items;
  next.splice(toIndex, 0, item);
  return next;
};

const getPrimaryPosition = (card: TeamCard) =>
  normalizePosition(card.display_primary_position ?? card.display_position);

const getSecondaryPositions = (card: TeamCard) =>
  splitPositions(card.display_secondary_positions ?? card.display_seconday_position);

const formatMetric = (value: number | null | undefined) =>
  typeof value === "number" && Number.isFinite(value) ? String(Math.round(value)) : "--";

const averagedMetric = (left: number | null | undefined, right: number | null | undefined) => {
  if (typeof left !== "number" || typeof right !== "number") return null;
  return Math.round((left + right) / 2);
};

const getTrueOverallValue = (card: TeamCard) =>
  card.true_overall ?? card.true_overall_rounded ?? null;

const getMetaOverallValue = (card: TeamCard) =>
  card.meta_overall ?? card.meta_overall_rounded ?? null;

const getYourOverallValue = (card: TeamCard) =>
  card.your_overall ?? card.your_overall_rounded ?? null;

const getYourWeightFromCard = (card: TeamCard): number => {
  const meta = getMetaOverallValue(card);
  const your = getYourOverallValue(card);
  if (typeof meta !== "number" || !Number.isFinite(meta) || Math.abs(meta) < 1e-9) return 1;
  if (typeof your !== "number" || !Number.isFinite(your)) return 1;
  return your / meta;
};

const getPositionMapValue = (
  map: Record<string, number> | null | undefined,
  position: string | null | undefined
) => {
  if (!map) return null;
  const key = normalizePosition(position);
  const value = map[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
};

const getMetricValueForPosition = (
  card: TeamCard,
  metric: ValueMetric,
  position: string | null | undefined
): number | null => {
  if (metric === "ovr") return card.ovr;
  if (metric === "true") {
    return getPositionMapValue(card.true_overall_by_position, position) ?? getTrueOverallValue(card);
  }
  if (metric === "meta") {
    return getPositionMapValue(card.meta_overall_by_position, position) ?? getMetaOverallValue(card);
  }
  const yourFromMap = getPositionMapValue(card.your_overall_by_position, position);
  if (yourFromMap !== null) return yourFromMap;
  const metaFromMap = getPositionMapValue(card.meta_overall_by_position, position);
  if (metaFromMap !== null) return metaFromMap * getYourWeightFromCard(card);
  return getYourOverallValue(card);
};

const getSlotMetricValue = (
  card: TeamCard,
  metric: ValueMetric,
  slotMeta: SlotMeta
): number | null => {
  if (slotMeta.targetPosition === "BENCH") {
    return getMetricValueForPosition(card, metric, getPrimaryPosition(card));
  }

  if (slotMeta.targetPosition === "BP") {
    const rp = getMetricValueForPosition(card, metric, "RP");
    const cp = getMetricValueForPosition(card, metric, "CP");
    const bullpenBest = [rp, cp].filter(
      (value): value is number => typeof value === "number" && Number.isFinite(value)
    );
    if (bullpenBest.length > 0) return Math.max(...bullpenBest);
    return getMetricValueForPosition(card, metric, getPrimaryPosition(card));
  }

  return getMetricValueForPosition(card, metric, slotMeta.targetPosition);
};

const compareCardsForSlot = (
  a: TeamCard,
  b: TeamCard,
  metric: ValueMetric,
  slotMeta: SlotMeta
) => {
  const metricDiff =
    (getSlotMetricValue(b, metric, slotMeta) ?? -1) -
    (getSlotMetricValue(a, metric, slotMeta) ?? -1);
  if (metricDiff !== 0) return metricDiff;
  const ovrDiff = b.ovr - a.ovr;
  if (ovrDiff !== 0) return ovrDiff;
  return a.name.localeCompare(b.name);
};

const matchesPositionSet = (
  card: TeamCard,
  allowed: Set<string>,
  filterMode: PositionFilterMode
) => {
  if (filterMode === "all") return true;
  const primary = getPrimaryPosition(card);
  if (allowed.has(primary)) return true;
  if (filterMode === "primary") return false;
  const secondary = getSecondaryPositions(card);
  return secondary.some((position) => allowed.has(position));
};

const cardMatchesSlot = (
  card: TeamCard,
  slotMeta: SlotMeta,
  filterMode: PositionFilterMode
) => {
  const wantsHitter = slotMeta.mode === "batters";
  if (wantsHitter && card.is_hitter === false) return false;
  if (!wantsHitter && card.is_hitter !== false) return false;

  if (slotMeta.targetPosition === "BENCH" || slotMeta.targetPosition === "DH") {
    return true;
  }

  if (slotMeta.targetPosition === "SP") {
    return matchesPositionSet(card, new Set(["SP"]), filterMode);
  }

  if (slotMeta.targetPosition === "BP") {
    return matchesPositionSet(card, new Set(["RP", "CP"]), filterMode);
  }

  return matchesPositionSet(card, new Set([slotMeta.targetPosition]), filterMode);
};

const getHandText = (card: TeamCard, mode: RosterMode) => {
  if (mode === "pitchers") return normalizeHand(card.throw_hand);
  return normalizeHand(card.bat_hand);
};

const getHitterAttributes = (card: TeamCard) => [
  `CON:${formatMetric(averagedMetric(card.contact_left, card.contact_right))}`,
  `POW:${formatMetric(averagedMetric(card.power_left, card.power_right))}`,
  `VIS:${formatMetric(card.plate_vision)}`,
  `CLU:${formatMetric(card.batting_clutch)}`,
  `SPD:${formatMetric(card.speed)}`,
  `FLD:${formatMetric(card.fielding_ability)}`,
];

const getPitcherAttributes = (card: TeamCard) => [
  `STA:${formatMetric(card.stamina)}`,
  `H/9:${formatMetric(card.hits_per_bf)}`,
  `K/9:${formatMetric(card.k_per_bf)}`,
  `VEL:${formatMetric(card.pitch_velocity)}`,
  `CTRL:${formatMetric(card.pitch_control)}`,
  `MVMT:${formatMetric(card.pitch_movement)}`,
];

const getAttributesForMode = (card: TeamCard, mode: RosterMode) => {
  if (mode === "pitchers") return getPitcherAttributes(card);
  return getHitterAttributes(card);
};

const fetchCardsForBuilder = async (path: string): Promise<TeamCard[]> => {
  return apiGetAuth<TeamCard[]>(path);
};

const getModeSlotKeys = (mode: RosterMode): AnySlotKey[] => {
  if (mode === "batters") return [...BATTING_LINEUP_SLOT_KEYS, ...BENCH_SLOT_KEYS];
  return [...ROTATION_SLOT_KEYS, ...BULLPEN_SLOT_KEYS];
};

type LineupEntry = { slotKey: BattingLineupSlotKey; card: TeamCard };

const DEFENSIVE_FILL_ORDER: BattingLineupSlotKey[] = ["c", "ss", "cf", "2b", "3b", "lf", "rf", "1b", "dh"];
const OUTFIELD_POSITIONS = new Set(["LF", "CF", "RF"]);
const INFIELD_POSITIONS = new Set(["1B", "2B", "3B", "SS"]);

const toStatValue = (value: number | null | undefined) =>
  (typeof value === "number" && Number.isFinite(value) ? value : 0);

const getHitterMetricValue = (card: TeamCard, metric: ValueMetric) =>
  getMetricValueForPosition(card, metric, getPrimaryPosition(card)) ?? card.ovr;

const getContactAverage = (card: TeamCard) => {
  const average = averagedMetric(card.contact_left, card.contact_right);
  return average === null ? 0 : average;
};

const getPowerAverage = (card: TeamCard) => {
  const average = averagedMetric(card.power_left, card.power_right);
  return average === null ? 0 : average;
};

const getHitterHand = (card: TeamCard) => normalizeHand(card.bat_hand);

const getLineupAdjacencyScore = (leftCard: TeamCard, rightCard: TeamCard) => {
  const leftHand = getHitterHand(leftCard);
  const rightHand = getHitterHand(rightCard);

  if (leftHand === "S" || rightHand === "S") return 10;
  if (leftHand === "--" || rightHand === "--") return 0;
  if (leftHand === rightHand) return -44;
  return 14;
};

const getLineupSpotScore = (
  card: TeamCard,
  metric: ValueMetric,
  spotIndex: number
) => {
  const metricValue = getHitterMetricValue(card, metric);
  const contact = getContactAverage(card);
  const power = getPowerAverage(card);
  const speed = toStatValue(card.speed);
  const clutch = toStatValue(card.batting_clutch);
  const vision = toStatValue(card.plate_vision);
  const switchBonus = getHitterHand(card) === "S" ? 24 : 0;

  if (spotIndex === 0) {
    return speed * 0.4 + contact * 0.33 + vision * 0.12 + metricValue * 0.15 + switchBonus * 0.4;
  }

  if (spotIndex === 1) {
    return (
      metricValue * 0.33 +
      contact * 0.24 +
      speed * 0.2 +
      power * 0.1 +
      vision * 0.13 +
      switchBonus * 1.75
    );
  }

  if (spotIndex === 2) {
    return power * 0.52 + metricValue * 0.24 + contact * 0.18 + clutch * 0.06;
  }

  if (spotIndex === 3) {
    return power * 0.42 + metricValue * 0.3 + contact * 0.18 + clutch * 0.1;
  }

  const depthWeight = [1, 0.97, 0.94, 0.91, 0.88][spotIndex - 4] ?? 0.85;
  const production = metricValue * 0.36 + contact * 0.29 + power * 0.25 + speed * 0.1;
  return production * depthWeight;
};

const evaluateLineupSequence = (
  entries: LineupEntry[],
  metric: ValueMetric
) => {
  let score = 0;
  for (let index = 0; index < entries.length; index += 1) {
    const current = entries[index];
    if (!current) continue;
    score += getLineupSpotScore(current.card, metric, index);

    if (index === 0) continue;
    const previous = entries[index - 1];
    if (!previous) continue;
    score += getLineupAdjacencyScore(previous.card, current.card);

    if (index >= 2) {
      const handA = getHitterHand(entries[index - 2].card);
      const handB = getHitterHand(previous.card);
      const handC = getHitterHand(current.card);
      const isLeftRight = handA !== "S" && handA !== "--" && handA === handB && handB === handC;
      if (isLeftRight) score -= 26;
    }
  }
  return score;
};

const buildPermutations = <T,>(items: T[]): T[][] => {
  if (items.length <= 1) return [items];
  const permutations: T[][] = [];
  for (let index = 0; index < items.length; index += 1) {
    const current = items[index];
    if (current === undefined) continue;
    const rest = [...items.slice(0, index), ...items.slice(index + 1)];
    for (const permutation of buildPermutations(rest)) {
      permutations.push([current, ...permutation]);
    }
  }
  return permutations;
};

const pickBestIndex = (entries: LineupEntry[], getScore: (entry: LineupEntry) => number) => {
  let bestIndex = -1;
  let bestScore = Number.NEGATIVE_INFINITY;

  for (let index = 0; index < entries.length; index += 1) {
    const candidate = entries[index];
    if (!candidate) continue;
    const score = getScore(candidate);
    if (score > bestScore) {
      bestScore = score;
      bestIndex = index;
    }
  }

  return bestIndex;
};

const optimizeBattingOrderFromGeneratedLineup = (
  generated: Partial<Record<AnySlotKey, TeamCard | null>>,
  metric: ValueMetric
) => {
  const lineupEntries: LineupEntry[] = BATTING_LINEUP_SLOT_KEYS
    .map((slotKey) => {
      const card = generated[slotKey];
      return card ? { slotKey, card } : null;
    })
    .filter((entry): entry is LineupEntry => entry !== null);

  if (lineupEntries.length <= 1) {
    return [...BATTING_LINEUP_SLOT_KEYS];
  }

  if (lineupEntries.length < 5) {
    const permutations = buildPermutations(lineupEntries);
    let best = permutations[0] ?? lineupEntries;
    let bestScore = Number.NEGATIVE_INFINITY;
    for (const permutation of permutations) {
      const score = evaluateLineupSequence(permutation, metric);
      if (score > bestScore) {
        best = permutation;
        bestScore = score;
      }
    }

    const selected = best.map((entry) => entry.slotKey);
    const missing = BATTING_LINEUP_SLOT_KEYS.filter((slotKey) => !selected.includes(slotKey));
    return [...selected, ...missing];
  }

  const remaining = [...lineupEntries];

  const thirdIndex = pickBestIndex(remaining, (entry) => getLineupSpotScore(entry.card, metric, 2));
  const third = thirdIndex >= 0 ? remaining.splice(thirdIndex, 1)[0] : null;

  const secondIndex = pickBestIndex(remaining, (entry) => {
    const base = getLineupSpotScore(entry.card, metric, 1);
    if (!third) return base;
    return base + getLineupAdjacencyScore(entry.card, third.card);
  });
  const second = secondIndex >= 0 ? remaining.splice(secondIndex, 1)[0] : null;

  const firstIndex = pickBestIndex(remaining, (entry) => {
    const base = getLineupSpotScore(entry.card, metric, 0);
    if (!second) return base;
    return base + getLineupAdjacencyScore(entry.card, second.card);
  });
  const first = firstIndex >= 0 ? remaining.splice(firstIndex, 1)[0] : null;

  const fourthIndex = pickBestIndex(remaining, (entry) => {
    const base = getLineupSpotScore(entry.card, metric, 3);
    if (!third) return base;
    return base + getLineupAdjacencyScore(third.card, entry.card);
  });
  const fourth = fourthIndex >= 0 ? remaining.splice(fourthIndex, 1)[0] : null;

  const anchors = [first, second, third, fourth].filter((entry): entry is LineupEntry => entry !== null);
  const tailPermutations = buildPermutations(remaining);

  let bestOrder: LineupEntry[] = [...anchors, ...remaining];
  let bestScore = Number.NEGATIVE_INFINITY;

  for (const permutation of tailPermutations) {
    const candidate = [...anchors, ...permutation];
    const candidateScore = evaluateLineupSequence(candidate, metric);
    if (candidateScore > bestScore) {
      bestOrder = candidate;
      bestScore = candidateScore;
    }
  }

  const selectedOrder = bestOrder.map((entry) => entry.slotKey);
  const missingSlots = BATTING_LINEUP_SLOT_KEYS.filter((slotKey) => !selectedOrder.includes(slotKey));
  return [...selectedOrder, ...missingSlots];
};

const getVersatilityCount = (card: TeamCard) => {
  const positions = new Set<string>([getPrimaryPosition(card), ...getSecondaryPositions(card)]);
  positions.delete("");
  return positions.size;
};

const hasAnyPositionFromSet = (card: TeamCard, positionSet: Set<string>) => {
  const allPositions = [getPrimaryPosition(card), ...getSecondaryPositions(card)];
  return allPositions.some((position) => positionSet.has(position));
};

const canCoverCatcher = (card: TeamCard) => {
  const primary = getPrimaryPosition(card);
  if (primary === "C") return true;
  return getSecondaryPositions(card).includes("C");
};

const canPlayOutfield = (card: TeamCard) => hasAnyPositionFromSet(card, OUTFIELD_POSITIONS);
const canPlayInfield = (card: TeamCard) => hasAnyPositionFromSet(card, INFIELD_POSITIONS);

const getBenchRoleScores = (card: TeamCard, metric: ValueMetric) => {
  const metricValue = getHitterMetricValue(card, metric);
  const leftySplit = toStatValue(card.contact_left) * 0.56 + toStatValue(card.power_left) * 0.44;
  const rightySplit = toStatValue(card.contact_right) * 0.56 + toStatValue(card.power_right) * 0.44;
  const speedScore = toStatValue(card.speed);
  const clutchScore = toStatValue(card.batting_clutch);
  const utilityScore =
    toStatValue(card.fielding_ability) * 0.56 + getVersatilityCount(card) * 9 + speedScore * 0.24;

  return {
    metricValue,
    leftySplit,
    rightySplit,
    speedScore,
    clutchScore,
    utilityScore,
    catcherCoverage: canCoverCatcher(card) ? 1 : 0,
  };
};

const selectBenchCardsWithVariety = (
  candidates: TeamCard[],
  metric: ValueMetric
) => {
  if (candidates.length <= BENCH_SLOT_KEYS.length) return [...candidates];

  const scoredCandidatesRaw = candidates
    .map((card) => {
      const role = getBenchRoleScores(card, metric);
      const shortlistScore =
        role.metricValue * 0.45 +
        Math.max(role.leftySplit, role.rightySplit) * 0.24 +
        role.speedScore * 0.16 +
        role.utilityScore * 0.15 +
        role.catcherCoverage * 18;
      return {
        card,
        role,
        shortlistScore,
        playerKey: getPlayerIdentityKey(card),
        canCatch: canCoverCatcher(card),
        canOF: canPlayOutfield(card),
        canINF: canPlayInfield(card),
      };
    })
    .sort((a, b) => b.shortlistScore - a.shortlistScore);

  const scoredCandidates: typeof scoredCandidatesRaw = [];
  const seenPlayers = new Set<string>();
  for (const candidate of scoredCandidatesRaw) {
    if (seenPlayers.has(candidate.playerKey)) continue;
    seenPlayers.add(candidate.playerKey);
    scoredCandidates.push(candidate);
  }

  const shortlist = scoredCandidates.slice(0, 36);
  if (shortlist.length <= BENCH_SLOT_KEYS.length) return shortlist.map((item) => item.card);

  const catchers = shortlist.filter((item) => item.canCatch);
  const outfielders = shortlist.filter((item) => item.canOF && !item.canCatch);
  const infielders = shortlist.filter((item) => item.canINF && !item.canCatch);
  const extras = shortlist.filter((item) => !item.canCatch);

  if (
    catchers.length === 0 ||
    outfielders.length === 0 ||
    infielders.length === 0 ||
    extras.length === 0
  ) {
    const fallback = shortlist
      .filter((item) => !item.canCatch)
      .sort((a, b) => b.shortlistScore - a.shortlistScore)
      .slice(0, BENCH_SLOT_KEYS.length - 1)
      .map((item) => item.card);

    const catcher = catchers[0]?.card;
    if (catcher) return [catcher, ...fallback].slice(0, BENCH_SLOT_KEYS.length);
    return shortlist.slice(0, BENCH_SLOT_KEYS.length).map((item) => item.card);
  }

  let bestSelection: [typeof shortlist[number], typeof shortlist[number], typeof shortlist[number], typeof shortlist[number]] | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  let bestKillerClutch = Number.NEGATIVE_INFINITY;
  let bestAverageClutch = Number.NEGATIVE_INFINITY;

  for (const catcher of catchers) {
    for (const outfielder of outfielders) {
      if (outfielder.playerKey === catcher.playerKey) continue;
      for (const infielder of infielders) {
        if (infielder.playerKey === catcher.playerKey || infielder.playerKey === outfielder.playerKey) {
          continue;
        }
        for (const extra of extras) {
          const keys = new Set([catcher.playerKey, outfielder.playerKey, infielder.playerKey, extra.playerKey]);
          if (keys.size !== 4) continue;

          const selection = [catcher, outfielder, infielder, extra] as const;
          const catcherCount = selection.filter((item) => item.canCatch).length;
          if (catcherCount !== 1) continue;

          const leftyKiller = selection.reduce((best, current) =>
            current.role.leftySplit > best.role.leftySplit ? current : best
          );
          const rightyKiller = selection.reduce((best, current) =>
            current.role.rightySplit > best.role.rightySplit ? current : best
          );

          const leftyMax = leftyKiller.role.leftySplit;
          const rightyMax = rightyKiller.role.rightySplit;
          const speedMax = Math.max(...selection.map((item) => item.role.speedScore));
          const utilityMax = Math.max(...selection.map((item) => item.role.utilityScore));
          const avgMetric =
            selection.reduce((sum, item) => sum + item.role.metricValue, 0) / selection.length;

          const catcherQuality =
            catcher.role.metricValue * 0.35 +
            catcher.role.utilityScore * 0.4 +
            catcher.role.clutchScore * 0.25;
          const outfieldQuality =
            outfielder.role.speedScore * 0.32 +
            Math.max(outfielder.role.leftySplit, outfielder.role.rightySplit) * 0.3 +
            outfielder.role.metricValue * 0.2 +
            outfielder.role.utilityScore * 0.18;
          const infieldQuality =
            infielder.role.utilityScore * 0.38 +
            infielder.role.metricValue * 0.27 +
            Math.max(infielder.role.leftySplit, infielder.role.rightySplit) * 0.2 +
            infielder.role.speedScore * 0.15;
          const extraQuality =
            extra.role.metricValue * 0.42 +
            Math.max(extra.role.leftySplit, extra.role.rightySplit) * 0.22 +
            extra.role.speedScore * 0.2 +
            extra.role.utilityScore * 0.16;

          const comboScore =
            leftyMax * 1.15 +
            rightyMax * 1.15 +
            speedMax * 1.0 +
            utilityMax * 1.0 +
            avgMetric * 0.9 +
            catcherQuality * 0.55 +
            outfieldQuality * 0.4 +
            infieldQuality * 0.4 +
            extraQuality * 0.3;

          const killerClutchSum =
            leftyKiller.playerKey === rightyKiller.playerKey
              ? leftyKiller.role.clutchScore
              : leftyKiller.role.clutchScore + rightyKiller.role.clutchScore;
          const averageClutch =
            selection.reduce((sum, item) => sum + item.role.clutchScore, 0) / selection.length;

          const betterScore = comboScore > bestScore + 1e-6;
          const tiedScore = Math.abs(comboScore - bestScore) <= 1e-6;
          const betterKillerClutch = killerClutchSum > bestKillerClutch + 1e-6;
          const tiedKillerClutch = Math.abs(killerClutchSum - bestKillerClutch) <= 1e-6;
          const betterAverageClutch = averageClutch > bestAverageClutch;

          if (
            betterScore ||
            (tiedScore && betterKillerClutch) ||
            (tiedScore && tiedKillerClutch && betterAverageClutch)
          ) {
            bestSelection = [catcher, outfielder, infielder, extra];
            bestScore = comboScore;
            bestKillerClutch = killerClutchSum;
            bestAverageClutch = averageClutch;
          }
        }
      }
    }
  }

  if (bestSelection) {
    return bestSelection.map((item) => item.card);
  }

  const fallbackCatcher = catchers[0];
  if (!fallbackCatcher) return shortlist.slice(0, BENCH_SLOT_KEYS.length).map((item) => item.card);

  const fallbackUsed = new Set([fallbackCatcher.playerKey]);
  const fallbackOrder = [fallbackCatcher.card];
  const pushFirst = (bucket: typeof shortlist) => {
    const candidate = bucket.find((item) => !fallbackUsed.has(item.playerKey) && !item.canCatch);
    if (!candidate) return;
    fallbackUsed.add(candidate.playerKey);
    fallbackOrder.push(candidate.card);
  };

  pushFirst(outfielders);
  pushFirst(infielders);
  pushFirst(extras);

  for (const candidate of extras) {
    if (fallbackOrder.length >= BENCH_SLOT_KEYS.length) break;
    if (fallbackUsed.has(candidate.playerKey)) continue;
    fallbackUsed.add(candidate.playerKey);
    fallbackOrder.push(candidate.card);
  }

  return fallbackOrder.slice(0, BENCH_SLOT_KEYS.length);
};

const isSlotRequiringPositionFilter = (slotMeta: SlotMeta) => {
  return !["BENCH", "DH"].includes(slotMeta.targetPosition);
};

export default function TeamBuilder() {
  const { isPro } = useBackendProStatus();
  const hasProAccess = isPro === true;
  const [mode, setMode] = useState<RosterMode>("batters");
  const [roster, setRoster] = useState<Record<AnySlotKey, TeamCard | null>>(EMPTY_ROSTER);
  const [battingOrder, setBattingOrder] = useState<BattingLineupSlotKey[]>([
    ...BATTING_LINEUP_SLOT_KEYS,
  ]);
  const [activeSlot, setActiveSlot] = useState<AnySlotKey | null>(null);
  const [searchText, setSearchText] = useState("");
  const [results, setResults] = useState<TeamCard[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [draggingSlot, setDraggingSlot] = useState<BattingLineupSlotKey | null>(null);
  const [positionFilterMode, setPositionFilterMode] = useState<PositionFilterMode>("primary");
  const [teamMetric, setTeamMetric] = useState<ValueMetric>("meta");
  const [sortMetric, setSortMetric] = useState<ValueMetric>("meta");
  const [allHitterCardsCache, setAllHitterCardsCache] = useState<TeamCard[] | null>(null);
  const [allPitcherCardsCache, setAllPitcherCardsCache] = useState<TeamCard[] | null>(null);
  const [generating, setGenerating] = useState(false);
  const isMetricLocked = (metric: ValueMetric) => metric === "your" && !hasProAccess;

  const requestIdRef = useRef(0);
  const battingOrderRef = useRef(battingOrder);
  const rowHeightRef = useRef(104);
  const dragEndedAtRef = useRef(0);
  const dragStateRef = useRef<{ slot: BattingLineupSlotKey | null; startIndex: number }>({
    slot: null,
    startIndex: -1,
  });

  useEffect(() => {
    battingOrderRef.current = battingOrder;
  }, [battingOrder]);

  useEffect(() => {
    setActiveSlot(null);
  }, [mode]);

  useEffect(() => {
    if (hasProAccess) return;
    if (teamMetric === "your") setTeamMetric("meta");
    if (sortMetric === "your") setSortMetric("meta");
  }, [hasProAccess, teamMetric, sortMetric]);

  const currentModeSlotKeys = useMemo(() => getModeSlotKeys(mode), [mode]);

  const selectedCardsForMode = useMemo(
    () => currentModeSlotKeys
      .map((slotKey) => roster[slotKey])
      .filter((card): card is TeamCard => card !== null),
    [currentModeSlotKeys, roster]
  );

  const selectedCount = selectedCardsForMode.length;
  const totalCount = currentModeSlotKeys.length;

  const metricLabel = useMemo(
    () => VALUE_METRIC_OPTIONS.find((option) => option.key === teamMetric)?.label ?? "Overall",
    [teamMetric]
  );

  const generateDisabled = generating;

  const averageOverall = useMemo(() => {
    const metricValues = currentModeSlotKeys
      .map((slotKey) => {
        const card = roster[slotKey];
        if (!card) return null;
        return getSlotMetricValue(card, teamMetric, SLOT_META[slotKey]);
      })
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));

    if (metricValues.length === 0) return null;
    const total = metricValues.reduce((sum, value) => sum + value, 0);
    return total / metricValues.length;
  }, [currentModeSlotKeys, roster, teamMetric]);

  const activeSlotMeta = useMemo(() => (activeSlot ? SLOT_META[activeSlot] : null), [activeSlot]);
  const activeSlotLabel = activeSlotMeta?.label ?? "";

  const filteredResults = useMemo(() => {
    if (!activeSlotMeta) return [];

    const blockedPlayers = new Set<string>();
    for (const slotKey of ALL_SLOT_KEYS) {
      if (slotKey === activeSlotMeta.key) continue;
      const selected = roster[slotKey];
      if (!selected) continue;
      blockedPlayers.add(getPlayerIdentityKey(selected));
    }

    return results.filter((card) => {
      if (!cardMatchesSlot(card, activeSlotMeta, positionFilterMode)) return false;
      if (blockedPlayers.has(getPlayerIdentityKey(card))) return false;
      return true;
    });
  }, [activeSlotMeta, positionFilterMode, results, roster]);

  const sortedFilteredResults = useMemo(() => {
    if (!activeSlotMeta) return filteredResults;
    return [...filteredResults].sort((a, b) => compareCardsForSlot(a, b, sortMetric, activeSlotMeta));
  }, [filteredResults, sortMetric, activeSlotMeta]);

  const filterDescription = useMemo(() => {
    if (!activeSlotMeta) return "";

    if (activeSlotMeta.targetPosition === "DH") {
      return "DH can use any hitter card (duplicate players are excluded).";
    }

    if (activeSlotMeta.targetPosition === "BENCH") {
      return "Bench can use any hitter card (duplicate players are excluded).";
    }

    if (activeSlotMeta.targetPosition === "BP") {
      if (positionFilterMode === "all") return "Showing all pitcher cards.";
      if (positionFilterMode === "secondary") {
        return "Showing bullpen cards by primary and secondary positions (RP/CP).";
      }
      return "Showing primary bullpen positions only (RP/CP).";
    }

    if (activeSlotMeta.targetPosition === "SP") {
      if (positionFilterMode === "all") return "Showing all pitcher cards.";
      if (positionFilterMode === "secondary") {
        return "Showing SP cards by primary and secondary positions.";
      }
      return "Showing SP primary-position cards only.";
    }

    if (positionFilterMode === "all") return "Showing all cards for this roster mode.";
    if (positionFilterMode === "secondary") {
      return `Showing ${activeSlotLabel} cards by primary and secondary position.`;
    }
    return `Showing ${activeSlotLabel} primary-position cards only.`;
  }, [activeSlotMeta, activeSlotLabel, positionFilterMode]);

  useEffect(() => {
    if (!activeSlotMeta) return;

    const timeoutId = setTimeout(() => {
      void loadCards(searchText, activeSlotMeta.mode);
    }, 250);

    return () => clearTimeout(timeoutId);
  }, [searchText, activeSlotMeta]);

  useEffect(() => {
    if (!activeSlotMeta) {
      setSearchText("");
      setResults([]);
      setError(null);
      setPositionFilterMode("primary");
    }
  }, [activeSlotMeta]);

  const loadCards = async (query: string, rosterMode: RosterMode) => {
    const requestId = ++requestIdRef.current;
    setLoading(true);
    setError(null);

    try {
      const trimmed = query.trim();

      const buildUrl = (offset: number) => {
        const params = new URLSearchParams({
          year: "25",
          is_hitter: rosterMode === "batters" ? "true" : "false",
          limit: "100",
          offset: String(offset),
        });
        if (trimmed) params.set("name", trimmed);
        return `/cards/?${params.toString()}`;
      };

      const firstBatch = await fetchCardsForBuilder(buildUrl(0));
      let cards = firstBatch;

      if (!trimmed && firstBatch.length === 100) {
        const secondBatch = await fetchCardsForBuilder(buildUrl(100));
        cards = [...firstBatch, ...secondBatch];
      }

      if (requestId !== requestIdRef.current) return;

      const unique = new Map<string, TeamCard>();
      for (const card of cards) unique.set(card.id, card);

      setResults(Array.from(unique.values()).filter((card) => card.year === 25));
    } catch (err) {
      if (requestId !== requestIdRef.current) return;
      if (err instanceof Error && err.message === "Not authenticated") {
        setError("Sign in to load personalized cards.");
      } else {
        setError("Could not load cards. Try again.");
      }
      console.error("Failed to fetch cards for team builder:", err);
    } finally {
      if (requestId === requestIdRef.current) {
        setLoading(false);
      }
    }
  };

  const assignCardToSlot = (card: TeamCard) => {
    if (!activeSlotMeta) return;
    const nextPlayerKey = getPlayerIdentityKey(card);

    setRoster((previous) => {
      const next = { ...previous };
      for (const slotKey of ALL_SLOT_KEYS) {
        const existing = next[slotKey];
        if (!existing) continue;

        if (existing.id === card.id) {
          next[slotKey] = null;
          continue;
        }

        if (getPlayerIdentityKey(existing) === nextPlayerKey) {
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
    if (rosterMode === "batters" && allHitterCardsCache) return allHitterCardsCache;
    if (rosterMode === "pitchers" && allPitcherCardsCache) return allPitcherCardsCache;

    const pageSize = 100;
    const maxPages = 40;
    const collected: TeamCard[] = [];

    for (let page = 0; page < maxPages; page += 1) {
      const offset = page * pageSize;
      const params = new URLSearchParams({
        year: "25",
        is_hitter: rosterMode === "batters" ? "true" : "false",
        limit: String(pageSize),
        offset: String(offset),
      });

      const batch = await fetchCardsForBuilder(`/cards/?${params.toString()}`);
      collected.push(...batch);

      if (batch.length < pageSize) break;
    }

    const unique = new Map<string, TeamCard>();
    for (const card of collected) unique.set(card.id, card);

    const cached = Array.from(unique.values()).filter((card) => card.year === 25);
    if (rosterMode === "batters") setAllHitterCardsCache(cached);
    else setAllPitcherCardsCache(cached);
    return cached;
  };

  const pickBestCardForSlot = (
    pool: TeamCard[],
    usedPlayers: Set<string>,
    slotMeta: SlotMeta,
    metric: ValueMetric
  ) => {
    let best: TeamCard | null = null;

    for (const card of pool) {
      if (usedPlayers.has(getPlayerIdentityKey(card))) continue;
      if (!cardMatchesSlot(card, slotMeta, "secondary")) continue;

      if (!best || compareCardsForSlot(card, best, metric, slotMeta) < 0) {
        best = card;
      }
    }

    return best;
  };

  const generatePitcherRoster = (
    pool: TeamCard[],
    usedPlayers: Set<string>,
    metric: ValueMetric
  ) => {
    const generated = {} as Partial<Record<AnySlotKey, TeamCard | null>>;
    for (const slotKey of [...ROTATION_SLOT_KEYS, ...BULLPEN_SLOT_KEYS]) {
      generated[slotKey] = null;
    }

    const assignSlot = (slotKey: AnySlotKey) => {
      const best = pickBestCardForSlot(pool, usedPlayers, SLOT_META[slotKey], metric);
      if (!best) return;
      generated[slotKey] = best;
      usedPlayers.add(getPlayerIdentityKey(best));
    };

    for (const slotKey of ROTATION_SLOT_KEYS) assignSlot(slotKey);
    for (const slotKey of BULLPEN_SLOT_KEYS) assignSlot(slotKey);
    return generated;
  };

  const generateBatterRoster = (
    pool: TeamCard[],
    usedPlayers: Set<string>,
    metric: ValueMetric
  ) => {
    const generated = {} as Partial<Record<AnySlotKey, TeamCard | null>>;
    for (const slotKey of [...BATTING_LINEUP_SLOT_KEYS, ...BENCH_SLOT_KEYS]) {
      generated[slotKey] = null;
    }

    for (const slotKey of DEFENSIVE_FILL_ORDER) {
      const best = pickBestCardForSlot(pool, usedPlayers, SLOT_META[slotKey], metric);
      if (!best) continue;
      generated[slotKey] = best;
      usedPlayers.add(getPlayerIdentityKey(best));
    }

    const optimizedOrder = optimizeBattingOrderFromGeneratedLineup(generated, metric);

    const benchCandidates = pool.filter((card) => !usedPlayers.has(getPlayerIdentityKey(card)));
    const selectedBenchCards = selectBenchCardsWithVariety(benchCandidates, metric);

    for (let index = 0; index < BENCH_SLOT_KEYS.length; index += 1) {
      const slotKey = BENCH_SLOT_KEYS[index];
      const selected = selectedBenchCards[index];
      if (!selected) continue;
      generated[slotKey] = selected;
      usedPlayers.add(getPlayerIdentityKey(selected));
    }

    return { generated, optimizedOrder };
  };

  const handleGenerateGreedy = async () => {
    if (generating) return;
    setGenerating(true);

    try {
      const [hitterPool, pitcherPool] = await Promise.all([
        loadAllCardsForMode("batters"),
        loadAllCardsForMode("pitchers"),
      ]);
      const usedPlayers = new Set<string>();
      const generatedBatters = generateBatterRoster(hitterPool, usedPlayers, teamMetric);
      const generatedPitchers = generatePitcherRoster(pitcherPool, usedPlayers, teamMetric);

      battingOrderRef.current = generatedBatters.optimizedOrder;
      setBattingOrder(generatedBatters.optimizedOrder);
      setRoster((previous) => ({ ...previous, ...generatedBatters.generated, ...generatedPitchers }));
    } catch (err) {
      console.error("Failed to generate greedy team:", err);
      if (err instanceof Error && err.message === "Not authenticated") {
        setError("Sign in to generate a personalized team.");
      } else {
        setError("Could not generate team right now.");
      }
    } finally {
      setGenerating(false);
    }
  };

  const dragResponders = useMemo(() => {
    const responders = {} as Record<BattingLineupSlotKey, ReturnType<typeof PanResponder.create>>;

    for (const slotKey of BATTING_LINEUP_SLOT_KEYS) {
      responders[slotKey] = PanResponder.create({
        onStartShouldSetPanResponder: () => true,
        onStartShouldSetPanResponderCapture: () => true,
        onMoveShouldSetPanResponder: (_, gestureState) => Math.abs(gestureState.dy) > 2,
        onMoveShouldSetPanResponderCapture: (_, gestureState) =>
          Math.abs(gestureState.dy) > 2,
        onPanResponderGrant: () => {
          const startIndex = battingOrderRef.current.indexOf(slotKey);
          dragStateRef.current = { slot: slotKey, startIndex };
          setDraggingSlot(slotKey);
        },
        onPanResponderMove: (_, gestureState) => {
          const dragState = dragStateRef.current;
          if (dragState.slot !== slotKey || dragState.startIndex < 0) return;

          const currentOrder = battingOrderRef.current;
          const currentIndex = currentOrder.indexOf(slotKey);
          if (currentIndex < 0) return;

          const step = Math.max(72, rowHeightRef.current);
          const delta =
            gestureState.dy >= 0
              ? Math.floor((gestureState.dy + step * 0.35) / step)
              : Math.ceil((gestureState.dy - step * 0.35) / step);

          const targetIndex = clamp(dragState.startIndex + delta, 0, currentOrder.length - 1);
          if (targetIndex === currentIndex) return;

          const nextOrder = moveItem(currentOrder, currentIndex, targetIndex);
          battingOrderRef.current = nextOrder;
          setBattingOrder(nextOrder);
        },
        onPanResponderTerminationRequest: () => false,
        onPanResponderRelease: () => {
          dragEndedAtRef.current = Date.now();
          setDraggingSlot(null);
          dragStateRef.current = { slot: null, startIndex: -1 };
        },
        onPanResponderTerminate: () => {
          dragEndedAtRef.current = Date.now();
          setDraggingSlot(null);
          dragStateRef.current = { slot: null, startIndex: -1 };
        },
      });
    }

    return responders;
  }, []);

  const renderRosterRow = (slotKey: AnySlotKey, orderIndex?: number) => {
    const slotMeta = SLOT_META[slotKey];
    const card = roster[slotKey];
    const isActive = activeSlotMeta?.key === slotKey;
    const isDragging = draggingSlot === slotKey;
    const isDraggable = slotMeta.draggable && mode === "batters";
    const isLineupSlot = typeof orderIndex === "number";

    return (
      <TouchableOpacity
        key={slotKey}
        activeOpacity={0.85}
        style={[
          styles.rowCard,
          isActive && styles.rowCardActive,
          isDragging && styles.rowCardDragging,
        ]}
        onLayout={(event) => {
          if (!isDraggable) return;
          const measured = event.nativeEvent.layout.height + 10;
          if (measured > 70) rowHeightRef.current = measured;
        }}
        onPress={() => {
          if (isDragging) return;
          if (Date.now() - dragEndedAtRef.current < 220) return;
          openCardPicker(slotKey);
        }}
      >
        <View style={styles.rowHeader}>
          <View style={styles.orderPill}>
            <Text style={styles.orderText}>
              {isLineupSlot ? orderIndex + 1 : slotMeta.label}
            </Text>
          </View>

          {isLineupSlot ? (
            <Text style={styles.slotLabel}>{slotMeta.label}</Text>
          ) : (
            <View style={styles.rowHeaderSpacer} />
          )}

          {isDraggable ? (
            <View style={styles.dragHandle} {...dragResponders[slotKey as BattingLineupSlotKey].panHandlers}>
              <Ionicons name="reorder-three" size={20} color={theme.colors.muted} />
            </View>
          ) : null}

          {card ? (
            <TouchableOpacity
              onPress={(event) => {
                event.stopPropagation();
                clearSlot(slotKey);
              }}
              hitSlop={{ top: 8, right: 8, bottom: 8, left: 8 }}
            >
              <Ionicons name="close-circle" size={18} color={theme.colors.muted} />
            </TouchableOpacity>
          ) : (
            <Ionicons name="add-circle-outline" size={18} color={theme.colors.muted} />
          )}
        </View>

        {card ? (
          <View style={styles.cardBody}>
            <Image source={{ uri: card.baked_img }} style={styles.cardImage} />
            <View style={styles.cardMeta}>
              <Text style={styles.cardName} numberOfLines={1}>
                {card.name} · {getHandText(card, slotMeta.mode)}
              </Text>
              <Text style={styles.cardDetail} numberOfLines={1}>
                {card.team_short_name} · {getPrimaryPosition(card)}
              </Text>
              <Text style={styles.attrText} numberOfLines={1}>
                {getAttributesForMode(card, slotMeta.mode).join("-")}
              </Text>
              <View style={styles.metricRow}>
                <View style={[styles.metricPill, styles.metricPillCurrent]}>
                  <Text style={styles.metricLabel}>OVR</Text>
                  <Text style={styles.metricValue}>{formatMetric(card.ovr)}</Text>
                </View>
                <View style={[styles.metricPill, styles.metricPillTrue]}>
                  <Text style={styles.metricLabel}>TRUE</Text>
                  <Text style={styles.metricValue}>
                    {formatMetric(getSlotMetricValue(card, "true", slotMeta))}
                  </Text>
                </View>
                <View style={[styles.metricPill, styles.metricPillMeta]}>
                  <Text style={styles.metricLabel}>META</Text>
                  <Text style={styles.metricValue}>
                    {formatMetric(getSlotMetricValue(card, "meta", slotMeta))}
                  </Text>
                </View>
                {hasProAccess ? (
                  <View style={[styles.metricPill, styles.metricPillYour]}>
                    <Text style={styles.metricLabel}>YOUR</Text>
                    <Text style={styles.metricValue}>
                      {formatMetric(getSlotMetricValue(card, "your", slotMeta))}
                    </Text>
                  </View>
                ) : null}
              </View>
            </View>
          </View>
        ) : (
          <Text style={styles.emptySlotText}>Tap to add a Year 25 card for this slot.</Text>
        )}
      </TouchableOpacity>
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <ScrollView
        contentContainerStyle={styles.content}
        showsVerticalScrollIndicator={false}
        scrollEnabled={draggingSlot === null}
      >
        <View style={styles.summaryCard}>
          <Text style={styles.summaryTitle}>Team Builder</Text>

          <View style={styles.metricSelectorRow}>
            {VALUE_METRIC_OPTIONS.map((option) => {
              const selected = option.key === teamMetric;
              const locked = isMetricLocked(option.key);
              return (
                <TouchableOpacity
                  key={option.key}
                  style={[
                    styles.metricSelectorChip,
                    locked && styles.metricSelectorChipLocked,
                    selected && !locked && styles.metricSelectorChipActive,
                  ]}
                  onPress={() => {
                    if (locked) return;
                    setTeamMetric(option.key);
                  }}
                  disabled={locked}
                >
                  <View style={styles.metricSelectorChipInner}>
                    <Text
                      numberOfLines={1}
                      style={[
                        styles.metricSelectorChipText,
                        selected && !locked && styles.metricSelectorChipTextActive,
                        locked && styles.metricSelectorChipTextLocked,
                      ]}
                    >
                      {option.label}
                    </Text>
                    {locked ? (
                      <Ionicons name="lock-closed" size={12} color="rgba(148, 163, 184, 0.95)" />
                    ) : null}
                  </View>
                </TouchableOpacity>
              );
            })}
          </View>

          <Text style={styles.summaryLabel}>{metricLabel} Team Avg</Text>
          <Text style={styles.summaryValue}>{averageOverall === null ? "--" : averageOverall.toFixed(1)}</Text>
          <Text style={styles.summarySubtext}>
            {metricLabel} average ({selectedCount}/{totalCount} slots filled)
          </Text>

          <TouchableOpacity
            style={[styles.generateButton, generateDisabled && styles.generateButtonDisabled]}
            onPress={() => void handleGenerateGreedy()}
            disabled={generateDisabled}
            activeOpacity={0.85}
          >
            {generateDisabled ? (
              <ActivityIndicator size="small" color="white" />
            ) : (
              <Ionicons name="flash" size={14} color="white" />
            )}
            <Text style={styles.generateButtonText}>
              {generating ? "Generating..." : "Generate Best Team"}
            </Text>
          </TouchableOpacity>
        </View>

        <View style={styles.modeToggleContainer}>
          <View style={styles.modeToggleRow}>
            {MODE_OPTIONS.map((option) => {
              const selected = option.key === mode;
              return (
                <TouchableOpacity
                  key={option.key}
                  style={[styles.modeChip, selected && styles.modeChipActive]}
                  onPress={() => setMode(option.key)}
                >
                  <Text style={[styles.modeChipText, selected && styles.modeChipTextActive]}>
                    {option.label}
                  </Text>
                </TouchableOpacity>
              );
            })}
          </View>
        </View>

        {mode === "batters" ? (
          <>
            <Text style={styles.sectionTitle}>Batting Lineup</Text>
            <Text style={styles.sectionHint}>Drag lineup rows by handle to set batting order.</Text>
            <View style={styles.lineupList}>
              {battingOrder.map((slotKey, index) => renderRosterRow(slotKey, index))}
            </View>

            <Text style={styles.sectionTitle}>Bench</Text>
            <View style={styles.lineupList}>
              {BENCH_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}
            </View>
          </>
        ) : (
          <>
            <Text style={styles.sectionTitle}>Starting Rotation</Text>
            <View style={styles.lineupList}>
              {ROTATION_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}
            </View>

            <Text style={styles.sectionTitle}>Bullpen</Text>
            <View style={styles.lineupList}>
              {BULLPEN_SLOT_KEYS.map((slotKey) => renderRosterRow(slotKey))}
            </View>
          </>
        )}
      </ScrollView>

      <Modal
        visible={activeSlotMeta !== null}
        transparent
        animationType="slide"
        onRequestClose={() => setActiveSlot(null)}
      >
        <View style={styles.modalWrap}>
          <Pressable style={StyleSheet.absoluteFill} onPress={() => setActiveSlot(null)} />
          <View style={styles.modalSheet}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>Choose Card for {activeSlotLabel}</Text>
              <TouchableOpacity onPress={() => setActiveSlot(null)}>
                <Ionicons name="close" size={22} color="white" />
              </TouchableOpacity>
            </View>

            <View style={styles.searchBox}>
              <Ionicons name="search" size={16} color={theme.colors.muted} />
              <TextInput
                style={styles.searchInput}
                placeholder="Search Year 25 cards..."
                placeholderTextColor={theme.colors.muted}
                value={searchText}
                onChangeText={setSearchText}
                autoCapitalize="words"
              />
            </View>

            {activeSlotMeta && isSlotRequiringPositionFilter(activeSlotMeta) ? (
              <View style={styles.filterModeRow}>
                {POSITION_FILTER_OPTIONS.map((option) => {
                  const selected = positionFilterMode === option.key;
                  return (
                    <TouchableOpacity
                      key={option.key}
                      style={[styles.filterModeChip, selected && styles.filterModeChipActive]}
                      onPress={() => setPositionFilterMode(option.key)}
                    >
                      <Ionicons
                        name={selected ? "checkmark-circle" : "ellipse-outline"}
                        size={13}
                        color={selected ? "#fbbf24" : theme.colors.muted}
                      />
                      <Text style={[styles.filterModeText, selected && styles.filterModeTextActive]}>
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            ) : null}

            <Text style={styles.filterDescription}>{filterDescription}</Text>

            <View style={styles.sortRow}>
              <Text style={styles.sortLabel}>Sort by:</Text>
              {VALUE_METRIC_OPTIONS.map((option) => {
                const selected = option.key === sortMetric;
                const locked = isMetricLocked(option.key);
                return (
                  <TouchableOpacity
                    key={option.key}
                    style={[
                      styles.sortChip,
                      locked && styles.sortChipLocked,
                      selected && !locked && styles.sortChipActive,
                    ]}
                    onPress={() => {
                      if (locked) return;
                      setSortMetric(option.key);
                    }}
                    disabled={locked}
                  >
                    <View style={styles.sortChipInner}>
                      <Text
                        numberOfLines={1}
                        style={[
                          styles.sortChipText,
                          selected && !locked && styles.sortChipTextActive,
                          locked && styles.sortChipTextLocked,
                        ]}
                      >
                        {option.chip}
                      </Text>
                      {locked ? (
                        <Ionicons name="lock-closed" size={10} color="rgba(148, 163, 184, 0.95)" />
                      ) : null}
                    </View>
                  </TouchableOpacity>
                );
              })}
            </View>

            {loading ? (
              <View style={styles.statusWrap}>
                <ActivityIndicator size="small" color="#fbbf24" />
                <Text style={styles.statusText}>Loading cards...</Text>
              </View>
            ) : error ? (
              <Text style={styles.errorText}>{error}</Text>
            ) : (
              <FlatList
                data={sortedFilteredResults}
                keyExtractor={(item) => item.id}
                showsVerticalScrollIndicator={false}
                contentContainerStyle={styles.resultList}
                ListEmptyComponent={<Text style={styles.statusText}>No cards found.</Text>}
                renderItem={({ item }) => (
                  <TouchableOpacity
                    style={styles.resultRow}
                    onPress={() => assignCardToSlot(item)}
                    activeOpacity={0.8}
                  >
                    <Image source={{ uri: item.baked_img }} style={styles.resultImage} />
                    <View style={styles.resultMeta}>
                      <Text style={styles.resultName} numberOfLines={1}>
                        {item.name} · {getHandText(item, activeSlotMeta?.mode ?? mode)}
                      </Text>
                      <Text style={styles.resultSubtext} numberOfLines={1}>
                        {item.team_short_name} · {getPrimaryPosition(item)} · Year {item.year}
                      </Text>
                      <Text style={styles.resultAttrText} numberOfLines={1}>
                        {getAttributesForMode(item, activeSlotMeta?.mode ?? mode).join("-")}
                      </Text>
                      <View style={styles.resultMetricRow}>
                        <Text style={styles.resultMetricText}>OVR {formatMetric(item.ovr)}</Text>
                        <Text style={styles.resultMetricText}>
                          TRUE {formatMetric(activeSlotMeta ? getSlotMetricValue(item, "true", activeSlotMeta) : getTrueOverallValue(item))}
                        </Text>
                        <Text style={styles.resultMetricText}>
                          META {formatMetric(activeSlotMeta ? getSlotMetricValue(item, "meta", activeSlotMeta) : getMetaOverallValue(item))}
                        </Text>
                        {hasProAccess ? (
                          <Text style={styles.resultMetricText}>
                            YOUR{" "}
                            {formatMetric(
                              activeSlotMeta
                                ? getSlotMetricValue(item, "your", activeSlotMeta)
                                : getMetricValueForPosition(item, "your", getPrimaryPosition(item))
                            )}
                          </Text>
                        ) : null}
                      </View>
                    </View>
                  </TouchableOpacity>
                )}
              />
            )}
          </View>
        </View>
      </Modal>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  backgroundLayer: {
    ...StyleSheet.absoluteFillObject,
    zIndex: -1,
  },
  content: {
    paddingHorizontal: 16,
    paddingTop: 20,
    paddingBottom: 140,
    gap: 12,
  },
  summaryCard: {
    borderRadius: 16,
    paddingHorizontal: 16,
    paddingVertical: 18,
    backgroundColor: "rgba(15,23,42,0.84)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
  },
  summaryTitle: {
    color: "white",
    fontSize: 18,
    fontWeight: "800",
    marginBottom: 10,
  },
  modeToggleContainer: {
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.1)",
    backgroundColor: "rgba(15,23,42,0.6)",
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  modeToggleRow: {
    flexDirection: "row",
    gap: 8,
  },
  modeChip: {
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.18)",
    backgroundColor: "rgba(255,255,255,0.04)",
    paddingHorizontal: 12,
    paddingVertical: 7,
  },
  modeChipActive: {
    borderColor: "rgba(74,222,128,0.65)",
    backgroundColor: "rgba(74,222,128,0.18)",
  },
  modeChipText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "700",
  },
  modeChipTextActive: {
    color: "white",
  },
  metricSelectorRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginBottom: 10,
  },
  metricSelectorChip: {
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.18)",
    backgroundColor: "rgba(255,255,255,0.04)",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  metricSelectorChipInner: {
    flexDirection: "row",
    alignItems: "center",
    gap: 5,
    flexWrap: "nowrap",
  },
  metricSelectorChipActive: {
    borderColor: "rgba(59,130,246,0.75)",
    backgroundColor: "rgba(59,130,246,0.18)",
  },
  metricSelectorChipLocked: {
    borderColor: "rgba(148,163,184,0.35)",
    backgroundColor: "rgba(15,23,42,0.58)",
  },
  metricSelectorChipText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "700",
    flexShrink: 0,
  },
  metricSelectorChipTextActive: {
    color: "white",
  },
  metricSelectorChipTextLocked: {
    color: "rgba(148,163,184,0.95)",
  },
  summaryLabel: {
    color: "rgba(255,255,255,0.82)",
    fontSize: 13,
    fontWeight: "600",
    letterSpacing: 0.4,
    textTransform: "uppercase",
    marginBottom: 6,
  },
  summaryValue: {
    color: "white",
    fontSize: 36,
    fontWeight: "800",
    lineHeight: 40,
  },
  summarySubtext: {
    marginTop: 4,
    color: theme.colors.muted,
    fontSize: 13,
  },
  generateButton: {
    marginTop: 12,
    borderRadius: 11,
    borderWidth: 1,
    borderColor: "rgba(251,191,36,0.45)",
    backgroundColor: "rgba(251,191,36,0.2)",
    paddingVertical: 9,
    paddingHorizontal: 12,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    gap: 7,
  },
  generateButtonDisabled: {
    opacity: 0.65,
  },
  generateButtonText: {
    color: "white",
    fontSize: 13,
    fontWeight: "800",
  },
  sectionTitle: {
    color: "white",
    fontSize: 18,
    fontWeight: "700",
    marginTop: 8,
  },
  sectionHint: {
    color: theme.colors.muted,
    fontSize: 12,
    marginTop: 2,
    marginBottom: 2,
  },
  lineupList: {
    gap: 10,
  },
  rowCard: {
    borderRadius: 14,
    backgroundColor: "rgba(15,23,42,0.75)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.1)",
    padding: 10,
    minHeight: 100,
  },
  rowCardActive: {
    borderColor: "#3b82f6",
    shadowColor: "#3b82f6",
    shadowOpacity: 0.22,
    shadowRadius: 8,
  },
  rowCardDragging: {
    borderColor: "#fbbf24",
    backgroundColor: "rgba(30, 41, 59, 0.94)",
    opacity: 0.9,
  },
  rowHeader: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    marginBottom: 8,
  },
  rowHeaderSpacer: {
    flex: 1,
  },
  orderPill: {
    minWidth: 28,
    height: 24,
    borderRadius: 12,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(59,130,246,0.2)",
    borderWidth: 1,
    borderColor: "rgba(59,130,246,0.6)",
    paddingHorizontal: 7,
  },
  orderText: {
    color: "#93c5fd",
    fontSize: 11,
    fontWeight: "800",
  },
  slotLabel: {
    color: "white",
    fontSize: 14,
    fontWeight: "700",
    marginRight: "auto",
  },
  dragHandle: {
    width: 30,
    height: 26,
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 8,
    backgroundColor: "rgba(255,255,255,0.03)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.08)",
  },
  cardBody: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  cardImage: {
    width: 52,
    height: 72,
    borderRadius: 8,
    backgroundColor: "rgba(255,255,255,0.06)",
  },
  cardMeta: {
    flex: 1,
    gap: 4,
  },
  cardName: {
    color: "white",
    fontSize: 14,
    fontWeight: "700",
  },
  cardDetail: {
    color: theme.colors.muted,
    fontSize: 12,
  },
  attrText: {
    color: "rgba(226,232,240,0.9)",
    fontSize: 9,
    lineHeight: 11,
    letterSpacing: -0.1,
  },
  metricRow: {
    flexDirection: "row",
    gap: 6,
    marginTop: 2,
    flexWrap: "wrap",
  },
  metricPill: {
    borderRadius: 999,
    paddingHorizontal: 8,
    paddingVertical: 3,
    borderWidth: 1,
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
  },
  metricPillCurrent: {
    borderColor: "rgba(74,222,128,0.6)",
    backgroundColor: "rgba(74,222,128,0.15)",
  },
  metricPillTrue: {
    borderColor: "rgba(59,130,246,0.6)",
    backgroundColor: "rgba(59,130,246,0.15)",
  },
  metricPillMeta: {
    borderColor: "rgba(251,191,36,0.6)",
    backgroundColor: "rgba(251,191,36,0.15)",
  },
  metricPillYour: {
    borderColor: "rgba(20,184,166,0.6)",
    backgroundColor: "rgba(20,184,166,0.16)",
  },
  metricLabel: {
    color: "rgba(248,250,252,0.9)",
    fontSize: 10,
    fontWeight: "800",
  },
  metricValue: {
    color: "white",
    fontSize: 10,
    fontWeight: "800",
  },
  emptySlotText: {
    color: theme.colors.muted,
    fontSize: 12,
    lineHeight: 17,
  },
  modalWrap: {
    flex: 1,
    justifyContent: "flex-end",
    backgroundColor: "rgba(0,0,0,0.25)",
  },
  modalSheet: {
    maxHeight: "82%",
    borderTopLeftRadius: 18,
    borderTopRightRadius: 18,
    paddingHorizontal: 16,
    paddingTop: 14,
    paddingBottom: 24,
    backgroundColor: "#0b1324",
    borderTopWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
  },
  modalHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: 12,
  },
  modalTitle: {
    color: "white",
    fontSize: 17,
    fontWeight: "700",
    flex: 1,
    marginRight: 8,
  },
  searchBox: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    height: 44,
    borderRadius: 11,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.13)",
    backgroundColor: "rgba(255,255,255,0.04)",
    paddingHorizontal: 12,
    marginBottom: 10,
  },
  searchInput: {
    flex: 1,
    color: "white",
    fontSize: 15,
    fontWeight: "500",
  },
  filterModeRow: {
    flexDirection: "row",
    gap: 6,
    marginBottom: 4,
  },
  filterModeChip: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    paddingVertical: 7,
    paddingHorizontal: 8,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    backgroundColor: "rgba(255,255,255,0.03)",
  },
  filterModeChipActive: {
    borderColor: "rgba(251,191,36,0.6)",
    backgroundColor: "rgba(251,191,36,0.1)",
  },
  filterModeText: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "600",
  },
  filterModeTextActive: {
    color: "#fde68a",
  },
  filterDescription: {
    color: theme.colors.muted,
    fontSize: 12,
    marginBottom: 8,
  },
  sortRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    marginBottom: 6,
  },
  sortLabel: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "600",
  },
  sortChip: {
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    backgroundColor: "rgba(255,255,255,0.03)",
    paddingHorizontal: 9,
    paddingVertical: 5,
  },
  sortChipInner: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
    flexWrap: "nowrap",
  },
  sortChipActive: {
    borderColor: "rgba(251,191,36,0.6)",
    backgroundColor: "rgba(251,191,36,0.12)",
  },
  sortChipLocked: {
    borderColor: "rgba(148,163,184,0.35)",
    backgroundColor: "rgba(15,23,42,0.58)",
  },
  sortChipText: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: "800",
    flexShrink: 0,
  },
  sortChipTextActive: {
    color: "#fde68a",
  },
  sortChipTextLocked: {
    color: "rgba(148,163,184,0.95)",
  },
  statusWrap: {
    paddingVertical: 26,
    alignItems: "center",
    gap: 10,
  },
  statusText: {
    color: theme.colors.muted,
    fontSize: 14,
    textAlign: "center",
    marginTop: 18,
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 14,
    marginTop: 8,
    textAlign: "center",
  },
  resultList: {
    paddingBottom: 8,
    gap: 8,
  },
  resultRow: {
    flexDirection: "row",
    alignItems: "center",
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.1)",
    backgroundColor: "rgba(255,255,255,0.03)",
    padding: 10,
    gap: 10,
  },
  resultImage: {
    width: 42,
    height: 58,
    borderRadius: 6,
    backgroundColor: "rgba(255,255,255,0.06)",
  },
  resultMeta: {
    flex: 1,
    gap: 2,
  },
  resultName: {
    color: "white",
    fontSize: 14,
    fontWeight: "700",
  },
  resultSubtext: {
    color: theme.colors.muted,
    fontSize: 12,
  },
  resultAttrText: {
    color: "rgba(226,232,240,0.9)",
    fontSize: 9,
    lineHeight: 11,
    letterSpacing: -0.1,
    marginTop: 2,
  },
  resultMetricRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: 8,
    marginTop: 4,
  },
  resultMetricText: {
    color: "#fde68a",
    fontSize: 10,
    fontWeight: "800",
  },
});
