export type TeamCard = {
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

export type RosterMode = "batters" | "pitchers";
export type PositionFilterMode = "primary" | "secondary" | "all";
export type ValueMetric = "ovr" | "true" | "meta" | "your";
export type SlotSection = "lineup" | "bench" | "rotation" | "bullpen";

export const BATTING_LINEUP_SLOT_KEYS = ["c", "1b", "2b", "3b", "ss", "lf", "cf", "rf", "dh"] as const;
export const BENCH_SLOT_KEYS = ["bn1", "bn2", "bn3", "bn4"] as const;
export const ROTATION_SLOT_KEYS = ["sp1", "sp2", "sp3", "sp4", "sp5"] as const;
export const BULLPEN_SLOT_KEYS = ["bp1", "bp2", "bp3", "bp4", "bp5", "bp6", "bp7", "bp8"] as const;

export const ALL_SLOT_KEYS = [
  ...BATTING_LINEUP_SLOT_KEYS,
  ...BENCH_SLOT_KEYS,
  ...ROTATION_SLOT_KEYS,
  ...BULLPEN_SLOT_KEYS,
] as const;

export type BattingLineupSlotKey = (typeof BATTING_LINEUP_SLOT_KEYS)[number];
export type AnySlotKey = (typeof ALL_SLOT_KEYS)[number];

export type SlotMeta = {
  key: AnySlotKey;
  label: string;
  mode: RosterMode;
  section: SlotSection;
  targetPosition: string;
  draggable: boolean;
};

export const SLOT_META: Record<AnySlotKey, SlotMeta> = {
  c: { key: "c", label: "C", mode: "batters", section: "lineup", targetPosition: "C", draggable: true },
  "1b": { key: "1b", label: "1B", mode: "batters", section: "lineup", targetPosition: "1B", draggable: true },
  "2b": { key: "2b", label: "2B", mode: "batters", section: "lineup", targetPosition: "2B", draggable: true },
  "3b": { key: "3b", label: "3B", mode: "batters", section: "lineup", targetPosition: "3B", draggable: true },
  ss: { key: "ss", label: "SS", mode: "batters", section: "lineup", targetPosition: "SS", draggable: true },
  lf: { key: "lf", label: "LF", mode: "batters", section: "lineup", targetPosition: "LF", draggable: true },
  cf: { key: "cf", label: "CF", mode: "batters", section: "lineup", targetPosition: "CF", draggable: true },
  rf: { key: "rf", label: "RF", mode: "batters", section: "lineup", targetPosition: "RF", draggable: true },
  dh: { key: "dh", label: "DH", mode: "batters", section: "lineup", targetPosition: "DH", draggable: true },
  bn1: { key: "bn1", label: "BN1", mode: "batters", section: "bench", targetPosition: "BENCH", draggable: false },
  bn2: { key: "bn2", label: "BN2", mode: "batters", section: "bench", targetPosition: "BENCH", draggable: false },
  bn3: { key: "bn3", label: "BN3", mode: "batters", section: "bench", targetPosition: "BENCH", draggable: false },
  bn4: { key: "bn4", label: "BN4", mode: "batters", section: "bench", targetPosition: "BENCH", draggable: false },
  sp1: { key: "sp1", label: "SP1", mode: "pitchers", section: "rotation", targetPosition: "SP", draggable: false },
  sp2: { key: "sp2", label: "SP2", mode: "pitchers", section: "rotation", targetPosition: "SP", draggable: false },
  sp3: { key: "sp3", label: "SP3", mode: "pitchers", section: "rotation", targetPosition: "SP", draggable: false },
  sp4: { key: "sp4", label: "SP4", mode: "pitchers", section: "rotation", targetPosition: "SP", draggable: false },
  sp5: { key: "sp5", label: "SP5", mode: "pitchers", section: "rotation", targetPosition: "SP", draggable: false },
  bp1: { key: "bp1", label: "BP1", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp2: { key: "bp2", label: "BP2", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp3: { key: "bp3", label: "BP3", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp4: { key: "bp4", label: "BP4", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp5: { key: "bp5", label: "BP5", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp6: { key: "bp6", label: "BP6", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp7: { key: "bp7", label: "BP7", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
  bp8: { key: "bp8", label: "BP8", mode: "pitchers", section: "bullpen", targetPosition: "BP", draggable: false },
};

export const POSITION_FILTER_OPTIONS = [
  { key: "primary", label: "Primary Only" },
  { key: "secondary", label: "Include Secondary" },
  { key: "all", label: "All Cards" },
] as const;

export const VALUE_METRIC_OPTIONS = [
  { key: "ovr", label: "Overall", chip: "OVR" },
  { key: "true", label: "True Overall", chip: "TRUE" },
  { key: "meta", label: "Meta Overall", chip: "META" },
  { key: "your", label: "Your Overall", chip: "YOUR" },
] as const;

export const MODE_OPTIONS: { key: RosterMode; label: string }[] = [
  { key: "batters", label: "Batters" },
  { key: "pitchers", label: "Pitchers" },
];

export const EMPTY_ROSTER = ALL_SLOT_KEYS.reduce((acc, key) => {
  acc[key] = null;
  return acc;
}, {} as Record<AnySlotKey, TeamCard | null>);

const normalizePosition = (value: string | null | undefined) => (value ?? "").trim().toUpperCase();

const normalizePlayerName = (value: string | null | undefined) =>
  (value ?? "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

export const normalizeHand = (value: string | null | undefined) => {
  const hand = (value ?? "").trim().toUpperCase();
  if (hand.startsWith("L")) return "L";
  if (hand.startsWith("R")) return "R";
  if (hand.startsWith("S")) return "S";
  return hand || "--";
};

export const getPlayerIdentityKey = (card: TeamCard) => {
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

export const moveItem = <T,>(items: T[], fromIndex: number, toIndex: number) => {
  if (fromIndex === toIndex) {
    return items;
  }
  const next = [...items];
  const [item] = next.splice(fromIndex, 1);
  if (item === undefined) {
    return items;
  }
  next.splice(toIndex, 0, item);
  return next;
};

export const getPrimaryPosition = (card: TeamCard) =>
  normalizePosition(card.display_primary_position ?? card.display_position);

export const getSecondaryPositions = (card: TeamCard) =>
  splitPositions(card.display_secondary_positions ?? card.display_seconday_position);

export const formatMetric = (value: number | null | undefined) =>
  typeof value === "number" && Number.isFinite(value) ? String(Math.round(value)) : "--";

const averagedMetric = (left: number | null | undefined, right: number | null | undefined) => {
  if (typeof left !== "number" || typeof right !== "number") {
    return null;
  }
  return Math.round((left + right) / 2);
};

export const getTrueOverallValue = (card: TeamCard) => card.true_overall ?? card.true_overall_rounded ?? null;
export const getMetaOverallValue = (card: TeamCard) => card.meta_overall ?? card.meta_overall_rounded ?? null;
export const getYourOverallValue = (card: TeamCard) => card.your_overall ?? card.your_overall_rounded ?? null;

const getYourWeightFromCard = (card: TeamCard): number => {
  const meta = getMetaOverallValue(card);
  const your = getYourOverallValue(card);
  if (typeof meta !== "number" || !Number.isFinite(meta) || Math.abs(meta) < 1e-9) {
    return 1;
  }
  if (typeof your !== "number" || !Number.isFinite(your)) {
    return 1;
  }
  return your / meta;
};

const getPositionMapValue = (map: Record<string, number> | null | undefined, position: string | null | undefined) => {
  if (!map) {
    return null;
  }
  const key = normalizePosition(position);
  const value = map[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
};

export const getMetricValueForPosition = (
  card: TeamCard,
  metric: ValueMetric,
  position: string | null | undefined,
): number | null => {
  if (metric === "ovr") {
    return card.ovr;
  }
  if (metric === "true") {
    return getPositionMapValue(card.true_overall_by_position, position) ?? getTrueOverallValue(card);
  }
  if (metric === "meta") {
    return getPositionMapValue(card.meta_overall_by_position, position) ?? getMetaOverallValue(card);
  }

  const yourFromMap = getPositionMapValue(card.your_overall_by_position, position);
  if (yourFromMap !== null) {
    return yourFromMap;
  }
  const metaFromMap = getPositionMapValue(card.meta_overall_by_position, position);
  if (metaFromMap !== null) {
    return metaFromMap * getYourWeightFromCard(card);
  }
  return getYourOverallValue(card);
};

export const getSlotMetricValue = (card: TeamCard, metric: ValueMetric, slotMeta: SlotMeta): number | null => {
  if (slotMeta.targetPosition === "BENCH") {
    return getMetricValueForPosition(card, metric, getPrimaryPosition(card));
  }

  if (slotMeta.targetPosition === "BP") {
    const rp = getMetricValueForPosition(card, metric, "RP");
    const cp = getMetricValueForPosition(card, metric, "CP");
    const bullpenBest = [rp, cp].filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    if (bullpenBest.length > 0) {
      return Math.max(...bullpenBest);
    }
    return getMetricValueForPosition(card, metric, getPrimaryPosition(card));
  }

  return getMetricValueForPosition(card, metric, slotMeta.targetPosition);
};

export const compareCardsForSlot = (a: TeamCard, b: TeamCard, metric: ValueMetric, slotMeta: SlotMeta) => {
  const metricDiff = (getSlotMetricValue(b, metric, slotMeta) ?? -1) - (getSlotMetricValue(a, metric, slotMeta) ?? -1);
  if (metricDiff !== 0) {
    return metricDiff;
  }
  const ovrDiff = b.ovr - a.ovr;
  if (ovrDiff !== 0) {
    return ovrDiff;
  }
  return a.name.localeCompare(b.name);
};

const matchesPositionSet = (card: TeamCard, allowed: Set<string>, filterMode: PositionFilterMode) => {
  if (filterMode === "all") {
    return true;
  }
  const primary = getPrimaryPosition(card);
  if (allowed.has(primary)) {
    return true;
  }
  if (filterMode === "primary") {
    return false;
  }
  const secondary = getSecondaryPositions(card);
  return secondary.some((position) => allowed.has(position));
};

export const cardMatchesSlot = (card: TeamCard, slotMeta: SlotMeta, filterMode: PositionFilterMode) => {
  const wantsHitter = slotMeta.mode === "batters";
  if (wantsHitter && card.is_hitter === false) {
    return false;
  }
  if (!wantsHitter && card.is_hitter !== false) {
    return false;
  }

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

export const getHandText = (card: TeamCard, mode: RosterMode) => {
  if (mode === "pitchers") {
    return normalizeHand(card.throw_hand);
  }
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

export const getAttributesForMode = (card: TeamCard, mode: RosterMode) => {
  if (mode === "pitchers") {
    return getPitcherAttributes(card);
  }
  return getHitterAttributes(card);
};

export const getModeSlotKeys = (mode: RosterMode): AnySlotKey[] => {
  if (mode === "batters") {
    return [...BATTING_LINEUP_SLOT_KEYS, ...BENCH_SLOT_KEYS];
  }
  return [...ROTATION_SLOT_KEYS, ...BULLPEN_SLOT_KEYS];
};

type LineupEntry = { slotKey: BattingLineupSlotKey; card: TeamCard };

const DEFENSIVE_FILL_ORDER: BattingLineupSlotKey[] = ["c", "ss", "cf", "2b", "3b", "lf", "rf", "1b", "dh"];
const OUTFIELD_POSITIONS = new Set(["LF", "CF", "RF"]);
const INFIELD_POSITIONS = new Set(["1B", "2B", "3B", "SS"]);

const toStatValue = (value: number | null | undefined) => (typeof value === "number" && Number.isFinite(value) ? value : 0);
const getHitterMetricValue = (card: TeamCard, metric: ValueMetric) => getMetricValueForPosition(card, metric, getPrimaryPosition(card)) ?? card.ovr;
const getContactAverage = (card: TeamCard) => averagedMetric(card.contact_left, card.contact_right) ?? 0;
const getPowerAverage = (card: TeamCard) => averagedMetric(card.power_left, card.power_right) ?? 0;
const getHitterHand = (card: TeamCard) => normalizeHand(card.bat_hand);

const getLineupAdjacencyScore = (leftCard: TeamCard, rightCard: TeamCard) => {
  const leftHand = getHitterHand(leftCard);
  const rightHand = getHitterHand(rightCard);
  if (leftHand === "S" || rightHand === "S") {
    return 10;
  }
  if (leftHand === "--" || rightHand === "--") {
    return 0;
  }
  if (leftHand === rightHand) {
    return -44;
  }
  return 14;
};

const getLineupSpotScore = (card: TeamCard, metric: ValueMetric, spotIndex: number) => {
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
    return metricValue * 0.33 + contact * 0.24 + speed * 0.2 + power * 0.1 + vision * 0.13 + switchBonus * 1.75;
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

const evaluateLineupSequence = (entries: LineupEntry[], metric: ValueMetric) => {
  let score = 0;
  for (let index = 0; index < entries.length; index += 1) {
    const current = entries[index];
    if (!current) {
      continue;
    }
    score += getLineupSpotScore(current.card, metric, index);

    if (index === 0) {
      continue;
    }

    const previous = entries[index - 1];
    if (!previous) {
      continue;
    }
    score += getLineupAdjacencyScore(previous.card, current.card);

    if (index >= 2) {
      const handA = getHitterHand(entries[index - 2].card);
      const handB = getHitterHand(previous.card);
      const handC = getHitterHand(current.card);
      const isSameHandRun = handA !== "S" && handA !== "--" && handA === handB && handB === handC;
      if (isSameHandRun) {
        score -= 26;
      }
    }
  }
  return score;
};

const buildPermutations = <T,>(items: T[]): T[][] => {
  if (items.length <= 1) {
    return [items];
  }
  const permutations: T[][] = [];
  for (let index = 0; index < items.length; index += 1) {
    const current = items[index];
    if (current === undefined) {
      continue;
    }
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
    if (!candidate) {
      continue;
    }
    const score = getScore(candidate);
    if (score > bestScore) {
      bestScore = score;
      bestIndex = index;
    }
  }

  return bestIndex;
};

const optimizeBattingOrderFromGeneratedLineup = (generated: Partial<Record<AnySlotKey, TeamCard | null>>, metric: ValueMetric) => {
  const lineupEntries: LineupEntry[] = BATTING_LINEUP_SLOT_KEYS.map((slotKey) => {
    const card = generated[slotKey];
    return card ? { slotKey, card } : null;
  }).filter((entry): entry is LineupEntry => entry !== null);

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
    if (!third) {
      return base;
    }
    return base + getLineupAdjacencyScore(entry.card, third.card);
  });
  const second = secondIndex >= 0 ? remaining.splice(secondIndex, 1)[0] : null;
  const firstIndex = pickBestIndex(remaining, (entry) => {
    const base = getLineupSpotScore(entry.card, metric, 0);
    if (!second) {
      return base;
    }
    return base + getLineupAdjacencyScore(entry.card, second.card);
  });
  const first = firstIndex >= 0 ? remaining.splice(firstIndex, 1)[0] : null;
  const fourthIndex = pickBestIndex(remaining, (entry) => {
    const base = getLineupSpotScore(entry.card, metric, 3);
    if (!third) {
      return base;
    }
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
  if (primary === "C") {
    return true;
  }
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
  const utilityScore = toStatValue(card.fielding_ability) * 0.56 + getVersatilityCount(card) * 9 + speedScore * 0.24;

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

const selectBenchCardsWithVariety = (candidates: TeamCard[], metric: ValueMetric) => {
  if (candidates.length <= BENCH_SLOT_KEYS.length) {
    return [...candidates];
  }

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
    if (seenPlayers.has(candidate.playerKey)) {
      continue;
    }
    seenPlayers.add(candidate.playerKey);
    scoredCandidates.push(candidate);
  }

  const shortlist = scoredCandidates.slice(0, 36);
  if (shortlist.length <= BENCH_SLOT_KEYS.length) {
    return shortlist.map((item) => item.card);
  }

  const catchers = shortlist.filter((item) => item.canCatch);
  const outfielders = shortlist.filter((item) => item.canOF && !item.canCatch);
  const infielders = shortlist.filter((item) => item.canINF && !item.canCatch);
  const extras = shortlist.filter((item) => !item.canCatch);

  if (catchers.length === 0 || outfielders.length === 0 || infielders.length === 0 || extras.length === 0) {
    const fallback = shortlist
      .filter((item) => !item.canCatch)
      .sort((a, b) => b.shortlistScore - a.shortlistScore)
      .slice(0, BENCH_SLOT_KEYS.length - 1)
      .map((item) => item.card);

    const catcher = catchers[0]?.card;
    if (catcher) {
      return [catcher, ...fallback].slice(0, BENCH_SLOT_KEYS.length);
    }
    return shortlist.slice(0, BENCH_SLOT_KEYS.length).map((item) => item.card);
  }

  let bestSelection:
    | [typeof shortlist[number], typeof shortlist[number], typeof shortlist[number], typeof shortlist[number]]
    | null = null;
  let bestScore = Number.NEGATIVE_INFINITY;
  let bestKillerClutch = Number.NEGATIVE_INFINITY;
  let bestAverageClutch = Number.NEGATIVE_INFINITY;

  for (const catcher of catchers) {
    for (const outfielder of outfielders) {
      if (outfielder.playerKey === catcher.playerKey) {
        continue;
      }
      for (const infielder of infielders) {
        if (infielder.playerKey === catcher.playerKey || infielder.playerKey === outfielder.playerKey) {
          continue;
        }
        for (const extra of extras) {
          const keys = new Set([catcher.playerKey, outfielder.playerKey, infielder.playerKey, extra.playerKey]);
          if (keys.size !== 4) {
            continue;
          }

          const selection = [catcher, outfielder, infielder, extra] as const;
          const catcherCount = selection.filter((item) => item.canCatch).length;
          if (catcherCount !== 1) {
            continue;
          }

          const leftyKiller = selection.reduce((best, current) =>
            current.role.leftySplit > best.role.leftySplit ? current : best,
          );
          const rightyKiller = selection.reduce((best, current) =>
            current.role.rightySplit > best.role.rightySplit ? current : best,
          );

          const leftyMax = leftyKiller.role.leftySplit;
          const rightyMax = rightyKiller.role.rightySplit;
          const speedMax = Math.max(...selection.map((item) => item.role.speedScore));
          const utilityMax = Math.max(...selection.map((item) => item.role.utilityScore));
          const avgMetric = selection.reduce((sum, item) => sum + item.role.metricValue, 0) / selection.length;

          const catcherQuality =
            catcher.role.metricValue * 0.35 + catcher.role.utilityScore * 0.4 + catcher.role.clutchScore * 0.25;
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
          const averageClutch = selection.reduce((sum, item) => sum + item.role.clutchScore, 0) / selection.length;

          const betterScore = comboScore > bestScore + 1e-6;
          const tiedScore = Math.abs(comboScore - bestScore) <= 1e-6;
          const betterKillerClutch = killerClutchSum > bestKillerClutch + 1e-6;
          const tiedKillerClutch = Math.abs(killerClutchSum - bestKillerClutch) <= 1e-6;
          const betterAverageClutch = averageClutch > bestAverageClutch;

          if (betterScore || (tiedScore && betterKillerClutch) || (tiedScore && tiedKillerClutch && betterAverageClutch)) {
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
  if (!fallbackCatcher) {
    return shortlist.slice(0, BENCH_SLOT_KEYS.length).map((item) => item.card);
  }

  const fallbackUsed = new Set([fallbackCatcher.playerKey]);
  const fallbackOrder = [fallbackCatcher.card];
  const pushFirst = (bucket: typeof shortlist) => {
    const candidate = bucket.find((item) => !fallbackUsed.has(item.playerKey) && !item.canCatch);
    if (!candidate) {
      return;
    }
    fallbackUsed.add(candidate.playerKey);
    fallbackOrder.push(candidate.card);
  };

  pushFirst(outfielders);
  pushFirst(infielders);
  pushFirst(extras);

  for (const candidate of extras) {
    if (fallbackOrder.length >= BENCH_SLOT_KEYS.length) {
      break;
    }
    if (fallbackUsed.has(candidate.playerKey)) {
      continue;
    }
    fallbackUsed.add(candidate.playerKey);
    fallbackOrder.push(candidate.card);
  }

  return fallbackOrder.slice(0, BENCH_SLOT_KEYS.length);
};

export const isSlotRequiringPositionFilter = (slotMeta: SlotMeta) => !["BENCH", "DH"].includes(slotMeta.targetPosition);

export const pickBestCardForSlot = (pool: TeamCard[], usedPlayers: Set<string>, slotMeta: SlotMeta, metric: ValueMetric) => {
  let best: TeamCard | null = null;

  for (const card of pool) {
    if (usedPlayers.has(getPlayerIdentityKey(card))) {
      continue;
    }
    if (!cardMatchesSlot(card, slotMeta, "secondary")) {
      continue;
    }
    if (!best || compareCardsForSlot(card, best, metric, slotMeta) < 0) {
      best = card;
    }
  }

  return best;
};

export const generatePitcherRoster = (pool: TeamCard[], usedPlayers: Set<string>, metric: ValueMetric) => {
  const generated = {} as Partial<Record<AnySlotKey, TeamCard | null>>;
  for (const slotKey of [...ROTATION_SLOT_KEYS, ...BULLPEN_SLOT_KEYS]) {
    generated[slotKey] = null;
  }

  const assignSlot = (slotKey: AnySlotKey) => {
    const best = pickBestCardForSlot(pool, usedPlayers, SLOT_META[slotKey], metric);
    if (!best) {
      return;
    }
    generated[slotKey] = best;
    usedPlayers.add(getPlayerIdentityKey(best));
  };

  for (const slotKey of ROTATION_SLOT_KEYS) {
    assignSlot(slotKey);
  }
  for (const slotKey of BULLPEN_SLOT_KEYS) {
    assignSlot(slotKey);
  }
  return generated;
};

export const generateBatterRoster = (pool: TeamCard[], usedPlayers: Set<string>, metric: ValueMetric) => {
  const generated = {} as Partial<Record<AnySlotKey, TeamCard | null>>;
  for (const slotKey of [...BATTING_LINEUP_SLOT_KEYS, ...BENCH_SLOT_KEYS]) {
    generated[slotKey] = null;
  }

  for (const slotKey of DEFENSIVE_FILL_ORDER) {
    const best = pickBestCardForSlot(pool, usedPlayers, SLOT_META[slotKey], metric);
    if (!best) {
      continue;
    }
    generated[slotKey] = best;
    usedPlayers.add(getPlayerIdentityKey(best));
  }

  const optimizedOrder = optimizeBattingOrderFromGeneratedLineup(generated, metric);
  const benchCandidates = pool.filter((card) => !usedPlayers.has(getPlayerIdentityKey(card)));
  const selectedBenchCards = selectBenchCardsWithVariety(benchCandidates, metric);

  for (let index = 0; index < BENCH_SLOT_KEYS.length; index += 1) {
    const slotKey = BENCH_SLOT_KEYS[index];
    const selected = selectedBenchCards[index];
    if (!selected) {
      continue;
    }
    generated[slotKey] = selected;
    usedPlayers.add(getPlayerIdentityKey(selected));
  }

  return { generated, optimizedOrder };
};
