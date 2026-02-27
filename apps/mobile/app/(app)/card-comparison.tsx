import { useEffect, useMemo, useState } from "react";
import {
  ActivityIndicator,
  Image,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";

import { ApiError, apiGet, apiGetAuth } from "../../src/lib/api";
import { theme } from "../../src/theme/colors";

type CardQuirk = {
  name: string;
};

type CardModel = {
  id: string;
  mlb_id?: number | null;
  name: string;
  team_short_name: string;
  ovr: number;
  rarity: string;
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
  age: number;
  bat_hand: string;
  throw_hand: string;
  weight: string;
  height: string;
  is_hitter: boolean;
  series_name: string;
  baked_img?: string | null;
  img?: string | null;
  year: number;
  stamina: number;
  pitching_clutch: number;
  hits_per_bf: number;
  k_per_bf: number;
  bb_per_bf: number;
  hr_per_bf: number;
  pitch_velocity: number;
  pitch_control: number;
  pitch_movement: number;
  contact_left: number;
  contact_right: number;
  power_left: number;
  power_right: number;
  plate_vision: number;
  plate_discipline: number;
  batting_clutch: number;
  bunting_ability: number;
  drag_bunting_ability: number;
  hitting_durability: number;
  fielding_durability: number;
  fielding_ability: number;
  arm_strength: number;
  arm_accuracy: number;
  reaction_time: number;
  blocking: number;
  speed: number;
  baserunning_ability: number;
  baserunning_aggression: number;
  quirks?: CardQuirk[];
};

type FieldKey = keyof CardModel | "your_overall";
type FieldRow = {
  key: FieldKey;
  label: string;
};

type ComparisonSortKey = "meta_overall" | "ovr" | "true_overall" | "your_overall";

const MAX_CARDS = 5;
const COMPARISON_SORT_OPTIONS: { key: ComparisonSortKey; label: string }[] = [
  { key: "meta_overall", label: "Meta Overall" },
  { key: "ovr", label: "OVR" },
  { key: "true_overall", label: "True Overall" },
  { key: "your_overall", label: "Your Overall" },
];

const FIELD_ROWS: FieldRow[] = [
  { key: "team_short_name", label: "Team" },
  { key: "year", label: "Year" },
  { key: "series_name", label: "Series" },
  { key: "rarity", label: "Rarity" },
  { key: "display_position", label: "Display Position" },
  { key: "display_primary_position", label: "Primary Position" },
  { key: "display_secondary_positions", label: "Secondary Positions" },
  { key: "ovr", label: "OVR" },
  { key: "your_overall", label: "Your Overall" },
  { key: "meta_overall", label: "Meta Overall" },
  { key: "meta_overall_rounded", label: "Meta OVR Rounded" },
  { key: "true_overall", label: "True Overall" },
  { key: "true_overall_rounded", label: "True OVR Rounded" },
  { key: "age", label: "Age" },
  { key: "bat_hand", label: "Bat Hand" },
  { key: "throw_hand", label: "Throw Hand" },
  { key: "height", label: "Height" },
  { key: "weight", label: "Weight" },
  { key: "is_hitter", label: "Is Hitter" },
  { key: "contact_left", label: "CON L" },
  { key: "contact_right", label: "CON R" },
  { key: "power_left", label: "POW L" },
  { key: "power_right", label: "POW R" },
  { key: "plate_vision", label: "VIS" },
  { key: "plate_discipline", label: "DISC" },
  { key: "batting_clutch", label: "CLT" },
  { key: "bunting_ability", label: "BUNT" },
  { key: "drag_bunting_ability", label: "DRAG BUNT" },
  { key: "hitting_durability", label: "HIT DUR" },
  { key: "fielding_durability", label: "FLD DUR" },
  { key: "fielding_ability", label: "FLD" },
  { key: "arm_strength", label: "ARM STR" },
  { key: "arm_accuracy", label: "ARM ACC" },
  { key: "reaction_time", label: "REAC" },
  { key: "blocking", label: "BLK" },
  { key: "speed", label: "SPD" },
  { key: "baserunning_ability", label: "BR ABIL" },
  { key: "baserunning_aggression", label: "BR AGG" },
  { key: "stamina", label: "STA" },
  { key: "pitching_clutch", label: "P CLT" },
  { key: "hits_per_bf", label: "H/9" },
  { key: "k_per_bf", label: "K/9" },
  { key: "bb_per_bf", label: "BB/9" },
  { key: "hr_per_bf", label: "HR/9" },
  { key: "pitch_velocity", label: "VEL" },
  { key: "pitch_control", label: "CTRL" },
  { key: "pitch_movement", label: "BRK" },
  { key: "quirks", label: "Quirks" },
];

export default function CardComparisonScreen() {
  const [slots, setSlots] = useState<(CardModel | null)[]>([null, null]);
  const [activeSlotIndex, setActiveSlotIndex] = useState<number | null>(null);
  const [query, setQuery] = useState("");
  const [searchSortKey, setSearchSortKey] = useState<ComparisonSortKey>("meta_overall");
  const [searchResults, setSearchResults] = useState<CardModel[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  useEffect(() => {
    if (activeSlotIndex === null) {
      setSearchResults([]);
      setSearchLoading(false);
      setSearchError(null);
      return;
    }

    const trimmed = query.trim();
    if (trimmed.length < 2) {
      setSearchResults([]);
      setSearchLoading(false);
      setSearchError(null);
      return;
    }

    let cancelled = false;
    const timer = setTimeout(async () => {
      setSearchLoading(true);
      setSearchError(null);
      try {
        const params = new URLSearchParams();
        params.set("name", trimmed);
        params.set("year", "25");
        params.set("limit", "30");
        const payload = await fetchComparisonCards(`/cards/?${params.toString()}`);
        if (!cancelled) {
          setSearchResults(normalizeCardResults(payload));
        }
      } catch (err) {
        if (!cancelled) {
          setSearchResults([]);
          setSearchError(err instanceof Error ? err.message : "Search failed.");
        }
      } finally {
        if (!cancelled) setSearchLoading(false);
      }
    }, 220);

    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
  }, [query, activeSlotIndex]);

  const selectedCards = useMemo(
    () => slots.filter((slot): slot is CardModel => slot !== null),
    [slots]
  );

  const visibleResults = useMemo(() => {
    if (activeSlotIndex === null) return [];

    const currentSlotId = slots[activeSlotIndex]?.id ?? null;
    const selectedIds = new Set(
      slots
        .map((slot, index) => (index === activeSlotIndex ? null : slot?.id ?? null))
        .filter((id): id is string => id !== null)
    );

    return searchResults.filter((card) => {
      if (selectedIds.has(card.id)) return false;
      if (currentSlotId && currentSlotId === card.id) return true;
      return true;
    })
      .sort((a, b) => {
        const aPrimary = getSortMetric(a, searchSortKey);
        const bPrimary = getSortMetric(b, searchSortKey);
        if (aPrimary !== bPrimary) return bPrimary - aPrimary;

        const aSecondary = getSortMetric(a, "ovr");
        const bSecondary = getSortMetric(b, "ovr");
        if (aSecondary !== bSecondary) return bSecondary - aSecondary;

        return a.name.localeCompare(b.name);
      });
  }, [searchResults, slots, activeSlotIndex, searchSortKey]);

  const rowMaxValue = useMemo(() => {
    const out = new Map<string, number>();
    FIELD_ROWS.forEach((row) => {
      const numericValues = selectedCards
        .map((card) => numericFromValue(getRowRawValue(card, row.key)))
        .filter((value): value is number => value !== null);
      if (numericValues.length > 1) out.set(String(row.key), Math.max(...numericValues));
    });
    return out;
  }, [selectedCards]);

  const openPicker = (index: number) => {
    setActiveSlotIndex(index);
    setQuery("");
  };

  const closePicker = () => {
    setActiveSlotIndex(null);
    setQuery("");
    setSearchResults([]);
    setSearchLoading(false);
    setSearchError(null);
  };

  const pickCardForActiveSlot = (card: CardModel) => {
    if (activeSlotIndex === null) return;
    setSlots((current) => {
      const next = [...current];
      next[activeSlotIndex] = card;
      const hasEmptySlot = next.some((item) => item === null);
      if (next.length < MAX_CARDS && !hasEmptySlot) {
        next.push(null);
      }
      return next;
    });
    closePicker();
  };

  const clearSlot = (index: number) => {
    setSlots((current) => {
      const next = [...current];
      next[index] = null;
      return trimTrailingEmptySlots(next);
    });
  };

  const addSlot = () => {
    setSlots((current) => {
      if (current.length >= MAX_CARDS) return current;
      return [...current, null];
    });
  };

  return (
    <View style={styles.screen}>
      <View style={styles.headerBar}>
        <Text style={styles.title}>Card Comparison</Text>
        <Text style={styles.subtitle}>Tap a card panel to search year 25 cards and compare side-by-side</Text>
      </View>

      <View style={styles.compareShell}>
        <ScrollView horizontal nestedScrollEnabled contentContainerStyle={styles.horizontalContent}>
          <View style={styles.compareContent}>
            <View style={styles.cardPanelsRow}>
              {slots.map((card, index) => (
                <View key={`slot-card-${index}`} style={styles.column}>
                  <Pressable
                    style={[styles.cardPanel, card ? styles.cardPanelFilled : styles.cardPanelEmpty]}
                    onPress={() => openPicker(index)}
                  >
                    {card ? (
                      <>
                        <View style={styles.cardPanelTop}>
                          {card.baked_img || card.img ? (
                            <Image
                              source={{ uri: card.baked_img || card.img || undefined }}
                              style={styles.cardImage}
                              resizeMode="cover"
                            />
                          ) : (
                            <View style={styles.cardImagePlaceholder}>
                              <Text style={styles.cardImagePlaceholderText}>NO IMG</Text>
                            </View>
                          )}
                          <View style={styles.cardTextWrap}>
                            <Text style={styles.cardName} numberOfLines={2}>
                              {card.name}
                            </Text>
                            <Text style={styles.cardSeries} numberOfLines={1}>
                              {card.series_name}
                            </Text>
                          </View>
                        </View>
                        <Pressable style={styles.removeButton} onPress={() => clearSlot(index)}>
                          <Text style={styles.removeButtonText}>x</Text>
                        </Pressable>
                      </>
                    ) : (
                      <View style={styles.emptyPanelContent}>
                        <Text style={styles.emptyPanelPlus}>+</Text>
                        <Text style={styles.emptyPanelText}>Add Card</Text>
                      </View>
                    )}
                  </Pressable>
                </View>
              ))}

              {slots.length < MAX_CARDS ? (
                <Pressable style={styles.addSlotPanel} onPress={addSlot}>
                  <Text style={styles.addSlotPlus}>+</Text>
                  <Text style={styles.addSlotText}>Add Slot</Text>
                </Pressable>
              ) : null}
            </View>

            <ScrollView style={styles.valuesScroll} nestedScrollEnabled>
              <View style={styles.valuesRow}>
                {slots.map((card, index) => (
                  <View key={`slot-values-${index}`} style={styles.column}>
                    {card ? (
                      FIELD_ROWS.map((row) => {
                        const rawValue = getRowRawValue(card, row.key);
                        const displayValue = formatCompareValue(rawValue, String(row.key));
                        const numericValue = numericFromValue(rawValue);
                        const maxValue = rowMaxValue.get(String(row.key));
                        const highlight =
                          maxValue !== undefined
                          && numericValue !== null
                          && numericValue === maxValue;

                        return (
                          <View
                            key={`${card.id}-${String(row.key)}`}
                            style={[styles.valueBox, highlight && styles.valueBoxHighlight]}
                          >
                            <Text style={[styles.valueLabel, highlight && styles.valueLabelHighlight]}>
                              {row.label}
                            </Text>
                            <Text style={[styles.valueText, highlight && styles.valueTextHighlight]}>
                              {displayValue}
                            </Text>
                          </View>
                        );
                      })
                    ) : (
                      <View style={styles.emptyValueBox}>
                        <Text style={styles.emptyValueText}>Select a card to load stats</Text>
                      </View>
                    )}
                  </View>
                ))}
                {slots.length < MAX_CARDS ? <View style={styles.addSlotSpacer} /> : null}
              </View>
            </ScrollView>
          </View>
        </ScrollView>
      </View>

      <Modal
        transparent
        animationType="fade"
        visible={activeSlotIndex !== null}
        onRequestClose={closePicker}
      >
        <View style={styles.modalBackdrop}>
          <Pressable style={styles.modalOverlayClose} onPress={closePicker} />
          <View style={styles.modalCard}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>Search Card</Text>
              <Pressable onPress={closePicker} style={styles.modalDoneButton}>
                <Text style={styles.modalDoneText}>Done</Text>
              </Pressable>
            </View>

            <View style={styles.modalSearchWrap}>
              <TextInput
                style={styles.modalSearchInput}
                value={query}
                onChangeText={setQuery}
                placeholder="Search by card name..."
                placeholderTextColor="rgba(148, 163, 184, 0.9)"
                autoCorrect={false}
                autoCapitalize="none"
              />
              <View style={styles.sortOptionsWrap}>
                <Text style={styles.sortOptionsLabel}>Sort results by</Text>
                <ScrollView horizontal showsHorizontalScrollIndicator={false}>
                  <View style={styles.sortOptionsRow}>
                    {COMPARISON_SORT_OPTIONS.map((option) => {
                      const active = option.key === searchSortKey;
                      return (
                        <Pressable
                          key={option.key}
                          onPress={() => setSearchSortKey(option.key)}
                          style={[styles.sortOptionChip, active && styles.sortOptionChipActive]}
                        >
                          <Text style={[styles.sortOptionText, active && styles.sortOptionTextActive]}>
                            {option.label}
                          </Text>
                        </Pressable>
                      );
                    })}
                  </View>
                </ScrollView>
              </View>
            </View>

            {searchLoading ? (
              <View style={styles.modalLoading}>
                <ActivityIndicator size="small" color={theme.colors.primary} />
              </View>
            ) : null}

            <ScrollView style={styles.modalResults} contentContainerStyle={styles.modalResultsContent}>
              {visibleResults.map((card) => (
                <Pressable
                  key={card.id}
                  style={styles.modalResultRow}
                  onPress={() => pickCardForActiveSlot(card)}
                >
                  {card.baked_img || card.img ? (
                    <Image
                      source={{ uri: card.baked_img || card.img || undefined }}
                      style={styles.modalResultImage}
                      resizeMode="cover"
                    />
                  ) : (
                    <View style={styles.modalResultImagePlaceholder}>
                      <Text style={styles.modalResultImagePlaceholderText}>N/A</Text>
                    </View>
                  )}
                  <View style={styles.modalResultTextWrap}>
                    <Text style={styles.modalResultName} numberOfLines={1}>
                      {card.name}
                    </Text>
                    <Text style={styles.modalResultSeries} numberOfLines={1}>
                      {card.series_name}
                    </Text>
                  </View>
                </Pressable>
              ))}

              {query.trim().length >= 2 && !searchLoading && visibleResults.length === 0 ? (
                <Text style={styles.modalEmptyText}>No cards found.</Text>
              ) : null}
              {searchError ? <Text style={styles.modalErrorText}>{searchError}</Text> : null}
            </ScrollView>
          </View>
        </View>
      </Modal>
    </View>
  );
}

async function fetchComparisonCards(path: string): Promise<unknown> {
  try {
    return await apiGetAuth<unknown>(path);
  } catch (err) {
    const isAuthMissing =
      (err instanceof ApiError && (err.status === 401 || err.status === 403))
      || (err instanceof Error && err.message === "Not authenticated");
    if (!isAuthMissing) throw err;
    return apiGet<unknown>(path);
  }
}

function getYourOverallValue(card: CardModel): number | null {
  const rounded = card.your_overall_rounded;
  if (typeof rounded === "number" && Number.isFinite(rounded)) return rounded;
  const raw = card.your_overall;
  if (typeof raw === "number" && Number.isFinite(raw)) return raw;
  return null;
}

function getSortMetric(card: CardModel, key: ComparisonSortKey): number {
  if (key === "meta_overall") {
    if (typeof card.meta_overall_rounded === "number" && Number.isFinite(card.meta_overall_rounded)) {
      return card.meta_overall_rounded;
    }
    if (typeof card.meta_overall === "number" && Number.isFinite(card.meta_overall)) {
      return card.meta_overall;
    }
    return card.ovr ?? 0;
  }

  if (key === "true_overall") {
    if (typeof card.true_overall_rounded === "number" && Number.isFinite(card.true_overall_rounded)) {
      return card.true_overall_rounded;
    }
    if (typeof card.true_overall === "number" && Number.isFinite(card.true_overall)) {
      return card.true_overall;
    }
    return card.ovr ?? 0;
  }

  if (key === "your_overall") {
    return getYourOverallValue(card) ?? card.ovr ?? 0;
  }

  return card.ovr ?? 0;
}

function getRowRawValue(card: CardModel, key: FieldKey): unknown {
  if (key === "your_overall") {
    return getYourOverallValue(card);
  }
  return card[key];
}

function numericFromValue(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  return null;
}

function trimTrailingEmptySlots(slots: (CardModel | null)[]): (CardModel | null)[] {
  const next = [...slots];
  while (next.length > 2 && next[next.length - 1] === null && next[next.length - 2] === null) {
    next.pop();
  }
  return next;
}

function normalizeCardResults(payload: unknown): CardModel[] {
  if (Array.isArray(payload)) {
    return payload as CardModel[];
  }
  if (!payload || typeof payload !== "object") return [];

  const candidates = [
    (payload as { items?: unknown }).items,
    (payload as { data?: unknown }).data,
    (payload as { results?: unknown }).results,
    (payload as { cards?: unknown }).cards,
  ];

  for (const candidate of candidates) {
    if (Array.isArray(candidate)) return candidate as CardModel[];
  }
  return [];
}

function formatCompareValue(value: unknown, key: string): string {
  if (value === null || value === undefined) return "-";
  if (key === "quirks" && Array.isArray(value)) {
    const names = value
      .map((item) => (typeof item === "object" && item && "name" in item ? String(item.name) : ""))
      .filter((name) => name.length > 0);
    return names.length > 0 ? names.join(", ") : "-";
  }
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") {
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(2);
  }
  if (typeof value === "string") return value || "-";
  if (typeof value === "object") {
    try {
      return JSON.stringify(value);
    } catch {
      return "-";
    }
  }
  return String(value);
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: theme.colors.background,
    paddingHorizontal: 10,
    paddingTop: 10,
    paddingBottom: 12,
    gap: 10,
  },
  headerBar: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.84)",
    paddingHorizontal: 10,
    paddingVertical: 8,
    gap: 3,
  },
  title: {
    color: "#f8fafc",
    fontSize: 18,
    fontWeight: "800",
  },
  subtitle: {
    color: "#94a3b8",
    fontSize: 11,
    fontWeight: "600",
  },
  compareShell: {
    flex: 1,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.64)",
    overflow: "hidden",
  },
  horizontalContent: {
    flexGrow: 1,
  },
  compareContent: {
    flex: 1,
  },
  cardPanelsRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    paddingHorizontal: 8,
    paddingTop: 8,
    paddingBottom: 7,
    gap: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.18)",
  },
  valuesScroll: {
    flex: 1,
  },
  valuesRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    paddingHorizontal: 8,
    paddingVertical: 8,
    gap: 8,
  },
  column: {
    width: 156,
    gap: 6,
  },
  cardPanel: {
    borderRadius: 12,
    borderWidth: 1,
    overflow: "hidden",
    position: "relative",
  },
  cardPanelFilled: {
    borderColor: "rgba(251, 191, 36, 0.7)",
    backgroundColor: "rgba(17, 24, 39, 0.95)",
    shadowColor: "#fbbf24",
    shadowOpacity: 0.18,
    shadowRadius: 8,
    shadowOffset: { width: 0, height: 4 },
    elevation: 5,
  },
  cardPanelEmpty: {
    minHeight: 88,
    borderStyle: "dashed",
    borderColor: "rgba(148, 163, 184, 0.42)",
    backgroundColor: "rgba(15, 23, 42, 0.62)",
    justifyContent: "center",
    alignItems: "center",
  },
  cardPanelTop: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingHorizontal: 7,
    paddingVertical: 6,
    backgroundColor: "rgba(30, 41, 59, 0.92)",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(251, 191, 36, 0.45)",
  },
  cardImage: {
    width: 38,
    height: 52,
    borderRadius: 5,
    borderWidth: 1,
    borderColor: "rgba(251, 191, 36, 0.55)",
    backgroundColor: "rgba(15, 23, 42, 0.92)",
  },
  cardImagePlaceholder: {
    width: 38,
    height: 52,
    borderRadius: 5,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.36)",
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    alignItems: "center",
    justifyContent: "center",
  },
  cardImagePlaceholderText: {
    color: "#94a3b8",
    fontSize: 9,
    fontWeight: "700",
  },
  cardTextWrap: {
    flex: 1,
    gap: 2,
    justifyContent: "center",
  },
  cardName: {
    color: "#f8fafc",
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 13,
  },
  cardSeries: {
    color: "#94a3b8",
    fontSize: 9,
    fontWeight: "700",
  },
  removeButton: {
    position: "absolute",
    top: 4,
    right: 4,
    width: 17,
    height: 17,
    borderRadius: 9,
    backgroundColor: "rgba(153, 27, 27, 0.72)",
    borderWidth: 1,
    borderColor: "rgba(254, 202, 202, 0.25)",
    alignItems: "center",
    justifyContent: "center",
  },
  removeButtonText: {
    color: "#fee2e2",
    fontSize: 10,
    fontWeight: "800",
    lineHeight: 11,
  },
  emptyPanelContent: {
    alignItems: "center",
    justifyContent: "center",
    gap: 2,
  },
  emptyPanelPlus: {
    color: "#60a5fa",
    fontSize: 28,
    fontWeight: "500",
    lineHeight: 29,
  },
  emptyPanelText: {
    color: "#94a3b8",
    fontSize: 11,
    fontWeight: "700",
  },
  valueBox: {
    minHeight: 48,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
    borderRadius: 10,
    backgroundColor: "rgba(15, 23, 42, 0.86)",
    paddingHorizontal: 8,
    paddingVertical: 6,
    justifyContent: "center",
    gap: 2,
  },
  valueBoxHighlight: {
    borderColor: "rgba(34, 197, 94, 0.72)",
    backgroundColor: "rgba(21, 128, 61, 0.26)",
  },
  valueLabel: {
    color: "#94a3b8",
    fontSize: 9,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.3,
  },
  valueLabelHighlight: {
    color: "#bbf7d0",
  },
  valueText: {
    color: "#e2e8f0",
    fontSize: 11,
    fontWeight: "700",
    lineHeight: 13,
  },
  valueTextHighlight: {
    color: "#dcfce7",
    fontWeight: "800",
  },
  emptyValueBox: {
    minHeight: 48,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderRadius: 10,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 8,
    paddingVertical: 6,
  },
  emptyValueText: {
    color: "#64748b",
    fontSize: 11,
    fontWeight: "600",
    textAlign: "center",
  },
  addSlotPanel: {
    width: 64,
    minHeight: 88,
    borderWidth: 1,
    borderStyle: "dashed",
    borderColor: "rgba(59, 130, 246, 0.5)",
    borderRadius: 14,
    backgroundColor: "rgba(30, 64, 175, 0.2)",
    alignItems: "center",
    justifyContent: "center",
    gap: 2,
    paddingHorizontal: 6,
  },
  addSlotSpacer: {
    width: 64,
  },
  addSlotPlus: {
    color: "#bfdbfe",
    fontSize: 20,
    fontWeight: "700",
    lineHeight: 20,
  },
  addSlotText: {
    color: "#dbeafe",
    fontSize: 10,
    fontWeight: "700",
    textAlign: "center",
  },
  modalBackdrop: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "rgba(2, 6, 23, 0.6)",
    padding: 16,
  },
  modalOverlayClose: {
    ...StyleSheet.absoluteFillObject,
  },
  modalCard: {
    width: "100%",
    maxWidth: 460,
    height: "78%",
    minHeight: 360,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.98)",
    overflow: "hidden",
  },
  modalHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.18)",
  },
  modalTitle: {
    color: "#f8fafc",
    fontSize: 14,
    fontWeight: "800",
  },
  modalDoneButton: {
    borderWidth: 1,
    borderColor: "rgba(59, 130, 246, 0.65)",
    borderRadius: 7,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "rgba(37, 99, 235, 0.25)",
  },
  modalDoneText: {
    color: "#dbeafe",
    fontSize: 11,
    fontWeight: "700",
  },
  modalSearchWrap: {
    padding: 10,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.16)",
    gap: 8,
  },
  modalSearchInput: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
    borderRadius: 8,
    backgroundColor: "rgba(15, 23, 42, 0.72)",
    color: "#e2e8f0",
    fontSize: 12,
    fontWeight: "600",
    paddingHorizontal: 10,
    paddingVertical: 8,
  },
  sortOptionsWrap: {
    gap: 6,
  },
  sortOptionsLabel: {
    color: "#94a3b8",
    fontSize: 10,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.4,
  },
  sortOptionsRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingRight: 8,
  },
  sortOptionChip: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.36)",
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.72)",
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  sortOptionChipActive: {
    borderColor: "rgba(251, 191, 36, 0.7)",
    backgroundColor: "rgba(251, 191, 36, 0.18)",
  },
  sortOptionText: {
    color: "#cbd5e1",
    fontSize: 11,
    fontWeight: "700",
  },
  sortOptionTextActive: {
    color: "#fde68a",
  },
  modalLoading: {
    paddingVertical: 10,
    alignItems: "center",
    justifyContent: "center",
  },
  modalResults: {
    flex: 1,
  },
  modalResultsContent: {
    flexGrow: 1,
    padding: 10,
    gap: 6,
  },
  modalResultRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    borderRadius: 8,
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    padding: 8,
  },
  modalResultImage: {
    width: 34,
    height: 46,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
    backgroundColor: "rgba(15, 23, 42, 0.9)",
  },
  modalResultImagePlaceholder: {
    width: 34,
    height: 46,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    alignItems: "center",
    justifyContent: "center",
  },
  modalResultImagePlaceholderText: {
    color: "#94a3b8",
    fontSize: 9,
    fontWeight: "700",
  },
  modalResultTextWrap: {
    flex: 1,
    gap: 1,
  },
  modalResultName: {
    color: "#e2e8f0",
    fontSize: 12,
    fontWeight: "700",
  },
  modalResultSeries: {
    color: "#94a3b8",
    fontSize: 11,
    fontWeight: "600",
  },
  modalEmptyText: {
    color: "#94a3b8",
    fontSize: 12,
    fontWeight: "600",
    textAlign: "center",
    paddingVertical: 14,
  },
  modalErrorText: {
    color: "#fca5a5",
    fontSize: 11,
    fontWeight: "600",
    textAlign: "center",
    paddingTop: 2,
    paddingBottom: 10,
  },
});
