import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "expo-router";
import { Ionicons } from "@expo/vector-icons";
import {
  ActivityIndicator,
  Animated,
  Image,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
  type ViewStyle,
} from "react-native";

import { ApiError, apiGet, apiGetAuth } from "../../src/lib/api";
import { useBackendProStatus } from "../../src/lib/proStatus";
import { theme } from "../../src/theme/colors";

type CardRanking = {
  id: string;
  mlb_id?: number | null;
  name: string;
  series_name?: string | null;
  img?: string | null;
  baked_img?: string | null;
  display_primary_position?: string | null;
  display_secondary_position?: string | null;
  display_secondary_positions?: string | null;
  display_seconday_position?: string | null;
  meta_overall?: number | null;
  meta_overall_rounded?: number | null;
  your_overall?: number | null;
  your_overall_rounded?: number | null;
  true_overall?: number | null;
  true_overall_rounded?: number | null;
  meta_overall_by_position?: Record<string, number> | null;
  your_overall_by_position?: Record<string, number> | null;
  true_overall_by_position?: Record<string, number> | null;
  bat_hand?: string | null;
  throw_hand?: string | null;
  is_hitter?: boolean;
  ovr: number;
  contact_left?: number | null;
  contact_right?: number | null;
  power_left?: number | null;
  power_right?: number | null;
  plate_vision?: number | null;
  plate_discipline?: number | null;
  batting_clutch?: number | null;
  bunting_ability?: number | null;
  drag_bunting_ability?: number | null;
  hitting_durability?: number | null;
  fielding_ability?: number | null;
  arm_strength?: number | null;
  arm_accuracy?: number | null;
  reaction_time?: number | null;
  blocking?: number | null;
  speed?: number | null;
  baserunning_ability?: number | null;
  baserunning_aggression?: number | null;
  stamina?: number | null;
  pitching_clutch?: number | null;
  hits_per_bf?: number | null;
  k_per_bf?: number | null;
  bb_per_bf?: number | null;
  hr_per_bf?: number | null;
  pitch_velocity?: number | null;
  pitch_control?: number | null;
  pitch_movement?: number | null;
  quirks?: { name: string }[];
  year?: number | null;
};

type SortDirection = "asc" | "desc";
type FilterMenuKey = "year" | "position" | "bat" | "pitch";
type AttributeKey =
  | "contact_left"
  | "contact_right"
  | "power_left"
  | "power_right"
  | "plate_vision"
  | "plate_discipline"
  | "batting_clutch"
  | "bunting_ability"
  | "drag_bunting_ability"
  | "hitting_durability"
  | "fielding_ability"
  | "arm_strength"
  | "arm_accuracy"
  | "reaction_time"
  | "blocking"
  | "speed"
  | "baserunning_ability"
  | "baserunning_aggression"
  | "stamina"
  | "pitching_clutch"
  | "hits_per_bf"
  | "k_per_bf"
  | "bb_per_bf"
  | "hr_per_bf"
  | "pitch_velocity"
  | "pitch_control"
  | "pitch_movement";
type SortKey = "name" | "hands" | "position" | "your" | "meta" | "true" | "ovr" | AttributeKey;

type AttributeColumn = {
  key: AttributeKey;
  label: string;
};

type MultiSelectOption = {
  key: string;
  label: string;
};

const ATTRIBUTE_COLUMNS: AttributeColumn[] = [
  { key: "contact_left", label: "CON L" },
  { key: "contact_right", label: "CON R" },
  { key: "power_left", label: "POW L" },
  { key: "power_right", label: "POW R" },
  { key: "plate_vision", label: "VIS" },
  { key: "plate_discipline", label: "DISC" },
  { key: "batting_clutch", label: "CLT" },
  { key: "bunting_ability", label: "BNT" },
  { key: "drag_bunting_ability", label: "DRG BNT" },
  { key: "hitting_durability", label: "H DUR" },
  { key: "fielding_ability", label: "FLD" },
  { key: "arm_strength", label: "ARM" },
  { key: "arm_accuracy", label: "ACC" },
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
];

const PAGE_SIZE = 30;
const YEAR_OPTIONS: number[] = [25, 24, 23, 22];
const POSITION_OPTIONS = ["C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH", "SP", "RP", "CP"];
const HAND_OPTIONS = ["L", "R", "S"];
const TEXT_SORT_KEYS: SortKey[] = ["name", "hands", "position"];

export default function CardsRankingsScreen() {
  const { isPro } = useBackendProStatus();
  const hasProAccess = isPro === true;
  const [page, setPage] = useState(1);
  const [rows, setRows] = useState<CardRanking[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hasNext, setHasNext] = useState(false);

  const router = useRouter();

  const [selectedYear, setSelectedYear] = useState<number>(25);
  const [selectedPositions, setSelectedPositions] = useState<string[]>([]);
  const [includeSecondary, setIncludeSecondary] = useState(false);
  const [selectedBatHands, setSelectedBatHands] = useState<string[]>([]);
  const [selectedPitchHands, setSelectedPitchHands] = useState<string[]>([]);
  const [openFilterMenu, setOpenFilterMenu] = useState<FilterMenuKey | null>(null);
  const [nameSearch, setNameSearch] = useState("");

  const [sortKey, setSortKey] = useState<SortKey>("meta");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const tableScrollY = useRef(new Animated.Value(0)).current;
  const pinnedRowsTranslateY = useMemo(
    () => Animated.multiply(tableScrollY, -1),
    [tableScrollY]
  );
  const onTableScroll = useMemo(
    () => Animated.event(
      [{ nativeEvent: { contentOffset: { y: tableScrollY } } }],
      { useNativeDriver: true }
    ),
    [tableScrollY]
  );
  const requestSequenceRef = useRef(0);

  const offset = useMemo(() => (page - 1) * PAGE_SIZE, [page]);

  const loadRows = useCallback(async () => {
    const requestSequence = requestSequenceRef.current + 1;
    requestSequenceRef.current = requestSequence;
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      params.set("limit", String(PAGE_SIZE));
      params.set("offset", String(offset));
      params.set("sort_by", sortKey);
      params.set("sort_dir", sortDirection);
      params.set("year", String(selectedYear));

      if (selectedPositions.length === 1) {
        params.set("position", selectedPositions[0]);
        params.set("include_secondary", includeSecondary ? "true" : "false");
      } else if (selectedPositions.length > 1) {
        params.set("positions", selectedPositions.join(","));
        params.set("include_secondary", includeSecondary ? "true" : "false");
      }

      if (selectedBatHands.length === 1) {
        params.set("bat_hand", selectedBatHands[0]);
      } else if (selectedBatHands.length > 1) {
        params.set("bat_hands", selectedBatHands.join(","));
      }

      if (selectedPitchHands.length === 1) {
        params.set("pitch_hand", selectedPitchHands[0]);
      } else if (selectedPitchHands.length > 1) {
        params.set("pitch_hands", selectedPitchHands.join(","));
      }

      const trimmedName = nameSearch.trim();
      if (trimmedName.length > 0) params.set("name", trimmedName);

      const data = await fetchRankingCards(`/cards/?${params.toString()}`);
      if (requestSequence !== requestSequenceRef.current) return;
      const safeRows = Array.isArray(data) ? data : [];
      const uniqueById = new Map<string, CardRanking>();
      for (const card of safeRows) {
        if (!uniqueById.has(card.id)) uniqueById.set(card.id, card);
      }
      const deduped = Array.from(uniqueById.values());
      const yearFiltered = deduped.filter((card) => matchesSelectedYear(card.year, selectedYear));
      setRows(yearFiltered);
      setHasNext(yearFiltered.length === PAGE_SIZE);
    } catch (err) {
      if (requestSequence !== requestSequenceRef.current) return;
      const message = err instanceof Error ? err.message : "Unable to load cards.";
      setError(message);
      setRows([]);
      setHasNext(false);
    } finally {
      if (requestSequence !== requestSequenceRef.current) return;
      setLoading(false);
    }
  }, [
    offset,
    selectedYear,
    selectedPositions,
    includeSecondary,
    selectedBatHands,
    selectedPitchHands,
    nameSearch,
    sortKey,
    sortDirection,
  ]);

  useEffect(() => {
    loadRows();
  }, [loadRows]);

  useEffect(() => {
    if (hasProAccess || sortKey !== "your") return;
    setSortKey("meta");
    setSortDirection("desc");
    setPage(1);
  }, [hasProAccess, sortKey]);

  const onPrev = () => {
    if (loading || page === 1) return;
    setPage((current) => Math.max(1, current - 1));
  };

  const onNext = () => {
    if (loading || !hasNext) return;
    setPage((current) => current + 1);
  };

  const onSortChange = (nextKey: SortKey) => {
    if (!hasProAccess && nextKey === "your") return;
    if (sortKey === nextKey) {
      setSortDirection((current) => (current === "asc" ? "desc" : "asc"));
      setPage(1);
      return;
    }
    setSortKey(nextKey);
    setSortDirection(TEXT_SORT_KEYS.includes(nextKey) ? "asc" : "desc");
    setPage(1);
  };

  const toggleYears = (value: number) => {
    setSelectedYear(value);
    setPage(1);
  };

  const togglePositions = (value: string) => {
    setSelectedPositions((current) => toggleSelection(current, value));
    setPage(1);
  };

  const toggleBatHands = (value: string) => {
    setSelectedBatHands((current) => toggleSelection(current, value));
    setPage(1);
  };

  const togglePitchHands = (value: string) => {
    setSelectedPitchHands((current) => toggleSelection(current, value));
    setPage(1);
  };

  const clearMenuSelections = () => {
    if (openFilterMenu === "year") {
      setSelectedYear(25);
      setPage(1);
      return;
    }
    if (openFilterMenu === "position") {
      setSelectedPositions([]);
      setIncludeSecondary(false);
      setPage(1);
      return;
    }
    if (openFilterMenu === "bat") {
      setSelectedBatHands([]);
      setPage(1);
      return;
    }
    if (openFilterMenu === "pitch") {
      setSelectedPitchHands([]);
      setPage(1);
    }
  };

  const menuOptions = useMemo((): MultiSelectOption[] => {
    if (openFilterMenu === "year") {
      return YEAR_OPTIONS.map((value) => ({ key: String(value), label: String(value) }));
    }
    if (openFilterMenu === "position") {
      return POSITION_OPTIONS.map((value) => ({ key: value, label: value }));
    }
    if (openFilterMenu === "bat" || openFilterMenu === "pitch") {
      return HAND_OPTIONS.map((value) => ({ key: value, label: value }));
    }
    return [];
  }, [openFilterMenu]);

  const selectedSet = useMemo(() => {
    if (openFilterMenu === "year") return new Set([String(selectedYear)]);
    if (openFilterMenu === "position") return new Set(selectedPositions);
    if (openFilterMenu === "bat") return new Set(selectedBatHands);
    if (openFilterMenu === "pitch") return new Set(selectedPitchHands);
    return new Set<string>();
  }, [openFilterMenu, selectedYear, selectedPositions, selectedBatHands, selectedPitchHands]);

  const onMenuToggleOption = (key: string) => {
    if (openFilterMenu === "year") {
      const yearValue = Number(key);
      if (!Number.isFinite(yearValue)) return;
      toggleYears(yearValue);
      return;
    }
    if (openFilterMenu === "position") {
      togglePositions(key);
      return;
    }
    if (openFilterMenu === "bat") {
      toggleBatHands(key);
      return;
    }
    if (openFilterMenu === "pitch") {
      togglePitchHands(key);
    }
  };
  const handleCardPress = (card: CardRanking) => {
    // Check if the card is a Live Series card
    const isLive = card.series_name?.toLowerCase() === 'live';
    
    if (isLive) {
      // Route to PlayerDetailsScreen (predictions route)
      router.push({ 
        pathname: '/predictions/[id]', 
        params: { id: card.id, cardData: JSON.stringify(card) } 
      });
    } else {
      // Route to standard CardScreen (non-live route)
      router.push({ 
        pathname: '/(app)/card', 
        params: { cardData: JSON.stringify(card) } 
      });
    }
  };

  return (
    <View style={styles.screen}>
      <View style={styles.header}>
        <Text style={styles.title}>Card Rankings</Text>
        <Text style={styles.subtitle}>30 cards per page</Text>
      </View>

      <View style={styles.filtersCardCompact}>
        <View style={styles.filtersRowCompact}>
          <FilterDropdownButton
            label="Year"
            value={String(selectedYear)}
            onPress={() => setOpenFilterMenu("year")}
          />
          <FilterDropdownButton
            label="Position"
            value={summaryLabel(selectedPositions, "All")}
            onPress={() => setOpenFilterMenu("position")}
          />
          <FilterDropdownButton
            label="Bats"
            value={summaryLabel(selectedBatHands, "All")}
            onPress={() => setOpenFilterMenu("bat")}
          />
          <FilterDropdownButton
            label="Throws"
            value={summaryLabel(selectedPitchHands, "All")}
            onPress={() => setOpenFilterMenu("pitch")}
          />
        </View>
        <TextInput
          style={styles.searchInput}
          placeholder="Search card name..."
          placeholderTextColor="rgba(148, 163, 184, 0.85)"
          value={nameSearch}
          onChangeText={(value) => {
            setNameSearch(value);
            setPage(1);
          }}
          autoCorrect={false}
          autoCapitalize="none"
        />
      </View>

      <Modal
        transparent
        animationType="fade"
        visible={openFilterMenu !== null}
        onRequestClose={() => setOpenFilterMenu(null)}
      >
        <View style={styles.modalBackdrop}>
          <Pressable style={styles.modalOverlayClose} onPress={() => setOpenFilterMenu(null)} />
          <View style={styles.modalCard}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>{filterMenuTitle(openFilterMenu)}</Text>
              <Pressable onPress={() => setOpenFilterMenu(null)} style={styles.modalDoneButton}>
                <Text style={styles.modalDoneText}>Done</Text>
              </Pressable>
            </View>

            {openFilterMenu === "position" ? (
              <View style={styles.modalInlineToggleRow}>
                <Text style={styles.modalInlineToggleLabel}>Include Secondary</Text>
                <Pressable
                  onPress={() => {
                    if (selectedPositions.length === 0) return;
                    setIncludeSecondary((current) => !current);
                    setPage(1);
                  }}
                  style={[
                    styles.modalInlineToggleButton,
                    includeSecondary && styles.modalInlineToggleButtonSelected,
                    selectedPositions.length === 0 && styles.modalInlineToggleButtonDisabled,
                  ]}
                  disabled={selectedPositions.length === 0}
                >
                  <Text
                    style={[
                      styles.modalInlineToggleButtonText,
                      includeSecondary && styles.modalInlineToggleButtonTextSelected,
                    ]}
                  >
                    {includeSecondary ? "On" : "Off"}
                  </Text>
                </Pressable>
              </View>
            ) : null}

            <ScrollView style={styles.modalList} contentContainerStyle={styles.modalListContent}>
              {menuOptions.map((option) => {
                const selected = selectedSet.has(option.key);
                return (
                  <Pressable
                    key={option.key}
                    style={[styles.modalOption, selected && styles.modalOptionSelected]}
                    onPress={() => onMenuToggleOption(option.key)}
                  >
                    <Text style={[styles.modalOptionText, selected && styles.modalOptionTextSelected]}>
                      {option.label}
                    </Text>
                    <Text style={[styles.modalCheck, selected && styles.modalCheckSelected]}>
                      {selected ? "✓" : ""}
                    </Text>
                  </Pressable>
                );
              })}
            </ScrollView>

            <View style={styles.modalFooter}>
              <Pressable style={styles.modalClearButton} onPress={clearMenuSelections}>
                <Text style={styles.modalClearText}>Clear</Text>
              </Pressable>
            </View>
          </View>
        </View>
      </Modal>

      {error ? (
        <View style={styles.errorCard}>
          <Text style={styles.errorText}>{error}</Text>
        </View>
      ) : null}

      <View style={styles.tableContainer}>
        <View style={styles.tableSplit}>
          <View style={styles.pinnedImageColumn}>
            <View style={styles.pinnedImageBody}>
              <Animated.View
                style={[styles.pinnedImageRows, { transform: [{ translateY: pinnedRowsTranslateY }] }]}
              >
                <View style={[styles.headerRow, styles.pinnedImageHeader]}>
                  <Text style={[styles.headerCell, styles.headerCellCentered]} numberOfLines={1}>
                    Img
                  </Text>
                </View>
                {rows.map((card, index) => (
                  <Pressable
                    key={`pinned-${card.id}`}
                    style={[styles.pinnedImageCell, index % 2 === 0 ? styles.evenRow : styles.oddRow]}
                    onPress={() => handleCardPress(card)}
                  
                  >
                    <CardImage uri={card.baked_img || card.img || null} />
                  </Pressable>
                ))}
              </Animated.View>
            </View>
          </View>

          <View style={styles.tableScrollPane}>
            <ScrollView horizontal contentContainerStyle={styles.tableContent} nestedScrollEnabled>
              <Animated.ScrollView
                nestedScrollEnabled
                onScroll={onTableScroll}
                scrollEventThrottle={16}
              >
                <View>
              <View style={[styles.row, styles.headerRow]}>
                <SortHeader
                  label="Card"
                  sortKey="name"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.nameCell}
                  center
                />
                <SortHeader
                  label="B/P"
                  sortKey="hands"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.handsCell}
                  center
                />
                <SortHeader
                  label="Position"
                  sortKey="position"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.positionCell}
                  center
                />
                <SortHeader
                  label="Your"
                  sortKey="your"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.overallCell}
                  center
                  disabled={!hasProAccess}
                  showLock={!hasProAccess}
                />
                <SortHeader
                  label="Meta"
                  sortKey="meta"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.overallCell}
                  center
                />
                <SortHeader
                  label="True"
                  sortKey="true"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.overallCell}
                  center
                />
                <SortHeader
                  label="OVR"
                  sortKey="ovr"
                  activeSortKey={sortKey}
                  direction={sortDirection}
                  onPress={onSortChange}
                  cellStyle={styles.overallCell}
                  center
                />

                {ATTRIBUTE_COLUMNS.map((column) => (
                  <SortHeader
                    key={column.key}
                    label={column.label}
                    sortKey={column.key}
                    activeSortKey={sortKey}
                    direction={sortDirection}
                    onPress={onSortChange}
                    cellStyle={styles.attributeCell}
                    center
                  />
                ))}
                <View style={[styles.headerStaticCell, styles.quirksColumn]}>
                  <Text style={[styles.headerCell, styles.headerCellCentered]} numberOfLines={1}>
                    Quirks
                  </Text>
                </View>
              </View>

                {rows.length === 0 && !loading ? (
                  <View style={styles.loadingRow}>
                    <Text style={styles.emptyText}>No cards found.</Text>
                  </View>
                ) : (
                  rows.map((card, index) => {
                  const metricPosition = metricPositionForCard(
                    card,
                    selectedPositions,
                    includeSecondary
                  );
                  const metaOverall = resolveOverallForPosition(
                    card.meta_overall_by_position,
                    metricPosition,
                    card.meta_overall_rounded,
                    card.meta_overall
                  );
                  const trueOverall = resolveOverallForPosition(
                    card.true_overall_by_position,
                    metricPosition,
                    card.true_overall_rounded,
                    card.true_overall
                  );
                  const yourOverall = resolveOverallForPosition(
                    card.your_overall_by_position,
                    metricPosition,
                    card.your_overall_rounded,
                    card.your_overall
                  );

                  const quirks = Array.isArray(card.quirks)
                    ? card.quirks
                        .map((quirk) => quirk?.name?.trim() || "")
                        .filter((name) => name.length > 0)
                    : [];
                  const quirkText = quirks.length > 0 ? quirks.join(", ") : "None";

                  return (
                      <Pressable
                        key={card.id}
                        style={[styles.row, index % 2 === 0 ? styles.evenRow : styles.oddRow]}
                        onPress={() => handleCardPress(card)}
                      >
                        <View
                          style={[
                            styles.cell,
                            styles.nameCell,
                            styles.stackCell,
                            sortKey === "name" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.mainCellText} numberOfLines={1}>
                            {card.name}
                          </Text>
                          <OverflowSubline text={card.series_name || "-"} centered />
                        </View>

                      <View
                        style={[
                          styles.cell,
                          styles.handsCell,
                          styles.valueCell,
                          sortKey === "hands" && styles.activeColumnCell,
                        ]}
                      >
                        <Text style={styles.centerCellText}>
                          {formatHand(card.bat_hand)}/{formatHand(card.throw_hand)}
                        </Text>
                      </View>

                      <View
                        style={[
                          styles.cell,
                          styles.positionCell,
                          styles.stackCell,
                          sortKey === "position" && styles.activeColumnCell,
                        ]}
                      >
                        <Text style={styles.mainCellText} numberOfLines={1}>
                          {card.display_primary_position || "-"}
                        </Text>
                        <OverflowSubline text={secondaryPosition(card)} centered />
                      </View>

                      <View
                        style={[
                          styles.cell,
                          styles.overallCell,
                          styles.valueCell,
                        ]}
                      >
                        {hasProAccess ? (
                          <Text style={styles.centerCellText}>{formatOverall(yourOverall)}</Text>
                        ) : (
                          <Ionicons name="lock-closed" size={12} color="rgba(148, 163, 184, 0.95)" />
                        )}
                      </View>
                      <View
                        style={[
                          styles.cell,
                          styles.overallCell,
                          styles.valueCell,
                          sortKey === "meta" && styles.activeColumnCell,
                        ]}
                      >
                        <Text style={styles.centerCellText}>{formatOverall(metaOverall)}</Text>
                      </View>
                      <View
                        style={[
                          styles.cell,
                          styles.overallCell,
                          styles.valueCell,
                          sortKey === "true" && styles.activeColumnCell,
                        ]}
                      >
                        <Text style={styles.centerCellText}>{formatOverall(trueOverall)}</Text>
                      </View>
                      <View
                        style={[
                          styles.cell,
                          styles.overallCell,
                          styles.valueCell,
                          sortKey === "ovr" && styles.activeColumnCell,
                        ]}
                      >
                        <Text style={styles.centerCellText}>{formatOverall(card.ovr)}</Text>
                      </View>

                      {ATTRIBUTE_COLUMNS.map((column) => (
                        <View
                          key={`${card.id}-${column.key}`}
                          style={[
                            styles.cell,
                            styles.attributeCell,
                            styles.valueCell,
                            sortKey === column.key && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>
                            {formatOverall(card[column.key])}
                          </Text>
                        </View>
                      ))}
                      <View style={[styles.cell, styles.quirksColumn]}>
                        <OverflowSubline text={quirkText} />
                      </View>
                      <View style={styles.rowRightEdge} />
                      </Pressable>
                    );
                  })
                )}
                </View>
              </Animated.ScrollView>
            </ScrollView>
          </View>
        </View>
        {loading ? (
          <View style={styles.tableLoadingOverlay}>
            <ActivityIndicator size="small" color={theme.colors.primary} />
          </View>
        ) : null}
      </View>

      <View style={styles.pagination}>
        <Pressable
          onPress={onPrev}
          disabled={loading || page === 1}
          style={[styles.pageButton, (loading || page === 1) && styles.pageButtonDisabled]}
        >
          <Text style={styles.pageButtonText}>Prev</Text>
        </Pressable>

        <Text style={styles.pageText}>Page {page}</Text>

        <Pressable
          onPress={onNext}
          disabled={loading || !hasNext}
          style={[styles.pageButton, (loading || !hasNext) && styles.pageButtonDisabled]}
        >
          <Text style={styles.pageButtonText}>Next</Text>
        </Pressable>
      </View>
    </View>
  );
}

type FilterDropdownButtonProps = {
  label: string;
  value: string;
  onPress: () => void;
};

type SortHeaderProps = {
  label: string;
  sortKey: SortKey;
  activeSortKey: SortKey;
  direction: SortDirection;
  onPress: (key: SortKey) => void;
  cellStyle: ViewStyle;
  center?: boolean;
  disabled?: boolean;
  showLock?: boolean;
};

function FilterDropdownButton({ label, value, onPress }: FilterDropdownButtonProps) {
  return (
    <Pressable style={styles.dropdownCompactButton} onPress={onPress}>
      <Text style={styles.dropdownCompactLabel}>{label}</Text>
      <Text style={styles.dropdownCompactValue} numberOfLines={1}>
        {value}
      </Text>
    </Pressable>
  );
}

function SortHeader({
  label,
  sortKey,
  activeSortKey,
  direction: _direction,
  onPress,
  cellStyle,
  center,
  disabled = false,
  showLock = false,
}: SortHeaderProps) {
  const isActive = !disabled && activeSortKey === sortKey;
  return (
    <Pressable
      onPress={() => {
        if (disabled) return;
        onPress(sortKey);
      }}
      disabled={disabled}
      style={[
        styles.headerPressable,
        cellStyle,
        center && styles.headerPressableCentered,
        disabled && styles.headerPressableDisabled,
        isActive && styles.headerPressableActive,
      ]}
    >
      <View style={[styles.headerLabelRow, center && styles.headerLabelRowCentered]}>
        <Text
          style={[
            styles.headerCell,
            center && styles.headerCellCentered,
            disabled && styles.headerCellDisabled,
            isActive && styles.headerCellActive,
          ]}
          numberOfLines={1}
        >
          {label}
        </Text>
        {showLock ? (
          <Ionicons name="lock-closed" size={10} color="rgba(148, 163, 184, 0.95)" />
        ) : null}
      </View>
    </Pressable>
  );
}

function OverflowSubline({ text, centered }: { text: string; centered?: boolean }) {
  return (
    <ScrollView
      horizontal
      nestedScrollEnabled
      showsHorizontalScrollIndicator={false}
      style={styles.sublineScroll}
      contentContainerStyle={[
        styles.sublineScrollContent,
        centered && styles.sublineScrollContentCentered,
      ]}
    >
      <Text style={styles.subCellText} numberOfLines={1}>
        {text}
      </Text>
    </ScrollView>
  );
}

function CardImage({ uri }: { uri: string | null }) {
  if (!uri) {
    return (
      <View style={styles.imagePlaceholder}>
        <Text style={styles.imagePlaceholderText}>N/A</Text>
      </View>
    );
  }
  return <Image source={{ uri }} style={styles.cardImage} resizeMode="cover" />;
}

function toggleSelection<T,>(items: T[], value: T): T[] {
  if (items.includes(value)) return items.filter((item) => item !== value);
  return [...items, value];
}

function summaryLabel(values: string[], fallback: string): string {
  if (values.length === 0) return fallback;
  if (values.length <= 2) return values.join(", ");
  return `${values[0]}, ${values[1]} +${values.length - 2}`;
}

async function fetchRankingCards(path: string): Promise<CardRanking[]> {
  try {
    return await apiGetAuth<CardRanking[]>(path);
  } catch (err) {
    const isAuthMissing =
      (err instanceof ApiError && (err.status === 401 || err.status === 403))
      || (err instanceof Error && err.message === "Not authenticated");
    if (!isAuthMissing) throw err;
    return apiGet<CardRanking[]>(path);
  }
}

function filterMenuTitle(menu: FilterMenuKey | null): string {
  if (menu === "year") return "Select Year";
  if (menu === "position") return "Select Positions";
  if (menu === "bat") return "Select Bats";
  if (menu === "pitch") return "Select Throws";
  return "Filter";
}

function normalizePosition(value: string | null | undefined) {
  return (value ?? "").trim().toUpperCase();
}

function primaryPosition(card: CardRanking) {
  return normalizePosition(card.display_primary_position);
}

function secondaryPositions(card: CardRanking): string[] {
  return secondaryPosition(card)
    .replace(/\//g, ",")
    .split(",")
    .map((value) => normalizePosition(value))
    .filter((value) => value.length > 0 && value !== "-");
}

function metricPositionForCard(
  card: CardRanking,
  selectedPositions: string[],
  includeSecondary: boolean
) {
  const primary = primaryPosition(card);
  if (selectedPositions.length === 0) return primary || null;
  if (selectedPositions.includes(primary)) return primary || null;
  if (!includeSecondary) return primary || null;

  const secondary = new Set(secondaryPositions(card));
  for (const position of selectedPositions) {
    const normalized = normalizePosition(position);
    if (secondary.has(normalized)) return normalized;
  }
  return primary || null;
}

function positionMapValue(map: Record<string, number> | null | undefined, position: string | null) {
  if (!map || !position) return null;
  const key = normalizePosition(position);
  const value = map[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function resolveOverallForPosition(
  map: Record<string, number> | null | undefined,
  position: string | null,
  rounded: number | null | undefined,
  exact: number | null | undefined
) {
  const mapped = positionMapValue(map, position);
  if (mapped !== null) return mapped;
  if (typeof rounded === "number" && Number.isFinite(rounded)) return rounded;
  if (typeof exact === "number" && Number.isFinite(exact)) return Math.round(exact);
  return null;
}

function secondaryPosition(card: CardRanking): string {
  return (
    card.display_secondary_position ||
    card.display_secondary_positions ||
    card.display_seconday_position ||
    "-"
  );
}

function formatHand(value: string | null | undefined) {
  const hand = (value ?? "").trim().toUpperCase();
  if (hand.startsWith("L")) return "L";
  if (hand.startsWith("R")) return "R";
  if (hand.startsWith("S")) return "S";
  return "--";
}

function formatOverall(value: number | null | undefined): string {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.round(value));
  return "-";
}

function matchesSelectedYear(cardYear: number | null | undefined, selectedYear: number): boolean {
  if (typeof cardYear !== "number" || !Number.isFinite(cardYear)) return false;
  const normalizedCardYear = cardYear >= 100 ? cardYear % 100 : cardYear;
  return normalizedCardYear === selectedYear;
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: theme.colors.background,
    paddingHorizontal: 10,
    paddingTop: 12,
    paddingBottom: 12,
    gap: 8,
  },
  header: {
    gap: 2,
  },
  title: {
    color: theme.colors.text,
    fontSize: 22,
    fontWeight: "800",
  },
  subtitle: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  filtersCardCompact: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
    borderRadius: 10,
    padding: 8,
    gap: 8,
    backgroundColor: "rgba(15, 23, 42, 0.82)",
  },
  filtersRowCompact: {
    flexDirection: "row",
    gap: 6,
    flexWrap: "wrap",
  },
  dropdownCompactButton: {
    flexGrow: 1,
    minWidth: 76,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.32)",
    backgroundColor: "rgba(30, 41, 59, 0.8)",
    borderRadius: 8,
    paddingHorizontal: 8,
    paddingVertical: 6,
  },
  dropdownCompactLabel: {
    color: "rgba(148, 163, 184, 0.95)",
    fontSize: 10,
    fontWeight: "700",
  },
  dropdownCompactValue: {
    color: "#e2e8f0",
    fontSize: 12,
    fontWeight: "700",
    marginTop: 1,
  },
  searchInput: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.32)",
    backgroundColor: "rgba(15, 23, 42, 0.7)",
    borderRadius: 8,
    color: "#f8fafc",
    fontSize: 12,
    fontWeight: "600",
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  modalInlineToggleRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.16)",
  },
  modalInlineToggleLabel: {
    color: "#cbd5e1",
    fontSize: 12,
    fontWeight: "700",
  },
  modalInlineToggleButton: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 5,
  },
  modalInlineToggleButtonSelected: {
    borderColor: "rgba(34, 197, 94, 0.75)",
    backgroundColor: "rgba(21, 128, 61, 0.25)",
  },
  modalInlineToggleButtonDisabled: {
    opacity: 0.45,
  },
  modalInlineToggleButtonText: {
    color: "#cbd5e1",
    fontSize: 11,
    fontWeight: "700",
  },
  modalInlineToggleButtonTextSelected: {
    color: "#dcfce7",
  },
  modalBackdrop: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "rgba(2, 6, 23, 0.6)",
    padding: 18,
  },
  modalOverlayClose: {
    ...StyleSheet.absoluteFillObject,
  },
  modalCard: {
    width: "100%",
    maxWidth: 420,
    maxHeight: "75%",
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
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
  modalList: {
    maxHeight: 330,
  },
  modalListContent: {
    padding: 10,
    gap: 6,
  },
  modalOption: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 8,
    backgroundColor: "rgba(15, 23, 42, 0.75)",
  },
  modalOptionSelected: {
    borderColor: "rgba(59, 130, 246, 0.7)",
    backgroundColor: "rgba(30, 64, 175, 0.28)",
  },
  modalOptionText: {
    color: "#cbd5e1",
    fontSize: 13,
    fontWeight: "700",
  },
  modalOptionTextSelected: {
    color: "#dbeafe",
  },
  modalCheck: {
    color: "rgba(148, 163, 184, 0.5)",
    fontSize: 14,
    fontWeight: "900",
    width: 18,
    textAlign: "center",
  },
  modalCheckSelected: {
    color: "#60a5fa",
  },
  modalFooter: {
    borderTopWidth: 1,
    borderTopColor: "rgba(148, 163, 184, 0.16)",
    paddingHorizontal: 10,
    paddingVertical: 9,
    alignItems: "flex-start",
  },
  modalClearButton: {
    borderWidth: 1,
    borderColor: "rgba(248, 113, 113, 0.5)",
    borderRadius: 7,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "rgba(153, 27, 27, 0.22)",
  },
  modalClearText: {
    color: "#fecaca",
    fontSize: 11,
    fontWeight: "700",
  },
  errorCard: {
    borderWidth: 1,
    borderColor: "rgba(248, 113, 113, 0.45)",
    backgroundColor: "rgba(127, 29, 29, 0.4)",
    borderRadius: 10,
    padding: 8,
  },
  errorText: {
    color: "#fecaca",
    fontSize: 12,
    fontWeight: "600",
  },
  tableContainer: {
    flex: 1,
    position: "relative",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    borderRadius: 10,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    overflow: "hidden",
  },
  tableSplit: {
    flex: 1,
    flexDirection: "row",
  },
  pinnedImageColumn: {
    width: 46,
    borderRightWidth: 1,
    borderRightColor: "rgba(148, 163, 184, 0.2)",
    backgroundColor: "rgba(15, 23, 42, 0.85)",
  },
  pinnedImageHeader: {
    minHeight: 40,
    alignItems: "center",
    justifyContent: "center",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.12)",
  },
  pinnedImageBody: {
    flex: 1,
    overflow: "hidden",
  },
  pinnedImageRows: {
    width: "100%",
  },
  pinnedImageCell: {
    minHeight: 56,
    alignItems: "center",
    justifyContent: "center",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.12)",
  },
  tableScrollPane: {
    flex: 1,
  },
  tableLoadingOverlay: {
    ...StyleSheet.absoluteFillObject,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(2, 6, 23, 0.22)",
  },
  tableContent: {
    minWidth: 2230,
  },
  row: {
    flexDirection: "row",
    alignItems: "stretch",
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.12)",
    minHeight: 56,
  },
  headerRow: {
    backgroundColor: "rgba(30, 41, 59, 0.95)",
    minHeight: 40,
  },
  evenRow: {
    backgroundColor: "rgba(15, 23, 42, 0.45)",
  },
  oddRow: {
    backgroundColor: "rgba(15, 23, 42, 0.2)",
  },
  loadingRow: {
    minHeight: 180,
    alignItems: "center",
    justifyContent: "center",
  },
  emptyText: {
    color: theme.colors.muted,
    fontSize: 14,
    fontWeight: "600",
  },
  cell: {
    color: theme.colors.text,
    fontSize: 12,
    paddingVertical: 3,
    paddingHorizontal: 1,
    fontWeight: "600",
    justifyContent: "center",
    alignItems: "center",
    alignSelf: "stretch",
    borderRightWidth: 1,
    borderRightColor: "rgba(148, 163, 184, 0.16)",
  },
  headerPressable: {
    justifyContent: "center",
    alignSelf: "stretch",
    paddingHorizontal: 1,
    borderRightWidth: 1,
    borderRightColor: "rgba(148, 163, 184, 0.2)",
  },
  headerStaticCell: {
    justifyContent: "center",
    alignSelf: "stretch",
    paddingHorizontal: 1,
    borderRightWidth: 1,
    borderRightColor: "rgba(148, 163, 184, 0.2)",
  },
  headerPressableCentered: {
    alignItems: "center",
  },
  headerPressableDisabled: {
    backgroundColor: "rgba(15, 23, 42, 0.55)",
  },
  headerPressableActive: {
    backgroundColor: "rgba(59, 130, 246, 0.16)",
  },
  headerLabelRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
  },
  headerLabelRowCentered: {
    justifyContent: "center",
  },
  headerCell: {
    color: "#cbd5e1",
    fontSize: 10,
    fontWeight: "800",
    letterSpacing: 0.2,
    paddingVertical: 8,
  },
  headerCellDisabled: {
    color: "rgba(148, 163, 184, 0.95)",
  },
  headerCellActive: {
    color: "#dbeafe",
  },
  headerCellCentered: {
    textAlign: "center",
  },
  mainCellText: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
    textAlign: "center",
  },
  subCellText: {
    color: "rgba(203, 213, 225, 0.7)",
    fontSize: 9,
    fontWeight: "600",
    textAlign: "center",
  },
  sublineScroll: {
    width: "100%",
    marginTop: 1,
  },
  sublineScrollContent: {
    paddingRight: 6,
  },
  sublineScrollContentCentered: {
    flexGrow: 1,
    justifyContent: "center",
  },
  centerCellText: {
    textAlign: "center",
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
  },
  stackCell: {
    justifyContent: "center",
    alignItems: "center",
    paddingTop: 4,
    paddingBottom: 0,
  },
  cardComboCell: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: 0,
    paddingVertical: 1,
  },
  cardThumbWrap: {
    width: 36,
    alignItems: "center",
    justifyContent: "center",
    marginTop: 1,
  },
  cardTextStack: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    paddingRight: 1,
    paddingTop: 2,
  },
  valueCell: {
    justifyContent: "center",
    alignItems: "center",
  },
  activeColumnCell: {
    backgroundColor: "rgba(59, 130, 246, 0.1)",
  },
  nameCell: {
    width: 152,
  },
  handsCell: {
    width: 48,
  },
  positionCell: {
    width: 72,
  },
  overallCell: {
    width: 52,
  },
  attributeCell: {
    width: 54,
  },
  quirksColumn: {
    width: 280,
  },
  rowRightEdge: {
    width: 1,
    alignSelf: "stretch",
    backgroundColor: "rgba(148, 163, 184, 0.16)",
  },
  cardImage: {
    width: 38,
    height: 50,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.28)",
    backgroundColor: "rgba(15, 23, 42, 0.7)",
  },
  imagePlaceholder: {
    width: 38,
    height: 50,
    borderRadius: 4,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    alignItems: "center",
    justifyContent: "center",
  },
  imagePlaceholderText: {
    color: theme.colors.muted,
    fontSize: 8,
    fontWeight: "700",
  },
  pagination: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  pageButton: {
    minWidth: 72,
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 9,
    borderWidth: 1,
    borderColor: "rgba(59, 130, 246, 0.5)",
    backgroundColor: "rgba(59, 130, 246, 0.22)",
    paddingVertical: 8,
    paddingHorizontal: 10,
  },
  pageButtonDisabled: {
    opacity: 0.45,
  },
  pageButtonText: {
    color: "#dbeafe",
    fontSize: 12,
    fontWeight: "700",
  },
  pageText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
});
