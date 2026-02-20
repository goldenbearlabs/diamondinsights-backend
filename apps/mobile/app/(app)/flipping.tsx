import { useCallback, useEffect, useMemo, useRef, useState } from "react";
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
import { useRouter } from "expo-router";

import {
  ApiError,
  FlippingRow,
  FlippingSortBy,
  GetFlippingRowsParams,
  getFlippingRows,
} from "../../src/lib/api";
import { theme } from "../../src/theme/colors";

const PAGE_SIZE = 30;
const PROFIT_GREEN = "#22c55e";
const LOSS_RED = "#f87171";

type SortDirection = "asc" | "desc";
type SortKey = FlippingSortBy;
const TEXT_SORT_KEYS: SortKey[] = ["name"];
type FilterMenuKey = "buy" | "sell" | "ovr";
type AppliedFilters = Pick<
  GetFlippingRowsParams,
  "min_buy" | "max_buy" | "min_sell" | "max_sell" | "min_ovr" | "max_ovr"
>;
type FilterDraft = {
  minBuy: string;
  maxBuy: string;
  minSell: string;
  maxSell: string;
  minOvr: string;
  maxOvr: string;
};

const EMPTY_FILTER_DRAFT: FilterDraft = {
  minBuy: "",
  maxBuy: "",
  minSell: "",
  maxSell: "",
  minOvr: "",
  maxOvr: "",
};

export default function Flipping() {
  const router = useRouter();
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [hasNext, setHasNext] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [rows, setRows] = useState<FlippingRow[]>([]);
  const [sortKey, setSortKey] = useState<SortKey>("profit_per_min");
  const [sortDirection, setSortDirection] = useState<SortDirection>("desc");
  const [profitableOnly, setProfitableOnly] = useState(false);
  const [openFilterMenu, setOpenFilterMenu] = useState<FilterMenuKey | null>(null);
  const [filterDraft, setFilterDraft] = useState<FilterDraft>(EMPTY_FILTER_DRAFT);
  const [appliedFilters, setAppliedFilters] = useState<AppliedFilters>({});
  const offset = useMemo(() => (page - 1) * PAGE_SIZE, [page]);
  const tableScrollY = useRef(new Animated.Value(0)).current;
  const pinnedRowsTranslateY = useMemo(
    () => Animated.multiply(tableScrollY, -1),
    [tableScrollY]
  );
  const onTableScroll = useMemo(
    () =>
      Animated.event(
        [{ nativeEvent: { contentOffset: { y: tableScrollY } } }],
        { useNativeDriver: true }
      ),
    [tableScrollY]
  );

  const fetchRows = useCallback(async () => {
    try {
      setError(null);
      const data = await getFlippingRows({
        limit: PAGE_SIZE,
        offset,
        sort_by: sortKey,
        sort_dir: sortDirection,
        profitable_only: profitableOnly,
        ...appliedFilters,
      });
      const nextRows = Array.isArray(data) ? data : [];
      setRows(nextRows);
      setHasNext(nextRows.length === PAGE_SIZE);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.body || `Error ${err.status}`);
      } else {
        setError("Failed to load flipping data.");
      }
      setRows([]);
      setHasNext(false);
    }
  }, [appliedFilters, offset, profitableOnly, sortDirection, sortKey]);

  useEffect(() => {
    setLoading(true);
    fetchRows().finally(() => setLoading(false));
  }, [fetchRows]);

  const positiveCount = useMemo(
    () => rows.filter((item) => item.profit > 0).length,
    [rows]
  );

  const onSortChange = (nextKey: SortKey) => {
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

  const onApplyMenuFilter = () => {
    if (!openFilterMenu) return;
    if (openFilterMenu === "buy") {
      setAppliedFilters((current) => ({
        ...current,
        min_buy: parseOptionalInt(filterDraft.minBuy),
        max_buy: parseOptionalInt(filterDraft.maxBuy),
      }));
    } else if (openFilterMenu === "sell") {
      setAppliedFilters((current) => ({
        ...current,
        min_sell: parseOptionalInt(filterDraft.minSell),
        max_sell: parseOptionalInt(filterDraft.maxSell),
      }));
    } else {
      setAppliedFilters((current) => ({
        ...current,
        min_ovr: parseOptionalInt(filterDraft.minOvr, 99),
        max_ovr: parseOptionalInt(filterDraft.maxOvr, 99),
      }));
    }
    setPage(1);
    setOpenFilterMenu(null);
  };

  const onClearMenuFilter = () => {
    if (!openFilterMenu) return;
    if (openFilterMenu === "buy") {
      setFilterDraft((current) => ({ ...current, minBuy: "", maxBuy: "" }));
      setAppliedFilters((current) => ({ ...current, min_buy: undefined, max_buy: undefined }));
    } else if (openFilterMenu === "sell") {
      setFilterDraft((current) => ({ ...current, minSell: "", maxSell: "" }));
      setAppliedFilters((current) => ({ ...current, min_sell: undefined, max_sell: undefined }));
    } else {
      setFilterDraft((current) => ({ ...current, minOvr: "", maxOvr: "" }));
      setAppliedFilters((current) => ({ ...current, min_ovr: undefined, max_ovr: undefined }));
    }
    setPage(1);
    setOpenFilterMenu(null);
  };

  const onPrev = () => {
    if (loading || page === 1) return;
    setPage((current) => Math.max(1, current - 1));
  };

  const onNext = () => {
    if (loading || !hasNext) return;
    setPage((current) => current + 1);
  };

  const openCard = (item: FlippingRow) => {
    router.push({
      pathname: "/(app)/card",
      params: {
        cardId: item.card_id,
        cardName: item.name || "Card",
        cardYear: String(item.year || ""),
        cardOvr: String(item.ovr),
        cardImg: item.baked_img || "",
      },
    });
  };

  return (
    <View style={styles.screen}>
      <View style={styles.header}>
        <Text style={styles.title}>Flipping</Text>
      </View>

      <View style={styles.filtersCardCompact}>
        <View style={styles.filtersRowCompact}>
          <FilterDropdownButton
            label="Buy"
            value={rangeSummary(appliedFilters.min_buy, appliedFilters.max_buy)}
            onPress={() => setOpenFilterMenu("buy")}
          />
          <FilterDropdownButton
            label="Sell"
            value={rangeSummary(appliedFilters.min_sell, appliedFilters.max_sell)}
            onPress={() => setOpenFilterMenu("sell")}
          />
          <FilterDropdownButton
            label="OVR"
            value={rangeSummary(appliedFilters.min_ovr, appliedFilters.max_ovr)}
            onPress={() => setOpenFilterMenu("ovr")}
          />
          <Pressable
            style={[
              styles.toggleButton,
              profitableOnly && styles.toggleButtonSelected,
            ]}
            onPress={() => {
              setProfitableOnly((current) => !current);
              setPage(1);
            }}
          >
            <Text
              style={[
                styles.toggleButtonText,
                profitableOnly && styles.toggleButtonTextSelected,
              ]}
            >
              {profitableOnly ? "Profitable: On" : "Profitable: Off"}
            </Text>
          </Pressable>
        </View>

        <Text style={styles.summary}>
          Showing {rows.length} cards. {positiveCount} currently profitable.
        </Text>
      </View>

      <Modal
        transparent
        visible={openFilterMenu !== null}
        animationType="fade"
        onRequestClose={() => setOpenFilterMenu(null)}
      >
        <View style={styles.modalBackdrop}>
          <Pressable style={styles.modalOverlayClose} onPress={() => setOpenFilterMenu(null)} />
          <View style={styles.modalCard}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>{filterMenuTitle(openFilterMenu)}</Text>
              <Pressable style={styles.modalDoneButton} onPress={() => setOpenFilterMenu(null)}>
                <Text style={styles.modalDoneText}>Done</Text>
              </Pressable>
            </View>

            <View style={styles.modalRangeBody}>
              <TextInput
                style={styles.filterInput}
                value={selectedMenuMinValue(openFilterMenu, filterDraft)}
                onChangeText={(value) => onMenuMinChange(openFilterMenu, value, setNumericDraft)}
                placeholder="Min"
                placeholderTextColor="rgba(148, 163, 184, 0.75)"
                keyboardType="numeric"
              />
              <TextInput
                style={styles.filterInput}
                value={selectedMenuMaxValue(openFilterMenu, filterDraft)}
                onChangeText={(value) => onMenuMaxChange(openFilterMenu, value, setNumericDraft)}
                placeholder="Max"
                placeholderTextColor="rgba(148, 163, 184, 0.75)"
                keyboardType="numeric"
              />
            </View>

            <View style={styles.modalFooter}>
              <Pressable style={styles.modalApplyButton} onPress={onApplyMenuFilter}>
                <Text style={styles.modalApplyText}>Apply</Text>
              </Pressable>
              <Pressable style={styles.modalClearButton} onPress={onClearMenuFilter}>
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
                {rows.map((item, index) => (
                  <Pressable
                    key={`pinned-${item.card_id}`}
                    style={[styles.pinnedImageCell, index % 2 === 0 ? styles.evenRow : styles.oddRow]}
                    onPress={() => openCard(item)}
                  >
                    <CardImage uri={item.baked_img || null} />
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
                      cellStyle={styles.cardCell}
                      center
                    />
                    <SortHeader
                      label="OVR"
                      sortKey="ovr"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.ovrCell}
                      center
                    />
                    <SortHeader
                      label="Profit/Min"
                      sortKey="profit_per_min"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.profitRateCell}
                      center
                    />
                    <SortHeader
                      label="Profit"
                      sortKey="profit"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.moneyCell}
                      center
                    />
                    <SortHeader
                      label="Buy"
                      sortKey="buy"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.moneyCell}
                      center
                    />
                    <SortHeader
                      label="Sell"
                      sortKey="sell"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.moneyCell}
                      center
                    />
                    <SortHeader
                      label="Profit %"
                      sortKey="margin"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.marginCell}
                      center
                    />
                    <SortHeader
                      label="Buys/Sells"
                      sortKey="buys_sells"
                      activeSortKey={sortKey}
                      direction={sortDirection}
                      onPress={onSortChange}
                      cellStyle={styles.buysSellsCell}
                      center
                    />
                  </View>

                  {rows.length === 0 && !loading ? (
                    <View style={styles.loadingRow}>
                      <Text style={styles.emptyText}>No flipping rows found.</Text>
                    </View>
                  ) : (
                    rows.map((item, index) => (
                      <View
                        key={item.card_id}
                        style={[styles.row, index % 2 === 0 ? styles.evenRow : styles.oddRow]}
                      >
                        <View
                          style={[
                            styles.cell,
                            styles.cardCell,
                            styles.stackCell,
                            sortKey === "name" && styles.activeColumnCell,
                          ]}
                        >
                          <Pressable onPress={() => openCard(item)} style={styles.cardNamePressable}>
                            <Text style={styles.mainCellText} numberOfLines={1}>
                              {item.name || "Unknown Card"}
                            </Text>
                          </Pressable>
                          <OverflowSubline
                            text={`${item.team || "-"} | ${item.series || "-"}${item.year ? ` | ${item.year}` : ""}`}
                            centered
                          />
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.ovrCell,
                            styles.valueCell,
                            sortKey === "ovr" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>{item.ovr}</Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.profitRateCell,
                            styles.valueCell,
                            sortKey === "profit_per_min" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={[styles.centerCellText, computeProfitPerMin(item) >= 0 ? styles.positiveText : styles.negativeText]}>
                            {formatRateStubs(computeProfitPerMin(item))}
                          </Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.moneyCell,
                            styles.valueCell,
                            sortKey === "profit" && styles.activeColumnCell,
                          ]}
                        >
                          <Text
                            style={[
                              styles.centerCellText,
                              item.profit >= 0 ? styles.positiveText : styles.negativeText,
                            ]}
                          >
                            {formatSignedStubs(item.profit)}
                          </Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.moneyCell,
                            styles.valueCell,
                            sortKey === "buy" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>
                            {formatStubs(item.effective_buy_price)}
                            {item.uses_quicksell_buy ? "*" : ""}
                          </Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.moneyCell,
                            styles.valueCell,
                            sortKey === "sell" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>{formatStubs(item.best_sell_price)}</Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.marginCell,
                            styles.valueCell,
                            sortKey === "margin" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>{formatPct(item.profit_margin_pct)}</Text>
                        </View>

                        <View
                          style={[
                            styles.cell,
                            styles.buysSellsCell,
                            styles.valueCell,
                            sortKey === "buys_sells" && styles.activeColumnCell,
                          ]}
                        >
                          <Text style={styles.centerCellText}>{item.buys_1h}/{item.sells_1h}</Text>
                        </View>
                        <View style={styles.rowRightEdge} />
                      </View>
                    ))
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

type SortHeaderProps = {
  label: string;
  sortKey: SortKey;
  activeSortKey: SortKey;
  direction: SortDirection;
  onPress: (key: SortKey) => void;
  cellStyle: ViewStyle;
  center?: boolean;
};

type FilterDropdownButtonProps = {
  label: string;
  value: string;
  onPress: () => void;
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
  direction,
  onPress,
  cellStyle,
  center,
}: SortHeaderProps) {
  const isActive = activeSortKey === sortKey;
  return (
    <Pressable
      onPress={() => onPress(sortKey)}
      style={[
        styles.headerPressable,
        cellStyle,
        center && styles.headerPressableCentered,
        isActive && styles.headerPressableActive,
      ]}
    >
      <Text
        style={[
          styles.headerCell,
          center && styles.headerCellCentered,
          isActive && styles.headerCellActive,
        ]}
        numberOfLines={1}
      >
        {label}
        {isActive ? (direction === "desc" ? " ↓" : " ↑") : ""}
      </Text>
    </Pressable>
  );
}

function filterMenuTitle(menu: FilterMenuKey | null): string {
  if (menu === "buy") return "Set Buy Range";
  if (menu === "sell") return "Set Sell Range";
  if (menu === "ovr") return "Set OVR Range";
  return "Filter";
}

function rangeSummary(min?: number, max?: number): string {
  if (min == null && max == null) return "Any";
  if (min != null && max != null) return `${min}-${max}`;
  if (min != null) return `${min}+`;
  return `<=${max}`;
}

function selectedMenuMinValue(menu: FilterMenuKey | null, draft: FilterDraft): string {
  if (menu === "buy") return draft.minBuy;
  if (menu === "sell") return draft.minSell;
  if (menu === "ovr") return draft.minOvr;
  return "";
}

function selectedMenuMaxValue(menu: FilterMenuKey | null, draft: FilterDraft): string {
  if (menu === "buy") return draft.maxBuy;
  if (menu === "sell") return draft.maxSell;
  if (menu === "ovr") return draft.maxOvr;
  return "";
}

function onMenuMinChange(
  menu: FilterMenuKey | null,
  value: string,
  setNumericDraft: (key: keyof FilterDraft, value: string) => void
) {
  if (menu === "buy") setNumericDraft("minBuy", value);
  if (menu === "sell") setNumericDraft("minSell", value);
  if (menu === "ovr") setNumericDraft("minOvr", value);
}

function onMenuMaxChange(
  menu: FilterMenuKey | null,
  value: string,
  setNumericDraft: (key: keyof FilterDraft, value: string) => void
) {
  if (menu === "buy") setNumericDraft("maxBuy", value);
  if (menu === "sell") setNumericDraft("maxSell", value);
  if (menu === "ovr") setNumericDraft("maxOvr", value);
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

function formatStubs(value: number): string {
  return `$${Math.round(value).toLocaleString()}`;
}

function formatSignedStubs(value: number): string {
  if (value > 0) return `+$${Math.round(value).toLocaleString()}`;
  if (value < 0) return `-$${Math.abs(Math.round(value)).toLocaleString()}`;
  return "$0";
}

function formatRateStubs(value: number): string {
  const rounded = Math.round(value);
  if (rounded > 0) return `+$${rounded.toLocaleString()}`;
  if (rounded < 0) return `-$${Math.abs(rounded).toLocaleString()}`;
  return "$0";
}

function formatPct(value: number | null): string {
  if (value == null || Number.isNaN(value)) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

function parseOptionalInt(value: string, max: number | null = null): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number.parseInt(trimmed, 10);
  if (!Number.isFinite(parsed) || parsed < 0) return undefined;
  if (max != null) return Math.min(parsed, max);
  return parsed;
}

function computeProfitPerMin(item: FlippingRow): number {
  return ((item.orders_1h / 2) * item.profit) / 60;
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
    gap: 8,
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
  filterInput: {
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
  summary: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
  },
  toggleButton: {
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
    backgroundColor: "rgba(15, 23, 42, 0.75)",
    borderRadius: 8,
    paddingHorizontal: 10,
    paddingVertical: 7,
  },
  toggleButtonSelected: {
    borderColor: "rgba(34, 197, 94, 0.75)",
    backgroundColor: "rgba(21, 128, 61, 0.25)",
  },
  toggleButtonText: {
    color: "#cbd5e1",
    fontSize: 11,
    fontWeight: "700",
  },
  toggleButtonTextSelected: {
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
  modalRangeBody: {
    padding: 12,
    gap: 8,
  },
  modalFooter: {
    borderTopWidth: 1,
    borderTopColor: "rgba(148, 163, 184, 0.16)",
    paddingHorizontal: 10,
    paddingVertical: 9,
    flexDirection: "row",
    gap: 8,
  },
  modalApplyButton: {
    borderWidth: 1,
    borderColor: "rgba(34, 197, 94, 0.65)",
    borderRadius: 7,
    paddingHorizontal: 10,
    paddingVertical: 5,
    backgroundColor: "rgba(21, 128, 61, 0.28)",
  },
  modalApplyText: {
    color: "#dcfce7",
    fontSize: 11,
    fontWeight: "700",
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
    minWidth: 740,
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
  headerPressableCentered: {
    alignItems: "center",
  },
  headerPressableActive: {
    backgroundColor: "rgba(59, 130, 246, 0.16)",
  },
  headerCell: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: "700",
    letterSpacing: 0.3,
    textTransform: "uppercase",
    paddingVertical: 8,
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
    fontWeight: "600",
    textAlign: "center",
  },
  cardNamePressable: {
    width: "100%",
    alignItems: "center",
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
  valueCell: {
    justifyContent: "center",
    alignItems: "center",
  },
  activeColumnCell: {
    backgroundColor: "rgba(59, 130, 246, 0.1)",
  },
  cardCell: {
    width: 180,
  },
  ovrCell: {
    width: 52,
  },
  moneyCell: {
    width: 84,
  },
  marginCell: {
    width: 74,
  },
  buysSellsCell: {
    width: 88,
  },
  profitRateCell: {
    width: 92,
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
  positiveText: {
    color: PROFIT_GREEN,
  },
  negativeText: {
    color: LOSS_RED,
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
