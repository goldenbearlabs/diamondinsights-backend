import React from "react";
import {
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  useWindowDimensions,
} from "react-native";
import { Ionicons } from "@expo/vector-icons";
import { theme } from "../theme/colors";

export type HitDataStat = "count" | "share" | "babip" | "woba" | "slug";
type HitFilterSide = "all" | "left" | "right";
export type HitZoneKey =
  | "infield_left"
  | "infield_right"
  | "outfield_left"
  | "outfield_center"
  | "outfield_right"
  | "homerun_left"
  | "homerun_center"
  | "homerun_right";

export type HitDataMap = {
  zones: Record<HitZoneKey, number>;
  total: number;
  pa: number;
  stat: HitDataStat;
  stats: {
    sweet_spot_pct: number;
    popup_rate: number;
    flyball_rate: number;
    groundball_rate?: number;
    gb_air_ratio: number;
    pulled_air_rate: number;
    oppo_air_rate: number;
    perfect_perfect_pct: number;
    extreme_contact_nopp_pct?: number;
  };
};

type PitcherSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  pitch_hand_code?: string | null;
};

type HitterSearchResult = {
  mlb_id: number;
  full_name: string;
  first_name?: string | null;
  last_name?: string | null;
  bat_side_code?: string | null;
};

type HitDataSectionProps = {
  data: HitDataMap | null;
  hitMode: "Hitting" | "Pitching";
  statKey: HitDataStat;
  statOptions: { label: string; value: HitDataStat }[];
  hitterOptions: { label: string; value: HitFilterSide }[];
  pitcherOptions: { label: string; value: HitFilterSide }[];
  hitterValue: HitFilterSide;
  pitcherValue: HitFilterSide;
  statLabel: string;
  hitterLabel: string;
  pitcherLabel: string;
  selectedHitter?: HitterSearchResult | null;
  selectedPitcher?: PitcherSearchResult | null;
  onSelectMode: (mode: "Hitting" | "Pitching") => void;
  onSelectStat: (value: HitDataStat) => void;
  onSelectHitter: (value: HitFilterSide) => void;
  onSelectPitcher: (value: HitFilterSide) => void;
  onSelectHitterPlayer?: (
    hitter: HitterSearchResult | null,
    opts?: { closeMenu?: boolean },
  ) => void;
  onSelectPitcherPlayer?: (
    pitcher: PitcherSearchResult | null,
    opts?: { closeMenu?: boolean },
  ) => void;
  hitterSearchQuery?: string;
  setHitterSearchQuery?: (value: string) => void;
  hitterSearchResults?: HitterSearchResult[];
  hitterSearchLoading?: boolean;
  hitterSearchError?: string | null;
  pitcherSearchQuery?: string;
  setPitcherSearchQuery?: (value: string) => void;
  pitcherSearchResults?: PitcherSearchResult[];
  pitcherSearchLoading?: boolean;
  pitcherSearchError?: string | null;
  onStatPress?: (label: string) => void;
  onResetFilters: () => void;
  onOpenAdvancedFilters: () => void;
  hasAdvancedFilters: boolean;
  selectedZone?: HitZoneKey | null;
  onSelectZone?: (zone: HitZoneKey) => void;
};

const EMPTY_ZONES: Record<HitZoneKey, number> = {
  infield_left: 0,
  infield_right: 0,
  outfield_left: 0,
  outfield_center: 0,
  outfield_right: 0,
  homerun_left: 0,
  homerun_center: 0,
  homerun_right: 0,
};

const formatRate = (value: number) => {
  if (Number.isNaN(value)) return "—";
  const fixed = value.toFixed(3);
  return value < 1 ? fixed.replace(/^0/, "") : fixed;
};

const formatPercent = (value: number) => `${Math.round(value)}%`;
const formatPercentValue = (value?: number | null) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return formatPercent(value);
};

const formatPlayerName = (player?: {
  full_name?: string | null;
  first_name?: string | null;
  last_name?: string | null;
  mlb_id?: number | null;
}) => {
  if (!player) return "—";
  const full = (player.full_name || "").trim();
  if (full) return full;
  const assembled = [player.first_name, player.last_name].filter(Boolean).join(" ").trim();
  if (assembled) return assembled;
  return player.mlb_id != null ? `Player ${player.mlb_id}` : "Player";
};

const formatPitcherHand = (pitcher?: PitcherSearchResult | null) => {
  const code = (pitcher?.pitch_hand_code || "").toUpperCase();
  if (!code) return "Pitcher";
  if (code === "L") return "LHP";
  if (code === "R") return "RHP";
  if (code === "S") return "SHP";
  return `${code}HP`;
};

const formatHitterSide = (hitter?: HitterSearchResult | null) => {
  const code = (hitter?.bat_side_code || "").toUpperCase();
  if (!code) return "Hitter";
  if (code === "L") return "LHB";
  if (code === "R") return "RHB";
  if (code === "S") return "Switch";
  return `${code}HB`;
};

export const HitDataSection = ({
  data,
  hitMode,
  statKey,
  statOptions,
  hitterOptions,
  pitcherOptions,
  hitterValue,
  pitcherValue,
  statLabel,
  hitterLabel,
  pitcherLabel,
  selectedHitter,
  selectedPitcher,
  onSelectMode,
  onSelectStat,
  onSelectHitter,
  onSelectPitcher,
  onSelectHitterPlayer,
  onSelectPitcherPlayer,
  hitterSearchQuery,
  setHitterSearchQuery,
  hitterSearchResults,
  hitterSearchLoading,
  hitterSearchError,
  pitcherSearchQuery,
  setPitcherSearchQuery,
  pitcherSearchResults,
  pitcherSearchLoading,
  pitcherSearchError,
  onStatPress,
  onResetFilters,
  onOpenAdvancedFilters,
  hasAdvancedFilters,
  selectedZone,
  onSelectZone,
}: HitDataSectionProps) => {
  const { width } = useWindowDimensions();
  const [activeMenu, setActiveMenu] = React.useState<null | "stat" | "hitter" | "pitcher">(
    null,
  );

  const sectionPaddingX = 14;
  const sectionPaddingY = 14;
  const contentGap = 12;

  const containerWidth = Math.max(0, Math.round(width - 24));
  const contentWidth = Math.max(0, containerWidth - sectionPaddingX * 2);

  const statsColumnWidth = Math.min(140, Math.max(96, Math.round(contentWidth * 0.32)));
  const fieldWidth = Math.max(0, Math.round(contentWidth - statsColumnWidth - contentGap));
  const fieldHeight = Math.round(fieldWidth * 1);

  const padding = Math.round(fieldWidth * 0.07);
  const homeX = fieldWidth / 2;
  const homeY = fieldHeight - padding;

  const infieldSize = Math.round(fieldWidth * 0.3);
  const diamondRadius = infieldSize / Math.SQRT2;
  const diamondCenterY = homeY - diamondRadius;

  const infieldDirtSize = Math.round(infieldSize * 1.55);
  const baseSize = Math.max(10, Math.round(fieldWidth * 0.05));
  const moundSize = Math.max(8, Math.round(fieldWidth * 0.04));

  const lineThickness = Math.max(1, Math.round(fieldWidth * 0.012));

  const lineLength = Math.round(fieldWidth * 0.78);
  const splitLineLength = Math.round(lineLength * 1.25);
  const foulLineLength = splitLineLength;

  const outfieldRadius = lineLength;
  const infieldBoundaryRadius = Math.min(
    outfieldRadius * 0.58,
    diamondRadius + infieldDirtSize * 0.6,
  );

  const leftAngle = (-135 * Math.PI) / 180;
  const rightAngle = (-45 * Math.PI) / 180;

  const leftCenterX = homeX + Math.cos(leftAngle) * (foulLineLength / 2);
  const leftCenterY = homeY + Math.sin(leftAngle) * (foulLineLength / 2);
  const rightCenterX = homeX + Math.cos(rightAngle) * (foulLineLength / 2);
  const rightCenterY = homeY + Math.sin(rightAngle) * (foulLineLength / 2);
  const splitAngles = [-105, -75];
  const splitLineStartRadius = infieldBoundaryRadius;
  const splitLineEndRadius = splitLineLength;
  const splitLineSegment = Math.max(0, splitLineEndRadius - splitLineStartRadius);
  const splitLines = splitAngles.map((deg) => {
    const radians = (deg * Math.PI) / 180;
    return {
      key: `${deg}`,
      left: homeX + Math.cos(radians) * (splitLineStartRadius + splitLineSegment / 2),
      top: homeY + Math.sin(radians) * (splitLineStartRadius + splitLineSegment / 2),
      rotate: `${deg}deg`,
    };
  });
  const splitThickness = Math.max(1, Math.round(lineThickness * 0.75));
  const centerLineLength = Math.round(infieldBoundaryRadius * 0.98);
  const centerLineAngle = -90;
  const centerLineRadians = (centerLineAngle * Math.PI) / 180;
  const centerLineX = homeX + Math.cos(centerLineRadians) * (centerLineLength / 2);
  const centerLineY = homeY + Math.sin(centerLineRadians) * (centerLineLength / 2);

  const basePositions = [
    { key: "third", x: homeX - diamondRadius, y: diamondCenterY },
    { key: "second", x: homeX, y: diamondCenterY - diamondRadius },
    { key: "first", x: homeX + diamondRadius, y: diamondCenterY },
  ];

  const moundY = homeY - diamondRadius * 0.96;
  const labelWidth = Math.max(36, Math.round(fieldWidth * 0.22));
  const labelHeight = Math.max(22, Math.round(fieldWidth * 0.11));
  const outfieldLabelRadius = (infieldBoundaryRadius + outfieldRadius) / 2;
  const homerunLabelRadius = Math.min(
    homeY - labelHeight * 0.8,
    outfieldRadius + (outfieldRadius - infieldBoundaryRadius) * 0.55,
  );
  const makeZoneLabel = (
    zoneKey: HitZoneKey,
    value: string,
    angleDeg: number,
    radius: number,
    variant: "infield" | "outfield" | "homerun",
  ) => {
    const radians = (angleDeg * Math.PI) / 180;
    const x = homeX + Math.cos(radians) * radius;
    const y = homeY + Math.sin(radians) * radius;
    return {
      key: zoneKey,
      zoneKey,
      value,
      variant,
      left: x - labelWidth / 2,
      top: y - labelHeight / 2,
    };
  };
  const zoneValues = data?.zones ?? EMPTY_ZONES;
  const formatValue = (value: number) => {
    if (statKey === "count") return Math.round(value).toString();
    if (statKey === "share") return formatPercent(value);
    return formatRate(value);
  };
  const hitZoneLabels = [
    makeZoneLabel(
      "infield_left",
      formatValue(zoneValues.infield_left),
      -112.5,
      infieldBoundaryRadius * 0.55,
      "infield",
    ),
    makeZoneLabel(
      "infield_right",
      formatValue(zoneValues.infield_right),
      -67.5,
      infieldBoundaryRadius * 0.55,
      "infield",
    ),
    makeZoneLabel(
      "outfield_left",
      formatValue(zoneValues.outfield_left),
      -120,
      outfieldLabelRadius,
      "outfield",
    ),
    makeZoneLabel(
      "outfield_center",
      formatValue(zoneValues.outfield_center),
      -90,
      outfieldLabelRadius,
      "outfield",
    ),
    makeZoneLabel(
      "outfield_right",
      formatValue(zoneValues.outfield_right),
      -60,
      outfieldLabelRadius,
      "outfield",
    ),
    makeZoneLabel(
      "homerun_left",
      formatValue(zoneValues.homerun_left),
      -120,
      homerunLabelRadius,
      "homerun",
    ),
    makeZoneLabel(
      "homerun_center",
      formatValue(zoneValues.homerun_center),
      -90,
      homerunLabelRadius,
      "homerun",
    ),
    makeZoneLabel(
      "homerun_right",
      formatValue(zoneValues.homerun_right),
      -60,
      homerunLabelRadius,
      "homerun",
    ),
  ];
  const stats = data?.stats;
  const hitStatCards = [
    { label: "Sweet Spot%", value: formatPercentValue(stats?.sweet_spot_pct) },
    { label: "Popup%", value: formatPercentValue(stats?.popup_rate) },
    { label: "Flyball%", value: formatPercentValue(stats?.flyball_rate) },
    { label: "GB/Air%", value: formatPercentValue(stats?.gb_air_ratio) },
    { label: "Pulled Air%", value: formatPercentValue(stats?.pulled_air_rate) },
    { label: "Oppo Air%", value: formatPercentValue(stats?.oppo_air_rate) },
    { label: "Perfect Perfect%", value: formatPercentValue(stats?.perfect_perfect_pct) },
  ];

  return (
    <View
      style={[
        styles.section,
        {
          paddingHorizontal: sectionPaddingX,
          paddingVertical: sectionPaddingY,
        },
      ]}
    >
      <View style={styles.sectionHeaderRow}>
        <View style={styles.sectionHeader}>
          <Text style={styles.title}>Hit Data</Text>
          <Text style={styles.subtitle}>Hit locations and spray tendencies.</Text>
        </View>
        <View style={styles.sectionToggle}>
          {(["Hitting", "Pitching"] as const).map((mode) => (
            <TouchableOpacity
              key={mode}
              style={[
                styles.sectionToggleButton,
                hitMode === mode && styles.sectionToggleButtonActive,
              ]}
              onPress={() => {
                setActiveMenu(null);
                onSelectMode(mode);
              }}
            >
              <Text
                style={[
                  styles.sectionToggleText,
                  hitMode === mode && styles.sectionToggleTextActive,
                ]}
              >
                {mode}
              </Text>
            </TouchableOpacity>
          ))}
        </View>
      </View>

      <View style={styles.hitFiltersStack}>
        <View style={styles.hitFiltersRow}>
          <View style={styles.hitFilterColumn}>
            <TouchableOpacity
              style={styles.hitFilterToggle}
              activeOpacity={0.8}
              onPress={() => setActiveMenu((prev) => (prev === "stat" ? null : "stat"))}
            >
              <Text style={styles.hitFilterToggleText} numberOfLines={1} ellipsizeMode="tail">
                {statLabel}
              </Text>
              <Text style={styles.hitFilterToggleIcon}>v</Text>
            </TouchableOpacity>
            {activeMenu === "stat" ? (
              <View style={styles.hitFilterMenu}>
                {statOptions.map((option, index) => {
                  const isLast = index === statOptions.length - 1;
                  const active = option.value === statKey;
                  return (
                    <TouchableOpacity
                      key={option.value}
                      style={[
                        styles.hitFilterItem,
                        isLast && styles.hitFilterItemLast,
                        active && styles.hitFilterItemActive,
                      ]}
                      onPress={() => {
                        onSelectStat(option.value);
                        setActiveMenu(null);
                      }}
                    >
                      <Text
                        style={[
                          styles.hitFilterItemText,
                          active && styles.hitFilterItemTextActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            ) : null}
          </View>
          <View style={styles.hitFilterColumn}>
            <TouchableOpacity
              style={styles.hitFilterToggle}
              activeOpacity={0.8}
              onPress={() => setActiveMenu((prev) => (prev === "hitter" ? null : "hitter"))}
            >
              <Text style={styles.hitFilterToggleText} numberOfLines={1} ellipsizeMode="tail">
                {hitterLabel}
              </Text>
              <Text style={styles.hitFilterToggleIcon}>v</Text>
            </TouchableOpacity>
            {activeMenu === "hitter" ? (
              <View style={styles.hitFilterMenu}>
                {selectedHitter && onSelectHitterPlayer ? (
                  <View style={styles.hitActiveRow}>
                    <View style={styles.hitActiveText}>
                      <Text style={styles.hitActiveLabel}>Selected hitter</Text>
                      <Text style={styles.hitActiveName} numberOfLines={1}>
                        {formatPlayerName(selectedHitter)}
                      </Text>
                    </View>
                    <TouchableOpacity
                      style={styles.hitClearButton}
                      onPress={() => onSelectHitterPlayer(null)}
                    >
                      <Text style={styles.hitClearText}>Clear</Text>
                    </TouchableOpacity>
                  </View>
                ) : null}

                {setHitterSearchQuery && hitterSearchQuery !== undefined ? (
                  <View style={styles.hitSearchBlock}>
                    <TextInput
                      value={hitterSearchQuery}
                      onChangeText={setHitterSearchQuery}
                      placeholder="Search hitter..."
                      placeholderTextColor={theme.colors.muted}
                      autoCapitalize="words"
                      autoCorrect={false}
                      style={styles.hitSearchInput}
                    />
                    <View style={styles.hitSearchResults}>
                      {hitterSearchLoading ? (
                        <Text style={styles.hitSearchStatus}>Searching...</Text>
                      ) : hitterSearchError ? (
                        <Text style={styles.hitSearchStatus}>{hitterSearchError}</Text>
                      ) : (hitterSearchResults?.length ?? 0) === 0 &&
                        hitterSearchQuery.trim() ? (
                        <Text style={styles.hitSearchStatus}>No matches.</Text>
                      ) : (
                        hitterSearchResults?.map((hitter) => (
                          <TouchableOpacity
                            key={hitter.mlb_id}
                            style={styles.hitSearchRow}
                            onPress={() => {
                              onSelectHitterPlayer?.(hitter);
                              setActiveMenu(null);
                            }}
                          >
                            <View style={styles.hitSearchText}>
                              <Text style={styles.hitSearchName} numberOfLines={2}>
                                {formatPlayerName(hitter)}
                              </Text>
                              <Text style={styles.hitSearchMeta}>
                                {formatHitterSide(hitter)}
                              </Text>
                            </View>
                          </TouchableOpacity>
                        ))
                      )}
                    </View>
                  </View>
                ) : null}

                {(setHitterSearchQuery || selectedHitter) ? (
                  <View style={styles.hitSearchDivider} />
                ) : null}

                {hitterOptions.map((option, index) => {
                  const isLast = index === hitterOptions.length - 1;
                  const active = option.value === hitterValue;
                  return (
                    <TouchableOpacity
                      key={option.value}
                      style={[
                        styles.hitFilterItem,
                        isLast && styles.hitFilterItemLast,
                        active && styles.hitFilterItemActive,
                      ]}
                      onPress={() => {
                        onSelectHitter(option.value);
                        onSelectHitterPlayer?.(null, { closeMenu: false });
                        setActiveMenu(null);
                      }}
                    >
                      <Text
                        style={[
                          styles.hitFilterItemText,
                          active && styles.hitFilterItemTextActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            ) : null}
          </View>
        </View>
        <View style={styles.hitFiltersRowSecondary}>
          <View style={styles.hitFilterColumn}>
            <TouchableOpacity
              style={styles.hitFilterToggle}
              activeOpacity={0.8}
              onPress={() => setActiveMenu((prev) => (prev === "pitcher" ? null : "pitcher"))}
            >
              <Text style={styles.hitFilterToggleText} numberOfLines={1} ellipsizeMode="tail">
                {pitcherLabel}
              </Text>
              <Text style={styles.hitFilterToggleIcon}>v</Text>
            </TouchableOpacity>
            {activeMenu === "pitcher" ? (
              <View style={styles.hitFilterMenu}>
                {selectedPitcher && onSelectPitcherPlayer ? (
                  <View style={styles.hitActiveRow}>
                    <View style={styles.hitActiveText}>
                      <Text style={styles.hitActiveLabel}>Selected pitcher</Text>
                      <Text style={styles.hitActiveName} numberOfLines={1}>
                        {formatPlayerName(selectedPitcher)}
                      </Text>
                    </View>
                    <TouchableOpacity
                      style={styles.hitClearButton}
                      onPress={() => onSelectPitcherPlayer(null)}
                    >
                      <Text style={styles.hitClearText}>Clear</Text>
                    </TouchableOpacity>
                  </View>
                ) : null}

                {setPitcherSearchQuery && pitcherSearchQuery !== undefined ? (
                  <View style={styles.hitSearchBlock}>
                    <TextInput
                      value={pitcherSearchQuery}
                      onChangeText={setPitcherSearchQuery}
                      placeholder="Search pitcher..."
                      placeholderTextColor={theme.colors.muted}
                      autoCapitalize="words"
                      autoCorrect={false}
                      style={styles.hitSearchInput}
                    />
                    <View style={styles.hitSearchResults}>
                      {pitcherSearchLoading ? (
                        <Text style={styles.hitSearchStatus}>Searching...</Text>
                      ) : pitcherSearchError ? (
                        <Text style={styles.hitSearchStatus}>{pitcherSearchError}</Text>
                      ) : (pitcherSearchResults?.length ?? 0) === 0 &&
                        pitcherSearchQuery.trim() ? (
                        <Text style={styles.hitSearchStatus}>No matches.</Text>
                      ) : (
                        pitcherSearchResults?.map((pitcher) => (
                          <TouchableOpacity
                            key={pitcher.mlb_id}
                            style={styles.hitSearchRow}
                            onPress={() => {
                              onSelectPitcherPlayer?.(pitcher);
                              setActiveMenu(null);
                            }}
                          >
                            <View style={styles.hitSearchText}>
                              <Text style={styles.hitSearchName} numberOfLines={2}>
                                {formatPlayerName(pitcher)}
                              </Text>
                              <Text style={styles.hitSearchMeta}>
                                {formatPitcherHand(pitcher)}
                              </Text>
                            </View>
                          </TouchableOpacity>
                        ))
                      )}
                    </View>
                  </View>
                ) : null}

                {(setPitcherSearchQuery || selectedPitcher) ? (
                  <View style={styles.hitSearchDivider} />
                ) : null}

                {pitcherOptions.map((option, index) => {
                  const isLast = index === pitcherOptions.length - 1;
                  const active = option.value === pitcherValue;
                  return (
                    <TouchableOpacity
                      key={option.value}
                      style={[
                        styles.hitFilterItem,
                        isLast && styles.hitFilterItemLast,
                        active && styles.hitFilterItemActive,
                      ]}
                      onPress={() => {
                        onSelectPitcher(option.value);
                        onSelectPitcherPlayer?.(null, { closeMenu: false });
                        setActiveMenu(null);
                      }}
                    >
                      <Text
                        style={[
                          styles.hitFilterItemText,
                          active && styles.hitFilterItemTextActive,
                        ]}
                      >
                        {option.label}
                      </Text>
                    </TouchableOpacity>
                  );
                })}
              </View>
            ) : null}
          </View>
          <View style={styles.hitFiltersActions}>
            <TouchableOpacity
              style={styles.hitResetButton}
              onPress={() => {
                setActiveMenu(null);
                onResetFilters();
              }}
            >
              <Text style={styles.hitResetText}>Reset</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[
                styles.hitAdvancedButton,
                hasAdvancedFilters && styles.hitAdvancedButtonActive,
              ]}
              onPress={() => {
                setActiveMenu(null);
                onOpenAdvancedFilters();
              }}
            >
              <Ionicons name="options" size={16} color={theme.colors.text} />
            </TouchableOpacity>
          </View>
        </View>
      </View>

      <View style={styles.hitContentRow}>
        <View style={[styles.hitField, { width: fieldWidth, height: fieldHeight }]}>
          <View style={[styles.fieldClip, { width: fieldWidth, height: fieldHeight }]}>
            <View
              style={[
                styles.outfieldArc,
                {
                  width: outfieldRadius * 2,
                  height: outfieldRadius * 2,
                  borderRadius: outfieldRadius,
                  borderWidth: lineThickness,
                  left: homeX - outfieldRadius,
                  top: homeY - outfieldRadius,
                },
              ]}
            />
            <View
              style={[
                styles.foulLine,
                {
                  width: foulLineLength,
                  height: lineThickness,
                  left: leftCenterX - foulLineLength / 2,
                  top: leftCenterY - lineThickness / 2,
                  transform: [{ rotate: "-135deg" }],
                },
              ]}
            />
            <View
              style={[
                styles.foulLine,
                {
                  width: foulLineLength,
                  height: lineThickness,
                  left: rightCenterX - foulLineLength / 2,
                  top: rightCenterY - lineThickness / 2,
                  transform: [{ rotate: "-45deg" }],
                },
              ]}
            />
          </View>

          <View
            style={[
              styles.infieldDirt,
              {
                width: infieldDirtSize,
                height: infieldDirtSize,
                left: homeX - infieldDirtSize / 2,
                top: diamondCenterY - infieldDirtSize / 2,
              },
            ]}
          />

          <View
            style={[
              styles.infieldDiamond,
              {
                width: infieldSize,
                height: infieldSize,
                left: homeX - infieldSize / 2,
                top: diamondCenterY - infieldSize / 2,
              },
            ]}
          />

          {splitLines.map((line) => (
            <View
              key={line.key}
              style={[
                styles.splitLine,
                {
                  width: splitLineSegment,
                  height: splitThickness,
                  left: line.left - splitLineSegment / 2,
                  top: line.top - splitThickness / 2,
                  transform: [{ rotate: line.rotate }],
                },
              ]}
            />
          ))}

          <View
            style={[
              styles.centerLine,
              {
                width: centerLineLength,
                height: splitThickness,
                left: centerLineX - centerLineLength / 2,
                top: centerLineY - splitThickness / 2,
                transform: [{ rotate: `${centerLineAngle}deg` }],
              },
            ]}
          />

          {hitZoneLabels.map((zone) => {
            const isActive = selectedZone === zone.zoneKey;
            return (
              <Pressable
                key={zone.key}
                onPress={() => onSelectZone?.(zone.zoneKey)}
                style={[
                  styles.hitZoneLabel,
                  zone.variant === "infield" && styles.hitZoneLabelInfield,
                  zone.variant === "outfield" && styles.hitZoneLabelOutfield,
                  zone.variant === "homerun" && styles.hitZoneLabelHomerun,
                  isActive && styles.hitZoneLabelActive,
                  {
                    width: labelWidth,
                    height: labelHeight,
                    left: zone.left,
                    top: zone.top,
                  },
                ]}
              >
                <Text style={styles.hitZoneValue}>{zone.value}</Text>
              </Pressable>
            );
          })}

          {basePositions.map((base) => (
            <View
              key={base.key}
              style={[
                styles.base,
                {
                  width: baseSize,
                  height: baseSize,
                  left: base.x - baseSize / 2,
                  top: base.y - baseSize / 2,
                },
              ]}
            />
          ))}

          <View
            style={[
              styles.homePlate,
              {
                width: baseSize,
                height: baseSize,
                left: homeX - baseSize / 2,
                top: homeY - baseSize / 2,
              },
            ]}
          />

          <View
            style={[
              styles.mound,
              {
                width: moundSize,
                height: moundSize,
                borderRadius: moundSize / 2,
                left: homeX - moundSize / 2,
                top: moundY - moundSize / 2,
              },
            ]}
          />
        </View>

        <View style={[styles.hitStats, { width: statsColumnWidth }]}>
          {hitStatCards.map((item) => (
            <Pressable
              key={item.label}
              style={({ pressed }) => [
                styles.hitStatRow,
                pressed && styles.hitStatRowPressed,
              ]}
              onPress={() => onStatPress?.(item.label)}
            >
              <Text style={styles.hitStatLabel} numberOfLines={1}>
                {item.label}
              </Text>
              <Text style={styles.hitStatValue} numberOfLines={1}>
                {item.value}
              </Text>
            </Pressable>
          ))}
        </View>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  section: {
    marginTop: 12,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    width: "100%",
    alignSelf: "stretch",
  },
  sectionHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  sectionHeader: {
    gap: 4,
    flex: 1,
  },
  title: {
    color: theme.colors.primary,
    fontSize: 14,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
  subtitle: {
    color: "rgba(226, 232, 240, 0.55)",
    fontSize: 11,
    fontWeight: "600",
  },
  sectionToggle: {
    flexDirection: "row",
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderRadius: 999,
    padding: 2,
    gap: 2,
    alignSelf: "flex-start",
  },
  sectionToggleButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
  },
  sectionToggleButtonActive: {
    backgroundColor: "#fbbf24",
  },
  sectionToggleText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
  },
  sectionToggleTextActive: {
    color: "#0f172a",
  },
  hitField: {
    marginTop: 12,
    borderRadius: 16,
    backgroundColor: "rgba(10, 16, 28, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
    overflow: "hidden",
    position: "relative",
    alignSelf: "stretch",
  },
  hitFiltersStack: {
    marginTop: 10,
    marginBottom: 6,
    width: "100%",
    gap: 8,
  },
  hitFiltersRow: {
    width: "100%",
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  hitFiltersRowSecondary: {
    width: "100%",
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  hitFiltersActions: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
  },
  hitFilterColumn: {
    flex: 1,
    minWidth: 0,
  },
  hitFilterToggle: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
    justifyContent: "space-between",
  },
  hitFilterToggleText: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
    flexShrink: 1,
  },
  hitFilterToggleIcon: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 10,
    fontWeight: "700",
  },
  hitResetButton: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
  },
  hitResetText: {
    color: "rgba(226, 232, 240, 0.85)",
    fontSize: 10,
    fontWeight: "700",
  },
  hitAdvancedButton: {
    paddingHorizontal: 10,
    paddingVertical: 6,
    borderRadius: 999,
    backgroundColor: "rgba(30, 41, 59, 0.7)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
  },
  hitAdvancedButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.8)",
  },
  hitFilterMenu: {
    marginTop: 6,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.9)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
  },
  hitActiveRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.18)",
    backgroundColor: "rgba(15, 23, 42, 0.75)",
  },
  hitActiveText: {
    flex: 1,
    marginRight: 6,
  },
  hitActiveLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 9,
    fontWeight: "600",
  },
  hitActiveName: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
    marginTop: 2,
  },
  hitClearButton: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.35)",
  },
  hitClearText: {
    color: "rgba(226, 232, 240, 0.8)",
    fontSize: 9,
    fontWeight: "700",
  },
  hitSearchBlock: {
    paddingHorizontal: 10,
    paddingTop: 8,
    paddingBottom: 10,
  },
  hitSearchInput: {
    height: 34,
    borderRadius: 10,
    paddingHorizontal: 10,
    backgroundColor: "rgba(30, 41, 59, 0.75)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.25)",
    color: theme.colors.text,
    fontSize: 12,
  },
  hitSearchResults: {
    marginTop: 8,
    gap: 6,
  },
  hitSearchRow: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderRadius: 10,
    backgroundColor: "rgba(30, 41, 59, 0.65)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.16)",
  },
  hitSearchText: {
    flex: 1,
    gap: 2,
  },
  hitSearchName: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
  },
  hitSearchMeta: {
    color: theme.colors.muted,
    fontSize: 9,
  },
  hitSearchStatus: {
    color: theme.colors.muted,
    fontSize: 10,
    paddingVertical: 4,
    textAlign: "center",
  },
  hitSearchDivider: {
    height: 1,
    backgroundColor: "rgba(148, 163, 184, 0.2)",
  },
  hitFilterItem: {
    paddingHorizontal: 10,
    paddingVertical: 8,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(148, 163, 184, 0.12)",
  },
  hitFilterItemLast: {
    borderBottomWidth: 0,
  },
  hitFilterItemActive: {
    backgroundColor: "rgba(251, 191, 36, 0.15)",
  },
  hitFilterItemText: {
    color: "rgba(226, 232, 240, 0.7)",
    fontSize: 11,
    fontWeight: "600",
  },
  hitFilterItemTextActive: {
    color: theme.colors.text,
    fontWeight: "700",
  },
  hitContentRow: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 12,
    flexWrap: "nowrap",
    width: "100%",
  },
  hitStats: {
    marginTop: 12,
    gap: 4,
  },
  hitStatRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 4,
    paddingVertical: 3,
    borderRadius: 5,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.22)",
    backgroundColor: "rgba(15, 23, 42, 0.55)",
  },
  hitStatRowPressed: {
    borderColor: "rgba(251, 191, 36, 0.8)",
    backgroundColor: "rgba(251, 191, 36, 0.08)",
  },
  hitStatLabel: {
    color: "rgba(226, 232, 240, 0.6)",
    fontSize: 9,
    fontWeight: "600",
    flexShrink: 1,
    marginRight: 6,
  },
  hitStatValue: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
  },
  fieldClip: {
    position: "absolute",
    left: 0,
    top: 0,
    overflow: "hidden",
  },
  outfieldArc: {
    position: "absolute",
    borderColor: "rgba(148, 163, 184, 0.6)",
  },
  foulLine: {
    position: "absolute",
    backgroundColor: "rgba(226, 232, 240, 0.7)",
  },
  splitLine: {
    position: "absolute",
    backgroundColor: "rgba(148, 163, 184, 0.5)",
  },
  centerLine: {
    position: "absolute",
    backgroundColor: "rgba(148, 163, 184, 0.55)",
  },
  hitZoneLabel: {
    position: "absolute",
    alignItems: "center",
    justifyContent: "center",
    borderRadius: 999,
    backgroundColor: "rgba(15, 23, 42, 0.55)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  hitZoneLabelInfield: {
    backgroundColor: "rgba(251, 191, 36, 0.2)",
    borderColor: "rgba(251, 191, 36, 0.55)",
  },
  hitZoneLabelOutfield: {
    backgroundColor: "rgba(56, 189, 248, 0.18)",
    borderColor: "rgba(56, 189, 248, 0.4)",
  },
  hitZoneLabelHomerun: {
    backgroundColor: "rgba(248, 113, 113, 0.18)",
    borderColor: "rgba(248, 113, 113, 0.4)",
  },
  hitZoneLabelActive: {
    borderColor: "rgba(251, 191, 36, 0.85)",
    backgroundColor: "rgba(251, 191, 36, 0.2)",
  },
  hitZoneValue: {
    color: "rgba(226, 232, 240, 0.92)",
    fontSize: 11,
    fontWeight: "700",
  },
  infieldDirt: {
    position: "absolute",
    backgroundColor: "rgba(30, 41, 59, 0.55)",
    borderColor: "rgba(148, 163, 184, 0.3)",
    borderWidth: 1,
    transform: [{ rotate: "45deg" }],
  },
  infieldDiamond: {
    position: "absolute",
    borderWidth: 2,
    borderColor: "rgba(226, 232, 240, 0.9)",
    backgroundColor: "transparent",
    transform: [{ rotate: "45deg" }],
  },
  base: {
    position: "absolute",
    backgroundColor: "rgba(226, 232, 240, 0.95)",
    borderWidth: 1,
    borderColor: "rgba(15, 23, 42, 0.9)",
    transform: [{ rotate: "45deg" }],
  },
  homePlate: {
    position: "absolute",
    backgroundColor: "rgba(251, 191, 36, 0.95)",
    borderWidth: 1,
    borderColor: "rgba(15, 23, 42, 0.9)",
    transform: [{ rotate: "45deg" }],
  },
  mound: {
    position: "absolute",
    backgroundColor: "rgba(226, 232, 240, 0.85)",
    borderWidth: 1,
    borderColor: "rgba(15, 23, 42, 0.8)",
  },
});
