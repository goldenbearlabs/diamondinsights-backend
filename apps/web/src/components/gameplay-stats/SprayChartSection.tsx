"use client";

import { useEffect, useRef, useState } from "react";

import { formatPercent } from "./format";
import type { HitterSide, HitDataMap, HitDataStat, HitZoneKey, PitcherHand } from "./types";
import styles from "./styles.module.css";

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

const HIT_STAT_OPTIONS: Array<{ label: string; value: HitDataStat }> = [
  { label: "Count", value: "count" },
  { label: "Share", value: "share" },
  { label: "BABIP", value: "babip" },
  { label: "xOBA", value: "woba" },
  { label: "SLG", value: "slug" },
];

const HITTER_SIDE_OPTIONS: Array<{ label: string; value: HitterSide }> = [
  { label: "All hitters", value: "all" },
  { label: "Left hitters", value: "left" },
  { label: "Right hitters", value: "right" },
];

const PITCHER_HAND_OPTIONS: Array<{ label: string; value: PitcherHand }> = [
  { label: "All pitchers", value: "all" },
  { label: "Left pitchers", value: "left" },
  { label: "Right pitchers", value: "right" },
];

type ZoneVariant = "infield" | "outfield" | "homerun";

type ZoneLayout = {
  key: HitZoneKey;
  x: number;
  y: number;
  variant: ZoneVariant;
};

type StatHelp = {
  title: string;
  description: string;
  formula: string;
};

const HIT_DATA_STAT_HELP: Record<string, StatHelp> = {
  PA: {
    title: "PA",
    description: "Total plate appearances in the current filtered sample.",
    formula: "PA = all plate appearances in current filters",
  },
  Total: {
    title: "Total",
    description: "Total tracked balls in play represented by this spray chart output.",
    formula: "Total = sum of all spray chart zone events",
  },
  "Sweet Spot%": {
    title: "Sweet Spot%",
    description: "Share of balls in play that were line drives, deep fly balls, or perfect-perfects.",
    formula: "Sweet Spot% = (LD + Deep FB + PP) / BIP x 100",
  },
  "Popup%": {
    title: "Popup%",
    description: "Share of balls in play that were popups.",
    formula: "Popup% = popups / BIP x 100",
  },
  "Flyball%": {
    title: "Flyball%",
    description: "Share of balls in play that were fly balls.",
    formula: "Flyball% = fly balls / BIP x 100",
  },
  "GB/Air%": {
    title: "GB/Air%",
    description: "Ground balls divided by air balls (fly balls + line drives).",
    formula: "GB/Air% = ground balls / (fly balls + line drives) x 100",
  },
  "Pulled Air%": {
    title: "Pulled Air%",
    description: "Share of air balls (fly balls + line drives + HR) hit to the pull side.",
    formula: "Pulled Air% = pulled air / air balls x 100",
  },
  "Oppo Air%": {
    title: "Oppo Air%",
    description: "Share of air balls (fly balls + line drives + HR) hit to opposite field.",
    formula: "Oppo Air% = opposite air / air balls x 100",
  },
  "Perfect Perfect%": {
    title: "Perfect Perfect%",
    description: "Share of balls in play that were perfect-perfect.",
    formula: "Perfect Perfect% = perfect-perfect / BIP x 100",
  },
};

type Props = {
  data: HitDataMap | null;
  snapshotData: HitDataMap | null;
  loading: boolean;
  snapshotLoading: boolean;
  error: string | null;
  mode: "Hitting" | "Pitching";
  onChangeMode: (mode: "Hitting" | "Pitching") => void;
  stat: HitDataStat;
  onChangeStat: (value: HitDataStat) => void;
  selections: HitZoneKey[];
  onSelectionChange: (zone: HitZoneKey) => void;
  onClearSelections: () => void;
  filterHitterSide: HitterSide;
  filterPitcherHand: PitcherHand;
  onChangeFilterHitterSide: (value: HitterSide) => void;
  onChangeFilterPitcherHand: (value: PitcherHand) => void;
  onResetFilters: () => void;
};

const HOME_X = 50;
const HOME_Y = 88;
const OUTFIELD_RADIUS = 68;
const INFIELD_BOUNDARY_RADIUS = 38;
const INFIELD_SIZE = 18;
const INFIELD_DIRT_SIZE = 28;
const FOUL_LINE_LENGTH = 72;
const SPLIT_START_RADIUS = INFIELD_BOUNDARY_RADIUS;
const SPLIT_END_RADIUS = 56;
const BASE_SIZE = 3.8;
const MOUND_SIZE = 2.8;
const LABEL_WIDTH = 14;
const LABEL_HEIGHT = 8;

function polar(angleDeg: number, radius: number) {
  const radians = (angleDeg * Math.PI) / 180;
  return {
    x: HOME_X + Math.cos(radians) * radius,
    y: HOME_Y + Math.sin(radians) * radius,
  };
}

function formatRate(value: number) {
  if (!Number.isFinite(value)) {
    return "-";
  }
  const fixed = value.toFixed(3);
  return value < 1 ? fixed.replace(/^0/, "") : fixed;
}

function formatHitValue(value: number, stat: HitDataStat): string {
  if (!Number.isFinite(value)) {
    return "-";
  }
  if (stat === "count") {
    return String(Math.round(value));
  }
  if (stat === "share") {
    return formatPercent(value);
  }
  return formatRate(value);
}

function formatMetric(value: number | null | undefined) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return formatPercent(value);
}

export function SprayChartSection({
  data,
  snapshotData,
  loading,
  snapshotLoading,
  error,
  mode,
  onChangeMode,
  stat,
  onChangeStat,
  selections,
  onSelectionChange,
  onClearSelections,
  filterHitterSide,
  filterPitcherHand,
  onChangeFilterHitterSide,
  onChangeFilterPitcherHand,
  onResetFilters,
}: Props) {
  const [activeMenu, setActiveMenu] = useState<null | "stat" | "hitter" | "pitcher">(null);
  const [statHelp, setStatHelp] = useState<StatHelp | null>(null);
  const filterRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const onClick = (event: MouseEvent) => {
      if (!filterRef.current) {
        return;
      }
      const target = event.target as Node;
      if (!filterRef.current.contains(target)) {
        setActiveMenu(null);
      }
    };

    window.addEventListener("mousedown", onClick);
    return () => window.removeEventListener("mousedown", onClick);
  }, []);

  const zoneValues = data?.zones ?? EMPTY_ZONES;

  const infieldRadius = INFIELD_BOUNDARY_RADIUS * 0.55;
  const outfieldLabelRadius = (INFIELD_BOUNDARY_RADIUS + OUTFIELD_RADIUS) / 2;
  const homerunLabelRadius = Math.min(
    HOME_Y - LABEL_HEIGHT,
    OUTFIELD_RADIUS + (OUTFIELD_RADIUS - INFIELD_BOUNDARY_RADIUS) * 0.55,
  );

  const zoneLayouts: ZoneLayout[] = [
    { key: "infield_left", ...polar(-112.5, infieldRadius), variant: "infield" },
    { key: "infield_right", ...polar(-67.5, infieldRadius), variant: "infield" },
    { key: "outfield_left", ...polar(-120, outfieldLabelRadius), variant: "outfield" },
    { key: "outfield_center", ...polar(-90, outfieldLabelRadius), variant: "outfield" },
    { key: "outfield_right", ...polar(-60, outfieldLabelRadius), variant: "outfield" },
    { key: "homerun_left", ...polar(-120, homerunLabelRadius), variant: "homerun" },
    { key: "homerun_center", ...polar(-90, homerunLabelRadius), variant: "homerun" },
    { key: "homerun_right", ...polar(-60, homerunLabelRadius), variant: "homerun" },
  ];

  const leftFoulEnd = polar(-135, FOUL_LINE_LENGTH);
  const rightFoulEnd = polar(-45, FOUL_LINE_LENGTH);
  const outfieldArcLeft = polar(-135, OUTFIELD_RADIUS);
  const outfieldArcRight = polar(-45, OUTFIELD_RADIUS);

  const splitLineDefs = [-105, -75].map((angle) => {
    const baseLength = SPLIT_END_RADIUS - SPLIT_START_RADIUS;
    const halfExtension = baseLength / 2;
    const startRadius = Math.max(0, SPLIT_START_RADIUS - halfExtension);
    const endRadius = Math.min(OUTFIELD_RADIUS, SPLIT_END_RADIUS + halfExtension);
    const start = polar(angle, startRadius);
    const end = polar(angle, endRadius);
    return { key: String(angle), start, end };
  });

  const diamondRadius = INFIELD_SIZE / Math.SQRT2;
  const diamondCenterY = HOME_Y - diamondRadius;
  const basePositions = [
    { key: "third", x: HOME_X - diamondRadius, y: diamondCenterY },
    { key: "second", x: HOME_X, y: diamondCenterY - diamondRadius },
    { key: "first", x: HOME_X + diamondRadius, y: diamondCenterY },
  ];
  const moundPosition = polar(-90, diamondRadius * 0.96);

  const snapshotStats = snapshotData?.stats;
  const statRows = [
    { label: "PA", value: String(data?.pa ?? 0) },
    { label: "Total", value: String(data?.total ?? 0) },
    { label: "Sweet Spot%", value: formatMetric(snapshotStats?.sweet_spot_pct) },
    { label: "Popup%", value: formatMetric(snapshotStats?.popup_rate) },
    { label: "Flyball%", value: formatMetric(snapshotStats?.flyball_rate) },
    { label: "GB/Air%", value: formatMetric(snapshotStats?.gb_air_ratio) },
    { label: "Pulled Air%", value: formatMetric(snapshotStats?.pulled_air_rate) },
    { label: "Oppo Air%", value: formatMetric(snapshotStats?.oppo_air_rate) },
    { label: "Perfect Perfect%", value: formatMetric(snapshotStats?.perfect_perfect_pct) },
  ];

  const statLabel = HIT_STAT_OPTIONS.find((option) => option.value === stat)?.label ?? "Count";
  const hitterLabel = HITTER_SIDE_OPTIONS.find((option) => option.value === filterHitterSide)?.label ?? "All hitters";
  const pitcherLabel = PITCHER_HAND_OPTIONS.find((option) => option.value === filterPitcherHand)?.label ?? "All pitchers";

  return (
    <section className={styles.spraySection}>
      <div className={styles.sectionHeaderRow}>
        <div>
          <p className={styles.statsGroupTitle}>Spray Chart</p>
          <p className={styles.sectionSubtitle}>Hit locations and spray tendencies.</p>
        </div>
        <div className={styles.toggle}>
          {(["Hitting", "Pitching"] as const).map((item) => (
            <button
              key={item}
              type="button"
              className={mode === item ? styles.toggleButtonActive : styles.toggleButton}
              onClick={() => onChangeMode(item)}
            >
              {item}
            </button>
          ))}
        </div>
      </div>

      <div className={styles.sprayFiltersRow} ref={filterRef}>
        <div className={styles.sprayFilterField}>
          <button
            type="button"
            className={styles.dropdownToggle}
            onClick={() => setActiveMenu((prev) => (prev === "stat" ? null : "stat"))}
          >
            <span>{statLabel}</span>
            <span>{activeMenu === "stat" ? "^" : "v"}</span>
          </button>
          {activeMenu === "stat" ? (
            <div className={`${styles.dropdownMenu} ${styles.dropdownMenuLeft}`}>
              {HIT_STAT_OPTIONS.map((option) => {
                const active = option.value === stat;
                return (
                  <button
                    key={option.value}
                    type="button"
                    className={active ? styles.dropdownOptionActive : styles.dropdownOption}
                    onClick={() => {
                      onChangeStat(option.value);
                      setActiveMenu(null);
                    }}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
          ) : null}
        </div>

        <div className={styles.sprayFilterField}>
          <button
            type="button"
            className={styles.dropdownToggle}
            onClick={() => setActiveMenu((prev) => (prev === "hitter" ? null : "hitter"))}
          >
            <span>{hitterLabel}</span>
            <span>{activeMenu === "hitter" ? "^" : "v"}</span>
          </button>
          {activeMenu === "hitter" ? (
            <div className={`${styles.dropdownMenu} ${styles.dropdownMenuLeft}`}>
              {HITTER_SIDE_OPTIONS.map((option) => {
                const active = option.value === filterHitterSide;
                return (
                  <button
                    key={option.value}
                    type="button"
                    className={active ? styles.dropdownOptionActive : styles.dropdownOption}
                    onClick={() => {
                      onChangeFilterHitterSide(option.value);
                      setActiveMenu(null);
                    }}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
          ) : null}
        </div>

        <div className={styles.sprayFilterField}>
          <button
            type="button"
            className={styles.dropdownToggle}
            onClick={() => setActiveMenu((prev) => (prev === "pitcher" ? null : "pitcher"))}
          >
            <span>{pitcherLabel}</span>
            <span>{activeMenu === "pitcher" ? "^" : "v"}</span>
          </button>
          {activeMenu === "pitcher" ? (
            <div className={`${styles.dropdownMenu} ${styles.dropdownMenuRight}`}>
              {PITCHER_HAND_OPTIONS.map((option) => {
                const active = option.value === filterPitcherHand;
                return (
                  <button
                    key={option.value}
                    type="button"
                    className={active ? styles.dropdownOptionActive : styles.dropdownOption}
                    onClick={() => {
                      onChangeFilterPitcherHand(option.value);
                      setActiveMenu(null);
                    }}
                  >
                    {option.label}
                  </button>
                );
              })}
            </div>
          ) : null}
        </div>

        <button
          type="button"
          className={styles.filterActionReset}
          onClick={() => {
            setActiveMenu(null);
            onResetFilters();
          }}
        >
          Reset
        </button>
      </div>

      {error ? <p className={styles.error}>{error}</p> : null}

      <div className={styles.sprayBody}>
        <div className={styles.strikeoutGridWrap}>
          <div className={styles.sprayField}>
            <svg
              className={styles.spraySvg}
              viewBox="0 0 100 100"
              preserveAspectRatio="none"
              aria-hidden
            >
              <path
                d={`M ${outfieldArcLeft.x} ${outfieldArcLeft.y} A ${OUTFIELD_RADIUS} ${OUTFIELD_RADIUS} 0 0 1 ${outfieldArcRight.x} ${outfieldArcRight.y}`}
                className={styles.sprayLineMajor}
              />
              <line x1={HOME_X} y1={HOME_Y} x2={leftFoulEnd.x} y2={leftFoulEnd.y} className={styles.sprayLineMajor} />
              <line x1={HOME_X} y1={HOME_Y} x2={rightFoulEnd.x} y2={rightFoulEnd.y} className={styles.sprayLineMajor} />

              {splitLineDefs.map((line) => (
                <line
                  key={line.key}
                  x1={line.start.x}
                  y1={line.start.y}
                  x2={line.end.x}
                  y2={line.end.y}
                  className={styles.sprayLineMinor}
                />
              ))}
            </svg>

            <div
              className={styles.sprayInfieldDirt}
              style={{
                width: `${INFIELD_DIRT_SIZE}%`,
                height: `${INFIELD_DIRT_SIZE}%`,
                left: `${HOME_X - INFIELD_DIRT_SIZE / 2}%`,
                top: `${diamondCenterY - INFIELD_DIRT_SIZE / 2}%`,
              }}
            />
            <div
              className={styles.sprayInfieldDiamond}
              style={{
                width: `${INFIELD_SIZE}%`,
                height: `${INFIELD_SIZE}%`,
                left: `${HOME_X - INFIELD_SIZE / 2}%`,
                top: `${diamondCenterY - INFIELD_SIZE / 2}%`,
              }}
            />

            {zoneLayouts.map((zone) => {
              const zoneValue = formatHitValue(zoneValues[zone.key], stat);
              const active = selections.includes(zone.key);
              return (
                <button
                  key={zone.key}
                  type="button"
                  className={[
                    styles.sprayZoneLabel,
                    zone.variant === "infield" ? styles.sprayZoneInfield : "",
                    zone.variant === "outfield" ? styles.sprayZoneOutfield : "",
                    zone.variant === "homerun" ? styles.sprayZoneHomerun : "",
                    active ? styles.sprayZoneActive : "",
                  ].join(" ")}
                  style={{
                    width: `${LABEL_WIDTH}%`,
                    height: `${LABEL_HEIGHT}%`,
                    left: `${zone.x}%`,
                    top: `${zone.y}%`,
                    marginLeft: `${-LABEL_WIDTH / 2}%`,
                    marginTop: `${-LABEL_HEIGHT / 2}%`,
                  }}
                  onClick={() => onSelectionChange(zone.key)}
                >
                  {loading ? "..." : zoneValue}
                </button>
              );
            })}

            {basePositions.map((base) => (
              <div
                key={base.key}
                className={styles.sprayBase}
                style={{
                  width: `${BASE_SIZE}%`,
                  height: `${BASE_SIZE}%`,
                  left: `${base.x - BASE_SIZE / 2}%`,
                  top: `${base.y - BASE_SIZE / 2}%`,
                }}
              />
            ))}

            <div
              className={styles.sprayHomePlate}
              style={{
                width: `${BASE_SIZE}%`,
                height: `${BASE_SIZE}%`,
                left: `${HOME_X - BASE_SIZE / 2}%`,
                top: `${HOME_Y - BASE_SIZE / 2}%`,
              }}
            />

            <div
              className={styles.sprayMound}
              style={{
                width: `${MOUND_SIZE}%`,
                height: `${MOUND_SIZE}%`,
                left: `${moundPosition.x - MOUND_SIZE / 2}%`,
                top: `${moundPosition.y - MOUND_SIZE / 2}%`,
              }}
            />
          </div>
        </div>

        <div className={styles.strikeoutSummaryWrap}>
          <div className={styles.sectionHeaderRow}>
            <p className={styles.statsGroupTitle}>Spray Snapshot</p>
            {selections.length > 0 ? (
              <button type="button" className={styles.filterActionButton} onClick={onClearSelections}>
                Clear Zones
              </button>
            ) : null}
          </div>
          <div className={styles.analyticsSummaryGrid}>
            {statRows.map((item) => (
              <div key={item.label} className={styles.summaryChip}>
                <div className={styles.summaryLabelRow}>
                  <p className={styles.summaryLabel}>{item.label}</p>
                  <button
                    type="button"
                    className={styles.summaryHelpButton}
                    aria-label={`Explain ${item.label}`}
                    onClick={() => setStatHelp(HIT_DATA_STAT_HELP[item.label] ?? null)}
                  >
                    ?
                  </button>
                </div>
                <p className={styles.summaryValue}>{snapshotLoading ? "..." : item.value}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {statHelp ? (
        <div className={styles.advancedModalOverlay}>
          <button
            type="button"
            className={styles.advancedBackdrop}
            aria-label="Close spray stat help"
            onClick={() => setStatHelp(null)}
          />
          <div className={`${styles.advancedModal} ${styles.statHelpModal}`}>
            <div className={styles.advancedModalHeader}>
              <p>{statHelp.title}</p>
              <button type="button" className={styles.filterActionButton} onClick={() => setStatHelp(null)}>
                Close
              </button>
            </div>
            <p className={styles.statHelpDescription}>{statHelp.description}</p>
            <p className={styles.statHelpSectionLabel}>Math</p>
            <p className={styles.statHelpFormula}>{statHelp.formula}</p>
          </div>
        </div>
      ) : null}
    </section>
  );
}
