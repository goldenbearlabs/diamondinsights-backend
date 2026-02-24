"use client";

import { useMemo, useState } from "react";

import { formatCount, formatPercent, formatRate } from "./format";
import { SprayChartSection } from "./SprayChartSection";
import { StrikeoutFilters } from "./StrikeoutFilters";
import { StrikeoutMap } from "./StrikeoutMap";
import type {
  HitterSide,
  HitDataMap,
  HitDataStat,
  HitZoneKey,
  OutType,
  PitcherHand,
  ShowAggregateStats,
  ShowHitterSearchResult,
  ShowPitcherSearchResult,
  StrikeoutCounts,
  StrikeoutMapData,
  StrikeoutSelection,
  StrikeoutStats,
  TimingType,
} from "./types";
import styles from "./styles.module.css";

type Props = {
  strikeoutMode: "Hitting" | "Pitching";
  onChangeStrikeoutMode: (mode: "Hitting" | "Pitching") => void;
  strikeoutMap: StrikeoutMapData | null;
  strikeoutLoading: boolean;
  strikeoutError: string | null;
  filterHitterSide: HitterSide;
  onChangeFilterHitterSide: (value: HitterSide) => void;
  filterPitcherHand: PitcherHand;
  onChangeFilterPitcherHand: (value: PitcherHand) => void;
  selectedPitcher: ShowPitcherSearchResult | null;
  selectedHitter: ShowHitterSearchResult | null;
  pitcherSearchQuery: string;
  onChangePitcherSearchQuery: (value: string) => void;
  hitterSearchQuery: string;
  onChangeHitterSearchQuery: (value: string) => void;
  pitcherSearchResults: ShowPitcherSearchResult[];
  hitterSearchResults: ShowHitterSearchResult[];
  pitcherSearchLoading: boolean;
  hitterSearchLoading: boolean;
  pitcherSearchError: string | null;
  hitterSearchError: string | null;
  onSelectPitcher: (value: ShowPitcherSearchResult | null) => void;
  onSelectHitter: (value: ShowHitterSearchResult | null) => void;
  pitchTypeOptions: string[];
  selectedPitchTypes: string[];
  onTogglePitchType: (pitchType: string) => void;
  minSpeed: string;
  onChangeMinSpeed: (value: string) => void;
  maxSpeed: string;
  onChangeMaxSpeed: (value: string) => void;
  timing: TimingType;
  onChangeTiming: (value: TimingType) => void;
  outType: OutType;
  onChangeOutType: (value: OutType) => void;
  advancedOpen: boolean;
  onToggleAdvanced: () => void;
  onCloseAdvanced: () => void;
  onResetFilters: () => void;
  hasAdvancedFilters: boolean;
  statsMode: "Hitting" | "Pitching";
  onChangeStatsMode: (mode: "Hitting" | "Pitching") => void;
  aggregateStats: ShowAggregateStats | null;
  aggregateLoading: boolean;
  aggregateError: string | null;
  sprayChartData: HitDataMap | null;
  sprayChartLoading: boolean;
  sprayChartError: string | null;
  sprayChartSnapshotData: HitDataMap | null;
  sprayChartSnapshotLoading: boolean;
  sprayChartMode: "Hitting" | "Pitching";
  onChangeSprayChartMode: (mode: "Hitting" | "Pitching") => void;
  sprayChartStat: HitDataStat;
  onChangeSprayChartStat: (stat: HitDataStat) => void;
  sprayChartSelections: HitZoneKey[];
  onChangeSprayChartSelection: (zone: HitZoneKey) => void;
  onClearSprayChartSelections: () => void;
};

type StatHelp = {
  title: string;
  description: string;
  formula: string;
};

const STRIKEOUT_STAT_HELP: Record<string, StatHelp> = {
  "K%": {
    title: "K%",
    description: "Share of plate appearances that end in a strikeout.",
    formula: "K% = strikeouts / plate appearances x 100",
  },
  "Chase %": {
    title: "Chase %",
    description: "Among strikeouts, the share where the batter chased out of the zone.",
    formula: "Chase% = chase strikeouts / strikeouts x 100",
  },
  "Freeze %": {
    title: "Freeze %",
    description: "Among strikeouts, the share where the batter took a called strike.",
    formula: "Freeze% = looking strikeouts / strikeouts x 100",
  },
  "Timing Bias": {
    title: "Timing Bias",
    description:
      "Bias toward early versus late timing on strikeout swings. Positive is more early, negative is more late.",
    formula: "Timing Bias = 100 x (E - L) / (E + L), where E=early Ks, L=late Ks",
  },
  "Mistime K%": {
    title: "Mistime K%",
    description: "Share of strikeouts that came on mistimed swings (early or late).",
    formula: "Mistime K% = (early + late) / strikeouts x 100",
  },
  "Eye K%": {
    title: "Eye K%",
    description: "Share of strikeouts that were either chase or looking.",
    formula: "Eye K% = (chase + looking) / strikeouts x 100",
  },
  "Location K%": {
    title: "Location K%",
    description: "Remainder after mistime and eye. Interpreted as location-driven Ks.",
    formula: "Location K% = 100 - (Mistime K% + Eye K%)",
  },
  PA: {
    title: "PA",
    description: "Total plate appearances in the currently filtered sample.",
    formula: "PA = all appearances that complete an at-bat outcome",
  },
};

function timingBias(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${value.toFixed(1)}%`;
}

function strikeoutSelectionKey(selection: StrikeoutSelection): string {
  if (selection.kind === "zone") {
    return `zone:${selection.row}:${selection.col}`;
  }
  return `outside:${selection.key}`;
}

function statsFromCounts(counts: StrikeoutCounts, basePa: number): StrikeoutStats | null {
  if (!basePa || basePa <= 0) {
    return null;
  }

  const kPct = (counts.k / basePa) * 100;
  if (counts.k === 0) {
    return {
      k_pct: kPct,
      chase_pct: 0,
      freeze_pct: 0,
      timing_pct: 0,
      timing_k_pct: 0,
      eye_k_pct: 0,
      location_k_pct: 0,
    };
  }

  const timingTotal = counts.early + counts.late;
  const timingPct = timingTotal ? ((counts.early - counts.late) / timingTotal) * 100 : 0;
  const timingKPct = (timingTotal / counts.k) * 100;
  const eyeKPct = (counts.eye / counts.k) * 100;
  const locationKPct = Math.max(0, Math.min(100, 100 - (timingKPct + eyeKPct)));

  return {
    k_pct: kPct,
    chase_pct: (counts.chase / counts.k) * 100,
    freeze_pct: (counts.look / counts.k) * 100,
    timing_pct: timingPct,
    timing_k_pct: timingKPct,
    eye_k_pct: eyeKPct,
    location_k_pct: locationKPct,
  };
}

function statGroups(data: ShowAggregateStats | null) {
  if (!data) {
    return {
      boxscorePrimary: [] as { label: string; value: string }[],
      boxscoreSecondary: [] as { label: string; value: string }[],
      advanced: [] as { label: string; value: string }[],
    };
  }

  const boxscore = [
    { label: "PA", value: formatCount(data.pa) },
    { label: "AB", value: formatCount(data.ab) },
    { label: "R", value: formatCount(data.r) },
    { label: "H", value: formatCount(data.h) },
    { label: "RBI", value: formatCount(data.rbi) },
    { label: "1B", value: formatCount(data.singles) },
    { label: "2B", value: formatCount(data.doubles) },
    { label: "3B", value: formatCount(data.triples) },
    { label: "HR", value: formatCount(data.hr) },
    { label: "BB", value: formatCount(data.bb) },
    { label: "SO", value: formatCount(data.so) },
    { label: "AVG", value: formatRate(data.avg) },
    { label: "OBP", value: formatRate(data.obp) },
    { label: "SLG", value: formatRate(data.slg) },
    { label: "OPS", value: formatRate(data.ops) },
    { label: "LOB", value: formatCount(data.lob) },
    { label: "GIDP%", value: formatPercent(data.gidp_pct) },
  ];

  const advanced = [
    { label: "wOBA", value: formatRate(data.woba) },
    { label: "ISO", value: formatRate(data.iso) },
    { label: "BABIP", value: formatRate(data.babip) },
    { label: "K%", value: formatPercent(data.k_pct) },
    { label: "BB%", value: formatPercent(data.bb_pct) },
    { label: "HR%", value: formatPercent(data.hr_pct) },
    { label: "XBH%", value: formatPercent(data.xbh_pct) },
    { label: "RS%", value: formatPercent(data.rs_pct) },
  ];

  return {
    boxscorePrimary: boxscore.slice(0, 9),
    boxscoreSecondary: boxscore.slice(9),
    advanced,
  };
}

export function AnalyticsSection({
  strikeoutMode,
  onChangeStrikeoutMode,
  strikeoutMap,
  strikeoutLoading,
  strikeoutError,
  filterHitterSide,
  onChangeFilterHitterSide,
  filterPitcherHand,
  onChangeFilterPitcherHand,
  selectedPitcher,
  selectedHitter,
  pitcherSearchQuery,
  onChangePitcherSearchQuery,
  hitterSearchQuery,
  onChangeHitterSearchQuery,
  pitcherSearchResults,
  hitterSearchResults,
  pitcherSearchLoading,
  hitterSearchLoading,
  pitcherSearchError,
  hitterSearchError,
  onSelectPitcher,
  onSelectHitter,
  pitchTypeOptions,
  selectedPitchTypes,
  onTogglePitchType,
  minSpeed,
  onChangeMinSpeed,
  maxSpeed,
  onChangeMaxSpeed,
  timing,
  onChangeTiming,
  outType,
  onChangeOutType,
  advancedOpen,
  onToggleAdvanced,
  onCloseAdvanced,
  onResetFilters,
  hasAdvancedFilters,
  statsMode,
  onChangeStatsMode,
  aggregateStats,
  aggregateLoading,
  aggregateError,
  sprayChartData,
  sprayChartLoading,
  sprayChartError,
  sprayChartSnapshotData,
  sprayChartSnapshotLoading,
  sprayChartMode,
  onChangeSprayChartMode,
  sprayChartStat,
  onChangeSprayChartStat,
  sprayChartSelections,
  onChangeSprayChartSelection,
  onClearSprayChartSelections,
}: Props) {
  const [strikeoutSelections, setStrikeoutSelections] = useState<StrikeoutSelection[]>([]);
  const [statHelp, setStatHelp] = useState<StatHelp | null>(null);
  const strikeoutStats = strikeoutMap?.stats;

  const activeStrikeoutStats = useMemo(() => {
    if (!strikeoutMap || strikeoutSelections.length === 0) {
      return strikeoutStats;
    }

    const aggregate: StrikeoutCounts = {
      k: 0,
      chase: 0,
      look: 0,
      eye: 0,
      early: 0,
      late: 0,
    };

    strikeoutSelections.forEach((selection) => {
      const counts =
        selection.kind === "zone"
          ? strikeoutMap.counts_by_zone?.[selection.row]?.[selection.col]
          : strikeoutMap.counts_by_outside?.[selection.key];
      if (!counts) {
        return;
      }

      aggregate.k += counts.k ?? 0;
      aggregate.chase += counts.chase ?? 0;
      aggregate.look += counts.look ?? 0;
      aggregate.eye += counts.eye ?? 0;
      aggregate.early += counts.early ?? 0;
      aggregate.late += counts.late ?? 0;
    });

    return statsFromCounts(aggregate, strikeoutMap.pa) ?? strikeoutStats;
  }, [strikeoutMap, strikeoutSelections, strikeoutStats]);

  const onSelectionChange = (selection: StrikeoutSelection) => {
    setStrikeoutSelections((prev) => {
      const key = strikeoutSelectionKey(selection);
      const exists = prev.some((item) => strikeoutSelectionKey(item) === key);
      if (exists) {
        return prev.filter((item) => strikeoutSelectionKey(item) !== key);
      }
      return [...prev, selection];
    });
  };

  const summary = [
    { label: "K%", value: formatPercent(activeStrikeoutStats?.k_pct) },
    { label: "Chase %", value: formatPercent(activeStrikeoutStats?.chase_pct) },
    { label: "Freeze %", value: formatPercent(activeStrikeoutStats?.freeze_pct) },
    { label: "Timing Bias", value: timingBias(activeStrikeoutStats?.timing_pct) },
    { label: "Mistime K%", value: formatPercent(activeStrikeoutStats?.timing_k_pct) },
    { label: "Eye K%", value: formatPercent(activeStrikeoutStats?.eye_k_pct) },
    { label: "Location K%", value: formatPercent(activeStrikeoutStats?.location_k_pct) },
    { label: "PA", value: formatCount(strikeoutMap?.pa ?? 0) },
  ];

  const groups = statGroups(aggregateStats);

  return (
    <section className={styles.analyticsSection}>
      <div className={styles.sectionHeaderRow}>
        <div>
          <h3 className={styles.sectionTitle}>Analytics</h3>
          <p className={styles.sectionSubtitle}>Filtered strikeout outcomes and zone patterns.</p>
        </div>

        <div className={styles.toggle}>
          {(["Hitting", "Pitching"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              className={strikeoutMode === mode ? styles.toggleButtonActive : styles.toggleButton}
              onClick={() => onChangeStrikeoutMode(mode)}
            >
              {mode}
            </button>
          ))}
        </div>
      </div>

      {strikeoutError ? <p className={styles.error}>{strikeoutError}</p> : null}

      <StrikeoutFilters
        hitterSide={filterHitterSide}
        onChangeHitterSide={onChangeFilterHitterSide}
        pitcherHand={filterPitcherHand}
        onChangePitcherHand={onChangeFilterPitcherHand}
        selectedPitcher={selectedPitcher}
        selectedHitter={selectedHitter}
        pitcherSearchQuery={pitcherSearchQuery}
        onChangePitcherSearchQuery={onChangePitcherSearchQuery}
        hitterSearchQuery={hitterSearchQuery}
        onChangeHitterSearchQuery={onChangeHitterSearchQuery}
        pitcherSearchResults={pitcherSearchResults}
        hitterSearchResults={hitterSearchResults}
        pitcherSearchLoading={pitcherSearchLoading}
        hitterSearchLoading={hitterSearchLoading}
        pitcherSearchError={pitcherSearchError}
        hitterSearchError={hitterSearchError}
        onSelectPitcher={onSelectPitcher}
        onSelectHitter={onSelectHitter}
        pitchTypeOptions={pitchTypeOptions}
        selectedPitchTypes={selectedPitchTypes}
        onTogglePitchType={onTogglePitchType}
        minSpeed={minSpeed}
        onChangeMinSpeed={onChangeMinSpeed}
        maxSpeed={maxSpeed}
        onChangeMaxSpeed={onChangeMaxSpeed}
        timing={timing}
        onChangeTiming={onChangeTiming}
        outType={outType}
        onChangeOutType={onChangeOutType}
        advancedOpen={advancedOpen}
        onToggleAdvanced={onToggleAdvanced}
        onCloseAdvanced={onCloseAdvanced}
        onReset={() => {
          setStrikeoutSelections([]);
          onResetFilters();
        }}
        hasAdvancedFilters={hasAdvancedFilters}
      />

      <div className={styles.analyticsTopRow}>
        <div className={styles.strikeoutGridWrap}>
          <p className={styles.statsGroupTitle}>Strike Zone Heat</p>
          <StrikeoutMap
            zones={strikeoutMap?.zones ?? [
              [0, 0, 0],
              [0, 0, 0],
              [0, 0, 0],
            ]}
            outside={
              strikeoutMap?.outside ?? {
                top_left: 0,
                top: 0,
                top_right: 0,
                right: 0,
                bottom_right: 0,
                bottom: 0,
                bottom_left: 0,
                left: 0,
              }
            }
            filterHitterSide={{ side: filterHitterSide }}
            selections={strikeoutSelections}
            onSelectionChange={onSelectionChange}
          />
        </div>

        <div className={styles.strikeoutSummaryWrap}>
          <div className={styles.sectionHeaderRow}>
            <p className={styles.statsGroupTitle}>Strikeout Snapshot</p>
            {strikeoutSelections.length > 0 ? (
              <button
                type="button"
                className={styles.filterActionButton}
                onClick={() => setStrikeoutSelections([])}
              >
                Clear Regions
              </button>
            ) : null}
          </div>
          <div className={styles.analyticsSummaryGrid}>
            {summary.map((row) => (
              <div key={row.label} className={styles.summaryChip}>
                <div className={styles.summaryLabelRow}>
                  <p className={styles.summaryLabel}>{row.label}</p>
                  <button
                    type="button"
                    className={styles.summaryHelpButton}
                    aria-label={`Explain ${row.label}`}
                    onClick={() => setStatHelp(STRIKEOUT_STAT_HELP[row.label] ?? null)}
                  >
                    ?
                  </button>
                </div>
                <p className={styles.summaryValue}>{strikeoutLoading ? "..." : row.value}</p>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className={styles.cardDivider} />

      <SprayChartSection
        data={sprayChartData}
        loading={sprayChartLoading}
        snapshotData={sprayChartSnapshotData}
        snapshotLoading={sprayChartSnapshotLoading}
        error={sprayChartError}
        mode={sprayChartMode}
        onChangeMode={onChangeSprayChartMode}
        stat={sprayChartStat}
        onChangeStat={onChangeSprayChartStat}
        selections={sprayChartSelections}
        onSelectionChange={onChangeSprayChartSelection}
        onClearSelections={onClearSprayChartSelections}
        filterHitterSide={filterHitterSide}
        filterPitcherHand={filterPitcherHand}
        onChangeFilterHitterSide={onChangeFilterHitterSide}
        onChangeFilterPitcherHand={onChangeFilterPitcherHand}
        onResetFilters={() => {
          onChangeFilterHitterSide("all");
          onChangeFilterPitcherHand("all");
          onSelectPitcher(null);
          onSelectHitter(null);
          onChangeSprayChartStat("count");
          onClearSprayChartSelections();
        }}
      />

      {statHelp ? (
        <div className={styles.advancedModalOverlay}>
          <button
            type="button"
            className={styles.advancedBackdrop}
            aria-label="Close stat help"
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

      <div className={styles.cardDivider} />

      <div className={styles.statsTableWrap}>
        <div className={styles.sectionHeaderRow}>
          <p className={styles.statsGroupTitle}>Stats Table</p>
          <div className={styles.toggle}>
            {(["Hitting", "Pitching"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                className={statsMode === mode ? styles.toggleButtonActive : styles.toggleButton}
                onClick={() => onChangeStatsMode(mode)}
              >
                {mode}
              </button>
            ))}
          </div>
        </div>

        {aggregateError ? <p className={styles.error}>{aggregateError}</p> : null}
        {aggregateLoading ? <p className={styles.muted}>Loading stats...</p> : null}

        <div className={styles.statsGroups}>
          <div className={styles.statsGroup}>
            <p className={styles.statsGroupTitle}>Boxscore</p>
            <div className={styles.statsChips}>
              {groups.boxscorePrimary.map((row) => (
                <div key={`bp-${row.label}`} className={styles.statsChip}>
                  <p className={styles.statsChipLabel}>{row.label}</p>
                  <p className={styles.statsChipValue}>{row.value}</p>
                </div>
              ))}
            </div>
            <div className={styles.statsChips}>
              {groups.boxscoreSecondary.map((row) => (
                <div key={`bs-${row.label}`} className={styles.statsChip}>
                  <p className={styles.statsChipLabel}>{row.label}</p>
                  <p className={styles.statsChipValue}>{row.value}</p>
                </div>
              ))}
            </div>
          </div>

          <div className={styles.statsGroup}>
            <p className={styles.statsGroupTitle}>Advanced</p>
            <div className={styles.statsChips}>
              {groups.advanced.map((row) => (
                <div key={`adv-${row.label}`} className={styles.statsChip}>
                  <p className={styles.statsChipLabel}>{row.label}</p>
                  <p className={styles.statsChipValue}>{row.value}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
