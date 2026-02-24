"use client";

import { useEffect, useRef, useState } from "react";

import type {
  HitterSide,
  OutType,
  PitcherHand,
  ShowHitterSearchResult,
  ShowPitcherSearchResult,
  TimingType,
} from "./types";
import styles from "./styles.module.css";

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

const TIMING_OPTIONS: Array<{ label: string; value: TimingType }> = [
  { label: "All", value: "all" },
  { label: "Late", value: "late" },
  { label: "Early", value: "early" },
];

const OUT_TYPE_OPTIONS: Array<{ label: string; value: OutType }> = [
  { label: "All", value: "all" },
  { label: "Looking", value: "looking" },
  { label: "Chasing", value: "chasing" },
];

function formatPitchTypeLabel(value: string): string {
  if (!value) {
    return "Unknown";
  }
  return value
    .replace(/[_-]+/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatPitcherName(player: ShowPitcherSearchResult): string {
  return player.full_name || [player.first_name, player.last_name].filter(Boolean).join(" ") || String(player.mlb_id);
}

function formatHitterName(player: ShowHitterSearchResult): string {
  return player.full_name || [player.first_name, player.last_name].filter(Boolean).join(" ") || String(player.mlb_id);
}

type Props = {
  hitterSide: HitterSide;
  onChangeHitterSide: (value: HitterSide) => void;
  pitcherHand: PitcherHand;
  onChangePitcherHand: (value: PitcherHand) => void;
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
  onReset: () => void;
  hasAdvancedFilters: boolean;
};

export function StrikeoutFilters({
  hitterSide,
  onChangeHitterSide,
  pitcherHand,
  onChangePitcherHand,
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
  onReset,
  hasAdvancedFilters,
}: Props) {
  const [activeMenu, setActiveMenu] = useState<null | "hitter" | "pitcher">(null);
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

  useEffect(() => {
    if (activeMenu !== "hitter" && hitterSearchQuery) {
      onChangeHitterSearchQuery("");
    }
    if (activeMenu !== "pitcher" && pitcherSearchQuery) {
      onChangePitcherSearchQuery("");
    }
  }, [activeMenu, hitterSearchQuery, pitcherSearchQuery, onChangeHitterSearchQuery, onChangePitcherSearchQuery]);

  const hitterLabel =
    selectedHitter?.full_name ||
    HITTER_SIDE_OPTIONS.find((option) => option.value === hitterSide)?.label ||
    "All hitters";

  const pitcherLabel =
    selectedPitcher?.full_name ||
    PITCHER_HAND_OPTIONS.find((option) => option.value === pitcherHand)?.label ||
    "All pitchers";

  return (
    <section className={styles.analyticsFiltersBlock}>
      <div className={styles.analyticsFiltersTop} ref={filterRef}>
        <div className={styles.analyticsFiltersGrid}>
          <div className={styles.analyticsFilterField}>
            <span>Hitter Filter</span>
            <div className={styles.dropdownWrap}>
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
                  {selectedHitter ? (
                    <div className={styles.selectedPlayerRow}>
                      <p>{formatHitterName(selectedHitter)}</p>
                      <button type="button" onClick={() => onSelectHitter(null)}>
                        Clear
                      </button>
                    </div>
                  ) : null}

                  <input
                    className={styles.filterInput}
                    value={hitterSearchQuery}
                    onChange={(event) => onChangeHitterSearchQuery(event.target.value)}
                    placeholder="Search hitter..."
                  />

                  {hitterSearchQuery.trim() ? (
                    <div className={styles.searchResultsPanel}>
                      {hitterSearchLoading ? <p className={styles.muted}>Searching...</p> : null}
                      {hitterSearchError ? <p className={styles.error}>{hitterSearchError}</p> : null}
                      {!hitterSearchLoading && !hitterSearchError && hitterSearchResults.length === 0 ? (
                        <p className={styles.muted}>No matches.</p>
                      ) : null}
                      {hitterSearchResults.map((hitter) => (
                        <button
                          key={hitter.mlb_id}
                          type="button"
                          className={styles.searchResultButton}
                          onClick={() => {
                            onSelectHitter(hitter);
                            onChangeHitterSearchQuery("");
                            setActiveMenu(null);
                          }}
                        >
                          {formatHitterName(hitter)}
                        </button>
                      ))}
                    </div>
                  ) : null}

                  <div className={styles.dropdownDivider} />

                  {HITTER_SIDE_OPTIONS.map((option) => {
                    const active = option.value === hitterSide;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        className={active ? styles.dropdownOptionActive : styles.dropdownOption}
                        onClick={() => {
                          onChangeHitterSide(option.value);
                          onChangeHitterSearchQuery("");
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
          </div>

          <div className={styles.analyticsFilterField}>
            <span>Pitcher Filter</span>
            <div className={styles.dropdownWrap}>
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
                  {selectedPitcher ? (
                    <div className={styles.selectedPlayerRow}>
                      <p>{formatPitcherName(selectedPitcher)}</p>
                      <button type="button" onClick={() => onSelectPitcher(null)}>
                        Clear
                      </button>
                    </div>
                  ) : null}

                  <input
                    className={styles.filterInput}
                    value={pitcherSearchQuery}
                    onChange={(event) => onChangePitcherSearchQuery(event.target.value)}
                    placeholder="Search pitcher..."
                  />

                  {pitcherSearchQuery.trim() ? (
                    <div className={styles.searchResultsPanel}>
                      {pitcherSearchLoading ? <p className={styles.muted}>Searching...</p> : null}
                      {pitcherSearchError ? <p className={styles.error}>{pitcherSearchError}</p> : null}
                      {!pitcherSearchLoading && !pitcherSearchError && pitcherSearchResults.length === 0 ? (
                        <p className={styles.muted}>No matches.</p>
                      ) : null}
                      {pitcherSearchResults.map((pitcher) => (
                        <button
                          key={pitcher.mlb_id}
                          type="button"
                          className={styles.searchResultButton}
                          onClick={() => {
                            onSelectPitcher(pitcher);
                            onChangePitcherSearchQuery("");
                            setActiveMenu(null);
                          }}
                        >
                          {formatPitcherName(pitcher)}
                        </button>
                      ))}
                    </div>
                  ) : null}

                  <div className={styles.dropdownDivider} />

                  {PITCHER_HAND_OPTIONS.map((option) => {
                    const active = option.value === pitcherHand;
                    return (
                      <button
                        key={option.value}
                        type="button"
                        className={active ? styles.dropdownOptionActive : styles.dropdownOption}
                        onClick={() => {
                          onChangePitcherHand(option.value);
                          onChangePitcherSearchQuery("");
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
          </div>
        </div>

        <div className={styles.analyticsFilterActions}>
          <button
            type="button"
            className={hasAdvancedFilters || advancedOpen ? styles.filterActionButtonActive : styles.filterActionButton}
            onClick={onToggleAdvanced}
          >
            Advanced
          </button>
          <button type="button" className={styles.filterActionReset} onClick={onReset}>
            Reset
          </button>
        </div>
      </div>

      {advancedOpen ? (
        <div className={styles.advancedModalOverlay}>
          <button
            type="button"
            className={styles.advancedBackdrop}
            aria-label="Close advanced filters"
            onClick={onCloseAdvanced}
          />
          <div className={styles.advancedModal}>
            <div className={styles.advancedModalHeader}>
              <p>Advanced Filters</p>
              <button type="button" className={styles.filterActionButton} onClick={onCloseAdvanced}>
                Close
              </button>
            </div>

            <div className={styles.analyticsAdvancedPanel}>
              <label className={styles.analyticsFilterField}>
                <span>Min Speed</span>
                <input
                  className={styles.filterNumber}
                  value={minSpeed}
                  onChange={(event) => onChangeMinSpeed(event.target.value)}
                  placeholder="0-99"
                />
              </label>
              <label className={styles.analyticsFilterField}>
                <span>Max Speed</span>
                <input
                  className={styles.filterNumber}
                  value={maxSpeed}
                  onChange={(event) => onChangeMaxSpeed(event.target.value)}
                  placeholder="0-99"
                />
              </label>
              <label className={styles.analyticsFilterField}>
                <span>Timing</span>
                <select
                  className={styles.filterSelect}
                  value={timing}
                  onChange={(event) => onChangeTiming(event.target.value as TimingType)}
                >
                  {TIMING_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className={styles.analyticsFilterField}>
                <span>Out Type</span>
                <select
                  className={styles.filterSelect}
                  value={outType}
                  onChange={(event) => onChangeOutType(event.target.value as OutType)}
                >
                  {OUT_TYPE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <div className={styles.pitchTypeFilters}>
                <span>Pitch Types</span>
                <div className={styles.pitchTypeChips}>
                  <button
                    type="button"
                    className={selectedPitchTypes.length === 0 ? styles.pitchTypeChipActive : styles.pitchTypeChip}
                    onClick={() => onTogglePitchType("all")}
                  >
                    All
                  </button>
                  {pitchTypeOptions.map((pitchType) => (
                    <button
                      key={pitchType}
                      type="button"
                      className={selectedPitchTypes.includes(pitchType) ? styles.pitchTypeChipActive : styles.pitchTypeChip}
                      onClick={() => onTogglePitchType(pitchType)}
                    >
                      {formatPitchTypeLabel(pitchType)}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </section>
  );
}
