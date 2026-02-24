"use client";

import { formatPercent } from "./format";
import type { HitDataMap, PitchTypeRank, StrikeoutMapData } from "./types";
import styles from "./styles.module.css";

type Props = {
  hitData: HitDataMap | null;
  strikeoutMapHitting: StrikeoutMapData | null;
  pitchTypeRanks: PitchTypeRank[];
  pitchTypeRanksLoading: boolean;
  pitchTypeRanksError: string | null;
  coachingLoading: boolean;
  coachingError: string | null;
};

function clamp(value: number, min: number, max: number) {
  return Math.max(min, Math.min(max, value));
}

export function CoachingSection({
  hitData,
  strikeoutMapHitting,
  pitchTypeRanks,
  pitchTypeRanksLoading,
  pitchTypeRanksError,
  coachingLoading,
  coachingError,
}: Props) {
  const hitStats = hitData?.stats;
  const strikeStats = strikeoutMapHitting?.stats;

  const flyRate = hitStats?.flyball_rate;
  const popupRate = hitStats?.popup_rate;
  const groundRate = hitStats?.groundball_rate;
  const perfectRate = hitStats?.perfect_perfect_pct;

  let launchTilt: number | null = null;
  if (
    flyRate !== undefined &&
    popupRate !== undefined &&
    groundRate !== undefined &&
    perfectRate !== undefined
  ) {
    const denom = 100 - perfectRate;
    if (denom > 0) {
      launchTilt = clamp((flyRate + popupRate - groundRate) / denom, -1, 1);
    }
  }

  const launchTiltStatus = launchTilt === null ? "unknown" : launchTilt < -0.2 ? "low" : launchTilt > 0.2 ? "high" : "ok";
  const launchTiltAdvice =
    launchTiltStatus === "low"
      ? "Start higher in your swing (PCI anchoring)."
      : launchTiltStatus === "high"
        ? "Start lower in your swing (PCI anchoring)."
        : launchTiltStatus === "ok"
          ? "All good."
          : "Not enough data yet.";

  const heartMissK = strikeStats?.heart_miss_k_pct ?? null;
  const inzoneSwingK = strikeStats?.inzone_swing_k_pct ?? null;
  const extremeContact = hitStats?.extreme_contact_nopp_pct ?? null;

  let slamScore: number | null = null;
  if (heartMissK !== null && inzoneSwingK !== null && extremeContact !== null) {
    slamScore = clamp(0.45 * heartMissK + 0.3 * inzoneSwingK + 0.25 * extremeContact, 0, 100);
  }

  const rankRows = [...pitchTypeRanks]
    .filter((row) => row.pitchType)
    .sort((a, b) => (b.kPct ?? -Infinity) - (a.kPct ?? -Infinity));

  const pillClass =
    launchTiltStatus === "ok"
      ? styles.pillGood
      : launchTiltStatus === "high"
        ? styles.pillWarn
        : launchTiltStatus === "low"
          ? styles.pillBad
          : "";

  return (
    <section className={styles.coachingStack}>
      <article className={styles.coachingCard}>
        <div className={styles.coachingHeaderRow}>
          <h3 className={styles.sectionTitle}>Launch Tilt</h3>
          <p className={styles.coachingMetric}>{launchTilt === null ? "-" : launchTilt.toFixed(2)}</p>
        </div>
        <p className={styles.coachingDescription}>
          Balance between ground and lifted contact after excluding perfect-perfect share.
        </p>
        <p className={`${styles.coachingPill} ${pillClass}`}>{launchTiltAdvice}</p>
      </article>

      <article className={styles.coachingCard}>
        <div className={styles.coachingHeaderRow}>
          <h3 className={styles.sectionTitle}>PCI Slamming Score</h3>
          <p className={styles.coachingMetric}>{slamScore === null ? "-" : `${slamScore.toFixed(1)}%`}</p>
        </div>
        <p className={styles.coachingDescription}>
          Combines heart miss K%, in-zone swing K%, and extreme contact%.
        </p>
        <div className={styles.summaryGrid}>
          <div className={styles.summaryChip}>
            <p className={styles.summaryLabel}>Heart Miss K%</p>
            <p className={styles.summaryValue}>{formatPercent(heartMissK)}</p>
          </div>
          <div className={styles.summaryChip}>
            <p className={styles.summaryLabel}>In-zone Swing K%</p>
            <p className={styles.summaryValue}>{formatPercent(inzoneSwingK)}</p>
          </div>
          <div className={styles.summaryChip}>
            <p className={styles.summaryLabel}>Extreme Contact%</p>
            <p className={styles.summaryValue}>{formatPercent(extremeContact)}</p>
          </div>
        </div>
      </article>

      <article className={styles.coachingCard}>
        <div className={styles.coachingHeaderRow}>
          <h3 className={styles.sectionTitle}>Worst & Best Pitches</h3>
          <p className={styles.muted}>Sorted by K% (hitting view)</p>
        </div>

        {coachingLoading ? <p className={styles.muted}>Loading coaching metrics...</p> : null}
        {coachingError ? <p className={styles.error}>{coachingError}</p> : null}
        {pitchTypeRanksLoading ? <p className={styles.muted}>Loading pitch rankings...</p> : null}
        {pitchTypeRanksError ? <p className={styles.error}>{pitchTypeRanksError}</p> : null}

        <div className={styles.pitchRankList}>
          {rankRows.slice(0, 8).map((row, index) => (
            <div key={`${row.pitchType}-${index}`} className={styles.pitchRankRow}>
              <p className={styles.pitchRankIndex}>#{index + 1}</p>
              <p className={styles.pitchRankLabel}>{row.pitchType}</p>
              <p className={styles.pitchRankValue}>{formatPercent(row.kPct)}</p>
            </div>
          ))}

          {!pitchTypeRanksLoading && rankRows.length === 0 ? (
            <p className={styles.muted}>No pitch ranking data available.</p>
          ) : null}
        </div>
      </article>
    </section>
  );
}
