"use client";

import { barColor, clampPercent, formatPercent } from "./format";
import type {
  BattingArchetype,
  PitchingArchetype,
  ShowGameSummary,
  ShowSkills,
} from "./types";
import styles from "./styles.module.css";

type Props = {
  gameSummary: ShowGameSummary | null;
  skills: ShowSkills | null;
  battingArchetype: BattingArchetype | null;
  pitchingArchetype: PitchingArchetype | null;
  skillMode: "Hitting" | "Pitching";
  onChangeSkillMode: (mode: "Hitting" | "Pitching") => void;
};

function formatDate(value?: string | null): string {
  if (!value) {
    return "-";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return "-";
  }
  return parsed.toLocaleDateString();
}

function formatRate(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(3);
}

function formatRatio(value?: number | null): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(2);
}

export function SummaryCards({
  gameSummary,
  skills,
  battingArchetype,
  pitchingArchetype,
  skillMode,
  onChangeSkillMode,
}: Props) {
  const detailRows = [
    { label: "Games Played", value: String(gameSummary?.games_played ?? 0) },
    { label: "Difficulty", value: gameSummary?.last_game_difficulty ?? "-" },
    { label: "Last Game Played", value: formatDate(gameSummary?.last_game_date) },
    { label: "Record", value: gameSummary?.record ?? "0-0" },
  ];

  const currentSkills = skillMode === "Hitting" ? skills?.hitting : skills?.pitching;
  const skillRows = [
    { label: "AVG", value: formatRate(currentSkills?.avg) },
    { label: "OBP", value: formatRate(currentSkills?.obp) },
    { label: "SLUG", value: formatRate(currentSkills?.slg) },
    { label: "OPS", value: formatRate(currentSkills?.ops) },
    { label: "K/BB", value: formatRatio(currentSkills?.kbb) },
  ];

  const percentRows = [
    { label: "Batting", value: battingArchetype?.overall ?? null, emphasis: true },
    { label: "Timing", value: battingArchetype?.timing ?? null },
    { label: "Location", value: battingArchetype?.location ?? null },
    { label: "Power", value: battingArchetype?.power ?? null },
    { label: "Pitching", value: pitchingArchetype?.overall ?? null, emphasis: true },
    { label: "Consistency", value: pitchingArchetype?.consistency ?? null },
    { label: "Strikeout", value: pitchingArchetype?.strikeout ?? null },
    { label: "Location", value: pitchingArchetype?.location ?? null },
  ];

  return (
    <section className={styles.cardsRow}>
      <article className={styles.card}>
        <h2>Details</h2>
        <div className={styles.cardBody}>
          {detailRows.map((row) => (
            <div key={row.label} className={styles.detailRow}>
              <span>{row.label}</span>
              <strong>{row.value}</strong>
            </div>
          ))}
        </div>

        <div className={styles.cardDivider} />

        <h3>Stats</h3>
        <div className={styles.toggleRow}>
          <div className={styles.toggle}>
            {(["Hitting", "Pitching"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                className={skillMode === mode ? styles.toggleButtonActive : styles.toggleButton}
                onClick={() => onChangeSkillMode(mode)}
              >
                {mode}
              </button>
            ))}
          </div>
        </div>

        <div className={styles.cardBody}>
          {skillRows.map((row) => (
            <div key={row.label} className={styles.detailRow}>
              <span>{row.label}</span>
              <strong>{row.value}</strong>
            </div>
          ))}
        </div>
      </article>

      <article className={styles.card}>
        <h2>Skills</h2>
        <div className={styles.cardBody}>
          {percentRows.map((row, index) => {
            const isDivider = index === 4;
            const pct = row.value;
            const width = clampPercent(pct);
            return (
              <div key={`${row.label}-${index}`}>
                {isDivider ? <div className={styles.skillDivider} /> : null}
                <div className={row.emphasis ? styles.percentRowEmphasis : styles.percentRow}>
                  <div className={styles.percentRowHeader}>
                    <span>{row.label}</span>
                    <strong className={row.emphasis ? styles.percentEmphasis : undefined}>
                      {formatPercent(pct)}
                    </strong>
                  </div>
                  <div className={row.emphasis ? styles.percentTrackEmphasis : styles.percentTrack}>
                    <div
                      className={styles.percentFill}
                      style={{ width: `${width}%`, backgroundColor: barColor(pct) }}
                    />
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </article>
    </section>
  );
}
