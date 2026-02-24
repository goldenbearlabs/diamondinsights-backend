"use client";

import { useMemo, useState } from "react";

import { formatCardName, formatCount, formatPercent, formatRate, sortHittingCards, sortPitchingCards } from "./format";
import type { ShowCardPitchingStats, ShowCardStats } from "./types";
import styles from "./styles.module.css";

type Props = {
  hittingCards: ShowCardStats[];
  pitchingCards: ShowCardPitchingStats[];
  loadingHitting: boolean;
  loadingPitching: boolean;
  errorHitting: string | null;
  errorPitching: string | null;
};

type SortDirection = "asc" | "desc";
type HittingSortKey = "player" | "pa" | "avg" | "obp" | "slg" | "ops" | "k_pct" | "bb_pct" | "hr";
type PitchingSortKey = "player" | "pa" | "era" | "whip" | "k_pct" | "bb_pct" | "hr_pct";

function HeaderButton({
  label,
  active,
  direction,
  onClick,
}: {
  label: string;
  active: boolean;
  direction: SortDirection;
  onClick: () => void;
}) {
  return (
    <button type="button" onClick={onClick}>
      {label}
      {active ? (direction === "asc" ? " ▲" : " ▼") : ""}
    </button>
  );
}

export function CardsSection({
  hittingCards,
  pitchingCards,
  loadingHitting,
  loadingPitching,
  errorHitting,
  errorPitching,
}: Props) {
  const [hittingFilter, setHittingFilter] = useState("");
  const [hittingMinPa, setHittingMinPa] = useState("");
  const [hittingSortKey, setHittingSortKey] = useState<HittingSortKey>("pa");
  const [hittingSortDir, setHittingSortDir] = useState<SortDirection>("desc");

  const [pitchingFilter, setPitchingFilter] = useState("");
  const [pitchingMinBf, setPitchingMinBf] = useState("");
  const [pitchingSortKey, setPitchingSortKey] = useState<PitchingSortKey>("pa");
  const [pitchingSortDir, setPitchingSortDir] = useState<SortDirection>("desc");

  const filteredHitting = useMemo(() => {
    const needle = hittingFilter.trim().toLowerCase();
    const minPa = hittingMinPa.trim() ? Number.parseInt(hittingMinPa, 10) : null;

    const rows = hittingCards.filter((row) => {
      if (minPa !== null && !Number.isNaN(minPa) && row.pa < minPa) {
        return false;
      }
      if (!needle) {
        return true;
      }
      const name = formatCardName(row).toLowerCase();
      return name.includes(needle) || String(row.mlb_id).includes(needle);
    });

    return sortHittingCards(rows, hittingSortKey as keyof ShowCardStats | "player", hittingSortDir);
  }, [hittingCards, hittingFilter, hittingMinPa, hittingSortKey, hittingSortDir]);

  const filteredPitching = useMemo(() => {
    const needle = pitchingFilter.trim().toLowerCase();
    const minBf = pitchingMinBf.trim() ? Number.parseInt(pitchingMinBf, 10) : null;

    const rows = pitchingCards.filter((row) => {
      if (minBf !== null && !Number.isNaN(minBf) && row.pa < minBf) {
        return false;
      }
      if (!needle) {
        return true;
      }
      const name = formatCardName(row).toLowerCase();
      return name.includes(needle) || String(row.mlb_id).includes(needle);
    });

    return sortPitchingCards(rows, pitchingSortKey as keyof ShowCardPitchingStats | "player", pitchingSortDir);
  }, [pitchingCards, pitchingFilter, pitchingMinBf, pitchingSortKey, pitchingSortDir]);

  const onHittingSort = (key: HittingSortKey) => {
    if (hittingSortKey === key) {
      setHittingSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setHittingSortKey(key);
    setHittingSortDir("desc");
  };

  const onPitchingSort = (key: PitchingSortKey) => {
    if (pitchingSortKey === key) {
      setPitchingSortDir((prev) => (prev === "asc" ? "desc" : "asc"));
      return;
    }
    setPitchingSortKey(key);
    setPitchingSortDir("desc");
  };

  return (
    <section className={styles.cardsSection}>
      <div className={styles.sectionHeaderRow}>
        <div>
          <h3 className={styles.sectionTitle}>Cards</h3>
          <p className={styles.sectionSubtitle}>Hitting and pitching card performance snapshots.</p>
        </div>
      </div>

      <div className={styles.cardsStack}>
        <article className={styles.card}>
          <div className={styles.tableHeaderRow}>
            <div>
              <h3 className={styles.sectionTitle}>Hitting Cards</h3>
              <p className={styles.tableSubtitle}>Boxscore and rate metrics by card.</p>
            </div>
          </div>

          <div className={styles.tableControls}>
            <input
              className={styles.filterInput}
              value={hittingFilter}
              onChange={(event) => setHittingFilter(event.target.value)}
              placeholder="Filter by player or MLB ID"
            />
            <input
              className={styles.filterNumber}
              value={hittingMinPa}
              onChange={(event) => setHittingMinPa(event.target.value.replace(/[^0-9]/g, ""))}
              placeholder="Min PA"
            />
          </div>

          {loadingHitting ? <p className={styles.muted}>Loading hitting cards...</p> : null}
          {errorHitting ? <p className={styles.error}>{errorHitting}</p> : null}

          <div className={styles.tableShell}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th><HeaderButton label="Player" active={hittingSortKey === "player"} direction={hittingSortDir} onClick={() => onHittingSort("player")} /></th>
                  <th><HeaderButton label="PA" active={hittingSortKey === "pa"} direction={hittingSortDir} onClick={() => onHittingSort("pa")} /></th>
                  <th><HeaderButton label="AVG" active={hittingSortKey === "avg"} direction={hittingSortDir} onClick={() => onHittingSort("avg")} /></th>
                  <th><HeaderButton label="OBP" active={hittingSortKey === "obp"} direction={hittingSortDir} onClick={() => onHittingSort("obp")} /></th>
                  <th><HeaderButton label="SLG" active={hittingSortKey === "slg"} direction={hittingSortDir} onClick={() => onHittingSort("slg")} /></th>
                  <th><HeaderButton label="OPS" active={hittingSortKey === "ops"} direction={hittingSortDir} onClick={() => onHittingSort("ops")} /></th>
                  <th><HeaderButton label="K%" active={hittingSortKey === "k_pct"} direction={hittingSortDir} onClick={() => onHittingSort("k_pct")} /></th>
                  <th><HeaderButton label="BB%" active={hittingSortKey === "bb_pct"} direction={hittingSortDir} onClick={() => onHittingSort("bb_pct")} /></th>
                  <th><HeaderButton label="HR" active={hittingSortKey === "hr"} direction={hittingSortDir} onClick={() => onHittingSort("hr")} /></th>
                </tr>
              </thead>
              <tbody>
                {filteredHitting.map((row) => (
                  <tr key={`hit-${row.mlb_id}`}>
                    <td>{formatCardName(row)}</td>
                    <td>{formatCount(row.pa)}</td>
                    <td>{formatRate(row.avg)}</td>
                    <td>{formatRate(row.obp)}</td>
                    <td>{formatRate(row.slg)}</td>
                    <td>{formatRate(row.ops)}</td>
                    <td>{formatPercent(row.k_pct)}</td>
                    <td>{formatPercent(row.bb_pct)}</td>
                    <td>{formatCount(row.hr)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>

        <article className={styles.card}>
          <div className={styles.tableHeaderRow}>
            <div>
              <h3 className={styles.sectionTitle}>Pitching Cards</h3>
              <p className={styles.tableSubtitle}>Pitching outcomes and strikeout rates by card.</p>
            </div>
          </div>

          <div className={styles.tableControls}>
            <input
              className={styles.filterInput}
              value={pitchingFilter}
              onChange={(event) => setPitchingFilter(event.target.value)}
              placeholder="Filter by player or MLB ID"
            />
            <input
              className={styles.filterNumber}
              value={pitchingMinBf}
              onChange={(event) => setPitchingMinBf(event.target.value.replace(/[^0-9]/g, ""))}
              placeholder="Min BF"
            />
          </div>

          {loadingPitching ? <p className={styles.muted}>Loading pitching cards...</p> : null}
          {errorPitching ? <p className={styles.error}>{errorPitching}</p> : null}

          <div className={styles.tableShell}>
            <table className={styles.table}>
              <thead>
                <tr>
                  <th><HeaderButton label="Player" active={pitchingSortKey === "player"} direction={pitchingSortDir} onClick={() => onPitchingSort("player")} /></th>
                  <th><HeaderButton label="BF" active={pitchingSortKey === "pa"} direction={pitchingSortDir} onClick={() => onPitchingSort("pa")} /></th>
                  <th><HeaderButton label="ERA" active={pitchingSortKey === "era"} direction={pitchingSortDir} onClick={() => onPitchingSort("era")} /></th>
                  <th><HeaderButton label="WHIP" active={pitchingSortKey === "whip"} direction={pitchingSortDir} onClick={() => onPitchingSort("whip")} /></th>
                  <th><HeaderButton label="K%" active={pitchingSortKey === "k_pct"} direction={pitchingSortDir} onClick={() => onPitchingSort("k_pct")} /></th>
                  <th><HeaderButton label="BB%" active={pitchingSortKey === "bb_pct"} direction={pitchingSortDir} onClick={() => onPitchingSort("bb_pct")} /></th>
                  <th><HeaderButton label="HR%" active={pitchingSortKey === "hr_pct"} direction={pitchingSortDir} onClick={() => onPitchingSort("hr_pct")} /></th>
                </tr>
              </thead>
              <tbody>
                {filteredPitching.map((row) => (
                  <tr key={`pit-${row.mlb_id}`}>
                    <td>{formatCardName(row)}</td>
                    <td>{formatCount(row.pa)}</td>
                    <td>{formatRate(row.era ?? null)}</td>
                    <td>{formatRate(row.whip ?? null)}</td>
                    <td>{formatPercent(row.k_pct)}</td>
                    <td>{formatPercent(row.bb_pct)}</td>
                    <td>{formatPercent(row.hr_pct)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </div>
    </section>
  );
}
