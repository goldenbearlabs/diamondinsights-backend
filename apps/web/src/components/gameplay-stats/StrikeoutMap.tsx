"use client";

import Image from "next/image";
import { useMemo } from "react";

import type { StrikeoutMapData, StrikeoutOutsideKey, StrikeoutSelection } from "./types";
import styles from "./styles.module.css";

type HitterSideFilter = {
  side: "left" | "right" | "all";
};

type Props = {
  zones: number[][];
  outside: StrikeoutMapData["outside"];
  filterHitterSide?: HitterSideFilter;
  selections?: StrikeoutSelection[];
  onSelectionChange?: (selection: StrikeoutSelection) => void;
};

const EMPTY_OUTSIDE: Record<StrikeoutOutsideKey, number> = {
  top_left: 0,
  top: 0,
  top_right: 0,
  right: 0,
  bottom_right: 0,
  bottom: 0,
  bottom_left: 0,
  left: 0,
};

const OUTSIDE_POSITIONS: Array<{
  key: StrikeoutOutsideKey;
  className: keyof typeof styles;
}> = [
  { key: "top_left", className: "outsideTopLeft" },
  { key: "top", className: "outsideTop" },
  { key: "top_right", className: "outsideTopRight" },
  { key: "right", className: "outsideRight" },
  { key: "bottom_right", className: "outsideBottomRight" },
  { key: "bottom", className: "outsideBottom" },
  { key: "bottom_left", className: "outsideBottomLeft" },
  { key: "left", className: "outsideLeft" },
];

function normalizeZones(input: number[][] | null | undefined): number[][] {
  return Array.from({ length: 3 }, (_, rowIndex) =>
    Array.from({ length: 3 }, (_, colIndex) => {
      const value = input?.[rowIndex]?.[colIndex];
      return Number.isFinite(value) ? Number(value) : 0;
    }),
  );
}

export function StrikeoutMap({ zones, outside, filterHitterSide, selections, onSelectionChange }: Props) {
  const safeZones = useMemo(() => normalizeZones(zones), [zones]);
  const safeOutside = outside ?? EMPTY_OUTSIDE;

  const max = useMemo(() => {
    const insideMax = Math.max(0, ...safeZones.flat());
    const outsideMax = Math.max(...Object.values(safeOutside), 0);
    return Math.max(1, insideMax, outsideMax);
  }, [safeZones, safeOutside]);

  const heatStyle = (count: number) => {
    const intensity = Math.min(1, count / max);
    const alpha = 0.12 + intensity * 0.65;
    return { backgroundColor: `rgba(248, 113, 113, ${alpha})` };
  };

  const hitterSide = filterHitterSide?.side ?? "all";
  const showLeftHitter = hitterSide === "all" || hitterSide === "left";
  const showRightHitter = hitterSide === "all" || hitterSide === "right";

  const isSelected = (candidate: StrikeoutSelection): boolean => {
    if (!selections || selections.length === 0) {
      return false;
    }
    if (candidate.kind === "zone") {
      return selections.some(
        (selection) => selection.kind === "zone" && selection.row === candidate.row && selection.col === candidate.col,
      );
    }
    return selections.some(
      (selection) => selection.kind === "outside" && selection.key === candidate.key,
    );
  };

  const handleSelection = (selection: StrikeoutSelection) => {
    if (!onSelectionChange) {
      return;
    }
    onSelectionChange(selection);
  };

  return (
    <div className={styles.strikeMapPanel}>
      <div className={styles.strikeMapRow}>
        <Image
          src="/images/lefty_hitter.png"
          alt="Left-handed batter"
          width={220}
          height={460}
          className={`${styles.strikeHitter} ${styles.strikeHitterLeft} ${!showLeftHitter ? styles.strikeHitterHidden : ""}`}
          priority={false}
        />

        <div className={styles.strikeMapColumn}>
          <div className={styles.strikeMapFrame}>
            <div className={styles.strikeZone}>
              <div className={styles.zoneGrid}>
                {safeZones.map((row, rowIndex) => (
                  <div key={`row-${rowIndex}`} className={styles.zoneRow}>
                    {row.map((count, colIndex) => (
                      <button
                        key={`cell-${rowIndex}-${colIndex}`}
                        type="button"
                        className={styles.zoneCell}
                        style={heatStyle(count)}
                        onClick={() => handleSelection({ kind: "zone", row: rowIndex, col: colIndex })}
                      >
                        <strong>{count}</strong>
                        {isSelected({ kind: "zone", row: rowIndex, col: colIndex }) ? <span className={styles.zoneSelectionRing} /> : null}
                      </button>
                    ))}
                  </div>
                ))}
              </div>

              <div className={`${styles.gridLineVertical} ${styles.gridLineV1}`} />
              <div className={`${styles.gridLineVertical} ${styles.gridLineV2}`} />
              <div className={`${styles.gridLineHorizontal} ${styles.gridLineH1}`} />
              <div className={`${styles.gridLineHorizontal} ${styles.gridLineH2}`} />
            </div>

            <div className={styles.outsideLabels}>
              {OUTSIDE_POSITIONS.map((outsideItem) => (
                <button
                  key={outsideItem.key}
                  type="button"
                  className={`${styles.outsideValue} ${styles[outsideItem.className]}`}
                  style={heatStyle(safeOutside[outsideItem.key] ?? 0)}
                  onClick={() => handleSelection({ kind: "outside", key: outsideItem.key })}
                >
                  <span>{safeOutside[outsideItem.key] ?? 0}</span>
                  {isSelected({ kind: "outside", key: outsideItem.key }) ? <span className={styles.outsideSelectionRing} /> : null}
                </button>
              ))}
            </div>
          </div>

          <div className={styles.plateWrap}>
            <div className={styles.platePoint} />
            <div className={styles.plateTop} />
          </div>
        </div>

        <Image
          src="/images/right_hitter.png"
          alt="Right-handed batter"
          width={220}
          height={460}
          className={`${styles.strikeHitter} ${styles.strikeHitterRight} ${!showRightHitter ? styles.strikeHitterHidden : ""}`}
          priority={false}
        />
      </div>
    </div>
  );
}
