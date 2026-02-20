import { useMemo } from "react";
import { Image, Pressable, StyleSheet, Text, View, useWindowDimensions } from "react-native";

import { theme } from "../theme/colors";

const LEFTY_HITTER = require("../../assets/images/lefty_hitter.png");
const RIGHT_HITTER = require("../../assets/images/right_hitter.png");
const GRID_LINE = Math.max(0.5, StyleSheet.hairlineWidth);

type StrikeoutOutside = {
  top_left: number;
  top: number;
  top_right: number;
  right: number;
  bottom_right: number;
  bottom: number;
  bottom_left: number;
  left: number;
};

type StrikeoutSelection =
  | { kind: "zone"; row: number; col: number }
  | { kind: "outside"; key: keyof StrikeoutOutside }
  | null;

type StrikeoutMapProps = {
  zones: number[][];
  outside: StrikeoutOutside;
  filterHitterSide?: {
    side: "left" | "right" | "all";
  };
  selections?: StrikeoutSelection[];
  onSelectionChange?: (selection: StrikeoutSelection) => void;
};

export const StrikeoutMap = ({
  zones,
  outside,
  filterHitterSide,
  selections,
  onSelectionChange,
}: StrikeoutMapProps) => {
  const { width } = useWindowDimensions();
  const mapSize = Math.min(220, Math.max(140, Math.round(width - 240)));
  const outsideThickness = Math.max(28, Math.round(mapSize * 0.24));
  const half = mapSize / 2;
  const strikeBump = Math.max(4, Math.round(mapSize * 0.07));
  const strikeSize = Math.min(mapSize - 8, mapSize - outsideThickness * 2 + strikeBump);
  const strikeInset = Math.max(0, (mapSize - strikeSize) / 2);
  const cornerOffset = Math.min(8, Math.round(outsideThickness * 0.45));
  const edgeOffset = Math.min(0, Math.round(outsideThickness * 0.7));
  const plateWidth = Math.max(36, Math.round(strikeSize * 0.6));
  const plateHeight = Math.max(16, Math.round(strikeSize * 0.22));
  const hitterSide = filterHitterSide?.side ?? "all";
  const showLeftHitter = hitterSide === "all" || hitterSide === "left";
  const showRightHitter = hitterSide === "all" || hitterSide === "right";
  const onSelect = (next: StrikeoutSelection) => {
    if (!onSelectionChange) return;
    onSelectionChange(next);
  };
  const isSelected = (candidate: StrikeoutSelection) => {
    if (!selections || selections.length === 0) return false;
    if (candidate.kind === "zone") {
      return selections.some(
        (sel) =>
          sel.kind === "zone" &&
          sel.row === candidate.row &&
          sel.col === candidate.col
      );
    }
    return selections.some(
      (sel) => sel.kind === "outside" && sel.key === candidate.key
    );
  };

  const max = useMemo(() => {
    const insideMax = Math.max(0, ...zones.flat());
    const outsideMax = Math.max(
      outside.top_left,
      outside.top,
      outside.top_right,
      outside.right,
      outside.bottom_right,
      outside.bottom,
      outside.bottom_left,
      outside.left
    );
    return Math.max(1, insideMax, outsideMax);
  }, [zones, outside]);

  const heatStyle = (count: number, opts?: { soft?: boolean }) => {
    const intensity = Math.min(1, count / max);
    const base = opts?.soft ? 0.12 : 0.2;
    const scale = opts?.soft ? 0.55 : 0.65;
    const alpha = base + intensity * scale;
    return {
      backgroundColor: `rgba(248, 113, 113, ${alpha})`,
    };
  };

  return (
    <View style={styles.panel}>
      <View style={styles.mapRow}>
        <Image
          source={LEFTY_HITTER}
          style={[
            styles.hitter,
            styles.hitterLeft,
            !showLeftHitter && { opacity: 0 },
          ]}
          resizeMode="contain"
        />

        <View style={styles.mapColumn}>
          <View style={[styles.mapFrame, { width: mapSize, height: mapSize }]}>
            <View style={styles.mapSurface}>
              <View style={styles.outsideLayer}>
                <View
                  style={[
                    styles.outsidePiece,
                    { top: 0, left: 0, width: half, height: outsideThickness },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { top: 0, left: 0, width: outsideThickness, height: half },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { top: 0, right: 0, width: half, height: outsideThickness },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { top: 0, right: 0, width: outsideThickness, height: half },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { bottom: 0, left: 0, width: half, height: outsideThickness },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { bottom: 0, left: 0, width: outsideThickness, height: half },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { bottom: 0, right: 0, width: half, height: outsideThickness },
                  ]}
                />
                <View
                  style={[
                    styles.outsidePiece,
                    { bottom: 0, right: 0, width: outsideThickness, height: half },
                  ]}
                />
              </View>

              <View
                style={[
                  styles.strikeZone,
                  {
                    width: strikeSize,
                    height: strikeSize,
                    top: strikeInset,
                    left: strikeInset,
                  },
                ]}
              >
                <View style={styles.zoneGrid}>
                  {zones.map((row, rowIndex) => (
                    <View key={`row-${rowIndex}`} style={styles.zoneRow}>
                      {row.map((count, colIndex) => {
                        const selected = isSelected({
                          kind: "zone",
                          row: rowIndex,
                          col: colIndex,
                        });
                        return (
                        <Pressable
                          key={`cell-${rowIndex}-${colIndex}`}
                          style={[styles.zoneCell, heatStyle(count)]}
                          onPress={() =>
                            onSelect({ kind: "zone", row: rowIndex, col: colIndex })
                          }
                        >
                          <Text style={styles.zoneValue}>{count}</Text>
                          {selected ? (
                            <View pointerEvents="none" style={styles.zoneSelectionRing} />
                          ) : null}
                        </Pressable>
                      );
                      })}
                    </View>
                  ))}
                </View>
                <View style={[styles.gridLineVertical, { left: "33.333%" }]} />
                <View style={[styles.gridLineVertical, { left: "66.666%" }]} />
                <View style={[styles.gridLineHorizontal, { top: "33.333%" }]} />
                <View style={[styles.gridLineHorizontal, { top: "66.666%" }]} />
              </View>

              <View style={styles.outsideLabels}>
                <Pressable
                  style={[
                    styles.outsideValue,
                    { top: cornerOffset, left: cornerOffset },
                    heatStyle(outside.top_left),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "top_left" })}
                >
                  <Text style={styles.outsideValueText}>{outside.top_left}</Text>
                  {isSelected({ kind: "outside", key: "top_left" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    styles.outsideValueTop,
                    { top: edgeOffset },
                    heatStyle(outside.top),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "top" })}
                >
                  <Text style={styles.outsideValueText}>{outside.top}</Text>
                  {isSelected({ kind: "outside", key: "top" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    { top: cornerOffset, right: cornerOffset },
                    heatStyle(outside.top_right),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "top_right" })}
                >
                  <Text style={styles.outsideValueText}>{outside.top_right}</Text>
                  {isSelected({ kind: "outside", key: "top_right" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    styles.outsideValueRight,
                    { right: edgeOffset },
                    heatStyle(outside.right),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "right" })}
                >
                  <Text style={styles.outsideValueText}>{outside.right}</Text>
                  {isSelected({ kind: "outside", key: "right" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    { bottom: cornerOffset, right: cornerOffset },
                    heatStyle(outside.bottom_right),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "bottom_right" })}
                >
                  <Text style={styles.outsideValueText}>{outside.bottom_right}</Text>
                  {isSelected({ kind: "outside", key: "bottom_right" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    styles.outsideValueBottom,
                    { bottom: edgeOffset },
                    heatStyle(outside.bottom),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "bottom" })}
                >
                  <Text style={styles.outsideValueText}>{outside.bottom}</Text>
                  {isSelected({ kind: "outside", key: "bottom" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    { bottom: cornerOffset, left: cornerOffset },
                    heatStyle(outside.bottom_left),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "bottom_left" })}
                >
                  <Text style={styles.outsideValueText}>{outside.bottom_left}</Text>
                  {isSelected({ kind: "outside", key: "bottom_left" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
                <Pressable
                  style={[
                    styles.outsideValue,
                    styles.outsideValueLeft,
                    { left: edgeOffset },
                    heatStyle(outside.left),
                  ]}
                  onPress={() => onSelect({ kind: "outside", key: "left" })}
                >
                  <Text style={styles.outsideValueText}>{outside.left}</Text>
                  {isSelected({ kind: "outside", key: "left" }) ? (
                    <View pointerEvents="none" style={styles.outsideSelectionRing} />
                  ) : null}
                </Pressable>
              </View>
            </View>
          </View>
          <View
            style={[
              styles.plateWrap,
              {
                width: plateWidth,
                height: plateHeight,
              },
            ]}
          >
            <View
              style={[
                styles.platePoint,
                {
                  borderLeftWidth: plateWidth / 2.1,
                  borderRightWidth: plateWidth / 2.1,
                  borderBottomWidth: plateHeight * 0.5,
                },
              ]}
            />
            <View style={[styles.plateTop, { width: plateWidth, height: plateHeight * 0.8 }]} />
          </View>
        </View>

        <Image
          source={RIGHT_HITTER}
          style={[
            styles.hitter,
            styles.hitterRight,
            !showRightHitter && { opacity: 0 },
          ]}
          resizeMode="contain"
        />
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  panel: {
    marginTop: 20,
    paddingHorizontal: 14,
    borderRadius: 14,
    backgroundColor: "rgba(15, 23, 42, 0.92)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    overflow: "hidden",
    width: 200,
    height: 250
  },
  mapRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "center",
    flexWrap: "nowrap",
    width: "100%",
    gap: 0,
    flex: 1,
  },
  mapColumn: {
    alignItems: "center",
    flexShrink: 0,
    justifyContent: "center",
    transform: [{ translateY: 35 }],
    zIndex: 2,
  },
  hitter: {
    height: 500,
    opacity: 0.5,
  },
  hitterLeft: {
    marginRight: -140,
    zIndex: 1,
  },
  hitterRight: {
    marginLeft: -140,
    zIndex: 1,
  },
  mapFrame: {
    borderRadius: 18,
    overflow: "hidden",
  },
  mapSurface: {
    flex: 1,
    position: "relative",
  },
  outsideLayer: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
  },
  outsidePiece: {
    position: "absolute",
  },
  strikeZone: {
    position: "absolute",
    borderWidth: 1,
    borderColor: "rgba(226, 232, 240, 0.5)",
    backgroundColor: "rgba(15, 23, 42, 0.7)",
  },
  zoneGrid: {
    flex: 1,
  },
  zoneRow: {
    flex: 1,
    flexDirection: "row",
  },
  zoneCell: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    position: "relative",
  },
  zoneValue: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
    textShadowColor: "rgba(0, 0, 0, 0.35)",
    textShadowOffset: { width: 0, height: 1 },
    textShadowRadius: 3,
  },
  gridLineVertical: {
    position: "absolute",
    top: 0,
    bottom: 0,
    width: GRID_LINE,
    backgroundColor: "rgba(226, 232, 240, 0.35)",
  },
  gridLineHorizontal: {
    position: "absolute",
    left: 0,
    right: 0,
    height: GRID_LINE,
    backgroundColor: "rgba(226, 232, 240, 0.35)",
  },
  outsideLabels: {
    position: "absolute",
    top: 0,
    left: 0,
    right: 0,
    bottom: 0,
  },
  outsideValue: {
    position: "absolute",
    paddingHorizontal: 6,
    paddingVertical: 2,
    borderRadius: 8,
    borderWidth: 1,
    borderColor: "rgba(248, 113, 113, 0.35)",
  },
  zoneSelectionRing: {
    position: "absolute",
    top: 2,
    left: 2,
    right: 2,
    bottom: 2,
    borderRadius: 3,
    borderWidth: 2,
    borderColor: "#fbbf24",
  },
  outsideSelectionRing: {
    position: "absolute",
    top: -3,
    left: -3,
    right: -3,
    bottom: -3,
    borderRadius: 10,
    borderWidth: 2,
    borderColor: "#fbbf24",
  },
  outsideValueTop: {
    left: "50%",
    transform: [{ translateX: -14 }],
  },
  outsideValueRight: {
    top: "50%",
    transform: [{ translateY: -10 }],
  },
  outsideValueBottom: {
    left: "50%",
    transform: [{ translateX: -14 }],
  },
  outsideValueLeft: {
    top: "50%",
    transform: [{ translateY: -10 }],
  },
  outsideValueText: {
    color: theme.colors.text,
    fontSize: 11,
    fontWeight: "700",
  },
  plateWrap: {
    marginTop: 2,
    alignItems: "center"
  },
  plateTop: {
    backgroundColor: "rgba(226, 232, 240, 0.85)",
    borderWidth: 1,
    borderColor: "rgba(226, 232, 240, 0.55)",
    borderTopLeftRadius: 2,
    borderTopRightRadius: 2,
  },
  platePoint: {
    width: 0,
    height: 0,
    borderLeftColor: "transparent",
    borderRightColor: "transparent",
    borderBottomColor: "rgba(226, 232, 240, 0.85)",
  },
});
