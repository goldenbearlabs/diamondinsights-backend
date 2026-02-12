import React from 'react';
import { View, Text, StyleSheet } from 'react-native';

interface PredictionAttributeBarProps {
  label: string;
  predictedValue: number;
  delta: number;
}

const PredictionAttributeBar: React.FC<PredictionAttributeBarProps> = ({ label, predictedValue, delta }) => {
  const roundedDelta = Math.round(delta);
  const barColor = roundedDelta > 0 ? '#4ade80' : roundedDelta < 0 ? '#f87171' : '#6b7280';
  const deltaText = roundedDelta > 0 ? `+${roundedDelta}` : roundedDelta < 0 ? `${roundedDelta}` : '0';
  const maxValue = 125;
  const fillWidth = Math.min((predictedValue / maxValue) * 100, 100);

  return (
    <View style={styles.container}>
      <Text style={styles.label}>{label}</Text>
      <View style={styles.barContainer}>
        <View style={styles.barTrack}>
          <View style={[styles.barFill, { width: `${fillWidth}%`, backgroundColor: barColor }]} />
        </View>
        <View style={styles.valueRow}>
          <Text style={[styles.value, { color: barColor }]}>{predictedValue}</Text>
          <View style={[styles.deltaBadge, { backgroundColor: barColor + '22', borderColor: barColor + '44' }]}>
            <Text style={[styles.deltaText, { color: barColor }]}>{deltaText}</Text>
          </View>
        </View>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 10,
  },
  label: {
    color: 'rgba(255,255,255,0.7)',
    fontSize: 13,
    width: 95,
    fontWeight: '500',
  },
  barContainer: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  barTrack: {
    flex: 1,
    height: 8,
    backgroundColor: 'rgba(255,255,255,0.08)',
    borderRadius: 4,
    overflow: 'hidden',
  },
  barFill: {
    height: '100%',
    borderRadius: 4,
  },
  valueRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    minWidth: 70,
    justifyContent: 'flex-end',
  },
  value: {
    fontSize: 14,
    fontWeight: '700',
    width: 28,
    textAlign: 'right',
  },
  deltaBadge: {
    paddingHorizontal: 5,
    paddingVertical: 1,
    borderRadius: 6,
    borderWidth: 1,
  },
  deltaText: {
    fontSize: 11,
    fontWeight: '700',
  },
});

export default PredictionAttributeBar;