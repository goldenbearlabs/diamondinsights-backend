import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { theme } from '../theme/colors';

const DEFAULT_BAR_COLOR = '#3b82f6';

type AttributeBarProps = {
  label: string;
  value: number;
  maxValue?: number; 
  barColor?: string;
};

export const AttributeBar = ({ label, value, maxValue = 125, barColor }: AttributeBarProps) => {
  const widthPercent = Math.min((value / maxValue) * 100, 100);

  return (
    <View style={styles.container}>
      <View style={styles.headerRow}>
        <Text style={styles.label}>{label}</Text>
        <Text style={styles.value}>{value}</Text>
      </View>
      
      <View style={styles.track}>
        <View style={[styles.fill, { width: `${widthPercent}%`, backgroundColor: barColor || DEFAULT_BAR_COLOR }]} />
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    marginBottom: 12,
  },
  headerRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 6,
  },
  label: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: '600',
    textTransform: 'uppercase',
  },
  value: {
    color: 'white',
    fontSize: 14,
    fontWeight: 'bold',
  },
  track: {
    height: 8,
    backgroundColor: 'rgba(255, 255, 255, 0.1)',
    borderRadius: 4,
    overflow: 'hidden',
  },
  fill: {
    height: '100%',
    backgroundColor: DEFAULT_BAR_COLOR,
    borderRadius: 4,
  },
});