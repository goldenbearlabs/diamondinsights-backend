import React from 'react';
import { View, Text, StyleSheet, ActivityIndicator } from 'react-native';
import { BarChart } from 'react-native-gifted-charts';
import { theme } from '../../theme/colors';

type Props = {
  buyVolume: number | null;
  sellVolume: number | null;
  loading: boolean;
};

const abbreviate = (n: number): string => {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'k';
  return n.toString();
};

export const MarketVolumeChart = ({ buyVolume, sellVolume, loading }: Props) => {
  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator color={theme.colors.primary} />
        <Text style={styles.loadingText}>Loading Volume...</Text>
      </View>
    );
  }

  if (buyVolume === null && sellVolume === null) {
    return (
      <View style={styles.loadingContainer}>
        <Text style={styles.loadingText}>No volume data available.</Text>
      </View>
    );
  }

  const buy = buyVolume ?? 0;
  const sell = sellVolume ?? 0;

  const barData = [
    {
      value: sell,
      label: 'Sell',
      frontColor: '#4ade80',
      topLabelComponent: () => (
        <Text style={[styles.barLabel, { color: '#4ade80' }]}>{abbreviate(sell)}</Text>
      ),
    },
    {
      value: buy,
      label: 'Buy',
      frontColor: '#f87171',
      topLabelComponent: () => (
        <Text style={[styles.barLabel, { color: '#f87171' }]}>{abbreviate(buy)}</Text>
      ),
    },
  ];

  return (
    <View style={styles.container}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>Recent Buy/Sell Volume</Text>
        <View style={styles.legendContainer}>
          <View style={[styles.dot, { backgroundColor: '#4ade80' }]} />
          <Text style={styles.legendText}>Sell</Text>
          <View style={[styles.dot, { backgroundColor: '#f87171', marginLeft: 8 }]} />
          <Text style={styles.legendText}>Buy</Text>
        </View>
      </View>

      <View style={styles.chartWrapper}>
        <BarChart
          data={barData}
          height={160}
          barWidth={60}
          spacing={40}
          initialSpacing={30}
          noOfSections={4}
          barBorderRadius={6}
          yAxisThickness={0}
          xAxisColor="rgba(255,255,255,0.1)"
          xAxisLabelTextStyle={{ color: theme.colors.muted, fontSize: 12, fontWeight: '600' }}
          yAxisTextStyle={{ color: theme.colors.muted, fontSize: 10 }}
          hideRules
          formatYLabel={(label) => {
            const num = parseInt(label);
            if (isNaN(num)) return '';
            return abbreviate(num);
          }}
        />
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    backgroundColor: 'rgba(2, 6, 23, 0.5)',
    borderRadius: 16,
    padding: 16,
    marginTop: 16,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.05)',
  },
  loadingContainer: {
    height: 160,
    justifyContent: 'center',
    alignItems: 'center',
    backgroundColor: 'rgba(2, 6, 23, 0.5)',
    borderRadius: 16,
    marginTop: 16,
  },
  loadingText: { color: theme.colors.muted, marginTop: 12, fontSize: 14 },
  headerRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 },
  title: { color: 'white', fontSize: 16, fontWeight: 'bold' },
  legendContainer: { flexDirection: 'row', alignItems: 'center' },
  legendText: { color: theme.colors.muted, fontSize: 12, marginLeft: 4, fontWeight: '600' },
  dot: { width: 8, height: 8, borderRadius: 4 },
  chartWrapper: { marginLeft: -10 },
  barLabel: { fontSize: 12, fontWeight: 'bold', marginBottom: 4 },
});