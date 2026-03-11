import React, { useMemo } from 'react';
import { View, Text, StyleSheet, Dimensions, ActivityIndicator } from 'react-native';
import { LineChart } from 'react-native-gifted-charts';
import { theme } from '../../theme/colors';

type CompletedOrder = {
  card_id: string;
  date: string;
  price: number;
  is_buy: boolean | null;
};

type Props = {
  data: CompletedOrder[];
  loading: boolean;
};

const SCREEN_WIDTH = Dimensions.get('window').width;

export const MarketSpreadChart = ({ data, loading }: Props) => {
  // Process raw completed orders into paired buy/sell line data
  const { sellLineData, buyLineData } = useMemo(() => {
    if (!data || data.length === 0) {
      return { sellLineData: [], buyLineData: [] };
    }

    // Data arrives sorted oldest→newest from API.
    // Walk through each order and forward-fill the last known buy/sell price
    // so both lines have a value at every point.
    let lastBuy = 0;
    let lastSell = 0;

    // First pass: find initial prices so lines don't start at 0
    for (const order of data) {
      if (order.is_buy === true && lastBuy === 0) lastBuy = order.price;
      if (order.is_buy === false && lastSell === 0) lastSell = order.price;
      if (lastBuy !== 0 && lastSell !== 0) break;
    }

    const labelInterval = Math.max(1, Math.floor(data.length / 5));

    const sell: any[] = [];
    const buy: any[] = [];

    data.forEach((order, index) => {
      if (order.is_buy === true) lastBuy = order.price;
      else if (order.is_buy === false) lastSell = order.price;

      const safeDate = order.date ? order.date.replace(' ', 'T') : new Date().toISOString();
      const dateLabel = new Date(safeDate).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });

      sell.push({
        value: lastSell,
        label: index % labelInterval === 0 ? dateLabel : '',
        labelTextStyle: { color: theme.colors.muted, width: 60, fontSize: 10 },
      });

      buy.push({
        value: lastBuy,
      });
    });

    return { sellLineData: sell, buyLineData: buy };
  }, [data]);

  const { yMax, yMin, stepValue, noOfSections } = useMemo(() => {
    if (sellLineData.length === 0) return { yMax: 100, yMin: 0, stepValue: 20, noOfSections: 5 };

    const allValues = [
      ...sellLineData.map((d: any) => d.value),
      ...buyLineData.map((d: any) => d.value),
    ].filter((v: number) => v > 0);

    if (allValues.length === 0) return { yMax: 100, yMin: 0, stepValue: 20, noOfSections: 5 };

    const rawMin = Math.min(...allValues);
    const rawMax = Math.max(...allValues);
    const range = rawMax - rawMin || 1;
    const padding = range * 0.15;
    const niceMin = Math.max(0, Math.floor((rawMin - padding) / 100) * 100);
    const niceMax = Math.ceil((rawMax + padding) / 100) * 100;
    const niceRange = niceMax - niceMin;

    const sections = 5;
    const step = Math.ceil(niceRange / sections / 50) * 50;

    return { yMax: niceMin + step * sections, yMin: niceMin, stepValue: step, noOfSections: sections };
  }, [buyLineData, sellLineData]);

  if (loading) {
    return (
      <View style={styles.loadingContainer}>
        <ActivityIndicator color={theme.colors.primary} />
        <Text style={styles.loadingText}>Loading Market Trends...</Text>
      </View>
    );
  }

  if (!data || !Array.isArray(data) || data.length === 0) {
    return (
      <View style={styles.loadingContainer}>
        <Text style={styles.loadingText}>No market history available.</Text>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <View style={styles.headerRow}>
        <Text style={styles.title}>Price History </Text>
        <View style={styles.legendContainer}>
          <View style={[styles.dot, { backgroundColor: '#4ade80' }]} />
          <Text style={styles.legendText}>Sell</Text>
          <View style={[styles.dot, { backgroundColor: '#f87171', marginLeft: 8 }]} />
          <Text style={styles.legendText}>Buy</Text>
        </View>
      </View>

      <View style={styles.chartWrapper}>
        <LineChart
          data={sellLineData}
          data2={buyLineData}
          height={220}
          width={SCREEN_WIDTH - 80}
          
          // Layout
          spacing={6}
          initialSpacing={10}
          scrollToEnd={true}
          
          maxValue={yMax - yMin}
          stepValue={stepValue}
          noOfSections={noOfSections}
          yAxisOffset={yMin}
          
          // Visuals
          yAxisLabelWidth={50}
          yAxisTextStyle={{ color: theme.colors.muted, fontSize: 10 }}
          xAxisLabelTextStyle={{ color: theme.colors.muted, fontSize: 10 }}
          
          formatYLabel={(label) => {
            const num = parseInt(label);
            if (isNaN(num)) return '';
            if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
            if (num >= 1000) {
              const k = num / 1000;
              return (k % 1 === 0 ? k.toFixed(0) : k.toFixed(1)) + 'k';
            }
            return num.toString();
          }}

          color="#4ade80"
          color2="#f87171"
          thickness={2}
          thickness2={2}
          hideDataPoints
          hideRules
          yAxisColor="transparent"
          xAxisColor="rgba(255,255,255,0.1)"
          
          areaChart
          startFillColor="#4ade80"
          startOpacity={0.1}
          endFillColor="#f87171"
          endOpacity={0.1}
          
          pointerConfig={{
            pointerStripHeight: 160,
            pointerStripColor: 'rgba(255,255,255,0.2)',
            pointerStripWidth: 2,
            pointerColor: 'white',
            radius: 6,
            pointerLabelWidth: 100,
            pointerLabelHeight: 90,
            activatePointersOnLongPress: true,
            autoAdjustPointerLabelPosition: false,
            pointerLabelComponent: (items: any) => {
              const sell = items[0]?.value;
              const buy = items[1]?.value;
              return (
                <View style={styles.tooltip}>
                  <Text style={styles.tooltipLabel}>Spread</Text>
                  <Text style={[styles.tooltipValue, { fontSize: 18 }]}>
                    {sell !== undefined && buy !== undefined ? (sell - buy).toLocaleString() : '-'}
                  </Text>
                  <View style={styles.divider} />
                  <Text style={[styles.tooltipSub, { color: '#4ade80' }]}>Sell Price: {sell?.toLocaleString()}</Text>
                  <Text style={[styles.tooltipSub, { color: '#f87171' }]}>Buy Price: {buy?.toLocaleString()}</Text>
                </View>
              );
            },
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
    height: 200,
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
  tooltip: { backgroundColor: '#1e293b', padding: 10, borderRadius: 8, borderWidth: 1, borderColor: 'rgba(255,255,255,0.1)' },
  tooltipLabel: { color: theme.colors.muted, fontSize: 10, marginBottom: 2 },
  tooltipValue: { color: 'white', fontSize: 16, fontWeight: 'bold' },
  tooltipSub: { fontSize: 10, fontWeight: '600', marginTop: 2 },
  divider: { height: 1, backgroundColor: 'rgba(255,255,255,0.1)', marginVertical: 4 },
});
