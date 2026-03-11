import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  View,
  Text,
  StyleSheet,
  FlatList,
  Image,
  ActivityIndicator,
  RefreshControl,
  TouchableOpacity,
} from 'react-native';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { useRouter } from 'expo-router';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { theme } from '../../theme/colors';
import { ApiError, apiGetAuth } from '../../lib/api';

const STUB_ICON = require('../../../assets/images/stub.png');

// ── Types ──────────────────────────────────────────────────────────────────────

type HoldingCard = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  rarity: string;
  predicted_ovr: number | null;
};

type Holding = {
  card_id: string;
  quantity: number;
  avg_price: number | null;
  user_predicted_ovr: number | null;
  card: HoldingCard;
};

type PortfolioData = {
  id: number;
  name: string;
  is_public: boolean;
  holdings: Holding[];
};

// ── Quicksell lookup ───────────────────────────────────────────────────────────

const getQuicksellValue = (ovr: number): number => {
  if (ovr >= 92) return 10000;
  const table: Record<number, number> = {
    91: 9000, 90: 8000, 89: 7000, 88: 5500, 87: 4500,
    86: 3750, 85: 3000, 84: 1500, 83: 1200, 82: 900,
    81: 600, 80: 400, 79: 150, 78: 125, 77: 100,
    76: 75, 75: 50,
  };
  if (table[ovr] !== undefined) return table[ovr];
  if (ovr >= 65) return 25;
  return 5;
};

const formatStubs = (n: number): string => {
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
};

// ── Component ──────────────────────────────────────────────────────────────────

type Props = {
  userId: string;
  username: string;
};

export default function PublicPortfolioScreen({ userId, username }: Props) {
  const router = useRouter();

  const [portfolio, setPortfolio] = useState<PortfolioData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchPortfolio = useCallback(async () => {
    try {
      setError(null);
      const data = await apiGetAuth<PortfolioData>(`/users/${userId}/portfolio`);
      setPortfolio(data);
    } catch (err: any) {
      if (err instanceof ApiError && err.status === 403) {
        setError('private');
      } else {
        setError(err?.message ?? 'Failed to load portfolio');
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [userId]);

  useEffect(() => {
    void fetchPortfolio();
  }, [fetchPortfolio]);

  const handleRefresh = useCallback(() => {
    setRefreshing(true);
    void fetchPortfolio();
  }, [fetchPortfolio]);

  const holdings = useMemo(() => portfolio?.holdings ?? [], [portfolio?.holdings]);

  const totals = useMemo(() => {
    let totalInvested = 0;
    let yourValue = 0;

    for (const h of holdings) {
      const price = h.avg_price ?? 0;
      totalInvested += h.quantity * price;
      const yourOvr = h.user_predicted_ovr ?? h.card.ovr;
      yourValue += h.quantity * getQuicksellValue(yourOvr);
    }

    return {
      totalInvested,
      yourValue,
      yourPL: yourValue - totalInvested,
    };
  }, [holdings]);

  const plColor = (val: number) => (val >= 0 ? '#4ade80' : '#f87171');

  const renderSummaryCard = (label: string, value: number, isProfit?: boolean) => (
    <View style={styles.summaryCard}>
      <Text style={styles.summaryLabel}>{label}</Text>
      <View style={styles.summaryValueRow}>
        <Image source={STUB_ICON} style={styles.stubIcon} />
        <Text
          style={[
            styles.summaryValue,
            isProfit !== undefined && { color: plColor(value) },
          ]}
        >
          {isProfit !== undefined && value !== 0 ? (value > 0 ? '+' : '') : ''}
          {formatStubs(value)}
        </Text>
      </View>
    </View>
  );

  const renderInvestmentItem = ({ item }: { item: Holding }) => {
    const price = item.avg_price ?? 0;
    const totalInvested = item.quantity * price;
    const yourOvr = item.user_predicted_ovr ?? item.card.ovr;
    const yourValue = item.quantity * getQuicksellValue(yourOvr);
    const yourPL = yourValue - totalInvested;

    return (
      <TouchableOpacity
        style={styles.investmentCard}
        activeOpacity={0.7}
        onPress={() =>
          router.push({
            pathname: '/predictions/[id]',
            params: {
              id: item.card.id,
              cardData: JSON.stringify(item.card),
            },
          })
        }
      >
        <View style={styles.investmentHeader}>
          <Image
            source={{ uri: item.card.baked_img }}
            style={styles.investmentImg}
            resizeMode="contain"
          />
          <View style={styles.investmentInfo}>
            <Text style={styles.investmentName} numberOfLines={1}>
              {item.card.name}
            </Text>
            <Text style={styles.investmentMeta}>
              {item.card.team_short_name} · {item.card.display_position} ·{' '}
              {item.card.ovr} OVR
            </Text>
          </View>
        </View>

        <View style={styles.investmentDivider} />

        <View style={styles.investmentStats}>
          <View style={styles.investmentStatCol}>
            <Text style={styles.statLabel}>Qty</Text>
            <Text style={styles.statValue}>{item.quantity}</Text>
          </View>
          <View style={styles.investmentStatCol}>
            <Text style={styles.statLabel}>Avg Buy</Text>
            <View style={styles.statValueRow}>
              <Image source={STUB_ICON} style={styles.stubIconSmall} />
              <Text style={styles.statValue}>{formatStubs(price)}</Text>
            </View>
          </View>
          <View style={styles.investmentStatCol}>
            <Text style={styles.statLabel}>Invested</Text>
            <View style={styles.statValueRow}>
              <Image source={STUB_ICON} style={styles.stubIconSmall} />
              <Text style={styles.statValue}>{formatStubs(totalInvested)}</Text>
            </View>
          </View>
        </View>

        <View style={styles.investmentPLRow}>
          <View style={styles.plBlock}>
            <Text style={styles.plBlockLabel}>
              Projection - ({yourOvr} OVR)
            </Text>
            <View style={styles.plValueRow}>
              <Image source={STUB_ICON} style={styles.stubIconSmall} />
              <Text style={[styles.plBlockValue, { color: plColor(yourPL) }]}>
                {yourPL >= 0 ? '+' : ''}
                {formatStubs(yourPL)}
              </Text>
            </View>
          </View>
        </View>
      </TouchableOpacity>
    );
  };

  // ── Main render ────────────────────────────────────────────────────────────

  if (loading) {
    return (
      <View style={styles.container}>
        <View style={styles.backgroundLayer}>
          <FloatingBackground />
        </View>
        <View style={styles.loadingContainer}>
          <ActivityIndicator size="large" color="#fbbf24" />
        </View>
      </View>
    );
  }

  if (error === 'private') {
    return (
      <View style={styles.container}>
        <View style={styles.backgroundLayer}>
          <FloatingBackground />
        </View>
        <View style={styles.loadingContainer}>
          <Ionicons name="lock-closed" size={48} color="rgba(255,255,255,0.15)" />
          <Text style={styles.emptyTitle}>{`${username}'s portfolio is private`}</Text>
        </View>
      </View>
    );
  }

  if (error) {
    return (
      <View style={styles.container}>
        <View style={styles.backgroundLayer}>
          <FloatingBackground />
        </View>
        <View style={styles.loadingContainer}>
          <Text style={styles.emptyTitle}>{error}</Text>
        </View>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <FlatList
        data={holdings}
        keyExtractor={(item) => item.card_id}
        renderItem={renderInvestmentItem}
        contentContainerStyle={styles.listContent}
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={handleRefresh}
            tintColor="#fff"
          />
        }
        ListHeaderComponent={
          <>
            {/* ── Header ─────────────────────────────────────────────── */}
            <View style={styles.headerRow}>
              <TouchableOpacity onPress={() => router.back()}>
                <Ionicons name="arrow-back" size={24} color="white" />
              </TouchableOpacity>
              <Text style={styles.headerTitle} numberOfLines={1}>
                {`${username}'s Investments`}
              </Text>
              <View style={{ width: 24 }} />
            </View>

            {/* ── Summary ────────────────────────────────────────────── */}
            <View style={styles.summaryRow}>
              {renderSummaryCard('Total Invested', totals.totalInvested)}
              {renderSummaryCard('Value', totals.yourValue)}
              {renderSummaryCard('P/L', totals.yourPL, true)}
            </View>

            {/* ── Cards header ───────────────────────────────────────── */}
            <View style={styles.investmentsHeader}>
              <Text style={styles.sectionTitle}>Investments</Text>
              <View style={styles.countBadge}>
                <Text style={styles.countBadgeText}>{holdings.length}</Text>
              </View>
            </View>
          </>
        }
        ListEmptyComponent={
          <View style={styles.emptyContainer}>
            <FontAwesome5
              name="briefcase"
              size={48}
              color="rgba(255,255,255,0.15)"
            />
            <Text style={styles.emptyTitle}>No investments yet</Text>
          </View>
        }
      />
    </View>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: theme.colors.background },
  backgroundLayer: { ...StyleSheet.absoluteFillObject, zIndex: -1 },
  listContent: { paddingHorizontal: 16, paddingTop: 20, paddingBottom: 40 },
  loadingContainer: { flex: 1, justifyContent: 'center', alignItems: 'center', gap: 16 },

  // Header
  headerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 20,
    gap: 12,
  },
  headerTitle: { flex: 1, fontSize: 22, fontWeight: '800', color: 'white' },

  // Summary
  summaryRow: { flexDirection: 'row', gap: 10, marginBottom: 10 },
  summaryCard: {
    flex: 1,
    backgroundColor: 'rgba(15, 23, 42, 0.6)',
    borderRadius: 14,
    padding: 14,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.06)',
  },
  summaryLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    marginBottom: 6,
  },
  summaryValueRow: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  summaryValue: { color: 'white', fontSize: 18, fontWeight: '800' },

  // Stub icon
  stubIcon: { width: 16, height: 16, resizeMode: 'contain', marginRight: 4 },
  stubIconSmall: { width: 12, height: 12, resizeMode: 'contain', marginRight: 2 },

  // Section title
  sectionTitle: {
    color: 'white',
    fontSize: 18,
    fontWeight: '700',
  },

  // Investments header
  investmentsHeader: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 10,
    marginTop: 14,
    marginBottom: 12,
  },
  countBadge: {
    backgroundColor: 'rgba(59, 130, 246, 0.15)',
    paddingHorizontal: 10,
    paddingVertical: 2,
    borderRadius: 12,
  },
  countBadgeText: { color: '#3b82f6', fontSize: 12, fontWeight: '800' },

  // Investment card
  investmentCard: {
    backgroundColor: 'rgba(15, 23, 42, 0.6)',
    borderRadius: 16,
    padding: 14,
    marginBottom: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.06)',
  },
  investmentHeader: { flexDirection: 'row', alignItems: 'center' },
  investmentImg: { width: 44, height: 60, marginRight: 12, borderRadius: 3 },
  investmentInfo: { flex: 1 },
  investmentName: { color: 'white', fontSize: 15, fontWeight: '700' },
  investmentMeta: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: '500',
    marginTop: 2,
  },
  investmentDivider: {
    height: 1,
    backgroundColor: 'rgba(255,255,255,0.06)',
    marginVertical: 10,
  },

  // Stats row
  investmentStats: {
    flexDirection: 'row',
    justifyContent: 'space-around',
    marginBottom: 10,
  },
  investmentStatCol: { alignItems: 'center' },
  statLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    textTransform: 'uppercase',
    letterSpacing: 0.3,
    marginBottom: 2,
  },
  statValue: { color: 'white', fontSize: 14, fontWeight: '700' },
  statValueRow: { flexDirection: 'row', alignItems: 'center' },

  // P/L row
  investmentPLRow: {
    flexDirection: 'row',
    backgroundColor: 'rgba(255,255,255,0.03)',
    borderRadius: 10,
    overflow: 'hidden',
  },
  plBlock: { flex: 1, alignItems: 'center', paddingVertical: 10 },
  plBlockLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    marginBottom: 3,
  },
  plValueRow: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  plBlockValue: { fontSize: 15, fontWeight: '800' },

  // Empty state
  emptyContainer: { alignItems: 'center', paddingTop: 50, paddingBottom: 30 },
  emptyTitle: { color: 'white', fontSize: 18, fontWeight: '700', marginTop: 16 },
});
