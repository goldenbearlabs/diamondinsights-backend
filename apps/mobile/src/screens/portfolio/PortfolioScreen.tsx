import React, { useState, useEffect, useCallback } from 'react';
import {
  View,
  Text,
  StyleSheet,
  TextInput,
  TouchableOpacity,
  FlatList,
  Image,
  ActivityIndicator,
  KeyboardAvoidingView,
  Platform,
  RefreshControl,
  Alert,
} from 'react-native';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { useRouter } from 'expo-router';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { theme } from '../../theme/colors';
import { apiGet, apiPostAuth, apiDeleteAuth, apiGetAuth, apiPatchAuth } from '../../lib/api';

const STUB_ICON = require('../../../assets/images/stub.png');

// ── Types ──────────────────────────────────────────────────────────────────────

type CardSearchResult = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  rarity: string;
  predicted_ovr: number | null;
};

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

export default function PortfolioScreen() {
  const router = useRouter();

  // Portfolio data
  const [portfolio, setPortfolio] = useState<PortfolioData | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  // Form state
  const [searchText, setSearchText] = useState('');
  const [searchResults, setSearchResults] = useState<CardSearchResult[]>([]);
  const [selectedCard, setSelectedCard] = useState<CardSearchResult | null>(null);
  const [quantity, setQuantity] = useState('');
  const [avgBuyPrice, setAvgBuyPrice] = useState('');
  const [projectedOvr, setProjectedOvr] = useState('');
  const [searching, setSearching] = useState(false);
  const [adding, setAdding] = useState(false);

  // ── Fetch portfolio ────────────────────────────────────────────────────────

  const fetchPortfolio = useCallback(async () => {
    try {
      const data = await apiGetAuth<PortfolioData>('/portfolios/me');
      setPortfolio(data);
    } catch (error) {
      console.error('Failed to fetch portfolio:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    fetchPortfolio();
  }, [fetchPortfolio]);

  const handleRefresh = () => {
    setRefreshing(true);
    fetchPortfolio();
  };

  // ── Search debounce ────────────────────────────────────────────────────────

  useEffect(() => {
    if (searchText.trim().length < 2 || selectedCard) {
      setSearchResults([]);
      return;
    }
    const timer = setTimeout(async () => {
      setSearching(true);
      try {
        const results = await apiGet<CardSearchResult[]>(
          `/cards?series=live&year=25&name=${encodeURIComponent(searchText)}&limit=8`
        );
        setSearchResults(results);
      } catch {
        setSearchResults([]);
      } finally {
        setSearching(false);
      }
    }, 400);
    return () => clearTimeout(timer);
  }, [searchText, selectedCard]);

  // ── Derived totals ─────────────────────────────────────────────────────────

  const holdings = portfolio?.holdings ?? [];

  const totals = holdings.reduce(
    (acc, h) => {
      const price = h.avg_price ?? 0;
      const totalInvested = h.quantity * price;
      const yourOvr = h.user_predicted_ovr ?? h.card.ovr;
      const yourValue = h.quantity * getQuicksellValue(yourOvr);
      const aiOvr = h.card.predicted_ovr ?? h.card.ovr;
      const aiValue = h.quantity * getQuicksellValue(aiOvr);
      return {
        totalInvested: acc.totalInvested + totalInvested,
        yourValue: acc.yourValue + yourValue,
        yourPL: acc.yourPL + (yourValue - totalInvested),
        aiValue: acc.aiValue + aiValue,
        aiPL: acc.aiPL + (aiValue - totalInvested),
      };
    },
    { totalInvested: 0, yourValue: 0, yourPL: 0, aiValue: 0, aiPL: 0 }
  );

  // ── Add investment ─────────────────────────────────────────────────────────

  const handleAdd = async () => {
    if (!selectedCard) return;
    const qty = parseInt(quantity, 10);
    const price = parseInt(avgBuyPrice, 10);
    const ovr = parseInt(projectedOvr, 10);
    if (!qty || !price || !ovr || qty <= 0 || price <= 0) return;

    setAdding(true);
    try {
      await apiPostAuth('/portfolios/me/holdings', {
        card_id: selectedCard.id,
        quantity: qty,
        avg_price: price,
        user_predicted_ovr: ovr,
      });
      // Reset form
      setSelectedCard(null);
      setSearchText('');
      setQuantity('');
      setAvgBuyPrice('');
      setProjectedOvr('');
      setSearchResults([]);
      // Refresh portfolio
      await fetchPortfolio();
    } catch (error) {
      console.error('Failed to add holding:', error);
      Alert.alert('Error', 'Failed to add investment. Please try again.');
    } finally {
      setAdding(false);
    }
  };

  // ── Remove investment ──────────────────────────────────────────────────────

  const handleRemove = (cardId: string, playerName: string) => {
    Alert.alert(
      'Remove Investment',
      `Remove ${playerName} from your portfolio?`,
      [
        { text: 'Cancel', style: 'cancel' },
        {
          text: 'Remove',
          style: 'destructive',
          onPress: async () => {
            try {
              await apiDeleteAuth(`/portfolios/me/holdings/${cardId}`);
              await fetchPortfolio();
            } catch (error) {
              console.error('Failed to remove holding:', error);
            }
          },
        },
      ]
    );
  };

  const togglePrivacy = async () => {
    if (!portfolio) return;

    try {
      const newIsPublic = !portfolio.is_public;
      
      // Optimistically update UI
      setPortfolio({ ...portfolio, is_public: newIsPublic });

      await apiPatchAuth('/portfolios/me', { is_public: newIsPublic });
    } catch (error) {
      console.error('Failed to toggle privacy:', error);
      // Revert on error
      await fetchPortfolio();
      Alert.alert('Error', 'Failed to update portfolio privacy');
    }
  };

  // ── Render helpers ─────────────────────────────────────────────────────────

  const plColor = (val: number) => (val >= 0 ? '#4ade80' : '#f87171');

  const renderSummaryCard = (label: string, value: number, isProfit?: boolean, isPro?: boolean) => (
    <View style={styles.summaryCard}>
      {isPro ? (
        <View style={styles.summaryLabelRow}>
          <View style={styles.proTagSmall}>
            <FontAwesome5 name="crown" size={8} color="#fbbf24" style={styles.proIcon} />
            <Text style={styles.proTagText}>PRO</Text>
          </View>
          <Text style={styles.summaryLabel}>{label}</Text>
        </View>
      ) : (
        <Text style={styles.summaryLabel}>{label}</Text>
      )}
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
    const aiOvr = item.card.predicted_ovr ?? item.card.ovr;
    const aiValue = item.quantity * getQuicksellValue(aiOvr);
    const aiPL = aiValue - totalInvested;

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
          <TouchableOpacity
            onPress={() => handleRemove(item.card_id, item.card.name)}
            hitSlop={{ top: 8, bottom: 8, left: 8, right: 8 }}
          >
            <Ionicons name="trash-outline" size={18} color={theme.colors.muted} />
          </TouchableOpacity>
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
            <Text style={styles.plBlockLabel}>Your Projection - ({yourOvr} OVR)</Text>
            <View style={styles.plValueRow}>
              <Image source={STUB_ICON} style={styles.stubIconSmall} />
              <Text style={[styles.plBlockValue, { color: plColor(yourPL) }]}>
                {yourPL >= 0 ? '+' : ''}
                {formatStubs(yourPL)}
              </Text>
            </View>
          </View>
          <View style={styles.plDivider} />
          <View style={styles.plBlock}>
            <View style={styles.plLabelRow}>
              <View style={styles.proTagSmall}>
                <FontAwesome5 name="crown" size={8} color="#fbbf24" style={styles.proIcon} />
                <Text style={styles.proTagText}>PRO</Text>
              </View>
              <Text style={styles.plBlockLabel}>({aiOvr} OVR)</Text>
            </View>
            <View style={styles.plValueRow}>
              <Image source={STUB_ICON} style={styles.stubIconSmall} />
              <Text style={[styles.plBlockValue, { color: plColor(aiPL) }]}>
                {aiPL >= 0 ? '+' : ''}
                {formatStubs(aiPL)}
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

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <KeyboardAvoidingView
        style={{ flex: 1 }}
        behavior={Platform.OS === 'ios' ? 'padding' : undefined}
      >
        <FlatList
          data={holdings}
          keyExtractor={(item) => item.card_id}
          renderItem={renderInvestmentItem}
          contentContainerStyle={styles.listContent}
          showsVerticalScrollIndicator={false}
          refreshControl={
            <RefreshControl
              refreshing={refreshing}
              onRefresh={handleRefresh}
              tintColor="#fff"
            />
          }
          ListHeaderComponent={
            <>
              {/* ── Section 1: Summary Dashboard ─────────────────────── */}
              <View style={styles.headerRow}>
                <Text style={styles.headerTitle}>My Investments</Text>
                <TouchableOpacity
                  style={[
                    styles.publicBadge,
                    !portfolio?.is_public && styles.publicBadgePrivate,
                  ]}
                  onPress={togglePrivacy}
                  activeOpacity={0.7}
                >
                  <Ionicons
                    name={portfolio?.is_public ? 'globe-outline' : 'lock-closed'}
                    size={12}
                    color={portfolio?.is_public ? '#3b82f6' : '#9ca3af'}
                  />
                  <Text
                    style={[
                      styles.publicBadgeText,
                      !portfolio?.is_public && styles.publicBadgeTextPrivate,
                    ]}
                  >
                    {portfolio?.is_public ? 'Public' : 'Private'}
                  </Text>
                </TouchableOpacity>
              </View>

              <View style={styles.summaryRow}>
                {renderSummaryCard('Total Invested', totals.totalInvested)}
                {renderSummaryCard('Value', totals.aiValue, undefined, true)}
                {renderSummaryCard('P/L', totals.aiPL, true, true)}
              </View>
              <View style={styles.summaryRow}>
                {renderSummaryCard('Your Value', totals.yourValue)}
                {renderSummaryCard('Your P/L', totals.yourPL, true)}
              </View>

              {/* ── Section 2: Add New Investment ─────────────────────── */}
              <Text style={styles.sectionTitle}>Add New Investment</Text>
              <View style={styles.formCard}>
                {/* Player Search */}
                <View style={styles.searchContainer}>
                  <Ionicons
                    name="search"
                    size={16}
                    color={theme.colors.muted}
                    style={{ marginRight: 8 }}
                  />
                  {selectedCard ? (
                    <View style={styles.selectedChip}>
                      <Image
                        source={{ uri: selectedCard.baked_img }}
                        style={styles.chipImg}
                        resizeMode="contain"
                      />
                      <Text style={styles.chipText}>{selectedCard.name}</Text>
                      <TouchableOpacity
                        onPress={() => {
                          setSelectedCard(null);
                          setSearchText('');
                        }}
                      >
                        <Ionicons
                          name="close-circle"
                          size={18}
                          color={theme.colors.muted}
                        />
                      </TouchableOpacity>
                    </View>
                  ) : (
                    <TextInput
                      style={styles.searchInput}
                      placeholder="Search player..."
                      placeholderTextColor={theme.colors.muted}
                      value={searchText}
                      onChangeText={setSearchText}
                      autoCapitalize="none"
                    />
                  )}
                  {searching && (
                    <ActivityIndicator size="small" color={theme.colors.primary} />
                  )}
                </View>

                {/* Search Results Dropdown */}
                {searchResults.length > 0 && !selectedCard && (
                  <View style={styles.dropdown}>
                    {searchResults.map((card) => (
                      <TouchableOpacity
                        key={card.id}
                        style={styles.dropdownItem}
                        onPress={() => {
                          setSelectedCard(card);
                          setSearchText(card.name);
                          setSearchResults([]);
                        }}
                      >
                        <Image
                          source={{ uri: card.baked_img }}
                          style={styles.dropdownImg}
                          resizeMode="contain"
                        />
                        <View style={{ flex: 1 }}>
                          <Text style={styles.dropdownName}>{card.name}</Text>
                          <Text style={styles.dropdownMeta}>
                            {card.team_short_name} · {card.display_position} ·{' '}
                            {card.ovr} OVR
                          </Text>
                        </View>
                        {card.predicted_ovr != null && (
                          <View
                            style={[
                              styles.predBadge,
                              {
                                borderColor:
                                  card.predicted_ovr > card.ovr
                                    ? '#4ade80'
                                    : card.predicted_ovr < card.ovr
                                    ? '#f87171'
                                    : 'rgba(255,255,255,0.1)',
                              },
                            ]}
                          >
                            <Text
                              style={[
                                styles.predBadgeText,
                                {
                                  color:
                                    card.predicted_ovr > card.ovr
                                      ? '#4ade80'
                                      : card.predicted_ovr < card.ovr
                                      ? '#f87171'
                                      : theme.colors.muted,
                                },
                              ]}
                            >
                              PRED {card.predicted_ovr}
                            </Text>
                          </View>
                        )}
                      </TouchableOpacity>
                    ))}
                  </View>
                )}

                {/* Number Inputs Row */}
                <View style={styles.inputRow}>
                  <View style={styles.inputGroup}>
                    <Text style={styles.inputLabel}>Quantity</Text>
                    <TextInput
                      style={styles.numberInput}
                      placeholder="0"
                      placeholderTextColor="rgba(255,255,255,0.2)"
                      value={quantity}
                      onChangeText={setQuantity}
                      keyboardType="number-pad"
                    />
                  </View>
                  <View style={styles.inputGroup}>
                    <Text style={styles.inputLabel}>Avg Buy Price</Text>
                    <TextInput
                      style={styles.numberInput}
                      placeholder="0"
                      placeholderTextColor="rgba(255,255,255,0.2)"
                      value={avgBuyPrice}
                      onChangeText={setAvgBuyPrice}
                      keyboardType="number-pad"
                    />
                  </View>
                  <View style={styles.inputGroup}>
                    <Text style={styles.inputLabel}>Projected OVR</Text>
                    <TextInput
                      style={styles.numberInput}
                      placeholder="0"
                      placeholderTextColor="rgba(255,255,255,0.2)"
                      value={projectedOvr}
                      onChangeText={setProjectedOvr}
                      keyboardType="number-pad"
                    />
                  </View>
                </View>

                {/* Add Button */}
                <TouchableOpacity
                  style={[
                    styles.addButton,
                    (!selectedCard || !quantity || !avgBuyPrice || !projectedOvr || adding) &&
                      styles.addButtonDisabled,
                  ]}
                  onPress={handleAdd}
                  disabled={
                    !selectedCard || !quantity || !avgBuyPrice || !projectedOvr || adding
                  }
                  activeOpacity={0.7}
                >
                  {adding ? (
                    <ActivityIndicator size="small" color="white" />
                  ) : (
                    <>
                      <Ionicons name="add-circle" size={20} color="white" />
                      <Text style={styles.addButtonText}>Add Investment</Text>
                    </>
                  )}
                </TouchableOpacity>
              </View>

              {/* ── Section 3: Header ─────────────────────────────────── */}
              <View style={styles.investmentsHeader}>
                <Text style={styles.sectionTitle}>Your Investments</Text>
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
              <Text style={styles.emptySubtext}>
                Add your first investment above
              </Text>
            </View>
          }
        />
      </KeyboardAvoidingView>
    </View>
  );
}

// ── Styles ────────────────────────────────────────────────────────────────────

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: theme.colors.background },
  backgroundLayer: { ...StyleSheet.absoluteFillObject, zIndex: -1 },
  listContent: { paddingHorizontal: 16, paddingTop: 20, paddingBottom: 40 },
  loadingContainer: { flex: 1, justifyContent: 'center', alignItems: 'center' },

  // Header
  headerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: 20,
  },
  headerTitle: { fontSize: 28, fontWeight: '800', color: 'white' },
  publicBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
    backgroundColor: 'rgba(59, 130, 246, 0.1)',
    paddingHorizontal: 10,
    paddingVertical: 5,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: 'rgba(59, 130, 246, 0.25)',
  },
  publicBadgePrivate: {
    backgroundColor: 'rgba(156, 163, 175, 0.1)',
    borderColor: 'rgba(156, 163, 175, 0.25)',
  },
  publicBadgeText: { color: '#3b82f6', fontSize: 11, fontWeight: '700' },
  publicBadgeTextPrivate: { color: '#9ca3af' },

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
  summaryLabelRow: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    gap: 4,
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

  // PRO badge (black pill with gold border)
  proTagSmall: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(0, 0, 0, 0.6)',
    paddingVertical: 3,
    paddingHorizontal: 6,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: '#fbbf24',
  },
  proIcon: {
    marginRight: 3,
  },
  proTagText: {
    color: '#fbbf24',
    fontWeight: '800',
    fontSize: 8,
    letterSpacing: 0.3,
  },

  // Section title
  sectionTitle: {
    color: 'white',
    fontSize: 18,
    fontWeight: '700',
    marginTop: 24,
    marginBottom: 12,
  },

  // Form card
  formCard: {
    backgroundColor: 'rgba(15, 23, 42, 0.6)',
    borderRadius: 16,
    padding: 16,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.06)',
  },
  searchContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(255,255,255,0.05)',
    borderRadius: 10,
    paddingHorizontal: 12,
    height: 56,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.08)',
  },
  searchInput: { flex: 1, color: 'white', fontSize: 14, fontWeight: '500' },
  selectedChip: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  chipImg: { width: 32, height: 44, borderRadius: 2 },
  chipText: { flex: 1, color: 'white', fontSize: 14, fontWeight: '600' },

  // Dropdown
  dropdown: {
    backgroundColor: 'rgba(15, 23, 42, 0.95)',
    borderRadius: 12,
    marginTop: 6,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.08)',
    overflow: 'hidden',
  },
  dropdownItem: {
    flexDirection: 'row',
    alignItems: 'center',
    paddingHorizontal: 12,
    paddingVertical: 10,
    borderBottomWidth: 1,
    borderBottomColor: 'rgba(255,255,255,0.04)',
  },
  dropdownImg: { width: 36, height: 50, marginRight: 10, borderRadius: 2 },
  dropdownName: { color: 'white', fontSize: 14, fontWeight: '600' },
  dropdownMeta: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: '500',
    marginTop: 1,
  },
  predBadge: {
    borderWidth: 1,
    borderRadius: 6,
    paddingHorizontal: 6,
    paddingVertical: 2,
  },
  predBadgeText: { fontSize: 10, fontWeight: '700' },

  // Number inputs
  inputRow: { flexDirection: 'row', gap: 10, marginTop: 12 },
  inputGroup: { flex: 1 },
  inputLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  numberInput: {
    backgroundColor: 'rgba(255,255,255,0.05)',
    borderRadius: 10,
    height: 44,
    paddingHorizontal: 12,
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
    textAlign: 'center',
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.08)',
  },

  // Add button
  addButton: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    gap: 8,
    backgroundColor: '#3b82f6',
    borderRadius: 12,
    height: 48,
    marginTop: 14,
  },
  addButtonDisabled: { opacity: 0.35 },
  addButtonText: { color: 'white', fontSize: 15, fontWeight: '700' },

  // Investments header
  investmentsHeader: {
    flexDirection: 'row',
    alignItems: 'baseline',
    gap: 10,
    marginTop: 24,
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
  plDivider: { width: 1, backgroundColor: 'rgba(255,255,255,0.06)' },
  plBlockLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    marginBottom: 3,
  },
  plLabelRow: {
    flexDirection: 'row',
    alignItems: 'flex-end',
    gap: 4,
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
  emptySubtext: { color: theme.colors.muted, fontSize: 14, marginTop: 6 },
});
