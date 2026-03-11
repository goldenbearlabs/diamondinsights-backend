import React, { useCallback, useEffect, useMemo, useState } from 'react';

import { 
  View, 
  Text, 
  StyleSheet, 
  TextInput, 
  TouchableOpacity, 
  FlatList, 
  ActivityIndicator,
  RefreshControl,
  Modal,
  Pressable,
  DeviceEventEmitter
} from 'react-native';
import { Image } from 'expo-image';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { useRouter } from 'expo-router';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { theme } from '../../theme/colors';

import { useBackendProStatus } from '../../lib/proStatus';
import { apiGet, apiGetAuth} from '../../lib/api';
import { auth } from '../../lib/firebase';

type CardData = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  series: string;
  display_position: string;
  age: number;
  is_hitter: boolean;
  rarity: string;
  comment_count: number;
  user_prediction_count: number;
  predicted_ovr: number | null;
  predicted_attributes: Record<string, number> | null;
  user_prediction: number | null;
};

const NON_PRO_ALLOWED_RARITIES = ['common', 'bronze'] as const;
const ALL_RARITIES = ['common', 'bronze', 'silver', 'gold', 'diamond'] as const;

export default function PredictionsScreen() {
  const router = useRouter();
  const { isPro } = useBackendProStatus();
  const enforceNonProRarity = isPro === false;
  
  
  const [cards, setCards] = useState<CardData[]>([]);
  const [searchText, setSearchText] = useState('');
  
  // Filter state
  const [selectedRarities, setSelectedRarities] = useState<string[]>([]);
  const [tempSelectedRarities, setTempSelectedRarities] = useState<string[]>([]);
  const [selectedPlayerType, setSelectedPlayerType] = useState<'all' | 'hitter' | 'pitcher'>('all');
  const [tempSelectedPlayerType, setTempSelectedPlayerType] = useState<'all' | 'hitter' | 'pitcher'>('all');
  const [selectedPopularity, setSelectedPopularity] = useState<'none' | 'most' | 'least'>('none');
  const [tempSelectedPopularity, setTempSelectedPopularity] = useState<'none' | 'most' | 'least'>('none');
  const [selectedDelta, setSelectedDelta] = useState<'none' | 'high' | 'low'>('none');
  const [tempSelectedDelta, setTempSelectedDelta] = useState<'none' | 'high' | 'low'>('none');
  const [filterModalOpen, setFilterModalOpen] = useState(false);
  const [currentFilterGroup, setCurrentFilterGroup] = useState<string | null>(null);
  
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(25); 
  const [hasMore, setHasMore] = useState(true); 
  const [refreshing, setRefreshing] = useState(false);
  const [debouncedSearchText, setDebouncedSearchText] = useState('');

  const effectiveSelectedRarities = useMemo(
    () => (enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : selectedRarities),
    [enforceNonProRarity, selectedRarities]
  );

  useEffect(() => {
    const delayDebounceFn = setTimeout(() => {
      setDebouncedSearchText(searchText);
      setPage(1);
    }, 500);
    return () => clearTimeout(delayDebounceFn);
  }, [searchText]);

 
  

  useEffect(() => {
    if (!enforceNonProRarity) return;
    const forcedRarities = [...NON_PRO_ALLOWED_RARITIES];
    setSelectedRarities(forcedRarities);
    setTempSelectedRarities(forcedRarities);
  }, [enforceNonProRarity]);

  useEffect(() => {
    // Listen for Predictions (Updates value AND increments count if new)
    const predSub = DeviceEventEmitter.addListener('PredictionUpdated', (event) => {
      const { cardId, newPrediction, isNewPrediction } = event;
      setCards((currentCards) => 
        currentCards.map((card) => {
          if (card.id === cardId) {
            return { 
              ...card, 
              user_prediction: newPrediction,
              // Only increase the total count if this wasn't an edit!
              user_prediction_count: isNewPrediction ? (card.user_prediction_count || 0) + 1 : card.user_prediction_count
            };
          }
          return card;
        })
      );
    });

    // Listen for New Comments (Increments count)
    const commentAddSub = DeviceEventEmitter.addListener('CommentAdded', (event) => {
      const { cardId } = event;
      setCards((currentCards) => 
        currentCards.map((card) => 
          card.id === cardId ? { ...card, comment_count: (card.comment_count || 0) + 1 } : card
        )
      );
    });

    // Listen for Deleted Comments (Decrements count)
    const commentDelSub = DeviceEventEmitter.addListener('CommentDeleted', (event) => {
      const { cardId } = event;
      setCards((currentCards) => 
        currentCards.map((card) => 
          card.id === cardId ? { ...card, comment_count: Math.max(0, (card.comment_count || 0) - 1) } : card
        )
      );
    });
    const predDelSub = DeviceEventEmitter.addListener('PredictionDeleted', (event) => {
      const { cardId } = event;
      setCards((currentCards) => 
        currentCards.map((card) => {
          if (card.id === cardId) {
            return { 
              ...card, 
              user_prediction: null, // Strips the blue badge away
              user_prediction_count: Math.max(0, (card.user_prediction_count || 0) - 1)
            };
          }
          return card;
        })
      );
    });

    // Cleanup all listeners when screen unmounts
    return () => {
      predSub.remove();
      commentAddSub.remove();
      commentDelSub.remove();
      predDelSub.remove();
    };
  }, []);

  
  const loadCards = useCallback(async (targetPage: number, targetLimit: number, query: string) => {
    setLoading(true);
    try {
      const offset = (targetPage - 1) * targetLimit;
      
      let url = `/cards?series=live&year=25&offset=${offset}&limit=${targetLimit}`;
      
      if (query.trim().length > 0) {
        url += `&name=${encodeURIComponent(query)}`;
      }
      
      
      // Add rarity filter to URL if any are selected (server-side filtering)
      if (effectiveSelectedRarities.length > 0) {
        url += `&rarity=${effectiveSelectedRarities.join(',')}`;
      }

      // Add player type filter
      if (selectedPlayerType === 'hitter') {
        url += '&is_hitter=true';
      } else if (selectedPlayerType === 'pitcher') {
        url += '&is_hitter=false';
      }

      // Add sort (only if a sort filter is active, otherwise default OVR desc)
      if (selectedPopularity !== 'none') {
        url += `&sort_by=popularity&desc=${selectedPopularity === 'most'}`;
      } else if (selectedDelta !== 'none') {
        url += `&sort_by=predicted_ovr_delta&desc=${selectedDelta === 'high'}`;
      }

      const newCards = auth.currentUser
        ? await apiGetAuth<CardData[]>(url) 
        : await apiGet<CardData[]>(url);

      
      setCards(newCards);
      
      setHasMore(newCards.length === targetLimit);

    } catch (error) {
      console.error("Failed to fetch cards:", error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [
    effectiveSelectedRarities,
    selectedDelta,
    selectedPlayerType,
    selectedPopularity,
  ]);
  useEffect(() => {
    void loadCards(page, limit, debouncedSearchText);
  }, [debouncedSearchText, limit, loadCards, page]);

  const handleRefresh = () => {
    setRefreshing(true);
    setPage(1);
    void loadCards(1, limit, debouncedSearchText);
  };

  const renderItem = ({ item }: { item: CardData }) => {
    return (
      <TouchableOpacity 
        style={styles.cardContainer} 
        activeOpacity={0.7}
        onPress={() => router.push({ pathname: "/predictions/[id]", params: { id: item.id, cardData: JSON.stringify(item) } })}
      >
        <Image source={ item.baked_img } style={styles.playerCardImage} contentFit="contain" transition={200}/>
        <View style={styles.infoColumn}>
          <Text style={styles.playerName} numberOfLines={1}>{item.name}</Text>
          <View style={styles.teamRow}>
            <Text style={styles.teamName}>{item.team_short_name}</Text>
            <View style={styles.verticalDivider} />
            <View style={styles.socialItem}><Ionicons name="bar-chart" size={10} color="#a78bfa" /><Text style={styles.socialText}>{item.user_prediction_count ?? 0}</Text></View>
            <View style={styles.socialItem}><FontAwesome5 name="comment-alt" size={10} color={theme.colors.muted} solid /><Text style={styles.socialText}>{item.comment_count ?? 0}</Text></View>
            {item.user_prediction != null && (
              <>
                <View style={styles.verticalDivider} />
                <View style={styles.socialItem}>
                  <Ionicons name="person" size={10} color="#3b82f6" />
                  <Text style={[styles.socialText, { color: '#3b82f6' }]}>
                    {item.user_prediction} OVR
                  </Text>
                </View>
              </>
            )}

          </View>


          <View style={styles.ratingRow}>
            <View style={styles.ratingBadge}><Text style={styles.ratingLabel}>CUR</Text><Text style={styles.currentRating}>{item.ovr}</Text></View>
            {item.predicted_ovr != null && (
              <>
                <Ionicons name="arrow-forward" size={14} color={item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : theme.colors.muted} style={{ marginHorizontal: 8 }} />
                <View style={[styles.ratingBadge, { borderColor: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : 'rgba(255,255,255,0.1)' }]}>
                  <Text style={[styles.ratingLabel, { color: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : theme.colors.muted }]}>PRED</Text>
                  <Text style={[styles.currentRating, { color: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : 'white' }]}>{item.predicted_ovr}</Text>
                </View>
              </>
            )}
          </View>
        </View>
        <View style={styles.arrowContainer}><Ionicons name="chevron-forward" size={20} color={theme.colors.muted} /></View>
      </TouchableOpacity>
    );
  };

  const renderFooter = () => {
    return (
      <View style={styles.footerContainer}>
        
        {/* Limit Selector */}
        <View style={styles.limitContainer}>
          <Text style={styles.footerLabel}>View:</Text>
          {[10, 25, 50].map((val) => (
            <TouchableOpacity 
              key={val} 
              style={[styles.limitBtn, limit === val && styles.limitBtnActive]}
              onPress={() => {
                if (limit !== val) {
                  setLimit(val);
                  setPage(1); // Reset to page 1 if limit changes
                }
              }}
            >
              <Text style={[styles.limitText, limit === val && styles.limitTextActive]}>{val}</Text>
            </TouchableOpacity>
          ))}
        </View>

        {/*Page Navigation */}
        <View style={styles.navRow}>
          <TouchableOpacity 
            style={[styles.navBtn, page === 1 && styles.navBtnDisabled]}
            disabled={page === 1}
            onPress={() => setPage(p => Math.max(1, p - 1))}
          >
            <Ionicons name="chevron-back" size={20} color={page === 1 ? '#555' : 'white'} />
            <Text style={[styles.navBtnText, page === 1 && { color: '#555' }]}>Prev</Text>
          </TouchableOpacity>

          <Text style={styles.pageNumber}>Page {page}</Text>

          <TouchableOpacity 
            style={[styles.navBtn, !hasMore && styles.navBtnDisabled]}
            disabled={!hasMore}
            onPress={() => setPage(p => p + 1)}
          >
            <Text style={[styles.navBtnText, !hasMore && { color: '#555' }]}>Next</Text>
            <Ionicons name="chevron-forward" size={20} color={!hasMore ? '#555' : 'white'} />
          </TouchableOpacity>
        </View>

      </View>
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}><FloatingBackground /></View>
      <View style={{ flex: 1, paddingTop: 20 }}>
        <View style={styles.content}>
          <Text style={styles.headerTitle}>Market Predictions</Text>
          
          
          <View style={styles.searchRow}>
            <View style={styles.searchInputContainer}>
              <Ionicons name="search" size={18} color={theme.colors.muted} style={{ marginRight: 8 }} />
              <TextInput 
                style={styles.searchInput}
                placeholder="Search players..."
                placeholderTextColor={theme.colors.muted}
                value={searchText}
                onChangeText={setSearchText}
                autoCapitalize="none"
              />
            </View>
            
            <TouchableOpacity 
              style={styles.filterBtn} 
              onPress={() => {
                setTempSelectedRarities(effectiveSelectedRarities);
                setTempSelectedPlayerType(selectedPlayerType);
                setTempSelectedPopularity(selectedPopularity);
                setTempSelectedDelta(selectedDelta);
                setFilterModalOpen(true);
              }}
            >
              <Ionicons name="options" size={20} color="white" />
              {((enforceNonProRarity ? 0 : selectedRarities.length) + (selectedPlayerType !== 'all' ? 1 : 0) + (selectedPopularity !== 'none' ? 1 : 0) + (selectedDelta !== 'none' ? 1 : 0)) > 0 && (
                <View style={styles.filterBadge}>
                  <Text style={styles.filterBadgeText}>{(enforceNonProRarity ? 0 : selectedRarities.length) + (selectedPlayerType !== 'all' ? 1 : 0) + (selectedPopularity !== 'none' ? 1 : 0) + (selectedDelta !== 'none' ? 1 : 0)}</Text>
                </View>
              )}
            </TouchableOpacity>
          </View>
          
          {/* Quick Filters Row */}
          <View style={styles.quickFiltersRow}>
            <TouchableOpacity 
              style={styles.quickFilterChip}
              onPress={() => {
                router.push('/(app)/my-predictions');
              }}
            >
              <Ionicons name="list" size={14} color="white" />
              <Text style={styles.quickFilterText}>
                My Predictions
              </Text>
            </TouchableOpacity>
            
          </View>


          {/* Filter Modal */}
          <Modal
            visible={filterModalOpen}
            transparent
            animationType="fade"
            onRequestClose={() => setFilterModalOpen(false)}
          >
            <View style={styles.modalOverlay}>
              <Pressable 
                style={styles.modalBackdrop} 
                onPress={() => {
                  setFilterModalOpen(false);
                  setCurrentFilterGroup(null);
                }}
              />
              <View style={styles.modalCard}>
                {currentFilterGroup === null ? (
                  // Filter Groups List
                  <>
                    <View style={styles.modalHeader}>
                      <Text style={styles.modalTitle}>Filters</Text>
                      <View style={{ flexDirection: 'row', alignItems: 'center', gap: 16 }}>
                        {((enforceNonProRarity ? false : selectedRarities.length > 0) || selectedPlayerType !== 'all' || selectedPopularity !== 'none' || selectedDelta !== 'none') && (
                          <TouchableOpacity onPress={() => {
                            setSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : []);
                            setSelectedPlayerType('all');
                            setSelectedPopularity('none');
                            setSelectedDelta('none');
                            setFilterModalOpen(false);
                            setCurrentFilterGroup(null);
                            setPage(1);
                          }}>
                            <Text style={{ color: '#f87171', fontSize: 14, fontWeight: '600' }}>Clear All</Text>
                          </TouchableOpacity>
                        )}
                        <TouchableOpacity onPress={() => setFilterModalOpen(false)}>
                          <Ionicons name="close" size={24} color="white" />
                        </TouchableOpacity>
                      </View>
                    </View>
                    <TouchableOpacity 
                      style={styles.filterGroupRow}
                      onPress={() => setCurrentFilterGroup('playerType')}
                    >
                      <View style={styles.filterGroupLeft}>
                        <Text style={styles.filterGroupLabel}>Player Type</Text>
                      </View>
                      <View style={styles.filterGroupRight}>
                        {tempSelectedPlayerType !== 'all' && (
                          <View style={styles.filterCountBadge}>
                            <Text style={styles.filterCountText}>1</Text>
                          </View>
                        )}
                        <Ionicons name="chevron-forward" size={20} color={theme.colors.muted} />
                      </View>
                    </TouchableOpacity>
                    <TouchableOpacity 
                      style={styles.filterGroupRow}
                      onPress={() => setCurrentFilterGroup('popularity')}
                    >
                      <View style={styles.filterGroupLeft}>
                        <Text style={styles.filterGroupLabel}>Popularity</Text>
                      </View>
                      <View style={styles.filterGroupRight}>
                        {tempSelectedPopularity !== 'none' && (
                          <View style={styles.filterCountBadge}>
                            <Text style={styles.filterCountText}>1</Text>
                          </View>
                        )}
                        <Ionicons name="chevron-forward" size={20} color={theme.colors.muted} />
                      </View>
                    </TouchableOpacity>
                    <TouchableOpacity 
                      style={styles.filterGroupRow}
                      onPress={() => setCurrentFilterGroup('delta')}
                    >
                      <View style={styles.filterGroupLeft}>
                        <Text style={styles.filterGroupLabel}>Predicted OVR</Text>
                      </View>
                      <View style={styles.filterGroupRight}>
                        {tempSelectedDelta !== 'none' && (
                          <View style={styles.filterCountBadge}>
                            <Text style={styles.filterCountText}>1</Text>
                          </View>
                        )}
                        <Ionicons name="chevron-forward" size={20} color={theme.colors.muted} />
                      </View>
                    </TouchableOpacity>
                    <TouchableOpacity 
                      style={styles.filterGroupRow}
                      onPress={() => setCurrentFilterGroup('rarity')}
                    >
                      <View style={styles.filterGroupLeft}>
                        <Text style={styles.filterGroupLabel}>Rarity</Text>
                      </View>
                      <View style={styles.filterGroupRight}>
                        {(!enforceNonProRarity && tempSelectedRarities.length > 0) && (
                          <View style={styles.filterCountBadge}>
                            <Text style={styles.filterCountText}>{tempSelectedRarities.length}</Text>
                          </View>
                        )}
                        <Ionicons name="chevron-forward" size={20} color={theme.colors.muted} />
                      </View>
                    </TouchableOpacity>
                    <View style={styles.modalActions}>
                      <TouchableOpacity
                        style={styles.applyButton}
                        onPress={() => {
                          setSelectedPlayerType(tempSelectedPlayerType);
                          setSelectedPopularity(tempSelectedPopularity);
                          setSelectedDelta(tempSelectedDelta);
                          setSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : tempSelectedRarities);
                          if (tempSelectedPopularity !== 'none') setSelectedDelta('none');
                          if (tempSelectedDelta !== 'none') setSelectedPopularity('none');
                          setFilterModalOpen(false);
                          setCurrentFilterGroup(null);
                          setPage(1);
                        }}
                      >
                        <Text style={styles.applyButtonText}>Apply All</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                ) : currentFilterGroup === 'popularity' ? (
                  // Popularity Sort Options
                  <>
                    <View style={styles.modalHeader}>
                      <TouchableOpacity 
                        style={styles.backButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Ionicons name="chevron-back" size={24} color="white" />
                        <Text style={styles.backText}>Back</Text>
                      </TouchableOpacity>
                      <TouchableOpacity onPress={() => setFilterModalOpen(false)}>
                        <Ionicons name="close" size={24} color="white" />
                      </TouchableOpacity>
                    </View>
                    <Text style={styles.modalSubtitle}>Sort by Popularity</Text>
                    {([['most', 'Most to Least'], ['least', 'Least to Most']] as const).map(([value, label]) => (
                      <TouchableOpacity
                        key={value}
                        style={styles.checkboxRow}
                        onPress={() => setTempSelectedPopularity(value)}
                      >
                        <Ionicons 
                          name={tempSelectedPopularity === value ? 'radio-button-on' : 'radio-button-off'} 
                          size={22} 
                          color={tempSelectedPopularity === value ? '#3b82f6' : 'rgba(255, 255, 255, 0.3)'} 
                          style={{ marginRight: 12 }}
                        />
                        <Text style={styles.checkboxLabel}>{label}</Text>
                      </TouchableOpacity>
                    ))}
                    <View style={styles.modalActions}>
                      <TouchableOpacity
                        style={styles.clearButton}
                        onPress={() => {
                          setTempSelectedPopularity('none');
                          setCurrentFilterGroup(null);
                        }}
                      >
                        <Text style={styles.clearButtonText}>Clear</Text>
                      </TouchableOpacity>
                      <TouchableOpacity
                        style={styles.applyButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Text style={styles.applyButtonText}>Done</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                ) : currentFilterGroup === 'delta' ? (
                  // Predicted OVR Sort Options
                  <>
                    <View style={styles.modalHeader}>
                      <TouchableOpacity 
                        style={styles.backButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Ionicons name="chevron-back" size={24} color="white" />
                        <Text style={styles.backText}>Back</Text>
                      </TouchableOpacity>
                      <TouchableOpacity onPress={() => setFilterModalOpen(false)}>
                        <Ionicons name="close" size={24} color="white" />
                      </TouchableOpacity>
                    </View>
                    <Text style={styles.modalSubtitle}>Sort by Predicted OVR</Text>
                    {([['high', 'Highest Predicted Increase'], ['low', 'Highest Predicted Decrease']] as const).map(([value, label]) => (
                      <TouchableOpacity
                        key={value}
                        style={styles.checkboxRow}
                        onPress={() => setTempSelectedDelta(value)}
                      >
                        <Ionicons 
                          name={tempSelectedDelta === value ? 'radio-button-on' : 'radio-button-off'} 
                          size={22} 
                          color={tempSelectedDelta === value ? '#3b82f6' : 'rgba(255, 255, 255, 0.3)'} 
                          style={{ marginRight: 12 }}
                        />
                        <Text style={styles.checkboxLabel}>{label}</Text>
                      </TouchableOpacity>
                    ))}
                    <View style={styles.modalActions}>
                      <TouchableOpacity
                        style={styles.clearButton}
                        onPress={() => {
                          setTempSelectedDelta('none');
                          setCurrentFilterGroup(null);
                        }}
                      >
                        <Text style={styles.clearButtonText}>Clear</Text>
                      </TouchableOpacity>
                      <TouchableOpacity
                        style={styles.applyButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Text style={styles.applyButtonText}>Done</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                ) : currentFilterGroup === 'playerType' ? (
                  // Player Type Options (Radio Buttons)
                  <>
                    <View style={styles.modalHeader}>
                      <TouchableOpacity 
                        style={styles.backButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Ionicons name="chevron-back" size={24} color="white" />
                        <Text style={styles.backText}>Back</Text>
                      </TouchableOpacity>
                      <TouchableOpacity onPress={() => setFilterModalOpen(false)}>
                        <Ionicons name="close" size={24} color="white" />
                      </TouchableOpacity>
                    </View>
                    <Text style={styles.modalSubtitle}>Select Player Type</Text>
                    {([['all', 'All Players'], ['hitter', 'Hitters'], ['pitcher', 'Pitchers']] as const).map(([value, label]) => (
                      <TouchableOpacity
                        key={value}
                        style={styles.checkboxRow}
                        onPress={() => setTempSelectedPlayerType(value)}
                      >
                        <Ionicons 
                          name={tempSelectedPlayerType === value ? 'radio-button-on' : 'radio-button-off'} 
                          size={22} 
                          color={tempSelectedPlayerType === value ? '#3b82f6' : 'rgba(255, 255, 255, 0.3)'} 
                          style={{ marginRight: 12 }}
                        />
                        <Text style={styles.checkboxLabel}>{label}</Text>
                      </TouchableOpacity>
                    ))}
                    <View style={styles.modalActions}>
                      <TouchableOpacity
                        style={styles.clearButton}
                        onPress={() => {
                          setTempSelectedPlayerType('all');
                          setCurrentFilterGroup(null);
                        }}
                      >
                        <Text style={styles.clearButtonText}>Clear</Text>
                      </TouchableOpacity>
                      <TouchableOpacity
                        style={styles.applyButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Text style={styles.applyButtonText}>Done</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                ) : (
                  // Rarity Options
                  <>
                    <View style={styles.modalHeader}>
                      <TouchableOpacity 
                        style={styles.backButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Ionicons name="chevron-back" size={24} color="white" />
                        <Text style={styles.backText}>Back</Text>
                      </TouchableOpacity>
                      <TouchableOpacity onPress={() => setFilterModalOpen(false)}>
                        <Ionicons name="close" size={24} color="white" />
                      </TouchableOpacity>
                    </View>
                    {enforceNonProRarity ? (
                      <Text style={styles.proRestrictionHint}>
                        Non-Pro access includes Bronze/Common market predictions.
                      </Text>
                    ) : null}
                    <Text style={styles.modalSubtitle}>Select Rarities</Text>
                    {ALL_RARITIES.map((rarity) => {
                      const isAlwaysIncludedForNonPro = enforceNonProRarity && NON_PRO_ALLOWED_RARITIES.includes(rarity);
                      const isLockedForNonPro = enforceNonProRarity && !NON_PRO_ALLOWED_RARITIES.includes(rarity);
                      const isChecked = isAlwaysIncludedForNonPro || tempSelectedRarities.includes(rarity);
                      const canToggle = !isLockedForNonPro && !isAlwaysIncludedForNonPro;

                      return (
                        <View key={rarity} style={[styles.checkboxRow, isLockedForNonPro && styles.checkboxRowLocked]}>
                          <TouchableOpacity
                            style={styles.checkboxRowMain}
                            disabled={!canToggle}
                            onPress={() => {
                              if (!canToggle) return;
                              setTempSelectedRarities(prev =>
                                prev.includes(rarity)
                                  ? prev.filter(r => r !== rarity)
                                  : [...prev, rarity]
                              );
                            }}
                          >
                            <View style={[
                              styles.checkbox,
                              isChecked && styles.checkboxChecked,
                              isAlwaysIncludedForNonPro && styles.checkboxForced
                            ]}>
                              {isChecked && (
                                <Ionicons name="checkmark" size={16} color="white" />
                              )}
                            </View>
                            <Text style={[styles.checkboxLabel, isLockedForNonPro && styles.checkboxLabelLocked]}>
                              {rarity.charAt(0).toUpperCase() + rarity.slice(1)}
                            </Text>
                          </TouchableOpacity>
                          {isLockedForNonPro ? (
                            <View style={styles.lockedOverlay}>
                              <Text style={styles.lockedOverlayText}>Subscribe to Pro to unlock</Text>
                              <TouchableOpacity
                                style={styles.lockedOverlayButton}
                                onPress={() => {
                                  setFilterModalOpen(false);
                                  setCurrentFilterGroup(null);
                                  router.push('/paywall');
                                }}
                              >
                                <Text style={styles.lockedOverlayButtonText}>Go Pro</Text>
                              </TouchableOpacity>
                            </View>
                          ) : null}
                        </View>
                      );
                    })}
                    <View style={styles.modalActions}>
                      <TouchableOpacity
                        style={styles.clearButton}
                        onPress={() => {
                          setTempSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : []);
                          setCurrentFilterGroup(null);
                        }}
                      >
                        <Text style={styles.clearButtonText}>Clear</Text>
                      </TouchableOpacity>
                      <TouchableOpacity
                        style={styles.applyButton}
                        onPress={() => setCurrentFilterGroup(null)}
                      >
                        <Text style={styles.applyButtonText}>Done</Text>
                      </TouchableOpacity>
                    </View>
                  </>
                )}
              </View>
            </View>
          </Modal>

          {loading ? (
            <View style={{ marginTop: 50 }}><ActivityIndicator size="large" color="#fbbf24" /></View>
          ) : (
            <FlatList
              data={cards}
              keyExtractor={(item) => item.id}
              renderItem={renderItem}
              contentContainerStyle={styles.listContent}
              showsVerticalScrollIndicator={false}
              refreshControl={<RefreshControl refreshing={refreshing} onRefresh={handleRefresh} tintColor="#fff" />}
              ListFooterComponent={cards.length > 0 ? renderFooter : null} // Only show footer if we have data
              ListEmptyComponent={
                ((enforceNonProRarity ? false : selectedRarities.length > 0) || selectedPlayerType !== 'all' || selectedPopularity !== 'none' || selectedDelta !== 'none') ? (
                  <View style={styles.emptyContainer}>
                    <Text style={styles.emptyText}>No cards match your filters.</Text>
                    <TouchableOpacity 
                      style={styles.clearFiltersButton}
                      onPress={() => {
                        setSelectedRarities(enforceNonProRarity ? [...NON_PRO_ALLOWED_RARITIES] : []);
                        setSelectedPlayerType('all');
                        setSelectedPopularity('none');
                        setSelectedDelta('none');
                        setPage(1);
                      }}
                    >
                      <Text style={styles.clearFiltersButtonText}>Clear Filters</Text>
                    </TouchableOpacity>
                  </View>
                ) : (
                  <Text style={styles.emptyText}>No players found.</Text>
                )
              }
            />
          )}
        </View>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: theme.colors.background },
  backgroundLayer: { ...StyleSheet.absoluteFillObject, zIndex: -1 },
  content: { flex: 1, paddingHorizontal: 16, paddingTop: 0 },
  headerTitle: { fontSize: 28, fontWeight: '800', color: 'white', marginBottom: 16 },
  searchRow: { flexDirection: 'row', gap: 12, marginBottom: 12 },
  searchInputContainer: { flex: 1, flexDirection: 'row', alignItems: 'center', backgroundColor: 'rgba(255, 255, 255, 0.05)', borderRadius: 12, paddingHorizontal: 12, height: 48, borderWidth: 1, borderColor: 'rgba(255, 255, 255, 0.1)' },
  searchInput: { flex: 1, color: 'white', fontSize: 16, fontWeight: '500' },
  filterBtn: { width: 48, height: 48, borderRadius: 12, backgroundColor: 'rgba(255, 255, 255, 0.05)', justifyContent: 'center', alignItems: 'center', borderWidth: 1, borderColor: 'rgba(255, 255, 255, 0.1)' },
  listContent: { paddingBottom: 40, gap: 16 },
  emptyText: { color: theme.colors.muted, textAlign: 'center', marginTop: 40, fontSize: 16 },
  cardContainer: { flexDirection: 'row', alignItems: 'center', backgroundColor: 'rgba(2, 6, 23, 0.6)', borderRadius: 16, padding: 10, borderWidth: 1, borderColor: 'rgba(255, 255, 255, 0.08)' },
  playerCardImage: { width: 50, height: 70, marginRight: 16, borderRadius: 4 },
  infoColumn: { flex: 1, justifyContent: 'center' },
  playerName: { color: 'white', fontSize: 16, fontWeight: 'bold', marginBottom: 4 },
  teamRow: { flexDirection: 'row', alignItems: 'center', marginBottom: 8 },
  teamName: { color: theme.colors.muted, fontSize: 12, fontWeight: '700', textTransform: 'uppercase', letterSpacing: 0.5 },
  verticalDivider: { width: 1, height: 12, backgroundColor: 'rgba(255,255,255,0.2)', marginHorizontal: 8 },
  socialItem: { flexDirection: 'row', alignItems: 'center', marginRight: 10, gap: 4 },
  socialText: { color: theme.colors.muted, fontSize: 10, fontWeight: '600' },
  ratingRow: { flexDirection: 'row', alignItems: 'center' },
  ratingBadge: { alignItems: 'center', justifyContent: 'center', paddingHorizontal: 8, paddingVertical: 2, borderRadius: 6, backgroundColor: 'rgba(255,255,255,0.05)', borderWidth: 1, borderColor: 'rgba(255,255,255,0.1)' },
  ratingLabel: { fontSize: 8, color: theme.colors.muted, fontWeight: '800', marginBottom: 1 },
  currentRating: { color: 'white', fontWeight: '700', fontSize: 14 },
  arrowContainer: { paddingLeft: 10 },
  quickFiltersRow: { flexDirection: 'row', marginBottom: 12, paddingRight: 20},
  quickFilterChip: { flexDirection: 'row', alignItems: 'center', gap: 6, backgroundColor: 'rgba(255, 255, 255, 0.05)', paddingVertical: 6, paddingHorizontal: 12, borderRadius: 10, borderWidth: 1, borderColor: 'rgba(255, 255, 255, 0.1)', alignSelf: 'flex-start' },
  quickFilterChipActive: { backgroundColor: '#3b82f6', borderColor: '#3b82f6' },
  quickFilterText: { color: theme.colors.muted, fontSize: 13, fontWeight: '600' },
  quickFilterTextActive: { color: 'white', fontWeight: 'bold' },
  proHintContainer: {
    flex: 1, 
  },
  proHintText: {
    color: theme.colors.muted,
    fontSize: 8,
    lineHeight: 16,
  },
  proHintLink: {
    color: '#fbbf24',
    fontWeight: 'bold',
  },

  footerContainer: {
    marginTop: 20,
    marginBottom: 80, 
    alignItems: 'center',
    gap: 16,
  },
  limitContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  footerLabel: {
    color: theme.colors.muted,
    fontSize: 14,
    fontWeight: '600',
  },
  limitBtn: {
    paddingVertical: 6,
    paddingHorizontal: 12,
    borderRadius: 8,
    backgroundColor: 'rgba(255,255,255,0.05)',
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.1)',
  },
  limitBtnActive: {
    backgroundColor: '#3b82f6',
    borderColor: '#3b82f6',
  },
  limitText: {
    color: theme.colors.muted,
    fontWeight: '600',
    fontSize: 12,
  },
  limitTextActive: {
    color: 'white',
    fontWeight: 'bold',
  },
  navRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 20,
    width: '100%',
    justifyContent: 'center',
  },
  navBtn: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    paddingVertical: 10,
    paddingHorizontal: 20,
    backgroundColor: 'rgba(255,255,255,0.05)',
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.1)',
  },
  navBtnDisabled: {
    opacity: 0.5,
    backgroundColor: 'transparent',
    borderColor: 'rgba(255,255,255,0.05)',
  },
  navBtnText: {
    color: 'white',
    fontWeight: '600',
    fontSize: 16,
  },
  pageNumber: {
    color: 'white',
    fontSize: 18,
    fontWeight: 'bold',
    minWidth: 40,
    textAlign: 'center',
  },
  // Filter Badge
  filterBadge: {
    position: 'absolute',
    top: -4,
    right: -4,
    backgroundColor: '#fbbf24',
    borderRadius: 10,
    minWidth: 20,
    height: 20,
    alignItems: 'center',
    justifyContent: 'center',
    borderWidth: 2,
    borderColor: theme.colors.background,
  },
  filterBadgeText: {
    color: theme.colors.background,
    fontSize: 11,
    fontWeight: 'bold',
  },
  // Filter Modal
  modalOverlay: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
  },
  modalBackdrop: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: 'rgba(0, 0, 0, 0.7)',
  },
  modalCard: {
    width: '85%',
    maxHeight: '70%',
    backgroundColor: 'rgba(15, 23, 42, 0.98)',
    borderRadius: 20,
    padding: 20,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
  modalHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 20,
  },
  modalTitle: {
    fontSize: 24,
    fontWeight: 'bold',
    color: 'white',
  },
  modalSubtitle: {
    fontSize: 16,
    fontWeight: '600',
    color: theme.colors.muted,
    marginBottom: 16,
  },
  backButton: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  backText: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
  },
  filterGroupRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 16,
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    marginBottom: 12,
  },
  filterGroupLeft: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
  },
  filterGroupLabel: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
  },
  filterGroupRight: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  filterCountBadge: {
    backgroundColor: '#fbbf24',
    borderRadius: 10,
    minWidth: 20,
    height: 20,
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 6,
  },
  filterCountText: {
    color: theme.colors.background,
    fontSize: 11,
    fontWeight: 'bold',
  },
  checkboxRow: {
    position: 'relative',
    paddingVertical: 12,
    paddingHorizontal: 16,
    backgroundColor: 'rgba(255, 255, 255, 0.03)',
    borderRadius: 10,
    marginBottom: 8,
  },
  checkboxRowMain: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  checkboxRowLocked: {
    backgroundColor: 'rgba(255, 255, 255, 0.015)',
  },
  checkbox: {
    width: 24,
    height: 24,
    borderRadius: 6,
    borderWidth: 2,
    borderColor: 'rgba(255, 255, 255, 0.3)',
    alignItems: 'center',
    justifyContent: 'center',
    marginRight: 12,
  },
  checkboxChecked: {
    backgroundColor: '#3b82f6',
    borderColor: '#3b82f6',
  },
  checkboxForced: {
    backgroundColor: '#4b5563',
    borderColor: '#9ca3af',
  },
  checkboxLabel: {
    color: 'white',
    fontSize: 16,
    fontWeight: '500',
  },
  checkboxLabelLocked: {
    color: 'rgba(255,255,255,0.45)',
  },
  lockedOverlay: {
    ...StyleSheet.absoluteFillObject,
    borderRadius: 10,
    backgroundColor: 'rgba(15, 23, 42, 0.75)',
    borderWidth: 1,
    borderColor: 'rgba(251, 191, 36, 0.35)',
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 12,
  },
  lockedOverlayText: {
    color: '#fde68a',
    fontSize: 12,
    fontWeight: '700',
    flexShrink: 1,
    marginRight: 8,
  },
  lockedOverlayButton: {
    backgroundColor: '#fbbf24',
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 6,
  },
  lockedOverlayButtonText: {
    color: '#111827',
    fontSize: 12,
    fontWeight: '800',
  },
  proRestrictionHint: {
    color: '#fcd34d',
    fontSize: 12,
    fontWeight: '600',
    marginBottom: 12,
  },
  modalActions: {
    flexDirection: 'row',
    gap: 12,
    marginTop: 20,
  },
  clearButton: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    alignItems: 'center',
  },
  clearButtonText: {
    color: 'white',
    fontSize: 16,
    fontWeight: '600',
  },
  applyButton: {
    flex: 1,
    paddingVertical: 14,
    borderRadius: 12,
    backgroundColor: '#3b82f6',
    alignItems: 'center',
  },
  applyButtonText: {
    color: 'white',
    fontSize: 16,
    fontWeight: 'bold',
  },
  // Empty State
  emptyContainer: {
    alignItems: 'center',
    marginTop: 40,
  },
  clearFiltersButton: {
    marginTop: 16,
    paddingVertical: 12,
    paddingHorizontal: 24,
    backgroundColor: '#3b82f6',
    borderRadius: 12,
  },
  clearFiltersButtonText: {
    color: 'white',
    fontSize: 14,
    fontWeight: 'bold',
  },
});
