import React, { useState, useEffect } from 'react';
import { 
  View, 
  Text, 
  StyleSheet, 
  TouchableOpacity, 
  FlatList, 
  ActivityIndicator,
  RefreshControl,
  DeviceEventEmitter
} from 'react-native';
import { Image } from 'expo-image';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { useRouter, Stack} from 'expo-router';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { theme } from '../../theme/colors';
import { apiGetAuth } from '../../lib/api'; 
import { useBackendProStatus } from '../../lib/proStatus';


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
  user_prediction: number | null;
};

export default function MyPredictionsScreen() {
  const router = useRouter();
  const { isPro, loading: proStatusLoading } = useBackendProStatus();
  const showProLock = isPro === false || (isPro === null && !proStatusLoading);
  const [cards, setCards] = useState<CardData[]>([]);
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const limit = 15; // Hardcoded limit
  const [hasMore, setHasMore] = useState(true); 
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    loadCards(page);
  }, [page]);

  useEffect(() => {
    const predSub = DeviceEventEmitter.addListener('PredictionUpdated', (event) => {
      const { cardId, newPrediction, isNewPrediction } = event;
      setCards((currentCards) => 
        currentCards.map((card) => {
          if (card.id === cardId) {
            return { 
              ...card, 
              user_prediction: newPrediction,
              user_prediction_count: isNewPrediction ? (card.user_prediction_count || 0) + 1 : card.user_prediction_count
            };
          }
          return card;
        })
      );
    });

    const commentAddSub = DeviceEventEmitter.addListener('CommentAdded', (event) => {
      const { cardId } = event;
      setCards((currentCards) => 
        currentCards.map((card) => 
          card.id === cardId ? { ...card, comment_count: (card.comment_count || 0) + 1 } : card
        )
      );
    });

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
      setCards((currentCards) => currentCards.filter((card) => card.id !== cardId));
    });

    return () => {
      predSub.remove();
      commentAddSub.remove();
      commentDelSub.remove();
      predDelSub.remove();
    };
  }, []);

  const loadCards = async (targetPage: number) => {
    setLoading(true);
    try {
      const offset = (targetPage - 1) * limit;
      // Hardcoded my_predictions flag and auth requirement
      const url = `/cards?series=live&year=25&offset=${offset}&limit=${limit}&my_predictions=true`;
      
      const newCards = await apiGetAuth<CardData[]>(url);
      setCards(newCards);
      setHasMore(newCards.length === limit);
    } catch (error) {
      console.error("Failed to fetch my predictions:", error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = () => {
    setRefreshing(true);
    setPage(1);
    loadCards(1);
  };

  const renderItem = ({ item }: { item: CardData }) => {
    const isLiveSeries = item.series?.toLowerCase().includes('live') || (item as any).series_name?.toLowerCase().includes('live');
    const isPredLocked = showProLock && isLiveSeries && item.ovr >= 75;
    return (
      <TouchableOpacity 
        style={styles.cardContainer} 
        activeOpacity={0.7}
        onPress={() => router.push({ pathname: "/predictions/[id]", params: { id: item.id, cardData: JSON.stringify(item) } })}
      >
        <Image source={item.baked_img} style={styles.playerCardImage} contentFit="contain" transition={200} />
        <View style={styles.infoColumn}>
          <Text style={styles.playerName} numberOfLines={1}>{item.name}</Text>
          <View style={styles.teamRow}>
            <Text style={styles.teamName}>{item.team_short_name}</Text>
            <View style={styles.verticalDivider} />
            <View style={styles.socialItem}><Ionicons name="bar-chart" size={10} color="#a78bfa" /><Text style={styles.socialText}>{item.user_prediction_count ?? 0}</Text></View>
            <View style={styles.socialItem}><FontAwesome5 name="comment-alt" size={10} color={theme.colors.muted} solid /><Text style={styles.socialText}>{item.comment_count ?? 0}</Text></View>
            
            {/* User Prediction Badge */}
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
              isPredLocked ? (
                // --- LOCKED COMMUNITY PRED BADGE ---
                <>
                  <Ionicons name="arrow-forward" size={14} color="#fbbf24" style={{ marginHorizontal: 8 }} />
                  <TouchableOpacity 
                    style={[styles.ratingBadge, { borderColor: '#fbbf24', paddingHorizontal: 12, alignItems: 'center' }]}
                    onPress={() => router.push('/paywall')}
                    activeOpacity={0.7}
                  >
                    <Text style={[styles.ratingLabel, { color: '#fbbf24' }]}>PRED</Text>
                    <FontAwesome5 name="lock" size={12} color="#fbbf24" style={{ marginTop: 2 }} />
                  </TouchableOpacity>
                </>
              ) : (
                // --- UNLOCKED COMMUNITY PRED BADGE ---
                <>
                  <Ionicons name="arrow-forward" size={14} color={item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : theme.colors.muted} style={{ marginHorizontal: 8 }} />
                  <View style={[styles.ratingBadge, { borderColor: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : 'rgba(255,255,255,0.1)' }]}>
                    <Text style={[styles.ratingLabel, { color: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : theme.colors.muted }]}>PRED</Text>
                    <Text style={[styles.currentRating, { color: item.predicted_ovr > item.ovr ? '#4ade80' : item.predicted_ovr < item.ovr ? '#f87171' : 'white' }]}>{item.predicted_ovr}</Text>
                  </View>
                </>
              )
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
      <Stack.Screen options={{ headerShown: false }} />
      <View style={styles.backgroundLayer}><FloatingBackground /></View>
      <SafeAreaView style={{ flex: 1 }} edges={['top']}>
        
        {/* Header with Back Button */}
        <View style={styles.navBar}>
          <TouchableOpacity onPress={() => router.back()} style={styles.backBtn}>
            <Ionicons name="arrow-back" size={24} color="white" />
            <Text style={styles.backText}>Back</Text>
          </TouchableOpacity>
        </View>

        <View style={styles.content}>
          <Text style={styles.headerTitle}>My Predictions</Text>
          
          {loading && page === 1 ? (
            <View style={{ marginTop: 50 }}><ActivityIndicator size="large" color="#fbbf24" /></View>
          ) : (
            <FlatList
              data={cards}
              keyExtractor={(item) => item.id}
              renderItem={renderItem}
              contentContainerStyle={styles.listContent}
              showsVerticalScrollIndicator={false}
              refreshControl={<RefreshControl refreshing={refreshing} onRefresh={handleRefresh} tintColor="#fff" />}
              ListFooterComponent={cards.length > 0 ? renderFooter : null}
              ListEmptyComponent={
                <View style={styles.emptyContainer}>
                  <Text style={styles.emptyText}>You haven't made any predictions yet.</Text>
                </View>
              }
            />
          )}
        </View>
      </SafeAreaView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: theme.colors.background },
  backgroundLayer: { ...StyleSheet.absoluteFillObject, zIndex: -1 },
  navBar: { paddingHorizontal: 16, paddingBottom: 10 },
  backBtn: { flexDirection: 'row', alignItems: 'center', gap: 8 },
  backText: { color: 'white', fontSize: 16, fontWeight: '600' },
  content: { flex: 1, paddingHorizontal: 16 },
  headerTitle: { fontSize: 28, fontWeight: '800', color: 'white', marginBottom: 16 },
  listContent: { paddingBottom: 40, gap: 16 },
  emptyContainer: { alignItems: 'center', marginTop: 40 },
  emptyText: { color: theme.colors.muted, textAlign: 'center', fontSize: 16 },
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
  footerContainer: { marginTop: 20, marginBottom: 80, alignItems: 'center' },
  navRow: { flexDirection: 'row', alignItems: 'center', gap: 20, width: '100%', justifyContent: 'center' },
  navBtn: { flexDirection: 'row', alignItems: 'center', gap: 6, paddingVertical: 10, paddingHorizontal: 20, backgroundColor: 'rgba(255,255,255,0.05)', borderRadius: 12, borderWidth: 1, borderColor: 'rgba(255,255,255,0.1)' },
  navBtnDisabled: { opacity: 0.5, backgroundColor: 'transparent', borderColor: 'rgba(255,255,255,0.05)' },
  navBtnText: { color: 'white', fontWeight: '600', fontSize: 16 },
  pageNumber: { color: 'white', fontSize: 18, fontWeight: 'bold', minWidth: 40, textAlign: 'center' },
});