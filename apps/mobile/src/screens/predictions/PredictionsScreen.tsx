import React, { useState, useEffect } from 'react';
import { 
  View, 
  Text, 
  StyleSheet, 
  TextInput, 
  TouchableOpacity, 
  FlatList, 
  Image,
  ActivityIndicator,
  RefreshControl
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { useRouter } from 'expo-router';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { theme } from '../../theme/colors';
import { apiGet } from '../../lib/api'; 

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
};

// HELPERS (Same as before)
const getFakePrediction = (baseOvr: number, id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) { hash = id.charCodeAt(i) + ((hash << 5) - hash); }
  return baseOvr + (1 + (Math.abs(hash) % 4));
};

const getFakeSocials = (id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) { hash = id.charCodeAt(i) + ((hash << 5) - hash); }
  return { likes: 10 + (Math.abs(hash) % 150), dislikes: Math.abs(hash) % 20, comments: 2 + (Math.abs(hash) % 40) };
};

export default function PredictionsScreen() {
  const router = useRouter();
  
  
  const [cards, setCards] = useState<CardData[]>([]);
  const [searchText, setSearchText] = useState('');
  
  
  const [loading, setLoading] = useState(false);
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(25); 
  const [hasMore, setHasMore] = useState(true); 
  const [refreshing, setRefreshing] = useState(false);

  // DEBOUNCE SEARCH (Reset to Page 1)
  useEffect(() => {
    const delayDebounceFn = setTimeout(() => {
      setPage(1); // Reset to page 1 when search changes
      loadCards(1, limit, searchText);
    }, 500);
    return () => clearTimeout(delayDebounceFn);
  }, [searchText]);

 
  // skip the first run because the search effect above handles initial load
  useEffect(() => {
    loadCards(page, limit, searchText);
  }, [page, limit]);

  
  const loadCards = async (targetPage: number, targetLimit: number, query: string) => {
    setLoading(true);
    try {
      const offset = (targetPage - 1) * targetLimit;
      
      let url = `/cards?series=live&year=25&offset=${offset}&limit=${targetLimit}`;
      if (query.trim().length > 0) {
        url += `&name=${encodeURIComponent(query)}`;
      }

      const newCards = await apiGet<CardData[]>(url); 
      setCards(newCards);

      
      setHasMore(newCards.length === targetLimit);

    } catch (error) {
      console.error("Failed to fetch cards:", error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  const handleRefresh = () => {
    setRefreshing(true);
    setPage(1);
    loadCards(1, limit, searchText);
  };

  const renderItem = ({ item }: { item: CardData }) => {
    const predictedOvr = getFakePrediction(item.ovr, item.id);
    const social = getFakeSocials(item.id);

    return (
      <TouchableOpacity 
        style={styles.cardContainer} 
        activeOpacity={0.7}
        onPress={() => router.push({ pathname: "/predictions/[id]", params: { id: item.id, cardData: JSON.stringify(item) } })}
      >
        <Image source={{ uri: item.baked_img }} style={styles.playerCardImage} resizeMode="contain" />
        <View style={styles.infoColumn}>
          <Text style={styles.playerName} numberOfLines={1}>{item.name}</Text>
          <View style={styles.teamRow}>
            <Text style={styles.teamName}>{item.team_short_name}</Text>
            <View style={styles.verticalDivider} />
            <View style={styles.socialItem}><FontAwesome5 name="thumbs-up" size={10} color="#4ade80" solid /><Text style={styles.socialText}>{social.likes}</Text></View>
            <View style={styles.socialItem}><FontAwesome5 name="thumbs-down" size={10} color="#f87171" solid /><Text style={styles.socialText}>{social.dislikes}</Text></View>
            <View style={styles.socialItem}><FontAwesome5 name="comment-alt" size={10} color={theme.colors.muted} solid /><Text style={styles.socialText}>{social.comments}</Text></View>
          </View>
          <View style={styles.ratingRow}>
            <View style={styles.ratingBadge}><Text style={styles.ratingLabel}>CUR</Text><Text style={styles.currentRating}>{item.ovr}</Text></View>
            <Ionicons name="arrow-forward" size={14} color="#4ade80" style={{ marginHorizontal: 8 }} />
            <View style={[styles.ratingBadge, { borderColor: '#4ade80' }]}><Text style={[styles.ratingLabel, { color: '#4ade80' }]}>PRED</Text><Text style={styles.predictedRating}>{predictedOvr}</Text></View>
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
      <SafeAreaView style={{ flex: 1 }} edges={['top']}>
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
            <TouchableOpacity style={styles.filterBtn}>
              <Ionicons name="options" size={20} color="white" />
            </TouchableOpacity>
          </View>

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
              ListEmptyComponent={<Text style={styles.emptyText}>No players found.</Text>}
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
  content: { flex: 1, paddingHorizontal: 16, paddingTop: 20 },
  headerTitle: { fontSize: 28, fontWeight: '800', color: 'white', marginBottom: 16 },
  searchRow: { flexDirection: 'row', gap: 12, marginBottom: 20 },
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
  predictedRating: { color: '#4ade80', fontWeight: '800', fontSize: 14 },
  arrowContainer: { paddingLeft: 10 },

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
});