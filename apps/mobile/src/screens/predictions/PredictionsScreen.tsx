import React, { useState, useEffect } from 'react';
import { 
  View, 
  Text, 
  StyleSheet, 
  TextInput, 
  TouchableOpacity, 
  FlatList, 
  Image,
  ActivityIndicator
} from 'react-native';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
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
};

// HELPER: Generate consistent fake prediction
const getFakePrediction = (baseOvr: number, id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = id.charCodeAt(i) + ((hash << 5) - hash);
  }
  const randomBoost = 1 + (Math.abs(hash) % 4); 
  return baseOvr + randomBoost;
};

// HELPER: Generate consistent fake social stats
const getFakeSocials = (id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = id.charCodeAt(i) + ((hash << 5) - hash);
  }
  return {
    likes: 10 + (Math.abs(hash) % 150),
    dislikes: Math.abs(hash) % 20,
    comments: 2 + (Math.abs(hash) % 40),
  };
};

export default function PredictionsScreen() {
  const [searchText, setSearchText] = useState('');
  const [cards, setCards] = useState<CardData[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchCards();
  }, []);

  const fetchCards = async () => {
    try {
      const data = await apiGet<CardData[]>('/cards?series=live&year=25'); 
      setCards(data);
    } catch (error) {
      console.error("Failed to fetch cards:", error);
    } finally {
      setLoading(false);
    }
  };

  const renderItem = ({ item }: { item: CardData }) => {
    const predictedOvr = getFakePrediction(item.ovr, item.id);
    const social = getFakeSocials(item.id);

    return (
      <TouchableOpacity style={styles.cardContainer} activeOpacity={0.7}>
        {/* Full Card Art */}
        <Image 
          source={{ uri: item.baked_img }} 
          style={styles.playerCardImage} 
          resizeMode="contain" 
        />

        {/* MIDDLE: Info */}
        <View style={styles.infoColumn}>
          <Text style={styles.playerName} numberOfLines={1}>{item.name}</Text>
          
          {/* SOCIAL ROW */}
          <View style={styles.teamRow}>
            <Text style={styles.teamName}>{item.team_short_name}</Text>
            
            <View style={styles.verticalDivider} />
            
            {/* Likes */}
            <View style={styles.socialItem}>
              <FontAwesome5 name="thumbs-up" size={10} color="#4ade80" solid />
              <Text style={styles.socialText}>{social.likes}</Text>
            </View>

            {/* Dislikes */}
            <View style={styles.socialItem}>
              <FontAwesome5 name="thumbs-down" size={10} color="#f87171" solid />
              <Text style={styles.socialText}>{social.dislikes}</Text>
            </View>

            {/* Comments */}
            <View style={styles.socialItem}>
              <FontAwesome5 name="comment-alt" size={10} color={theme.colors.muted} solid />
              <Text style={styles.socialText}>{social.comments}</Text>
            </View>
          </View>
          
          <View style={styles.ratingRow}>
            <View style={styles.ratingBadge}>
              <Text style={styles.ratingLabel}>CUR</Text>
              <Text style={styles.currentRating}>{item.ovr}</Text>
            </View>
            
            <Ionicons name="arrow-forward" size={14} color="#4ade80" style={{ marginHorizontal: 8 }} />
            
            <View style={[styles.ratingBadge, { borderColor: '#4ade80' }]}>
              <Text style={[styles.ratingLabel, { color: '#4ade80' }]}>PRED</Text>
              <Text style={styles.predictedRating}>{predictedOvr}</Text>
            </View>
          </View>
        </View>

        {/* 3. RIGHT: Chevron */}
        <View style={styles.arrowContainer}>
          <Ionicons name="chevron-forward" size={20} color={theme.colors.muted} />
        </View>
      </TouchableOpacity>
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

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
              />
            </View>
            <TouchableOpacity style={styles.filterBtn}>
              <Ionicons name="options" size={20} color="white" />
            </TouchableOpacity>
          </View>

          {loading ? (
            <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center' }}>
              <ActivityIndicator size="large" color="#fbbf24" />
            </View>
          ) : (
            <FlatList
              data={cards}
              keyExtractor={(item) => item.id}
              renderItem={renderItem}
              contentContainerStyle={styles.listContent}
              showsVerticalScrollIndicator={false}
              extraData={searchText} 
            />
          )}
        </View>
      </SafeAreaView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  backgroundLayer: {
    ...StyleSheet.absoluteFillObject,
    zIndex: -1,
  },
  content: {
    flex: 1,
    paddingHorizontal: 16,
    paddingTop: 20,
  },
  headerTitle: {
    fontSize: 28,
    fontWeight: '800',
    color: 'white',
    marginBottom: 16,
  },
  searchRow: {
    flexDirection: 'row',
    gap: 12,
    marginBottom: 20,
  },
  searchInputContainer: {
    flex: 1,
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    borderRadius: 12,
    paddingHorizontal: 12,
    height: 48,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
  searchInput: {
    flex: 1,
    color: 'white',
    fontSize: 16,
    fontWeight: '500',
  },
  filterBtn: {
    width: 48,
    height: 48,
    borderRadius: 12,
    backgroundColor: 'rgba(255, 255, 255, 0.05)',
    justifyContent: 'center',
    alignItems: 'center',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
  },
  
  listContent: {
    paddingBottom: 120,
    gap: 16, 
  },
  cardContainer: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(2, 6, 23, 0.6)', 
    borderRadius: 16,
    padding: 10,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.08)',
  },
  playerCardImage: {
    width: 50, 
    height: 70, 
    marginRight: 16,
    borderRadius: 4, 
  },
  infoColumn: {
    flex: 1,
    justifyContent: 'center',
  },
  playerName: {
    color: 'white',
    fontSize: 16,
    fontWeight: 'bold',
    marginBottom: 4,
  },
  
  // --- SOCIAL ROW STYLES ---
  teamRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: 8,
  },
  teamName: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
  },
  verticalDivider: {
    width: 1,
    height: 12,
    backgroundColor: 'rgba(255,255,255,0.2)',
    marginHorizontal: 8,
  },
  socialItem: {
    flexDirection: 'row',
    alignItems: 'center',
    marginRight: 10,
    gap: 4,
  },
  socialText: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
  },

  // --- RATING STYLES ---
  ratingRow: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  ratingBadge: {
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: 8,
    paddingVertical: 2,
    borderRadius: 6,
    backgroundColor: 'rgba(255,255,255,0.05)',
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.1)',
  },
  ratingLabel: {
    fontSize: 8,
    color: theme.colors.muted,
    fontWeight: '800',
    marginBottom: 1,
  },
  currentRating: {
    color: 'white',
    fontWeight: '700',
    fontSize: 14,
  },
  predictedRating: {
    color: '#4ade80',
    fontWeight: '800',
    fontSize: 14,
  },
  arrowContainer: {
    paddingLeft: 10,
  }
});