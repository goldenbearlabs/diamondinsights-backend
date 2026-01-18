import React, { useEffect, useState, useRef } from 'react';
import { View, Text, Image, Dimensions, StyleSheet, Animated } from 'react-native';
import { apiGet } from '../lib/api';
import { theme } from '../theme/colors';

const { width } = Dimensions.get('window');


const CARD_WIDTH = width * 0.45; 
const SPACING = 10;
const SNAP_INTERVAL = CARD_WIDTH + SPACING * 2;
const SPACER_WIDTH = (width - SNAP_INTERVAL) / 2;

type CardData = {
  id: string;
  name: string;
  img: string;
  ovr: number;
};


const getFakePrediction = (baseOvr: number, id: string) => {
  let hash = 0;
  for (let i = 0; i < id.length; i++) {
    hash = id.charCodeAt(i) + ((hash << 5) - hash);
  }
  
  
  const randomBoost = 1 + (Math.abs(hash) % 300) / 100; 
  
  return (baseOvr + randomBoost - 1.0).toFixed(2); 
};

export const PredictionCarousel = () => {
  const [cards, setCards] = useState<CardData[]>([]);
  const scrollX = useRef(new Animated.Value(0)).current;

  useEffect(() => {
    // API Call: Live Series, Diamond, 2025, Limit 6
    apiGet<CardData[]>('/cards?series=live&rarity=diamond&year=25&limit=6')
      .then((data) => setCards(data))
      .catch((err) => console.error(err));
  }, []);

  if (cards.length === 0) return null;

  return (
    <View style={styles.container}>
      
      <Animated.FlatList
        data={cards}
        keyExtractor={(item) => item.id}
        horizontal
        showsHorizontalScrollIndicator={false}
        snapToInterval={SNAP_INTERVAL}
        decelerationRate="fast"
        style={{ flexGrow: 0 }}
        contentContainerStyle={{
          paddingHorizontal: SPACER_WIDTH - 20, 
        }}
        onScroll={Animated.event(
          [{ nativeEvent: { contentOffset: { x: scrollX } } }],
          { useNativeDriver: true }
        )}
        renderItem={({ item, index }) => {
          const inputRange = [
            (index - 1) * SNAP_INTERVAL,
            index * SNAP_INTERVAL,
            (index + 1) * SNAP_INTERVAL,
          ];

          const scale = scrollX.interpolate({
            inputRange,
            outputRange: [0.85, 1, 0.85],
            extrapolate: 'clamp',
          });

          const opacity = scrollX.interpolate({
            inputRange,
            outputRange: [0.5, 1, 0.5],
            extrapolate: 'clamp',
          });

          
          const predictedOvr = getFakePrediction(item.ovr, item.id);

          return (
            <Animated.View style={[styles.cardWrapper, { transform: [{ scale }], opacity }]}>
              <Image source={{ uri: item.img }} style={styles.cardImage} resizeMode="contain" />
              
              <View style={styles.predictionPill}>
                <View style={styles.scoreRow}>
                    <Text style={styles.currentScore}>{item.ovr}</Text>
                    <Text style={styles.arrow}>➔</Text>
                    <Text style={styles.predictedScore}>{predictedOvr}</Text>
                </View>
                <Text style={styles.fakeLabel}>AI PREDICTION EXAMPLE</Text>
              </View>
            </Animated.View>
          );
        }}
      />
      <Text style={styles.disclaimer}>*Get Pro to see all live market predictions + more!</Text>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    marginTop: 10, 
    marginHorizontal: -20, 
    marginBottom: 10,
  },
  cardWrapper: {
    width: CARD_WIDTH,
    marginHorizontal: SPACING,
    alignItems: 'center',
  },
  cardImage: {
    width: '100%',
    height: CARD_WIDTH * 1.15, 
    marginBottom: 12, 
    zIndex: 2,
  },
  predictionPill: {
    backgroundColor: 'rgba(2, 6, 23, 0.95)',
    width: '95%',
    paddingVertical: 10,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.1)',
    alignItems: 'center',
    zIndex: 1,
  },
  scoreRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
    marginBottom: 2,
  },
  currentScore: {
    fontSize: 18,
    fontWeight: 'bold',
    color: theme.colors.muted,
  },
  arrow: {
    fontSize: 16,
    color: '#22c55e',
    fontWeight: '900',
  },
  predictedScore: {
    fontSize: 22,
    fontWeight: '900',
    color: '#22c55e',
  },
  fakeLabel: {
    fontSize: 9,
    color: '#fbbf24',
    fontWeight: 'bold',
    letterSpacing: 0.5,
  },
  disclaimer: {
    textAlign: 'center',
    color: theme.colors.muted,
    fontSize: 10,
    fontStyle: 'italic',
    marginTop: 12,
    opacity: 0.6,
  }
});