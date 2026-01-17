// apps/mobile/src/screens/home/HomeScreen.tsx

import { useEffect, useState } from "react";
import { Image, StyleSheet, Text, View, TouchableOpacity, ScrollView } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { apiGet } from "../../lib/api";
import { theme } from "../../theme/colors";

type Card = {
  id: string;
  name: string;
  img: string;
};

export default function HomeScreen() {
  const [card, setCard] = useState<Card | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let isMounted = true;
    apiGet<Card[]>("/cards?limit=1")
      .then((cards) => {
        if (!isMounted) return;
        setCard(cards[0] ?? null);
      })
      .catch((err: Error) => {
        if (!isMounted) return;
        setError(err.message);
      });
    return () => {
      isMounted = false;
    };
  }, []);

  return (
    <View style={styles.container}>
      {/* Background Blobs */}
      <View style={styles.backgroundLayer}>
        <View style={[styles.blob, { top: -80, left: -60, backgroundColor: theme.colors.blobs.left }]} />
        <View style={[styles.blob, { top: 80, right: -40, backgroundColor: theme.colors.blobs.right }]} />
      </View>

      <SafeAreaView style={{ flex: 1 }}>
        <ScrollView contentContainerStyle={styles.content}>
          
          {/* Header */}
          <View style={styles.headerRow}>
            <Image
              // Note: We go up 3 levels now (../../../) to get back to root assets
              source={require("../../../assets/images/placeholder.png")}
              style={styles.logo}
              resizeMode="cover"
            />
            <View>
              <Text style={styles.titleDiamond}>Diamond</Text>
              <Text style={styles.titleInsights}>Insights</Text>
            </View>
          </View>

          {/* Subheader */}
          <Text style={styles.subtitle}>
            View Cards, Track Investments, & Improve Your Game.
          </Text>

          {/* Buttons */}
          <View style={styles.buttonGroup}>
            <TouchableOpacity style={styles.btnPrimary} onPress={() => {}}>
               <Image 
                source={require("../../../assets/images/stub.png")} 
                style={styles.btnIcon}
              />
              <Text style={styles.btnTextWhite}>Get Started</Text>
            </TouchableOpacity>

            <TouchableOpacity style={styles.btnSecondary} onPress={() => {}}>
              <Text style={styles.btnTextBlue}>Explore Cards</Text>
            </TouchableOpacity>
          </View>

          {/* Card Panel */}
          <View style={styles.cardPanel}>
            {error ? (
              <Text style={styles.errorText}>{error}</Text>
            ) : card ? (
              <>
                <Text style={styles.cardTitle}>{card.name}</Text>
                <Image
                  source={{ uri: card.img }}
                  style={styles.cardImage}
                  resizeMode="contain"
                />
              </>
            ) : (
              <Text style={styles.loadingText}>Loading card...</Text>
            )}
          </View>

        </ScrollView>
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
    overflow: 'hidden',
  },
  blob: {
    position: 'absolute',
    width: 200,
    height: 200,
    borderRadius: 100,
  },
  content: {
    padding: theme.spacing.l,
  },
  headerRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginBottom: theme.spacing.xl,
    marginTop: theme.spacing.m,
  },
  logo: {
    width: 80,
    height: 80,
    borderRadius: 16,
    marginRight: 16,
  },
  titleDiamond: {
    fontSize: 36,
    fontWeight: 'bold',
    color: theme.colors.text,
    lineHeight: 40,
  },
  titleInsights: {
    fontSize: 36,
    fontWeight: 'bold',
    color: theme.colors.primary,
    lineHeight: 40,
  },
  subtitle: {
    fontSize: 18,
    color: theme.colors.muted,
    marginBottom: theme.spacing.xl,
    fontWeight: '500',
  },
  buttonGroup: {
    gap: 16,
  },
  btnPrimary: {
    backgroundColor: theme.colors.primary,
    paddingVertical: 16,
    paddingHorizontal: 24,
    borderRadius: 12,
    flexDirection: 'row',
    justifyContent: 'center',
    alignItems: 'center',
  },
  btnSecondary: {
    borderWidth: 1,
    borderColor: theme.colors.border,
    paddingVertical: 16,
    paddingHorizontal: 24,
    borderRadius: 12,
    justifyContent: 'center',
    alignItems: 'center',
  },
  btnIcon: {
    width: 20,
    height: 20,
    marginRight: 8,
    tintColor: 'white'
  },
  btnTextWhite: {
    color: 'white',
    fontSize: 18,
    fontWeight: 'bold',
  },
  btnTextBlue: {
    color: '#60a5fa',
    fontSize: 18,
    fontWeight: 'bold',
  },
  cardPanel: {
    marginTop: 40,
    padding: 24,
    backgroundColor: theme.colors.card,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: theme.colors.border,
    alignItems: 'center',
  },
  cardTitle: {
    color: 'white',
    fontSize: 20,
    fontWeight: 'bold',
    marginBottom: 16,
    textAlign: 'center',
  },
  cardImage: {
    width: 220,
    height: 320,
    borderRadius: 8,
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 16,
  },
  loadingText: {
    color: theme.colors.muted,
    fontStyle: 'italic',
  }
});