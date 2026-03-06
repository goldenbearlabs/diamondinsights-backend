import { useEffect, useState } from "react";
import { StyleSheet, Text, View, TouchableOpacity, ScrollView, Linking} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useRouter } from "expo-router"; // <-- 1. Added router import
import { apiGet } from "../../lib/api";
import { theme } from "../../theme/colors";
import { FloatingBackground } from "../../homescreencomponents/FloatingBackground";
import { PredictionCarousel } from "../../homescreencomponents/PredictionCarousel";
import { TrustStats } from "../../homescreencomponents/TrustStats"; 
import { HowItWorks } from "../../homescreencomponents/HowItWorks"; 
import { ContactCard } from "../../homescreencomponents/ContactCard";

const RosterCountdown = () => {
  const [timeLeft, setTimeLeft] = useState({ d: 4, h: 12, m: 30, s: 0 });
  useEffect(() => {
    const timer = setInterval(() => {
      setTimeLeft((prev) => {
        if (prev.s > 0) return { ...prev, s: prev.s - 1 };
        return { ...prev, s: 59 }; 
      });
    }, 1000);
    return () => clearInterval(timer);
  }, []);

  return (
    <View style={styles.countdownPill}>
      <Text style={styles.countdownLabel}>NEXT UPDATE:</Text>
      <View style={styles.timerRow}>
        <Text style={styles.timeText}>{timeLeft.d}d {timeLeft.h}h {timeLeft.m}m {timeLeft.s}s</Text>
      </View>
    </View>
  );
};

export default function HomeScreen() {
  const router = useRouter(); 

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <SafeAreaView style={{ flex: 1 }} edges={["left", "right"]}>

        <ScrollView 
          contentContainerStyle={styles.scrollContent} 
          showsVerticalScrollIndicator={false}
        >
          {/* THE GLASS CARD */}
          <View style={styles.mainCard}>
            <View 
            style={styles.headerContainer}>
              <Text 
              style={styles.titleMain}
              numberOfLines={1}
              adjustsFontSizeToFit
              >
                Diamond<Text style={styles.titleHighlight}>Insights</Text>
              </Text>
              <Text style={styles.subtitle}>
                The <Text style={styles.goldText}>#1</Text> App for dominating   MLB The Show
              </Text>
            </View>

            <View style={{ marginBottom: 24 }}>
              <RosterCountdown />
            </View>

            <View style={styles.buttonGroup}>
              <TouchableOpacity style={styles.btnPrimary} onPress={() => router.push('/(app)/portfolio')}>
                <Text style={styles.btnTextWhite}>Create Investment Portfolio</Text>
                
              </TouchableOpacity>
              <TouchableOpacity style={styles.btnSecondary} onPress={() => router.push('/(app)/cards')}>
                <Text style={styles.btnTextSecondary}>View All Cards</Text>
              </TouchableOpacity>
            </View>
            <PredictionCarousel />
          </View>

          <View style={[styles.mainCard, { marginTop: 24 }]}>
          <TrustStats />
          <HowItWorks />
          </View>

          <ContactCard />

          
        </ScrollView>
      </SafeAreaView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { 
    flex: 1, 
    backgroundColor: theme.colors.background 
  },
  backgroundLayer: { 
    ...StyleSheet.absoluteFillObject 
  },
  
 
  scrollContent: {
    paddingTop: theme.spacing.l,
    paddingHorizontal: 12,
    paddingBottom: 0,
  },
  

  mainCard: {
    width: '100%',
    backgroundColor: 'rgba(2, 6, 23, 0.7)',
    borderRadius: 24,
    paddingTop: 32,
    paddingBottom: 20, 
    paddingHorizontal: 16,
    alignItems: 'center',
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 10 },
    shadowOpacity: 0.3,
    shadowRadius: 20,
  },
  headerContainer: { marginBottom: 24, alignItems: 'center' },
  titleMain: { fontSize: 38, fontWeight: '800', color: theme.colors.text, textAlign: 'center' },
  titleHighlight: { color: theme.colors.primary },
  subtitle: { marginTop: theme.spacing.m, fontSize: 22, color: theme.colors.muted, textAlign: 'center', fontWeight: '600' },
  goldText: { color: '#fbbf24', fontWeight: '900', fontSize: 34 },
  countdownPill: { flexDirection: 'row', alignItems: 'center', backgroundColor: 'rgba(0, 0, 0, 0.4)', paddingVertical: 8, paddingHorizontal: 16, borderRadius: 20, borderWidth: 1, borderColor: 'rgba(255,255,255,0.05)' },
  countdownLabel: { color: theme.colors.muted, fontSize: 12, fontWeight: '700', marginRight: 8 },
  timeText: { color: '#fbbf24', fontSize: 14, fontWeight: 'bold', fontVariant: ['tabular-nums'] },
  buttonGroup: { width: '100%', gap: 16 },
  btnPrimary: { backgroundColor: theme.colors.primary, paddingVertical: 18, borderRadius: 14, flexDirection: 'row', justifyContent: 'center', alignItems: 'center', shadowColor: theme.colors.primary, shadowOffset: { width: 0, height: 4 }, shadowOpacity: 0.3, shadowRadius: 8 },
  btnSecondary: { paddingVertical: 18, borderRadius: 14, justifyContent: 'center', alignItems: 'center', backgroundColor: 'rgba(255, 255, 255, 0.05)', borderWidth: 1, borderColor: 'rgba(255, 255, 255, 0.1)' },
  btnTextWhite: { color: 'white', fontSize: 18, fontWeight: 'bold' },
  btnTextSecondary: { color: 'white', fontSize: 18, fontWeight: '600' },
  timerRow: {
    flexDirection: 'row',
    alignItems: 'center',
  },
});