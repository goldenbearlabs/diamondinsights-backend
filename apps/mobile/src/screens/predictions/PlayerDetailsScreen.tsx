import React from 'react';
import { View, Text, StyleSheet, Image, ScrollView, TouchableOpacity } from 'react-native';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { AttributeBar } from '../../predictionscomponents/AttributeBar'; 
import { theme } from '../../theme/colors';

// 1. HARDCODED LIST OF TWO-WAY PLAYERS
const TWO_WAY_PLAYERS = [
  "Shohei Ohtani",
  "Babe Ruth", // Just in case you add legends later!
];

export default function PlayerDetailsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const card = params.cardData ? JSON.parse(params.cardData as string) : null;

  if (!card) return null;

  const isTwoWay = TWO_WAY_PLAYERS.includes(card.name);

  
  const showPitching = card.is_hitter === false || isTwoWay;
  const showBatting = card.is_hitter === true || isTwoWay;

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <SafeAreaView style={{ flex: 1 }} edges={['top']}>
        {/* HEADER: Back Button */}
        <View style={styles.navBar}>
          <TouchableOpacity 
            onPress={() => router.replace('/predictions')} 
            style={styles.backBtn}
          >
            <Ionicons name="arrow-back" size={24} color="white" />
            <Text style={styles.backText}>Back</Text>
          </TouchableOpacity>
        </View>

        <ScrollView contentContainerStyle={styles.scrollContent}>
          
          {/* bio section */}
          <View style={styles.glassCard}>
            <View style={styles.topRow}>
              <Image 
                source={{ uri: card.baked_img }} 
                style={styles.cardArt} 
                resizeMode="contain" 
              />
              <View style={styles.bioColumn}>
                <Text style={styles.playerName}>{card.name}</Text>
                <Text style={styles.teamText}>
                  {card.team_short_name} • {card.display_position} • Age: {card.age}
                </Text>
                <Text style={styles.teamText}>Throws: {card.throw_hand} • Bats: {card.bat_hand}</Text>
                <View style={styles.divider} />
                <View style={styles.statBadge}>
                  <Text style={styles.statLabel}>OVERALL</Text>
                  <Text style={styles.statValue}>{card.ovr}</Text>
                </View>
              </View>
            </View>
          </View>

          {/* attribute section */}
          <Text style={styles.sectionTitle}>Attributes</Text>
          <View style={styles.glassCard}>
            
            {/* pitching attributes */}
            {showPitching && (
              <>
                <Text style={styles.subHeader}>Pitching</Text>
                <AttributeBar label="Stamina" value={card.stamina || 0} />
                <AttributeBar label="Pitching Clutch" value={card.pitching_clutch || 0} />
                <AttributeBar label="H/9" value={card.hits_per_bf || 0} /> 
                <AttributeBar label="K/9" value={card.k_per_bf || 0} />
                <AttributeBar label="BB/9" value={card.bb_per_bf || 0} />
                <AttributeBar label="HR/9" value={card.hr_per_bf || 0} />
                {/* Add a spacer if we are about to show batting stats below */}
                {showBatting && <View style={{ height: 24 }} />}
              </>
            )}

            {/* batting */}
            {showBatting && (
               <>
                <Text style={styles.subHeader}>Batting</Text>
                <AttributeBar label="Contact R" value={card.contact_right || 0} />
                <AttributeBar label="Contact L" value={card.contact_left || 0} />
                <AttributeBar label="Power R" value={card.power_right || 0} />
                <AttributeBar label="Power L" value={card.power_left || 0} />
                <AttributeBar label="Vision" value={card.plate_vision || 0} />
                <AttributeBar label="Clutch" value={card.batting_clutch || 0} />
                <View style={{ height: 16 }} />
              </>
            )}

            {/* Fielding  */}
             <Text style={styles.subHeader}>Fielding</Text>
             <AttributeBar label="Fielding" value={card.fielding_ability || 0} />
             <AttributeBar label="Arm Strength" value={card.arm_strength || 0} />
             <AttributeBar label="Reaction" value={card.reaction_time || 0} />

          </View>

        </ScrollView>
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
  scrollContent: { paddingHorizontal: 16, paddingBottom: 50 },
  glassCard: {
    backgroundColor: 'rgba(2, 6, 23, 0.7)',
    borderRadius: 16,
    padding: 16,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    marginBottom: 24,
  },
  topRow: { flexDirection: 'row', gap: 16 },
  cardArt: { width: 130, height: 182, borderRadius: 8 },
  bioColumn: { flex: 1, justifyContent: 'center' },
  playerName: { color: 'white', fontSize: 22, fontWeight: 'bold', marginBottom: 4 },
  teamText: { color: theme.colors.muted, fontSize: 14, fontWeight: '600' },
  divider: { height: 1, backgroundColor: 'rgba(255,255,255,0.1)', marginVertical: 12 },
  statBadge: { 
    backgroundColor: 'rgba(59, 130, 246, 0.15)',
    padding: 10, borderRadius: 8, alignItems: 'center', alignSelf: 'flex-start', borderWidth: 1, borderColor: '#3b82f6'
  },
  statLabel: { color: '#3b82f6', fontSize: 10, fontWeight: 'bold' },
  statValue: { color: '#3b82f6', fontSize: 24, fontWeight: '900' },
  sectionTitle: { color: 'white', fontSize: 20, fontWeight: 'bold', marginBottom: 12 },
  subHeader: { color: '#3b82f6', fontSize: 14, fontWeight: '700', marginBottom: 12, textTransform: 'uppercase' },
});