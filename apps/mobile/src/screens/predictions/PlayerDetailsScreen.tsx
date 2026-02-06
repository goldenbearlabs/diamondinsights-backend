import React from 'react';
import { View, Text, StyleSheet, Image, ScrollView, TouchableOpacity, KeyboardAvoidingView, Platform } from 'react-native';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { AttributeBar } from '../../predictionscomponents/AttributeBar'; 
import { theme } from '../../theme/colors';
import { TextInput, Alert, ActivityIndicator } from 'react-native';
import { useState, useEffect } from 'react';
import { getUserPrediction, saveUserPrediction } from '../../lib/api';
import { CardCommentsSection } from '../../components/predictions/CardCommentsSection';

const TWO_WAY_PLAYERS = [
  "Shohei Ohtani",
];

export default function PlayerDetailsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const card = params.cardData ? JSON.parse(params.cardData as string) : null;

  if (!card) return null;

  const isTwoWay = TWO_WAY_PLAYERS.includes(card.name);

  const showPitching = card.is_hitter === false || isTwoWay;
  const showBatting = card.is_hitter === true || isTwoWay;
 
  const [userPrediction, setUserPrediction] = useState<string>('');
  const [loadingPred, setLoadingPred] = useState(false);
  const [isSubmitted, setIsSubmitted] = useState(false);

  useEffect(() => {
    if (card?.id) {
      setLoadingPred(true);
      getUserPrediction(card.id)
        .then(res => {
          setUserPrediction(res.predicted_ovr.toString());
          setIsSubmitted(true);
        })
        .catch(() => {}) // Ignore 404s (no prediction yet)
        .finally(() => setLoadingPred(false));
    }
  }, [card?.id]);

  const handlePredict = async () => {
    const val = parseInt(userPrediction, 10);
    if (isNaN(val) || val < 0 || val > 99) {
      Alert.alert("Invalid Input", "Please enter a valid overall (0-99).");
      return;
    }
    try {
      setLoadingPred(true);
      await saveUserPrediction({ card_id: card.id, predicted_ovr: val });
      setIsSubmitted(true);
      Alert.alert("Success", "Your prediction has been saved!");
    } catch (e: any) {
      Alert.alert("Error", e.message || "Failed to save prediction");
    } finally {
      setLoadingPred(false);
    }
  };

  const handleInfoPress = () => {
    Alert.alert(
      "Prediction Info",
      "These are your personal predictions based on what overall you think this player will go up/down to after the next roster update. Your predictions will be scored after the next roster update.\n\nNote: All predictions are locked and finalized 48 hours before the next roster update.",
      [{ text: "Got it" }]
    );
  };

  return (
    <View style={styles.container}>
      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <SafeAreaView style={{ flex: 1 }} edges={['top']}>
        {/* HEADER: Back Button */}
        <View style={styles.navBar}>
          <TouchableOpacity 
            onPress={() => router.back()} 
            style={styles.backBtn}
          >
            <Ionicons name="arrow-back" size={24} color="white" />
            <Text style={styles.backText}>Back</Text>
          </TouchableOpacity>
        </View>

        <KeyboardAvoidingView 
          behavior={Platform.OS === 'ios' ? 'padding' : 'height'} 
          style={{ flex: 1 }}
          keyboardVerticalOffset={Platform.OS === 'ios' ? 10 : 0}
        >
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

          {/* COMMENTS SECTION */}
          <CardCommentsSection cardId={card.id} />

          {/* USER PREDICTION SECTION (New Location) */}
          <View style={[styles.glassCard, { marginTop: 16, padding: 16 }]}>
            {isSubmitted ? (
               // SUBMITTED STATE UI
              <View style={{ flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between' }}>
                <View style={{ flexDirection: 'row', alignItems: 'center', gap: 12 }}>
                  <Ionicons name="checkmark-circle" size={32} color="#22c55e" />
                  <View>
                    <Text style={{ color: 'white', fontSize: 16, fontWeight: 'bold' }}>
                      Prediction Submitted
                    </Text>
                    <Text style={{ color: theme.colors.muted, fontSize: 14 }}>
                      You predicted: <Text style={{ color: 'white', fontWeight: 'bold' }}>{userPrediction}</Text>
                    </Text>
                  </View>
                </View>

                <TouchableOpacity 
                  onPress={() => setIsSubmitted(false)}
                  style={{
                    paddingHorizontal: 12,
                    paddingVertical: 8,
                    backgroundColor: 'rgba(255,255,255,0.1)',
                    borderRadius: 8
                  }}
                >
                  <Text style={{ color: 'white', fontWeight: '600', fontSize: 12 }}>Change</Text>
                </TouchableOpacity>
              </View>
            ) : (
              // INPUT STATE UI
              <>
                {/* Header with Info Icon */}
                <View style={{ flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                  <Text style={{ color: 'white', fontWeight: 'bold', fontSize: 18 }}>
                    Your Prediction
                  </Text>
                  <TouchableOpacity onPress={handleInfoPress} hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}>
                    <Ionicons name="information-circle-outline" size={22} color={theme.colors.muted} />
                  </TouchableOpacity>
                </View>

                {/* Input and Button Row */}
                <View style={{ flexDirection: 'row', alignItems: 'center', gap: 12 }}>
                  <View style={{ 
                    backgroundColor: 'rgba(255,255,255,0.1)', 
                    borderRadius: 8, 
                    paddingHorizontal: 12,
                    height: 50,
                    justifyContent: 'center',
                    flex: 1
                  }}>
                    <TextInput
                      value={userPrediction}
                      onChangeText={setUserPrediction}
                      placeholder="Enter OVR (e.g. 88)"
                      placeholderTextColor="rgba(255,255,255,0.5)"
                      style={{ color: 'white', fontSize: 18, fontWeight: 'bold' }}
                      keyboardType="numeric"
                      maxLength={2}
                    />
                  </View>
                  
                  <TouchableOpacity
                    onPress={handlePredict}
                    disabled={loadingPred}
                    style={{
                      backgroundColor: theme.colors.primary,
                      height: 50,
                      borderRadius: 8,
                      paddingHorizontal: 20,
                      justifyContent: 'center',
                      alignItems: 'center',
                      opacity: loadingPred ? 0.7 : 1
                    }}
                  >
                    {loadingPred ? (
                      <ActivityIndicator color="white" />
                    ) : (
                      <Text style={{ color: 'white', fontWeight: 'bold' }}>Predict</Text>
                    )}
                  </TouchableOpacity>
                </View>
              </>
            )}
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
        </KeyboardAvoidingView>
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