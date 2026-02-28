import React from 'react';
import { View, Text, StyleSheet, ScrollView, TouchableOpacity, KeyboardAvoidingView, Platform, DeviceEventEmitter} from 'react-native';
import { Image } from 'expo-image';
import { useLocalSearchParams, useRouter } from 'expo-router';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons, FontAwesome5 } from '@expo/vector-icons';
import { FloatingBackground } from '../../homescreencomponents/FloatingBackground';
import { AttributeBar } from '../../predictionscomponents/AttributeBar';
import PredictionAttributeBar from '../../predictionscomponents/PredictionAttributeBar';
import { theme } from '../../theme/colors';
import { TextInput, Alert, ActivityIndicator } from 'react-native';
import { useState, useEffect } from 'react';
import Svg, { Path, G } from 'react-native-svg';
import { apiGet, getUserPrediction, saveUserPrediction } from '../../lib/api';
import { useBackendProStatus } from '../../lib/proStatus';
import { CardCommentsSection } from '../../components/predictions/CardCommentsSection';
import { MarketSpreadChart } from '../../components/playerdetails/MarketSpreadChart';
import { MarketVolumeChart } from '../../components/playerdetails/MarketVolumeChart';

const TWO_WAY_PLAYERS = [
  "Shohei Ohtani",
];

const STUB_ICON = require('../../../assets/images/stub.png');

const BATTING_PREDICTION_KEYS = [
  { key: 'CON_R', label: 'Contact R' },
  { key: 'CON_L', label: 'Contact L' },
  { key: 'POW_R', label: 'Power R' },
  { key: 'POW_L', label: 'Power L' },
  { key: 'VIS', label: 'Vision' },
  { key: 'CLT', label: 'Clutch' },
];

const PITCHING_PREDICTION_KEYS = [
  { key: 'STA', label: 'Stamina' },
  { key: 'PCLT', label: 'Clutch' },
  { key: 'H_9', label: 'H/9' },
  { key: 'K_9', label: 'K/9' },
  { key: 'BB_9', label: 'BB/9' },
];

export default function PlayerDetailsScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const { isPro, loading: proStatusLoading } = useBackendProStatus();
  const card = params.cardData ? JSON.parse(params.cardData as string) : null;

  if (!card) return null;

  const isTwoWay = TWO_WAY_PLAYERS.includes(card.name);

  const showPitching = card.is_hitter === false || isTwoWay;
  const showBatting = card.is_hitter === true || isTwoWay;

  const BATTING_COLOR = '#3b82f6';
  const PITCHING_COLOR = '#fbbf24';
  const FIELDING_COLOR = '#22c55e';
  const RUNNING_COLOR = '#A78BFA';
 
  const [userPrediction, setUserPrediction] = useState<string>('');
  const [loadingPred, setLoadingPred] = useState(false);
  const [isSubmitted, setIsSubmitted] = useState(false);
  const [activeTab, setActiveTab] = useState<'attributes' | 'market' | 'stats' | 'pro'>('attributes');
  const [activeAttrTab, setActiveAttrTab] = useState<'attributes' | 'quirks' | 'pitches'>('attributes');
  
  const [buyPrice, setBuyPrice] = useState<number | null>(null);
  const [sellPrice, setSellPrice] = useState<number | null>(null);
  const [buyVolume, setBuyVolume] = useState<number | null>(null);
  const [sellVolume, setSellVolume] = useState<number | null>(null);
  // 2. New State for Candles
  const [marketCandles, setMarketCandles] = useState<any[]>([]);
  const [loadingMarket, setLoadingMarket] = useState(false);

  type CardQuirk = { card_id: string; name: string; description: string; img: string };
  const [quirks, setQuirks] = useState<CardQuirk[]>([]);
  const [loadingQuirks, setLoadingQuirks] = useState(false);

  type Pitch = { card_id: string; name: string; speed: number; control: number; movement: number };
  const [pitches, setPitches] = useState<Pitch[]>([]);
  const [loadingPitches, setLoadingPitches] = useState(false);

  type SplitStats = { split: string; [key: string]: any };
  type SeasonStats = {
    is_hitter: boolean;
    season: number;
    batting?: { overall: SplitStats; splits: SplitStats[] } | null;
    pitching?: { overall: SplitStats; splits: SplitStats[] } | null;
  };
  const [seasonStats, setSeasonStats] = useState<SeasonStats | null>(null);
  const [loadingStats, setLoadingStats] = useState(false);
  const [activeWindow, setActiveWindow] = useState<'season' | '7d' | '14d' | 'last_update'>('season');
  const showProStatusPending = isPro === null && proStatusLoading;
  const showProLock = isPro === false || (isPro === null && !proStatusLoading);
  const canAccessLastUpdateWindow = isPro === true;

  useEffect(() => {
    if (card?.id) {
      setLoadingPred(true);
      getUserPrediction(card.id)
        .then(res => {
          setUserPrediction(res.predicted_ovr.toString());
          setIsSubmitted(true);
        })
        .catch(() => {}) 
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
      DeviceEventEmitter.emit('PredictionUpdated', { cardId: card.id, newPrediction: val });
      Alert.alert("Success", "Your prediction has been saved!");
    } catch (e: any) {
      Alert.alert("Error", e.message || "Failed to save prediction");
    } finally {
      setLoadingPred(false);
    }
  };

  useEffect(() => {
    const fetchMarket = async () => {
      if (!card?.id) return;
      setLoadingMarket(true);

      try {
        const [buyRes, sellRes, candlesRes, volumeRes] = await Promise.all([
          apiGet<any[]>(`/completed_orders/latest?card_id=${card.id}&is_buy=true&limit=1`),
          apiGet<any[]>(`/completed_orders/latest?card_id=${card.id}&is_buy=false&limit=1`),
          apiGet<any[]>(`/completed_orders/${card.id}/history?limit=500`),
          apiGet<any[]>(`/market_candles/?card_id=${card.id}&series=live&limit=1`)
        ]);

        setBuyPrice(buyRes?.[0]?.price ?? null);
        setSellPrice(sellRes?.[0]?.price ?? null);
        setMarketCandles(candlesRes || []);
        setBuyVolume(volumeRes?.[0]?.buy_volume ?? null);
        setSellVolume(volumeRes?.[0]?.sell_volume ?? null);
      } catch (err) {
        setBuyPrice(null);
        setSellPrice(null);
        setBuyVolume(null);
        setSellVolume(null);
        setMarketCandles([]);
      } finally {
        setLoadingMarket(false);
      }
    };

    fetchMarket();
  }, [card?.id]);

  useEffect(() => {
    if (!card?.id) return;
    setLoadingQuirks(true);
    apiGet<{ card_id: string; name: string; description: string; img: string }[]>(`/quirks/${card.id}`)
      .then(res => setQuirks(res))
      .catch(() => setQuirks([]))
      .finally(() => setLoadingQuirks(false));
  }, [card?.id]);

  useEffect(() => {
    if (!card?.id) return;
    setLoadingStats(true);
    const effectiveWindow =
      activeWindow === 'last_update' && !canAccessLastUpdateWindow ? 'season' : activeWindow;
    const windowParam = effectiveWindow !== 'season' ? `&window=${effectiveWindow}` : '';
    apiGet<SeasonStats>(`/mlb_stats/season/${card.id}?season=2025${windowParam}`)
      .then(res => setSeasonStats(res))
      .catch(() => setSeasonStats(null))
      .finally(() => setLoadingStats(false));
  }, [card?.id, activeWindow, canAccessLastUpdateWindow]);

  useEffect(() => {
    if (activeWindow === 'last_update' && !canAccessLastUpdateWindow) {
      setActiveWindow('season');
    }
  }, [activeWindow, canAccessLastUpdateWindow]);

  useEffect(() => {
    if (!card?.id) return;
    // reset inner attributes tab when card changes
    setActiveAttrTab('attributes');
    setLoadingPitches(true);
    apiGet<Pitch[]>(`/pitches/${card.id}`)
      .then(res => setPitches(res))
      .catch(() => setPitches([]))
      .finally(() => setLoadingPitches(false));
  }, [card?.id]);

  const SPLIT_LABELS: Record<string, string> = {
    vslhp: 'vs LHP', vsrhp: 'vs RHP', vslhb: 'vs LHB', vsrhb: 'vs RHB', risp: 'RISP', overall: 'Overall',
  };

  const formatStat = (val: number | undefined, decimals: number = 3): string => {
    if (val == null) return '-';
    if (decimals === 3) return val.toFixed(3).replace(/^0/, '');
    return val.toFixed(decimals);
  };

  const PitchGauge = ({ value, color, label }: { value: number; color: string; label: string }) => {
    const radius = 36;
    const strokeWidth = 10;
    const padding = Math.ceil(strokeWidth / 2) + 2;
    const cx = radius + padding;
    const cy = radius + padding;
    const width = cx * 2;
    const height = cy + 4;
    const max = 99;
    const clamped = Math.max(0, Math.min(value, max));
    const pct = clamped / max;

    const arcLen = Math.PI * radius; // length of semicircle
    const path = `M ${cx - radius} ${cy} A ${radius} ${radius} 0 0 1 ${cx + radius} ${cy}`;

    return (
      <View style={{ width: width, alignItems: 'center' }}>
        <Svg width={width} height={height}>
          <Path d={path} stroke="rgba(255,255,255,0.08)" strokeWidth={strokeWidth} fill="none" strokeLinecap="round" />
          <Path
            d={path}
            stroke={color}
            strokeWidth={strokeWidth}
            fill="none"
            strokeLinecap="round"
            strokeDasharray={`${arcLen} ${arcLen}`}
            strokeDashoffset={arcLen * (1 - pct)}
          />
        </Svg>
        <Text style={{ color: 'white', fontWeight: '700', marginTop: 8 }}>{label}</Text>
        <Text style={{ color: theme.colors.muted, fontSize: 12 }}>{value}</Text>
      </View>
    );
  };

  const handleInfoPress = () => {
    Alert.alert(
      "Prediction Info",
      "These are your personal predictions based on what overall you think this player will go up/down to after the next roster update. Your predictions will be scored after the next roster update. Check out the Leaderboard to see how you rank!\n\nNote: All predictions are locked and finalized 48 hours before the next roster update.",
      [{ text: "Got it" }]
    );
  };

  const handleLastUpdateWindowPress = () => {
    if (canAccessLastUpdateWindow) {
      setActiveWindow('last_update');
      return;
    }

    Alert.alert(
      "Pro Feature",
      "Since Last Roster Update stats are available for Pro users.",
      [
        { text: "Not now", style: "cancel" },
        { text: "Go Pro", onPress: () => router.push('/paywall') },
      ]
    );
  };

  const getSellNowPrice = (ovr: number): number => {
    if (ovr >= 95) return 10000;
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
        <ScrollView
          contentContainerStyle={styles.scrollContent}
          keyboardShouldPersistTaps="handled"
          keyboardDismissMode="on-drag"
        >
          
          {/* bio section */}
          <View style={styles.glassCard}>
            <View style={styles.topRow}>
              <Image 
                source={card.baked_img}
                style={styles.cardArt} 
                contentFit="contain"
                transition={200}
              />
              <View style={styles.bioColumn}>
                <Text style={styles.playerName}>{card.name}</Text>
                <Text style={styles.teamText}>
                  {card.team_short_name} • {card.display_position} • Age: {card.age}
                </Text>
                <Text style={styles.teamText}>Throws: {card.throw_hand} • Bats: {card.bat_hand}</Text>
                <View style={styles.divider} />
                <View style={styles.overallRow}>
                  <View style={styles.statBadge}>
                    <Text style={styles.statLabel}>OVERALL</Text>
                    <Text style={styles.statValue}>{card.ovr}</Text>
                  </View>
                  {card.predicted_ovr != null && (
                    <View style={[styles.statBadge, {
                      backgroundColor: card.predicted_ovr > card.ovr ? 'rgba(74, 222, 128, 0.15)' : card.predicted_ovr < card.ovr ? 'rgba(248, 113, 113, 0.15)' : 'rgba(107, 114, 128, 0.15)',
                      borderColor: card.predicted_ovr > card.ovr ? '#4ade80' : card.predicted_ovr < card.ovr ? '#f87171' : '#6b7280',
                    }]}>
                      <Text style={[styles.statLabel, {
                        color: card.predicted_ovr > card.ovr ? '#4ade80' : card.predicted_ovr < card.ovr ? '#f87171' : '#6b7280',
                      }]}>PRED</Text>
                      <Text style={[styles.statValue, {
                        color: card.predicted_ovr > card.ovr ? '#4ade80' : card.predicted_ovr < card.ovr ? '#f87171' : '#6b7280',
                      }]}>{card.predicted_ovr}</Text>
                    </View>
                  )}
                </View>
              </View>
            </View>
          </View>

          {/* thin divider under main tabs */}
          
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

          {/* attribute / market / stats / pro tabs */}
          <View style={styles.tabsContainer}>
            <TouchableOpacity
              onPress={() => setActiveTab('attributes')}
              style={[styles.tabButton, activeTab === 'attributes' && styles.tabButtonActive]}
            >
              <Text style={[styles.tabText, activeTab === 'attributes' && styles.tabTextActive]}>Attributes</Text>
            </TouchableOpacity>
            <TouchableOpacity
              onPress={() => setActiveTab('market')}
              style={[styles.tabButton, activeTab === 'market' && styles.tabButtonActive]}
            >
              <Text style={[styles.tabText, activeTab === 'market' && styles.tabTextActive]}>Market</Text>
            </TouchableOpacity>
            <TouchableOpacity
              onPress={() => setActiveTab('stats')}
              style={[styles.tabButton, activeTab === 'stats' && styles.tabButtonActive]}
            >
              <Text style={[styles.tabText, activeTab === 'stats' && styles.tabTextActive]}>MLB Stats</Text>
            </TouchableOpacity>
            <TouchableOpacity
              onPress={() => setActiveTab('pro')}
              style={[styles.tabButton, activeTab === 'pro' && styles.tabButtonActivePro]}
            >
              <View style={{ flexDirection: 'row', alignItems: 'center', gap: 4 }}>
                <FontAwesome5 name="crown" size={9} color={activeTab === 'pro' ? '#fbbf24' : theme.colors.muted} />
                <Text style={[styles.tabText, activeTab === 'pro' && { color: '#fbbf24', fontWeight: '700' }]}>PRO</Text>
              </View>
            </TouchableOpacity>
          </View>
          <View style={styles.tabsDivider} />

          {activeTab === 'attributes' && (
            <>
              <Text style={styles.sectionTitle}>Attributes</Text>

              {/* Inner attribute tabs: Attributes | Quirks | Pitches (pitchers only) */}
              <View style={[styles.windowFilterRow, styles.innerTabPills]}>
                <TouchableOpacity style={[styles.windowPill, activeAttrTab === 'attributes' && styles.windowPillActive]} onPress={() => setActiveAttrTab('attributes')}>
                  <Text style={[styles.windowPillText, activeAttrTab === 'attributes' && styles.windowPillTextActive]}>Attributes</Text>
                </TouchableOpacity>
                <TouchableOpacity style={[styles.windowPill, activeAttrTab === 'quirks' && styles.windowPillActive]} onPress={() => setActiveAttrTab('quirks')}>
                  <Text style={[styles.windowPillText, activeAttrTab === 'quirks' && styles.windowPillTextActive]}>Quirks</Text>
                </TouchableOpacity>
                {showPitching && (
                  <TouchableOpacity style={[styles.windowPill, activeAttrTab === 'pitches' && styles.windowPillActive]} onPress={() => setActiveAttrTab('pitches')}>
                    <Text style={[styles.windowPillText, activeAttrTab === 'pitches' && styles.windowPillTextActive]}>Pitches</Text>
                  </TouchableOpacity>
                )}
              </View>

              {/* Attributes inner tab */}
              {activeAttrTab === 'attributes' && (
                <View style={styles.glassCard}>
                  {/* pitching attributes */}
                  {showPitching && (
                    <>
                      <Text style={[styles.subHeader, { color: PITCHING_COLOR }]}>Pitching</Text>
                      <View style={[styles.subHeaderDivider, { backgroundColor: PITCHING_COLOR }]} />
                      <AttributeBar label="Stamina" value={card.stamina || 0} barColor={PITCHING_COLOR} />
                      <AttributeBar label="Pitching Clutch" value={card.pitching_clutch || 0} barColor={PITCHING_COLOR} />
                      <AttributeBar label="H/9" value={card.hits_per_bf || 0} barColor={PITCHING_COLOR} /> 
                      <AttributeBar label="K/9" value={card.k_per_bf || 0} barColor={PITCHING_COLOR} />
                      <AttributeBar label="BB/9" value={card.bb_per_bf || 0} barColor={PITCHING_COLOR} />
                      {showBatting && <View style={{ height: 24 }} />}
                    </>
                  )}

                  {/* batting */}
                  {showBatting && (
                    <>
                      <Text style={[styles.subHeader, { color: BATTING_COLOR }]}>Batting</Text>
                      <View style={[styles.subHeaderDivider, { backgroundColor: BATTING_COLOR }]} />
                      <Text style={[styles.subHeaderSmall, { color: BATTING_COLOR }]}>Contact</Text>
                      <AttributeBar label="Contact R" value={card.contact_right || 0} barColor={BATTING_COLOR} />
                      <AttributeBar label="Contact L" value={card.contact_left || 0} barColor={BATTING_COLOR} />
                      <AttributeBar label="Vision" value={card.plate_vision || 0} barColor={BATTING_COLOR} />
                      <AttributeBar label="Clutch" value={card.batting_clutch || 0} barColor={BATTING_COLOR} />
                      <View style={{ height: 12 }} />
                      <Text style={[styles.subHeaderSmall, { color: BATTING_COLOR }]}>Power</Text>
                      <AttributeBar label="Power R" value={card.power_right || 0} barColor={BATTING_COLOR} />
                      <AttributeBar label="Power L" value={card.power_left || 0} barColor={BATTING_COLOR} />
                      <View style={{ height: 16 }} />
                    </>
                  )}

                  {/* Fielding  */}
                  <Text style={[styles.subHeader, { color: FIELDING_COLOR }]}>Fielding</Text>
                  <View style={[styles.subHeaderDivider, { backgroundColor: FIELDING_COLOR }]} />
                  <AttributeBar label="Fielding" value={card.fielding_ability || 0} barColor={FIELDING_COLOR} maxValue={99} />
                  <AttributeBar label="Arm Strength" value={card.arm_strength || 0} barColor={FIELDING_COLOR} maxValue={99} />
                  <AttributeBar label="Arm Accuracy" value={card.arm_accuracy || 0} barColor={FIELDING_COLOR} maxValue={99} />
                  <AttributeBar label="Reaction Time" value={card.reaction_time || 0} barColor={FIELDING_COLOR} maxValue={99} />
                  <AttributeBar label="Blocking" value={card.blocking || 0} barColor={FIELDING_COLOR} maxValue={99} />

                  {/* Running (hitters only) */}
                  {card.is_hitter && (
                    <>
                      <View style={{ height: 16 }} />
                      <Text style={[styles.subHeader, { color: RUNNING_COLOR }]}>Running</Text>
                      <View style={[styles.subHeaderDivider, { backgroundColor: RUNNING_COLOR }]} />
                      <AttributeBar label="Speed" value={card.speed || 0} barColor={RUNNING_COLOR} maxValue={99} />
                      <AttributeBar label="Baserunning Ability" value={card.baserunning_ability || 0} barColor={RUNNING_COLOR} maxValue={99} />
                      <AttributeBar label="Baserunning Aggression" value={card.baserunning_aggression || 0} barColor={RUNNING_COLOR} maxValue={99} />
                    </>
                  )}
                </View>
              )}

              {/* Quirks inner tab */}
              {activeAttrTab === 'quirks' && (
                <View style={styles.glassCard}>
                  {loadingQuirks ? (
                    <ActivityIndicator color="white" />
                  ) : quirks.length === 0 ? (
                    <Text style={{ color: theme.colors.muted, textAlign: 'center', paddingVertical: 8 }}>No quirks</Text>
                  ) : (
                    quirks.map((quirk, index) => (
                      <View
                        key={quirk.name}
                        style={[
                          styles.quirkRow,
                          index < quirks.length - 1 && styles.quirkRowBorder,
                        ]}
                      >
                        <Image source={{ uri: quirk.img }} style={styles.quirkImg} />
                        <View style={styles.quirkText}>
                          <Text style={styles.quirkName}>{quirk.name}</Text>
                          <Text style={styles.quirkDescription}>{quirk.description}</Text>
                        </View>
                      </View>
                    ))
                  )}
                </View>
              )}

              {/* Pitches inner tab (pitchers only) */}
              {activeAttrTab === 'pitches' && showPitching && (
                <View style={styles.glassCard}>
                  {loadingPitches ? (
                    <ActivityIndicator color="white" />
                  ) : pitches.length === 0 ? (
                    <Text style={{ color: theme.colors.muted, textAlign: 'center', paddingVertical: 8 }}>No pitch data</Text>
                  ) : (
                    pitches.map(p => (
                      <View key={p.name} style={{ marginBottom: 12 }}>
                        <Text style={[styles.subHeader, { textAlign: 'center', marginBottom: 8 }]}>{p.name}</Text>
                        <View style={{ flexDirection: 'row', justifyContent: 'space-around', paddingVertical: 8 }}>
                          <PitchGauge label="Velocity" value={p.speed} color="#ef4444" />
                          <PitchGauge label="Control" value={p.control} color="#60a5fa" />
                          <PitchGauge label="Movement" value={p.movement} color="#a855f7" />
                        </View>
                      </View>
                    ))
                  )}
                </View>
              )}

            </>
          )}
          {activeTab === 'market' && (
            <>
              <Text style={styles.sectionTitle}>Market</Text>
              <View style={styles.glassCard}>
                <View style={styles.marketGrid}>
                  <View style={styles.marketColumn}>
                    <Text style={styles.marketLabel}>Buy Order</Text>
                    {loadingMarket ? (
                      <Text style={[styles.marketValue, styles.marketValueText]}>Loading...</Text>
                    ) : buyPrice !== null ? (
                      <View style={styles.marketValueRow}>
                        <Image source={STUB_ICON} style={styles.marketIcon} />
                        <Text style={styles.marketValue}>{buyPrice.toLocaleString()}</Text>
                      </View>
                    ) : (
                      <Text style={[styles.marketValue, styles.marketValueText]}>N/A</Text>
                    )}
                  </View>

                  <View style={styles.marketColumn}>
                    <Text style={styles.marketLabel}>Quick Sell</Text>
                    <View style={styles.marketValueRow}>
                      <Image source={STUB_ICON} style={styles.marketIcon} />
                      <Text style={styles.marketValue}>{getSellNowPrice(card.ovr).toLocaleString()}</Text>
                    </View>
                  </View>

                  <View style={styles.marketColumn}>
                    <Text style={styles.marketLabel}>Sell Order</Text>
                    {loadingMarket ? (
                      <Text style={[styles.marketValue, styles.marketValueText]}>Loading...</Text>
                    ) : sellPrice !== null ? (
                      <View style={styles.marketValueRow}>
                        <Image source={STUB_ICON} style={styles.marketIcon} />
                        <Text style={styles.marketValue}>{sellPrice.toLocaleString()}</Text>
                      </View>
                    ) : (
                      <Text style={[styles.marketValue, styles.marketValueText]}>N/A</Text>
                    )}
                  </View>
                </View>
              </View>

              {/* NEW CHART ADDED HERE */}
              <MarketSpreadChart data={marketCandles} loading={loadingMarket} />
              <MarketVolumeChart buyVolume={buyVolume} sellVolume={sellVolume} loading={loadingMarket} />
            </>
          )}
          {activeTab === 'stats' && (
            <>
              <Text style={styles.sectionTitle}>MLB Stats</Text>
              {/* Window filter pills */}
              <View style={styles.windowFilterGrid}>
                <View style={styles.windowFilterRow}>
                  {([
                    { key: 'season', label: 'Season' },
                    { key: '7d',     label: 'Last 7 Days' },
                    { key: '14d',    label: 'Last 14 Days' },
                  ] as { key: 'season' | '7d' | '14d' | 'last_update'; label: string }[]).map(({ key: w, label }) => (
                    <TouchableOpacity
                      key={w}
                      style={[styles.windowPill, activeWindow === w && styles.windowPillActive]}
                      onPress={() => setActiveWindow(w)}
                    >
                      <Text style={[styles.windowPillText, activeWindow === w && styles.windowPillTextActive]}>
                        {label}
                      </Text>
                    </TouchableOpacity>
                  ))}
                </View>
                <TouchableOpacity
                  style={[
                    styles.windowPillFull,
                    activeWindow === 'last_update' && styles.windowPillActive,
                    !canAccessLastUpdateWindow && styles.windowPillLocked,
                  ]}
                  onPress={handleLastUpdateWindowPress}
                >
                  <View style={styles.lockedWindowPillContent}>
                    <Text
                      style={[
                        styles.windowPillText,
                        activeWindow === 'last_update' && styles.windowPillTextActive,
                      ]}
                    >
                      Since Last Roster Update
                    </Text>
                    {!canAccessLastUpdateWindow ? (
                      <FontAwesome5 name="crown" size={10} color="#fbbf24" />
                    ) : null}
                  </View>
                </TouchableOpacity>
              </View>
              {(() => {
                const windowLabel =
                  activeWindow === '7d' ? 'Last 7 Days' :
                  activeWindow === '14d' ? 'Last 14 Days' :
                  activeWindow === 'last_update' ? 'Since Last Roster Update' :
                  'Season Totals';
                return (
              <>{loadingStats ? (
                <View style={styles.glassCard}>
                  <ActivityIndicator color="white" />
                </View>
              ) : !seasonStats || (!seasonStats.batting && !seasonStats.pitching) ? (
                <View style={styles.glassCard}>
                  <Text style={{ color: theme.colors.muted, textAlign: 'center', paddingVertical: 24 }}>N/A</Text>
                </View>
              ) : (
                <>
                  {/* Batting Stats */}
                  {seasonStats.batting && (
                    <>
                      {isTwoWay && <Text style={[styles.subHeader, { color: BATTING_COLOR, marginBottom: 8 }]}>Batting</Text>}
                      {/* Batting Overall Badges */}
                      <View style={styles.statsBadgeRow}>
                        {(['avg', 'obp', 'slg', 'ops'] as const).map(key => (
                          <View key={key} style={styles.statsBadge}>
                            <Text style={styles.statsBadgeLabel}>{key.toUpperCase()}</Text>
                            <Text style={styles.statsBadgeValue}>{formatStat(seasonStats.batting!.overall[key])}</Text>
                          </View>
                        ))}
                      </View>

                      {/* Batting Overall Counting Stats */}
                      <View style={styles.glassCard}>
                        <Text style={[styles.subHeader, { color: BATTING_COLOR }]}>{windowLabel}</Text>
                        <View style={[styles.subHeaderDivider, { backgroundColor: BATTING_COLOR }]} />
                        <View style={styles.statsGrid}>
                          {(['pa', 'ab', 'h', 'hr', 'rbi', 'r', 'bb', 'so', 'doubles', 'triples', 'hbp', 'tb'] as const).map(key => (
                            <View key={key} style={styles.statsGridItem}>
                              <Text style={styles.statsGridLabel}>{key === 'doubles' ? '2B' : key === 'triples' ? '3B' : key.toUpperCase()}</Text>
                              <Text style={styles.statsGridValue}>{seasonStats.batting!.overall[key] ?? 0}</Text>
                            </View>
                          ))}
                        </View>
                      </View>

                      {/* Batting Splits */}
                      <View style={styles.glassCard}>
                        <Text style={[styles.subHeader, { color: BATTING_COLOR }]}>Splits</Text>
                        <View style={[styles.subHeaderDivider, { backgroundColor: BATTING_COLOR }]} />
                        <View style={styles.splitTableRow}>
                          <Text style={[styles.splitTableCell, styles.splitTableHeader, { flex: 1.2 }]}></Text>
                          {['AVG', 'OBP', 'SLG', 'AB', 'H', 'HR', 'BB', 'K'].map(h => (
                            <Text key={h} style={[styles.splitTableCell, styles.splitTableHeader]}>{h}</Text>
                          ))}
                        </View>
                        {seasonStats.batting!.splits.map(split => (
                          <View key={split.split} style={styles.splitTableRow}>
                            <Text style={[styles.splitTableCell, { flex: 1.2, color: 'white', fontWeight: '600' }]}>{SPLIT_LABELS[split.split] || split.split}</Text>
                            <Text style={styles.splitTableCell}>{formatStat(split.avg)}</Text>
                            <Text style={styles.splitTableCell}>{formatStat(split.obp)}</Text>
                            <Text style={styles.splitTableCell}>{formatStat(split.slg)}</Text>
                            <Text style={styles.splitTableCell}>{split.ab}</Text>
                            <Text style={styles.splitTableCell}>{split.h}</Text>
                            <Text style={styles.splitTableCell}>{split.hr}</Text>
                            <Text style={styles.splitTableCell}>{split.bb}</Text>
                            <Text style={styles.splitTableCell}>{split.so}</Text>
                          </View>
                        ))}
                      </View>
                    </>
                  )}

                  {/* Spacer between batting and pitching for two-way players */}
                  {isTwoWay && seasonStats.batting && seasonStats.pitching && (
                    <View style={{ height: 8 }} />
                  )}

                  {/* Pitching Stats */}
                  {seasonStats.pitching && (
                    <>
                      {isTwoWay && <Text style={[styles.subHeader, { color: PITCHING_COLOR, marginBottom: 8 }]}>Pitching</Text>}
                      {/* Pitching Overall Badges */}
                      <View style={styles.statsBadgeRow}>
                        {(['era', 'whip', 'k9', 'ip'] as const).map(key => (
                          <View key={key} style={styles.statsBadge}>
                            <Text style={styles.statsBadgeLabel}>{key === 'k9' ? 'K/9' : key.toUpperCase()}</Text>
                            <Text style={styles.statsBadgeValue}>
                              {key === 'era' || key === 'whip' ? formatStat(seasonStats.pitching!.overall[key], 2)
                                : key === 'k9' ? formatStat(seasonStats.pitching!.overall[key], 2)
                                : seasonStats.pitching!.overall[key]}
                            </Text>
                          </View>
                        ))}
                      </View>

                      {/* Pitching Overall Counting Stats */}
                      <View style={styles.glassCard}>
                        <Text style={[styles.subHeader, { color: PITCHING_COLOR }]}>{windowLabel}</Text>
                        <View style={[styles.subHeaderDivider, { backgroundColor: PITCHING_COLOR }]} />
                        <View style={styles.statsGrid}>
                          {(['ip', 'h', 'er', 'hr', 'bb', 'k', 'batters_faced', 'strike_pct'] as const).map(key => (
                            <View key={key} style={styles.statsGridItem}>
                              <Text style={styles.statsGridLabel}>
                                {key === 'batters_faced' ? 'BF' : key === 'strike_pct' ? 'STR%' : key.toUpperCase()}
                              </Text>
                              <Text style={styles.statsGridValue}>
                                {key === 'strike_pct'
                                  ? `${(((seasonStats.pitching!.overall[key] as number) ?? 0) * 100).toFixed(1)}%`
                                  : seasonStats.pitching!.overall[key] ?? 0}
                              </Text>
                            </View>
                          ))}
                        </View>
                      </View>

                      {/* Pitching Splits */}
                      <View style={styles.glassCard}>
                        <Text style={[styles.subHeader, { color: PITCHING_COLOR }]}>Splits</Text>
                        <View style={[styles.subHeaderDivider, { backgroundColor: PITCHING_COLOR }]} />
                        <View style={styles.splitTableRow}>
                          <Text style={[styles.splitTableCell, styles.splitTableHeader, { flex: 1.2 }]}></Text>
                          {['ERA', 'WHIP', 'IP', 'H', 'ER', 'K', 'BB', 'HR'].map(h => (
                            <Text key={h} style={[styles.splitTableCell, styles.splitTableHeader]}>{h}</Text>
                          ))}
                        </View>
                        {seasonStats.pitching!.splits.map(split => (
                          <View key={split.split} style={styles.splitTableRow}>
                            <Text style={[styles.splitTableCell, { flex: 1.2, color: 'white', fontWeight: '600' }]}>{SPLIT_LABELS[split.split] || split.split}</Text>
                            <Text style={styles.splitTableCell}>{formatStat(split.era, 2)}</Text>
                            <Text style={styles.splitTableCell}>{formatStat(split.whip, 2)}</Text>
                            <Text style={styles.splitTableCell}>{split.ip}</Text>
                            <Text style={styles.splitTableCell}>{split.h}</Text>
                            <Text style={styles.splitTableCell}>{split.er}</Text>
                            <Text style={styles.splitTableCell}>{split.k}</Text>
                            <Text style={styles.splitTableCell}>{split.bb}</Text>
                            <Text style={styles.splitTableCell}>{split.hr}</Text>
                          </View>
                        ))}
                      </View>
                    </>
                  )}
                </>
              )}</>
                );
              })()}
            </>
          )}
          {activeTab === 'pro' && (
            <>
              <View style={styles.predictionHeader}>
                <View style={styles.proBadge}>
                  <FontAwesome5 name="crown" size={10} color="#fbbf24" style={styles.proIcon} />
                  <Text style={styles.proText}>PRO</Text>
                </View>
                <Text style={[styles.sectionTitle, { color: '#fbbf24', marginBottom: 0 }]}>Predicted Attributes</Text>
              </View>
              <Text style={styles.proSectionSubheader}>
                {showProLock || showProStatusPending
                  ? 'See projected individual batting and pitching attribute changes for this card. Sign up for Pro to unlock full access.'
                  : 'Projected individual batting and pitching attribute changes from our latest model run.'}
              </Text>
              <View style={[styles.glassCard, styles.proCard]}>
                <View style={showProLock || showProStatusPending ? styles.proContentObscured : undefined}>

                  {/* Pitching Predictions */}
                  {PITCHING_PREDICTION_KEYS.some(({ key }) =>
                    card.predicted_attributes?.[`pit_pred_${key}_new`] != null
                  ) && (
                    <>
                      <Text style={[styles.subHeader, { color: PITCHING_COLOR }]}>Pitching</Text>
                      <View style={[styles.subHeaderDivider, { backgroundColor: PITCHING_COLOR }]} />
                      {PITCHING_PREDICTION_KEYS.map(({ key, label }) => {
                        const newVal = card.predicted_attributes?.[`pit_pred_${key}_new`];
                        const delta = card.predicted_attributes?.[`pit_pred_${key}_delta`];
                        if (newVal == null || delta == null) return null;
                        return (
                          <PredictionAttributeBar
                            key={key}
                            label={label}
                            predictedValue={Math.round(newVal)}
                            delta={delta}
                          />
                        );
                      })}
                      {showBatting && <View style={{ height: 24 }} />}
                    </>
                  )}

                  {/* Batting Predictions */}
                  {BATTING_PREDICTION_KEYS.some(({ key }) =>
                    card.predicted_attributes?.[`hit_pred_${key}_new`] != null
                  ) && (
                    <>
                      <Text style={[styles.subHeader, { color: BATTING_COLOR }]}>Batting</Text>
                      <View style={[styles.subHeaderDivider, { backgroundColor: BATTING_COLOR }]} />
                      {BATTING_PREDICTION_KEYS.map(({ key, label }) => {
                        const newVal = card.predicted_attributes?.[`hit_pred_${key}_new`];
                        const delta = card.predicted_attributes?.[`hit_pred_${key}_delta`];
                        if (newVal == null || delta == null) return null;
                        return (
                          <PredictionAttributeBar
                            key={key}
                            label={label}
                            predictedValue={Math.round(newVal)}
                            delta={delta}
                          />
                        );
                      })}
                    </>
                  )}
                </View>

                {showProStatusPending ? (
                  <View style={styles.proPendingOverlay}>
                    <ActivityIndicator size="small" color="#fbbf24" />
                    <Text style={styles.proPendingText}>Checking Pro access...</Text>
                  </View>
                ) : null}

                {showProLock ? (
                  <View style={styles.proLockOverlay}>
                    <Text style={styles.proLockTitle}>Sign up for Pro to see predicted attributes</Text>
                    <Text style={styles.proLockDescription}>
                      Unlock projected increases and decreases for key batting and pitching ratings before roster updates.
                    </Text>
                    <TouchableOpacity
                      style={styles.proLockButton}
                      onPress={() => router.push('/paywall')}
                    >
                      <FontAwesome5 name="crown" size={12} color="#111827" />
                      <Text style={styles.proLockButtonText}>Go Pro</Text>
                    </TouchableOpacity>
                  </View>
                ) : null}

              </View>
            </>
          )}

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
  overallRow: {
    flexDirection: 'row',
    gap: 12,
  },
  statLabel: { color: '#3b82f6', fontSize: 10, fontWeight: 'bold' },
  statValue: { color: '#3b82f6', fontSize: 24, fontWeight: '900' },
  tabsContainer: { flexDirection: 'row', gap: 8, marginBottom: 12 },
  tabButton: {
    flex: 1,
    paddingVertical: 8,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.12)',
    backgroundColor: 'rgba(15, 23, 42, 0.35)',
    alignItems: 'center',
  },
  tabButtonActive: {
    backgroundColor: 'rgba(59, 130, 246, 0.2)',
    borderColor: 'rgba(59, 130, 246, 0.6)',
  },
  tabText: { color: theme.colors.muted, fontSize: 12, fontWeight: '600' },
  tabTextActive: { color: theme.colors.text, fontWeight: '700' },
  tabButtonActivePro: {
    backgroundColor: 'rgba(251, 191, 36, 0.15)',
    borderColor: 'rgba(251, 191, 36, 0.6)',
  },
  windowFilterGrid: {
    flexDirection: 'column',
    gap: 8,
    marginBottom: 14,
  },
  windowFilterRow: {
    flexDirection: 'row',
    gap: 8,
  },
  innerTabPills: {
    marginBottom: 14,
  },
  windowPill: {
    flex: 1,
    paddingVertical: 7,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.12)',
    backgroundColor: 'rgba(15, 23, 42, 0.35)',
    alignItems: 'center',
  },
  windowPillFull: {
    paddingVertical: 7,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.12)',
    backgroundColor: 'rgba(15, 23, 42, 0.35)',
    alignItems: 'center',
  },
  windowPillLocked: {
    borderColor: 'rgba(251, 191, 36, 0.45)',
  },
  lockedWindowPillContent: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 6,
  },
  windowPillActive: {
    backgroundColor: 'rgba(59, 130, 246, 0.2)',
    borderColor: 'rgba(59, 130, 246, 0.6)',
  },
  windowPillText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: '600',
  },
  windowPillTextActive: {
    color: 'white',
    fontWeight: '700',
  },
  sectionTitle: { color: 'white', fontSize: 20, fontWeight: 'bold', marginBottom: 12 },
  subHeader: { color: '#3b82f6', fontSize: 15, fontWeight: '700', marginBottom: 12, textTransform: 'uppercase' },
  subHeaderDivider: { height: 1, opacity: 0.45, marginBottom: 10 },
  subHeaderSmall: { color: '#3b82f6', fontSize: 12, fontWeight: '600', marginBottom: 8, textTransform: 'uppercase' },
  marketGrid: { flexDirection: 'row', gap: 16 },
  marketColumn: { flex: 1 },
  marketLabel: { color: theme.colors.muted, fontSize: 13, fontWeight: '600' },
  marketValueRow: { flexDirection: 'row', alignItems: 'center', gap: 6, marginTop: 6 },
  marketIcon: { width: 16, height: 16, resizeMode: 'contain' },
  marketValue: { color: 'white', fontSize: 16, fontWeight: '700' },
  marketValueText: { marginTop: 6 },
  quirkRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    paddingVertical: 10,
  },
  quirkRowBorder: {
    borderBottomWidth: 1,
    borderBottomColor: 'rgba(255,255,255,0.08)',
  },
  quirkImg: {
    width: 44,
    height: 44,
    borderRadius: 8,
    backgroundColor: 'rgba(255,255,255,0.05)',
  },
  quirkText: {
    flex: 1,
  },
  quirkName: {
    color: 'white',
    fontSize: 14,
    fontWeight: '700',
    marginBottom: 2,
  },
  quirkDescription: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: '400',
    lineHeight: 16,
  },
  statsBadgeRow: {
    flexDirection: 'row',
    gap: 8,
    marginBottom: 16,
  },
  statsBadge: {
    flex: 1,
    backgroundColor: 'rgba(2, 6, 23, 0.7)',
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.1)',
    paddingVertical: 12,
    alignItems: 'center',
  },
  statsBadgeLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '700',
    textTransform: 'uppercase',
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  statsBadgeValue: {
    color: 'white',
    fontSize: 20,
    fontWeight: '900',
  },
  statsGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
  },
  statsGridItem: {
    width: '25%',
    alignItems: 'center',
    paddingVertical: 8,
  },
  statsGridLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '600',
    textTransform: 'uppercase',
    marginBottom: 2,
  },
  statsGridValue: {
    color: 'white',
    fontSize: 16,
    fontWeight: '700',
  },
  splitTableRow: {
    flexDirection: 'row',
    paddingVertical: 6,
    borderBottomWidth: 1,
    borderBottomColor: 'rgba(255,255,255,0.05)',
  },
  splitTableCell: {
    flex: 1,
    color: theme.colors.muted,
    fontSize: 11,
    textAlign: 'center',
  },
  splitTableHeader: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: '700',
    textTransform: 'uppercase',
  },
  predictionHeader: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    marginBottom: 12,
  },
  proBadge: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(0, 0, 0, 0.6)',
    paddingVertical: 4,
    paddingHorizontal: 8,
    borderRadius: 20,
    borderWidth: 1,
    borderColor: '#fbbf24',
  },
  proIcon: {
    marginRight: 4,
  },
  proText: {
    color: '#fbbf24',
    fontWeight: '800',
    fontSize: 10,
    letterSpacing: 0.4,
  },
  proSectionSubheader: {
    color: theme.colors.muted,
    fontSize: 13,
    fontWeight: '500',
    lineHeight: 18,
    marginBottom: 12,
  },
  proCard: {
    position: 'relative',
    overflow: 'hidden',
  },
  proContentObscured: {
    opacity: 0.28,
  },
  proPendingOverlay: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: 'rgba(2, 6, 23, 0.42)',
    justifyContent: 'center',
    alignItems: 'center',
    gap: 10,
  },
  proPendingText: {
    color: '#fde68a',
    fontSize: 12,
    fontWeight: '600',
  },
  proLockOverlay: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: 'rgba(2, 6, 23, 0.74)',
    borderWidth: 1,
    borderColor: 'rgba(251, 191, 36, 0.35)',
    borderRadius: 16,
    paddingHorizontal: 18,
    justifyContent: 'center',
    alignItems: 'center',
  },
  proLockTitle: {
    color: '#fde68a',
    fontSize: 16,
    fontWeight: '800',
    textAlign: 'center',
    marginBottom: 8,
  },
  proLockDescription: {
    color: '#e5e7eb',
    fontSize: 13,
    fontWeight: '500',
    lineHeight: 18,
    textAlign: 'center',
    marginBottom: 16,
  },
  proLockButton: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    backgroundColor: '#fbbf24',
    borderRadius: 999,
    paddingHorizontal: 14,
    paddingVertical: 9,
  },
  proLockButtonText: {
    color: '#111827',
    fontWeight: '800',
    fontSize: 12,
    letterSpacing: 0.2,
  },
  subSectionTitle: {
    color: 'rgba(255,255,255,0.5)',
    fontSize: 13,
    fontWeight: '600',
    textTransform: 'uppercase',
    letterSpacing: 1,
    marginTop: 12,
    marginBottom: 8,
  },
  tabsDivider: {
    height: 1,
    backgroundColor: 'rgba(255,255,255,0.06)',
    marginBottom: 12,
  },
});
