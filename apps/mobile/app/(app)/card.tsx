import React from 'react';
import {
  View,
  Text,
  StyleSheet,
  Image,
  ScrollView,
  TouchableOpacity,
  KeyboardAvoidingView,
  Platform,
  ActivityIndicator,
} from 'react-native';
import { Stack, useLocalSearchParams, useRouter } from 'expo-router';
import { SafeAreaView } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { FloatingBackground } from '../../src/homescreencomponents/FloatingBackground';
import { AttributeBar } from '../../src/predictionscomponents/AttributeBar';
import { theme } from '../../src/theme/colors';
import { useState, useEffect } from 'react';
import Svg, { Path } from 'react-native-svg';
import { apiGet } from '../../src/lib/api';

const TWO_WAY_PLAYERS = [
  'Shohei Ohtani',
];

export default function CardScreen() {
  const router = useRouter();
  const params = useLocalSearchParams();
  const card = params.cardData ? JSON.parse(params.cardData as string) : null;

  if (!card) return null;

  const isTwoWay = TWO_WAY_PLAYERS.includes(card.name);
  const showPitching = card.is_hitter === false || isTwoWay;
  const showBatting = card.is_hitter === true || isTwoWay;

  const BATTING_COLOR = '#3b82f6';
  const PITCHING_COLOR = '#fbbf24';
  const FIELDING_COLOR = '#22c55e';
  const RUNNING_COLOR = '#A78BFA';

  type CardQuirk = { card_id: string; name: string; description: string; img: string };
  const [quirks, setQuirks] = useState<CardQuirk[]>([]);
  const [loadingQuirks, setLoadingQuirks] = useState(false);

  type Pitch = { card_id: string; name: string; speed: number; control: number; movement: number };
  const [pitches, setPitches] = useState<Pitch[]>([]);
  const [loadingPitches, setLoadingPitches] = useState(false);

  const [activeAttrTab, setActiveAttrTab] = useState<'attributes' | 'quirks' | 'pitches'>('attributes');

  useEffect(() => {
    if (!card?.id) return;
    setLoadingQuirks(true);
    apiGet<CardQuirk[]>(`/quirks/${card.id}`)
      .then((res) => setQuirks(res))
      .catch(() => setQuirks([]))
      .finally(() => setLoadingQuirks(false));
  }, [card?.id]);

  useEffect(() => {
    if (!card?.id) return;
    setLoadingPitches(true);
    apiGet<Pitch[]>(`/pitches/${card.id}`)
      .then((res) => setPitches(res))
      .catch(() => setPitches([]))
      .finally(() => setLoadingPitches(false));
  }, [card?.id]);

  const formatStat = (val: number | undefined, decimals: number = 0): string => {
    if (val == null) return '-';
    return String(Math.round(val));
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

    const arcLen = Math.PI * radius;
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

  return (
    <View style={styles.container}>
      {/* Removes the default Expo Router native header that was pushing content down */}
      <Stack.Screen options={{ headerShown: false }} />

      <View style={styles.backgroundLayer}>
        <FloatingBackground />
      </View>

      <SafeAreaView style={{ flex: 1 }} edges={['top']}>
        <View style={styles.navBar}>
          <TouchableOpacity onPress={() => router.back()} style={styles.backBtn}>
            <Ionicons name="arrow-back" size={24} color="white" />
            <Text style={styles.backText}>Back</Text>
          </TouchableOpacity>
        </View>

        <KeyboardAvoidingView behavior={Platform.OS === 'ios' ? 'padding' : 'height'} style={{ flex: 1 }} keyboardVerticalOffset={Platform.OS === 'ios' ? 10 : 0}>
          <ScrollView contentContainerStyle={styles.scrollContent} keyboardShouldPersistTaps="handled" keyboardDismissMode="on-drag">

            <View style={styles.glassCard}>
              <View style={styles.topRow}>
                <Image source={{ uri: card.baked_img }} style={styles.cardArt} resizeMode="contain" />
                <View style={styles.bioColumn}>
                  <Text style={styles.playerName}>{card.name}</Text>
                  <Text style={styles.teamText}>{card.team_short_name} • {card.display_position} • Age: {card.age}</Text>
                  <Text style={styles.teamText}>Throws: {card.throw_hand} • Bats: {card.bat_hand}</Text>
                  <View style={styles.divider} />
                  <View style={styles.overallRow}>
                    <View style={styles.statBadge}>
                      <Text style={styles.statLabel}>OVERALL</Text>
                      <Text style={styles.statValue}>{card.ovr}</Text>
                    </View>

                    {card.true_overall != null && (
                    <View style={[styles.statBadge, styles.trueBadge]}>
                      <Text style={[styles.statLabel, styles.trueText]}>TRUE</Text>
                      <Text style={[styles.statValue, styles.trueText]}>
                        {Math.round(Number(card.true_overall))}
                      </Text>
                    </View>
                  )}

                    {card.meta_overall != null && (
                    <View style={[styles.statBadge, styles.metaBadge]}>
                      <Text style={[styles.statLabel, styles.metaText]}>META</Text>
                      <Text style={[styles.statValue, styles.metaText]}>
                        {Math.round(Number(card.meta_overall))}
                      </Text>
                    </View>
                  )}
                  </View>
                </View>
              </View>
            </View>

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

                <Text style={[styles.subHeader, { color: FIELDING_COLOR }]}>Fielding</Text>
                <View style={[styles.subHeaderDivider, { backgroundColor: FIELDING_COLOR }]} />
                <AttributeBar label="Fielding" value={card.fielding_ability || 0} barColor={FIELDING_COLOR} maxValue={99} />
                <AttributeBar label="Arm Strength" value={card.arm_strength || 0} barColor={FIELDING_COLOR} maxValue={99} />
                <AttributeBar label="Arm Accuracy" value={card.arm_accuracy || 0} barColor={FIELDING_COLOR} maxValue={99} />
                <AttributeBar label="Reaction Time" value={card.reaction_time || 0} barColor={FIELDING_COLOR} maxValue={99} />
                <AttributeBar label="Blocking" value={card.blocking || 0} barColor={FIELDING_COLOR} maxValue={99} />

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
                    <View key={quirk.name} style={[styles.quirkRow, index < quirks.length - 1 && styles.quirkRowBorder]}>
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

            {/* Pitches inner tab */}
            {activeAttrTab === 'pitches' && showPitching && (
              <View style={styles.glassCard}>
                {loadingPitches ? (
                  <ActivityIndicator color="white" />
                ) : pitches.length === 0 ? (
                  <Text style={{ color: theme.colors.muted, textAlign: 'center', paddingVertical: 8 }}>No pitch data</Text>
                ) : (
                  pitches.map((p) => (
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
    padding: 8, borderRadius: 8, alignItems: 'center', alignSelf: 'flex-start', borderWidth: 1, borderColor: '#3b82f6'
  },
  overallRow: {
    flexDirection: 'row',
    gap: 10,
  },
  statLabel: { color: '#3b82f6', fontSize: 9, fontWeight: 'bold' },
  statValue: { color: '#3b82f6', fontSize: 18, fontWeight: '900' },
  trueBadge: {
    backgroundColor: 'rgba(168, 85, 247, 0.15)',
    borderColor: '#a855f7',
  },
  trueText: {
    color: '#a855f7',
  },
  metaBadge: {
    backgroundColor: 'rgba(251, 191, 36, 0.15)',
    borderColor: '#fbbf24',
  },
  metaText: {
    color: '#fbbf24',
  },
  sectionTitle: { color: 'white', fontSize: 20, fontWeight: 'bold', marginBottom: 12 },
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
  subHeader: { color: '#3b82f6', fontSize: 15, fontWeight: '700', marginBottom: 12, textTransform: 'uppercase' },
  subHeaderDivider: { height: 1, opacity: 0.45, marginBottom: 10 },
  subHeaderSmall: { color: '#3b82f6', fontSize: 12, fontWeight: '600', marginBottom: 8, textTransform: 'uppercase' },
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
});