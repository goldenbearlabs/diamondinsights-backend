import React from 'react';
import { View, Text, StyleSheet } from 'react-native';
import { FontAwesome5 } from '@expo/vector-icons';
import { theme } from '../theme/colors';

const STEPS = [
  { 
    icon: 'satellite-dish', 
    title: 'Data Collection', 
    desc: 'We aggregate real-time player stats and historic trends.', 
    color: '#3b82f6' // Blue
  },
  { 
    icon: 'microchip', 
    title: 'AI Analysis', 
    desc: 'Our model identifies patterns and predicts rating upgrades.', 
    color: '#a855f7' // Purple
  },
  { 
    icon: 'gem', 
    title: 'Profit', 
    desc: 'Get actionable insights before the market catches up.', 
    color: '#22c55e' // Green
  },
  { 
    icon: 'gamepad', 
    title: 'Analyze Your Gameplay', 
    desc: 'View advanced stats and deep analytics from your actual MLB The Show games.', 
    color: '#ef4444' // Red
  },
  { 
    icon: 'trophy', 
    title: 'Leaderboard', 
    desc: 'Make your own roster predictions and compete against the community for the top spot.', 
    color: '#fbbf24' // Gold/Yellow
  },
];

export const HowItWorks = () => {
  return (
    <View style={styles.container}>
      <Text style={styles.header}>HOW IT WORKS</Text>
      <View style={styles.list}>
        {STEPS.map((step, index) => (
          <View key={index} style={styles.card}>
            <View style={[styles.iconBox, { backgroundColor: `${step.color}20` }]}> 
              <FontAwesome5 name={step.icon as any} size={18} color={step.color} />
            </View>
            <View style={styles.textCol}>
              <Text style={styles.title}>{step.title}</Text>
              <Text style={styles.desc}>{step.desc}</Text>
            </View>
          </View>
        ))}
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    width: '100%',
    marginTop: 30, 
  },
  header: {
    fontSize: 18,
    fontWeight: '800',
    color: 'white',
    textAlign: 'center',
    marginBottom: 20,
    letterSpacing: 1,
  },
  list: { gap: 12 },
  card: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: 'rgba(255, 255, 255, 0.03)', 
    padding: 16,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.05)',
  },
  iconBox: {
    width: 40, height: 40, borderRadius: 10,
    justifyContent: 'center', alignItems: 'center', marginRight: 14,
  },
  textCol: { flex: 1 },
  title: { color: 'white', fontSize: 15, fontWeight: 'bold', marginBottom: 2 },
  desc: { color: theme.colors.muted, fontSize: 13, lineHeight: 18 },
});