import { Tabs } from 'expo-router';
import { FontAwesome5 } from '@expo/vector-icons';
import { View } from 'react-native';

export default function TabLayout() {
  return (
    <Tabs
      screenOptions={{
        // 1. Dark Theme Styling for the Tab Bar
        tabBarStyle: {
          backgroundColor: '#0f172a', // Dark slate background
          borderTopColor: 'rgba(255,255,255,0.1)', // Subtle top border
          height: 88, // Taller bar for modern look
          paddingTop: 8,
        },
        // 2. Text styling
        tabBarActiveTintColor: '#3b82f6', // The "Blue" you requested
        tabBarInactiveTintColor: '#64748b', // Muted gray for inactive
        tabBarLabelStyle: {
          fontSize: 11,
          fontWeight: '600',
          marginBottom: 8,
        },
        // 3. Hide the top header (we have our own headers in screens)
        headerShown: false,
      }}
    >
      {/* 1. HOME TAB (Your current index.tsx) */}
      <Tabs.Screen
        name="index"
        options={{
          title: 'Home',
          tabBarIcon: ({ color }) => (
            <FontAwesome5 name="home" size={20} color={color} />
          ),
        }}
      />

      {/* 2. PREDICTIONS TAB */}
      <Tabs.Screen
        name="predictions"
        options={{
          title: 'Predictions',
          tabBarIcon: ({ color }) => (
            <FontAwesome5 name="chart-line" size={20} color={color} />
          ),
        }}
      />

      {/* NEW PORTFOLIO TAB */}
      <Tabs.Screen
        name="portfolio"
        options={{
          title: 'Portfolio',
          tabBarIcon: ({ color }) => (
            <FontAwesome5 name="briefcase" size={20} color={color} />
          ),
        }}
      />

      {/* 3. CHAT TAB */}
      <Tabs.Screen
        name="chat"
        options={{
          title: 'Chat',
          tabBarIcon: ({ color }) => (
            <FontAwesome5 name="comment-dots" size={20} color={color} />
          ),
        }}
      />

      {/* 4. EXPLORE TAB */}
      <Tabs.Screen
        name="explore"
        options={{
          title: 'Explore',
          tabBarIcon: ({ color }) => (
            <FontAwesome5 name="compass" size={22} color={color} />
          ),
        }}
      />
    </Tabs>
  );
}