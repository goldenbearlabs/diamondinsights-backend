import { Tabs } from "expo-router";
import { AppHeader } from "../../src/components/AppHeader";
import { BottomNav, BOTTOM_NAV_HEIGHT } from "../../src/components/BottomNav";
import { useSafeAreaInsets } from "react-native-safe-area-context";

export default function TabLayout() {
  const insets = useSafeAreaInsets();
  const tabBarHeight = BOTTOM_NAV_HEIGHT + Math.max(insets.bottom, 10);

  return (
    <Tabs
      backBehavior="history"
      tabBar={() => <BottomNav />}
      screenOptions={{
        tabBarStyle: {
          height: tabBarHeight,
          backgroundColor: "transparent",
          borderTopWidth: 0,
        },
        headerShown: true,
        header: () => <AppHeader />,
        headerStyle: {
          height: 56 + insets.top,
        },
      }}
    >
      <Tabs.Screen name="index" options={{ title: "Home" }} />
      <Tabs.Screen name="predictions" options={{ title: "Predictions" }} />
      <Tabs.Screen name="flipping" options={{ title: "Flipping" }} />
      <Tabs.Screen name="portfolio" options={{ title: "Portfolio" }} />
      <Tabs.Screen name="leaderboard" options={{ title: "Leaderboard" }} />
      <Tabs.Screen name="rankings" options={{ title: "Rankings" }} />
      <Tabs.Screen name="gameplay-stats" options={{ title: "Stats" }} />
      <Tabs.Screen name="records" options={{ title: "Records" }} />
      <Tabs.Screen name="card-comparison" options={{ title: "Card Comparison" }} />
      <Tabs.Screen name="team-builder" options={{ title: "Team Builder" }} />
      <Tabs.Screen name="cards" options={{ title: "Cards" }} />
      <Tabs.Screen name="card" options={{ title: "Card", href: null }} />
      <Tabs.Screen name="trending" options={{ title: "Trending" }} />
      <Tabs.Screen name="chat" options={{ title: "Chat" }} />
      <Tabs.Screen name="explore" options={{ title: "Explore" }} />
      <Tabs.Screen name="account" options={{ title: "Account" }} />
    </Tabs>
  );
}
