import { Stack, useRouter, useSegments } from "expo-router";
import { useEffect, useState } from "react";
import { View, ActivityIndicator } from "react-native";
import { onAuthStateChanged, User } from "firebase/auth";
import { auth } from "../src/lib/firebase";
import { theme } from "../src/theme/colors"; // Optional: for background color
import { syncRevenueCatUser } from "../src/lib/revenuecat";

export default function RootLayout() {
  const router = useRouter();
  const segments = useSegments();

  const [ready, setReady] = useState(false);
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (u) => {
      setUser(u);
      setReady(true);
    });
    return unsub;
  }, []);

  useEffect(() => {
    if (!ready) return;

    const group = segments[0];
    const inAuth = group === "(auth)";

    if (!user && !inAuth) router.replace("/(auth)/signin");
    if (user && inAuth) router.replace("/(app)");
  }, [ready, user, segments, router]);

  useEffect(() => {
    let cancelled = false;

    const syncRevenueCat = async () => {
      try {
        await syncRevenueCatUser(user?.uid ?? null);
      } catch (err) {
        if (!cancelled) {
          console.warn("[revenuecat] failed to sync user", err);
        }
      }
    };

    syncRevenueCat();

    return () => {
      cancelled = true;
    };
  }, [user?.uid]);

  if (!ready) {
    return (
      <View style={{ flex: 1, alignItems: "center", justifyContent: "center" }}>
        <ActivityIndicator />
      </View>
    );
  }

  return (
    <Stack screenOptions={{ headerShown: false, contentStyle: { backgroundColor: theme.colors.background } }}>
    
      <Stack.Screen name="(auth)" options={{ headerShown: false }} />

      
      <Stack.Screen name="(app)" options={{ headerShown: false }} />

      
      <Stack.Screen 
        name="predictions/[id]" 
        options={{ 
          presentation: 'card',
          headerShown: false, 
        }} 
      />

      <Stack.Screen
        name="game/[gameId]"
        options={{
          presentation: "card",
          headerShown: false,
        }}
      />
    </Stack>
  );
}
