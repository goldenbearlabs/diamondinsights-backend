import { Stack } from "expo-router";
import { StatusBar } from "expo-status-bar";
import { View } from "react-native";
import { theme } from "../src/theme/colors";

export default function RootLayout() {
  return (
    <View style={{ flex: 1, backgroundColor: theme.colors.background }}>
      {/* StatusBar style="light" ensures the time/battery icons are white 
        so they show up on your dark blue background 
      */}
      <StatusBar style="light" />

      <Stack
        screenOptions={{
          // Set the global background color for all screens
          contentStyle: {
            backgroundColor: theme.colors.background,
          },
          // Style the default header 
          headerStyle: {
            backgroundColor: theme.colors.background,
          },
          headerTintColor: theme.colors.text,
          headerTitleStyle: {
            fontWeight: "bold",
          },
        }}
      >
        {/* We hide the header for "index" (Home) because 
          your HomeScreen has its own custom header with the logo.
        */}
        <Stack.Screen name="index" options={{ headerShown: false }} />
      </Stack>
    </View>
  );
}