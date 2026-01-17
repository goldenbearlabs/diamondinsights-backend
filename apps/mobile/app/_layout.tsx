import { Stack } from "expo-router";
import { ThemeProvider, useTheme } from "../src/theme/ThemeProvider";

function RootStack() {
  const theme = useTheme();
  return (
    <Stack
      screenOptions={{
        headerTitle: "Diamond Insights",
        headerStyle: { backgroundColor: theme.colors.surface },
        headerTitleStyle: { color: theme.colors.text },
        headerTintColor: theme.colors.text,
      }}
    />
  );
}

export default function RootLayout() {
  return (
    <ThemeProvider>
      <RootStack />
    </ThemeProvider>
  );
}
