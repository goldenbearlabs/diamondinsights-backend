import { useMemo } from "react";
import type { ReactNode } from "react";
import { SafeAreaView, StyleSheet, ViewStyle } from "react-native";
import { useTheme } from "../theme/ThemeProvider";

type ScreenProps = {
  children: ReactNode;
  style?: ViewStyle;
};

export function Screen({ children, style }: ScreenProps) {
  const theme = useTheme();
  const styles = useMemo(
    () =>
      StyleSheet.create({
        container: {
          flex: 1,
          backgroundColor: theme.colors.background,
          padding: theme.spacing.md,
        },
      }),
    [theme],
  );

  return <SafeAreaView style={[styles.container, style]}>{children}</SafeAreaView>;
}
