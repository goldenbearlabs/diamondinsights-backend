import { useMemo } from "react";
import type { ReactNode } from "react";
import { StyleSheet, Text, TextStyle } from "react-native";
import { useTheme } from "../theme/ThemeProvider";

type Variant = "title" | "subtitle" | "body" | "caption";

type ThemedTextProps = {
  children: ReactNode;
  variant?: Variant;
  style?: TextStyle;
};

export function ThemedText({
  children,
  variant = "body",
  style,
}: ThemedTextProps) {
  const theme = useTheme();
  const styles = useMemo(
    () =>
      StyleSheet.create({
        title: {
          fontSize: theme.typography.sizes.xl,
          lineHeight: theme.typography.lineHeights.xl,
          fontWeight: theme.typography.weights.bold,
          color: theme.colors.text,
        },
        subtitle: {
          fontSize: theme.typography.sizes.lg,
          lineHeight: theme.typography.lineHeights.lg,
          fontWeight: theme.typography.weights.medium,
          color: theme.colors.text,
        },
        body: {
          fontSize: theme.typography.sizes.md,
          lineHeight: theme.typography.lineHeights.md,
          fontWeight: theme.typography.weights.regular,
          color: theme.colors.text,
        },
        caption: {
          fontSize: theme.typography.sizes.sm,
          lineHeight: theme.typography.lineHeights.sm,
          fontWeight: theme.typography.weights.regular,
          color: theme.colors.muted,
        },
      }),
    [theme],
  );

  return <Text style={[styles[variant], style]}>{children}</Text>;
}
