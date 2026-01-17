import type { ReactNode } from "react";
import { Children } from "react";
import { Pressable, StyleSheet, Text, View, ViewStyle } from "react-native";
import { useTheme } from "../theme/ThemeProvider";

type Variant = "primary" | "ghost";

type ButtonProps = {
  children: ReactNode;
  leftIcon?: ReactNode;
  rightIcon?: ReactNode;
  onPress?: () => void;
  variant?: Variant;
  style?: ViewStyle;
};

export function Button({
  children,
  leftIcon,
  rightIcon,
  onPress,
  variant = "primary",
  style,
}: ButtonProps) {
  const theme = useTheme();
  const styles = StyleSheet.create({
    base: {
      paddingVertical: theme.spacing.sm,
      paddingHorizontal: theme.spacing.lg,
      borderRadius: theme.radius.md,
      justifyContent: "center",
    },
    content: {
      flexDirection: "row",
      alignItems: "center",
      justifyContent: "center",
    },
    iconLeft: {
      marginRight: theme.spacing.sm,
    },
    iconRight: {
      marginLeft: theme.spacing.sm,
    },
    primary: {
      backgroundColor: theme.colors.primary,
    },
    ghost: {
      backgroundColor: "transparent",
      borderWidth: 1,
      borderColor: theme.colors.border,
    },
    primaryText: {
      color: theme.colors.primaryText,
      fontWeight: theme.typography.weights.bold,
    },
    ghostText: {
      color: theme.colors.text,
      fontWeight: theme.typography.weights.medium,
    },
  });

  const textStyle = variant === "primary" ? styles.primaryText : styles.ghostText;
  const content = Children.map(children, (child) => {
    if (typeof child === "string" || typeof child === "number") {
      return <Text style={textStyle}>{child}</Text>;
    }
    return child;
  });

  return (
    <Pressable
      style={[styles.base, styles[variant], style]}
      onPress={onPress}
    >
      <View style={styles.content}>
        {leftIcon ? <View style={styles.iconLeft}>{leftIcon}</View> : null}
        {content}
        {rightIcon ? <View style={styles.iconRight}>{rightIcon}</View> : null}
      </View>
    </Pressable>
  );
}
