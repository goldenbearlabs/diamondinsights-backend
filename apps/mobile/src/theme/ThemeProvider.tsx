import { createContext, useContext, useMemo } from "react";
import type { ReactNode } from "react";
import { useColorScheme } from "react-native";
import { darkTheme, lightTheme, Theme } from "./index";

type ThemeMode = "light" | "dark";
type ThemeProviderProps = {
  children: ReactNode;
  override?: ThemeMode;
};

const ThemeContext = createContext<Theme>(lightTheme);

export function ThemeProvider({ children, override }: ThemeProviderProps) {
  const colorScheme = useColorScheme();
  const theme = useMemo(() => {
    const mode: ThemeMode =
      override || (colorScheme === "dark" ? "dark" : "light");
    return mode === "dark" ? darkTheme : lightTheme;
  }, [colorScheme, override]);

  return <ThemeContext.Provider value={theme}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  return useContext(ThemeContext);
}
