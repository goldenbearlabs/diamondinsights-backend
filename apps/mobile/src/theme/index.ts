import { darkColors, lightColors } from "./colors";
import { radius, spacing, typography } from "./tokens";

export type Colors = typeof lightColors;
export type Theme = {
  colors: Colors;
  spacing: typeof spacing;
  radius: typeof radius;
  typography: typeof typography;
};

export const lightTheme: Theme = {
  colors: lightColors,
  spacing,
  radius,
  typography,
};

export const darkTheme: Theme = {
  colors: darkColors,
  spacing,
  radius,
  typography,
};
