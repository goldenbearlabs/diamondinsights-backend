export const API_BASE_URL = (process.env.EXPO_PUBLIC_API_BASE_URL || "").trim();
export const RC_API_KEY = (
  process.env.EXPO_PUBLIC_RC_TEST_API_KEY ||
  process.env.EXPO_PUBLIC_RC_API_KEY ||
  ""
).trim();
export const RC_PRO_ENTITLEMENT_ID = (
  process.env.EXPO_PUBLIC_RC_ENTITLEMENT_ID ||
  process.env.EXPO_PUBLIC_RC_PRO_ENTITLEMENT_ID ||
  "pro"
).trim();
