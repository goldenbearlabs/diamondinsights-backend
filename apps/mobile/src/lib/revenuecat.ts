import Purchases from "react-native-purchases";
import RevenueCatUI, { PAYWALL_RESULT } from "react-native-purchases-ui";

import { RC_API_KEY, RC_PRO_ENTITLEMENT_ID } from "./config";

export type ProPaywallResult = {
  result: PAYWALL_RESULT;
  purchasedOrRestored: boolean;
};

export async function syncRevenueCatUser(firebaseUid: string | null): Promise<void> {
  if (!RC_API_KEY) return;

  const configured = await Purchases.isConfigured();
  if (!configured) {
    Purchases.configure({
      apiKey: RC_API_KEY,
      appUserID: firebaseUid || undefined,
    });
    return;
  }

  if (!firebaseUid) {
    try {
      await Purchases.logOut();
    } catch {
      // Ignore if RevenueCat is already in anonymous mode.
    }
    return;
  }

  const info = await Purchases.getCustomerInfo();
  if (info.originalAppUserId !== firebaseUid) {
    await Purchases.logIn(firebaseUid);
  }
}

export async function getRevenueCatProStatus(): Promise<boolean> {
  if (!RC_API_KEY) return false;
  if (!(await Purchases.isConfigured())) return false;

  const info = await Purchases.getCustomerInfo();
  return Boolean(info.entitlements.active[RC_PRO_ENTITLEMENT_ID]);
}

export async function presentProPaywall(): Promise<ProPaywallResult> {
  if (!RC_API_KEY) {
    throw new Error("Missing EXPO_PUBLIC_RC_API_KEY");
  }

  const result = await RevenueCatUI.presentPaywallIfNeeded({
    requiredEntitlementIdentifier: RC_PRO_ENTITLEMENT_ID,
  });

  return {
    result,
    purchasedOrRestored:
      result === PAYWALL_RESULT.PURCHASED || result === PAYWALL_RESULT.RESTORED,
  };
}
