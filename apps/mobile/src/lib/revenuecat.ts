import { Linking, Platform } from "react-native";
import Purchases, {
  type CustomerInfo,
  type PurchasesOffering,
  type PurchasesPackage,
} from "react-native-purchases";
import RevenueCatUI, { PAYWALL_RESULT } from "react-native-purchases-ui";

import { RC_API_KEY, RC_PRO_ENTITLEMENT_ID } from "./config";

export type ProPaywallResult = {
  result: PAYWALL_RESULT;
  purchasedOrRestored: boolean;
};

const isNativeRevenueCatPlatform = Platform.OS === "ios" || Platform.OS === "android";

const requireRevenueCatKey = () => {
  if (!RC_API_KEY) {
    throw new Error("Missing EXPO_PUBLIC_RC_TEST_API_KEY");
  }
};

export const isRevenueCatEnabled = (): boolean => isNativeRevenueCatPlatform && Boolean(RC_API_KEY);

export async function initializeRevenueCat(appUserId: string | null = null): Promise<void> {
  if (!isNativeRevenueCatPlatform) return;
  requireRevenueCatKey();

  if (await Purchases.isConfigured()) return;

  Purchases.configure({
    apiKey: RC_API_KEY,
    appUserID: appUserId || undefined,
  });
}

export async function identifyRevenueCatUser(firebaseUid: string | null): Promise<void> {
  if (!isNativeRevenueCatPlatform || !RC_API_KEY) return;
  await initializeRevenueCat(firebaseUid);
  const currentAppUserId = await Purchases.getAppUserID();

  if (!firebaseUid) {
    // Avoid noisy "already anonymous" warnings on cold start.
    if (!currentAppUserId.startsWith("$RCAnonymousID:")) {
      try {
        await Purchases.logOut();
      } catch {
        // Ignore if RevenueCat is already in anonymous mode.
      }
    }
    return;
  }

  if (currentAppUserId !== firebaseUid) {
    await Purchases.logIn(firebaseUid);
  }
}

export async function syncRevenueCatUser(firebaseUid: string | null): Promise<void> {
  await identifyRevenueCatUser(firebaseUid);
}

export async function getRevenueCatCustomerInfo(): Promise<CustomerInfo | null> {
  if (!isNativeRevenueCatPlatform || !RC_API_KEY) return null;
  await initializeRevenueCat();
  return Purchases.getCustomerInfo();
}

export async function getRevenueCatCurrentOffering(): Promise<PurchasesOffering | null> {
  if (!isNativeRevenueCatPlatform || !RC_API_KEY) return null;
  await initializeRevenueCat();
  const offerings = await Purchases.getOfferings();
  return offerings.current;
}

export async function purchaseRevenueCatPackage(
  revenueCatPackage: PurchasesPackage,
): Promise<CustomerInfo> {
  await initializeRevenueCat();
  const result = await Purchases.purchasePackage(revenueCatPackage);
  return result.customerInfo;
}

export async function restoreRevenueCatPurchases(): Promise<CustomerInfo> {
  await initializeRevenueCat();
  return Purchases.restorePurchases();
}

export async function openRevenueCatManageSubscriptions(): Promise<void> {
  if (Platform.OS === "ios") {
    await initializeRevenueCat();
    await Purchases.showManageSubscriptions();
    return;
  }

  if (Platform.OS === "android") {
    const androidSubscriptionsUrl = "https://play.google.com/store/account/subscriptions";
    const canOpen = await Linking.canOpenURL(androidSubscriptionsUrl);
    if (!canOpen) {
      throw new Error("Unable to open subscription management.");
    }
    await Linking.openURL(androidSubscriptionsUrl);
    return;
  }

  throw new Error("Subscription management is only available on iOS and Android.");
}

export const hasRevenueCatProEntitlement = (customerInfo: CustomerInfo | null): boolean =>
  Boolean(customerInfo?.entitlements.active[RC_PRO_ENTITLEMENT_ID]);

export async function getRevenueCatProStatus(): Promise<boolean> {
  return hasRevenueCatProEntitlement(await getRevenueCatCustomerInfo());
}

export async function presentProPaywall(): Promise<ProPaywallResult> {
  await initializeRevenueCat();

  const result = await RevenueCatUI.presentPaywallIfNeeded({
    requiredEntitlementIdentifier: RC_PRO_ENTITLEMENT_ID,
  });

  return {
    result,
    purchasedOrRestored:
      result === PAYWALL_RESULT.PURCHASED || result === PAYWALL_RESULT.RESTORED,
  };
}
