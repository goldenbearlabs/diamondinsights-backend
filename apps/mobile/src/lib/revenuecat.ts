import { Linking, Platform } from "react-native";
import Purchases, {
  LOG_LEVEL,
  type CustomerInfo,
  type PurchasesOffering,
  type PurchasesOfferings,
  type PurchasesPackage,
} from "react-native-purchases";
import RevenueCatUI, { PAYWALL_RESULT } from "react-native-purchases-ui";

import { RC_API_KEY, RC_API_KEY_SOURCE, RC_PRO_ENTITLEMENT_ID } from "./config";

export type ProPaywallResult = {
  result: PAYWALL_RESULT;
  purchasedOrRestored: boolean;
};

const isNativeRevenueCatPlatform = Platform.OS === "ios" || Platform.OS === "android";
let revenueCatDebugLoggingConfigured = false;

export type RevenueCatDebugState = {
  platform: string;
  hasApiKey: boolean;
  apiKeySource: string | null;
  entitlementId: string;
  isConfigured: boolean;
  appUserId: string | null;
  currentOfferingId: string | null;
  allOfferingIds: string[];
  packageCountsByOfferingId: Record<string, number>;
};

const requireRevenueCatKey = () => {
  if (!RC_API_KEY) {
    throw new Error("Missing EXPO_PUBLIC_RC_API_KEY or EXPO_PUBLIC_RC_TEST_API_KEY");
  }
};

export const isRevenueCatEnabled = (): boolean => isNativeRevenueCatPlatform && Boolean(RC_API_KEY);

async function configureRevenueCatDebugLogging(): Promise<void> {
  if (!__DEV__ || revenueCatDebugLoggingConfigured) return;
  await Purchases.setLogLevel(LOG_LEVEL.DEBUG);
  Purchases.setLogHandler((level, message) => {
    console.log(`[revenuecat:${String(level).toLowerCase()}] ${message}`);
  });
  revenueCatDebugLoggingConfigured = true;
}

export async function initializeRevenueCat(appUserId: string | null = null): Promise<void> {
  if (!isNativeRevenueCatPlatform) return;
  requireRevenueCatKey();

  if (await Purchases.isConfigured()) {
    await configureRevenueCatDebugLogging();
    return;
  }

  Purchases.configure({
    apiKey: RC_API_KEY,
    appUserID: appUserId || undefined,
    diagnosticsEnabled: __DEV__,
  });
  await configureRevenueCatDebugLogging();
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

export async function getRevenueCatOfferings(): Promise<PurchasesOfferings | null> {
  if (!isNativeRevenueCatPlatform || !RC_API_KEY) return null;
  await initializeRevenueCat();
  return Purchases.getOfferings();
}

export async function getRevenueCatCurrentOffering(): Promise<PurchasesOffering | null> {
  const offerings = await getRevenueCatOfferings();
  return offerings?.current ?? null;
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

export async function getRevenueCatDebugState(
  offeringsOverride: PurchasesOfferings | null = null,
): Promise<RevenueCatDebugState> {
  const isConfigured = isNativeRevenueCatPlatform && RC_API_KEY ? await Purchases.isConfigured() : false;
  const offerings = offeringsOverride ?? (isConfigured ? await Purchases.getOfferings() : null);
  const appUserId = isConfigured ? await Purchases.getAppUserID() : null;
  const allOfferings = offerings ? Object.values(offerings.all) : [];

  return {
    platform: Platform.OS,
    hasApiKey: Boolean(RC_API_KEY),
    apiKeySource: RC_API_KEY_SOURCE,
    entitlementId: RC_PRO_ENTITLEMENT_ID,
    isConfigured,
    appUserId,
    currentOfferingId: offerings?.current?.identifier ?? null,
    allOfferingIds: allOfferings.map((offering) => offering.identifier),
    packageCountsByOfferingId: Object.fromEntries(
      allOfferings.map((offering) => [offering.identifier, offering.availablePackages.length]),
    ),
  };
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
