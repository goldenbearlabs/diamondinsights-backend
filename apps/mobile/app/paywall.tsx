import { useCallback, useEffect, useMemo, useState, type ComponentProps } from "react";
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { Ionicons } from "@expo/vector-icons";
import { useRouter } from "expo-router";
import type { CustomerInfo, PurchasesOffering, PurchasesPackage } from "react-native-purchases";

import { auth } from "../src/lib/firebase";
import {
  getRevenueCatCurrentOffering,
  getRevenueCatCustomerInfo,
  hasRevenueCatProEntitlement,
  isRevenueCatEnabled,
  openRevenueCatManageSubscriptions,
  purchaseRevenueCatPackage,
  restoreRevenueCatPurchases,
  syncRevenueCatUser,
} from "../src/lib/revenuecat";
import {
  getMyEntitlements,
  type EntitlementsMeResponse,
} from "../src/lib/api";
import { clearBackendProStatus, setBackendProStatus } from "../src/lib/proStatus";
import { theme } from "../src/theme/colors";

type IoniconName = ComponentProps<typeof Ionicons>["name"];

const PRO_FEATURES: { title: string; description: string; icon: IoniconName }[] = [
  {
    title: "Complete Prediction Dataset",
    description:
      "Unlock the full prediction feed, including individual attributes and deeper trend signals.",
    icon: "sparkles-outline",
  },
  {
    title: "Advanced Prediction Filters",
    description:
      "Run pro-grade stat filters to slice projections by role archetype, upside, and consistency.",
    icon: "options-outline",
  },
  {
    title: "Full Gameplay Analytics",
    description:
      "Get expanded gameplay breakdowns for both your account and the opponents you face.",
    icon: "bar-chart-outline",
  },
  {
    title: '"Your Overall" Engine',
    description:
      "See a meta overall that adapts to your play style, not just a one-size-fits-all rating.",
    icon: "speedometer-outline",
  },
  {
    title: "Full Community Access",
    description:
      "Join the full member layer of the community with deeper discussion and shared strategy.",
    icon: "people-outline",
  },
];

const formatPackageType = (packageType: string): string =>
  packageType
    .replace("$rc_", "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());

const getErrorMessage = (err: unknown, fallback: string): string => {
  if (err instanceof Error && err.message) return err.message;
  if (typeof err === "string" && err.trim()) return err.trim();
  return fallback;
};

const formatDateTime = (raw: string | null | undefined): string | null => {
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toLocaleString();
};

const getRevenueCatPackageProduct = (
  revenueCatPackage: PurchasesPackage,
): PurchasesPackage["product"] | null => {
  const packageWithCompatibilityProduct = revenueCatPackage as PurchasesPackage & {
    storeProduct?: PurchasesPackage["product"];
  };

  return packageWithCompatibilityProduct.product ?? packageWithCompatibilityProduct.storeProduct ?? null;
};

export default function PaywallScreen() {
  const router = useRouter();

  const [customerInfo, setCustomerInfo] = useState<CustomerInfo | null>(null);
  const [offering, setOffering] = useState<PurchasesOffering | null>(null);
  const [backendEntitlements, setBackendEntitlements] = useState<EntitlementsMeResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [syncingBackend, setSyncingBackend] = useState(false);
  const [restoring, setRestoring] = useState(false);
  const [managingSubscription, setManagingSubscription] = useState(false);
  const [purchaseLoadingId, setPurchaseLoadingId] = useState<string | null>(null);
  const [selectedPackageId, setSelectedPackageId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [backendError, setBackendError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const availablePackages = useMemo(() => offering?.availablePackages ?? [], [offering]);
  const selectedPackage =
    availablePackages.find((candidate) => candidate.identifier === selectedPackageId) ??
    availablePackages[0] ??
    null;
  const proActive = hasRevenueCatProEntitlement(customerInfo);
  const backendProActive = Boolean(backendEntitlements?.has_pro);
  const backendProEntitlement = useMemo(
    () =>
      backendEntitlements?.entitlements.find(
        (candidate) => candidate.entitlement_id === backendEntitlements.pro_entitlement_id,
      ) ?? null,
    [backendEntitlements],
  );

  useEffect(() => {
    const packageStillExists = availablePackages.some(
      (candidate) => candidate.identifier === selectedPackageId,
    );
    if (packageStillExists || !availablePackages.length) return;
    setSelectedPackageId(availablePackages[0].identifier);
  }, [availablePackages, selectedPackageId]);

  const loadRevenueCat = useCallback(async (showPrimaryLoader: boolean): Promise<void> => {
    if (showPrimaryLoader) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }

    setError(null);
    setBackendError(null);
    try {
      if (!isRevenueCatEnabled()) {
        setCustomerInfo(null);
        setOffering(null);
        setBackendEntitlements(null);
        clearBackendProStatus();
        setError(
          "RevenueCat is not configured. Add EXPO_PUBLIC_RC_TEST_API_KEY in apps/mobile/.env and run a native development build.",
        );
        return;
      }

      await syncRevenueCatUser(auth.currentUser?.uid ?? null);
      const [nextCustomerInfoResult, nextOfferingResult, nextBackendEntitlementsResult] =
        await Promise.allSettled([
        getRevenueCatCustomerInfo(),
        getRevenueCatCurrentOffering(),
        getMyEntitlements(),
        ]);

      if (nextCustomerInfoResult.status === "rejected") {
        throw nextCustomerInfoResult.reason;
      }
      if (nextOfferingResult.status === "rejected") {
        throw nextOfferingResult.reason;
      }

      setCustomerInfo(nextCustomerInfoResult.value);
      setOffering(nextOfferingResult.value);

      if (nextBackendEntitlementsResult.status === "fulfilled") {
        setBackendEntitlements(nextBackendEntitlementsResult.value);
        setBackendProStatus(Boolean(nextBackendEntitlementsResult.value.has_pro));
      } else {
        setBackendEntitlements(null);
        setBackendError(
          getErrorMessage(
            nextBackendEntitlementsResult.reason,
            "Failed to load backend entitlement status.",
          ),
        );
      }

      if (nextOfferingResult.value?.availablePackages?.length) {
        setSelectedPackageId((current) => current ?? nextOfferingResult.value.availablePackages[0].identifier);
      }
    } catch (err: unknown) {
      const message = getErrorMessage(err, "Failed to load RevenueCat paywall data.");
      setError(message);
      if (!message.toLowerCase().includes("revenuecat")) {
        setBackendError(message);
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  const waitForBackendProSync = useCallback(async (timeoutMs = 30000, intervalMs = 2500): Promise<boolean> => {
    const startedAt = Date.now();

    while (Date.now() - startedAt < timeoutMs) {
      try {
        const nextBackendEntitlements = await getMyEntitlements();
        setBackendEntitlements(nextBackendEntitlements);
        setBackendProStatus(Boolean(nextBackendEntitlements.has_pro));
        setBackendError(null);

        if (nextBackendEntitlements.has_pro) {
          return true;
        }
      } catch (err: unknown) {
        setBackendError(getErrorMessage(err, "Failed to check backend entitlement status."));
      }

      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }

    return false;
  }, []);

  useEffect(() => {
    void loadRevenueCat(true);
  }, [loadRevenueCat]);

  const purchaseSelectedPackage = async () => {
    if (!selectedPackage) return;

    setPurchaseLoadingId(selectedPackage.identifier);
    setError(null);
    setNotice(null);

    try {
      await purchaseRevenueCatPackage(selectedPackage);
      setNotice("Purchase complete. Confirming backend webhook...");
      setSyncingBackend(true);
      const synced = await waitForBackendProSync();
      setNotice(
        synced
          ? "Purchase complete. Backend Pro entitlement is active."
          : "Purchase completed, but backend Pro entitlement is still syncing.",
      );
      await loadRevenueCat(false);
    } catch (err: any) {
      if (err?.userCancelled) {
        setNotice("Purchase cancelled.");
        return;
      }
      setError(err?.message ?? "Unable to complete purchase.");
    } finally {
      setSyncingBackend(false);
      setPurchaseLoadingId(null);
    }
  };

  const restorePurchases = async () => {
    setRestoring(true);
    setError(null);
    setNotice(null);
    try {
      await restoreRevenueCatPurchases();
      setNotice("Restore complete. Confirming backend webhook...");
      setSyncingBackend(true);
      const synced = await waitForBackendProSync();
      setNotice(
        synced
          ? "Restore complete. Backend Pro entitlement is active."
          : "Restore completed, but backend Pro entitlement is still syncing.",
      );
      await loadRevenueCat(false);
    } catch (err: any) {
      setError(err?.message ?? "Unable to restore purchases.");
    } finally {
      setSyncingBackend(false);
      setRestoring(false);
    }
  };

  const manageOrCancelMembership = async () => {
    setManagingSubscription(true);
    setError(null);
    setNotice(null);
    try {
      await openRevenueCatManageSubscriptions();
    } catch (err: any) {
      setError(err?.message ?? "Unable to open subscription management.");
    } finally {
      setManagingSubscription(false);
    }
  };

  return (
    <SafeAreaView style={styles.safeArea} edges={["top", "left", "right"]}>
      <View style={styles.backgroundLayer} pointerEvents="none">
        <View style={styles.topGlow} />
        <View style={styles.bottomGlow} />
      </View>

      <ScrollView contentContainerStyle={styles.container} showsVerticalScrollIndicator={false}>
        <View style={styles.headerRow}>
          <Pressable style={styles.backButton} onPress={() => router.back()}>
            <Ionicons name="chevron-back" size={18} color={theme.colors.text} />
          </Pressable>
          <Text style={styles.headerTitle}>Diamond Insights</Text>
          <View style={styles.backButtonPlaceholder} />
        </View>

        <View style={styles.introCard}>
          <Text style={styles.introEyebrow}>Pro Membership</Text>
          <Text style={styles.introTitle}>Plans for players who want the full edge.</Text>
          <Text style={styles.introBody}>
            Choose a plan below to unlock every premium insight and analytics feature.
          </Text>
          <View style={[styles.statusPill, proActive && styles.statusPillActive]}>
            <Text style={[styles.statusPillText, proActive && styles.statusPillTextActive]}>
              {proActive ? "Pro is Active" : "Pro is Not Active"}
            </Text>
          </View>
          <View style={styles.statusChecks}>
            <Text style={styles.statusCheckText}>
              RevenueCat entitlement: {proActive ? "active" : "inactive"}
            </Text>
            <Text style={styles.statusCheckText}>
              Backend entitlement: {backendProActive ? "active" : "inactive"}
            </Text>
            {backendProEntitlement?.updated_at ? (
              <Text style={styles.statusCheckTextMuted}>
                Backend updated: {formatDateTime(backendProEntitlement.updated_at) ?? "unknown"}
              </Text>
            ) : null}
            {syncingBackend ? (
              <Text style={styles.statusCheckTextMuted}>Checking backend status...</Text>
            ) : null}
            {backendError ? <Text style={styles.backendErrorText}>{backendError}</Text> : null}
            {proActive && !backendProActive ? (
              <Text style={styles.backendWarningText}>
                RevenueCat is active but backend is not. Check webhook URL/auth and app_user_id mapping.
              </Text>
            ) : null}
          </View>
        </View>

        <View style={styles.sectionCard}>
          <Text style={styles.sectionTitle}>What you unlock</Text>
          {PRO_FEATURES.map((feature, idx) => (
            <View key={feature.title} style={styles.unlockCard}>
              <View style={styles.unlockBadge}>
                <Text style={styles.unlockBadgeText}>{idx + 1}</Text>
              </View>
              <View style={styles.unlockContent}>
                <View style={styles.unlockTitleRow}>
                  <Ionicons name={feature.icon} size={16} color="#fbbf24" />
                  <Text style={styles.unlockTitle}>{feature.title}</Text>
                </View>
                <Text style={styles.unlockDescription}>{feature.description}</Text>
              </View>
            </View>
          ))}
        </View>

        <View style={styles.sectionCard}>
          <View style={styles.packagesHeader}>
            <Text style={styles.sectionTitle}>Plans</Text>
            <TouchableOpacity style={styles.refreshButton} onPress={() => void loadRevenueCat(false)}>
              <Text style={styles.refreshButtonText}>{refreshing ? "Refreshing..." : "Refresh"}</Text>
            </TouchableOpacity>
          </View>

          {offering ? <Text style={styles.offeringMeta}>Offering: {offering.identifier}</Text> : null}
          {loading ? (
            <View style={styles.loadingState}>
              <ActivityIndicator color={theme.colors.text} />
            </View>
          ) : null}
          {error ? <Text style={styles.errorText}>{error}</Text> : null}
          {notice ? <Text style={styles.noticeText}>{notice}</Text> : null}

          {!loading && !availablePackages.length ? (
            <Text style={styles.emptyText}>No packages are available in the current offering.</Text>
          ) : null}

          {availablePackages.map((revenueCatPackage) => {
            const product = getRevenueCatPackageProduct(revenueCatPackage);
            const selected = selectedPackage?.identifier === revenueCatPackage.identifier;
            return (
              <Pressable
                key={revenueCatPackage.identifier}
                style={[styles.packageCard, selected && styles.packageCardSelected]}
                onPress={() => setSelectedPackageId(revenueCatPackage.identifier)}
              >
                <View style={styles.packageTopRow}>
                  <Text style={styles.packageTitle}>
                    {product?.title || revenueCatPackage.identifier}
                  </Text>
                  <Text style={styles.packagePrice}>{product?.priceString || "--"}</Text>
                </View>
                <View style={styles.packageMetaRow}>
                  <Text style={styles.packageMeta}>{formatPackageType(revenueCatPackage.packageType)}</Text>
                  {selected ? (
                    <View style={styles.selectedTag}>
                      <Text style={styles.selectedTagText}>Selected</Text>
                    </View>
                  ) : null}
                </View>
              </Pressable>
            );
          })}

          <TouchableOpacity
            style={[styles.purchaseButton, (!selectedPackage || purchaseLoadingId) && styles.disabledButton]}
            disabled={!selectedPackage || Boolean(purchaseLoadingId)}
            onPress={purchaseSelectedPackage}
          >
            <Text style={styles.purchaseButtonText}>
              {purchaseLoadingId ? "Purchasing..." : "Start Pro Membership"}
            </Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.restoreButton, restoring && styles.disabledButton]}
            disabled={restoring}
            onPress={restorePurchases}
          >
            <Text style={styles.restoreButtonText}>{restoring ? "Restoring..." : "Restore Purchases"}</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.manageButton, managingSubscription && styles.disabledButton]}
            disabled={managingSubscription}
            onPress={manageOrCancelMembership}
          >
            <Text style={styles.manageButtonText}>
              {managingSubscription ? "Opening..." : "Manage or Cancel Membership"}
            </Text>
          </TouchableOpacity>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  backgroundLayer: {
    ...StyleSheet.absoluteFillObject,
  },
  topGlow: {
    position: "absolute",
    top: -180,
    left: -120,
    width: 360,
    height: 360,
    borderRadius: 180,
    backgroundColor: "rgba(251, 191, 36, 0.24)",
  },
  bottomGlow: {
    position: "absolute",
    bottom: -220,
    right: -160,
    width: 420,
    height: 420,
    borderRadius: 210,
    backgroundColor: "rgba(59, 130, 246, 0.22)",
  },
  container: {
    paddingHorizontal: theme.spacing.l,
    paddingBottom: theme.spacing.xxl,
    gap: theme.spacing.m,
  },
  headerRow: {
    paddingTop: theme.spacing.s,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  backButton: {
    width: 34,
    height: 34,
    borderRadius: 17,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  backButtonPlaceholder: {
    width: 34,
    height: 34,
  },
  headerTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "800",
    letterSpacing: 0.4,
  },
  introCard: {
    borderRadius: 20,
    padding: theme.spacing.l,
    gap: 10,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    backgroundColor: "rgba(15, 23, 42, 0.7)",
  },
  introEyebrow: {
    color: "#93c5fd",
    fontSize: 11,
    fontWeight: "800",
    textTransform: "uppercase",
    letterSpacing: 1,
  },
  introTitle: {
    color: theme.colors.text,
    fontSize: 24,
    fontWeight: "900",
    lineHeight: 30,
  },
  introBody: {
    color: theme.colors.muted,
    fontSize: 14,
    lineHeight: 20,
  },
  statusPill: {
    marginTop: 4,
    alignSelf: "flex-start",
    borderRadius: 999,
    backgroundColor: "rgba(148,163,184,0.14)",
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.4)",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  statusPillActive: {
    backgroundColor: "rgba(34,197,94,0.18)",
    borderColor: "rgba(34,197,94,0.45)",
  },
  statusPillText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "700",
  },
  statusPillTextActive: {
    color: "#86efac",
  },
  statusChecks: {
    marginTop: 2,
    gap: 2,
  },
  statusCheckText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "600",
  },
  statusCheckTextMuted: {
    color: theme.colors.muted,
    fontSize: 12,
  },
  backendWarningText: {
    color: "#fbbf24",
    fontSize: 12,
    lineHeight: 16,
  },
  backendErrorText: {
    color: theme.colors.error,
    fontSize: 12,
    lineHeight: 16,
  },
  sectionCard: {
    borderRadius: 16,
    padding: theme.spacing.m,
    backgroundColor: "rgba(15,23,42,0.55)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.1)",
    gap: 10,
  },
  sectionTitle: {
    color: theme.colors.text,
    fontSize: 17,
    fontWeight: "800",
  },
  unlockCard: {
    flexDirection: "row",
    alignItems: "flex-start",
    gap: 10,
    paddingVertical: 2,
  },
  unlockBadge: {
    width: 24,
    height: 24,
    borderRadius: 12,
    backgroundColor: "rgba(251,191,36,0.2)",
    borderWidth: 1,
    borderColor: "rgba(251,191,36,0.4)",
    alignItems: "center",
    justifyContent: "center",
    marginTop: 2,
  },
  unlockBadgeText: {
    color: "#fbbf24",
    fontSize: 12,
    fontWeight: "800",
  },
  unlockContent: {
    flex: 1,
    gap: 4,
  },
  unlockTitleRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
  },
  unlockTitle: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  unlockDescription: {
    flex: 1,
    color: theme.colors.muted,
    fontSize: 13,
    lineHeight: 18,
  },
  packagesHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  refreshButton: {
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.18)",
    borderRadius: 10,
    paddingHorizontal: 10,
    paddingVertical: 6,
    backgroundColor: "rgba(255,255,255,0.06)",
  },
  refreshButtonText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  offeringMeta: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  loadingState: {
    paddingVertical: 16,
    alignItems: "center",
    justifyContent: "center",
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
    textAlign: "center",
  },
  noticeText: {
    color: theme.colors.primary,
    fontSize: 13,
    textAlign: "center",
  },
  emptyText: {
    color: theme.colors.muted,
    fontSize: 13,
    textAlign: "center",
    paddingVertical: 8,
  },
  packageCard: {
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.1)",
    backgroundColor: "rgba(255,255,255,0.02)",
    padding: 12,
    gap: 4,
  },
  packageCardSelected: {
    borderColor: "#fbbf24",
    backgroundColor: "rgba(251,191,36,0.12)",
  },
  packageTopRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
  },
  packageTitle: {
    flex: 1,
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  packagePrice: {
    color: "#fbbf24",
    fontSize: 14,
    fontWeight: "800",
  },
  packageMeta: {
    color: theme.colors.muted,
    fontSize: 12,
    textTransform: "capitalize",
  },
  packageMetaRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 8,
  },
  selectedTag: {
    borderRadius: 999,
    borderWidth: 1,
    borderColor: "rgba(251,191,36,0.45)",
    backgroundColor: "rgba(251,191,36,0.16)",
    paddingHorizontal: 8,
    paddingVertical: 2,
  },
  selectedTagText: {
    color: "#fbbf24",
    fontSize: 11,
    fontWeight: "700",
  },
  purchaseButton: {
    marginTop: 6,
    borderRadius: 12,
    paddingVertical: 12,
    alignItems: "center",
    backgroundColor: "#fbbf24",
  },
  purchaseButtonText: {
    color: "#111827",
    fontSize: 14,
    fontWeight: "800",
  },
  restoreButton: {
    borderRadius: 12,
    paddingVertical: 11,
    alignItems: "center",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.2)",
    backgroundColor: "rgba(255,255,255,0.04)",
  },
  restoreButtonText: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "700",
  },
  manageButton: {
    borderRadius: 12,
    paddingVertical: 11,
    alignItems: "center",
    borderWidth: 1,
    borderColor: "rgba(148,163,184,0.32)",
    backgroundColor: "rgba(148,163,184,0.08)",
  },
  manageButtonText: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "700",
  },
  disabledButton: {
    opacity: 0.65,
  },
});
