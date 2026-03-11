import { useEffect, useState } from "react";
import {
  ActivityIndicator,
  Alert,
  DeviceEventEmitter,
  Image,
  Linking,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  TouchableOpacity,
  View,
  KeyboardAvoidingView,
  Platform,
  TouchableWithoutFeedback,
  Keyboard

} from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { Ionicons } from "@expo/vector-icons";
import { useLocalSearchParams, useRouter } from "expo-router";
import * as ImagePicker from "expo-image-picker";
import { sendPasswordResetEmail, signOut } from "firebase/auth";

import {
  ApiError,
  apiDeleteAuth,
  apiGetAuth,
  apiPostAuth,
  apiPutAuth,
} from "../../src/lib/api";
import { toReadableAuthError } from "../../src/lib/authErrors";
import { auth } from "../../src/lib/firebase";
import { invalidateAvatarCache } from "../../src/lib/profileImage";
import { useBackendProStatus } from "../../src/lib/proStatus";
import { openRevenueCatManageSubscriptions } from "../../src/lib/revenuecat";
import { uploadProfileImage } from "../../src/lib/storage";
import { WEB_BASE_URL } from "../../src/lib/config";
import { Avatar } from "../../src/components/Avatar";
import { theme } from "../../src/theme/colors";

const STUB_ICON = require("../../assets/images/stub.png");
const PRIVACY_POLICY_URL = `${WEB_BASE_URL}/privacy-policy`;
const TERMS_AND_CONDITIONS_URL = `${WEB_BASE_URL}/terms-and-conditions`;

type UserProfile = {
  id: number;
  firebase_id?: string | null;
  email?: string | null;
  display_name: string;
  profile_img_path: string;
  latest_points_total?: number | null;
  is_me: boolean;
  description?: string | null;
};

type ShowProfile = {
  username: string;
  display_level?: number | null;
  games_played?: number | null;
  linked_at: string;
  last_refreshed_at: string;
  online_stats: {
    year: number;
    wins?: number | null;
    losses?: number | null;
    hr?: number | null;
    runs_per_game?: number | null;
    stolen_bases?: number | null;
    batting_average?: number | null;
    era?: number | null;
    k_per_9?: number | null;
    whip?: number | null;
  }[];
};

type PortfolioHolding = {
  card_id: string;
  quantity: number;
  avg_price: number | null;
  card: {
    id: string;
    name: string;
    baked_img: string;
    ovr: number;
    predicted_ovr: number | null;
  };
};

type UserPortfolio = {
  id: number;
  name: string;
  is_public: boolean;
  holdings: PortfolioHolding[];
};

const formatPortfolioStubs = (n: number): string => {
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
};

const formatScore = (value: number | null | undefined): string => {
  if (value === null || value === undefined) return "-";
  return value.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 2 });
};

export default function AccountScreen() {
  const params = useLocalSearchParams<{ userId?: string | string[] }>();
  const userId = Array.isArray(params.userId) ? params.userId[0] : params.userId;
  const router = useRouter();

  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [showProfile, setShowProfile] = useState<ShowProfile | null>(null);
  const [showLoading, setShowLoading] = useState(false);
  const [showError, setShowError] = useState<string | null>(null);
  const [portfolioData, setPortfolioData] = useState<UserPortfolio | null>(null);
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioError, setPortfolioError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"Investing" | "Gameplay">("Gameplay");

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [deletingAccount, setDeletingAccount] = useState(false);
  const [signingOut, setSigningOut] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [modalError, setModalError] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editEmail, setEditEmail] = useState("");
  const [editDescription, setEditDescription] = useState("");
  const [newPhotoUri, setNewPhotoUri] = useState<string | null>(null);
  const [linkOpen, setLinkOpen] = useState(false);
  const [linkUsername, setLinkUsername] = useState("");
  const [linkNotice, setLinkNotice] = useState<string | null>(null);
  const [linkError, setLinkError] = useState<string | null>(null);
  const [linking, setLinking] = useState(false);
  const [managingMembership, setManagingMembership] = useState(false);
  const [membershipError, setMembershipError] = useState<string | null>(null);
  const { isPro: isBackendPro, loading: proStatusLoading, refresh: refreshProStatus } = useBackendProStatus();

  useEffect(() => {
    let active = true;

    const loadProfile = async () => {
      setLoading(true);
      setPageError(null);
      try {
        const path = userId ? `/users/${userId}` : "/users/me";
        const data = await apiGetAuth<UserProfile>(path);
        if (!active) return;
        setProfile(data);
        setEditName(data.display_name);
        setEditEmail(data.email ?? "");
        setEditDescription(data.description ?? "");
      } catch (err: any) {
        if (!active) return;
        setPageError(err?.message ?? "Failed to load profile");
      } finally {
        if (active) setLoading(false);
      }
    };

    loadProfile();

    return () => {
      active = false;
    };
  }, [userId]);

  useEffect(() => {
    if (!profile) return;
    let active = true;

    const loadShowProfile = async () => {
      setShowLoading(true);
      setShowError(null);
      setShowProfile(null);
      try {
        const path = profile.is_me ? "/users/me/show" : `/users/${profile.id}/show`;
        const data = await apiGetAuth<ShowProfile>(path);
        if (!active) return;
        setShowProfile(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 404) {
          setShowProfile(null);
          setShowError(null);
        } else {
          setShowError(err?.message ?? "Failed to load The Show profile");
        }
      } finally {
        if (active) setShowLoading(false);
      }
    };

    loadShowProfile();

    return () => {
      active = false;
    };
  }, [profile, profile?.id, profile?.is_me]);

  useEffect(() => {
    let active = true;

    const loadPortfolio = async () => {
      if (!profile) return;
      setPortfolioLoading(true);
      setPortfolioError(null);
      try {
        const path = profile.is_me 
          ? "/portfolios/me" 
          : `/portfolios/users/${profile.id}/portfolio`;
        const data = await apiGetAuth<UserPortfolio>(path);
        if (!active) return;
        setPortfolioData(data);
      } catch (err: any) {
        if (!active) return;
        if (err instanceof ApiError && err.status === 403) {
          setPortfolioData(null);
          setPortfolioError("private");
        } else if (err instanceof ApiError && err.status === 404) {
          setPortfolioData(null);
          setPortfolioError("none");
        } else {
          setPortfolioError(err?.message ?? "Failed to load portfolio");
        }
      } finally {
        if (active) setPortfolioLoading(false);
      }
    };

    if (activeTab === "Investing") {
      loadPortfolio();
    }

    return () => {
      active = false;
    };
  }, [profile, profile?.id, profile?.is_me, activeTab]);

  useEffect(() => {
    if (!profile?.is_me) return;
    void refreshProStatus(true);
  }, [profile?.is_me, refreshProStatus]);


  const openSettings = () => {
    if (!profile) return;
    setEditName(profile.display_name);
    setEditEmail(profile.email ?? "");
    setEditDescription
    setNewPhotoUri(null);
    setNotice(null);
    setModalError(null);
    setSettingsOpen(true);
  };

  const pickProfilePhoto = async () => {
    setNotice(null);

    const { status, canAskAgain } = await ImagePicker.getMediaLibraryPermissionsAsync();

    if (status === "denied" && !canAskAgain) {
      Alert.alert(
        "Photo Access Required",
        "You previously denied photo access. Please enable it in Settings to set a profile photo.",
        [
          { text: "Cancel", style: "cancel" },
          { text: "Open Settings", onPress: () => Linking.openSettings() },
        ]
      );
      return;
    }

    if (status !== "granted") {
      await new Promise<void>((resolve) =>
        Alert.alert(
          "Photo Access",
          "Diamond Insights needs access to your photo library to let you set a profile photo.",
          [{ text: "Continue", onPress: () => resolve() }]
        )
      );
      const perm = await ImagePicker.requestMediaLibraryPermissionsAsync();
      if (!perm.granted) {
        setNotice("Photo permission denied.");
        return;
      }
    }

    const result = await ImagePicker.launchImageLibraryAsync({
      mediaTypes: ["images"],
      allowsEditing: true,
      aspect: [1, 1],
      quality: 0.85,
    });
    if (!result.canceled) setNewPhotoUri(result.assets[0].uri);
  };

  const openLinkModal = () => {
    setLinkUsername("");
    setLinkNotice(null);
    setLinkError(null);
    setLinkOpen(true);
  };

  const linkShowProfile = async () => {
    const username = linkUsername.trim();
    if (!username) {
      setLinkError("Username is required.");
      return;
    }
    setLinkError(null);
    setLinkNotice(null);
    setLinking(true);
    try {
      const data = await apiPostAuth<ShowProfile>("/users/me/show/link", { username });
      setShowProfile(data);
      setLinkOpen(false);
      setLinkUsername("");
      setLinkNotice("Account linked.");
    } catch (err: any) {
      if (err instanceof ApiError) {
        if (err.status === 404) {
          setLinkError("No MLB The Show account found for that username.");
        } else if (err.status === 409) {
          setLinkError("That MLB The Show username is already linked. If this is your account, please contact support@goldenbearlabs.com.");
        } else {
          setLinkError(err.body || "Failed to link account");
        }
      } else {
        setLinkError(err?.message ?? "Failed to link account");
      }
    } finally {
      setLinking(false);
    }
  };


  const saveProfile = async () => {
    if (!profile) return;
    setSaving(true);
    setNotice(null);
    setModalError(null);
    try {
      const updates: Record<string, any> = {};
      const nextName = editName.trim();
      const nextEmail = editEmail.trim();
      const nextDescription = editDescription.trim();

      if (nextName && nextName !== profile.display_name) {
        updates.display_name = nextName;
      }
      if (nextEmail && nextEmail !== (profile.email ?? "")) {
        updates.email = nextEmail;
      }
      if (nextDescription !== (profile.description ?? "")) {
        updates.description = nextDescription === "" ? null : nextDescription;
      }

      let profileImgPath = profile.profile_img_path;
      if (newPhotoUri) {
        const uid = auth.currentUser?.uid;
        if (!uid) throw new Error("Not authenticated");
        profileImgPath = await uploadProfileImage(newPhotoUri, uid);
        updates.profile_img_path = profileImgPath;
      }

      if (Object.keys(updates).length === 0) {
        setSettingsOpen(false);
        return;
      }

      const updated = await apiPutAuth<UserProfile>("/users/me", updates);
      setProfile(updated);
      if (newPhotoUri) {
        invalidateAvatarCache(updated.profile_img_path);
        DeviceEventEmitter.emit("profile-image-updated");
      }
      setSettingsOpen(false);
      setNewPhotoUri(null);
      setNotice("Profile updated.");
    } catch (err: any) {
      setModalError(err?.message ?? "Failed to update profile");
    } finally {
      setSaving(false);
    }
  };

  const resetPassword = async () => {
    const email = profile?.email ?? editEmail.trim();
    if (!email) {
      setNotice("Email is required to reset password.");
      return;
    }
    setSaving(true);
    setNotice(null);
    setModalError(null);
    try {
      await sendPasswordResetEmail(auth, email);
      setNotice("Password reset email sent.");
    } catch (err: any) {
      setModalError(err?.message ?? "Failed to send reset email");
    } finally {
      setSaving(false);
    }
  };

  const deleteAccount = async () => {
    setDeletingAccount(true);
    setNotice(null);
    setModalError(null);
    try {
      await apiDeleteAuth<void>("/users/me");
      await signOut(auth).catch(() => undefined);
      setSettingsOpen(false);
      router.replace("/(auth)/signin");
    } catch (err: unknown) {
      setModalError(toReadableAuthError(err, "Failed to delete account"));
    } finally {
      setDeletingAccount(false);
    }
  };

  const confirmDeleteAccount = () => {
    Alert.alert(
      "Delete account",
      "This permanently deletes your account and data. This cannot be undone.",
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Delete",
          style: "destructive",
          onPress: () => {
            void deleteAccount();
          },
        },
      ],
    );
  };

  const confirmLogOut = () => {
    Alert.alert(
      "Log out",
      "Are you sure you want to log out? You will need your password to sign back in.",
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Log out",
          style: "destructive", 
          onPress: async () => {
            setSigningOut(true);
            try {
              await signOut(auth);
              setSettingsOpen(false);
              router.replace("/(auth)/signin");
            } catch (err: any) {
              setModalError(err?.message ?? "Failed to log out");
            } finally {
              setSigningOut(false);
            }
          },
        },
      ]
    );
  };

  const manageMembership = async () => {
    setManagingMembership(true);
    setMembershipError(null);
    try {
      await openRevenueCatManageSubscriptions();
    } catch (err: any) {
      setMembershipError(err?.message ?? "Unable to open subscription management.");
    } finally {
      setManagingMembership(false);
    }
  };

  const openLegalDocument = async (url: string) => {
    try {
      await Linking.openURL(url);
    } catch {
      Alert.alert("Unable to open link", "Please try again in a moment.");
    }
  };

  const sortedOnlineStats = [...(showProfile?.online_stats ?? [])].sort((a, b) => a.year - b.year);
  const summaryStats = sortedOnlineStats.length ? sortedOnlineStats[sortedOnlineStats.length - 1] : null;
  const aggregateRecord = sortedOnlineStats.reduce(
    (acc, row) => {
      acc.wins += row.wins ?? 0;
      acc.losses += row.losses ?? 0;
      if (row.wins !== null && row.wins !== undefined) acc.hasWins = true;
      if (row.losses !== null && row.losses !== undefined) acc.hasLosses = true;
      return acc;
    },
    { wins: 0, losses: 0, hasWins: false, hasLosses: false },
  );
  const recordText =
    aggregateRecord.hasWins || aggregateRecord.hasLosses
      ? `${aggregateRecord.wins}-${aggregateRecord.losses}`
      : "-";
  const formatStat = (value?: number | null) =>
    value === null || value === undefined ? "-" : String(value);
  const formatFloat = (value?: number | null, digits = 2) =>
    value === null || value === undefined ? "-" : value.toFixed(digits);

  return (
    <SafeAreaView style={styles.safeArea} edges={["left", "right"]}>
      <ScrollView contentContainerStyle={styles.container} showsVerticalScrollIndicator={false}>
        <View style={styles.headerRow}>
          <View style={{ flexDirection: 'row', alignItems: 'center', gap: 12, flex: 1, paddingRight: 16 }}>
            {userId ? (
              <TouchableOpacity 
                onPress={() => router.back()} 
                hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}
              >
                <Ionicons name="arrow-back" size={24} color={theme.colors.text} />
              </TouchableOpacity>
            ) : null}
            <Text 
              style={[styles.title, { flexShrink: 1 }]}
              numberOfLines={1}
              adjustsFontSizeToFit
              minimumFontScale={0.65}
            >
              {profile && !profile.is_me ? `${profile.display_name}'s Account` : "Account"}
            </Text>
          </View>

          {profile?.is_me ? (
            <TouchableOpacity style={styles.settingsButton} onPress={openSettings}>
              <Ionicons name="settings-outline" size={20} color={theme.colors.text} />
            </TouchableOpacity>
          ) : null}
        </View>

        {loading ? (
          <View style={styles.loadingState}>
            <ActivityIndicator color={theme.colors.text} />
          </View>
        ) : pageError ? (
          <View style={styles.loadingState}>
            <Text style={styles.errorText}>{pageError}</Text>
          </View>
        ) : profile ? (
          <>
            <View style={styles.profileHeader}>
              <View style={styles.avatarFrame}>
                <Avatar
                  firebasePath={profile.profile_img_path}
                  size={120}
                />
              </View>
              <View style={styles.profileInfo}>
                <View>
                  <Text 
                    style={styles.nameLarge}
                    numberOfLines={1}                
                    adjustsFontSizeToFit={true}      
                    minimumFontScale={0.7}           
                  >
                    {profile.display_name}
                  </Text>
                  {profile.description ? (
                    <Text style={{ marginTop: 4, color: theme.colors.muted, fontSize: 13, lineHeight: 18 }} numberOfLines={3}>
                      {profile.description}
                    </Text>
                  ) : null}
                </View>
                <View style={styles.summaryRow}>
                  <View style={styles.summaryItem}>
                    <Text style={[styles.summaryValue, styles.summaryValueAccent]}>
                      {formatScore(profile.latest_points_total)}
                    </Text>
                    <Text style={styles.summaryLabel}>Score</Text>
                  </View>
                  <View style={styles.summaryItem}>
                    <Text style={styles.summaryValue}>{recordText}</Text>
                    <Text style={styles.summaryLabel}>Record</Text>
                  </View>
                </View>
              </View>
            </View>

            {profile.is_me ? (
              proStatusLoading && isBackendPro === null ? (
                <View style={styles.proManageCard}>
                  <ActivityIndicator color={theme.colors.text} />
                </View>
              ) : isBackendPro ? (
                <View style={styles.proManageCard}>
                  <View style={styles.proManageRow}>
                    <Text style={styles.proManageTitle}>Pro membership active</Text>
                    <TouchableOpacity
                      style={[styles.proManageButton, managingMembership && styles.buttonDisabled]}
                      onPress={manageMembership}
                      disabled={managingMembership}
                    >
                      <Text style={styles.proManageButtonText}>
                        {managingMembership ? "Opening..." : "Manage Subscription"}
                      </Text>
                    </TouchableOpacity>
                  </View>
                  {membershipError ? <Text style={styles.errorText}>{membershipError}</Text> : null}
                </View>
              ) : (
                <View style={styles.proCtaCard}>
                  <Text style={styles.proCtaTitle}>Become Pro Member</Text>
                  <Text style={styles.proCtaDescription}>
                    Unlock premium Diamond Insights features with a Pro membership.
                  </Text>
                  <TouchableOpacity
                    style={styles.proCtaButton}
                    onPress={() => router.push("/paywall")}
                  >
                    <Text style={styles.proCtaButtonText}>Become Pro Member</Text>
                  </TouchableOpacity>
                </View>
              )
            ) : null}

            <View style={styles.tabRow}>
              <TouchableOpacity
                style={[
                  styles.tabButton,
                  activeTab === "Investing" && styles.tabButtonActive,
                ]}
                onPress={() => setActiveTab("Investing")}
              >
                <Text
                  style={[
                    styles.tabText,
                    activeTab === "Investing" && styles.tabTextActive,
                  ]}
                >
                  Investing
                </Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[
                  styles.tabButton,
                  activeTab === "Gameplay" && styles.tabButtonActive,
                ]}
                onPress={() => setActiveTab("Gameplay")}
              >
                <Text
                  style={[
                    styles.tabText,
                    activeTab === "Gameplay" && styles.tabTextActive,
                  ]}
                >
                  Gameplay
                </Text>
              </TouchableOpacity>
            </View>

            <View style={styles.tabCard}>
              {activeTab === "Investing" ? (
                portfolioLoading ? (
                  <View style={styles.loadingInline}>
                    <ActivityIndicator color={theme.colors.text} />
                  </View>
                ) : portfolioError === "private" ? (
                  <View style={styles.portfolioPrivateContainer}>
                    <Ionicons name="lock-closed" size={24} color="rgba(255,255,255,0.2)" />
                    <Text style={styles.sectionText}>
                      {profile?.display_name}'s portfolio is private
                    </Text>
                  </View>
                ) : portfolioError === "none" ? (
                  <Text style={styles.sectionText}>No portfolio found</Text>
                ) : portfolioError ? (
                  <Text style={styles.errorText}>{portfolioError}</Text>
                ) : portfolioData ? (
                  <TouchableOpacity
                    style={styles.portfolioOverview}
                    activeOpacity={0.7}
                    onPress={() => {
                      if (profile?.is_me) {
                        router.push("/(app)/portfolio");
                      } else if (profile) {
                        router.push({
                          pathname: "/portfolio/[userId]",
                          params: {
                            userId: profile.id.toString(),
                            username: profile.display_name,
                          },
                        });
                      }
                    }}
                  >
                    <View style={styles.portfolioOverviewRow}>
                      <View style={styles.portfolioOverviewStat}>
                        <Text style={styles.portfolioOverviewLabel}>Total Invested</Text>
                        <View style={styles.portfolioOverviewValueRow}>
                          <Image source={STUB_ICON} style={styles.stubIconSmall} />
                          <Text style={styles.portfolioOverviewValue}>
                            {formatPortfolioStubs(
                              portfolioData.holdings.reduce(
                                (sum, h) => sum + h.quantity * (h.avg_price ?? 0),
                                0
                              )
                            )}
                          </Text>
                        </View>
                      </View>
                      <View style={styles.portfolioOverviewDivider} />
                      <View style={styles.portfolioOverviewStat}>
                        <Text style={styles.portfolioOverviewLabel}>Total Cards</Text>
                        <Text style={styles.portfolioOverviewValue}>
                          {portfolioData.holdings.length}
                        </Text>
                      </View>
                      <Ionicons name="chevron-forward" size={16} color={theme.colors.muted} />
                    </View>
                  </TouchableOpacity>
                ) : (
                  <Text style={styles.sectionText}>No investments yet</Text>
                )
              ) : showLoading ? (
                <View style={styles.loadingInline}>
                  <ActivityIndicator color={theme.colors.text} />
                </View>
              ) : showError ? (
                <Text style={styles.errorText}>{showError}</Text>
              ) : showProfile ? (
                <>
                  <View style={styles.sectionHeaderRow}>
                    <Text style={styles.sectionTitle}>MLB The Show</Text>
                    <Text style={styles.sectionMeta}>{showProfile.username}</Text>
                  </View>

                  <View style={styles.detailGrid}>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>Level</Text>
                      <Text style={styles.detailValue}>
                        {formatStat(showProfile.display_level)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>Games Played</Text>
                      <Text style={styles.detailValue}>
                        {formatStat(showProfile.games_played)}
                      </Text>
                    </View>
                  </View>

                  <View style={styles.onlineRow}>
                    <View style={styles.onlineLine} />
                    <Text style={styles.onlineText}>Online</Text>
                    <View style={styles.onlineLine} />
                  </View>

                  <View style={styles.detailGrid}>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>Wins</Text>
                      <Text style={styles.detailValue}>
                        {formatStat(summaryStats?.wins)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>Losses</Text>
                      <Text style={styles.detailValue}>
                        {formatStat(summaryStats?.losses)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>ER</Text>
                      <Text style={styles.detailValue}>
                        {formatFloat(summaryStats?.runs_per_game, 2)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>ERA</Text>
                      <Text style={styles.detailValue}>
                        {formatFloat(summaryStats?.era, 2)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>AVG</Text>
                      <Text style={styles.detailValue}>
                        {formatFloat(summaryStats?.batting_average, 3)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>K/9</Text>
                      <Text style={styles.detailValue}>
                        {formatFloat(summaryStats?.k_per_9, 2)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>HR</Text>
                      <Text style={styles.detailValue}>{formatStat(summaryStats?.hr)}</Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>WHIP</Text>
                      <Text style={styles.detailValue}>
                        {formatFloat(summaryStats?.whip, 2)}
                      </Text>
                    </View>
                    <View style={styles.detailItem}>
                      <Text style={styles.detailLabel}>SB</Text>
                      <Text style={styles.detailValue}>
                        {formatStat(summaryStats?.stolen_bases)}
                      </Text>
                    </View>
                  </View>
                </>
              ) : profile.is_me ? (
                <>
                  <Text style={styles.sectionText}>
                    Link your MLB The Show account to show gameplay stats.
                  </Text>
                  <TouchableOpacity style={styles.linkButton} onPress={openLinkModal}>
                    <Text style={styles.linkButtonText}>Link MLB The Show account</Text>
                  </TouchableOpacity>
                  {linkNotice ? <Text style={styles.noticeText}>{linkNotice}</Text> : null}
                </>
              ) : (
                <Text style={styles.sectionText}>No linked The Show profile.</Text>
              )}
            </View>
          </>
        ) : null}
      </ScrollView>

      <Modal transparent visible={settingsOpen} animationType="fade">
        <KeyboardAvoidingView 
          behavior={Platform.OS === "ios" ? "padding" : "height"}
          style={styles.modalOverlay}
        >
          {/* Dismiss keyboard AND close modal if they click the dark background */}
          <Pressable 
            style={styles.modalBackdrop} 
            onPress={() => { Keyboard.dismiss(); setSettingsOpen(false); }} 
          />
          
          {/* Dismiss keyboard if they click empty space inside the card */}
          <TouchableWithoutFeedback onPress={Keyboard.dismiss}>
            <View style={styles.modalCard}>
              <View style={styles.modalHeader}>
                <Text style={styles.modalTitle}>Edit Account</Text>
                <TouchableOpacity onPress={() => { Keyboard.dismiss(); setSettingsOpen(false); }}>
                  <Ionicons name="close" size={22} color={theme.colors.text} />
                </TouchableOpacity>
              </View>

              <TouchableOpacity style={styles.photoButton} onPress={pickProfilePhoto}>
                <Ionicons name="image-outline" size={18} color={theme.colors.text} />
                <Text style={styles.photoButtonText}>
                  {newPhotoUri ? "Change selected photo" : "Change profile photo"}
                </Text>
              </TouchableOpacity>

              {newPhotoUri ? (
                <Image source={{ uri: newPhotoUri }} style={styles.photoPreview} />
              ) : null}

              <View style={styles.fieldGroup}>
              <View style={{ flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-end' }}>
                <Text style={styles.label}>Display name</Text>
                <Text style={{ color: theme.colors.muted, fontSize: 11, fontWeight: '600' }}>
                  {editName.length} / 20
                </Text>
              </View>
              <TextInput
                value={editName}
                onChangeText={setEditName}
                placeholder="Display name"
                placeholderTextColor={theme.colors.muted}
                maxLength={20} 
                style={styles.input}
              />
            </View>

              <View style={styles.fieldGroup}>
                <Text style={styles.label}>Email</Text>
                <TextInput
                  value={editEmail}
                  onChangeText={setEditEmail}
                  placeholder="Email address"
                  placeholderTextColor={theme.colors.muted}
                  autoCapitalize="none"
                  keyboardType="email-address"
                  style={styles.input}
                />
              </View>

              <View style={styles.fieldGroup}>
                <View style={{ flexDirection: 'row', justifyContent: 'space-between', alignItems: 'flex-end' }}>
                  <Text style={styles.label}>Bio</Text>
                  <Text style={{ color: theme.colors.muted, fontSize: 11, fontWeight: '600' }}>
                    {editDescription.length} / 70
                  </Text>
                </View>
                <TextInput
                  value={editDescription}
                  onChangeText={setEditDescription}
                  placeholder="Tell us about yourself..."
                  placeholderTextColor={theme.colors.muted}
                  multiline
                  maxLength={70}
                  style={[styles.input, { minHeight: 80, textAlignVertical: "top" }]}
                />
              </View>

            {notice ? <Text style={styles.noticeText}>{notice}</Text> : null}
            {modalError ? <Text style={styles.errorText}>{modalError}</Text> : null}

            <View style={styles.legalSection}>
              <Text style={styles.legalTitle}>Legal</Text>
              <TouchableOpacity
                style={styles.legalButton}
                onPress={() => {
                  void openLegalDocument(PRIVACY_POLICY_URL);
                }}
              >
                <Text style={styles.legalButtonText}>Privacy Policy</Text>
                <Ionicons name="open-outline" size={16} color={theme.colors.text} />
              </TouchableOpacity>
              <TouchableOpacity
                style={styles.legalButton}
                onPress={() => {
                  void openLegalDocument(TERMS_AND_CONDITIONS_URL);
                }}
              >
                <Text style={styles.legalButtonText}>Terms & Conditions</Text>
                <Ionicons name="open-outline" size={16} color={theme.colors.text} />
              </TouchableOpacity>
            </View>

              <View style={styles.modalActions}>
                <TouchableOpacity style={styles.resetButton} onPress={resetPassword} disabled={saving || deletingAccount}>
                  <Text style={styles.resetText}>Reset password</Text>
                </TouchableOpacity>
                <TouchableOpacity style={styles.saveButton} onPress={saveProfile} disabled={saving || deletingAccount}>
                  <Text style={styles.saveText}>{saving ? "Saving..." : "Save changes"}</Text>
                </TouchableOpacity>
              </View>

              <TouchableOpacity
                style={[styles.logoutButton, signingOut && styles.buttonDisabled]}
                onPress={confirmLogOut}
                disabled={saving || deletingAccount || signingOut}
              >
                <Text style={styles.logoutText}>
                  {signingOut ? "Logging out..." : "Log out"}
                </Text>
              </TouchableOpacity>

              <TouchableOpacity
                style={[styles.deleteAccountButton, deletingAccount && styles.deleteAccountButtonDisabled]}
                onPress={confirmDeleteAccount}
                disabled={saving || deletingAccount}
              >
                <Text style={styles.deleteAccountText}>
                  {deletingAccount ? "Deleting account..." : "Delete account"}
                </Text>
              </TouchableOpacity>
            </View>
          </TouchableWithoutFeedback>
        </KeyboardAvoidingView>
      </Modal>

      <Modal transparent visible={linkOpen} animationType="fade">
        <KeyboardAvoidingView 
          behavior={Platform.OS === "ios" ? "padding" : "height"}
          style={styles.modalOverlay}
        >
          <Pressable 
            style={styles.modalBackdrop} 
            onPress={() => { Keyboard.dismiss(); setLinkOpen(false); }} 
          />
          <TouchableWithoutFeedback onPress={Keyboard.dismiss}>
            <View style={styles.modalCard}>
              <View style={styles.modalHeader}>
                <Text style={styles.modalTitle}>Link MLB The Show Account</Text>
                <TouchableOpacity onPress={() => { Keyboard.dismiss(); setLinkOpen(false); }}>
                  <Ionicons name="close" size={22} color={theme.colors.text} />
                </TouchableOpacity>
              </View>

              <View style={styles.fieldGroup}>
                <Text style={styles.label}>Username</Text>
                <TextInput
                  value={linkUsername}
                  onChangeText={setLinkUsername}
                  placeholder="MLB The Show username"
                  placeholderTextColor={theme.colors.muted}
                  autoCapitalize="none"
                  style={styles.input}
                />
              </View>

              {linkError ? <Text style={styles.errorText}>{linkError}</Text> : null}

              <View style={styles.modalActions}>
                <TouchableOpacity style={styles.resetButton} onPress={() => { Keyboard.dismiss(); setLinkOpen(false); }}>
                  <Text style={styles.resetText}>Cancel</Text>
                </TouchableOpacity>
                <TouchableOpacity style={styles.saveButton} onPress={linkShowProfile} disabled={linking}>
                  <Text style={styles.saveText}>{linking ? "Linking..." : "Link account"}</Text>
                </TouchableOpacity>
              </View>
            </View>
          </TouchableWithoutFeedback>
        </KeyboardAvoidingView>
      </Modal>
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  safeArea: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  container: {
    paddingHorizontal: theme.spacing.l,
    paddingTop: theme.spacing.l,
    paddingBottom: theme.spacing.l,
    gap: theme.spacing.l,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  title: {
    color: theme.colors.text,
    fontSize: 28,
    fontWeight: "700",
  },
  settingsButton: {
    width: 36,
    height: 36,
    borderRadius: 18,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  loadingState: {
    minHeight: 200,
    alignItems: "center",
    justifyContent: "center",
  },
  profileHeader: {
    flexDirection: "row",
    gap: theme.spacing.m,
    padding: theme.spacing.m,
    borderRadius: 20,
    backgroundColor: "rgba(15, 23, 42, 0.6)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.08)",
    alignItems: "stretch",
  },
  avatarFrame: {
    width: 120,
    height: 120,
    borderRadius: 60,
    overflow: "hidden",
  },
  avatarLarge: {
    width: "100%",
    height: "100%",
    resizeMode: "cover",
  },
  profileInfo: {
    flex: 1,
    justifyContent: "space-between",
  },
  nameLarge: {
    color: theme.colors.text,
    fontSize: 26,
    fontWeight: "800",
  },
  summaryRow: {
    flexDirection: "row",
    gap: theme.spacing.m,
  },
  summaryItem: {
    flex: 1,
    alignItems: "flex-start",
  },
  summaryLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.9,
    marginTop: 4,
  },
  summaryValue: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  summaryValueAccent: {
    color: "#fbbf24",
  },
  proCtaCard: {
    padding: theme.spacing.m,
    borderRadius: 16,
    backgroundColor: "rgba(251, 191, 36, 0.08)",
    borderWidth: 1,
    borderColor: "rgba(251, 191, 36, 0.35)",
    gap: 10,
  },
  proCtaTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  proCtaDescription: {
    color: theme.colors.muted,
    fontSize: 13,
    lineHeight: 18,
  },
  proCtaButton: {
    marginTop: 2,
    borderRadius: 12,
    paddingVertical: 11,
    backgroundColor: "#fbbf24",
    alignItems: "center",
  },
  proCtaButtonText: {
    color: "#111827",
    fontSize: 14,
    fontWeight: "800",
  },
  proManageCard: {
    padding: theme.spacing.m,
    borderRadius: 16,
    backgroundColor: "rgba(59, 130, 246, 0.08)",
    borderWidth: 1,
    borderColor: "rgba(59, 130, 246, 0.28)",
    gap: 8,
    minHeight: 56,
    justifyContent: "center",
  },
  proManageRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  proManageTitle: {
    color: theme.colors.text,
    fontSize: 15,
    fontWeight: "700",
    flexShrink: 1,
  },
  proManageButton: {
    borderRadius: 999,
    paddingHorizontal: 12,
    paddingVertical: 8,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.22)",
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  proManageButtonText: {
    color: theme.colors.text,
    fontSize: 12,
    fontWeight: "700",
  },
  buttonDisabled: {
    opacity: 0.65,
  },
  tabRow: {
    flexDirection: "row",
    gap: 12,
  },
  tabButton: {
    flex: 1,
    paddingVertical: 10,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    backgroundColor: "rgba(15, 23, 42, 0.35)",
    alignItems: "center",
  },
  tabButtonActive: {
    backgroundColor: "rgba(59, 130, 246, 0.2)",
    borderColor: "rgba(59, 130, 246, 0.6)",
  },
  tabText: {
    color: theme.colors.muted,
    fontSize: 14,
    fontWeight: "600",
  },
  tabTextActive: {
    color: theme.colors.text,
    fontWeight: "700",
  },
  tabCard: {
    padding: theme.spacing.l,
    borderRadius: 16,
    backgroundColor: "rgba(15, 23, 42, 0.4)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.08)",
    gap: theme.spacing.m,
  },
  sectionTitle: {
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "700",
  },
  sectionHeaderRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    marginBottom: theme.spacing.s,
  },
  sectionMeta: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  sectionText: {
    marginTop: theme.spacing.s,
    color: theme.colors.muted,
    fontSize: 14,
    lineHeight: 20,
  },
  onlineRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 10,
  },
  onlineLine: {
    flex: 1,
    height: 1,
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  onlineText: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "600",
    letterSpacing: 1.2,
    textTransform: "uppercase",
  },
  loadingInline: {
    alignItems: "center",
    justifyContent: "center",
    paddingVertical: theme.spacing.s,
  },
  detailGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    justifyContent: "space-between",
    rowGap: theme.spacing.m,
  },
  detailItem: {
    width: "48%",
    paddingVertical: theme.spacing.s,
    paddingHorizontal: theme.spacing.m,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.5)",
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.06)",
  },
  detailLabel: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    textTransform: "uppercase",
    letterSpacing: 0.8,
  },
  detailValue: {
    marginTop: 6,
    color: theme.colors.text,
    fontSize: 16,
    fontWeight: "700",
  },
  linkButton: {
    marginTop: theme.spacing.m,
    paddingVertical: 12,
    borderRadius: 12,
    backgroundColor: theme.colors.primary,
    alignItems: "center",
  },
  linkButtonText: {
    color: "white",
    fontSize: 14,
    fontWeight: "700",
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
  modalOverlay: {
    flex: 1,
    justifyContent: "center",
    alignItems: "center",
    backgroundColor: "#020617",
  },
  modalBackdrop: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "#020617",
  },
  modalCard: {
    width: "88%",
    borderRadius: 20,
    backgroundColor: "#0f172a",
    padding: theme.spacing.l,
    gap: theme.spacing.m,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.08)",
  },
  modalHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
  },
  modalTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "700",
  },
  photoButton: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    backgroundColor: "#111827",
  },
  photoButtonText: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "600",
  },
  photoPreview: {
    width: 96,
    height: 96,
    borderRadius: 48,
    alignSelf: "center",
  },
  fieldGroup: {
    gap: 6,
  },
  label: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  input: {
    borderWidth: 1,
    borderColor: "rgba(255, 255, 255, 0.12)",
    backgroundColor: "#111827",
    color: theme.colors.text,
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 12,
    fontSize: 15,
  },
  legalSection: {
    gap: 10,
  },
  legalTitle: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.8,
    textTransform: "uppercase",
  },
  legalButton: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
    paddingVertical: 12,
    paddingHorizontal: 14,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.12)",
    backgroundColor: "#111827",
  },
  legalButtonText: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "600",
  },
  modalActions: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 12,
  },
  resetButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.16)",
    alignItems: "center",
  },
  resetText: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "600",
  },
  saveButton: {
    flex: 1,
    paddingVertical: 12,
    borderRadius: 12,
    backgroundColor: theme.colors.primary,
    alignItems: "center",
  },
  saveText: {
    color: "white",
    fontSize: 13,
    fontWeight: "700",
  },
  logoutButton: {
    borderRadius: 12,
    paddingVertical: 11,
    borderWidth: 1,
    borderColor: "rgba(255,255,255,0.16)",
    backgroundColor: "rgba(255,255,255,0.05)",
    alignItems: "center",
    marginTop: 12,
    marginBottom: -4, 
  },
  logoutText: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "700",
  },
  deleteAccountButton: {
    borderRadius: 12,
    paddingVertical: 11,
    borderWidth: 1,
    borderColor: "rgba(239,68,68,0.5)",
    backgroundColor: "rgba(239,68,68,0.12)",
    alignItems: "center",
  },
  deleteAccountButtonDisabled: {
    opacity: 0.65,
  },
  deleteAccountText: {
    color: "#f87171",
    fontSize: 13,
    fontWeight: "700",
  },
  portfolioPrivateContainer: {
    alignItems: "center",
    gap: 8,
    paddingVertical: 8,
  },
  portfolioOverview: {
    marginTop: 4,
  },
  portfolioOverviewRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 16,
  },
  portfolioOverviewStat: {
    flex: 1,
    alignItems: "center",
  },
  portfolioOverviewLabel: {
    color: theme.colors.muted,
    fontSize: 10,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.5,
    marginBottom: 4,
  },
  portfolioOverviewValueRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
  },
  portfolioOverviewValue: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "800",
  },
  portfolioOverviewDivider: {
    width: 1,
    height: 32,
    backgroundColor: "rgba(255,255,255,0.08)",
  },
  stubIconSmall: {
    width: 14,
    height: 14,
    resizeMode: "contain",
  },
});
