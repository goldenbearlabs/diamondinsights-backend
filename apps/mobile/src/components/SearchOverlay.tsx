import { useEffect, useMemo, useRef, useState } from "react";
import {
  ActivityIndicator,
  Image,
  Modal,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TouchableOpacity,
  View,
  useWindowDimensions,
} from "react-native";
import { FontAwesome5, Ionicons } from "@expo/vector-icons";
import { useRouter } from "expo-router";

import { apiGet } from "../lib/api";
import { resolveAvatarUrl } from "../lib/profileImage";
import { Avatar } from "./Avatar";
import { theme } from "../theme/colors";

const ACCENT = "#fbbf24";

export type SearchMode = "all" | "users" | "cards";

type UserResult = {
  id: number;
  display_name: string;
  profile_img_url: string;
};

type CardResult = {
  id: string;
  name: string;
  year: number;
  ovr: number;
  meta_overall_rounded?: number | null;
  img: string;
  baked_img?: string; // Added baked_img
  series_name: string; // Added series_name
  is_live_set: boolean;
};

type SearchResponse = {
  users: UserResult[];
  cards: CardResult[];
};

type Props = {
  visible: boolean;
  query: string;
  mode: SearchMode;
  onModeChange: (mode: SearchMode) => void;
  onClose: () => void;
  topOffset: number;
  panelWidth?: number;
  panelRightInset?: number;
};

export const SearchResultsPanel = ({
  visible,
  query,
  mode,
  onModeChange,
  onClose,
  topOffset,
  panelWidth,
  panelRightInset,
}: Props) => {
  const router = useRouter();
  const { height } = useWindowDimensions();
  const [results, setResults] = useState<SearchResponse>({ users: [], cards: [] });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userImages, setUserImages] = useState<Record<number, string>>({});
  const requestRef = useRef(0);

  useEffect(() => {
    if (!visible) {
      setResults({ users: [], cards: [] });
      setLoading(false);
      setError(null);
    }
  }, [visible]);

  useEffect(() => {
    if (!visible) return;
    const trimmed = query.trim();
    if (!trimmed) {
      setResults({ users: [], cards: [] });
      setLoading(false);
      setError(null);
      return;
    }

    const handle = setTimeout(async () => {
      const requestId = requestRef.current + 1;
      requestRef.current = requestId;
      setLoading(true);
      setError(null);

      try {
        // Enforce the Year 25 search here
        const params = new URLSearchParams({ q: trimmed, limit: "12", year: "25" });
        if (mode === "users") params.set("users_only", "true");
        if (mode === "cards") params.set("cards_only", "true");
        const data = await apiGet<SearchResponse>(`/search?${params.toString()}`);
        if (requestRef.current !== requestId) return;
        setResults(data);
        await resolveUserImages(data.users);
      } catch (err: any) {
        if (requestRef.current !== requestId) return;
        setError(err?.message ?? "Search failed.");
      } finally {
        if (requestRef.current === requestId) setLoading(false);
      }
    }, 250);

    return () => clearTimeout(handle);
  }, [query, mode, visible]);

  const resolveUserImages = async (users: UserResult[]) => {
    const pending = users.filter((user) => user.profile_img_url && !userImages[user.id]);
    if (!pending.length) return;

    const entries = await Promise.all(
      pending.map(async (user) => {
        try {
          const url = await resolveAvatarUrl(user.profile_img_url);
          return url ? ([user.id, url] as const) : null;
        } catch {
          return null;
        }
      })
    );

    if (!entries.length) return;
    setUserImages((prev) => {
      const next = { ...prev };
      entries.forEach((entry) => {
        if (!entry) return;
        next[entry[0]] = entry[1];
      });
      return next;
    });
  };

  const showUsers = mode !== "cards";
  const showCards = mode !== "users";
  const hasUsers = results.users.length > 0;
  const hasCards = results.cards.length > 0;
  const empty = !loading && !error && !hasUsers && !hasCards && query.trim();
  const idle = !loading && !error && !hasUsers && !hasCards && !query.trim();

  const modeOptions = useMemo(
    () => [
      { key: "all" as const, label: "All" },
      { key: "users" as const, label: "Users" },
      { key: "cards" as const, label: "Cards" },
    ],
    []
  );

  const handleUserPress = (userId: number) => {
    onClose();
    router.push({ pathname: "/(app)/account", params: { userId: String(userId) } });
  };

  const handleCardPress = async (card: CardResult) => {
    try {
      const fullCardData = await apiGet(`/cards/${card.id}`);
      
      onClose();

      const targetPath = card.is_live_set ? `/predictions/${card.id}` : "/(app)/card";

      router.push({
        pathname: targetPath as any, 
        params: {
          cardData: JSON.stringify(fullCardData),
        },
      });
    } catch (err) {
      console.error("Failed to fetch full card details", err);
      // Fallback in case of error
      onClose();
      const fallbackPath = card.is_live_set ? `/predictions/${card.id}` : "/(app)/card";
      router.push({ 
        pathname: fallbackPath as any, 
        params: { cardData: JSON.stringify(card) } 
      });
    }
  };

  if (!visible) return null;

  const maxPanelHeight = Math.max(260, height - topOffset - 24);
  const shouldExpand = Boolean(query.trim()) || loading || error;
  const panelHeight = shouldExpand ? maxPanelHeight : 84;
  const panelWrapStyle = panelWidth
    ? { right: panelRightInset ?? 16, width: panelWidth, paddingHorizontal: 0 }
    : { left: 0, right: 0 };

  return (
    <Modal transparent visible={visible} animationType="fade" onRequestClose={onClose}>
      <View style={styles.overlay}>
        <Pressable style={StyleSheet.absoluteFill} onPress={onClose} />
        <View style={[styles.panelWrap, panelWrapStyle, { top: topOffset }]}>
          <View style={[styles.panel, { height: panelHeight }]}>
            <View style={styles.modeRow}>
              {modeOptions.map((option) => {
                const active = option.key === mode;
                return (
                  <TouchableOpacity
                    key={option.key}
                    style={[styles.modeButton, active && styles.modeButtonActive]}
                    onPress={() => onModeChange(option.key)}
                  >
                    <Text style={[styles.modeText, active && styles.modeTextActive]}>
                      {option.label}
                    </Text>
                  </TouchableOpacity>
                );
              })}
            </View>

            <ScrollView
              style={styles.results}
              contentContainerStyle={styles.resultsContent}
              showsVerticalScrollIndicator={false}
            >
              {loading ? (
                <View style={styles.centered}>
                  <ActivityIndicator color={ACCENT} />
                </View>
              ) : error ? (
                <Text style={styles.errorText}>{error}</Text>
              ) : idle ? (
                <Text style={styles.emptyText}>Start typing to search.</Text>
              ) : empty ? (
                <Text style={styles.emptyText}>No results yet.</Text>
              ) : (
                <>
                  {showUsers && hasUsers ? (
                    <>
                      <Text style={styles.sectionTitle}>Users</Text>
                      {results.users.map((user) => (
                        <TouchableOpacity
                          key={`user-${user.id}`}
                          style={styles.resultRow}
                          onPress={() => handleUserPress(user.id)}
                        >
                          <View style={styles.avatarWrap}>
                            <Avatar
                              firebasePath={user.profile_img_url}
                              size={32}
                              borderColor="rgba(255, 255, 255, 0.12)"
                              borderWidth={1}
                            />
                          </View>
                          <View style={styles.resultText}>
                            <Text style={styles.resultTitle}>{user.display_name}</Text>
                            <Text style={styles.resultMeta}>User profile</Text>
                          </View>
                          <Ionicons name="chevron-forward" size={16} color={theme.colors.muted} />
                        </TouchableOpacity>
                      ))}
                    </>
                  ) : null}

                  {showCards && hasCards ? (
                    <>
                      <Text style={styles.sectionTitle}>Cards</Text>
                      {results.cards.map((card) => (
                        <TouchableOpacity
                          key={`card-${card.id}`}
                          style={styles.resultRow}
                          onPress={() => handleCardPress(card)}
                        >
                          <Image 
                            source={{ uri: card.baked_img || card.img }} 
                            style={styles.cardImage} 
                            resizeMode="contain"
                          />
                          <View style={styles.resultText}>
                            <Text style={styles.resultTitle}>{card.name}</Text>
                            <Text style={styles.resultMeta}>
                              {card.series_name} · OVR {card.ovr}
                            </Text>
                          </View>
                          <FontAwesome5 name="chevron-right" size={12} color={theme.colors.muted} />
                        </TouchableOpacity>
                      ))}
                    </>
                  ) : null}
                </>
              )}
            </ScrollView>
          </View>
        </View>
      </View>
    </Modal>
  );
};

const styles = StyleSheet.create({
  overlay: {
    flex: 1,
    backgroundColor: "rgba(2, 6, 23, 0.25)",
  },
  panelWrap: {
    position: "absolute",
    paddingHorizontal: 16,
  },
  panel: {
    width: "100%",
    paddingHorizontal: 16,
    paddingTop: 12,
    paddingBottom: 16,
    borderRadius: 20,
    backgroundColor: "rgba(2, 6, 23, 0.96)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
    shadowColor: "#000",
    shadowOpacity: 0.4,
    shadowRadius: 16,
    shadowOffset: { width: 0, height: 8 },
    elevation: 10,
  },
  modeRow: {
    flexDirection: "row",
    gap: 8,
    marginBottom: 12,
  },
  modeButton: {
    paddingVertical: 6,
    paddingHorizontal: 12,
    borderRadius: 12,
    backgroundColor: "rgba(15, 23, 42, 0.8)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.2)",
  },
  modeButtonActive: {
    borderColor: "rgba(251, 191, 36, 0.6)",
    backgroundColor: "rgba(251, 191, 36, 0.14)",
  },
  modeText: {
    fontSize: 12,
    fontWeight: "700",
    color: theme.colors.muted,
    letterSpacing: 0.2,
  },
  modeTextActive: {
    color: ACCENT,
  },
  results: {
    flex: 1,
  },
  resultsContent: {
    paddingBottom: 20,
    gap: 10,
  },
  sectionTitle: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.4,
    textTransform: "uppercase",
    marginTop: 6,
  },
  resultRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingVertical: 8,
    paddingHorizontal: 12,
    borderRadius: 14,
    backgroundColor: "rgba(15, 23, 42, 0.85)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.14)",
  },
  avatarWrap: {
    marginRight: 12,
  },
  cardImage: {
    width: 50,    // Increased width
    height: 70,   // Increased height drastically
    borderRadius: 4, 
    marginRight: 14,
  },
  resultText: {
    flex: 1,
  },
  resultTitle: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  resultMeta: {
    color: theme.colors.muted,
    fontSize: 11,
    marginTop: 2,
  },
  centered: {
    paddingVertical: 40,
    alignItems: "center",
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
    paddingVertical: 20,
    textAlign: "center",
  },
  emptyText: {
    color: theme.colors.muted,
    fontSize: 13,
    paddingVertical: 20,
    textAlign: "center",
  },
});
