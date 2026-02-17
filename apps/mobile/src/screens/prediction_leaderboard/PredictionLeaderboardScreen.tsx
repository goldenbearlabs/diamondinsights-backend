import { Ionicons } from "@expo/vector-icons";
import { useCallback, useEffect, useState } from "react";
import {
  ActivityIndicator,
  Image,
  RefreshControl,
  ScrollView,
  StyleSheet,
  Text,
  View,
} from "react-native";

import { Avatar } from "../../components/Avatar";
import {
  ApiError,
  getPredictionLeaderboard,
  LeaderboardEntry,
} from "../../lib/api";
import { theme } from "../../theme/colors";

function formatOrdinal(value: number): string {
  const abs = Math.abs(value);
  const mod100 = abs % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${value}th`;
  const mod10 = abs % 10;
  if (mod10 === 1) return `${value}st`;
  if (mod10 === 2) return `${value}nd`;
  if (mod10 === 3) return `${value}rd`;
  return `${value}th`;
}

export default function PredictionLeaderboardScreen() {
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [items, setItems] = useState<LeaderboardEntry[]>([]);
  const [totalParticipants, setTotalParticipants] = useState(0);
  const [myRank, setMyRank] = useState<number | null>(null);
  const [myPredictionCount, setMyPredictionCount] = useState<number | null>(null);

  const fetchLeaderboard = useCallback(async () => {
    try {
      setError(null);
      const data = await getPredictionLeaderboard();
      setItems(data.items);
      setTotalParticipants(data.total_participants);
      setMyRank(data.my_rank);
      setMyPredictionCount(data.my_prediction_count);
    } catch (err) {
      if (err instanceof ApiError) {
        setError(err.body || `Error ${err.status}`);
      } else {
        setError("Failed to load leaderboard");
      }
    }
  }, []);

  useEffect(() => {
    setLoading(true);
    fetchLeaderboard().finally(() => setLoading(false));
  }, [fetchLeaderboard]);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    await fetchLeaderboard();
    setRefreshing(false);
  }, [fetchLeaderboard]);

  const rankColor = (rank: number) => {
    if (rank === 1) return "#fbbf24"; // gold
    if (rank === 2) return "#cbd5e1"; // silver
    if (rank === 3) return "#d97706"; // bronze
    return theme.colors.muted;
  };

  const renderRow = (entry: LeaderboardEntry) => {
    const isTop3 = entry.rank <= 3;
    return (
      <View
        key={entry.user_id}
        style={[styles.row, isTop3 && styles.rowTop3]}
      >
        {/* Rank */}
        <View style={[styles.rankBadge, isTop3 && { borderColor: rankColor(entry.rank) }]}>
          <Text style={[styles.rankText, { color: rankColor(entry.rank) }]}>
            {entry.rank}
          </Text>
        </View>

        {/* Avatar */}
        <Avatar
          firebasePath={entry.profile_img_path}
          size={36}
        />

        {/* Name + predictions count */}
        <View style={styles.infoCol}>
          <Text style={styles.displayName} numberOfLines={1}>
            {entry.display_name}
          </Text>
          <Text style={styles.predCountText}>
            {entry.prediction_count} prediction{entry.prediction_count !== 1 ? "s" : ""}
          </Text>
        </View>

        {/* Score */}
        <View style={styles.scoreCol}>
          <Text style={styles.scoreLabel}>Score</Text>
          <Text style={styles.scoreValue}>—</Text>
        </View>
      </View>
    );
  };

  return (
    <View style={styles.container}>
      <ScrollView
        contentContainerStyle={styles.content}
        keyboardShouldPersistTaps="handled"
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={onRefresh}
            tintColor={theme.colors.muted}
          />
        }
      >
        <Text style={styles.title}>Prediction Leaderboard</Text>
        <Text style={styles.subtitle}>Top 50 Users</Text>

        {/* Roster update notice */}
        <View style={styles.noticeCard}>
          <Ionicons name="information-circle-outline" size={14} color={theme.colors.muted} />
          <Text style={styles.noticeText}>
            Scores will be finalized after the next roster update
          </Text>
        </View>

        {/* Your rank indicator */}
        {myRank != null && myPredictionCount != null ? (
          <View style={styles.myRankCard}>
            <Ionicons name="trophy-outline" size={16} color="#fbbf24" />
            <Text style={styles.myRankText}>
              Your rank is{" "}
              <Text style={styles.myRankHighlight}>{formatOrdinal(myRank)}</Text>
              {" "}with{" "}
              <Text style={styles.myRankHighlight}>{myPredictionCount}</Text>
              {" "}prediction{myPredictionCount !== 1 ? "s" : ""} and{" "}
              <Text style={styles.myRankHighlight}>—</Text>
              {" "}score
            </Text>
          </View>
        ) : null}

        {/* Participants count */}
        {!loading && !error ? (
          <Text style={styles.participantsText}>
            {totalParticipants} participant{totalParticipants !== 1 ? "s" : ""}
          </Text>
        ) : null}

        {/* Loading */}
        {loading ? (
          <View style={styles.loadingWrap}>
            <ActivityIndicator color={theme.colors.primary} size="large" />
          </View>
        ) : error ? (
          <Text style={styles.errorText}>{error}</Text>
        ) : items.length === 0 ? (
          <Text style={styles.emptyText}>No predictions yet. Be the first!</Text>
        ) : (
          <View style={styles.listWrap}>
            {/* Column header */}
            <View style={styles.headerRow}>
              <Text style={[styles.headerText, { width: 44 }]}>#</Text>
              <Text style={[styles.headerText, { flex: 1, marginLeft: 44 }]}>User</Text>
              <Text style={[styles.headerText, { width: 56, textAlign: "center" }]}>Score</Text>
            </View>
            {items.map(renderRow)}
          </View>
        )}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  content: {
    padding: 16,
    paddingBottom: 110,
    gap: 12,
  },
  title: {
    color: theme.colors.text,
    fontSize: 26,
    fontWeight: "700",
    letterSpacing: 0.2,
  },
  subtitle: {
    color: theme.colors.muted,
    fontSize: 13,
    marginTop: -4,
  },
  myRankCard: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    borderRadius: 10,
    borderWidth: 1,
    borderColor: "rgba(251, 191, 36, 0.35)",
    backgroundColor: "rgba(64, 40, 8, 0.45)",
    paddingVertical: 10,
    paddingHorizontal: 14,
  },
  myRankText: {
    color: theme.colors.text,
    fontSize: 13,
    fontWeight: "600",
  },
  myRankHighlight: {
    color: "#fbbf24",
    fontWeight: "800",
  },
  participantsText: {
    color: theme.colors.muted,
    fontSize: 12,
    fontWeight: "600",
  },
  loadingWrap: {
    paddingTop: 40,
    alignItems: "center",
  },
  errorText: {
    color: theme.colors.error,
    fontSize: 13,
    paddingTop: 6,
  },
  emptyText: {
    color: theme.colors.muted,
    fontSize: 13,
    paddingTop: 6,
  },
  listWrap: {
    gap: 6,
  },
  headerRow: {
    flexDirection: "row",
    alignItems: "center",
    paddingHorizontal: 10,
    paddingVertical: 6,
  },
  headerText: {
    color: theme.colors.muted,
    fontSize: 11,
    fontWeight: "700",
    letterSpacing: 0.5,
    textTransform: "uppercase",
  },
  row: {
    flexDirection: "row",
    alignItems: "center",
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.24)",
    backgroundColor: "rgba(15, 23, 42, 0.82)",
    paddingHorizontal: 10,
    paddingVertical: 10,
    gap: 10,
  },
  rowTop3: {
    borderColor: "rgba(251, 191, 36, 0.3)",
    backgroundColor: "rgba(64, 40, 8, 0.25)",
  },
  rankBadge: {
    width: 34,
    height: 34,
    borderRadius: 8,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(251, 191, 36, 0.12)",
    borderWidth: 1,
    borderColor: "rgba(148, 163, 184, 0.3)",
  },
  rankText: {
    fontSize: 14,
    fontWeight: "800",
  },
  avatar: {
    width: 36,
    height: 36,
    borderRadius: 18,
  },
  infoCol: {
    flex: 1,
    minWidth: 0,
  },
  displayName: {
    color: theme.colors.text,
    fontSize: 14,
    fontWeight: "700",
  },
  predCountText: {
    color: theme.colors.muted,
    fontSize: 11,
    marginTop: 1,
  },
  scoreCol: {
    alignItems: "center",
    width: 56,
  },
  scoreLabel: {
    color: theme.colors.muted,
    fontSize: 9,
    fontWeight: "700",
    letterSpacing: 0.3,
    textTransform: "uppercase",
  },
  scoreValue: {
    color: theme.colors.muted,
    fontSize: 15,
    fontWeight: "800",
  },
  noticeCard: {
    flexDirection: "row",
    alignItems: "center",
    gap: 6,
    paddingVertical: 8,
    paddingHorizontal: 12,
    borderRadius: 8,
    backgroundColor: "rgba(59, 130, 246, 0.08)",
    borderWidth: 1,
    borderColor: "rgba(59, 130, 246, 0.2)",
  },
  noticeText: {
    color: theme.colors.muted,
    fontSize: 11,
    fontStyle: "italic",
    flex: 1,
  },
});
