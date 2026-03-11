"use client";

import type { CSSProperties } from "react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { Check, ChevronDown, ChevronUp, Info, RefreshCcw, Trophy } from "lucide-react";
import { useRouter } from "next/navigation";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { ApiError, getPredictionLeaderboard, type LeaderboardEntry } from "@/lib/api";
import { getFirebaseAuth } from "@/lib/firebase";
import { resolveAvatarUrl } from "@/lib/profile-image";

import styles from "./page.module.css";

const ROSTER_UPDATES = ["Roster Update 1"] as const;

function formatOrdinal(value: number): string {
  const abs = Math.abs(value);
  const mod100 = abs % 100;
  if (mod100 >= 11 && mod100 <= 13) {
    return `${value}th`;
  }
  const mod10 = abs % 10;
  if (mod10 === 1) {
    return `${value}st`;
  }
  if (mod10 === 2) {
    return `${value}nd`;
  }
  if (mod10 === 3) {
    return `${value}rd`;
  }
  return `${value}th`;
}

function rankColor(rank: number): string {
  if (rank === 1) {
    return "#fbbf24";
  }
  if (rank === 2) {
    return "#cbd5e1";
  }
  if (rank === 3) {
    return "#d97706";
  }
  return "#94a3b8";
}

function formatScore(score: number | null): string {
  if (score == null || Number.isNaN(score)) {
    return "—";
  }
  return score.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export default function LeaderboardPage() {
  const router = useRouter();

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);

  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [items, setItems] = useState<LeaderboardEntry[]>([]);
  const [totalParticipants, setTotalParticipants] = useState(0);
  const [myRank, setMyRank] = useState<number | null>(null);
  const [myPredictionCount, setMyPredictionCount] = useState<number | null>(null);

  const [selectedUpdate, setSelectedUpdate] = useState<(typeof ROSTER_UPDATES)[number]>(ROSTER_UPDATES[0]);
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const dropdownRef = useRef<HTMLDivElement | null>(null);

  const [avatarUrls, setAvatarUrls] = useState<Record<number, string | null>>({});

  const fetchLeaderboard = useCallback(
    async (options?: { background?: boolean }) => {
      const background = options?.background ?? false;
      if (!background) {
        setLoading(true);
      }

      try {
        setError(null);
        const data = await getPredictionLeaderboard();
        setItems(data.items);
        setTotalParticipants(data.total_participants);
        setMyRank(data.my_rank);
        setMyPredictionCount(data.my_prediction_count);
        if (data.items.length === 0) {
          setAvatarUrls({});
        }
      } catch (err: unknown) {
        if (err instanceof ApiError) {
          if (err.status === 401 || err.status === 403) {
            router.replace("/signin");
            return;
          }
          setError(err.body || `Error ${err.status}`);
        } else {
          setError("Failed to load leaderboard");
        }
      } finally {
        if (!background) {
          setLoading(false);
        }
      }
    },
    [router],
  );

  useEffect(() => {
    let unsubscribe: (() => void) | null = null;

    try {
      const auth = getFirebaseAuth();
      unsubscribe = onAuthStateChanged(auth, (user) => {
        setFirebaseUser(user);
        setAuthReady(true);
        if (!user) {
          router.replace("/signin");
        }
      });
    } catch {
      queueMicrotask(() => {
        setAuthReady(true);
        setError("Firebase auth is not configured.");
      });
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!authReady || !firebaseUser) {
      return;
    }
    void fetchLeaderboard();
  }, [authReady, firebaseUser, fetchLeaderboard]);

  useEffect(() => {
    if (items.length === 0) {
      return;
    }

    let active = true;

    void Promise.all(
      items.map(async (entry) => {
        const resolved = await resolveAvatarUrl(entry.profile_img_path);
        return [entry.user_id, resolved] as const;
      }),
    ).then((pairs) => {
      if (!active) {
        return;
      }
      const next: Record<number, string | null> = {};
      for (const [userId, resolvedUrl] of pairs) {
        next[userId] = resolvedUrl;
      }
      setAvatarUrls(next);
    });

    return () => {
      active = false;
    };
  }, [items]);

  useEffect(() => {
    const onMouseDown = (event: MouseEvent) => {
      if (!dropdownRef.current) {
        return;
      }
      if (!dropdownRef.current.contains(event.target as Node)) {
        setDropdownOpen(false);
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
    };
  }, []);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    await fetchLeaderboard({ background: true });
    setRefreshing(false);
  }, [fetchLeaderboard]);

  const renderedRows = useMemo(() => {
    return items.map((entry) => {
      const isTop3 = entry.rank <= 3;
      const accent = rankColor(entry.rank);
      const style = { "--rank-accent": accent } as CSSProperties;
      const avatarSrc = avatarUrls[entry.user_id] || "/images/default_profile.png";

      return (
        <div key={entry.user_id} className={`${styles.row} ${isTop3 ? styles.rowTop3 : ""}`}>
          <div className={styles.rankBadge} style={style}>
            <span className={styles.rankText}>{entry.rank}</span>
          </div>

          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img src={avatarSrc} alt={`${entry.display_name} avatar`} className={styles.avatar} />

          <div className={styles.infoCol}>
            <span className={styles.displayName}>{entry.display_name}</span>
            <span className={styles.predCountText}>
              {entry.prediction_count} prediction{entry.prediction_count !== 1 ? "s" : ""}
            </span>
          </div>

          <div className={styles.scoreCol}>
            <span className={styles.scoreLabel}>Score</span>
            <span className={styles.scoreValue}>{formatScore(entry.score)}</span>
          </div>
        </div>
      );
    });
  }, [avatarUrls, items]);

  if (!authReady) {
    return (
      <div className={styles.page}>
        <FloatingShieldsBackground />
        <Navbar />
        <main className={styles.content}>
          <p className={styles.loadingText}>Loading leaderboard...</p>
        </main>
      </div>
    );
  }

  if (!firebaseUser) {
    return null;
  }

  return (
    <div className={styles.page}>
      <FloatingShieldsBackground />
      <div className={styles.texture} />
      <Navbar />

      <main className={styles.content}>
        <header className={styles.header}>
          <h1>Prediction Leaderboard</h1>
          <p>Top 50 scores from the previous roster update</p>
        </header>

        <div className={styles.noticeCard}>
          <Info size={14} />
          <span>Scores will be finalized after the next roster update</span>
        </div>

        <div className={styles.controlsRow}>
          <div className={styles.dropdownContainer} ref={dropdownRef}>
            <button
              type="button"
              className={styles.dropdownButton}
              onClick={() => setDropdownOpen((prev) => !prev)}
              aria-expanded={dropdownOpen}
            >
              <span>{selectedUpdate}</span>
              {dropdownOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
            </button>

            {dropdownOpen ? (
              <div className={styles.dropdownMenu}>
                {ROSTER_UPDATES.map((update, index) => {
                  const isActive = update === selectedUpdate;
                  return (
                    <button
                      key={update}
                      type="button"
                      className={`${styles.dropdownOption} ${isActive ? styles.dropdownOptionActive : ""} ${
                        index === ROSTER_UPDATES.length - 1 ? styles.dropdownOptionLast : ""
                      }`}
                      onClick={() => {
                        setSelectedUpdate(update);
                        setDropdownOpen(false);
                      }}
                    >
                      <span>{update}</span>
                      {isActive ? <Check size={14} /> : null}
                    </button>
                  );
                })}
              </div>
            ) : null}
          </div>

          <button type="button" className={styles.refreshButton} onClick={onRefresh} disabled={refreshing || loading}>
            <RefreshCcw size={14} />
            <span>{refreshing ? "Refreshing" : "Refresh"}</span>
          </button>
        </div>

        {myRank != null && myPredictionCount != null ? (
          <div className={styles.myRankCard}>
            <Trophy size={16} />
            <p>
              Your rank is <strong>{formatOrdinal(myRank)}</strong> with <strong>{myPredictionCount}</strong> prediction
              {myPredictionCount !== 1 ? "s" : ""} and <strong>—</strong> score
            </p>
          </div>
        ) : null}

        {!loading && !error ? (
          <p className={styles.participantsText}>
            {totalParticipants.toLocaleString()} participant{totalParticipants !== 1 ? "s" : ""}
          </p>
        ) : null}

        {loading ? (
          <div className={styles.loadingWrap}>
            <div className={styles.spinner} />
            <p className={styles.loadingText}>Loading leaderboard...</p>
          </div>
        ) : null}

        {!loading && error ? <div className={styles.errorCard}>{error}</div> : null}

        {!loading && !error && items.length === 0 ? (
          <div className={styles.emptyCard}>No predictions yet. Be the first!</div>
        ) : null}

        {!loading && !error && items.length > 0 ? (
          <section className={styles.listWrap}>
            <div className={styles.headerRow}>
              <span className={styles.colRank}>#</span>
              <span className={styles.colUser}>User</span>
              <span className={styles.colScore}>Score</span>
            </div>
            <div className={styles.rows}>{renderedRows}</div>
          </section>
        ) : null}
      </main>
    </div>
  );
}
