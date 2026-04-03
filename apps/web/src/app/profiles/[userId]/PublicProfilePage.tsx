"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { BarChart3, BriefcaseBusiness, Lock, Trophy } from "lucide-react";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { ApiError, apiGet, apiGetAuth } from "@/lib/api";
import { resolveAvatarUrl } from "@/lib/profile-image";

import styles from "./page.module.css";

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

type HoldingCard = {
  id: string;
  name: string;
  team_short_name: string;
  ovr: number;
  baked_img: string;
  display_position: string;
  rarity: string;
  predicted_ovr: number | null;
};

type Holding = {
  card_id: string;
  quantity: number;
  avg_price: number | null;
  user_predicted_ovr: number | null;
  card: HoldingCard;
};

type PortfolioData = {
  id: number;
  name: string;
  is_public: boolean;
  holdings: Holding[];
};

type ActiveTab = "Gameplay" | "Investing";

const QUICKSELL_TABLE: Record<number, number> = {
  91: 9000,
  90: 8000,
  89: 7000,
  88: 5500,
  87: 4500,
  86: 3750,
  85: 3000,
  84: 1500,
  83: 1200,
  82: 900,
  81: 600,
  80: 400,
  79: 150,
  78: 125,
  77: 100,
  76: 75,
  75: 50,
};

function getQuicksellValue(ovr: number): number {
  if (ovr >= 92) {
    return 10000;
  }
  if (QUICKSELL_TABLE[ovr] !== undefined) {
    return QUICKSELL_TABLE[ovr];
  }
  if (ovr >= 65) {
    return 25;
  }
  return 5;
}

function formatStubs(value: number): string {
  if (Math.abs(value) >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(2)}M`;
  }
  if (Math.abs(value) >= 1_000) {
    return `${(value / 1_000).toFixed(1)}K`;
  }
  return value.toLocaleString();
}

function formatScore(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function formatStat(value: number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return String(value);
}

function formatFloat(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined) {
    return "-";
  }
  return value.toFixed(digits);
}

function isUnauthedError(error: unknown): boolean {
  return error instanceof Error && error.message === "Not authenticated";
}

async function apiGetMaybeAuthed<T>(path: string): Promise<T> {
  try {
    return await apiGetAuth<T>(path);
  } catch (error: unknown) {
    if (isUnauthedError(error)) {
      return apiGet<T>(path);
    }
    throw error;
  }
}

export default function PublicProfilePage({ userId }: { userId: string }) {
  const router = useRouter();

  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState<ActiveTab>("Gameplay");

  const [showProfile, setShowProfile] = useState<ShowProfile | null>(null);
  const [showLoading, setShowLoading] = useState(false);
  const [showError, setShowError] = useState<string | null>(null);

  const [portfolio, setPortfolio] = useState<PortfolioData | null>(null);
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioError, setPortfolioError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setLoading(true);
    setPageError(null);

    void (async () => {
      try {
        const data = await apiGetMaybeAuthed<UserProfile>(`/users/${userId}`);
        if (!active) {
          return;
        }
        if (data.is_me) {
          router.replace("/account");
          return;
        }
        setProfile(data);
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        setPageError(error instanceof Error ? error.message : "Failed to load profile.");
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [router, userId]);

  useEffect(() => {
    let active = true;

    if (!profile?.profile_img_path) {
      setAvatarUrl(null);
      return;
    }

    void resolveAvatarUrl(profile.profile_img_path).then((resolved) => {
      if (active) {
        setAvatarUrl(resolved);
      }
    });

    return () => {
      active = false;
    };
  }, [profile?.profile_img_path]);

  useEffect(() => {
    if (!profile) {
      return;
    }

    let active = true;
    setShowLoading(true);
    setShowError(null);
    setShowProfile(null);

    void (async () => {
      try {
        const data = await apiGet<ShowProfile>(`/users/${profile.id}/show`);
        if (active) {
          setShowProfile(data);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setShowProfile(null);
          setShowError(null);
          return;
        }
        setShowError(error instanceof Error ? error.message : "Failed to load gameplay profile.");
      } finally {
        if (active) {
          setShowLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [profile]);

  useEffect(() => {
    if (!profile || activeTab !== "Investing") {
      return;
    }

    let active = true;
    setPortfolioLoading(true);
    setPortfolioError(null);
    setPortfolio(null);

    void (async () => {
      try {
        const data = await apiGetAuth<PortfolioData>(`/portfolios/users/${profile.id}/portfolio`);
        if (active) {
          setPortfolio(data);
        }
      } catch (error: unknown) {
        if (!active) {
          return;
        }
        if (isUnauthedError(error)) {
          setPortfolioError("signin");
          return;
        }
        if (error instanceof ApiError && error.status === 403) {
          setPortfolioError("private");
          return;
        }
        if (error instanceof ApiError && error.status === 404) {
          setPortfolioError("none");
          return;
        }
        setPortfolioError(error instanceof Error ? error.message : "Failed to load portfolio.");
      } finally {
        if (active) {
          setPortfolioLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [activeTab, profile]);

  const sortedOnlineStats = useMemo(() => {
    return [...(showProfile?.online_stats ?? [])].sort((a, b) => a.year - b.year);
  }, [showProfile?.online_stats]);

  const summaryStats = sortedOnlineStats.length > 0 ? sortedOnlineStats[sortedOnlineStats.length - 1] : null;

  const recordText = useMemo(() => {
    const aggregate = sortedOnlineStats.reduce(
      (acc, row) => {
        acc.wins += row.wins ?? 0;
        acc.losses += row.losses ?? 0;
        if (row.wins !== null && row.wins !== undefined) {
          acc.hasWins = true;
        }
        if (row.losses !== null && row.losses !== undefined) {
          acc.hasLosses = true;
        }
        return acc;
      },
      { wins: 0, losses: 0, hasWins: false, hasLosses: false },
    );

    if (!aggregate.hasWins && !aggregate.hasLosses) {
      return "-";
    }

    return `${aggregate.wins}-${aggregate.losses}`;
  }, [sortedOnlineStats]);

  const holdings = useMemo(() => portfolio?.holdings ?? [], [portfolio?.holdings]);

  const portfolioTotals = useMemo(() => {
    return holdings.reduce(
      (acc, holding) => {
        const avgPrice = holding.avg_price ?? 0;
        const invested = holding.quantity * avgPrice;
        const ovr = holding.user_predicted_ovr ?? holding.card.predicted_ovr ?? holding.card.ovr;
        const value = holding.quantity * getQuicksellValue(ovr);

        return {
          invested: acc.invested + invested,
          value: acc.value + value,
          pl: acc.pl + (value - invested),
        };
      },
      { invested: 0, value: 0, pl: 0 },
    );
  }, [holdings]);

  if (loading) {
    return (
      <main className={styles.page}>
        <Navbar />
        <FloatingShieldsBackground />
        <div className={styles.texture} />
        <div className={styles.content}>
          <section className={styles.panel}>
            <p className={styles.muted}>Loading profile...</p>
          </section>
        </div>
      </main>
    );
  }

  if (pageError || !profile) {
    return (
      <main className={styles.page}>
        <Navbar />
        <FloatingShieldsBackground />
        <div className={styles.texture} />
        <div className={styles.content}>
          <section className={styles.errorCard}>
            <p>{pageError || "User not found."}</p>
          </section>
        </div>
      </main>
    );
  }

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <section className={styles.hero}>
          <div className={styles.heroHeader}>
            <div className={styles.identity}>
              <Image
                src={avatarUrl || "/images/default_profile.png"}
                alt={profile.display_name}
                width={96}
                height={96}
                className={styles.avatar}
                unoptimized={Boolean(avatarUrl)}
              />

              <div className={styles.identityCopy}>
                <p className={styles.eyebrow}>Player Page</p>
                <h1>{profile.display_name}</h1>
                {profile.description ? <p className={styles.description}>{profile.description}</p> : null}

                <div className={styles.badges}>
                  <span className={styles.badge}>
                    <Trophy size={14} aria-hidden />
                    Score {formatScore(profile.latest_points_total)}
                  </span>
                  <span className={styles.badge}>
                    <BarChart3 size={14} aria-hidden />
                    Record {recordText}
                  </span>
                  {showProfile ? (
                    <span className={styles.badge}>
                      <BriefcaseBusiness size={14} aria-hidden />
                      Show {showProfile.username}
                    </span>
                  ) : null}
                </div>
              </div>
            </div>

            {showProfile ? (
              <Link href={`/gameplay-stats?user=${encodeURIComponent(showProfile.username)}`} className={styles.primaryLink}>
                Open Full Gameplay Stats
              </Link>
            ) : null}
          </div>

          <div className={styles.tabRow}>
            <button
              type="button"
              className={activeTab === "Gameplay" ? styles.tabButtonActive : styles.tabButton}
              onClick={() => setActiveTab("Gameplay")}
            >
              Gameplay
            </button>
            <button
              type="button"
              className={activeTab === "Investing" ? styles.tabButtonActive : styles.tabButton}
              onClick={() => setActiveTab("Investing")}
            >
              Investing
            </button>
          </div>
        </section>

        {activeTab === "Gameplay" ? (
          <section className={styles.panel}>
            {showLoading ? <p className={styles.muted}>Loading gameplay profile...</p> : null}
            {showError ? <p className={styles.errorText}>{showError}</p> : null}

            {!showLoading && !showError && !showProfile ? (
              <p className={styles.muted}>No linked MLB The Show profile.</p>
            ) : null}

            {!showLoading && !showError && showProfile ? (
              <>
                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Gameplay Summary</h2>
                    <p>Read-only snapshot from the linked MLB The Show account.</p>
                  </div>
                  {summaryStats ? <span className={styles.yearPill}>Season {summaryStats.year}</span> : null}
                </div>

                <div className={styles.statGrid}>
                  <div className={styles.statCard}>
                    <span>Username</span>
                    <strong>{showProfile.username}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>Level</span>
                    <strong>{formatStat(showProfile.display_level)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>Games Played</span>
                    <strong>{formatStat(showProfile.games_played)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>Wins</span>
                    <strong>{formatStat(summaryStats?.wins)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>Losses</span>
                    <strong>{formatStat(summaryStats?.losses)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>AVG</span>
                    <strong>{formatFloat(summaryStats?.batting_average, 3)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>ERA</span>
                    <strong>{formatFloat(summaryStats?.era, 2)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>K/9</span>
                    <strong>{formatFloat(summaryStats?.k_per_9, 2)}</strong>
                  </div>
                  <div className={styles.statCard}>
                    <span>WHIP</span>
                    <strong>{formatFloat(summaryStats?.whip, 2)}</strong>
                  </div>
                </div>
              </>
            ) : null}
          </section>
        ) : (
          <section className={styles.panel}>
            {portfolioLoading ? <p className={styles.muted}>Loading portfolio...</p> : null}

            {!portfolioLoading && portfolioError === "signin" ? (
              <p className={styles.muted}>Sign in to view portfolios on the web app.</p>
            ) : null}

            {!portfolioLoading && portfolioError === "private" ? (
              <div className={styles.lockedState}>
                <Lock size={18} aria-hidden />
                <span>{profile.display_name}&apos;s portfolio is private.</span>
              </div>
            ) : null}

            {!portfolioLoading && portfolioError === "none" ? (
              <p className={styles.muted}>No portfolio found.</p>
            ) : null}

            {!portfolioLoading &&
            portfolioError &&
            portfolioError !== "signin" &&
            portfolioError !== "private" &&
            portfolioError !== "none" ? (
              <p className={styles.errorText}>{portfolioError}</p>
            ) : null}

            {!portfolioLoading && !portfolioError && portfolio ? (
              <>
                <div className={styles.sectionHeader}>
                  <div>
                    <h2>Portfolio</h2>
                    <p>Read-only holdings snapshot from their public investment page.</p>
                  </div>
                  <span className={styles.yearPill}>{holdings.length} holdings</span>
                </div>

                <div className={styles.summaryGrid}>
                  <div className={styles.summaryCard}>
                    <span>Total Invested</span>
                    <strong>{formatStubs(portfolioTotals.invested)}</strong>
                  </div>
                  <div className={styles.summaryCard}>
                    <span>Projected Value</span>
                    <strong>{formatStubs(portfolioTotals.value)}</strong>
                  </div>
                  <div className={styles.summaryCard}>
                    <span>P/L</span>
                    <strong className={portfolioTotals.pl >= 0 ? styles.positiveValue : styles.negativeValue}>
                      {portfolioTotals.pl > 0 ? "+" : ""}
                      {formatStubs(portfolioTotals.pl)}
                    </strong>
                  </div>
                </div>

                {holdings.length === 0 ? (
                  <p className={styles.muted}>No investments yet.</p>
                ) : (
                  <div className={styles.tableWrap}>
                    <table className={styles.table}>
                      <thead>
                        <tr>
                          <th>Card</th>
                          <th>Qty</th>
                          <th>Avg Buy</th>
                          <th>Invested</th>
                          <th>Value</th>
                          <th>P/L</th>
                        </tr>
                      </thead>
                      <tbody>
                        {holdings.map((holding) => {
                          const avgPrice = holding.avg_price ?? 0;
                          const invested = holding.quantity * avgPrice;
                          const ovr = holding.user_predicted_ovr ?? holding.card.predicted_ovr ?? holding.card.ovr;
                          const value = holding.quantity * getQuicksellValue(ovr);
                          const pl = value - invested;

                          return (
                            <tr key={holding.card_id}>
                              <td>
                                <Link href={`/cards/${holding.card.id}`} className={styles.cardLink}>
                                  <Image
                                    src={holding.card.baked_img}
                                    alt={holding.card.name}
                                    width={42}
                                    height={58}
                                    className={styles.cardImage}
                                    unoptimized
                                  />
                                  <span>
                                    <strong>{holding.card.name}</strong>
                                    <small>
                                      {holding.card.team_short_name} · {holding.card.display_position} · OVR {holding.card.ovr}
                                    </small>
                                  </span>
                                </Link>
                              </td>
                              <td>{holding.quantity}</td>
                              <td>{formatStubs(avgPrice)}</td>
                              <td>{formatStubs(invested)}</td>
                              <td>{formatStubs(value)}</td>
                              <td className={pl >= 0 ? styles.positiveValue : styles.negativeValue}>
                                {pl > 0 ? "+" : ""}
                                {formatStubs(pl)}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}
              </>
            ) : null}
          </section>
        )}
      </div>
    </main>
  );
}
