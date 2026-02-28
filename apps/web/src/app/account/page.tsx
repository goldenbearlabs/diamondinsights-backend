"use client";

import Image from "next/image";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { ChangeEvent, useEffect, useMemo, useState } from "react";
import { onAuthStateChanged, sendPasswordResetEmail, signOut, type User } from "firebase/auth";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import {
  ApiError,
  apiDeleteAuth,
  apiGetAuth,
  apiPostAuth,
  apiPutAuth,
  getMyEntitlements,
  type EntitlementsMeResponse,
} from "@/lib/api";
import { toReadableAuthError } from "@/lib/auth-errors";
import { getFirebaseAuth } from "@/lib/firebase";
import {
  emitProfileImageUpdated,
  invalidateAvatarCache,
  resolveAvatarUrl,
} from "@/lib/profile-image";
import { uploadProfileImage } from "@/lib/storage";

import styles from "./page.module.css";

type UserProfile = {
  id: number;
  firebase_id?: string | null;
  email?: string | null;
  display_name: string;
  profile_img_path: string;
  latest_points_total?: number | null;
  is_me: boolean;
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

function formatPortfolioStubs(value: number): string {
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

export default function AccountPage() {
  const router = useRouter();

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);

  const [profile, setProfile] = useState<UserProfile | null>(null);
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);

  const [activeTab, setActiveTab] = useState<"Investing" | "Gameplay">("Gameplay");
  const [showProfile, setShowProfile] = useState<ShowProfile | null>(null);
  const [showLoading, setShowLoading] = useState(false);
  const [showError, setShowError] = useState<string | null>(null);
  const [linkUsername, setLinkUsername] = useState("");
  const [linking, setLinking] = useState(false);
  const [linkError, setLinkError] = useState<string | null>(null);
  const [linkNotice, setLinkNotice] = useState<string | null>(null);

  const [portfolioData, setPortfolioData] = useState<UserPortfolio | null>(null);
  const [portfolioLoading, setPortfolioLoading] = useState(false);
  const [portfolioError, setPortfolioError] = useState<string | null>(null);

  const [entitlements, setEntitlements] = useState<EntitlementsMeResponse | null>(null);
  const [entitlementsLoading, setEntitlementsLoading] = useState(false);
  const [entitlementsError, setEntitlementsError] = useState<string | null>(null);

  const [settingsOpen, setSettingsOpen] = useState(false);
  const [editName, setEditName] = useState("");
  const [editEmail, setEditEmail] = useState("");
  const [newPhotoFile, setNewPhotoFile] = useState<File | null>(null);
  const [newPhotoPreview, setNewPhotoPreview] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [deletingAccount, setDeletingAccount] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [settingsError, setSettingsError] = useState<string | null>(null);

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
    } catch (error: unknown) {
      setPageError(toReadableAuthError(error, "Firebase is not configured for web auth."));
      setAuthReady(true);
      setFirebaseUser(null);
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!authReady) {
      return;
    }
    if (!firebaseUser) {
      setLoading(false);
      return;
    }

    let active = true;
    setLoading(true);
    setPageError(null);

    void (async () => {
      try {
        const data = await apiGetAuth<UserProfile>("/users/me");
        if (!active) {
          return;
        }
        setProfile(data);
        setEditName(data.display_name);
        setEditEmail(data.email ?? "");
      } catch (error: unknown) {
        if (active) {
          setPageError(toReadableAuthError(error, "Failed to load account profile"));
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [authReady, firebaseUser]);

  useEffect(() => {
    let active = true;
    if (!profile?.profile_img_path) {
      setAvatarUrl(null);
      return;
    }

    void resolveAvatarUrl(profile.profile_img_path).then((url) => {
      if (active) {
        setAvatarUrl(url);
      }
    });

    return () => {
      active = false;
    };
  }, [profile?.profile_img_path]);

  useEffect(() => {
    if (!profile) {
      setShowProfile(null);
      setShowError(null);
      setShowLoading(false);
      return;
    }

    let active = true;
    setShowLoading(true);
    setShowError(null);
    setShowProfile(null);

    void (async () => {
      try {
        const data = await apiGetAuth<ShowProfile>("/users/me/show");
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
        setShowError(toReadableAuthError(error, "Failed to load The Show profile"));
      } finally {
        if (active) {
          setShowLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [profile?.id]);

  useEffect(() => {
    if (!profile || activeTab !== "Investing") {
      return;
    }

    let active = true;
    setPortfolioLoading(true);
    setPortfolioError(null);
    setPortfolioData(null);

    void (async () => {
      try {
        const data = await apiGetAuth<UserPortfolio>("/portfolios/me");
        if (active) {
          setPortfolioData(data);
        }
      } catch (error: unknown) {
        if (!active) {
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
        setPortfolioError(toReadableAuthError(error, "Failed to load portfolio"));
      } finally {
        if (active) {
          setPortfolioLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [activeTab, profile?.id]);

  useEffect(() => {
    if (!profile?.is_me) {
      setEntitlements(null);
      setEntitlementsError(null);
      setEntitlementsLoading(false);
      return;
    }

    let active = true;
    setEntitlementsLoading(true);
    setEntitlementsError(null);

    void (async () => {
      try {
        const data = await getMyEntitlements();
        if (active) {
          setEntitlements(data);
        }
      } catch (error: unknown) {
        if (active) {
          setEntitlementsError(toReadableAuthError(error, "Failed to load subscription status"));
        }
      } finally {
        if (active) {
          setEntitlementsLoading(false);
        }
      }
    })();

    return () => {
      active = false;
    };
  }, [profile?.is_me]);

  useEffect(() => {
    if (!newPhotoFile) {
      setNewPhotoPreview(null);
      return;
    }
    const nextUrl = URL.createObjectURL(newPhotoFile);
    setNewPhotoPreview(nextUrl);
    return () => {
      URL.revokeObjectURL(nextUrl);
    };
  }, [newPhotoFile]);

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

  const totalInvested = portfolioData?.holdings.reduce((sum, holding) => {
    return sum + holding.quantity * (holding.avg_price ?? 0);
  }, 0);

  const openSettings = () => {
    if (!profile) {
      return;
    }
    setEditName(profile.display_name);
    setEditEmail(profile.email ?? "");
    setNewPhotoFile(null);
    setNotice(null);
    setSettingsError(null);
    setSettingsOpen(true);
  };

  const onPhotoFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0] ?? null;
    setNewPhotoFile(file);
  };

  const linkShowProfile = async () => {
    const username = linkUsername.trim();
    if (!username) {
      setLinkError("Username is required.");
      return;
    }

    setLinking(true);
    setLinkError(null);
    setLinkNotice(null);
    try {
      const data = await apiPostAuth<ShowProfile>("/users/me/show/link", { username });
      setShowProfile(data);
      setLinkUsername("");
      setLinkNotice("Account linked.");
    } catch (error: unknown) {
      if (error instanceof ApiError && error.status === 404) {
        setLinkError("No MLB The Show account found for that username.");
      } else if (error instanceof ApiError && error.status === 409) {
        setLinkError("That MLB The Show username is already linked.");
      } else {
        setLinkError(toReadableAuthError(error, "Failed to link account"));
      }
    } finally {
      setLinking(false);
    }
  };

  const saveProfile = async () => {
    if (!profile) {
      return;
    }
    if (!firebaseUser) {
      setSettingsError("Not authenticated");
      return;
    }

    setSaving(true);
    setNotice(null);
    setSettingsError(null);
    try {
      const updates: Record<string, string> = {};
      const nextName = editName.trim();
      const nextEmail = editEmail.trim();

      if (nextName && nextName !== profile.display_name) {
        updates.display_name = nextName;
      }
      if (nextEmail && nextEmail !== (profile.email ?? "")) {
        updates.email = nextEmail;
      }

      if (newPhotoFile) {
        const profileImgPath = await uploadProfileImage(newPhotoFile, firebaseUser.uid);
        updates.profile_img_path = profileImgPath;
      }

      if (Object.keys(updates).length === 0) {
        setNotice("No changes to save.");
        return;
      }

      const updated = await apiPutAuth<UserProfile>("/users/me", updates);
      setProfile(updated);

      if (updates.profile_img_path) {
        invalidateAvatarCache(updates.profile_img_path);
        const refreshedAvatar = await resolveAvatarUrl(updates.profile_img_path, { bustCache: true });
        setAvatarUrl(refreshedAvatar);
        emitProfileImageUpdated(updates.profile_img_path);
      }

      setNewPhotoFile(null);
      setNotice("Profile updated.");
    } catch (error: unknown) {
      setSettingsError(toReadableAuthError(error, "Failed to update profile"));
    } finally {
      setSaving(false);
    }
  };

  const resetPassword = async () => {
    const email = profile?.email ?? editEmail.trim();
    if (!email) {
      setSettingsError("Email is required to reset password.");
      return;
    }

    setSaving(true);
    setNotice(null);
    setSettingsError(null);
    try {
      const auth = getFirebaseAuth();
      await sendPasswordResetEmail(auth, email);
      setNotice("Password reset email sent.");
    } catch (error: unknown) {
      setSettingsError(toReadableAuthError(error, "Failed to send reset email"));
    } finally {
      setSaving(false);
    }
  };

  const deleteAccount = async () => {
    const confirmed = window.confirm("Delete account permanently? This cannot be undone.");
    if (!confirmed) {
      return;
    }

    setDeletingAccount(true);
    setNotice(null);
    setSettingsError(null);
    try {
      await apiDeleteAuth<void>("/users/me");
      const auth = getFirebaseAuth();
      await signOut(auth).catch(() => undefined);
      setSettingsOpen(false);
      router.replace("/signin");
    } catch (error: unknown) {
      setSettingsError(toReadableAuthError(error, "Failed to delete account"));
    } finally {
      setDeletingAccount(false);
    }
  };

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <header className={styles.header}>
          <div className={styles.headerTop}>
            <div>
              <h1>Account</h1>
              <p>Manage profile, gameplay stats, and your linked data.</p>
            </div>
            {profile ? (
              <button
                type="button"
                className={styles.settingsButton}
                onClick={openSettings}
                aria-label="Open account settings"
              >
                <svg viewBox="0 0 24 24" aria-hidden className={styles.settingsIcon}>
                  <path d="M19.43 12.98a7.7 7.7 0 0 0 .06-.98 7.7 7.7 0 0 0-.06-.98l2.11-1.65a.5.5 0 0 0 .12-.64l-2-3.46a.5.5 0 0 0-.6-.22l-2.49 1a7.2 7.2 0 0 0-1.7-.98l-.38-2.65a.5.5 0 0 0-.49-.42h-4a.5.5 0 0 0-.49.42L9.12 5.07a7.2 7.2 0 0 0-1.7.98l-2.49-1a.5.5 0 0 0-.6.22l-2 3.46a.5.5 0 0 0 .12.64l2.11 1.65a7.7 7.7 0 0 0-.06.98c0 .33.02.66.06.98l-2.11 1.65a.5.5 0 0 0-.12.64l2 3.46a.5.5 0 0 0 .6.22l2.49-1c.52.4 1.09.73 1.7.98l.38 2.65a.5.5 0 0 0 .49.42h4a.5.5 0 0 0 .49-.42l.38-2.65c.61-.25 1.18-.58 1.7-.98l2.49 1a.5.5 0 0 0 .6-.22l2-3.46a.5.5 0 0 0-.12-.64l-2.11-1.65ZM12 15.5A3.5 3.5 0 1 1 12 8.5a3.5 3.5 0 0 1 0 7Z" />
                </svg>
              </button>
            ) : null}
          </div>
        </header>

        {!authReady || loading ? (
          <section className={styles.card}>
            <p className={styles.muted}>Loading account...</p>
          </section>
        ) : null}

        {pageError ? (
          <section className={styles.card}>
            <p className={styles.error}>{pageError}</p>
            <Link href="/signin" className={styles.inlineLink}>
              Back to sign in
            </Link>
          </section>
        ) : null}

        {!loading && !pageError && profile ? (
          <>
            <section className={styles.card}>
              <div className={styles.profileHeader}>
                <Image
                  src={avatarUrl || "/images/default_profile.png"}
                  alt="Profile"
                  className={styles.avatar}
                  width={92}
                  height={92}
                  unoptimized={Boolean(avatarUrl)}
                />
                <div>
                  <h2>{profile.display_name}</h2>
                  <div className={styles.summaryRow}>
                    <div className={styles.summaryItem}>
                      <strong>{formatScore(profile.latest_points_total)}</strong>
                      <span>Score</span>
                    </div>
                    <div className={styles.summaryItem}>
                      <strong>{recordText}</strong>
                      <span>Record</span>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            <section className={`${styles.card} ${styles.proCard}`}>
              <div className={styles.proHeader}>
                <h3>Diamond Pro</h3>
                <span className={entitlements?.has_pro ? styles.proActive : styles.proFree}>
                  {entitlements?.has_pro ? "Active" : "Free"}
                </span>
              </div>
              <p className={styles.muted}>
                {entitlements?.has_pro
                  ? "Your Pro entitlement is active."
                  : "Upgrade in mobile app to unlock Pro features."}
              </p>
              {entitlements?.pro_expires_at ? (
                <p className={styles.meta}>
                  Renews through {new Date(entitlements.pro_expires_at).toLocaleDateString()}
                </p>
              ) : null}
              {entitlementsLoading ? <p className={styles.muted}>Loading subscription status...</p> : null}
              {entitlementsError ? <p className={styles.error}>{entitlementsError}</p> : null}
            </section>

            <section className={styles.tabRow}>
              <button
                type="button"
                className={activeTab === "Investing" ? styles.tabButtonActive : styles.tabButton}
                onClick={() => setActiveTab("Investing")}
              >
                Investing
              </button>
              <button
                type="button"
                className={activeTab === "Gameplay" ? styles.tabButtonActive : styles.tabButton}
                onClick={() => setActiveTab("Gameplay")}
              >
                Gameplay
              </button>
            </section>

            <section className={styles.card}>
              {activeTab === "Investing" ? (
                <>
                  {portfolioLoading ? <p className={styles.muted}>Loading portfolio...</p> : null}
                  {!portfolioLoading && portfolioError === "private" ? (
                    <p className={styles.muted}>Your portfolio is private.</p>
                  ) : null}
                  {!portfolioLoading && portfolioError === "none" ? (
                    <p className={styles.muted}>No portfolio found.</p>
                  ) : null}
                  {!portfolioLoading && portfolioError && portfolioError !== "private" && portfolioError !== "none" ? (
                    <p className={styles.error}>{portfolioError}</p>
                  ) : null}
                  {!portfolioLoading && !portfolioError && portfolioData ? (
                    <div className={styles.portfolioSummary}>
                      <div>
                        <span>Total Invested</span>
                        <strong>{formatPortfolioStubs(totalInvested ?? 0)}</strong>
                      </div>
                      <div>
                        <span>Total Cards</span>
                        <strong>{portfolioData.holdings.length}</strong>
                      </div>
                    </div>
                  ) : null}
                </>
              ) : (
                <>
                  {showLoading ? <p className={styles.muted}>Loading gameplay profile...</p> : null}
                  {showError ? <p className={styles.error}>{showError}</p> : null}
                  {!showLoading && !showError && showProfile ? (
                    <div className={styles.statsGrid}>
                      <div className={styles.statItem}>
                        <span>Username</span>
                        <strong>{showProfile.username}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>Level</span>
                        <strong>{formatStat(showProfile.display_level)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>Games Played</span>
                        <strong>{formatStat(showProfile.games_played)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>Wins</span>
                        <strong>{formatStat(summaryStats?.wins)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>Losses</span>
                        <strong>{formatStat(summaryStats?.losses)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>ERA</span>
                        <strong>{formatFloat(summaryStats?.era, 2)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>AVG</span>
                        <strong>{formatFloat(summaryStats?.batting_average, 3)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>K/9</span>
                        <strong>{formatFloat(summaryStats?.k_per_9, 2)}</strong>
                      </div>
                      <div className={styles.statItem}>
                        <span>WHIP</span>
                        <strong>{formatFloat(summaryStats?.whip, 2)}</strong>
                      </div>
                    </div>
                  ) : null}
                  {!showLoading && !showError && !showProfile ? (
                    <div className={styles.linkBox}>
                      <p className={styles.muted}>Link your MLB The Show account to show gameplay stats.</p>
                      <div className={styles.linkRow}>
                        <input
                          type="text"
                          value={linkUsername}
                          onChange={(event) => setLinkUsername(event.target.value)}
                          placeholder="MLB The Show username"
                          className={styles.input}
                        />
                        <button type="button" className={styles.primaryButton} onClick={linkShowProfile} disabled={linking}>
                          {linking ? "Linking..." : "Link"}
                        </button>
                      </div>
                      {linkNotice ? <p className={styles.notice}>{linkNotice}</p> : null}
                      {linkError ? <p className={styles.error}>{linkError}</p> : null}
                    </div>
                  ) : null}
                </>
              )}
            </section>
          </>
        ) : null}
      </div>

      {settingsOpen && profile ? (
        <div className={styles.modalOverlay} role="dialog" aria-modal="true" aria-label="Edit account">
          <button
            type="button"
            className={styles.modalBackdrop}
            onClick={() => setSettingsOpen(false)}
            aria-label="Close account settings"
          />
          <section className={styles.modalCard}>
            <div className={styles.modalHeader}>
              <h3>Edit Account</h3>
              <button
                type="button"
                className={styles.modalClose}
                onClick={() => setSettingsOpen(false)}
                aria-label="Close"
              >
                x
              </button>
            </div>

            <div className={styles.settingsGrid}>
              <label className={styles.field}>
                <span>Display Name</span>
                <input
                  type="text"
                  className={styles.input}
                  value={editName}
                  onChange={(event) => setEditName(event.target.value)}
                  placeholder="Display name"
                />
              </label>
              <label className={styles.field}>
                <span>Email</span>
                <input
                  type="email"
                  className={styles.input}
                  value={editEmail}
                  onChange={(event) => setEditEmail(event.target.value)}
                  placeholder="Email"
                />
              </label>
            </div>

            <label className={styles.secondaryButton}>
              {newPhotoFile ? "Change selected photo" : "Change profile photo"}
              <input type="file" accept="image/*" onChange={onPhotoFileChange} hidden />
            </label>

            {newPhotoPreview ? (
              <Image
                src={newPhotoPreview}
                alt="Profile preview"
                className={styles.avatarPreview}
                width={88}
                height={88}
                unoptimized
              />
            ) : null}

            {notice ? <p className={styles.notice}>{notice}</p> : null}
            {settingsError ? <p className={styles.error}>{settingsError}</p> : null}

            <div className={styles.actionsRow}>
              <button
                type="button"
                className={styles.secondaryButton}
                onClick={resetPassword}
                disabled={saving || deletingAccount}
              >
                Reset password
              </button>
              <button
                type="button"
                className={styles.primaryButton}
                onClick={saveProfile}
                disabled={saving || deletingAccount}
              >
                {saving ? "Saving..." : "Save changes"}
              </button>
            </div>

            <button
              type="button"
              className={styles.dangerButton}
              onClick={deleteAccount}
              disabled={saving || deletingAccount}
            >
              {deletingAccount ? "Deleting account..." : "Delete account"}
            </button>
          </section>
        </div>
      ) : null}
    </main>
  );
}
