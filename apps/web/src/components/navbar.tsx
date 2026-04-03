"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { onAuthStateChanged } from "firebase/auth";

import styles from "./navbar.module.css";
import { apiGet, apiGetAuth } from "@/lib/api";
import { CURRENT_CARD_YEAR } from "@/lib/config";
import { getFirebaseAuth } from "@/lib/firebase";
import { PROFILE_IMAGE_UPDATED_EVENT, resolveAvatarUrl } from "@/lib/profile-image";

type NavItem = {
  label: string;
  href: string;
};

type NavGroup = {
  label: string;
  items: NavItem[];
};

type UserOut = {
  profile_img_path: string;
};

type SearchMode = "showprofiles" | "users" | "cards";

type UserSearchResult = {
  id: number;
  display_name: string;
  profile_img_url: string;
};

type CardSearchResult = {
  id: string;
  name: string;
  year: number;
  ovr: number;
  series_name: string;
  img?: string | null;
  baked_img?: string | null;
  meta_overall_rounded?: number | null;
};

type SearchResponse = {
  users: UserSearchResult[];
  cards: CardSearchResult[];
};

type ShowProfileSearchResult = {
  user_id: number | null;
  username: string;
  display_name: string | null;
  profile_img_url: string | null;
};

const NAV_GROUPS: NavGroup[] = [
  {
    label: "Market",
    items: [
      { label: "Predictions", href: "/predictions" },
      { label: "Flipping", href: "/flipping" },
      { label: "Portfolio", href: "/portfolio" },
      { label: "Leaderboard", href: "/leaderboard" },
    ],
  },
  {
    label: "Gameplay",
    items: [
      { label: "Stats", href: "/gameplay-stats" },
      { label: "Records", href: "/records" },
      { label: "Rankings", href: "/rankings" },
      { label: "Card Comparison", href: "/card-comparison" },
      { label: "Team Builder", href: "/team-builder" },
      { label: "Cards", href: "/cards" },
    ],
  },
  {
    label: "Community",
    items: [
      { label: "Chat", href: "/chat" },
      { label: "Trending", href: "/trending" },
    ],
  },
];

function isItemActive(pathname: string, href: string): boolean {
  return pathname === href || pathname.startsWith(`${href}/`);
}

function isGroupActive(pathname: string, group: NavGroup): boolean {
  return group.items.some((item) => isItemActive(pathname, item.href));
}

function SearchResultAvatar({
  path,
  alt,
}: {
  path: string | null | undefined;
  alt: string;
}) {
  const [src, setSrc] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    void Promise.resolve(path ? resolveAvatarUrl(path) : null).then((resolved) => {
      if (active) {
        setSrc(resolved);
      }
    });

    return () => {
      active = false;
    };
  }, [path]);

  return (
    <Image
      src={src || "/images/default_profile.png"}
      alt={alt}
      width={34}
      height={34}
      className={styles.searchAvatar}
      unoptimized={Boolean(src)}
    />
  );
}

function buildSearchHref(query: string, mode: SearchMode): string {
  const params = new URLSearchParams({ mode });
  const trimmed = query.trim();
  if (trimmed) {
    params.set("q", trimmed);
  }
  return `/search?${params.toString()}`;
}

function SearchAutocomplete({
  query,
  mode,
  onQueryChange,
  onModeChange,
  onSubmit,
  onSelect,
  variant,
}: {
  query: string;
  mode: SearchMode;
  onQueryChange: (value: string) => void;
  onModeChange: (mode: SearchMode) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onSelect: (href: string) => void;
  variant: "desktop" | "mobile";
}) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const requestRef = useRef(0);
  const [open, setOpen] = useState(false);
  const [users, setUsers] = useState<UserSearchResult[]>([]);
  const [cards, setCards] = useState<CardSearchResult[]>([]);
  const [showProfiles, setShowProfiles] = useState<ShowProfileSearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const resetResults = () => {
    setUsers([]);
    setCards([]);
    setShowProfiles([]);
    setLoading(false);
    setError(null);
  };

  useEffect(() => {
    if (!open) {
      return;
    }

    const handlePointerDown = (event: MouseEvent | TouchEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    };

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("touchstart", handlePointerDown);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("touchstart", handlePointerDown);
    };
  }, [open]);

  useEffect(() => {
    if (!open) {
      return;
    }

    const trimmed = query.trim();
    if (!trimmed) {
      return;
    }

    const timeoutId = window.setTimeout(() => {
      const requestId = requestRef.current + 1;
      requestRef.current = requestId;
      setLoading(true);
      setError(null);

      void (async () => {
        try {
          if (mode === "showprofiles") {
            const params = new URLSearchParams({ q: trimmed, limit: "10" });
            const data = await apiGet<ShowProfileSearchResult[]>(`/users/show/search?${params.toString()}`);
            if (requestRef.current !== requestId) {
              return;
            }
            setShowProfiles(data);
            setUsers([]);
            setCards([]);
          } else {
            const params = new URLSearchParams({ q: trimmed, limit: "10" });
            if (mode === "users") {
              params.set("users_only", "true");
            } else {
              params.set("cards_only", "true");
              params.set("year", CURRENT_CARD_YEAR);
            }

            const data = await apiGet<SearchResponse>(`/search?${params.toString()}`);
            if (requestRef.current !== requestId) {
              return;
            }
            setUsers(data.users);
            setCards(data.cards);
            setShowProfiles([]);
          }
        } catch (nextError: unknown) {
          if (requestRef.current !== requestId) {
            return;
          }
          setError(nextError instanceof Error ? nextError.message : "Search failed.");
          setUsers([]);
          setCards([]);
          setShowProfiles([]);
        } finally {
          if (requestRef.current === requestId) {
            setLoading(false);
          }
        }
      })();
    }, 250);

    return () => {
      window.clearTimeout(timeoutId);
    };
  }, [mode, open, query]);

  const hasResults = users.length > 0 || cards.length > 0 || showProfiles.length > 0;
  const isIdle = !query.trim() && !loading && !error;
  const isEmpty = !isIdle && !loading && !error && !hasResults;

  return (
    <div ref={rootRef} className={`${styles.searchShell} ${variant === "mobile" ? styles.searchShellMobile : ""}`}>
      <form
        className={variant === "desktop" ? styles.desktopSearch : styles.mobileSearch}
        onSubmit={(event) => {
          setOpen(false);
          onSubmit(event);
        }}
      >
        <input
          type="search"
          value={query}
          onChange={(event) => {
            const nextValue = event.target.value;
            onQueryChange(nextValue);
            if (!nextValue.trim()) {
              resetResults();
            }
            setOpen(true);
          }}
          onFocus={() => setOpen(true)}
          placeholder="Search cards, users, or Show profiles"
          aria-label="Search"
        />
      </form>

      {open ? (
        <div className={`${styles.searchPanel} ${variant === "mobile" ? styles.searchPanelMobile : ""}`}>
          <div className={styles.searchModeRow}>
            <button
              type="button"
              className={mode === "showprofiles" ? styles.searchModeButtonActive : styles.searchModeButton}
              onClick={() => onModeChange("showprofiles")}
            >
              Show Profiles
            </button>
            <button
              type="button"
              className={mode === "users" ? styles.searchModeButtonActive : styles.searchModeButton}
              onClick={() => onModeChange("users")}
            >
              Users
            </button>
            <button
              type="button"
              className={mode === "cards" ? styles.searchModeButtonActive : styles.searchModeButton}
              onClick={() => onModeChange("cards")}
            >
              Cards
            </button>
          </div>

          <div className={styles.searchResults}>
            {loading ? <p className={styles.searchHint}>Loading results...</p> : null}
            {error ? <p className={styles.searchError}>{error}</p> : null}
            {isIdle ? <p className={styles.searchHint}>Start typing to search.</p> : null}
            {isEmpty ? <p className={styles.searchHint}>No results yet.</p> : null}

            {!loading && !error && mode === "showprofiles"
              ? showProfiles.map((profile) => (
                  <button
                    key={profile.username}
                    type="button"
                    className={styles.searchResultRow}
                    onClick={() => {
                      setOpen(false);
                      onSelect(`/gameplay-stats?user=${encodeURIComponent(profile.username)}`);
                    }}
                  >
                    <SearchResultAvatar path={profile.profile_img_url} alt={profile.display_name || profile.username} />
                    <span className={styles.searchResultCopy}>
                      <strong>{profile.username}</strong>
                      <small>{profile.display_name ? `${profile.display_name} · Gameplay stats` : "Gameplay stats"}</small>
                    </span>
                  </button>
                ))
              : null}

            {!loading && !error && mode === "users"
              ? users.map((user) => (
                  <button
                    key={user.id}
                    type="button"
                    className={styles.searchResultRow}
                    onClick={() => {
                      setOpen(false);
                      onSelect(`/profiles/${user.id}`);
                    }}
                  >
                    <SearchResultAvatar path={user.profile_img_url} alt={user.display_name} />
                    <span className={styles.searchResultCopy}>
                      <strong>{user.display_name}</strong>
                      <small>User profile</small>
                    </span>
                  </button>
                ))
              : null}

            {!loading && !error && mode === "cards"
              ? cards.map((card) => (
                  <button
                    key={card.id}
                    type="button"
                    className={styles.searchResultRow}
                    onClick={() => {
                      setOpen(false);
                      onSelect(`/cards/${card.id}`);
                    }}
                  >
                    <Image
                      src={card.baked_img || card.img || "/images/logo.png"}
                      alt={card.name}
                      width={34}
                      height={48}
                      className={styles.searchCardThumb}
                      unoptimized
                    />
                    <span className={styles.searchResultCopy}>
                      <strong>{card.name}</strong>
                      <small>
                        {card.series_name} · OVR {card.meta_overall_rounded ?? card.ovr}
                      </small>
                    </span>
                  </button>
                ))
              : null}
          </div>
        </div>
      ) : null}
    </div>
  );
}

export default function Navbar() {
  const pathname = usePathname() ?? "/";
  const router = useRouter();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [openDesktopGroup, setOpenDesktopGroup] = useState<string | null>(null);
  const [desktopSearch, setDesktopSearch] = useState("");
  const [mobileSearch, setMobileSearch] = useState("");
  const [desktopSearchMode, setDesktopSearchMode] = useState<SearchMode>("showprofiles");
  const [mobileSearchMode, setMobileSearchMode] = useState<SearchMode>("showprofiles");
  const [accountHref, setAccountHref] = useState("/signin");
  const [avatarSrc, setAvatarSrc] = useState<string | null>(null);
  const mounted = typeof document !== "undefined";
  const isSignedIn = accountHref === "/account";

  const activeGroups = useMemo(
    () => new Set(NAV_GROUPS.filter((group) => isGroupActive(pathname, group)).map((group) => group.label)),
    [pathname],
  );

  const onRunSearch = (query: string, mode: SearchMode) => {
    router.push(buildSearchHref(query, mode));
  };

  const onDesktopSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onRunSearch(desktopSearch, desktopSearchMode);
  };

  const onMobileSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onRunSearch(mobileSearch, mobileSearchMode);
    setMobileMenuOpen(false);
  };

  useEffect(() => {
    let runId = 0;
    let unsubscribe: (() => void) | null = null;
    let removeProfileImageListener: (() => void) | null = null;

    try {
      const auth = getFirebaseAuth();
      const loadAvatarForUser = async (uid: string, bustCache = false) => {
        runId += 1;
        const currentRunId = runId;

        setAccountHref("/account");

        const fallbackPath = `users/${uid}/profile.jpg`;
        let profilePath = fallbackPath;
        try {
          const me = await apiGetAuth<UserOut>("/users/me");
          const nextPath = me.profile_img_path?.trim();
          if (nextPath) {
            profilePath = nextPath;
          }
        } catch {
          // Keep fallback path.
        }

        const resolved = await resolveAvatarUrl(profilePath, { bustCache });
        if (currentRunId !== runId) {
          return;
        }
        setAvatarSrc(resolved);
      };

      unsubscribe = onAuthStateChanged(auth, (user) => {
        if (!user) {
          runId += 1;
          setAccountHref("/signin");
          setAvatarSrc(null);
          return;
        }

        void loadAvatarForUser(user.uid);
      });

      const onProfileImageUpdated = () => {
        const uid = auth.currentUser?.uid;
        if (!uid) {
          return;
        }
        void loadAvatarForUser(uid, true);
      };

      window.addEventListener(PROFILE_IMAGE_UPDATED_EVENT, onProfileImageUpdated);
      removeProfileImageListener = () => {
        window.removeEventListener(PROFILE_IMAGE_UPDATED_EVENT, onProfileImageUpdated);
      };
    } catch {}

    return () => {
      runId += 1;
      if (unsubscribe) {
        unsubscribe();
      }
      if (removeProfileImageListener) {
        removeProfileImageListener();
      }
    };
  }, []);

  useEffect(() => {
    if (!mobileMenuOpen) {
      return;
    }
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [mobileMenuOpen]);

  const mobileLayer = (
    <>
      <div
        className={`${styles.mobileOverlay} ${mobileMenuOpen ? styles.mobileOverlayOpen : ""}`}
        onClick={() => setMobileMenuOpen(false)}
        aria-hidden
      />
      <aside
        id="mobile-nav-drawer"
        className={`${styles.mobileDrawer} ${mobileMenuOpen ? styles.mobileDrawerOpen : ""}`}
        aria-hidden={!mobileMenuOpen}
      >
        <SearchAutocomplete
          query={mobileSearch}
          mode={mobileSearchMode}
          onQueryChange={setMobileSearch}
          onModeChange={setMobileSearchMode}
          onSubmit={onMobileSubmit}
          onSelect={(href) => {
            router.push(href);
            setMobileMenuOpen(false);
          }}
          variant="mobile"
        />

        {NAV_GROUPS.map((group) => (
          <details key={group.label} className={styles.mobileSection} open={activeGroups.has(group.label)}>
            <summary>{group.label}</summary>
            <div className={styles.mobileLinks}>
              {group.items.map((item) => (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setMobileMenuOpen(false)}
                  className={`${styles.mobileLink} ${isItemActive(pathname, item.href) ? styles.mobileLinkActive : ""}`}
                >
                  {item.label}
                </Link>
              ))}
            </div>
          </details>
        ))}
      </aside>
    </>
  );

  return (
    <nav className={styles.root}>
      <div className={styles.inner}>
        <div className={styles.left}>
          <button
            type="button"
            className={styles.hamburger}
            onClick={() => setMobileMenuOpen((prev) => !prev)}
            aria-expanded={mobileMenuOpen}
            aria-controls="mobile-nav-drawer"
            aria-label="Toggle menu"
          >
            <span />
            <span />
            <span />
          </button>

          <Link href="/" className={styles.brand} onClick={() => setMobileMenuOpen(false)}>
            <Image src="/images/logo.png" alt="DiamondInsights" width={44} height={44} className={styles.brandLogo} priority />
            <span className={styles.brandText}>
              <span>Diamond</span>
              <span className={styles.brandTextInsights}>Insights</span>
            </span>
          </Link>
        </div>

        <div className={styles.desktopGroups} onMouseLeave={() => setOpenDesktopGroup(null)}>
          {NAV_GROUPS.map((group) => (
            <div
              key={group.label}
              className={styles.group}
              onMouseEnter={() => setOpenDesktopGroup(group.label)}
              onFocus={() => setOpenDesktopGroup(group.label)}
              onBlur={(event) => {
                if (!event.currentTarget.contains(event.relatedTarget as Node | null)) {
                  setOpenDesktopGroup(null);
                }
              }}
            >
              <button
                type="button"
                className={styles.groupButton}
                aria-haspopup="menu"
                aria-expanded={openDesktopGroup === group.label}
                aria-controls={`desktop-dropdown-${group.label.toLowerCase()}`}
                onClick={() => setOpenDesktopGroup((prev) => (prev === group.label ? null : group.label))}
              >
                <span className={activeGroups.has(group.label) ? styles.groupButtonActive : ""}>{group.label}</span>
                <span className={styles.groupChevron} aria-hidden>
                  ▾
                </span>
              </button>

              <div
                id={`desktop-dropdown-${group.label.toLowerCase()}`}
                className={`${styles.dropdown} ${openDesktopGroup === group.label ? styles.dropdownOpen : ""}`}
                role="menu"
              >
                {group.items.map((item) => (
                  <Link
                    key={item.href}
                    href={item.href}
                    onClick={() => {
                      setMobileMenuOpen(false);
                      setOpenDesktopGroup(null);
                    }}
                    className={`${styles.dropdownLink} ${isItemActive(pathname, item.href) ? styles.dropdownLinkActive : ""}`}
                    role="menuitem"
                  >
                    {item.label}
                  </Link>
                ))}
              </div>
            </div>
          ))}
        </div>

        <SearchAutocomplete
          query={desktopSearch}
          mode={desktopSearchMode}
          onQueryChange={setDesktopSearch}
          onModeChange={setDesktopSearchMode}
          onSubmit={onDesktopSubmit}
          onSelect={(href) => router.push(href)}
          variant="desktop"
        />

        <div className={styles.right}>
          {isSignedIn ? (
            <Link
              href={accountHref}
              className={styles.accountButton}
              aria-label="Open account"
              onClick={() => setMobileMenuOpen(false)}
            >
              <Image
                src={avatarSrc || "/images/default_profile.png"}
                alt={avatarSrc ? "Profile" : "Default profile"}
                width={30}
                height={30}
                className={styles.accountAvatar}
                unoptimized={Boolean(avatarSrc)}
              />
            </Link>
          ) : (
            <Link href="/signin" className={styles.signInButton} onClick={() => setMobileMenuOpen(false)}>
              Sign In
            </Link>
          )}
        </div>
      </div>
      {mounted ? createPortal(mobileLayer, document.body) : null}
    </nav>
  );
}
