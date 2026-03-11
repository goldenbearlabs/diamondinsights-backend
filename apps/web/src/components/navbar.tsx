"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { FormEvent, useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { onAuthStateChanged } from "firebase/auth";

import styles from "./navbar.module.css";
import { apiGetAuth } from "@/lib/api";
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
      { label: "Blogs", href: "/blogs" },
    ],
  },
];

function isItemActive(pathname: string, href: string): boolean {
  return pathname === href || pathname.startsWith(`${href}/`);
}

function isGroupActive(pathname: string, group: NavGroup): boolean {
  return group.items.some((item) => isItemActive(pathname, item.href));
}

export default function Navbar() {
  const pathname = usePathname() ?? "/";
  const router = useRouter();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [openDesktopGroup, setOpenDesktopGroup] = useState<string | null>(null);
  const [desktopSearch, setDesktopSearch] = useState("");
  const [mobileSearch, setMobileSearch] = useState("");
  const [accountHref, setAccountHref] = useState("/signin");
  const [avatarSrc, setAvatarSrc] = useState<string | null>(null);
  const mounted = typeof document !== "undefined";
  const isSignedIn = accountHref === "/account";

  const activeGroups = useMemo(
    () => new Set(NAV_GROUPS.filter((group) => isGroupActive(pathname, group)).map((group) => group.label)),
    [pathname],
  );

  const onRunSearch = (query: string) => {
    const trimmed = query.trim();
    if (!trimmed) {
      router.push("/search");
      return;
    }
    router.push(`/search?q=${encodeURIComponent(trimmed)}`);
  };

  const onDesktopSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onRunSearch(desktopSearch);
  };

  const onMobileSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    onRunSearch(mobileSearch);
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
        <form className={styles.mobileSearch} onSubmit={onMobileSubmit}>
          <input
            type="search"
            value={mobileSearch}
            onChange={(event) => setMobileSearch(event.target.value)}
            placeholder="Search users or cards"
            aria-label="Search"
          />
        </form>

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
                <span className={activeGroups.has(group.label) ? styles.groupButtonActive : ""}>
                  {group.label}
                </span>
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

        <form className={styles.desktopSearch} onSubmit={onDesktopSubmit}>
          <input
            type="search"
            value={desktopSearch}
            onChange={(event) => setDesktopSearch(event.target.value)}
            placeholder="Search users or cards"
            aria-label="Search"
          />
        </form>

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
