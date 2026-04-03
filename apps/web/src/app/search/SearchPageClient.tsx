"use client";

import Image from "next/image";
import Link from "next/link";
import { FormEvent, useEffect, useMemo, useRef, useState } from "react";
import { Search } from "lucide-react";
import { useRouter } from "next/navigation";

import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { apiGet } from "@/lib/api";
import { CURRENT_CARD_YEAR } from "@/lib/config";
import { resolveAvatarUrl } from "@/lib/profile-image";

import styles from "./page.module.css";

type SearchMode = "showprofiles" | "users" | "cards";

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
  is_live_set: boolean;
  series_name: string;
  rarity: string;
  img?: string | null;
  baked_img?: string | null;
  meta_overall_rounded?: number | null;
};

type SearchResponse = {
  users: UserResult[];
  cards: CardResult[];
};

type ShowProfileSearchResult = {
  user_id: number | null;
  username: string;
  display_name: string | null;
  profile_img_url: string | null;
};

type SearchAvatarProps = {
  path: string | null | undefined;
  alt: string;
  size?: number;
};

function normalizeMode(mode: string | undefined): SearchMode {
  if (mode === "showprofiles" || mode === "users" || mode === "cards") {
    return mode;
  }
  return "showprofiles";
}

function SearchAvatar({ path, alt, size = 42 }: SearchAvatarProps) {
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
      width={size}
      height={size}
      className={styles.avatar}
      unoptimized={Boolean(src)}
    />
  );
}

export default function SearchPageClient({
  initialQuery,
  initialMode,
}: {
  initialQuery: string;
  initialMode?: string;
}) {
  const router = useRouter();
  const requestRef = useRef(0);

  const [query, setQuery] = useState(initialQuery);
  const [submittedQuery, setSubmittedQuery] = useState(initialQuery);
  const [mode, setMode] = useState<SearchMode>(normalizeMode(initialMode));
  const [results, setResults] = useState<SearchResponse>({ users: [], cards: [] });
  const [showProfiles, setShowProfiles] = useState<ShowProfileSearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const trimmed = submittedQuery.trim();
    if (!trimmed) {
      return;
    }

    const requestId = requestRef.current + 1;
    requestRef.current = requestId;
    setLoading(true);
    setError(null);

    void (async () => {
      try {
        if (mode === "showprofiles") {
          const showParams = new URLSearchParams({ q: trimmed, limit: "12" });
          const showData = await apiGet<ShowProfileSearchResult[]>(`/users/show/search?${showParams.toString()}`);
          if (requestRef.current !== requestId) {
            return;
          }
          setResults({ users: [], cards: [] });
          setShowProfiles(showData);
          return;
        }

        const params = new URLSearchParams({ q: trimmed, limit: "12" });
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

        setResults(data);
        setShowProfiles([]);
      } catch (nextError: unknown) {
        if (requestRef.current !== requestId) {
          return;
        }
        setError(nextError instanceof Error ? nextError.message : "Search failed.");
        setResults({ users: [], cards: [] });
        setShowProfiles([]);
      } finally {
        if (requestRef.current === requestId) {
          setLoading(false);
        }
      }
    })();
  }, [mode, submittedQuery]);

  const totalCount = useMemo(() => {
    return results.users.length + results.cards.length + showProfiles.length;
  }, [results.cards.length, results.users.length, showProfiles.length]);

  const onSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = query.trim();
    setSubmittedQuery(trimmed);
    if (!trimmed) {
      setResults({ users: [], cards: [] });
      setShowProfiles([]);
      setLoading(false);
      setError(null);
    }

    const params = new URLSearchParams({ mode });
    if (trimmed) {
      params.set("q", trimmed);
    }
    router.replace(`/search?${params.toString()}`, { scroll: false });
  };

  const empty = !loading && !error && submittedQuery.trim() && totalCount === 0;

  return (
    <main className={styles.page}>
      <Navbar />
      <FloatingShieldsBackground />
      <div className={styles.texture} />

      <div className={styles.content}>
        <section className={styles.hero}>
          <div>
            <p className={styles.eyebrow}>Search</p>
            <h1>Cards, users, and Show profiles</h1>
            <p className={styles.heroCopy}>Search one pool at a time: linked Show usernames, public user pages, or current-season cards.</p>
          </div>

          <form className={styles.searchForm} onSubmit={onSubmit}>
            <label className={styles.searchField}>
              <Search size={16} aria-hidden />
              <input
                type="search"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search cards, users, or Show profiles"
                aria-label="Search cards, users, or Show profiles"
              />
            </label>

            <div className={styles.modeRow}>
              <button
                type="button"
                className={mode === "showprofiles" ? styles.modeButtonActive : styles.modeButton}
                onClick={() => setMode("showprofiles")}
              >
                Show Profiles
              </button>
              <button
                type="button"
                className={mode === "users" ? styles.modeButtonActive : styles.modeButton}
                onClick={() => setMode("users")}
              >
                Users
              </button>
              <button
                type="button"
                className={mode === "cards" ? styles.modeButtonActive : styles.modeButton}
                onClick={() => setMode("cards")}
              >
                Cards
              </button>
            </div>
          </form>
        </section>

        {!submittedQuery.trim() ? (
          <section className={styles.placeholderCard}>
            <p>Enter a name, username, or card title to start searching.</p>
          </section>
        ) : null}

        {loading ? (
          <section className={styles.placeholderCard}>
            <p>Loading results...</p>
          </section>
        ) : null}

        {error ? (
          <section className={styles.errorCard}>
            <p>{error}</p>
          </section>
        ) : null}

        {empty ? (
          <section className={styles.placeholderCard}>
            <p>{`No results for "${submittedQuery}".`}</p>
          </section>
        ) : null}

        {!loading && !error && mode === "users" && results.users.length > 0 ? (
          <section className={styles.panel}>
            <div className={styles.panelHeader}>
              <h2>User Profiles</h2>
              <span>{results.users.length}</span>
            </div>

            <div className={styles.resultGrid}>
              {results.users.map((user) => (
                <Link key={`user-${user.id}`} href={`/profiles/${user.id}`} className={styles.resultCard}>
                  <SearchAvatar path={user.profile_img_url} alt={user.display_name} />
                  <div className={styles.resultCopy}>
                    <strong>{user.display_name}</strong>
                    <span>Profile, portfolio, and linked gameplay</span>
                  </div>
                </Link>
              ))}
            </div>
          </section>
        ) : null}

        {!loading && !error && mode === "showprofiles" && showProfiles.length > 0 ? (
          <section className={styles.panel}>
            <div className={styles.panelHeader}>
              <h2>Show Profiles</h2>
              <span>{showProfiles.length}</span>
            </div>

            <div className={styles.resultGrid}>
              {showProfiles.map((profile) => (
                <Link
                  key={`show-${profile.username}`}
                  href={`/gameplay-stats?user=${encodeURIComponent(profile.username)}`}
                  className={styles.resultCard}
                >
                  <SearchAvatar path={profile.profile_img_url} alt={profile.display_name || profile.username} />
                  <div className={styles.resultCopy}>
                    <strong>{profile.username}</strong>
                    <span>{profile.display_name ? `${profile.display_name} · Gameplay stats` : "Gameplay stats"}</span>
                  </div>
                </Link>
              ))}
            </div>
          </section>
        ) : null}

        {!loading && !error && mode === "cards" && results.cards.length > 0 ? (
          <section className={styles.panel}>
            <div className={styles.panelHeader}>
              <h2>Cards</h2>
              <span>{results.cards.length}</span>
            </div>

            <div className={styles.cardGrid}>
              {results.cards.map((card) => (
                <Link key={card.id} href={`/cards/${card.id}`} className={styles.cardResult}>
                  <Image
                    src={card.baked_img || card.img || "/images/logo.png"}
                    alt={card.name}
                    width={72}
                    height={100}
                    className={styles.cardImage}
                    unoptimized
                  />
                  <div className={styles.resultCopy}>
                    <strong>{card.name}</strong>
                    <span>
                      {card.series_name} · OVR {card.meta_overall_rounded ?? card.ovr}
                    </span>
                    <small>{card.year}</small>
                  </div>
                </Link>
              ))}
            </div>
          </section>
        ) : null}
      </div>
    </main>
  );
}
