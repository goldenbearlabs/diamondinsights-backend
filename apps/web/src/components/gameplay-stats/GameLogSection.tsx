"use client";

import { useMemo, useState } from "react";

import { GameDetailSection } from "./GameDetailSection";
import { formatDate } from "./format";
import type { ShowGameLogItem } from "./types";
import styles from "./styles.module.css";

type Props = {
  games: ShowGameLogItem[];
  username: string | null;
  loading: boolean;
  error: string | null;
};

type ResultFilter = "all" | "wins" | "losses";

function perspective(game: ShowGameLogItem, username: string | null) {
  const normalized = username ? username.toLowerCase() : null;
  const isHome = normalized !== null && (game.home_profile_username || "").toLowerCase() === normalized;
  const isAway = normalized !== null && (game.away_profile_username || "").toLowerCase() === normalized;

  const userResult = isHome ? game.home_result : isAway ? game.away_result : null;
  const scoreFor = isHome ? game.home_runs : isAway ? game.away_runs : game.home_runs;
  const scoreAgainst = isHome ? game.away_runs : isAway ? game.home_runs : game.away_runs;
  const opponentName = isHome
    ? game.away_full_name || game.away_profile_username
    : isAway
      ? game.home_full_name || game.home_profile_username
      : game.away_full_name || game.away_profile_username;
  const location = isHome ? "vs" : isAway ? "@" : "vs";

  return {
    userResult,
    scoreFor,
    scoreAgainst,
    opponentName,
    location,
  };
}

function dedupeKey(game: ShowGameLogItem, username: string | null): string {
  const item = perspective(game, username);
  const parsedDate = new Date(game.date);
  const dateKey = Number.isNaN(parsedDate.getTime())
    ? (game.date || "").slice(0, 10)
    : parsedDate.toISOString().slice(0, 10);
  const opponent = (item.opponentName || "").trim().toLowerCase();
  return `${dateKey}|${opponent}`;
}

export function GameLogSection({ games, username, loading, error }: Props) {
  const [difficulty, setDifficulty] = useState<string>("all");
  const [result, setResult] = useState<ResultFilter>("all");
  const [ballpark, setBallpark] = useState("");
  const [selectedGame, setSelectedGame] = useState<ShowGameLogItem | null>(null);

  const difficultyOptions = useMemo(() => {
    const values = new Set<string>();
    games.forEach((game) => {
      if (game.difficulty) {
        values.add(game.difficulty);
      }
    });
    return ["all", ...Array.from(values).sort((a, b) => a.localeCompare(b))];
  }, [games]);

  const filteredGames = useMemo(() => {
    const query = ballpark.trim().toLowerCase();
    const sorted = [...games]
      .filter((game) => {
        if (difficulty !== "all" && (game.difficulty || "").toLowerCase() !== difficulty.toLowerCase()) {
          return false;
        }

        const gamePerspective = perspective(game, username);
        if (result === "wins" && gamePerspective.userResult !== "W") {
          return false;
        }
        if (result === "losses" && gamePerspective.userResult !== "L") {
          return false;
        }

        if (query && !(game.ball_park_name || "").toLowerCase().includes(query)) {
          return false;
        }

        return true;
      })
      .sort((a, b) => new Date(b.date).getTime() - new Date(a.date).getTime());

    const seen = new Set<string>();
    const deduped: ShowGameLogItem[] = [];
    sorted.forEach((game) => {
      const key = dedupeKey(game, username);
      if (seen.has(key)) {
        return;
      }
      seen.add(key);
      deduped.push(game);
    });

    return deduped;
  }, [games, difficulty, result, ballpark, username]);

  if (selectedGame) {
    return <GameDetailSection game={selectedGame} username={username} onBack={() => setSelectedGame(null)} />;
  }

  return (
    <section className={styles.gameLogSection}>
      <div className={styles.sectionHeaderRow}>
        <div>
          <h3 className={styles.sectionTitle}>Game Log</h3>
          <p className={styles.sectionSubtitle}>Recent online games with difficulty and park filters.</p>
        </div>
      </div>

      <div className={styles.filtersRow}>
        <select
          className={styles.filterSelect}
          value={difficulty}
          onChange={(event) => setDifficulty(event.target.value)}
        >
          {difficultyOptions.map((option) => (
            <option key={option} value={option}>
              {option === "all" ? "All difficulties" : option}
            </option>
          ))}
        </select>

        <select className={styles.filterSelect} value={result} onChange={(event) => setResult(event.target.value as ResultFilter)}>
          <option value="all">All results</option>
          <option value="wins">Wins</option>
          <option value="losses">Losses</option>
        </select>

        <input
          className={styles.filterInput}
          value={ballpark}
          onChange={(event) => setBallpark(event.target.value)}
          placeholder="Filter by ballpark"
        />
      </div>

      {loading ? <p className={styles.muted}>Loading game log...</p> : null}
      {error ? <p className={styles.error}>{error}</p> : null}

      <div className={styles.gamesList}>
        {filteredGames.map((game) => {
          const item = perspective(game, username);
          const win = item.userResult === "W";
          return (
            <button
              key={`${game.game_id}-${game.date}`}
              type="button"
              className={styles.gameRowButton}
              onClick={() => setSelectedGame(game)}
            >
              <div className={styles.gameRow}>
                <div className={styles.gameTop}>
                  <p className={styles.gameOpponent}>
                    {item.location} {item.opponentName}
                  </p>
                  <p className={`${styles.gameScore} ${win ? styles.gameScoreWin : styles.gameScoreLoss}`}>
                    {item.userResult || "-"} {item.scoreFor}-{item.scoreAgainst}
                  </p>
                </div>
                <p className={styles.gameMeta}>
                  {formatDate(game.date)} • {game.difficulty || "Unknown"} • {game.ball_park_name || "Unknown park"}
                </p>
                {game.summary ? <p className={styles.gameSummary}>{game.summary}</p> : null}
              </div>
            </button>
          );
        })}

        {!loading && filteredGames.length === 0 ? <p className={styles.muted}>No games found.</p> : null}
      </div>
    </section>
  );
}
