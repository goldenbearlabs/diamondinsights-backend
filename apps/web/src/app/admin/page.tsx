"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";

import styles from "./page.module.css";

type AdminMessage = {
  id: number;
  text: string;
  user_display_name: string | null;
  user_firebase_id: string | null;
  created_at: string | null;
  likes_count: number;
};

type AdminComment = {
  id: number;
  content: string;
  card_id: string;
  user_display_name: string | null;
  user_firebase_id: string | null;
  created_at: string | null;
  likes_count: number;
};

type SessionPayload = {
  authenticated?: boolean;
};

type RosterSettings = {
  next_roster_update_at: string | null;
  updated_at: string | null;
};

type AggregatorJobResponse = {
  job_id: string;
  job_type: string;
  enqueued_at: string;
};

type AdminTab = "moderation" | "roster";

const AGGREGATOR_CONFIRM_TEXT = "AGGREGATE PAST ROSTER-UPDATE";

async function extractDetail(response: Response, fallback: string): Promise<string> {
  const payload = await response.json().catch(() => null);
  if (payload && typeof payload.detail === "string" && payload.detail.trim()) {
    return payload.detail;
  }
  return fallback;
}

function formatUtc(iso: string | null): string {
  if (!iso) {
    return "n/a";
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return iso;
  }
  return date.toLocaleString();
}

function toLocalDateTimeInput(iso: string | null): string {
  if (!iso) {
    return "";
  }
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const offsetMs = date.getTimezoneOffset() * 60_000;
  const localDate = new Date(date.getTime() - offsetMs);
  return localDate.toISOString().slice(0, 16);
}

function toIsoFromLocalInput(value: string): string | null {
  if (!value.trim()) {
    return null;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString();
}

export default function AdminPage() {
  const [activeTab, setActiveTab] = useState<AdminTab>("moderation");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [authenticated, setAuthenticated] = useState(false);
  const [checkingSession, setCheckingSession] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyKey, setBusyKey] = useState<string | null>(null);

  const [messages, setMessages] = useState<AdminMessage[]>([]);
  const [comments, setComments] = useState<AdminComment[]>([]);
  const [loadingModeration, setLoadingModeration] = useState(false);

  const [rosterSettings, setRosterSettings] = useState<RosterSettings>({
    next_roster_update_at: null,
    updated_at: null,
  });
  const [rosterInput, setRosterInput] = useState("");
  const [loadingRoster, setLoadingRoster] = useState(false);

  const [confirmPhrase, setConfirmPhrase] = useState("");
  const [confirmChecked, setConfirmChecked] = useState(false);
  const [lastAggregatorJob, setLastAggregatorJob] = useState<AggregatorJobResponse | null>(null);

  const canRunAggregator = useMemo(() => {
    return confirmChecked && confirmPhrase.trim() === AGGREGATOR_CONFIRM_TEXT;
  }, [confirmChecked, confirmPhrase]);

  const loadModerationData = useCallback(async () => {
    setLoadingModeration(true);
    setError(null);
    try {
      const [messagesRes, commentsRes] = await Promise.all([
        fetch("/api/admin/messages?limit=100", { cache: "no-store" }),
        fetch("/api/admin/comments?limit=100", { cache: "no-store" }),
      ]);

      if (messagesRes.status === 401 || commentsRes.status === 401) {
        setAuthenticated(false);
        setMessages([]);
        setComments([]);
        return;
      }

      if (!messagesRes.ok) {
        const detail = await extractDetail(messagesRes, "Failed to load messages");
        throw new Error(detail);
      }
      if (!commentsRes.ok) {
        const detail = await extractDetail(commentsRes, "Failed to load comments");
        throw new Error(detail);
      }

      const nextMessages = (await messagesRes.json()) as AdminMessage[];
      const nextComments = (await commentsRes.json()) as AdminComment[];
      setMessages(nextMessages);
      setComments(nextComments);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load moderation data");
    } finally {
      setLoadingModeration(false);
    }
  }, []);

  const loadRosterSettings = useCallback(async () => {
    setLoadingRoster(true);
    setError(null);
    try {
      const response = await fetch("/api/admin/roster-settings", { cache: "no-store" });
      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }
      if (!response.ok) {
        const detail = await extractDetail(response, "Failed to load roster settings");
        throw new Error(detail);
      }

      const payload = (await response.json()) as RosterSettings;
      setRosterSettings(payload);
      setRosterInput(toLocalDateTimeInput(payload.next_roster_update_at));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load roster settings");
    } finally {
      setLoadingRoster(false);
    }
  }, []);

  const initializeSession = useCallback(async () => {
    setCheckingSession(true);
    setError(null);
    try {
      const response = await fetch("/api/admin/session", { cache: "no-store" });
      const payload = (await response.json().catch(() => ({}))) as SessionPayload;
      if (response.ok && payload.authenticated) {
        setAuthenticated(true);
        await Promise.all([loadModerationData(), loadRosterSettings()]);
      } else {
        setAuthenticated(false);
      }
    } catch {
      setAuthenticated(false);
      setError("Failed to verify admin session");
    } finally {
      setCheckingSession(false);
    }
  }, [loadModerationData, loadRosterSettings]);

  useEffect(() => {
    void initializeSession();
  }, [initializeSession]);

  async function handleLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBusyKey("login");
    setError(null);

    try {
      const response = await fetch("/api/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });

      if (!response.ok) {
        const detail = await extractDetail(response, "Login failed");
        throw new Error(detail);
      }

      setAuthenticated(true);
      setPassword("");
      await Promise.all([loadModerationData(), loadRosterSettings()]);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
      setAuthenticated(false);
    } finally {
      setBusyKey(null);
    }
  }

  async function handleLogout() {
    setBusyKey("logout");
    setError(null);
    try {
      await fetch("/api/admin/logout", { method: "POST" });
    } finally {
      setAuthenticated(false);
      setMessages([]);
      setComments([]);
      setBusyKey(null);
    }
  }

  async function handleDeleteMessage(messageId: number) {
    if (!window.confirm(`Delete chat message #${messageId}?`)) {
      return;
    }

    const key = `message:${messageId}`;
    setBusyKey(key);
    setError(null);
    try {
      const response = await fetch(`/api/admin/messages/${messageId}`, { method: "DELETE" });
      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }
      if (!response.ok) {
        const detail = await extractDetail(response, "Failed to delete message");
        throw new Error(detail);
      }
      setMessages((prev) => prev.filter((row) => row.id !== messageId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete message");
    } finally {
      setBusyKey(null);
    }
  }

  async function handleDeleteComment(commentId: number) {
    if (!window.confirm(`Delete card comment #${commentId}?`)) {
      return;
    }

    const key = `comment:${commentId}`;
    setBusyKey(key);
    setError(null);
    try {
      const response = await fetch(`/api/admin/comments/${commentId}`, { method: "DELETE" });
      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }
      if (!response.ok) {
        const detail = await extractDetail(response, "Failed to delete comment");
        throw new Error(detail);
      }
      setComments((prev) => prev.filter((row) => row.id !== commentId));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete comment");
    } finally {
      setBusyKey(null);
    }
  }

  async function handleSaveRosterSetting() {
    setBusyKey("save-roster-setting");
    setError(null);
    try {
      const nextValue = rosterInput.trim() ? toIsoFromLocalInput(rosterInput) : null;
      if (rosterInput.trim() && !nextValue) {
        throw new Error("Invalid date/time format");
      }

      const response = await fetch("/api/admin/roster-settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ next_roster_update_at: nextValue }),
      });
      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }
      if (!response.ok) {
        const detail = await extractDetail(response, "Failed to save roster setting");
        throw new Error(detail);
      }

      const payload = (await response.json()) as RosterSettings;
      setRosterSettings(payload);
      setRosterInput(toLocalDateTimeInput(payload.next_roster_update_at));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save roster setting");
    } finally {
      setBusyKey(null);
    }
  }

  async function handleEnqueueAggregator() {
    if (!canRunAggregator) {
      setError(`Type '${AGGREGATOR_CONFIRM_TEXT}' and acknowledge the warning.`);
      return;
    }

    const finalConfirm = window.confirm(
      "Final confirmation: enqueue 'roster-update-aggregator' now? This action is intended for irreversible operations.",
    );
    if (!finalConfirm) {
      return;
    }

    setBusyKey("enqueue-aggregator");
    setError(null);
    try {
      const response = await fetch("/api/admin/jobs/roster-update-aggregator", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ confirm_text: AGGREGATOR_CONFIRM_TEXT }),
      });
      if (response.status === 401) {
        setAuthenticated(false);
        return;
      }
      if (!response.ok) {
        const detail = await extractDetail(response, "Failed to enqueue roster-update-aggregator");
        throw new Error(detail);
      }

      const payload = (await response.json()) as AggregatorJobResponse;
      setLastAggregatorJob(payload);
      setConfirmPhrase("");
      setConfirmChecked(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to enqueue roster-update-aggregator");
    } finally {
      setBusyKey(null);
    }
  }

  if (checkingSession) {
    return (
      <main className={styles.page}>
        <div className={styles.loginPanel}>
          <h1>Admin Dashboard</h1>
          <p>Checking admin session...</p>
        </div>
      </main>
    );
  }

  if (!authenticated) {
    return (
      <main className={styles.page}>
        <form className={styles.loginPanel} onSubmit={handleLogin}>
          <h1>Admin Dashboard</h1>
          <p>Sign in to moderate content and run critical operations.</p>

          <label className={styles.field}>
            <span>Username</span>
            <input
              required
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
          </label>

          <label className={styles.field}>
            <span>Password</span>
            <input
              required
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          <button type="submit" disabled={busyKey === "login"}>
            {busyKey === "login" ? "Signing in..." : "Sign in"}
          </button>
          {error ? <p className={styles.error}>{error}</p> : null}
        </form>
      </main>
    );
  }

  return (
    <main className={styles.page}>
      <div className={styles.shell}>
        <aside className={styles.sidebar}>
          <div>
            <h1>Diamond Insights</h1>
            <p>Admin Dashboard</p>
          </div>

          <nav className={styles.tabList}>
            <button
              className={activeTab === "moderation" ? styles.tabActive : styles.tab}
              onClick={() => setActiveTab("moderation")}
            >
              Moderation
            </button>
            <button
              className={activeTab === "roster" ? styles.tabActive : styles.tab}
              onClick={() => setActiveTab("roster")}
            >
              Roster Ops
            </button>
          </nav>

          <button className={styles.signOutButton} onClick={() => void handleLogout()} disabled={busyKey === "logout"}>
            {busyKey === "logout" ? "Signing out..." : "Sign out"}
          </button>
        </aside>

        <section className={styles.content}>
          <header className={styles.contentHeader}>
            <div>
              <h2>{activeTab === "moderation" ? "Moderation Queue" : "Roster Operations"}</h2>
              <p>{activeTab === "moderation" ? "Manage user content." : "Schedule and execute roster update operations."}</p>
            </div>
            <div className={styles.kpiRow}>
              <div className={styles.kpi}>
                <span>Messages</span>
                <strong>{messages.length}</strong>
              </div>
              <div className={styles.kpi}>
                <span>Comments</span>
                <strong>{comments.length}</strong>
              </div>
            </div>
          </header>

          {error ? <p className={styles.errorBanner}>{error}</p> : null}

          {activeTab === "moderation" ? (
            <div className={styles.tabContent}>
              <div className={styles.sectionHeader}>
                <h3>Chat Messages</h3>
                <button onClick={() => void loadModerationData()} disabled={loadingModeration || !!busyKey}>
                  {loadingModeration ? "Refreshing..." : "Refresh"}
                </button>
              </div>
              <div className={styles.dataGrid}>
                {messages.map((message) => {
                  const rowKey = `message:${message.id}`;
                  return (
                    <article key={message.id} className={styles.itemCard}>
                      <div className={styles.itemMeta}>
                        <span>#{message.id}</span>
                        <span>{message.user_display_name || message.user_firebase_id || "Unknown user"}</span>
                        <span>{formatUtc(message.created_at)}</span>
                      </div>
                      <p>{message.text}</p>
                      <div className={styles.itemActions}>
                        <span>Likes: {message.likes_count}</span>
                        <button
                          className={styles.deleteButton}
                          onClick={() => void handleDeleteMessage(message.id)}
                          disabled={busyKey === rowKey}
                        >
                          {busyKey === rowKey ? "Deleting..." : "Delete"}
                        </button>
                      </div>
                    </article>
                  );
                })}
                {!messages.length ? <p className={styles.empty}>No chat messages found.</p> : null}
              </div>

              <div className={styles.sectionHeader}>
                <h3>Card Comments</h3>
              </div>
              <div className={styles.dataGrid}>
                {comments.map((comment) => {
                  const rowKey = `comment:${comment.id}`;
                  return (
                    <article key={comment.id} className={styles.itemCard}>
                      <div className={styles.itemMeta}>
                        <span>#{comment.id}</span>
                        <span>{comment.user_display_name || comment.user_firebase_id || "Unknown user"}</span>
                        <span>Card: {comment.card_id}</span>
                        <span>{formatUtc(comment.created_at)}</span>
                      </div>
                      <p>{comment.content}</p>
                      <div className={styles.itemActions}>
                        <span>Likes: {comment.likes_count}</span>
                        <button
                          className={styles.deleteButton}
                          onClick={() => void handleDeleteComment(comment.id)}
                          disabled={busyKey === rowKey}
                        >
                          {busyKey === rowKey ? "Deleting..." : "Delete"}
                        </button>
                      </div>
                    </article>
                  );
                })}
                {!comments.length ? <p className={styles.empty}>No comments found.</p> : null}
              </div>
            </div>
          ) : (
            <div className={styles.tabContent}>
              <section className={styles.settingsCard}>
                <h3>Next Roster Update</h3>
                <p>Store the next expected roster update timestamp as a single persisted value.</p>
                <label className={styles.field}>
                  <span>Next roster update date/time</span>
                  <input
                    type="datetime-local"
                    value={rosterInput}
                    onChange={(event) => setRosterInput(event.target.value)}
                  />
                </label>
                <div className={styles.inlineActions}>
                  <button onClick={() => void handleSaveRosterSetting()} disabled={busyKey === "save-roster-setting"}>
                    {busyKey === "save-roster-setting" ? "Saving..." : "Save date"}
                  </button>
                  <button
                    className={styles.ghostButton}
                    onClick={() => setRosterInput("")}
                    disabled={busyKey === "save-roster-setting"}
                  >
                    Clear input
                  </button>
                  <button
                    className={styles.ghostButton}
                    onClick={() => void loadRosterSettings()}
                    disabled={loadingRoster || !!busyKey}
                  >
                    {loadingRoster ? "Refreshing..." : "Reload"}
                  </button>
                </div>
                <div className={styles.readout}>
                  <span>
                    Current value: <strong>{formatUtc(rosterSettings.next_roster_update_at)}</strong>
                  </span>
                  <span>
                    Last updated: <strong>{formatUtc(rosterSettings.updated_at)}</strong>
                  </span>
                </div>
              </section>

              <section className={styles.dangerCard}>
                <h3>Aggregate Past Roster-Update</h3>
                <p>
                  This action enqueues <code>roster-update-aggregator</code>. Treat this as irreversible and use only when
                  explicitly required.
                </p>

                <label className={styles.field}>
                  <span>Type confirmation text exactly</span>
                  <input
                    value={confirmPhrase}
                    onChange={(event) => setConfirmPhrase(event.target.value)}
                    placeholder={AGGREGATOR_CONFIRM_TEXT}
                  />
                </label>

                <label className={styles.checkboxRow}>
                  <input
                    type="checkbox"
                    checked={confirmChecked}
                    onChange={(event) => setConfirmChecked(event.target.checked)}
                  />
                  <span>I understand this cannot be rolled back.</span>
                </label>

                <button
                  className={styles.dangerButton}
                  onClick={() => void handleEnqueueAggregator()}
                  disabled={!canRunAggregator || busyKey === "enqueue-aggregator"}
                >
                  {busyKey === "enqueue-aggregator" ? "Enqueuing..." : "Aggregate past roster-update"}
                </button>

                {lastAggregatorJob ? (
                  <div className={styles.readout}>
                    <span>
                      Last job id: <strong>{lastAggregatorJob.job_id}</strong>
                    </span>
                    <span>
                      Enqueued at: <strong>{formatUtc(lastAggregatorJob.enqueued_at)}</strong>
                    </span>
                  </div>
                ) : null}
              </section>
            </div>
          )}
        </section>
      </div>
    </main>
  );
}
