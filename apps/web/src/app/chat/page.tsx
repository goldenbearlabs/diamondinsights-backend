"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { onAuthStateChanged, type User } from "firebase/auth";
import { MessageCircle, RefreshCcw, Sparkles, Users } from "lucide-react";
import { useRouter } from "next/navigation";

import { ChatComposer } from "@/components/chat/ChatComposer";
import { ChatMessageRow } from "@/components/chat/ChatMessageRow";
import { FloatingShieldsBackground } from "@/components/FloatingShieldsBackground";
import Navbar from "@/components/navbar";
import { toReadableAuthError } from "@/lib/auth-errors";
import { getFirebaseAuth } from "@/lib/firebase";
import { useChatSocket } from "@/hooks/useChatSocket";
import type { ChatMessage } from "@/types/chat";

import styles from "./page.module.css";

function statusCopy(isConnected: boolean): string {
  return isConnected ? "Live now" : "Reconnecting";
}

export default function ChatPage() {
  const router = useRouter();

  const [authReady, setAuthReady] = useState(false);
  const [firebaseUser, setFirebaseUser] = useState<User | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);

  const [replyingTo, setReplyingTo] = useState<ChatMessage | null>(null);
  const [editingMessage, setEditingMessage] = useState<ChatMessage | null>(null);
  const [editText, setEditText] = useState("");

  const listRef = useRef<HTMLDivElement | null>(null);
  const shouldStickToBottomRef = useRef(true);

  const { messages, isConnected, error, sendMessage, editMessage, deleteMessage, toggleLike, reconnect } =
    useChatSocket(firebaseUser);

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
    } catch (err: unknown) {
      queueMicrotask(() => {
        setAuthError(toReadableAuthError(err, "Firebase auth is not configured for web."));
        setAuthReady(true);
      });
    }

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, [router]);

  useEffect(() => {
    if (!listRef.current || !shouldStickToBottomRef.current) {
      return;
    }
    listRef.current.scrollTop = listRef.current.scrollHeight;
  }, [messages]);

  const handleScroll = () => {
    const node = listRef.current;
    if (!node) {
      return;
    }

    const distanceFromBottom = node.scrollHeight - node.scrollTop - node.clientHeight;
    shouldStickToBottomRef.current = distanceFromBottom < 96;
  };

  const messageCount = messages.length;
  const currentUid = firebaseUser?.uid ?? null;

  const connectionBadgeClass = isConnected ? styles.connectionLive : styles.connectionOffline;
  const sidebarItems = useMemo(
    () => [
      {
        icon: <Users size={16} />,
        label: "Room",
        value: "Community chat",
      },
      {
        icon: <MessageCircle size={16} />,
        label: "Messages",
        value: messageCount.toLocaleString(),
      },
      {
        icon: <Sparkles size={16} />,
        label: "Status",
        value: statusCopy(isConnected),
      },
    ],
    [isConnected, messageCount],
  );

  const openEditModal = (message: ChatMessage) => {
    setEditingMessage(message);
    setEditText(message.text);
  };

  const closeEditModal = () => {
    setEditingMessage(null);
    setEditText("");
  };

  const submitEdit = () => {
    if (!editingMessage) {
      return;
    }

    const trimmed = editText.trim();
    if (!trimmed) {
      return;
    }

    const updated = editMessage(editingMessage.id, trimmed);
    if (updated) {
      closeEditModal();
    }
  };

  const confirmDelete = (message: ChatMessage) => {
    const confirmed = window.confirm("Delete this message?");
    if (!confirmed) {
      return;
    }

    const deleted = deleteMessage(message.id);
    if (deleted && editingMessage?.id === message.id) {
      closeEditModal();
    }
  };

  if (!authReady) {
    return (
      <div className={styles.page}>
        <FloatingShieldsBackground />
        <Navbar />
        <main className={styles.loadingState}>
          <div className={styles.spinner} />
          <p>Loading chat...</p>
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
        <section className={styles.hero}>
          <div className={styles.heroCopy}>
            <span className={`${styles.connectionBadge} ${connectionBadgeClass}`}>
              <span className={styles.connectionDot} />
              {statusCopy(isConnected)}
            </span>
            <h1>Community Chat</h1>
            <p>
              Same live room as the mobile app, rebuilt for the website with reply threads, likes, and quick edit tools.
            </p>
          </div>

          <button type="button" className={styles.refreshButton} onClick={reconnect}>
            <RefreshCcw size={15} />
            <span>Reconnect</span>
          </button>
        </section>

        {authError ? <div className={styles.errorBanner}>{authError}</div> : null}
        {error ? <div className={styles.errorBanner}>{error}</div> : null}

        <section className={styles.layout}>
          <aside className={styles.sidebar}>
            <div className={styles.sidebarCard}>
              <h2>Room Snapshot</h2>
              <div className={styles.metricList}>
                {sidebarItems.map((item) => (
                  <div key={item.label} className={styles.metricRow}>
                    <div className={styles.metricLabel}>
                      {item.icon}
                      <span>{item.label}</span>
                    </div>
                    <strong>{item.value}</strong>
                  </div>
                ))}
              </div>
            </div>

            <div className={styles.sidebarCard}>
              <h2>Posting Tips</h2>
              <ul className={styles.tipList}>
                <li>Press `Enter` to send and `Shift` + `Enter` for a new line.</li>
                <li>Reply inline to keep trade ideas and lineup talk anchored.</li>
                <li>Use likes to surface the strongest takes without flooding the room.</li>
              </ul>
            </div>
          </aside>

          <section className={styles.chatShell}>
            <header className={styles.chatHeader}>
              <div>
                <h2>Main Room</h2>
                <p>Real-time discussion with the Diamond Insights community.</p>
              </div>
              <span className={styles.messageCount}>{messageCount} message{messageCount === 1 ? "" : "s"}</span>
            </header>

            <div ref={listRef} className={styles.messageList} onScroll={handleScroll}>
              {messages.length === 0 ? (
                <div className={styles.emptyState}>
                  <MessageCircle size={24} />
                  <p>No messages yet. Start the conversation.</p>
                </div>
              ) : (
                messages.map((message) => (
                  <ChatMessageRow
                    key={message.id}
                    message={message}
                    currentFirebaseUid={currentUid}
                    onReply={setReplyingTo}
                    onEdit={openEditModal}
                    onDelete={confirmDelete}
                    onLike={toggleLike}
                  />
                ))
              )}
            </div>

            <ChatComposer
              onSend={sendMessage}
              replyingTo={replyingTo}
              onCancelReply={() => setReplyingTo(null)}
              disabled={!isConnected}
            />
          </section>
        </section>
      </main>

      {editingMessage ? (
        <div className={styles.modalBackdrop} role="presentation" onClick={closeEditModal}>
          <div className={styles.modalCard} role="dialog" aria-modal="true" aria-labelledby="chat-edit-title" onClick={(event) => event.stopPropagation()}>
            <div className={styles.modalHeader}>
              <div>
                <p className={styles.modalEyebrow}>Edit message</p>
                <h2 id="chat-edit-title">Update your post</h2>
              </div>
              <button type="button" className={styles.modalClose} onClick={closeEditModal} aria-label="Close edit dialog">
                ×
              </button>
            </div>

            <textarea
              className={styles.modalTextarea}
              value={editText}
              onChange={(event) => setEditText(event.target.value)}
              maxLength={500}
              rows={6}
              autoFocus
            />

            <div className={styles.modalActions}>
              <button type="button" className={styles.deleteButton} onClick={() => confirmDelete(editingMessage)}>
                Delete
              </button>
              <button type="button" className={styles.saveButton} onClick={submitEdit} disabled={editText.trim().length === 0}>
                Save changes
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
