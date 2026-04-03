"use client";

import { useEffect, useMemo, useState } from "react";
import { Heart, Pencil, Reply, Trash2 } from "lucide-react";

import { resolveAvatarUrl } from "@/lib/profile-image";
import type { ChatMessage } from "@/types/chat";

import styles from "./chat.module.css";

type ChatMessageRowProps = {
  message: ChatMessage;
  currentFirebaseUid: string | null;
  onReply: (message: ChatMessage) => void;
  onEdit: (message: ChatMessage) => void;
  onDelete: (message: ChatMessage) => void;
  onLike: (id: number) => void;
};

function formatMessageTime(value: string): string {
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(new Date(value));
}

export function ChatMessageRow({
  message,
  currentFirebaseUid,
  onReply,
  onEdit,
  onDelete,
  onLike,
}: ChatMessageRowProps) {
  const [avatarUrl, setAvatarUrl] = useState<string | null>(null);

  useEffect(() => {
    let active = true;

    void resolveAvatarUrl(message.userImage).then((resolved) => {
      if (active) {
        setAvatarUrl(resolved);
      }
    });

    return () => {
      active = false;
    };
  }, [message.userImage]);

  const timeText = useMemo(() => formatMessageTime(message.createdAt), [message.createdAt]);
  const likeCount = message.likedByFirebaseIds?.length ?? 0;
  const likedByMe = currentFirebaseUid ? message.likedByFirebaseIds?.includes(currentFirebaseUid) : false;

  return (
    <article className={`${styles.messageCard} ${message.isMe ? styles.messageCardMine : ""}`}>
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img
        src={avatarUrl || "/images/default_profile.png"}
        alt={`${message.userName} avatar`}
        className={styles.avatar}
      />

      <div className={styles.messageBody}>
        <div className={styles.messageTopRow}>
          <div className={styles.identityBlock}>
            <strong className={styles.userName}>{message.userName}</strong>
            <span className={styles.timestamp}>{timeText}</span>
            {message.editedAt ? <span className={styles.editedTag}>edited</span> : null}
          </div>

          <div className={styles.actionRow}>
            <button
              type="button"
              className={`${styles.actionButton} ${likedByMe ? styles.actionButtonLiked : ""}`}
              onClick={() => onLike(message.id)}
              aria-label={likedByMe ? "Unlike message" : "Like message"}
            >
              <Heart size={15} fill={likedByMe ? "currentColor" : "none"} />
              <span>{likeCount}</span>
            </button>

            <button type="button" className={styles.actionButton} onClick={() => onReply(message)} aria-label="Reply">
              <Reply size={15} />
            </button>

            {message.isMe ? (
              <>
                <button type="button" className={styles.actionButton} onClick={() => onEdit(message)} aria-label="Edit">
                  <Pencil size={15} />
                </button>
                <button
                  type="button"
                  className={styles.actionButton}
                  onClick={() => onDelete(message)}
                  aria-label="Delete"
                >
                  <Trash2 size={15} />
                </button>
              </>
            ) : null}
          </div>
        </div>

        {message.replyTo ? (
          <div className={styles.replyContext}>
            <span className={styles.replyLine} />
            <p>
              Replying to <strong>{message.replyTo.userName}</strong>: {message.replyTo.text}
            </p>
          </div>
        ) : null}

        <p className={styles.messageText}>{message.text}</p>
      </div>
    </article>
  );
}
