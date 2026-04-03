"use client";

import { useEffect, useRef, useState } from "react";
import { Send, X } from "lucide-react";

import type { ChatMessage } from "@/types/chat";

import styles from "./chat.module.css";

type ChatComposerProps = {
  onSend: (text: string, replyToId?: number) => boolean;
  replyingTo: ChatMessage | null;
  onCancelReply: () => void;
  disabled?: boolean;
};

export function ChatComposer({ onSend, replyingTo, onCancelReply, disabled = false }: ChatComposerProps) {
  const [text, setText] = useState("");
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    const node = textareaRef.current;
    if (!node) {
      return;
    }

    node.style.height = "0px";
    node.style.height = `${Math.min(node.scrollHeight, 164)}px`;
  }, [text]);

  const submit = () => {
    const trimmed = text.trim();
    if (!trimmed || disabled) {
      return;
    }

    const sent = onSend(trimmed, replyingTo?.id);
    if (!sent) {
      return;
    }

    setText("");
    onCancelReply();
  };

  return (
    <div className={styles.composer}>
      {replyingTo ? (
        <div className={styles.replyBanner}>
          <div className={styles.replyBannerText}>
            <span>Replying to</span>
            <strong>{replyingTo.userName}</strong>
          </div>
          <button type="button" className={styles.inlineButton} onClick={onCancelReply} aria-label="Cancel reply">
            <X size={16} />
          </button>
        </div>
      ) : null}

      <div className={styles.composerRow}>
        <textarea
          ref={textareaRef}
          className={styles.textarea}
          value={text}
          onChange={(event) => setText(event.target.value)}
          placeholder={replyingTo ? "Write a reply..." : "Message the Diamond Insights community"}
          maxLength={500}
          rows={1}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              submit();
            }
          }}
          disabled={disabled}
        />

        <button
          type="button"
          className={styles.sendButton}
          onClick={submit}
          disabled={disabled || text.trim().length === 0}
          aria-label="Send message"
        >
          <Send size={16} />
        </button>
      </div>
    </div>
  );
}
