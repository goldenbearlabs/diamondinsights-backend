"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { User } from "firebase/auth";

import type { ChatMessage, WebSocketMessage } from "@/types/chat";

type SocketUrlResponse = {
  url?: string;
};

function withIsMe(message: ChatMessage, firebaseUser: User | null): ChatMessage {
  if (firebaseUser?.uid && message.userFirebaseId === firebaseUser.uid) {
    return { ...message, isMe: true };
  }
  return message;
}

export function useChatSocket(firebaseUser: User | null) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const shouldReconnectRef = useRef(true);

  const clearReconnectTimer = useCallback(() => {
    if (reconnectTimerRef.current !== null) {
      window.clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
  }, []);

  const disconnect = useCallback(() => {
    clearReconnectTimer();

    const socket = socketRef.current;
    socketRef.current = null;
    if (socket && socket.readyState === WebSocket.OPEN) {
      socket.close();
    } else if (socket && socket.readyState === WebSocket.CONNECTING) {
      socket.close();
    }

    setIsConnected(false);
  }, [clearReconnectTimer]);

  const connect = useCallback(async () => {
    if (!firebaseUser) {
      return;
    }

    const activeSocket = socketRef.current;
    if (activeSocket && (activeSocket.readyState === WebSocket.OPEN || activeSocket.readyState === WebSocket.CONNECTING)) {
      return;
    }

    clearReconnectTimer();
    setError(null);

    try {
      const token = await firebaseUser.getIdToken(true);
      const response = await fetch("/api/chat/socket-url", { cache: "no-store" });
      if (!response.ok) {
        throw new Error("Unable to resolve chat server URL.");
      }

      const payload = (await response.json()) as SocketUrlResponse;
      const baseUrl = payload.url?.trim();
      if (!baseUrl) {
        throw new Error("Chat server URL is missing.");
      }

      const socket = new WebSocket(`${baseUrl}?token=${encodeURIComponent(token)}`);
      socketRef.current = socket;

      socket.onopen = () => {
        setMessages([]);
        setIsConnected(true);
      };

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data) as WebSocketMessage;
          const payloadMessage = message.payload;

          if (message.type === "history_item" || message.type === "new_message") {
            if (!payloadMessage || typeof payloadMessage !== "object" || !("id" in payloadMessage)) {
              return;
            }

            const nextMessage = withIsMe(payloadMessage as ChatMessage, firebaseUser);
            setMessages((prev) => {
              if (prev.some((item) => item.id === nextMessage.id)) {
                return prev;
              }
              return [...prev, nextMessage];
            });
            return;
          }

          if (message.type === "update_message") {
            if (!payloadMessage || typeof payloadMessage !== "object" || !("id" in payloadMessage)) {
              return;
            }

            const nextMessage = withIsMe(payloadMessage as ChatMessage, firebaseUser);
            setMessages((prev) => prev.map((item) => (item.id === nextMessage.id ? { ...item, ...nextMessage } : item)));
            return;
          }

          if (message.type === "delete_message") {
            const rawId = payloadMessage && typeof payloadMessage === "object" && "id" in payloadMessage ? payloadMessage.id : null;
            const messageId = typeof rawId === "number" ? rawId : Number(rawId);
            if (Number.isFinite(messageId)) {
              setMessages((prev) => prev.filter((item) => item.id !== messageId));
            }
          }
        } catch {
          setError("Received an invalid chat payload.");
        }
      };

      socket.onclose = () => {
        setIsConnected(false);
        socketRef.current = null;

        if (!shouldReconnectRef.current) {
          return;
        }

        reconnectTimerRef.current = window.setTimeout(() => {
          void connect();
        }, 2500);
      };

      socket.onerror = () => {
        setError("Chat connection failed.");
      };
    } catch (err: unknown) {
      const message = err instanceof Error && err.message.trim() ? err.message.trim() : "Unable to connect to chat.";
      setError(message);
      setIsConnected(false);
    }
  }, [clearReconnectTimer, firebaseUser]);

  useEffect(() => {
    shouldReconnectRef.current = true;

    if (!firebaseUser) {
      disconnect();
      setMessages([]);
      setError(null);
      return () => {
        shouldReconnectRef.current = false;
      };
    }

    void connect();

    const reconnect = () => {
      disconnect();
      void connect();
    };

    const onVisibilityChange = () => {
      if (document.visibilityState === "visible") {
        reconnect();
      }
    };

    window.addEventListener("online", reconnect);
    document.addEventListener("visibilitychange", onVisibilityChange);

    return () => {
      shouldReconnectRef.current = false;
      window.removeEventListener("online", reconnect);
      document.removeEventListener("visibilitychange", onVisibilityChange);
      disconnect();
    };
  }, [connect, disconnect, firebaseUser]);

  const sendCommand = useCallback((payload: Record<string, unknown>) => {
    const socket = socketRef.current;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      setError("Chat is reconnecting. Try again in a moment.");
      void connect();
      return false;
    }

    socket.send(JSON.stringify(payload));
    return true;
  }, [connect]);

  const sendMessage = useCallback((text: string, replyToId?: number) => {
    return sendCommand({
      type: "new_message",
      text,
      parentId: replyToId,
    });
  }, [sendCommand]);

  const editMessage = useCallback((id: number, text: string) => {
    return sendCommand({
      type: "edit_message",
      id,
      text,
    });
  }, [sendCommand]);

  const deleteMessage = useCallback((id: number) => {
    return sendCommand({
      type: "delete_message",
      id,
    });
  }, [sendCommand]);

  const toggleLike = useCallback((id: number) => {
    return sendCommand({
      type: "toggle_like",
      id,
    });
  }, [sendCommand]);

  const reconnect = useCallback(() => {
    disconnect();
    void connect();
  }, [connect, disconnect]);

  return {
    messages,
    isConnected,
    error,
    sendMessage,
    editMessage,
    deleteMessage,
    toggleLike,
    reconnect,
  };
}
