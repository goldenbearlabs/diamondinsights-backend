// apps/mobile/src/hooks/useChatSocket.ts
import { useEffect, useRef, useState, useCallback } from "react";
import { AppState, AppStateStatus } from "react-native";
import { auth } from "../lib/firebase"; 
import { ChatMessage, WebSocketMessage } from "../types/chat";

const getWsUrl = () => {
  // Make sure this is your REAL IP address
  const apiUrl = process.env.EXPO_PUBLIC_API_BASE_URL || "http://192.168.1.127:8000"; 
  return apiUrl.replace(/^http/, "ws") + "/ws/chat";
};

export const useChatSocket = () => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  
  const ws = useRef<WebSocket | null>(null);
  const appState = useRef(AppState.currentState);

  const connect = useCallback(async () => {
    if (ws.current?.readyState === WebSocket.OPEN || ws.current?.readyState === WebSocket.CONNECTING) {
      return;
    }

    try {
      const user = auth.currentUser;
      if (!user) return;
      
      const token = await user.getIdToken(true);

      const url = `${getWsUrl()}?token=${token}`;
      console.log("Connecting to Chat:", url);
      
      const socket = new WebSocket(url);
      ws.current = socket;

      socket.onopen = () => {
        setIsConnected(true);
        console.log("Chat Connected");
      };

      socket.onmessage = (e) => {
        try {
          const data = JSON.parse(e.data) as WebSocketMessage;
          const currentUserUid = auth.currentUser?.uid;

          const fixIsMe = (msg: any) => {
            if (currentUserUid && msg.userFirebaseId === currentUserUid) {
              return { ...msg, isMe: true };
            }
            return msg;
          };
          

          if (data.type === "history_item") {
            setMessages((prev) => {
              if (prev.some((msg) => msg.id === data.payload.id)) {
                return prev; // Ignore it
              }
              return [fixIsMe(data.payload), ...prev];
            });
          } 
          else if (data.type === "new_message") {
            setMessages((prev) => {
              if (prev.some((msg) => msg.id === data.payload.id)) {
                return prev; 
              }
              return [fixIsMe(data.payload), ...prev];
            });
          } 
          else if (data.type === "update_message") {
            setMessages((prev) =>
              prev.map((msg) =>
                msg.id === data.payload.id ? { ...msg, ...data.payload } : msg
              )
            );
          }
          else if (data.type === "delete_message"){
            setMessages((prev) => prev.filter((msg) => msg.id !== data.payload.id));
          }
        } catch (err) {
          console.error("Chat Parse Error:", err);
        }
      };

      

      socket.onclose = (e) => {
        setIsConnected(false);
        console.log("Chat Disconnected", e.reason);
      };

      socket.onerror = (e) => {
        console.log("Chat Error", e);
      };

    } catch (err) {
      console.error("Auth Token Error:", err);
      setIsConnected(false);
    }
  }, []);

  const disconnect = useCallback(() => {
    if (ws.current) {
      ws.current.close();
      ws.current = null;
    }
    setIsConnected(false);
  }, []);

  useEffect(() => {
    connect();

    const subscription = AppState.addEventListener("change", (nextAppState: AppStateStatus) => {
      if (
        appState.current.match(/inactive|background/) &&
        nextAppState === "active"
      ) {
        console.log("App has come to the foreground! Reconnecting chat...");
        connect();
      }

      appState.current = nextAppState;
    });

    return () => {
      disconnect();
      subscription.remove();
    };
  }, [connect, disconnect]);

  const sendMessage = (text: string, replyToId?: number) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(
        JSON.stringify({
          type: "new_message",
          text,
          parentId: replyToId,
        })
      );
    } else {
        console.warn("Cannot send: Chat not connected");
        connect(); 
    }
  };

  const editMessage = (id: number, newText: string) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(
        JSON.stringify({
          type: "edit_message",
          id,
          text: newText,
        })
      );
    }
  };

  const deleteMessage = (id: number) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(
        JSON.stringify({
          type: "delete_message",
          id,
        })
      );
    }
  };

  const toggleLike = (id: number) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(
        JSON.stringify({
          type: "toggle_like",
          id,
        })
      );
    }
  };

  return { messages, isConnected, sendMessage, editMessage, deleteMessage, toggleLike };
};