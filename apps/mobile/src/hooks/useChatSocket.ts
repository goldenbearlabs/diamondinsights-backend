import { useEffect, useRef, useState } from "react";
import { auth } from "../lib/firebase"; 
import { ChatMessage, WebSocketMessage } from "../types/chat";

const getWsUrl = () => {
  const apiUrl = process.env.EXPO_PUBLIC_API_BASE_URL || "http://192.168.1.127:8000"; // Fallback
  return apiUrl.replace(/^http/, "ws") + "/ws/chat";
};

export const useChatSocket = () => {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const ws = useRef<WebSocket | null>(null);

  useEffect(() => {
    let active = true;

    const connect = async () => {
      // 1. Get Token
      const user = auth.currentUser;
      if (!user) return;
      const token = await user.getIdToken();

      // 2. Open Connection
      const url = `${getWsUrl()}?token=${token}`;
      const socket = new WebSocket(url);
      ws.current = socket;

      socket.onopen = () => {
        if (active) setIsConnected(true);
        console.log("Chat Connected");
      };

      socket.onmessage = (e) => {
        if (!active) return;
        
        try {
          const data = JSON.parse(e.data) as WebSocketMessage;
          
          if (data.type === "history_item") {
            // Append to end (list is inverted, so 'end' is top of history)
            setMessages((prev) => [...prev, data.payload]);
          } 
          else if (data.type === "new_message") {
            // Add to start (bottom of screen)
            setMessages((prev) => [data.payload, ...prev]);
          } 
          else if (data.type === "update_message") {
            // Find and swap text
            setMessages((prev) =>
              prev.map((msg) =>
                msg.id === data.payload.id ? { ...msg, ...data.payload } : msg
              )
            );
          }
        } catch (err) {
          console.error("Chat Parse Error:", err);
        }
      };

      socket.onclose = () => {
        if (active) setIsConnected(false);
        console.log("Chat Disconnected");
      };
    };

    connect();

    return () => {
      active = false;
      ws.current?.close();
    };
  }, []);

  const sendMessage = (text: string, replyToId?: number) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(
        JSON.stringify({
          type: "new_message",
          text,
          parentId: replyToId,
        })
      );
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

  return { messages, isConnected, sendMessage, editMessage };
};