// apps/mobile/src/types/chat.ts

export interface ChatUser {
    id: number;
    userName: string;
    userImage?: string;
  }
  
  export interface ReplyPreview {
    id: number;
    userName: string;
    text: string;
  }
  
  export interface ChatMessage {
    id: number;
    text: string;
    userId: number;
    userName: string;
    userImage?: string;
    createdAt: string; 
    editedAt?: string | null;
    isMe: boolean;
    replyTo?: ReplyPreview | null;
  }
  
  export interface WebSocketMessage {
    type: "history_item" | "new_message" | "update_message";
    payload: ChatMessage;
  }