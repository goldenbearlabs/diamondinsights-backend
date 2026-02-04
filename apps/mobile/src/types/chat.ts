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
    userFirebaseId?: string;
    userName: string;
    userImage?: string;
    createdAt: string;
    editedAt?: string | null;
    isMe?: boolean;
    
    likedByFirebaseIds: string[];  
    replyTo?: {
      id: number;
      userName: string;
      text: string;
    };
  }
  
  export interface WebSocketMessage {
    type: "new_message" | "history_item" | "update_message" | "delete_message";
    payload: any;
  }