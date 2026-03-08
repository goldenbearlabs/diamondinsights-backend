// apps/mobile/src/components/chat/ChatRow.tsx
import React from "react";
import { StyleSheet, Text, TouchableOpacity, View } from "react-native";
import { FontAwesome5, Ionicons } from "@expo/vector-icons";
import {useRouter} from "expo-router";
import { auth } from "../../lib/firebase"; 

import { Avatar } from "../Avatar";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

interface ChatRowProps {
  message: ChatMessage;
  onReply: (msg: ChatMessage) => void;
  onLongPress: (msg: ChatMessage) => void;
  onLike: (id: number) => void; }

export const ChatRow = ({ message, onReply, onLongPress, onLike }: ChatRowProps) => {
  const currentUserUid = auth.currentUser?.uid;
  const router = useRouter();
  const handleProfilePress = () => {
    
    if (message.userId) {
      router.push({
        pathname: "/(app)/account",
        params: { userId: message.userId }
      });
    }
  };
  const likeCount = message.likedByFirebaseIds?.length || 0;
  
  const isLikedByMe = currentUserUid 
    ? message.likedByFirebaseIds?.includes(currentUserUid)
    : false;
  

  const timeString = new Date(message.createdAt).toLocaleString([], {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

  return (
    <TouchableOpacity 
      style={styles.container} 
      onLongPress={() => onLongPress(message)}
      delayLongPress={300}
      activeOpacity={0.7}
    >
      <View style={styles.avatar}>
        <TouchableOpacity onPress={handleProfilePress} activeOpacity={0.7}>
          <Avatar firebasePath={message.userImage} size={40} />
        </TouchableOpacity>
      </View>

      <View style={styles.content}>
        <View style={styles.header}>
          <View style={styles.headerLeft}>
            <TouchableOpacity onPress={handleProfilePress} activeOpacity={0.7}>
              <Text style={styles.username}>{message.userName}</Text>
            </TouchableOpacity>
            <Text style={styles.timestamp}>{timeString}</Text>
            {message.editedAt && <Text style={styles.edited}>(edited)</Text>}
          </View>
          
          <View style={styles.actions}>
            <TouchableOpacity 
              onPress={() => onLike(message.id)} 
              style={styles.actionBtn}
              hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}
            >
              <Ionicons 
                name={isLikedByMe ? "heart" : "heart-outline"} 
                size={16} 
                color={isLikedByMe ? "#ef4444" : theme.colors.muted} 
              />
              {likeCount > 0 && <Text style={styles.likeCount}>{likeCount}</Text>}
            </TouchableOpacity>

            <TouchableOpacity 
              onPress={() => onReply(message)} 
              style={styles.actionBtn}
              hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}
            >
              <FontAwesome5 name="reply" size={14} color={theme.colors.muted} />
            </TouchableOpacity>
          </View>
        </View>

        {message.replyTo && (
          <View style={styles.replyContext}>
            <View style={styles.replyLine} />
            <Text style={styles.replyText} numberOfLines={1}>
              Replying to <Text style={{fontWeight: '700'}}>{message.replyTo.userName}</Text>: {message.replyTo.text}
            </Text>
          </View>
        )}

        <Text style={styles.text}>{message.text}</Text>
      </View>
    </TouchableOpacity>
  );
};

const styles = StyleSheet.create({
  container: {
    flexDirection: "row",
    paddingHorizontal: 16,
    paddingVertical: 10,
  },
  avatar: {
    marginRight: 12,
  },
  content: {
    flex: 1,
    justifyContent: "center",
  },
  header: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 4,
    justifyContent: "space-between", 
  },
  headerLeft: {
    flexDirection: "row",
    alignItems: "center",
    flex: 1,
    flexWrap: 'wrap', 
  },
  username: {
    color: theme.colors.text,
    fontWeight: "700",
    fontSize: 15,
    marginRight: 8,
  },
  timestamp: {
    color: theme.colors.muted,
    fontSize: 11,
    marginRight: 8,
  },
  edited: {
    color: theme.colors.muted,
    fontSize: 10,
    fontStyle: "italic",
    marginRight: 8,
  },
  actions: {
    flexDirection: "row",
    alignItems: "center",
    gap: 16,
  },
  actionBtn: {
    flexDirection: "row",
    alignItems: "center",
    gap: 4,
  },
  likeCount: {
    color: "#ef4444",
    fontSize: 12,
    fontWeight: "600",
  },
  replyContext: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 6,
    opacity: 0.8,
  },
  replyLine: {
    width: 2,
    height: 12,
    backgroundColor: theme.colors.muted,
    marginRight: 6,
    borderRadius: 1,
  },
  replyText: {
    color: theme.colors.muted,
    fontSize: 11,
    flex: 1,
  },
  text: {
    color: "#e2e8f0",
    fontSize: 15,
    lineHeight: 20,
  },
});