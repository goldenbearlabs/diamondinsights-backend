// apps/mobile/src/components/chat/ChatRow.tsx
import React, { useEffect, useState } from "react";
import { Image, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import { FontAwesome5, Ionicons } from "@expo/vector-icons"; 
import { getDownloadURL, ref } from "firebase/storage";
import { storage, auth } from "../../lib/firebase"; 

import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

const DEFAULT_AVATAR = require("../../../assets/images/default_profile.png");

interface ChatRowProps {
  message: ChatMessage;
  onReply: (msg: ChatMessage) => void;
  onLongPress: (msg: ChatMessage) => void;
  onLike: (id: number) => void; }

export const ChatRow = ({ message, onReply, onLongPress, onLike }: ChatRowProps) => {
  const [avatarUri, setAvatarUri] = useState<string | null>(null);

  const currentUserUid = auth.currentUser?.uid;
  
  const likeCount = message.likedByFirebaseIds?.length || 0;
  
  const isLikedByMe = currentUserUid 
    ? message.likedByFirebaseIds?.includes(currentUserUid)
    : false;
  // ------------------

  const timeString = new Date(message.createdAt).toLocaleTimeString([], { 
    hour: 'numeric', minute: '2-digit' 
  });

  useEffect(() => {
    let active = true;

    const fetchImage = async () => {
      if (!message.userImage) {
        setAvatarUri(null);
        return;
      }

      if (message.userImage.startsWith("http")) {
        setAvatarUri(message.userImage);
        return;
      }

      try {
        const url = await getDownloadURL(ref(storage, message.userImage));
        if (active) {
          setAvatarUri(url);
        }
      } catch (err) {
        if (active) setAvatarUri(null);
      }
    };

    fetchImage();

    return () => {
      active = false;
    };
  }, [message.userImage]);

  return (
    <TouchableOpacity 
      style={styles.container} 
      onLongPress={() => onLongPress(message)}
      delayLongPress={300}
      activeOpacity={0.7}
    >
      {/* Avatar Image */}
      <Image
        source={avatarUri ? { uri: avatarUri } : DEFAULT_AVATAR}
        style={styles.avatar}
      />

      <View style={styles.content}>
        {/* Header: Name + Time + Icons */}
        <View style={styles.header}>
          <View style={styles.headerLeft}>
            <Text style={styles.username}>{message.userName}</Text>
            <Text style={styles.timestamp}>{timeString}</Text>
            {message.editedAt && <Text style={styles.edited}>(edited)</Text>}
          </View>
          
          <View style={styles.actions}>
            {/* LIKE BUTTON */}
            <TouchableOpacity 
              onPress={() => onLike(message.id)} 
              style={styles.actionBtn}
              hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}
            >
              <Ionicons 
                // Show filled heart if I liked it, outline if not
                name={isLikedByMe ? "heart" : "heart-outline"} 
                size={16} 
                // Red if I liked it, muted gray otherwise
                color={isLikedByMe ? "#ef4444" : theme.colors.muted} 
              />
              {likeCount > 0 && <Text style={styles.likeCount}>{likeCount}</Text>}
            </TouchableOpacity>

            {/* REPLY BUTTON */}
            <TouchableOpacity 
              onPress={() => onReply(message)} 
              style={styles.actionBtn}
              hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}
            >
              <FontAwesome5 name="reply" size={14} color={theme.colors.muted} />
            </TouchableOpacity>
          </View>
        </View>

        {/* Reply Context */}
        {message.replyTo && (
          <View style={styles.replyContext}>
            <View style={styles.replyLine} />
            <Text style={styles.replyText} numberOfLines={1}>
              Replying to <Text style={{fontWeight: '700'}}>{message.replyTo.userName}</Text>: {message.replyTo.text}
            </Text>
          </View>
        )}

        {/* The Message Text */}
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
    width: 40,
    height: 40,
    borderRadius: 20,
    marginRight: 12,
    backgroundColor: theme.colors.border,
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