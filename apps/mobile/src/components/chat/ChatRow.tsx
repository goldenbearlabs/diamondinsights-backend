// apps/mobile/src/components/chat/ChatRow.tsx
import { Image, StyleSheet, Text, TouchableOpacity, View } from "react-native";
import { FontAwesome5 } from "@expo/vector-icons";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

const DEFAULT_AVATAR = require("../../../assets/images/default_profile.png");

interface ChatRowProps {
  message: ChatMessage;
  onReply: (msg: ChatMessage) => void;
  onLongPress: (msg: ChatMessage) => void;
}

export const ChatRow = ({ message, onReply, onLongPress }: ChatRowProps) => {
  const timeString = new Date(message.createdAt).toLocaleTimeString([], { 
    hour: 'numeric', minute: '2-digit' 
  });

  return (
    <TouchableOpacity 
      style={styles.container} 
      onLongPress={() => onLongPress(message)}
      delayLongPress={300}
      activeOpacity={0.7}
    >
      {/* Avatar */}
      <Image
        source={message.userImage ? { uri: message.userImage } : DEFAULT_AVATAR}
        style={styles.avatar}
      />

      <View style={styles.content}>
        {/* Header: Name + Time + Icons */}
        <View style={styles.header}>
          <Text style={styles.username}>{message.userName}</Text>
          <Text style={styles.timestamp}>{timeString}</Text>
          {message.editedAt && <Text style={styles.edited}>(edited)</Text>}
          
          {/* Visible Reply Button */}
          <TouchableOpacity onPress={() => onReply(message)} style={styles.replyBtn}>
            <FontAwesome5 name="reply" size={12} color={theme.colors.muted} />
          </TouchableOpacity>
        </View>

        {/* Reply Context (If this message IS a reply) */}
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
    // marginVertical: 2, // Optional: Spacing between rows
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
  replyBtn: {
    padding: 4,
  },
  replyContext: {
    flexDirection: "row",
    alignItems: "center",
    marginBottom: 4,
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
    color: "#e2e8f0", // Slightly off-white for reading comfort
    fontSize: 15,
    lineHeight: 20,
  },
});