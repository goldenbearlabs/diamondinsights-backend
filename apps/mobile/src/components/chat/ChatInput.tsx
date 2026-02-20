// apps/mobile/src/components/chat/ChatInput.tsx
import { useState, useEffect } from "react";
import { StyleSheet, TextInput, TouchableOpacity, View, Text, Keyboard } from "react-native";
import { FontAwesome5 } from "@expo/vector-icons";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

interface ChatInputProps {
  onSend: (text: string, replyToId?: number) => void;
  replyingTo: ChatMessage | null;
  onCancelReply: () => void;
  placeholder?: string;
}

export const ChatInput = ({ onSend, replyingTo, onCancelReply, placeholder }: ChatInputProps) => {
  const [text, setText] = useState("");

  const handleSend = () => {
    if (!text.trim()) return;
    onSend(text, replyingTo?.id);
    setText("");
    onCancelReply();
  };

  return (
    <View style={styles.container}>
      {/* Reply Banner */}
      {replyingTo && (
        <View style={styles.replyBanner}>
          <Text style={styles.replyLabel} numberOfLines={1}>
            Replying to <Text style={{fontWeight: "bold"}}>{replyingTo.userName}</Text>
          </Text>
          <TouchableOpacity onPress={onCancelReply}>
            <FontAwesome5 name="times" size={14} color={theme.colors.muted} />
          </TouchableOpacity>
        </View>
      )}

      {/* Input Bar */}
      <View style={styles.inputRow}>
        <TextInput
          style={styles.input}
          placeholder={replyingTo ? "Write a reply..." : (placeholder || "Message #Main Chat")}
          placeholderTextColor={theme.colors.muted}
          value={text}
          onChangeText={setText}
          multiline
          maxLength={500}
        />
        <TouchableOpacity 
          style={[styles.sendBtn, !text.trim() && styles.sendBtnDisabled]} 
          onPress={handleSend}
          disabled={!text.trim()}
        >
          <FontAwesome5 name="paper-plane" size={16} color="white" />
        </TouchableOpacity>
      </View>
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    backgroundColor: theme.colors.background,
    borderTopWidth: 1,
    borderTopColor: theme.colors.border,
    paddingBottom: 8, // For safe area considerations
  },
  replyBanner: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    backgroundColor: "rgba(30, 41, 59, 0.5)", // Slate 800 with opacity
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderLeftWidth: 3,
    borderLeftColor: theme.colors.primary,
  },
  replyLabel: {
    color: theme.colors.muted,
    fontSize: 12,
    flex: 1,
    marginRight: 10,
  },
  inputRow: {
    flexDirection: "row",
    alignItems: "flex-end", 
    padding: 12,
  },
  input: {
    flex: 1,
    backgroundColor: theme.colors.inputBackground,
    color: theme.colors.text,
    borderRadius: 20,
    paddingHorizontal: 16,
    paddingVertical: 10,
    paddingRight: 40, 
    maxHeight: 100,
    fontSize: 15,
  },
  sendBtn: {
    backgroundColor: theme.colors.primary,
    width: 36,
    height: 36,
    borderRadius: 18,
    alignItems: "center",
    justifyContent: "center",
    marginLeft: 10,
    marginBottom: 2,
  },
  sendBtnDisabled: {
    backgroundColor: theme.colors.border,
    opacity: 0.5,
  },
});