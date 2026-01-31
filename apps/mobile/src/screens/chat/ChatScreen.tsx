// apps/mobile/src/screens/chat/ChatScreen.tsx
import React, { useState } from "react";
import {
  FlatList,
  KeyboardAvoidingView,
  Platform,
  StyleSheet,
  View,
  Modal,
  TextInput,
  TouchableOpacity,
  Text,
  Keyboard
} from "react-native";
import { StatusBar } from "expo-status-bar";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { useChatSocket } from "../../hooks/useChatSocket";
import { ChatRow } from "../../components/chat/ChatRow";
import { ChatInput } from "../../components/chat/ChatInput";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

const ChatScreen = () => {
  const insets = useSafeAreaInsets();
  const { messages, sendMessage, editMessage, isConnected } = useChatSocket();
  
  // State for Reply
  const [replyingTo, setReplyingTo] = useState<ChatMessage | null>(null);

  // State for Editing
  const [editingMsg, setEditingMsg] = useState<ChatMessage | null>(null);
  const [editText, setEditText] = useState("");

  // Calculate offset for KeyboardAvoidingView
  // iOS needs to account for the top inset (notch) + header height (approx 44)
  const headerHeight = insets.top + 44; 

  const handleLongPress = (msg: ChatMessage) => {
    if (msg.isMe) {
      setEditText(msg.text);
      setEditingMsg(msg);
    }
  };

  const submitEdit = () => {
    if (editingMsg && editText.trim()) {
      editMessage(editingMsg.id, editText);
      setEditingMsg(null);
    }
  };

  return (
    <View style={styles.container}>
      <StatusBar style="light" />
      
      {/* Spacer for Top Safe Area */}
      <View style={{ height: insets.top, backgroundColor: theme.colors.background }} />

      {/* Connection Warning (Optional - helps debug "Disconnected" state) */}
      {!isConnected && (
        <View style={styles.connectionWarning}>
          <Text style={styles.warningText}>Connecting to chat...</Text>
        </View>
      )}

      <KeyboardAvoidingView
        style={{ flex: 1 }}
        behavior={Platform.OS === "ios" ? "padding" : undefined}
        keyboardVerticalOffset={Platform.OS === "ios" ? headerHeight : 0}
      >
        <FlatList
          data={messages}
          keyExtractor={(item) => item.id.toString()}
          renderItem={({ item }) => (
            <ChatRow 
              message={item} 
              onReply={setReplyingTo} 
              onLongPress={handleLongPress}
            />
          )}
          inverted // Critical: Makes list scroll from bottom up
          contentContainerStyle={styles.listContent}
          // Dismiss keyboard when dragging the list
          onScrollBeginDrag={Keyboard.dismiss} 
        />

        <ChatInput
          onSend={sendMessage}
          replyingTo={replyingTo}
          onCancelReply={() => setReplyingTo(null)}
        />
      </KeyboardAvoidingView>

      {/* Simple Edit Modal */}
      <Modal
        visible={!!editingMsg}
        transparent
        animationType="fade"
        onRequestClose={() => setEditingMsg(null)}
      >
        <View style={styles.modalOverlay}>
          <View style={styles.modalCard}>
            <Text style={styles.modalTitle}>Edit Message</Text>
            <TextInput 
              style={styles.modalInput}
              value={editText}
              onChangeText={setEditText}
              multiline
              autoFocus
            />
            <View style={styles.modalButtons}>
              <TouchableOpacity onPress={() => setEditingMsg(null)} style={styles.cancelBtn}>
                <Text style={styles.btnText}>Cancel</Text>
              </TouchableOpacity>
              <TouchableOpacity onPress={submitEdit} style={styles.saveBtn}>
                <Text style={styles.btnText}>Save</Text>
              </TouchableOpacity>
            </View>
          </View>
        </View>
      </Modal>

    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  connectionWarning: {
    backgroundColor: theme.colors.error,
    paddingVertical: 4,
    alignItems: "center",
  },
  warningText: {
    color: "white",
    fontSize: 12,
    fontWeight: "600",
  },
  listContent: {
    paddingVertical: 16,
  },
  // Edit Modal Styles
  modalOverlay: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.7)",
    justifyContent: "center",
    padding: 24,
  },
  modalCard: {
    backgroundColor: theme.colors.background,
    borderRadius: 16,
    padding: 20,
    borderWidth: 1,
    borderColor: theme.colors.border,
  },
  modalTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "bold",
    marginBottom: 12,
  },
  modalInput: {
    backgroundColor: theme.colors.inputBackground,
    color: theme.colors.text,
    borderRadius: 8,
    padding: 12,
    minHeight: 80,
    textAlignVertical: "top",
    marginBottom: 16,
  },
  modalButtons: {
    flexDirection: "row",
    justifyContent: "flex-end",
    gap: 12,
  },
  cancelBtn: {
    paddingVertical: 8,
    paddingHorizontal: 16,
  },
  saveBtn: {
    backgroundColor: theme.colors.primary,
    paddingVertical: 8,
    paddingHorizontal: 16,
    borderRadius: 8,
  },
  btnText: {
    color: "white",
    fontWeight: "600",
  }
});

export default ChatScreen;