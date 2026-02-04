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
  Keyboard,
  Alert
} from "react-native";
import { StatusBar } from "expo-status-bar";
import { FontAwesome5 } from "@expo/vector-icons";

import { useChatSocket } from "../../hooks/useChatSocket";
import { ChatRow } from "../../components/chat/ChatRow";
import { ChatInput } from "../../components/chat/ChatInput";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

export default function ChatScreen() {
  const { messages, sendMessage, editMessage, deleteMessage, toggleLike, isConnected } = useChatSocket();
  
  const [replyingTo, setReplyingTo] = useState<ChatMessage | null>(null);
  const [editingMsg, setEditingMsg] = useState<ChatMessage | null>(null);
  const [editText, setEditText] = useState("");

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

  const handleDelete = () => {
    if (!editingMsg) return;
    
    Alert.alert(
      "Delete Message",
      "Are you sure you want to delete this message?",
      [
        { text: "Cancel", style: "cancel" },
        { 
          text: "Delete", 
          style: "destructive", 
          onPress: () => {
            deleteMessage(editingMsg.id);
            setEditingMsg(null);
          }
        }
      ]
    );
  };

  return (
    <View style={styles.container}>
      <StatusBar style="light" />

      <View style={{ flex: 1, paddingTop: 20 }}>
        
        {/* HEADER SECTION */}
        <View style={styles.headerContent}>
          <Text style={styles.headerTitle}>Chat Room</Text>
          <Text style={styles.headerSubtitle}>
            Share and Discuss MLB The Show Roster Update Predictions!
          </Text>
        </View>

       
        <KeyboardAvoidingView
          style={{ flex: 1 }}
          behavior={Platform.OS === "ios" ? "padding" : undefined}
          keyboardVerticalOffset={Platform.OS === "ios" ? 90 : 0} 
        >
          <FlatList
            data={messages}
            keyExtractor={(item) => item.id.toString()}
            renderItem={({ item }) => (
              <ChatRow 
                message={item} 
                onReply={setReplyingTo} 
                onLongPress={handleLongPress}
                onLike={toggleLike}
              />
            )}
            inverted 
            contentContainerStyle={styles.listContent}
            onScrollBeginDrag={Keyboard.dismiss} 
          />

          <ChatInput
            onSend={sendMessage}
            replyingTo={replyingTo}
            onCancelReply={() => setReplyingTo(null)}
          />
        </KeyboardAvoidingView>
      </View>

      {/* Edit / Delete Modal */}
      <Modal
        visible={!!editingMsg}
        transparent
        animationType="fade"
        onRequestClose={() => setEditingMsg(null)}
      >
        <KeyboardAvoidingView 
          behavior={Platform.OS === "ios" ? "padding" : "height"}
          style={styles.modalOverlay}
        >
          <View style={styles.modalCard}>
            <View style={styles.modalHeader}>
              <Text style={styles.modalTitle}>Edit Message</Text>
              <TouchableOpacity onPress={() => setEditingMsg(null)}>
                <FontAwesome5 name="times" size={16} color={theme.colors.muted} />
              </TouchableOpacity>
            </View>

            <TextInput 
              style={styles.modalInput}
              value={editText}
              onChangeText={setEditText}
              multiline
              autoFocus
            />

            <View style={styles.modalButtons}>
              <TouchableOpacity onPress={handleDelete} style={styles.deleteBtn}>
                <FontAwesome5 name="trash" size={14} color="#ef4444" />
                <Text style={styles.deleteText}>Delete</Text>
              </TouchableOpacity>

              <TouchableOpacity onPress={submitEdit} style={styles.saveBtn}>
                <Text style={styles.saveText}>Save Changes</Text>
              </TouchableOpacity>
            </View>
          </View>
        </KeyboardAvoidingView>
      </Modal>

    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: theme.colors.background,
  },
  headerContent: {
    paddingHorizontal: 16,
    marginBottom: 8,
  },
  headerTitle: {
    fontSize: 28,
    fontWeight: "800",
    color: "white",
    marginBottom: 8,
  },
  headerSubtitle: {
    color: theme.colors.muted,
    fontSize: 13,
    fontStyle: "italic",
    lineHeight: 18,
  },
  listContent: {
    paddingVertical: 16,
  },
  modalOverlay: {
    flex: 1,
    backgroundColor: "rgba(0,0,0,0.8)",
    justifyContent: "center", 
    padding: 24,
  },
  modalCard: {
    backgroundColor: theme.colors.background, 
    borderRadius: 16,
    padding: 20,
    borderWidth: 1,
    borderColor: theme.colors.border,
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.3,
    shadowRadius: 8,
    marginBottom: 100,
  },
  modalHeader: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: 16,
  },
  modalTitle: {
    color: theme.colors.text,
    fontSize: 18,
    fontWeight: "bold",
  },
  modalInput: {
    backgroundColor: theme.colors.inputBackground,
    color: theme.colors.text,
    borderRadius: 12,
    padding: 16,
    minHeight: 100,
    textAlignVertical: "top",
    marginBottom: 20,
    fontSize: 16,
  },
  modalButtons: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
  },
  deleteBtn: {
    flexDirection: "row",
    alignItems: "center",
    padding: 10,
  },
  deleteText: {
    color: "#ef4444", 
    fontWeight: "600",
    marginLeft: 8,
  },
  saveBtn: {
    backgroundColor: theme.colors.primary,
    paddingVertical: 12,
    paddingHorizontal: 20,
    borderRadius: 8,
  },
  saveText: {
    color: "white",
    fontWeight: "600",
  }
});