import React, { useState, useRef } from "react";
import {
  View,
  Text,
  StyleSheet,
  LayoutAnimation,
  TouchableOpacity,
  ScrollView,
  Platform,
  UIManager,
  Modal,
  TextInput,
  KeyboardAvoidingView,
  Alert
} from "react-native";
import { FontAwesome5 } from "@expo/vector-icons";
import { useCardComments } from "../../hooks/useCardComments";
import { ChatInput } from "../chat/ChatInput";
import { ChatRow } from "../chat/ChatRow";
import { theme } from "../../theme/colors";
import { ChatMessage } from "../../types/chat";

if (Platform.OS === 'android' && UIManager.setLayoutAnimationEnabledExperimental) {
  UIManager.setLayoutAnimationEnabledExperimental(true);
}

interface CardCommentsSectionProps {
  cardId: string;
}

export const CardCommentsSection = ({ cardId }: CardCommentsSectionProps) => {
  const { comments, postComment, toggleLike, editComment, deleteComment } = useCardComments(cardId);
  const [expanded, setExpanded] = useState(false);
  const [replyingTo, setReplyingTo] = useState<ChatMessage | null>(null);
  const [editingMsg, setEditingMsg] = useState<ChatMessage | null>(null);
  const [editText, setEditText] = useState("");
  const scrollViewRef = useRef<ScrollView>(null);

  const toggleExpanded = () => {
    LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
    setExpanded(!expanded);
  };

  const handleSend = (text: string, replyToId?: number) => {
      postComment(text, replyToId);
      setReplyingTo(null);
  };

  const handleLongPress = (msg: ChatMessage) => {
    if (msg.isMe) {
      setEditText(msg.text);
      setEditingMsg(msg);
    }
  };

  const submitEdit = () => {
    if (editingMsg && editText.trim()) {
      editComment(editingMsg.id, editText.trim());
      setEditingMsg(null);
    }
  };

  const handleDelete = () => {
    if (!editingMsg) return;

    Alert.alert(
      "Delete Comment",
      "Are you sure you want to delete this comment?",
      [
        { text: "Cancel", style: "cancel" },
        {
          text: "Delete",
          style: "destructive",
          onPress: () => {
            deleteComment(editingMsg.id);
            setEditingMsg(null);
          }
        }
      ]
    );
  };

  return (
    <View style={styles.container}>
      <TouchableOpacity 
        onPress={toggleExpanded} 
        style={styles.header} 
        activeOpacity={0.7}
      >
        <Text style={styles.title}>Comments ({comments.length})</Text>
        <FontAwesome5 
            name={expanded ? "chevron-up" : "chevron-down"} 
            size={16} 
            color="white" 
        />
      </TouchableOpacity>

      {expanded && (
        <View style={styles.content}>
            {comments.length === 0 ? (
                <View style={styles.emptyContainer}>
                    <Text style={styles.emptyText}>No comments yet.</Text>
                    <Text style={styles.emptySubText}>Start the conversation!</Text>
                </View>
            ) : (
                <ScrollView 
                    ref={scrollViewRef}
                    style={styles.scrollList}
                    contentContainerStyle={styles.listContent}
                    nestedScrollEnabled={true}
                    onContentSizeChange={() => scrollViewRef.current?.scrollToEnd({ animated: true })}
                >
                    
                    {comments.map((item) => (
                        <ChatRow 
                            key={item.id}
                            message={item}
                            onReply={setReplyingTo}
                            onLike={(id) => toggleLike(id)}
                        onLongPress={handleLongPress}
                        />
                    ))}
                </ScrollView>
            )}
            
            <ChatInput 
                onSend={handleSend}
                replyingTo={replyingTo}
                onCancelReply={() => setReplyingTo(null)}
                placeholder="Add a comment..."
            />
        </View>
      )}

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
              <Text style={styles.modalTitle}>Edit Comment</Text>
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
};

const styles = StyleSheet.create({
  container: {
    backgroundColor: 'rgba(2, 6, 23, 0.7)', 
    marginTop: 16,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: 'rgba(255, 255, 255, 0.1)',
    overflow: "hidden",
  },
  header: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    padding: 16,
  },
  title: {
    fontSize: 18,
    fontWeight: "bold",
    color: 'white',
  },
  content: {
    borderTopWidth: 1,
    borderTopColor: 'rgba(255, 255, 255, 0.1)',
  },
  scrollList: {
    maxHeight: 400,
  },
  listContent: {
    paddingVertical: 10,
  },
  emptyContainer: {
    padding: 24,
    alignItems: "center",
  },
  emptyText: {
    color: 'white',
    fontSize: 16,
    fontWeight: "600",
    marginBottom: 4,
  },
  emptySubText: {
    color: theme.colors.muted,
    fontSize: 14,
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