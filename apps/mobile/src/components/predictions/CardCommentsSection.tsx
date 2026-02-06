import React, { useState, useRef } from "react";
import { View, Text, StyleSheet, LayoutAnimation, TouchableOpacity, ScrollView, Platform, UIManager } from "react-native";
import { FontAwesome5, Ionicons } from "@expo/vector-icons";
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
  const { comments, postComment, toggleLike } = useCardComments(cardId);
  const [expanded, setExpanded] = useState(false);
  const [replyingTo, setReplyingTo] = useState<ChatMessage | null>(null);
  const scrollViewRef = useRef<ScrollView>(null);

  const toggleExpanded = () => {
    LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut);
    setExpanded(!expanded);
  };

  const handleSend = (text: string, replyToId?: number) => {
      postComment(text, replyToId);
      setReplyingTo(null);
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
                            onLongPress={() => {}} 
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
    </View>
  );
};

const styles = StyleSheet.create({
  container: {
    backgroundColor: 'rgba(2, 6, 23, 0.7)', // Matches glassCard
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
  }
});