import { useState, useCallback, useEffect } from "react";
import { Alert } from "react-native";
import { apiGetAuth, apiPostAuth, apiDeleteAuth, apiPutAuth } from "../lib/api";
import { ChatMessage } from "../types/chat"; // We reuse ChatMessage type for consistency
import { auth } from "../lib/firebase";

// Helper interface for the actual API response structure
interface CommentOut {
  id: number;
  created_at: string; // ISO date
  updated_at: string | null;
  edited_at: string | null;
  content: string;
  is_deleted: boolean;
  user_id: number;
  user_firebase_id: string;
  user_display_name: string;
  user_profile_img: string | null;
  likes_count: number;
  is_liked_by_me: boolean;
  parent_id?: number | null;
}

export const useCardComments = (cardId: string) => {
  const [comments, setComments] = useState<ChatMessage[]>([]);
  const [loading, setLoading] = useState(false);

  // Convert API Response -> UI Model (ChatMessage)
  const mapCommentToMessage = useCallback((c: CommentOut, allComments: CommentOut[]): ChatMessage => {
    const currentUserUid = auth.currentUser?.uid || "unknown";
    
    // We construct a fake array of UIDs for likes because ChatRow logic expects it
    // If I liked it, add my UID. Fills the rest with dummies.
    const likes: string[] = [];
    if (c.is_liked_by_me) {
      likes.push(currentUserUid);
    }
    const otherLikesCount = c.likes_count - (c.is_liked_by_me ? 1 : 0);
    for (let i = 0; i < otherLikesCount; i++) {
        likes.push(`dummy_${c.id}_${i}`);
    }

    // Resolve parent info for the UI reply preview
    let replyTo = undefined;
    if (c.parent_id) {
        const parent = allComments.find(p => p.id === c.parent_id);
        if (parent) {
            replyTo = {
                id: parent.id,
                userName: parent.user_display_name,
                text: parent.content
            };
        }
    }

    return {
      id: c.id,
      text: c.content,
      userId: c.user_id,
      userName: c.user_display_name,
      userFirebaseId: c.user_firebase_id,
      userImage: c.user_profile_img ?? undefined,
      createdAt: c.created_at,
      editedAt: c.edited_at,
      likedByFirebaseIds: likes,
      replyTo: replyTo,
      isMe: c.user_firebase_id === currentUserUid,
    };
  }, []);

  const fetchComments = useCallback(async () => {
    try {
      setLoading(true);
      const data = await apiGetAuth<CommentOut[]>(`/comments/card/${cardId}`);
      const mapped = data.map(c => mapCommentToMessage(c, data));
      setComments(mapped.reverse());
    } catch (err) {
      console.error("Failed to fetch comments", err);
    } finally {
      setLoading(false);
    }
  }, [cardId, mapCommentToMessage]);

  const postComment = async (text: string, replyToId?: number) => {
    try {
      await apiPostAuth(`/comments/card/${cardId}`, { content: text, parent_id: replyToId });
      // Refresh to get the real ID and server timestamp
      await fetchComments(); 
    } catch (err) {
      console.error(err);
      Alert.alert("Error", "Failed to post comment");
    }
  };

  const editComment = async (commentId: number, text: string) => {
    try {
      await apiPutAuth(`/comments/${commentId}`, { content: text });
      await fetchComments();
    } catch (err) {
      console.error(err);
      Alert.alert("Error", "Failed to edit comment");
    }
  };

  const deleteComment = async (commentId: number) => {
    try {
      await apiDeleteAuth(`/comments/${commentId}`);
      await fetchComments();
    } catch (err) {
      console.error(err);
      Alert.alert("Error", "Failed to delete comment");
    }
  };

  const toggleLike = async (commentId: number) => {
    const comment = comments.find(c => c.id === commentId);
    if (!comment) return;

    const currentUserUid = auth.currentUser?.uid;
    if (!currentUserUid) return;

    const isLiked = comment.likedByFirebaseIds.includes(currentUserUid);

    // Optimistic Update
    setComments(prev => prev.map(c => {
        if (c.id !== commentId) return c;
        const newLikes = isLiked 
            ? c.likedByFirebaseIds.filter(uid => uid !== currentUserUid)
            : [...c.likedByFirebaseIds, currentUserUid];
        return { ...c, likedByFirebaseIds: newLikes };
    }));

    try {
        if (isLiked) {
            await apiDeleteAuth(`/comments/${commentId}/like`);
        } else {
            await apiPostAuth(`/comments/${commentId}/like`, {});
        }
    } catch (err) {
        console.error("Failed to toggle like", err);
        fetchComments(); // Revert on error
    }
  };

  useEffect(() => {
    if (cardId) fetchComments();
  }, [cardId, fetchComments]);

  return {
    comments,
    loading,
    refreshComments: fetchComments,
    postComment,
    toggleLike,
    editComment,
    deleteComment
  };
};