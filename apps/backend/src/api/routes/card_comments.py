from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, status
from firebase_admin import auth as fb_auth
from pydantic import BaseModel
from sqlalchemy import desc, func, select, exists
from sqlalchemy.orm import Session, selectinload

from shared.db.database import get_db
from shared.db.models import Card, Comment, CommentLike, Users
from src.api.routes.users import firebase_claims
from src.schemas.card_comment import CommentCreate, CommentOut
router = APIRouter(prefix="/comments", tags=["comments"])




def get_current_user(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims)
) -> Users:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


def get_optional_user(
    db: Session = Depends(get_db),
    authorization: Optional[str] = Header(default=None)
) -> Optional[Users]:
    if not authorization:
        return None

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        return None

    try:
        decoded_token = fb_auth.verify_id_token(parts[1])
        uid = decoded_token.get("uid")
        if not uid:
            return None
        return db.scalar(select(Users).where(Users.firebase_id == uid))
    except (ValueError, Exception): 
        return None


# --- Endpoints ---

@router.get("/card/{card_id}", response_model=List[CommentOut])
def get_card_comments(
    card_id: str,
    db: Session = Depends(get_db),
    current_user: Optional[Users] = Depends(get_optional_user)
):
    query = (
        select(Comment)
        .options(selectinload(Comment.user), selectinload(Comment.likes))
        .where(Comment.card_id == card_id)
        .order_by(desc(Comment.created_at))
    )
    comments = db.scalars(query).all()

    results = []
    for c in comments:
        likes_count = len(c.likes)
        is_liked = False
        if current_user:
            is_liked = any(like.user_id == current_user.id for like in c.likes)

        # Handle deleted content
        content_scan = "[deleted]" if c.is_deleted else c.content
        
        results.append(CommentOut(
            id=c.id,
            created_at=c.created_at,
            updated_at=c.updated_at,
            content=content_scan,
            is_deleted=c.is_deleted,
            user_id=c.user_id,
            user_display_name=c.user.display_name,
            user_profile_img=c.user.profile_img_url,
            likes_count=likes_count,
            is_liked_by_me=is_liked,
            parent_id=c.parent_id
        ))
    
    return results


@router.post("/card/{card_id}", response_model=CommentOut)
def create_comment(
    card_id: str,
    body: CommentCreate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user)
):
    # Verify card exists
    card_exists = db.scalar(select(exists().where(Card.id == card_id)))
    if not card_exists:
        raise HTTPException(status_code=404, detail="Card not found")

    # Verify parent if provided
    if body.parent_id:
        parent = db.get(Comment, body.parent_id)
        if not parent:
            raise HTTPException(status_code=404, detail="Parent comment not found")
        if parent.card_id != card_id:
            raise HTTPException(status_code=400, detail="Parent comment belongs to a different card")

    new_comment = Comment(
        content=body.content,
        user_id=user.id,
        card_id=card_id,
        parent_id=body.parent_id
    )
    db.add(new_comment)
    db.commit()
    db.refresh(new_comment)

    # Re-fetch to populate relationships for schema
    new_comment = db.scalar(
        select(Comment)
        .options(selectinload(Comment.user), selectinload(Comment.likes))
        .where(Comment.id == new_comment.id)
    )

    return CommentOut(
        id=new_comment.id,
        created_at=new_comment.created_at,
        updated_at=new_comment.updated_at,
        content=new_comment.content,
        is_deleted=new_comment.is_deleted,
        user_id=user.id,
        user_display_name=user.display_name,
        user_profile_img=user.profile_img_url,
        likes_count=0,
        is_liked_by_me=False,
        parent_id=new_comment.parent_id
    )


@router.delete("/{comment_id}")
def delete_comment(
    comment_id: int,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user)
):
    comment = db.get(Comment, comment_id)
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")

    if comment.user_id != user.id:
        raise HTTPException(status_code=403, detail="Not authorized to delete this comment")

    # Soft delete
    comment.is_deleted = True
    comment.content = "[deleted]"
    
    db.commit()
    return {"message": "Comment deleted"}


@router.post("/{comment_id}/like")
def like_comment(
    comment_id: int,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user)
):
    comment = db.get(Comment, comment_id)
    if not comment:
        raise HTTPException(status_code=404, detail="Comment not found")

    existing_like = db.scalar(
        select(CommentLike).where(
            CommentLike.user_id == user.id,
            CommentLike.comment_id == comment_id
        )
    )
    
    if existing_like:
        return {"message": "Already liked"}

    new_like = CommentLike(user_id=user.id, comment_id=comment_id)
    db.add(new_like)
    db.commit()
    return {"message": "Liked"}


@router.delete("/{comment_id}/like")
def unlike_comment(
    comment_id: int,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user)
):
    existing_like = db.scalar(
        select(CommentLike).where(
            CommentLike.user_id == user.id,
            CommentLike.comment_id == comment_id
        )
    )
    
    if not existing_like:
        return {"message": "Not liked"}

    db.delete(existing_like)
    db.commit()
    return {"message": "Unliked"}