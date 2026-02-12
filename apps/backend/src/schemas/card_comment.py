from typing import Optional, Dict, Any
from pydantic import BaseModel
from datetime import datetime

class CommentCreate(BaseModel):
    content: str
    parent_id: Optional[int] = None


class CommentUpdate(BaseModel):
    content: str


class CommentOut(BaseModel):
    id: int
    created_at: datetime
    updated_at: Optional[datetime]
    edited_at: Optional[datetime]
    content: str
    is_deleted: bool
    user_id: int
    user_firebase_id: str
    user_display_name: str
    user_profile_img: Optional[str]
    likes_count: int
    is_liked_by_me: bool = False
    parent_id: Optional[int]

    class Config:
        from_attributes = True