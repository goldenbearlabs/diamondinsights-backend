from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, joinedload, selectinload

from shared.db.database import get_db
from shared.db.models import AdminRosterConfig, Comment, Message
from shared.queue.queue import Queue
from shared.queue.redis_connector import RedisConnector
from src.chat_websockets import manager
from src.core.admin_auth import require_admin_basic

router = APIRouter(prefix="/admin", tags=["admin"])

ROSTER_AGGREGATOR_CONFIRM_TEXT = "AGGREGATE PAST ROSTER-UPDATE"


def _bounded_limit(limit: int) -> int:
    return max(1, min(limit, 200))


class RosterSettingsUpdateBody(BaseModel):
    next_roster_update_at: datetime | None = None


class RosterUpdateAggregatorBody(BaseModel):
    confirm_text: str


@router.get("/auth/check")
def check_admin_auth(_: str = Depends(require_admin_basic)):
    return {"authenticated": True}


@router.get("/messages")
def list_messages(
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    rows = (
        db.query(Message)
        .options(joinedload(Message.user), selectinload(Message.liked_by_users))
        .order_by(Message.timestamp.desc())
        .limit(_bounded_limit(limit))
        .all()
    )

    return [
        {
            "id": row.id,
            "text": row.content,
            "user_id": row.user_id,
            "user_display_name": row.user.display_name if row.user else None,
            "user_firebase_id": row.user.firebase_id if row.user else None,
            "created_at": row.timestamp.isoformat() if row.timestamp else None,
            "edited_at": row.edited_at.isoformat() if row.edited_at else None,
            "likes_count": len(row.liked_by_users or []),
        }
        for row in rows
    ]


@router.delete("/messages/{message_id}")
async def delete_message(
    message_id: int,
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    row = db.query(Message).filter(Message.id == message_id).first()
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Message not found")

    db.delete(row)
    db.commit()

    await manager.broadcast({"type": "delete_message", "payload": {"id": message_id}})
    return {"deleted_id": message_id}


@router.get("/comments")
def list_comments(
    limit: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    rows = (
        db.query(Comment)
        .options(joinedload(Comment.user), selectinload(Comment.likes))
        .order_by(Comment.created_at.desc())
        .limit(_bounded_limit(limit))
        .all()
    )

    return [
        {
            "id": row.id,
            "content": row.content,
            "card_id": row.card_id,
            "user_id": row.user_id,
            "user_display_name": row.user.display_name if row.user else None,
            "user_firebase_id": row.user.firebase_id if row.user else None,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "edited_at": row.edited_at.isoformat() if row.edited_at else None,
            "is_deleted": bool(row.is_deleted),
            "likes_count": len(row.likes or []),
        }
        for row in rows
    ]


@router.delete("/comments/{comment_id}")
def delete_comment(
    comment_id: int,
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    row = db.get(Comment, comment_id)
    if not row:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Comment not found")

    db.delete(row)
    db.commit()
    return {"deleted_id": comment_id}


@router.get("/roster-settings")
def get_roster_settings(
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    row = db.get(AdminRosterConfig, 1)
    if not row:
        return {
            "next_roster_update_at": None,
            "updated_at": None,
        }

    return {
        "next_roster_update_at": row.next_roster_update_at.isoformat() if row.next_roster_update_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.put("/roster-settings")
def set_roster_settings(
    body: RosterSettingsUpdateBody,
    db: Session = Depends(get_db),
    _: str = Depends(require_admin_basic),
):
    next_date = body.next_roster_update_at
    if next_date is not None and next_date.tzinfo is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="next_roster_update_at must include timezone information",
        )

    row = db.get(AdminRosterConfig, 1)
    if not row:
        row = AdminRosterConfig(singleton_id=1)
        db.add(row)

    row.next_roster_update_at = next_date
    db.commit()
    db.refresh(row)

    return {
        "next_roster_update_at": row.next_roster_update_at.isoformat() if row.next_roster_update_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@router.post("/jobs/roster-update-aggregator")
def enqueue_roster_update_aggregator(
    body: RosterUpdateAggregatorBody,
    _: str = Depends(require_admin_basic),
):
    if body.confirm_text.strip() != ROSTER_AGGREGATOR_CONFIRM_TEXT:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"confirm_text must be exactly '{ROSTER_AGGREGATOR_CONFIRM_TEXT}'",
        )

    try:
        queue = Queue(redis_connector=RedisConnector())
        payload = queue.enqueue("roster-update-aggregator")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to enqueue roster-update-aggregator: {exc}",
        ) from exc

    return {
        "job_id": payload.job_id,
        "job_type": payload.job_type,
        "args": payload.args,
        "enqueued_at": payload.enqueued_at.isoformat(),
    }
