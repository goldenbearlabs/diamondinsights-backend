from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, status
from sqlalchemy.orm import Session, joinedload
from firebase_admin import auth
from datetime import datetime
import json
import logging

from src.database.database import get_db 
from src.database.models import Message, Users
from src.websockets import manager

router = APIRouter()
logger = logging.getLogger("uvicorn")

async def get_current_user_ws(token: str, db: Session):
    try:
        decoded_token = auth.verify_id_token(token)
        uid = decoded_token['uid']
        user = db.query(Users).filter(Users.firebase_id == uid).first()
        return user
    except Exception as e:
        logger.error(f"WS Auth Failed: {e}")
        return None

def serialize_message(msg: Message, current_user_id: int):
    """Helper to format message for the client"""
    return {
        "id": msg.id,
        "text": msg.content,
        "userId": msg.user_id,
        "userName": msg.user.display_name,
        "userImage": msg.user.profile_img_url,
        "createdAt": msg.timestamp.isoformat(),
        "editedAt": msg.edited_at.isoformat() if msg.edited_at else None,
        "isMe": msg.user_id == current_user_id,
        "replyTo": {
            "id": msg.parent.id,
            "userName": msg.parent.user.display_name,
            "text": msg.parent.content[:50] + "..." if len(msg.parent.content) > 50 else msg.parent.content
        } if msg.parent else None
    }

@router.websocket("/ws/chat")
async def websocket_endpoint(
    websocket: WebSocket, 
    token: str, 
    db: Session = Depends(get_db)
):
    # 1. Authenticate
    user = await get_current_user_ws(token, db)
    if not user:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 2. Connect
    await manager.connect(websocket)
    
    try:
        # 3. Load History (Last 50)
        history = (
            db.query(Message)
            .options(joinedload(Message.user), joinedload(Message.parent).joinedload(Message.user))
            .order_by(Message.timestamp.desc())
            .limit(50)
            .all()
        )
        
        # Send history oldest-first
        for msg in reversed(history):
            await websocket.send_json({
                "type": "history_item",
                "payload": serialize_message(msg, user.id)
            })

        # 4. Listen Loop (Expecting JSON commands now)
        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            command = data.get("type")
            
            if command == "new_message":
                # Handle New Message (and Replies)
                content = data.get("text")
                parent_id = data.get("parentId") 

                new_msg = Message(
                    content=content, 
                    user_id=user.id,
                    parent_id=parent_id
                )
                db.add(new_msg)
                db.commit()
                # Refresh to get ID and relationships
                db.refresh(new_msg)
                db.refresh(new_msg, ["user"]) # Load User relation for display name
                if parent_id:
                     db.refresh(new_msg, ["parent"]) # Load Parent for reply preview
                
                # Broadcast using the helper
                await manager.broadcast({
                    "type": "new_message",
                    "payload": serialize_message(new_msg, -1) 
                })

            elif command == "edit_message":
                msg_id = data.get("id")
                new_text = data.get("text")
                
                msg_to_edit = db.query(Message).filter(Message.id == msg_id).first()
                
                if msg_to_edit and msg_to_edit.user_id == user.id:
                    msg_to_edit.content = new_text
                    msg_to_edit.edited_at = datetime.utcnow()
                    db.commit()
                    
                    await manager.broadcast({
                        "type": "update_message",
                        "payload": {
                            "id": msg_id,
                            "text": new_text,
                            "editedAt": msg_to_edit.edited_at.isoformat()
                        }
                    })

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WS Error: {e}")
        manager.disconnect(websocket)