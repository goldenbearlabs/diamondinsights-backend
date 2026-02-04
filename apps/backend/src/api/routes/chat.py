from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, status
from sqlalchemy.orm import Session, joinedload, selectinload
from firebase_admin import auth
from datetime import datetime
import json
import logging

from shared.db.database import get_db 
from shared.db.models import Message, Users
from src.chat_websockets import manager

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

def serialize_message(msg: Message):
    """
    Helper to format message. 
    NOTE: We send 'likedByUsers' (list of IDs) so the frontend 
    can calculate 'isLiked' and 'likeCount' itself.
    """
    return {
        "id": msg.id,
        "text": msg.content,
        "userId": msg.user_id,
        "userFirebaseId": msg.user.firebase_id,
        "userName": msg.user.display_name,
        "userImage": msg.user.profile_img_url,
        "createdAt": msg.timestamp.isoformat(),
        "editedAt": msg.edited_at.isoformat() if msg.edited_at else None,
        
        "likedByFirebaseIds": [u.firebase_id for u in msg.liked_by_users],

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
    user = await get_current_user_ws(token, db)
    if not user:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await manager.connect(websocket)
    
    try:
        history = (
            db.query(Message)
            .options(
                joinedload(Message.user), 
                joinedload(Message.parent).joinedload(Message.user),
                selectinload(Message.liked_by_users) 
            )
            .order_by(Message.timestamp.desc())
            .limit(50)
            .all()
        )
        
        for msg in reversed(history):
            await websocket.send_json({
                "type": "history_item",
                "payload": serialize_message(msg)
            })

        while True:
            data_str = await websocket.receive_text()
            data = json.loads(data_str)
            command = data.get("type")
            
            if command == "new_message":
                content = data.get("text")
                parent_id = data.get("parentId") 

                new_msg = Message(content=content, user_id=user.id, parent_id=parent_id)
                db.add(new_msg)
                db.commit()
                
                db.refresh(new_msg)
                db.query(Message).options(
                    joinedload(Message.user),
                    joinedload(Message.parent),
                    selectinload(Message.liked_by_users)
                ).filter(Message.id == new_msg.id).first()
                
                await manager.broadcast({
                    "type": "new_message",
                    "payload": serialize_message(new_msg) 
                })

            elif command == "edit_message":
                msg_id = data.get("id")
                new_text = data.get("text")
                msg = db.query(Message).filter(Message.id == msg_id).first()
                
                if msg and msg.user_id == user.id:
                    msg.content = new_text
                    msg.edited_at = datetime.utcnow()
                    db.commit()
                    
                    
                    await manager.broadcast({
                        "type": "update_message",
                        "payload": serialize_message(msg)
                    })

            elif command == "delete_message":
                msg_id = data.get("id")
                msg = db.query(Message).filter(Message.id == msg_id).first()
                if msg and msg.user_id == user.id:
                    db.delete(msg)
                    db.commit()
                    await manager.broadcast({
                        "type": "delete_message",
                        "payload": { "id": msg_id }
                    })

            elif command == "toggle_like":
                msg_id = data.get("id")
                
                msg = db.query(Message).options(selectinload(Message.liked_by_users)).filter(Message.id == msg_id).first()
                
                if msg:
                    if user in msg.liked_by_users:
                        msg.liked_by_users.remove(user) # Unlike
                    else:
                        msg.liked_by_users.append(user) # Like
                    
                    db.commit()
                    
                    
                    await manager.broadcast({
                        "type": "update_message",
                        "payload": serialize_message(msg)
                    })

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WS Error: {e}")
        manager.disconnect(websocket)