from __future__ import annotations

import unicodedata
from typing import Optional

from firebase_admin import auth as fb_auth
from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.database.database import get_db
from src.database.models import Users


def _normalize_search(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s


def firebase_claims(authorization: Optional[str] = Header(default=None)) -> dict:
    if not authorization:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing Authorization header")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Authorization header")

    token = parts[1].strip()
    try:
        return fb_auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired Firebase token")


def firebase_claims_optional(authorization: Optional[str] = Header(default=None)) -> dict:
    if not authorization:
        return {}

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid Authorization header")

    token = parts[1].strip()
    try:
        return fb_auth.verify_id_token(token)
    except Exception:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired Firebase token")


def _ensure_display_name_unique(db: Session, display_name: str, current_user_id: Optional[int] = None) -> None:
    norm = _normalize_search(display_name)

    existing = db.scalar(select(Users).where(Users.search_display_name == norm))
    if not existing:
        existing = db.scalar(select(Users).where(func.lower(Users.display_name) == func.lower(display_name)))

    if existing and (current_user_id is None or existing.id != current_user_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Display name already in use")


router = APIRouter(prefix="/users", tags=["users"])


class SignUpBody(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)
    profile_img_path: Optional[str] = None


class UserProfileOut(BaseModel):
    id: int
    firebase_id: Optional[str] = None
    email: Optional[str] = None
    display_name: str
    profile_img_path: str
    is_me: bool

    @staticmethod
    def from_orm_user(u: Users, *, is_me: bool) -> "UserProfileOut":
        return UserProfileOut(
            id=u.id,
            firebase_id=u.firebase_id if is_me else None,
            email=u.email if is_me else None,
            display_name=u.display_name,
            profile_img_path=u.profile_img_url,
            is_me=is_me,
        )


class UpdateUserBody(BaseModel):
    display_name: Optional[str] = Field(default=None, min_length=1, max_length=80)
    email: Optional[EmailStr] = None
    profile_img_path: Optional[str] = None


@router.post("/signup", response_model=UserProfileOut)
def signup(
    body: SignUpBody,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> UserProfileOut:
    uid = claims.get("uid")
    email = claims.get("email")
    if not uid or not email:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token missing uid/email")

    existing_email = db.scalar(select(Users).where(Users.email == email))
    if existing_email and existing_email.firebase_id != uid:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already in use")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    profile_path = body.profile_img_path or f"users/{uid}/profile.jpg"

    if user:
        changed = False
        if user.display_name != body.display_name:
            _ensure_display_name_unique(db, body.display_name, current_user_id=user.id)
            user.display_name = body.display_name
            user.search_display_name = _normalize_search(body.display_name)
            changed = True
        if user.profile_img_url != profile_path:
            user.profile_img_url = profile_path
            changed = True
        if changed:
            db.commit()
            db.refresh(user)
        return UserProfileOut.from_orm_user(user, is_me=True)

    _ensure_display_name_unique(db, body.display_name)

    user = Users(
        firebase_id=uid,
        email=email,
        display_name=body.display_name,
        search_display_name=_normalize_search(body.display_name),
        profile_img_url=profile_path,
    )
    db.add(user)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        user = db.scalar(select(Users).where(Users.firebase_id == uid))
        if not user:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")
    else:
        db.refresh(user)

    return UserProfileOut.from_orm_user(user, is_me=True)


@router.get("/me", response_model=UserProfileOut)
def me(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> UserProfileOut:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return UserProfileOut.from_orm_user(user, is_me=True)


@router.get("/{user_id}", response_model=UserProfileOut)
def get_user_profile(
    user_id: int,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
) -> UserProfileOut:
    user = db.scalar(select(Users).where(Users.id == user_id))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    is_me = claims.get("uid") == user.firebase_id
    return UserProfileOut.from_orm_user(user, is_me=is_me)


@router.put("/me", response_model=UserProfileOut)
def update_me(
    body: UpdateUserBody,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> UserProfileOut:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")

    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    current_display_name = user.display_name
    current_email = user.email
    current_profile_img = user.profile_img_url

    next_display_name = body.display_name if body.display_name is not None else current_display_name
    next_email = body.email if body.email is not None else current_email
    next_profile_img = body.profile_img_path if body.profile_img_path is not None else current_profile_img

    if next_display_name != current_display_name:
        _ensure_display_name_unique(db, next_display_name, current_user_id=user.id)

    if next_email != current_email:
        existing_email = db.scalar(select(Users).where(Users.email == next_email))
        if existing_email and existing_email.id != user.id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already in use")

    fb_updates = {}
    if next_display_name != current_display_name:
        fb_updates["display_name"] = next_display_name
    if next_email != current_email:
        fb_updates["email"] = next_email
    if next_profile_img != current_profile_img:
        fb_updates["photo_url"] = next_profile_img

    if fb_updates:
        try:
            fb_auth.update_user(uid, **fb_updates)
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to update Firebase profile",
            )

    user.display_name = next_display_name
    user.search_display_name = _normalize_search(next_display_name)
    user.email = next_email
    user.profile_img_url = next_profile_img

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        if fb_updates:
            try:
                fb_auth.update_user(
                    uid,
                    display_name=current_display_name,
                    email=current_email,
                    photo_url=current_profile_img,
                )
            except Exception:
                pass
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update user")

    db.refresh(user)
    return UserProfileOut.from_orm_user(user, is_me=True)
