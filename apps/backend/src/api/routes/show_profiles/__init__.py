from fastapi import APIRouter

from . import analytics, cards, game_log, profile, search

router = APIRouter(tags=["show-profile"])
public_router = APIRouter(tags=["show-profile"])

router.include_router(profile.router, prefix="/users/me/show")
router.include_router(game_log.router, prefix="/users/me/show")
router.include_router(analytics.router, prefix="/users/me/show")
router.include_router(cards.router, prefix="/users/me/show")
router.include_router(search.router, prefix="/users/me/show")

public_router.include_router(search.public_router, prefix="/users")
public_router.include_router(profile.public_router, prefix="/users")
public_router.include_router(game_log.public_router, prefix="/users")
public_router.include_router(analytics.public_router, prefix="/users")
public_router.include_router(cards.public_router, prefix="/users")
