
import time
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request

from shared.core.firebase_admin import init_firebase_admin
from src.api.routes import (
    cards,
    completed_orders,
    listings,
    market_candles,
    mlb_game_batting_stats,
    players,
    quirks,
    records,
    users,
    show_profiles,
    search,
    chat
)

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_firebase_admin()
    yield

app = FastAPI(title="DiamondInsights API", lifespan=lifespan)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = time.time() - start_time

    print(f"PATH: {request.url.path} | TIME: {process_time * 1000:.2f} ms")

    response.headers["X-Process-Time-Sec"] = str(process_time)

    return response

app.include_router(cards.router)
app.include_router(listings.router)
app.include_router(completed_orders.router)
app.include_router(quirks.router)
app.include_router(market_candles.router)
app.include_router(mlb_game_batting_stats.router)
app.include_router(players.router)
app.include_router(users.router)
app.include_router(records.router)
app.include_router(show_profiles.router)
app.include_router(show_profiles.public_router)
app.include_router(search.router)
app.include_router(chat.router)

@app.get("/")
def health_check():
    return {"status": "API is running", "project": "DiamondInsights"}
