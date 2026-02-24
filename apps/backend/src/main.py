import time
from contextlib import asynccontextmanager
import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from shared.core.firebase_admin import init_firebase_admin
from src.api.main import api_router
from src.core.cache import close_cache_client, init_cache_client

logger = logging.getLogger(__name__)
load_dotenv()

DEFAULT_CORS_ALLOW_ORIGINS = (
    "https://diamondinsights.app",
    "https://www.diamondinsights.app",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
)


def get_cors_allow_origins() -> list[str]:
    raw_origins = (os.getenv("CORS_ALLOW_ORIGINS") or "").strip()
    if not raw_origins:
        return list(DEFAULT_CORS_ALLOW_ORIGINS)

    origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    return origins or list(DEFAULT_CORS_ALLOW_ORIGINS)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Performs startup and cleanup operations for the FastAPI app instance
    Operations before the yield are run before the app starts.
    Operations after the yield are run after the app shuts down.
    Exceptions not handled inside the context will re-raise at the yield

    Args:
        app: FastAPI instance this context serves
    """
    init_firebase_admin()
    app.state.cache = init_cache_client()
    try:
        yield
    finally:
        close_cache_client(app.state.cache)

app = FastAPI(title="DiamondInsights API", 
              version="1.0.0",
              summary="API serves the web & mobile for Diamond Insights and lives on a Digital Ocean server.",
              lifespan=lifespan)

cors_allow_origins = get_cors_allow_origins()
logger.info("CORS enabled for origins: %s", cors_allow_origins)
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allow_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time.time()

    response = await call_next(request)

    process_time = time.time() - start_time

    print(f"PATH: {request.url.path} | TIME: {process_time * 1000:.2f} ms")

    response.headers["X-Process-Time-Sec"] = str(process_time)

    return response

app.include_router(api_router)

@app.get("/")
def health_check():
    return {"status": "API is running", "project": "DiamondInsights"}
