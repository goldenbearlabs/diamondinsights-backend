
import time
from fastapi import FastAPI, Request
from dotenv import load_dotenv
load_dotenv()
from backend.src.api.routes import cards, listings, completed_orders, quirks, market_candles, mlb_game_batting_stats, players





app = FastAPI(title="DiamondInsights API")

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

@app.get("/")
def health_check():
    return {"status": "API is running", "project": "DiamondInsights"}