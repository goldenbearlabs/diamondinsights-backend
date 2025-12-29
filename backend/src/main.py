
from fastapi import FastAPI
from dotenv import load_dotenv
load_dotenv()
from backend.src.api.routes import cards, listings, completed_orders, quirks, market_candles




app = FastAPI(title="DiamondInsights API")

app.include_router(cards.router)
app.include_router(listings.router)
app.include_router(completed_orders.router)
app.include_router(quirks.router)
app.include_router(market_candles.router)

@app.get("/")
def health_check():
    return {"status": "API is running", "project": "DiamondInsights"}