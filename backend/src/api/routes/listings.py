from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from backend.src.database.database import get_db
from backend.src.database.models import Listing, Card
from backend.src.schemas.listing import ListingResponse

router = APIRouter(prefix="/listings", tags=["listings"])



@router.get("/", response_model=List[ListingResponse])
def get_listings(
    sort_by: str = Query("buy"),
    series: Optional[str] = Query(None),
    desc: bool = Query(True),
    limit: int = Query(50, le=100),
    offset: int=0,
    db: Session = Depends(get_db)

):
    "gets all the listings sorted by best_buy_price desc by default"

    query = db.query(Listing, Card).join(Card, Listing.card_id == Card.id)

    # can choose which column to sort by
    if sort_by == "sell":
        sort_column = Listing.best_sell_price
    else:
        sort_column = Listing.best_buy_price

    if desc:
        query = query.order_by(sort_column.desc())
    else:
        query = query.order_by(sort_column.asc())

    if series:
        query = query.filter(Card.series_name.ilike(series))

    results = query.limit(limit).offset(offset).all()

    #query returns a tuple: (Listing_Object, Card_object)
     #basically our SQL select clause
    listings = []
    for listing, card in results:
        listings.append({
            "card_id": listing.card_id,
            "name": card.name,
            "team": card.team_short_name,
            "ovr": card.ovr,
            "series": card.series_name,
            "best_sell_price": listing.best_sell_price,
            "best_buy_price": listing.best_buy_price

        })

    return listings


@router.get("/{card_id}", response_model=ListingResponse)
def get_listing(card_id: str, db: Session = Depends(get_db)):
    "gets listing (market price) for a single card"

    query = (
        db.query(Listing, Card)
        .join(Card, Listing.card_id == Card.id)
        .filter(Listing.card_id == card_id)
        .first()
    )

    if not query:
        raise HTTPException(status_code=404, detail="Listing not found")
    
    listing, card = query



    return {
        "card_id": listing.card_id,
        "name": card.name,
        "team": card.team_short_name,
        "ovr": card.ovr,
        "best_sell_price": listing.best_sell_price,
        "best_buy_price": listing.best_buy_price
    }





