from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from shared.db.database import get_db
from shared.db.models import (
    Users,
    Portfolio,
    PortfolioHolding,
    PortfolioTransaction,
    Card,
    CardPrediction,
)
from src.api.routes.users import firebase_claims
from src.schemas.portfolio import (
    HoldingCreate,
    HoldingUpdate,
    PortfolioPrivacyUpdate,
    HoldingResponse,
    HoldingCardInfo,
    PortfolioResponse,
)

router = APIRouter(prefix="/portfolios", tags=["portfolios"])


# ── Auth dependency (same pattern as user_predictions) ──────────────────────────

def get_current_user(
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims),
) -> Users:
    uid = claims.get("uid")
    if not uid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )
    user = db.scalar(select(Users).where(Users.firebase_id == uid))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    return user


# ── Helper: get or auto-create the user's single portfolio ──────────────────────

def get_or_create_portfolio(db: Session, user: Users) -> Portfolio:
    portfolio = db.scalar(
        select(Portfolio).where(Portfolio.user_id == user.id)
    )
    if not portfolio:
        portfolio = Portfolio(user_id=user.id, name="Default")
        db.add(portfolio)
        db.commit()
        db.refresh(portfolio)
    return portfolio


# ── Helper: attach predicted_ovr and predicted_attributes to each holding's card ─

def attach_predicted_ovr(db: Session, holdings: list[PortfolioHolding]) -> None:
    """Fetches the latest CardPrediction predicted_ovr and predicted_attributes for each holding's card."""
    for holding in holdings:
        if holding.card:
            pred = db.scalar(
                select(CardPrediction)
                .where(CardPrediction.card_id == holding.card_id)
                .order_by(CardPrediction.run_id.desc())
                .limit(1)
            )
            if pred:
                holding.card.predicted_ovr = pred.predicted_ovr
                holding.card.predicted_attributes = pred.predicted_attributes


# ── GET /portfolios/me ──────────────────────────────────────────────────────────

@router.get("/me", response_model=PortfolioResponse)
def get_my_portfolio(
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    """Return the authenticated user's portfolio with all holdings and card data."""
    portfolio = get_or_create_portfolio(db, user)

    # Re-fetch with eager-loaded holdings + cards
    portfolio = db.scalar(
        select(Portfolio)
        .where(Portfolio.id == portfolio.id)
        .options(
            selectinload(Portfolio.holdings).selectinload(PortfolioHolding.card)
        )
    )

    attach_predicted_ovr(db, portfolio.holdings)

    return portfolio


# ── POST /portfolios/me/holdings ────────────────────────────────────────────────

@router.post("/me/holdings", response_model=HoldingResponse, status_code=status.HTTP_201_CREATED)
def add_holding(
    body: HoldingCreate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    """Add a holding to the user's portfolio. Upserts if card already exists."""
    portfolio = get_or_create_portfolio(db, user)

    card = db.scalar(select(Card).where(Card.id == body.card_id))
    if not card:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Card not found"
        )

    existing = db.scalar(
        select(PortfolioHolding).where(
            PortfolioHolding.portfolio_id == portfolio.id,
            PortfolioHolding.card_id == body.card_id,
        )
    )

    if existing:
        total_qty = existing.quantity + body.quantity
        if existing.avg_price is not None:
            weighted_avg = (
                (existing.quantity * existing.avg_price) + (body.quantity * body.avg_price)
            ) // total_qty
        else:
            weighted_avg = body.avg_price
        existing.quantity = total_qty
        existing.avg_price = weighted_avg
        existing.user_predicted_ovr = body.user_predicted_ovr
        holding = existing
    else:
        holding = PortfolioHolding(
            portfolio_id=portfolio.id,
            card_id=body.card_id,
            quantity=body.quantity,
            avg_price=body.avg_price,
            user_predicted_ovr=body.user_predicted_ovr,
        )
        db.add(holding)

    txn = PortfolioTransaction(
        portfolio_id=portfolio.id,
        card_id=body.card_id,
        qty_delta=body.quantity,
        price=body.avg_price,
    )
    db.add(txn)

    db.commit()
    db.refresh(holding)

    holding = db.scalar(
        select(PortfolioHolding)
        .where(
            PortfolioHolding.portfolio_id == portfolio.id,
            PortfolioHolding.card_id == body.card_id,
        )
        .options(selectinload(PortfolioHolding.card))
    )

    pred = db.scalar(
        select(CardPrediction.predicted_ovr)
        .where(CardPrediction.card_id == body.card_id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
    )
    if holding.card:
        holding.card.predicted_ovr = pred

    return holding


# ── PUT /portfolios/me/holdings/{card_id} ───────────────────────────────────────

@router.put("/me/holdings/{card_id}", response_model=HoldingResponse)
def update_holding(
    card_id: str,
    body: HoldingUpdate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    """Partially update an existing holding."""
    portfolio = get_or_create_portfolio(db, user)

    holding = db.scalar(
        select(PortfolioHolding)
        .where(
            PortfolioHolding.portfolio_id == portfolio.id,
            PortfolioHolding.card_id == card_id,
        )
        .options(selectinload(PortfolioHolding.card))
    )

    if not holding:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Holding not found"
        )

    if body.quantity is not None:
        holding.quantity = body.quantity
    if body.avg_price is not None:
        holding.avg_price = body.avg_price
    if body.user_predicted_ovr is not None:
        holding.user_predicted_ovr = body.user_predicted_ovr

    db.commit()
    db.refresh(holding)

    # Attach predicted_ovr
    pred = db.scalar(
        select(CardPrediction.predicted_ovr)
        .where(CardPrediction.card_id == card_id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
    )
    if holding.card:
        holding.card.predicted_ovr = pred

    return holding


# ── DELETE /portfolios/me/holdings/{card_id} ────────────────────────────────────

@router.delete("/me/holdings/{card_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_holding(
    card_id: str,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    """Remove a holding from the user's portfolio."""
    portfolio = get_or_create_portfolio(db, user)

    holding = db.scalar(
        select(PortfolioHolding).where(
            PortfolioHolding.portfolio_id == portfolio.id,
            PortfolioHolding.card_id == card_id,
        )
    )

    if not holding:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Holding not found"
        )

    # Record removal transaction
    txn = PortfolioTransaction(
        portfolio_id=portfolio.id,
        card_id=card_id,
        qty_delta=-holding.quantity,
        price=holding.avg_price or 0,
    )
    db.add(txn)

    db.delete(holding)
    db.commit()


# ── PATCH /portfolios/me ────────────────────────────────────────────────────────

@router.patch("/me", response_model=PortfolioResponse)
def update_portfolio_privacy(
    body: PortfolioPrivacyUpdate,
    db: Session = Depends(get_db),
    user: Users = Depends(get_current_user),
):
    """Toggle portfolio privacy (public/private)."""
    portfolio = get_or_create_portfolio(db, user)
    
    portfolio.is_public = body.is_public
    db.commit()
    db.refresh(portfolio)
    
    # Re-fetch with eager-loaded holdings + cards
    portfolio = db.scalar(
        select(Portfolio)
        .where(Portfolio.id == portfolio.id)
        .options(
            selectinload(Portfolio.holdings).selectinload(PortfolioHolding.card)
        )
    )
    
    attach_predicted_ovr(db, portfolio.holdings)
    
    return portfolio


# ── GET /users/{user_id}/portfolio ──────────────────────────────────────────────

@router.get("/users/{user_id}/portfolio", response_model=PortfolioResponse)
def get_user_portfolio(
    user_id: int,
    db: Session = Depends(get_db),
    current_user: Users = Depends(get_current_user),
):
    """Get a user's portfolio. Only visible if portfolio is public or if requesting user is the owner."""
    # Find the user
    user = db.scalar(select(Users).where(Users.id == user_id))
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
        )
    
    # Get their portfolio
    portfolio = db.scalar(
        select(Portfolio)
        .where(Portfolio.user_id == user_id)
        .options(
            selectinload(Portfolio.holdings).selectinload(PortfolioHolding.card)
        )
    )
    
    if not portfolio:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found"
        )
    
    # Check privacy: only allow if public or if owner
    is_owner = current_user.id == user_id
    if not portfolio.is_public and not is_owner:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="This portfolio is private"
        )
    
    attach_predicted_ovr(db, portfolio.holdings)
    
    return portfolio
