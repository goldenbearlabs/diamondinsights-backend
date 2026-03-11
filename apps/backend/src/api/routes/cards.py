import os
from collections import defaultdict
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Dict, List, Optional
from pydantic import ValidationError
from redis import Redis
from sqlalchemy import or_, func
from sqlalchemy.orm import Session, selectinload, aliased
from shared.db.database import get_db
from shared.db.models import Card, Comment, UserPrediction, CardPrediction, Users, Listing
from src.schemas.card import CardResponse
from src.api.routes.users import firebase_claims_optional
from src.api.routes.show_profiles.profile import _get_profile_for_user
from src.api.routes.show_profiles.analytics import _load_your_ovr_weights_cached
from src.core.cache import build_cache_key, get_cache_client, get_cached_json, set_cached_json

router = APIRouter(prefix="/cards", tags=["cards"])

def _read_positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, value)


CARDS_RANKINGS_CACHE_TTL_SEC = _read_positive_int_env("CARDS_RANKINGS_CACHE_TTL_SEC", 3600)


def _parse_csv_tokens(value: Optional[str], upper: bool = False) -> List[str]:
    if not value:
        return []
    out: List[str] = []
    seen: set[str] = set()
    for token in value.split(","):
        normalized = token.strip()
        if not normalized:
            continue
        if upper:
            normalized = normalized.upper()
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _parse_years(year: Optional[int], years: Optional[str]) -> List[int]:
    parsed: List[int] = []
    seen: set[int] = set()
    if year is not None:
        seen.add(year)
        parsed.append(year)
    for token in _parse_csv_tokens(years):
        try:
            value = int(token)
        except ValueError:
            continue
        if value in seen:
            continue
        seen.add(value)
        parsed.append(value)
    return parsed


def _normalize_position(value: Optional[str]) -> str:
    return (value or "").strip().upper()


def _normalize_cache_text(value: Optional[str]) -> str:
    return (value or "").strip().lower()


def _serialize_cache_list(values: List[object]) -> str:
    if not values:
        return "-"
    return ",".join(str(value) for value in values)


def _secondary_positions(card: Card) -> set[str]:
    raw = (card.display_secondary_positions or "").replace("/", ",")
    out: set[str] = set()
    for token in raw.split(","):
        normalized = _normalize_position(token)
        if normalized:
            out.add(normalized)
    return out


def _primary_position(card: Card) -> str:
    return _normalize_position(getattr(card, "display_primary_position", None) or card.display_position)


def _metric_position_for_sort(
    card: Card,
    selected_positions: List[str],
    include_secondary: bool,
) -> Optional[str]:
    primary = _primary_position(card)
    if not selected_positions:
        return primary or None

    if primary in selected_positions:
        return primary

    if include_secondary:
        secondary = _secondary_positions(card)
        for selected in selected_positions:
            if selected in secondary:
                return selected

    return primary or None


def _rounded_number(value: Optional[float]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _position_metric_value(map_data: Optional[dict], position: Optional[str]) -> Optional[int]:
    if not map_data or not position:
        return None
    value = map_data.get(position)
    return _rounded_number(value)


def _metric_value(
    card: Card,
    metric: str,
    selected_positions: List[str],
    include_secondary: bool,
) -> Optional[int]:
    metric_position = _metric_position_for_sort(card, selected_positions, include_secondary)
    if metric == "meta":
        from_map = _position_metric_value(card.meta_overall_by_position, metric_position)
        if from_map is not None:
            return from_map
        return _rounded_number(card.meta_overall_rounded if card.meta_overall_rounded is not None else card.meta_overall)
    if metric == "true":
        from_map = _position_metric_value(card.true_overall_by_position, metric_position)
        if from_map is not None:
            return from_map
        return _rounded_number(card.true_overall_rounded if card.true_overall_rounded is not None else card.true_overall)
    return None


def _your_weight_map_for_claims(db: Session, claims: dict) -> dict[str, float]:
    uid = (claims or {}).get("uid")
    if not uid:
        return {}

    user = db.query(Users).filter(Users.firebase_id == uid).first()
    if not user:
        return {}

    sp = _get_profile_for_user(db, user.id)
    if not sp:
        return {}

    payload = _load_your_ovr_weights_cached(sp.username)
    out: dict[str, float] = {}
    for item in payload.weights:
        try:
            out[f"{item.role}:{int(item.mlb_id)}"] = float(item.weight)
        except (TypeError, ValueError):
            continue
    return out


def _your_weight_for_card(card: Card, your_weight_map: dict[str, float]) -> float:
    mlb_id = getattr(card, "mlb_id", None)
    if not isinstance(mlb_id, int):
        return 1.0
    role = "pitching" if getattr(card, "is_hitter", True) is False else "hitting"
    return float(your_weight_map.get(f"{role}:{mlb_id}", 1.0))


def _attach_card_your_overall_fields(card: Card, your_weight_map: dict[str, float]) -> None:
    weight = _your_weight_for_card(card, your_weight_map)

    meta_exact_raw = card.meta_overall if card.meta_overall is not None else card.meta_overall_rounded
    meta_exact = None
    if meta_exact_raw is not None:
        try:
            meta_exact = float(meta_exact_raw)
        except (TypeError, ValueError):
            meta_exact = None
    your_exact = None if meta_exact is None else (meta_exact * weight)
    your_rounded = _rounded_number(your_exact)

    your_by_position: dict[str, float] = {}
    for position, value in (card.meta_overall_by_position or {}).items():
        try:
            weighted = float(value) * weight
        except (TypeError, ValueError):
            continue
        your_by_position[position] = weighted

    card.your_overall = your_exact
    card.your_overall_rounded = your_rounded
    card.your_overall_by_position = your_by_position


def _quicksell_value_for_ovr(ovr: Optional[int]) -> Optional[int]:
    if ovr is None:
        return None

    if ovr < 65:
        return 5
    if ovr <= 74:
        return 25
    if ovr <= 79:
        return 50 + ((ovr - 75) * 25)

    quicksell_by_ovr = {
        80: 400,
        81: 600,
        82: 900,
        83: 1200,
        84: 1500,
        85: 3000,
        86: 3750,
        87: 4500,
        88: 5500,
        89: 7000,
        90: 8000,
        91: 9000,
    }
    if ovr >= 92:
        return 10000
    return quicksell_by_ovr.get(ovr)


def _normalized_listing_price(price: Optional[int], ovr: Optional[int]) -> Optional[int]:
    if price is None or price == 0:
        return _quicksell_value_for_ovr(ovr)
    return price


def _apply_market_prices_to_card(
    card: Card,
    best_buy_price: Optional[int],
    best_sell_price: Optional[int],
) -> None:
    quicksell_value = _quicksell_value_for_ovr(card.ovr)
    normalized_buy_price = _normalized_listing_price(best_buy_price, card.ovr)
    normalized_sell_price = _normalized_listing_price(best_sell_price, card.ovr)

    card.quicksell_value = quicksell_value
    card.best_buy_price = normalized_buy_price
    card.best_sell_price = normalized_sell_price
    card.buy_now_uses_quicksell = (
        quicksell_value is not None
        and normalized_buy_price is not None
        and normalized_buy_price == quicksell_value
    )

    if card.buy_now_uses_quicksell:
        card.buy_now_above_quicksell_pct = 0.0
        return

    if quicksell_value is None or quicksell_value <= 0 or normalized_buy_price is None:
        card.buy_now_above_quicksell_pct = None
        return

    premium_pct = ((float(normalized_buy_price) - float(quicksell_value)) / float(quicksell_value)) * 100.0
    card.buy_now_above_quicksell_pct = round(premium_pct, 1)


def _community_prediction_value(user_predictions: List[int], base_prediction: Optional[int]) -> Optional[int]:
    if not user_predictions:
        return _rounded_number(base_prediction)

    raw_mean = float(sum(user_predictions)) / float(len(user_predictions))
    filtered = [value for value in user_predictions if abs(float(value) - raw_mean) <= 10.0]

    values_for_average: List[float] = [float(value) for value in filtered]
    if base_prediction is not None:
        values_for_average.append(float(base_prediction))

    if not values_for_average:
        return _rounded_number(raw_mean)

    return _rounded_number(sum(values_for_average) / float(len(values_for_average)))


def _community_prediction_map_for_cards(
    db: Session,
    cards: List[Card],
) -> Dict[str, Optional[int]]:
    if not cards:
        return {}

    card_ids = [card.id for card in cards]
    rows = (
        db.query(UserPrediction.card_id, UserPrediction.predicted_ovr)
        .filter(UserPrediction.card_id.in_(card_ids))
        .all()
    )

    grouped_predictions: Dict[str, List[int]] = defaultdict(list)
    for card_id, predicted_ovr in rows:
        try:
            grouped_predictions[card_id].append(int(predicted_ovr))
        except (TypeError, ValueError):
            continue

    out: Dict[str, Optional[int]] = {}
    for card in cards:
        base_prediction = _rounded_number(getattr(card, "predicted_ovr", None))
        out[card.id] = _community_prediction_value(grouped_predictions.get(card.id, []), base_prediction)
    return out


@router.get("/{card_id}", response_model=CardResponse)
def get_card(
    card_id: str,
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
):
    """
    gets a single card by its id |
    Response Time: ~190ms
    """

    card = (
        db.query(Card)
        .options(selectinload(Card.position_overalls), selectinload(Card.quirks))
        .filter(Card.id == card_id)
        .first()
    )

    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    comment_count_sub = (
        db.query(func.count(Comment.id))
        .filter(Comment.card_id == Card.id, Comment.is_deleted == False)
        .correlate(Card)
        .scalar_subquery()
        .label("comment_count")
    )
    prediction_count_sub = (
        db.query(func.count(UserPrediction.user_id))
        .filter(UserPrediction.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("user_prediction_count")
    )
    predicted_ovr_sub = (
        db.query(CardPrediction.predicted_ovr)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_ovr")
    )
    predicted_attrs_sub = (
        db.query(CardPrediction.predicted_attributes)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_attributes")
    )
    best_buy_price_sub = (
        db.query(Listing.best_buy_price)
        .filter(Listing.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("best_buy_price")
    )
    best_sell_price_sub = (
        db.query(Listing.best_sell_price)
        .filter(Listing.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("best_sell_price")
    )

    row = (
        db.query(
            Card,
            comment_count_sub,
            prediction_count_sub,
            predicted_ovr_sub,
            predicted_attrs_sub,
            best_buy_price_sub,
            best_sell_price_sub,
        )
        .options(selectinload(Card.position_overalls), selectinload(Card.quirks))
        .filter(Card.id == card_id)
        .first()
    )
    
    if not row:
        raise HTTPException(status_code=404, detail="Card not found")
    
    card, comment_count, user_prediction_count, predicted_ovr, predicted_attributes, best_buy_price, best_sell_price = row
    card.comment_count = comment_count or 0
    card.user_prediction_count = user_prediction_count or 0
    card.predicted_ovr = predicted_ovr
    card.predicted_attributes = predicted_attributes
    _apply_market_prices_to_card(card, best_buy_price, best_sell_price)
    card.community_predicted_ovr = _community_prediction_map_for_cards(db, [card]).get(card.id)
    your_weight_map = _your_weight_map_for_claims(db, claims)
    _attach_card_your_overall_fields(card, your_weight_map)
    return card


@router.get("", response_model=List[CardResponse], include_in_schema=False)
@router.get("/", response_model=List[CardResponse])
def get_cards(
    is_hitter: Optional[bool] = Query(None),
    team: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    series: Optional[str] = Query(None),
    year: Optional[int] = Query(None),
    years: Optional[str] = Query(None),
    position: Optional[str] = Query(None),
    positions: Optional[str] = Query(None),
    include_secondary: bool = Query(False),
    bat_hand: Optional[str] = Query(None),
    bat_hands: Optional[str] = Query(None),
    throw_hand: Optional[str] = Query(None),
    pitch_hand: Optional[str] = Query(None),
    pitch_hands: Optional[str] = Query(None),
    rarity: Optional[str] = Query(None),
    sort_by: Optional[str] = Query(None),
    sort_dir: Optional[str] = Query(None),
    desc: bool = Query(True),
    limit: int = Query(50, le=100),
    offset: int = 0,
    my_predictions: bool = Query(False),
    db: Session = Depends(get_db),
    claims: dict = Depends(firebase_claims_optional),
    cache: Redis | None = Depends(get_cache_client),
):
    """
    gets multiple cards (with optional filters) |
    Response Time: ~150 - 240ms for first time loading |
    These queries don't join with any other tables.
    """

    year_values = _parse_years(year, years)
    rarity_values = _parse_csv_tokens(rarity)
    position_values = _parse_csv_tokens(positions, upper=True)
    if position is not None:
        single_position = position.strip().upper()
        if single_position:
            position_values = [single_position] + [value for value in position_values if value != single_position]
    bat_hand_values = _parse_csv_tokens(bat_hands, upper=True)
    if bat_hand is not None:
        single_bat = bat_hand.strip().upper()
        if single_bat:
            bat_hand_values = [single_bat] + [value for value in bat_hand_values if value != single_bat]
    pitch_hand_values = _parse_csv_tokens(pitch_hands, upper=True)
    chosen_throw_hand = throw_hand if throw_hand is not None else pitch_hand
    if chosen_throw_hand is not None:
        single_throw = chosen_throw_hand.strip().upper()
        if single_throw:
            pitch_hand_values = [single_throw] + [value for value in pitch_hand_values if value != single_throw]
    selected_positions_for_metric: List[str] = position_values
    normalized_sort_by = (sort_by or "ovr").strip().lower()
    requested_sort_dir = (sort_dir or ("desc" if desc else "asc")).strip().lower()
    normalized_sort_dir = "asc" if requested_sort_dir == "asc" else "desc"
    reverse = normalized_sort_dir == "desc"
    normalized_limit = max(0, limit)
    normalized_offset = max(0, offset)
    user_id_for_cache = str((claims or {}).get("uid") or "anon")
    cache_key = build_cache_key(
        "cards",
        "rankings_v3",
        user_id_for_cache,
        is_hitter if is_hitter is not None else "any",
        _normalize_cache_text(team),
        _normalize_cache_text(name),
        _normalize_cache_text(series),
        _serialize_cache_list(year_values),
        _serialize_cache_list(position_values),
        int(include_secondary),
        _serialize_cache_list(bat_hand_values),
        _serialize_cache_list(pitch_hand_values),
        _normalize_cache_text(rarity),
        normalized_sort_by,
        normalized_sort_dir,
        normalized_limit,
        normalized_offset,
        int(my_predictions),
    )
    cached = get_cached_json(cache, cache_key)
    if cached is not None:
        cards_payload = cached.get("cards")
        if isinstance(cards_payload, list):
            try:
                return [CardResponse.model_validate(item) for item in cards_payload]
            except (TypeError, ValueError, ValidationError):
                pass

    comment_count_sub = (
        db.query(func.count(Comment.id))
        .filter(Comment.card_id == Card.id, Comment.is_deleted == False)
        .correlate(Card)
        .scalar_subquery()
        .label("comment_count")
    )
    prediction_count_sub = (
        db.query(func.count(UserPrediction.user_id))
        .filter(UserPrediction.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("user_prediction_count")
    )
    predicted_ovr_sub = (
        db.query(CardPrediction.predicted_ovr)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_ovr")
    )
    predicted_attrs_sub = (
        db.query(CardPrediction.predicted_attributes)
        .filter(CardPrediction.card_id == Card.id)
        .order_by(CardPrediction.run_id.desc())
        .limit(1)
        .correlate(Card)
        .scalar_subquery()
        .label("predicted_attributes")
    )
    best_buy_price_sub = (
        db.query(Listing.best_buy_price)
        .filter(Listing.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("best_buy_price")
    )
    best_sell_price_sub = (
        db.query(Listing.best_sell_price)
        .filter(Listing.card_id == Card.id)
        .correlate(Card)
        .scalar_subquery()
        .label("best_sell_price")
    )

    current_user_id = -1
    uid = (claims or {}).get("uid")
    if uid:
        user_for_pred = db.query(Users).filter(Users.firebase_id == uid).first()
        if user_for_pred:
            current_user_id = user_for_pred.id

    UP_Alias = aliased(UserPrediction)

    user_prediction_sub = (
        db.query(UP_Alias.predicted_ovr)
        .filter(UP_Alias.card_id == Card.id, UP_Alias.user_id == current_user_id)
        .correlate(Card)
        .scalar_subquery()
        .label("user_prediction")
    )

    query = db.query(
        Card,
        comment_count_sub,
        prediction_count_sub,
        predicted_ovr_sub,
        predicted_attrs_sub,
        best_buy_price_sub,
        best_sell_price_sub,
        user_prediction_sub,
    ).options(selectinload(Card.position_overalls), selectinload(Card.quirks))


    if my_predictions:
        uid = (claims or {}).get("uid")
        if not uid:
            raise HTTPException(status_code=401, detail="Must be logged in to view your predictions")
        
        user = db.query(Users).filter(Users.firebase_id == uid).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
            
        query = query.join(UserPrediction, UserPrediction.card_id == Card.id).filter(UserPrediction.user_id == user.id)


    if is_hitter is not None:
        query = query.filter(Card.is_hitter == is_hitter)

    if team is not None:
        query = query.filter(Card.team_short_name.ilike(team))

    if name is not None:
        query = query.filter(Card.name.ilike(f"%{name}%"))

    if series is not None:
        query = query.filter(Card.series_name.ilike(series))

    if year_values:
        if len(year_values) == 1:
            query = query.filter(Card.year == year_values[0])
        else:
            query = query.filter(Card.year.in_(year_values))

    if position_values:
        primary_match = func.upper(Card.display_position).in_(position_values)
        if include_secondary:
            normalized_secondary = func.replace(
                func.replace(func.upper(func.coalesce(Card.display_secondary_positions, "")), " ", ""),
                "/",
                ",",
            )
            secondary_conditions = []
            for value in position_values:
                secondary_conditions.extend(
                    [
                        normalized_secondary == value,
                        normalized_secondary.like(f"{value},%"),
                        normalized_secondary.like(f"%,{value},%"),
                        normalized_secondary.like(f"%,{value}"),
                    ]
                )
            query = query.filter(or_(primary_match, *secondary_conditions))
        else:
            query = query.filter(primary_match)

    if bat_hand_values:
        if len(bat_hand_values) == 1:
            query = query.filter(func.upper(Card.bat_hand) == bat_hand_values[0])
        else:
            query = query.filter(func.upper(Card.bat_hand).in_(bat_hand_values))

    if pitch_hand_values:
        if len(pitch_hand_values) == 1:
            query = query.filter(func.upper(Card.throw_hand) == pitch_hand_values[0])
        else:
            query = query.filter(func.upper(Card.throw_hand).in_(pitch_hand_values))

    if rarity_values:
        if len(rarity_values) == 1:
            single_rarity = rarity_values[0]
            if "%" in single_rarity or "_" in single_rarity:
                query = query.filter(Card.rarity.ilike(single_rarity))
            else:
                query = query.filter(func.lower(Card.rarity) == single_rarity.lower())
        else:
            query = query.filter(func.lower(Card.rarity).in_([value.lower() for value in rarity_values]))

    rows = query.all()
    cards: List[Card] = []
    for row in rows:
        card, comment_count, user_prediction_count, predicted_ovr, predicted_attributes, best_buy_price, best_sell_price, user_prediction = row
        card.comment_count = comment_count or 0
        card.user_prediction_count = user_prediction_count or 0
        card.predicted_ovr = predicted_ovr
        card.predicted_attributes = predicted_attributes
        _apply_market_prices_to_card(card, best_buy_price, best_sell_price)
        card.user_prediction = user_prediction
        cards.append(card)

    # Defensive dedupe to ensure one row per card id in all modes.
    unique_by_id: dict[str, Card] = {}
    for card in cards:
        if card.id not in unique_by_id:
            unique_by_id[card.id] = card
    cards = list(unique_by_id.values())

    your_weight_map = _your_weight_map_for_claims(db, claims)

    def sort_key(card: Card):
        if normalized_sort_by == "name":
            return (card.name or "").lower()
        if normalized_sort_by == "hands":
            return f"{(card.bat_hand or '').upper()}/{(card.throw_hand or '').upper()}"
        if normalized_sort_by == "position":
            return _primary_position(card)
        if normalized_sort_by == "meta":
            value = _metric_value(card, "meta", selected_positions_for_metric, include_secondary)
            return -1 if value is None else value
        if normalized_sort_by == "true":
            value = _metric_value(card, "true", selected_positions_for_metric, include_secondary)
            return -1 if value is None else value
        if normalized_sort_by == "ovr":
            return card.ovr if card.ovr is not None else -1
        if normalized_sort_by == "your":
            base = _metric_value(card, "meta", selected_positions_for_metric, include_secondary)
            if base is None:
                return -1
            weighted = _rounded_number(float(base) * _your_weight_for_card(card, your_weight_map))
            return -1 if weighted is None else weighted
        
        if normalized_sort_by == "popularity":
            return card.user_prediction_count if card.user_prediction_count is not None else -1
            
        if normalized_sort_by == "predicted_ovr_delta":
            if card.predicted_ovr is not None and card.ovr is not None:
                return card.predicted_ovr - card.ovr
            return -999  if reverse else 999

        value = getattr(card, normalized_sort_by, None)
        rounded = _rounded_number(value)
        if rounded is not None:
            return rounded
        if value is None:
            return -1
        return str(value).lower()

    if normalized_sort_by:
        cards.sort(key=sort_key, reverse=reverse)
    else:
        cards.sort(key=lambda card: card.ovr if card.ovr is not None else -1, reverse=desc)

    for card in cards:
        _attach_card_your_overall_fields(card, your_weight_map)

    start = normalized_offset
    end = start + normalized_limit
    page_cards = cards[start:end]
    community_prediction_map = _community_prediction_map_for_cards(db, page_cards)
    for card in page_cards:
        card.community_predicted_ovr = community_prediction_map.get(card.id)

    response_cards = [CardResponse.model_validate(card) for card in page_cards]
    set_cached_json(
        cache,
        cache_key,
        {"cards": [card.model_dump(mode="json") for card in response_cards]},
        ttl_sec=CARDS_RANKINGS_CACHE_TTL_SEC,
    )
    return response_cards
