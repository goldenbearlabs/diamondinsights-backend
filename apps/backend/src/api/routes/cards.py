from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from sqlalchemy import func, or_
from sqlalchemy.orm import Session, selectinload
from shared.db.database import get_db
from shared.db.models import Card
from src.schemas.card import CardResponse

router = APIRouter(prefix="/cards", tags=["cards"])


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


@router.get("/{card_id}", response_model=CardResponse)
def get_card(card_id: str, db: Session = Depends(get_db)):
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

    return card


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
    db: Session = Depends(get_db),
):
    """
    gets multiple cards (with optional filters) |
    Response Time: ~150 - 240ms for first time loading |
    These queries don't join with any other tables.
    """

    query = db.query(Card).options(selectinload(Card.position_overalls), selectinload(Card.quirks))

    if is_hitter is not None:
        query = query.filter(Card.is_hitter == is_hitter)

    if team is not None:
        query = query.filter(Card.team_short_name.ilike(team))

    if name is not None:
        query = query.filter(Card.name.ilike(f"%{name}%"))

    if series is not None:
        query = query.filter(Card.series_name.ilike(series))

    year_values = _parse_years(year, years)
    if year_values:
        if len(year_values) == 1:
            query = query.filter(Card.year == year_values[0])
        else:
            query = query.filter(Card.year.in_(year_values))

    position_values = _parse_csv_tokens(positions, upper=True)
    if position is not None:
        single_position = position.strip().upper()
        if single_position:
            position_values = [single_position] + [value for value in position_values if value != single_position]

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

    bat_hand_values = _parse_csv_tokens(bat_hands, upper=True)
    if bat_hand is not None:
        single_bat = bat_hand.strip().upper()
        if single_bat:
            bat_hand_values = [single_bat] + [value for value in bat_hand_values if value != single_bat]
    if bat_hand_values:
        if len(bat_hand_values) == 1:
            query = query.filter(func.upper(Card.bat_hand) == bat_hand_values[0])
        else:
            query = query.filter(func.upper(Card.bat_hand).in_(bat_hand_values))

    pitch_hand_values = _parse_csv_tokens(pitch_hands, upper=True)
    chosen_throw_hand = throw_hand if throw_hand is not None else pitch_hand
    if chosen_throw_hand is not None:
        single_throw = chosen_throw_hand.strip().upper()
        if single_throw:
            pitch_hand_values = [single_throw] + [value for value in pitch_hand_values if value != single_throw]
    if pitch_hand_values:
        if len(pitch_hand_values) == 1:
            query = query.filter(func.upper(Card.throw_hand) == pitch_hand_values[0])
        else:
            query = query.filter(func.upper(Card.throw_hand).in_(pitch_hand_values))

    if rarity is not None:
        query = query.filter(Card.rarity.ilike(rarity))

    cards = query.all()

    # Defensive dedupe to ensure one row per card id in all modes.
    unique_by_id: dict[str, Card] = {}
    for card in cards:
        if card.id not in unique_by_id:
            unique_by_id[card.id] = card
    cards = list(unique_by_id.values())

    selected_positions_for_metric: List[str] = position_values
    normalized_sort_by = (sort_by or "ovr").strip().lower()
    normalized_sort_dir = (sort_dir or ("desc" if desc else "asc")).strip().lower()
    reverse = normalized_sort_dir != "asc"

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

    start = max(0, offset)
    end = start + max(0, limit)
    return cards[start:end]
