from typing import List, Optional
import datetime
from sqlalchemy import Column, ForeignKey, Table, Date, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    BigInteger,
    Text,
    UniqueConstraint,
    text,
    String,
    SmallInteger
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship
from shared.db.database import Base

card_quirk_association = Table(
    "card_quirks",
    Base.metadata,
    Column("card_id", ForeignKey("cards.id"), primary_key=True),
    Column("quirk_name", ForeignKey("quirks.name"), primary_key=True),
)

card_location_association = Table(
    "card_locations",
    Base.metadata,
    Column("card_id", ForeignKey("cards.id"), primary_key=True),
    Column("location_name", ForeignKey("locations.name"), primary_key=True),
)

class Card(Base):
    __tablename__ = "cards"
    __table_args__ = (
        UniqueConstraint("year", "source_uuid", name="uq_cards_year_source_uuid"),
        Index("ix_cards_year_mlb_id", "year", "mlb_id"),
        Index("ix_cards_year_mlb_id_display_position", "year", "mlb_id", "display_position"),
    )

    id: Mapped[str] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column()
    source_uuid: Mapped[str] = mapped_column()
    year: Mapped[int] = mapped_column()
    img: Mapped[str] = mapped_column()
    baked_img: Mapped[Optional[str]] = mapped_column()
    name: Mapped[str] = mapped_column(index=True)
    search_name: Mapped[str] = mapped_column(index=True)
    short_description: Mapped[Optional[str]] = mapped_column()
    rarity: Mapped[str] = mapped_column()
    team: Mapped[str] = mapped_column()
    team_short_name: Mapped[str] = mapped_column()
    ovr: Mapped[int] = mapped_column()
    series_name: Mapped[str] = mapped_column(ForeignKey("series.name"))
    display_position: Mapped[str] = mapped_column()
    display_secondary_positions: Mapped[Optional[str]] = mapped_column()
    jersey_number: Mapped[int] = mapped_column()
    age: Mapped[int] = mapped_column()
    bat_hand: Mapped[str] = mapped_column()
    throw_hand: Mapped[str] = mapped_column()
    weight: Mapped[str] = mapped_column()
    height: Mapped[str] = mapped_column()
    born: Mapped[str] = mapped_column()
    is_hitter: Mapped[bool] = mapped_column()
    stamina: Mapped[int] = mapped_column()
    pitching_clutch: Mapped[int] = mapped_column()
    hits_per_bf: Mapped[int] = mapped_column()
    k_per_bf: Mapped[int] = mapped_column()
    bb_per_bf: Mapped[int] = mapped_column()
    hr_per_bf: Mapped[int] = mapped_column()
    pitch_velocity: Mapped[int] = mapped_column()
    pitch_control: Mapped[int] = mapped_column()
    pitch_movement: Mapped[int] = mapped_column()
    contact_left: Mapped[int] = mapped_column()
    contact_right: Mapped[int] = mapped_column()
    power_left: Mapped[int] = mapped_column()
    power_right: Mapped[int] = mapped_column()
    plate_vision: Mapped[int] = mapped_column()
    plate_discipline: Mapped[int] = mapped_column()
    batting_clutch: Mapped[int] = mapped_column()
    bunting_ability: Mapped[int] = mapped_column()
    drag_bunting_ability: Mapped[int] = mapped_column()
    hitting_durability: Mapped[int] = mapped_column()
    fielding_durability: Mapped[int] = mapped_column()
    fielding_ability: Mapped[int] = mapped_column()
    arm_strength: Mapped[int] = mapped_column()
    arm_accuracy: Mapped[int] = mapped_column()
    reaction_time: Mapped[int] = mapped_column()
    blocking: Mapped[int] = mapped_column()
    speed: Mapped[int] = mapped_column()
    baserunning_ability: Mapped[int] = mapped_column()
    baserunning_aggression: Mapped[int] = mapped_column()
    hit_rank_image: Mapped[Optional[str]] = mapped_column()
    fielding_rank_image: Mapped[Optional[str]] = mapped_column()
    pitches: Mapped[List["Pitch"]] = relationship(
        back_populates="card", cascade="all, delete-orphan"
    )
    quirks: Mapped[List["Quirk"]] = relationship(
        secondary=card_quirk_association,
        back_populates="cards"
    )
    is_sellable: Mapped[Optional[bool]] = mapped_column()
    has_augment: Mapped[Optional[bool]] = mapped_column()
    augment_text: Mapped[Optional[str]] = mapped_column()
    augment_end_date: Mapped[Optional[datetime.date]] = mapped_column(Date)
    has_matchup: Mapped[bool] = mapped_column()
    stars: Mapped[Optional[str]] = mapped_column()
    trend: Mapped[Optional[str]] = mapped_column()
    new_rank: Mapped[int] = mapped_column()
    has_rank_change: Mapped[Optional[bool]] = mapped_column()
    event: Mapped[Optional[bool]] = mapped_column()
    set_name: Mapped[Optional[str]] = mapped_column()
    is_live_set: Mapped[bool] = mapped_column()
    ui_anim_index: Mapped[Optional[int]] = mapped_column()
    locations: Mapped[List["Location"]] = relationship(
        secondary=card_location_association,
        back_populates="cards"
    )

    series: Mapped["Series"] = relationship(back_populates="cards")

    mlb_id: Mapped[Optional[int]] = mapped_column(ForeignKey("players.mlb_id"), nullable=True)
    player: Mapped[Optional["Player"]] = relationship(back_populates="cards")

    listing: Mapped[Optional["Listing"]] = relationship(
        back_populates="card",
        uselist=False, 
        cascade="all, delete-orphan"
    )
    comments: Mapped[List["Comment"]] = relationship(back_populates="card", cascade="all, delete-orphan")
    predictions: Mapped[List["CardPrediction"]] = relationship(back_populates="card", passive_deletes=True)
    position_overalls: Mapped[List["CardPositionOverall"]] = relationship(
        back_populates="card", passive_deletes=True, cascade="all, delete-orphan"
    )
    votes: Mapped[List["CardVote"]] = relationship(back_populates="card", passive_deletes=True)
    portfolio_holdings: Mapped[List["PortfolioHolding"]] = relationship(back_populates="card", passive_deletes=True)

    def _preferred_position_overall(self) -> Optional["CardPositionOverall"]:
        rows = self.position_overalls or []
        if not rows:
            return None

        primary_pos = (self.display_position or "").strip().upper()
        for row in rows:
            if (row.position or "").strip().upper() == primary_pos:
                return row
        for row in rows:
            if row.is_primary:
                return row
        return rows[0]

    @property
    def display_seconday_position(self) -> Optional[str]:
        # Backward-compatible alias for older API clients.
        return self.display_secondary_positions

    @property
    def display_primary_position(self) -> str:
        return self.display_position

    @property
    def true_overall(self) -> Optional[float]:
        row = self._preferred_position_overall()
        return None if row is None else float(row.true_overall)

    @property
    def true_overall_rounded(self) -> Optional[int]:
        row = self._preferred_position_overall()
        return None if row is None else int(row.true_overall_rounded)

    @property
    def meta_overall(self) -> Optional[float]:
        row = self._preferred_position_overall()
        return None if row is None else float(row.meta_overall)

    @property
    def meta_overall_rounded(self) -> Optional[int]:
        row = self._preferred_position_overall()
        return None if row is None else int(row.meta_overall_rounded)

    @property
    def true_overall_by_position(self) -> dict[str, int]:
        rows = self.position_overalls or []
        out: dict[str, int] = {}
        for row in rows:
            position = (row.position or "").strip().upper()
            if not position:
                continue
            out[position] = int(row.true_overall_rounded)
        return out

    @property
    def meta_overall_by_position(self) -> dict[str, int]:
        rows = self.position_overalls or []
        out: dict[str, int] = {}
        for row in rows:
            position = (row.position or "").strip().upper()
            if not position:
                continue
            out[position] = int(row.meta_overall_rounded)
        return out
    
    def __repr__(self) -> str:
        return f"CARD (id={self.id}, name={self.name}, series={self.series_name}, ovr={self.ovr})"

class Series(Base):
    __tablename__ = "series"

    name: Mapped[str] = mapped_column(primary_key=True)

    cards: Mapped[List["Card"]] = relationship(back_populates="series")

    def __repr__(self) -> str:
        return f"SERIES (name={self.name})"
    
class Location(Base):
    __tablename__ = "locations"

    name: Mapped[str] = mapped_column(primary_key=True)

    cards: Mapped[List["Card"]] = relationship(
        secondary=card_location_association,
        back_populates="locations"
    )

    def __repr__(self) -> str:
        return f"LOCATION (name={self.name})"

class Quirk(Base):
    __tablename__ = "quirks"

    name: Mapped[str] = mapped_column(primary_key=True)
    description: Mapped[str] = mapped_column()
    img: Mapped[str] = mapped_column()

    cards: Mapped[List["Card"]] = relationship(
        secondary=card_quirk_association,
        back_populates="quirks"
    )

    def __repr__(self) -> str:
        return f"QUIRK (name={self.name})"

class Pitch(Base):
    __tablename__ = "pitches"

    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id"), primary_key=True)
    name: Mapped[str] = mapped_column(primary_key=True)
    speed: Mapped[int] = mapped_column()
    control: Mapped[int] = mapped_column()
    movement: Mapped[int] = mapped_column()

    card: Mapped["Card"] = relationship(back_populates="pitches")

    def __repr__(self) -> str:
        return f"PITCH (card.id={self.card_id}, name={self.name})"
    
class Listing(Base):
    __tablename__ = "listings"

    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id"), primary_key=True)
    best_sell_price: Mapped[Optional[int]] = mapped_column()
    best_buy_price: Mapped[Optional[int]] = mapped_column()

    price_history: Mapped[List["PriceHistory"]] = relationship(
        back_populates="listing", cascade="all, delete-orphan"
    )
    orders: Mapped[List["CompletedOrder"]] = relationship(
        back_populates="listing", cascade="all, delete-orphan"
    )
    candles: Mapped[List["MarketCandle"]] = relationship(
        back_populates="listing", cascade="all, delete-orphan"
    )

    card: Mapped["Card"] = relationship(back_populates="listing")

    def __repr__(self) -> str:
        return f"LISTING (card_id={self.card_id}, best_sell={self.best_sell_price}, best_buy={self.best_buy_price})"

class PriceHistory(Base):
    __tablename__ = "price_history"

    card_id: Mapped[str] = mapped_column(ForeignKey("listings.card_id"), primary_key=True)
    date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    best_buy_price: Mapped[Optional[int]] = mapped_column()
    best_sell_price: Mapped[Optional[int]] = mapped_column()
    volume: Mapped[Optional[int]] = mapped_column()

    listing: Mapped["Listing"] = relationship(back_populates="price_history")

    def __repr__(self) -> str:
        return f"PRICE_HISTORY (card_id={self.card_id}, date={self.date}, best_buy_price={self.best_buy_price}, best_sell_price={self.best_sell_price})"

class CompletedOrder(Base):
    # We store the past 48 hours here.
    __tablename__ = "completed_orders"

    card_id: Mapped[str] = mapped_column(ForeignKey("listings.card_id"), primary_key=True)
    date: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    price: Mapped[int] = mapped_column()
    is_buy: Mapped[Optional[bool]] = mapped_column()

    listing: Mapped["Listing"] = relationship(back_populates="orders")

    def __repr__(self) -> str:
        return f"COMPLETED_ORDER (card_id={self.card_id}, date={self.date}, price={self.price}, is_buy={self.is_buy})"
    
class MarketCandle(Base):
    # This is a history table where start time is basically the start of each day at yesterdays midnight and 
    # we summarize the completed order table so after its pruned we have a saved summary. It will run in its own
    # job at midnight
    __tablename__ = "market_candles"

    card_id: Mapped[str] = mapped_column(ForeignKey("listings.card_id"), primary_key=True)
    start_time: Mapped[datetime.datetime] = mapped_column(primary_key=True)

    open_buy_price: Mapped[int] = mapped_column()
    open_sell_price: Mapped[int] = mapped_column()
    low_buy_price: Mapped[int] = mapped_column()
    low_sell_price: Mapped[int] = mapped_column()
    high_buy_price: Mapped[int] = mapped_column()
    high_sell_price: Mapped[int] = mapped_column()
    close_buy_price: Mapped[int] = mapped_column()
    close_sell_price: Mapped[int] = mapped_column()
    sell_volume: Mapped[int] = mapped_column()
    buy_volume: Mapped[int] = mapped_column()

    listing: Mapped["Listing"] = relationship(back_populates="candles")

    def __repr__(self) -> str:
        return f"MARKET_CANDLES (card_id={self.card_id}, start_time={self.start_time}, open_buy={self.open_buy_price})"

class RosterUpdate(Base):
    __tablename__ = "roster_updates"

    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[datetime.date] = mapped_column(primary_key=True)
    is_major: Mapped[bool] = mapped_column()
    is_fielding: Mapped[bool] = mapped_column()

    card_updates: Mapped[List["CardUpdate"]] = relationship(
        back_populates="roster_update", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"ROSTER_UPDATES (id={self.id}, date={self.date}, major?={self.is_major})" 
    
class CardUpdate(Base):
    __tablename__ = "card_updates"

    update_id: Mapped[int] = mapped_column(primary_key=True)
    update_date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id"), primary_key=True)
    
    new_ovr: Mapped[int] = mapped_column()
    new_rarity: Mapped[str] = mapped_column()
    old_ovr: Mapped[int] = mapped_column()
    old_rarity: Mapped[str] = mapped_column()
    trend_display: Mapped[str] = mapped_column()

    roster_update: Mapped["RosterUpdate"] = relationship(
        back_populates="card_updates"
    )
    
    __table_args__ = (
        ForeignKeyConstraint(
            ["update_id", "update_date"],
            ["roster_updates.id", "roster_updates.date"],
        ),
    )

    card: Mapped["Card"] = relationship()
    attribute_changes: Mapped[List["CardAttributeChange"]] = relationship(
        back_populates="card_update",
        cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"CARD_UPDATES (id={self.update_id}, date={self.update_date}, card={self.card_id})" 

class CardAttributeChange(Base):
    __tablename__ = "card_attribute_changes"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    update_id: Mapped[int] = mapped_column()
    update_date: Mapped[datetime.date] = mapped_column(Date)
    card_id: Mapped[str] = mapped_column()
    
    name: Mapped[str] = mapped_column()
    new_value: Mapped[int] = mapped_column()
    old_value: Mapped[int] = mapped_column()
    direction: Mapped[str] = mapped_column()
    delta: Mapped[str] = mapped_column()
    color: Mapped[str] = mapped_column()

    card_update: Mapped["CardUpdate"] = relationship(
        back_populates="attribute_changes"
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["update_id", "update_date", "card_id"],
            ["card_updates.update_id", "card_updates.update_date", "card_updates.card_id"],
        ),
    )

    def __repr__(self) -> str:
        return f"ATTR_CHANGE ({self.name}: {self.delta})"

class Player(Base):
    __tablename__ = "players"
    __table_args__ = (
        Index("ix_players_last_name", "last_name"),
        Index("ix_players_last_name_lower", text("lower(last_name)")),
        Index("ix_players_last_name_no_dot_lower", text("lower(replace(last_name, '.', ''))")),
    )

    mlb_id: Mapped[int] = mapped_column(primary_key=True)
    full_name: Mapped[str] = mapped_column()
    first_name: Mapped[str] = mapped_column()
    last_name: Mapped[str] = mapped_column()
    number: Mapped[str] = mapped_column()
    birth_date: Mapped[datetime.date] = mapped_column(Date)
    current_age: Mapped[int] = mapped_column()
    birth_location_id: Mapped[Optional[int]] = mapped_column(ForeignKey("birth_locations.id"))
    height: Mapped[Optional[str]] = mapped_column()
    weight: Mapped[Optional[str]] = mapped_column()
    active: Mapped[bool] = mapped_column()
    current_team_id: Mapped[Optional[int]] = mapped_column(ForeignKey("mlb_teams.id"))
    position_id: Mapped[int] = mapped_column(ForeignKey("mlb_positions.id"))
    boxscore_name: Mapped[str] = mapped_column()
    draft_year: Mapped[Optional[int]] = mapped_column()
    mlb_debut_date: Mapped[Optional[datetime.date]] = mapped_column()
    bat_side_code: Mapped[str] = mapped_column()
    pitch_hand_code: Mapped[str] = mapped_column()
    strike_zone_top: Mapped[str] = mapped_column()
    strike_zone_bottom: Mapped[str] = mapped_column()

    cards: Mapped[List["Card"]] = relationship(back_populates="player")
    team: Mapped[Optional["MLBTeam"]] = relationship(back_populates="players")
    birth_location: Mapped[Optional["BirthLocation"]] = relationship()
    position: Mapped["MLBPosition"] = relationship()
    game_boxscores: Mapped[List["MLBGameBoxscore"]] = relationship(back_populates="player")

    def __repr__(self) -> str:
        return f"PLAYERS (mlb_id={self.mlb_id}, full_name={self.full_name})"
    
class BirthLocation(Base):
    __tablename__ = "birth_locations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    city: Mapped[str] = mapped_column()
    state_province: Mapped[Optional[str]] = mapped_column()
    country: Mapped[str] = mapped_column()

    def __repr__(self) -> str:
        return f"BIRTH_LOCATION (id={self.id}, city={self.city}, country={self.country})"
    
class MLBTeam(Base):
    __tablename__ = "mlb_teams"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    abbreviation: Mapped[str] = mapped_column()
    location_name: Mapped[str] = mapped_column()
    team_name: Mapped[str] = mapped_column()
    active: Mapped[bool] = mapped_column()

    players: Mapped[List["Player"]] = relationship(back_populates="team")

    def __repr__(self) -> str:
        return f"MLB_TEAMS (id={self.id}, city={self.name}, country={self.location_name})"

class MLBPosition(Base):
    __tablename__ = "mlb_positions"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    abbreviation: Mapped[str] = mapped_column()

    def __repr__(self) -> str:
        return f"MLB_POSITIONS (id={self.id}, city={self.name}, country={self.abbreviation})"

class Milestone(Base):
    __tablename__ = "milestones"

    id: Mapped[int] = mapped_column(primary_key=True)

    mlb_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"))
    current_value: Mapped[int] = mapped_column()
    achievement_value: Mapped[int] = mapped_column()
    type: Mapped[str] = mapped_column()
    duration: Mapped[str] = mapped_column()
    stat_desc: Mapped[str] = mapped_column()
    stat_abbrev: Mapped[str] = mapped_column()
    stat_difference: Mapped[int] = mapped_column()

    player: Mapped["Player"] = relationship()

    def __repr__(self) -> str:
        return f"MILESTONES (id={self.id}, mlb_id={self.mlb_id}, stat_abbrev={self.stat_abbrev})"
    
class MLBGame(Base):
    __tablename__ = "mlb_games"

    id: Mapped[int] = mapped_column(primary_key=True)

    game_type: Mapped[str] = mapped_column()
    season: Mapped[int] = mapped_column()
    game_date: Mapped[datetime.datetime] = mapped_column()
    status_code: Mapped[str] = mapped_column()
    home_team_id: Mapped[int] = mapped_column(ForeignKey("mlb_teams.id"))
    away_team_id: Mapped[int] = mapped_column(ForeignKey("mlb_teams.id"))

    boxscores: Mapped[List["MLBGameBoxscore"]] = relationship(back_populates="game")

    def __repr__(self) -> str:
        return f"MLB_GAMES (id={self.id}, game_date={self.game_date}, home={self.home_team_id}, away={self.away_team_id})"

class MLBGameBoxscore(Base):
    __tablename__ = "mlb_game_boxscores"

    game_id: Mapped[int] = mapped_column(ForeignKey("mlb_games.id"), primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"), primary_key=True)
    team_id: Mapped[int] = mapped_column(ForeignKey("mlb_teams.id"))

    game: Mapped["MLBGame"] = relationship(back_populates="boxscores")

    pitching_stats: Mapped[List["MLBGamePitchingStats"]] = relationship(
        back_populates="boxscore",
        cascade="all, delete-orphan"
    )
    batting_stats: Mapped[List["MLBGameBattingStats"]] = relationship(
        back_populates="boxscore",
        cascade="all, delete-orphan" 
    )
    fielding_stats: Mapped[Optional["MLBGameFieldingStats"]] = relationship(
        back_populates="boxscore",
        cascade="all, delete-orphan",
        uselist=False
    )
    baserunning_stats: Mapped[Optional["MLBGameBaserunningStats"]] = relationship(
        back_populates="boxscore",
        cascade="all, delete-orphan",
        uselist=False
    )

    player: Mapped["Player"] = relationship("Player", back_populates="game_boxscores")
    team: Mapped["MLBTeam"] = relationship("MLBTeam")

    def __repr__(self) -> str:
        return f"MLB_GAME_BOXSCORES (game_id={self.game_id}, player_id={self.player_id})"

class MLBGameBattingStats(Base):
    __tablename__ = "mlb_game_batting_stats"

    game_id: Mapped[int] = mapped_column(ForeignKey("mlb_games.id"), primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"), primary_key=True)
    split: Mapped[str] = mapped_column(primary_key=True) # vslhp, vsrhp, risp

    pa: Mapped[int] = mapped_column()
    r: Mapped[int] = mapped_column()
    h: Mapped[int] = mapped_column()
    doubles: Mapped[int] = mapped_column()
    triples: Mapped[int] = mapped_column()
    hr: Mapped[int] = mapped_column()
    hbp: Mapped[int] = mapped_column()
    tb: Mapped[int] = mapped_column()
    rbi: Mapped[int] = mapped_column()
    so: Mapped[int] = mapped_column()
    bb: Mapped[int] = mapped_column()
    intentional_walks: Mapped[int] = mapped_column()
    ab: Mapped[int] = mapped_column()
    flyOuts: Mapped[int] = mapped_column()
    groundOuts: Mapped[int] = mapped_column()
    airOuts: Mapped[int] = mapped_column()
    gidp: Mapped[int] = mapped_column()
    gitp: Mapped[int] = mapped_column()
    lob: Mapped[int] = mapped_column()
    sac_bunts: Mapped[int] = mapped_column()
    sac_flies: Mapped[int] = mapped_column()
    pop_outs: Mapped[int] = mapped_column()
    line_outs: Mapped[int] = mapped_column()

    boxscore: Mapped["MLBGameBoxscore"] = relationship(back_populates="batting_stats")

    __table_args__ = (
        ForeignKeyConstraint(
            ['game_id', 'player_id'],
            ['mlb_game_boxscores.game_id', 'mlb_game_boxscores.player_id'],
        ),
    )

    def __repr__(self) -> str:
        return f"MLB_GAME_BATTING_STATS (game_id={self.game_id}, player_id={self.player_id}, split={self.split}, pa={self.pa})"
    
class MLBGamePitchingStats(Base):
    __tablename__ = "mlb_game_pitching_stats"

    game_id: Mapped[int] = mapped_column(ForeignKey("mlb_games.id"), primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"), primary_key=True)
    split: Mapped[str] = mapped_column(primary_key=True)

    outs_pitched: Mapped[int] = mapped_column()
    ip: Mapped[float] = mapped_column()
    ab: Mapped[int] = mapped_column()
    pitches_thrown: Mapped[int] = mapped_column()
    h: Mapped[int] = mapped_column()
    doubles: Mapped[int] = mapped_column()
    triples: Mapped[int] = mapped_column()
    hr: Mapped[int] = mapped_column()
    bb: Mapped[int] = mapped_column()
    k: Mapped[int] = mapped_column()
    intentional_walks: Mapped[int] = mapped_column()
    wins: Mapped[int] = mapped_column()
    losses: Mapped[int] = mapped_column()
    saves: Mapped[int] = mapped_column()
    save_opportunities: Mapped[int] = mapped_column()
    holds: Mapped[int] = mapped_column()
    blown_saves: Mapped[int] = mapped_column()
    r: Mapped[int] = mapped_column()
    er: Mapped[int] = mapped_column()
    batters_faced: Mapped[int] = mapped_column()
    balls_thrown: Mapped[int] = mapped_column()
    strikes_thrown: Mapped[int] = mapped_column()
    balks: Mapped[int] = mapped_column()
    wild_pitches: Mapped[int] = mapped_column()
    inherited_runners: Mapped[int] = mapped_column()
    inherited_runners_scored: Mapped[int] = mapped_column()

    boxscore: Mapped["MLBGameBoxscore"] = relationship(back_populates="pitching_stats")

    __table_args__ = (
        ForeignKeyConstraint(
            ['game_id', 'player_id'],
            ['mlb_game_boxscores.game_id', 'mlb_game_boxscores.player_id'],
        ),
    )

    def __repr__(self) -> str:
        return f"MLB_GAME_PITCHING_STATS (game_id={self.game_id}, player_id={self.player_id}, split={self.split}, ip={self.ip})"

class MLBGameBaserunningStats(Base):
    __tablename__ = "mlb_game_baserunning_stats"

    game_id: Mapped[int] = mapped_column(ForeignKey("mlb_games.id"), primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"), primary_key=True)

    sb: Mapped[int] = mapped_column()
    caught_stealing: Mapped[int] = mapped_column()

    boxscore: Mapped["MLBGameBoxscore"] = relationship(back_populates="baserunning_stats")

    __table_args__ = (
        ForeignKeyConstraint(
            ['game_id', 'player_id'],
            ['mlb_game_boxscores.game_id', 'mlb_game_boxscores.player_id'],
        ),
    )

    def __repr__(self) -> str:
        return f"MLB_GAME_BASERUNNING_STATS (game_id={self.game_id}, player_id={self.player_id}, sb={self.sb})"

class MLBGameFieldingStats(Base):
    __tablename__ = "mlb_game_fielding_stats"

    game_id: Mapped[int] = mapped_column(ForeignKey("mlb_games.id"), primary_key=True)
    player_id: Mapped[int] = mapped_column(ForeignKey("players.mlb_id"), primary_key=True)

    assists: Mapped[int] = mapped_column()
    put_outs: Mapped[int] = mapped_column()
    errors: Mapped[int] = mapped_column()
    chances: Mapped[int] = mapped_column()
    passed_balls: Mapped[int] = mapped_column()
    pickoffs: Mapped[int] = mapped_column()
    stolen_bases_allowed: Mapped[int] = mapped_column()
    caught_stealing: Mapped[int] = mapped_column()

    boxscore: Mapped["MLBGameBoxscore"] = relationship(back_populates="fielding_stats")

    __table_args__ = (
        ForeignKeyConstraint(
            ['game_id', 'player_id'],
            ['mlb_game_boxscores.game_id', 'mlb_game_boxscores.player_id'],
        ),
    )

    def __repr__(self) -> str:
        return f"MLB_GAME_FIELDING_STATS (game_id={self.game_id}, player_id={self.player_id}, put_outs={self.put_outs})"
    
class Users(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    firebase_id: Mapped[str] = mapped_column(unique=True)
    email: Mapped[str] = mapped_column(unique=True)
    display_name: Mapped[str] = mapped_column()
    search_display_name: Mapped[str] = mapped_column(index=True)
    profile_img_url: Mapped[str] = mapped_column()
    portfolios: Mapped[List["Portfolio"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    card_votes: Mapped[List["CardVote"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    update_scores: Mapped[List["UserUpdateScore"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    show_profile: Mapped[Optional["ShowProfile"]] = relationship(
        back_populates="user", uselist=False, cascade="all, delete-orphan"
    )
    messages: Mapped[List["Message"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    comments: Mapped[List["Comment"]] = relationship(back_populates="user", cascade="all, delete-orphan")
    comment_likes: Mapped[List["CommentLike"]] = relationship(back_populates="user", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"USERS (id={self.id}, firebase_id={self.firebase_id}, email={self.email})"
    

def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)

class PredictionRun(Base):
    __tablename__ = "prediction_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    run_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    scope: Mapped[str] = mapped_column()
    model_version: Mapped[str] = mapped_column()
    status: Mapped[str] = mapped_column(default="success")
    notes: Mapped[Optional[str]] = mapped_column()

    card_predictions: Mapped[List["CardPrediction"]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )

    __table_args__ = (
        CheckConstraint("scope in ('standard','fielding')", name="ck_prediction_runs_scope"),
    )

class CardPrediction(Base):
    __tablename__ = "card_predictions"

    run_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("prediction_runs.id", ondelete="CASCADE"), primary_key=True
    )
    card_id: Mapped[str] = mapped_column(
        ForeignKey("cards.id", ondelete="CASCADE"), primary_key=True
    )

    predicted_ovr: Mapped[Optional[int]] = mapped_column(Integer)
    predicted_attributes: Mapped[dict] = mapped_column(JSONB, default=dict)
    predicted_rarity: Mapped[Optional[str]] = mapped_column()
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    run: Mapped["PredictionRun"] = relationship(back_populates="card_predictions")
    card: Mapped["Card"] = relationship(back_populates="predictions")

    __table_args__ = (
        Index("ix_card_predictions_card_run_desc", "card_id", "run_id"),
    )


class CardPositionOverall(Base):
    __tablename__ = "card_position_overalls"

    card_id: Mapped[str] = mapped_column(
        ForeignKey("cards.id", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[str] = mapped_column(String(8), primary_key=True)

    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    is_hitter: Mapped[bool] = mapped_column(Boolean, nullable=False)

    true_overall: Mapped[float] = mapped_column(Float, nullable=False)
    true_overall_rounded: Mapped[int] = mapped_column(Integer, nullable=False)

    meta_overall: Mapped[float] = mapped_column(Float, nullable=False)
    meta_overall_rounded: Mapped[int] = mapped_column(Integer, nullable=False)

    computed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, nullable=False, index=True
    )

    card: Mapped["Card"] = relationship(back_populates="position_overalls")

    __table_args__ = (
        Index("ix_card_position_overalls_position", "position"),
    )
    
class UpdateSchedule(Base):
    __tablename__ = "update_schedule"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    next_update_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), index=True)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

class Portfolio(Base):
    __tablename__ = "portfolios"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    name: Mapped[str] = mapped_column()
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    user: Mapped["Users"] = relationship(back_populates="portfolios")
    holdings: Mapped[List["PortfolioHolding"]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan"
    )
    transactions: Mapped[List["PortfolioTransaction"]] = relationship(
        back_populates="portfolio", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_portfolios_user_name"),
    )

class PortfolioHolding(Base):
    __tablename__ = "portfolio_holdings"

    portfolio_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("portfolios.id", ondelete="CASCADE"), primary_key=True
    )
    card_id: Mapped[str] = mapped_column(
        ForeignKey("cards.id", ondelete="CASCADE"), primary_key=True
    )

    quantity: Mapped[int] = mapped_column(Integer, default=0)
    avg_price: Mapped[Optional[int]] = mapped_column(Integer)
    user_predicted_ovr: Mapped[Optional[int]] = mapped_column(Integer)
    notes: Mapped[Optional[str]] = mapped_column()
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    portfolio: Mapped["Portfolio"] = relationship(back_populates="holdings")
    card: Mapped["Card"] = relationship(back_populates="portfolio_holdings")

    __table_args__ = (
        Index("ix_portfolio_holdings_portfolio", "portfolio_id"),
        Index("ix_portfolio_holdings_card", "card_id"),
    )

class PortfolioTransaction(Base):
    __tablename__ = "portfolio_transactions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    portfolio_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("portfolios.id", ondelete="CASCADE"), index=True
    )
    card_id: Mapped[str] = mapped_column(
        ForeignKey("cards.id", ondelete="CASCADE"), index=True
    )

    qty_delta: Mapped[int] = mapped_column(Integer)
    price: Mapped[int] = mapped_column(Integer)
    occurred_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    notes: Mapped[Optional[str]] = mapped_column()

    portfolio: Mapped["Portfolio"] = relationship(back_populates="transactions")
    card: Mapped["Card"] = relationship()

class CardVote(Base):
    __tablename__ = "card_votes"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id", ondelete="CASCADE"), index=True)

    update_id: Mapped[int] = mapped_column(Integer)
    update_date: Mapped[datetime.date] = mapped_column(Date)

    vote_value: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    user: Mapped["Users"] = relationship(back_populates="card_votes")
    card: Mapped["Card"] = relationship(back_populates="votes")
    result: Mapped[Optional["VoteResult"]] = relationship(
        back_populates="vote", uselist=False, cascade="all, delete-orphan"
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["update_id", "update_date"],
            ["roster_updates.id", "roster_updates.date"],
            name="fk_card_votes_roster_update",
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "user_id", "card_id", "update_id", "update_date", name="uq_card_votes_unique_vote"
        ),
        CheckConstraint("vote_value >= -100 AND vote_value <= 100", name="ck_card_votes_vote_value"),
        Index("ix_card_votes_update_card", "update_id", "update_date", "card_id"),
        Index("ix_card_votes_user_update", "user_id", "update_id", "update_date"),
    )

class CardVoteAggregate(Base):
    __tablename__ = "card_vote_aggregates"

    update_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    update_date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    card_id: Mapped[str] = mapped_column(
        ForeignKey("cards.id", ondelete="CASCADE"), primary_key=True
    )

    count_total: Mapped[int] = mapped_column(Integer, default=0)
    sum_votes: Mapped[int] = mapped_column(Integer, default=0)
    count_pos: Mapped[int] = mapped_column(Integer, default=0)
    count_neg: Mapped[int] = mapped_column(Integer, default=0)
    count_zero: Mapped[int] = mapped_column(Integer, default=0)

    updated_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    card: Mapped["Card"] = relationship()

    __table_args__ = (
        ForeignKeyConstraint(
            ["update_id", "update_date"],
            ["roster_updates.id", "roster_updates.date"],
            name="fk_card_vote_aggs_roster_update",
            ondelete="CASCADE",
        ),
        Index("ix_card_vote_aggs_update_card", "update_id", "update_date", "card_id"),
    )


class VoteResult(Base):
    __tablename__ = "vote_results"

    vote_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("card_votes.id", ondelete="CASCADE"), primary_key=True
    )

    did_update: Mapped[bool] = mapped_column(Boolean, default=False)
    actual_old_ovr: Mapped[Optional[int]] = mapped_column(Integer)
    actual_new_ovr: Mapped[Optional[int]] = mapped_column(Integer)
    actual_delta_ovr: Mapped[Optional[int]] = mapped_column(Integer)

    correct: Mapped[Optional[bool]] = mapped_column(Boolean)
    points_awarded: Mapped[int] = mapped_column(Integer, default=0)
    computed_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    vote: Mapped["CardVote"] = relationship(back_populates="result")


class UserUpdateScore(Base):
    __tablename__ = "user_update_scores"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    update_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    update_date: Mapped[datetime.date] = mapped_column(Date, primary_key=True)

    votes_count: Mapped[int] = mapped_column(Integer, default=0)
    correct_count: Mapped[int] = mapped_column(Integer, default=0)
    points_total: Mapped[int] = mapped_column(Integer, default=0)
    computed_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    user: Mapped["Users"] = relationship(back_populates="update_scores")

    __table_args__ = (
        ForeignKeyConstraint(
            ["update_id", "update_date"],
            ["roster_updates.id", "roster_updates.date"],
            name="fk_user_update_scores_roster_update",
            ondelete="CASCADE",
        ),
        Index("ix_user_update_scores_user_recent", "user_id", "computed_at"),
    )


class ShowProfile(Base):
    __tablename__ = "show_profiles"

    username: Mapped[str] = mapped_column(index=True, primary_key=True)

    user_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
        index=True,
    )

    display_level: Mapped[Optional[int]] = mapped_column(Integer)
    games_played: Mapped[Optional[int]] = mapped_column(Integer)
    nameplate_equipped: Mapped[Optional[str]] = mapped_column()
    icon_equipped: Mapped[Optional[str]] = mapped_column()

    raw_json: Mapped[dict] = mapped_column(JSONB, default=dict)

    first_seen_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        nullable=False,
    )
    claimed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_refreshed_at: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        default=_utcnow,
        nullable=False,
    )

    user: Mapped["Users | None"] = relationship(back_populates="show_profile")
    online_stats: Mapped[List["ShowProfileOnlineStats"]] = relationship(
        back_populates="profile", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index(
            "uq_show_profiles_user_id_not_null",
            "user_id",
            unique=True,
            postgresql_where=text("user_id IS NOT NULL"),
        ),
    )


class ShowProfileOnlineStats(Base):
    __tablename__ = "show_profile_online_stats"

    profile_username: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_profiles.username", ondelete="CASCADE"),
        primary_key=True,
    )
    year: Mapped[int] = mapped_column(Integer, primary_key=True)

    wins: Mapped[Optional[int]] = mapped_column(Integer)
    losses: Mapped[Optional[int]] = mapped_column(Integer)
    hr: Mapped[Optional[int]] = mapped_column(Integer)
    runs_per_game: Mapped[Optional[float]] = mapped_column(Float)
    stolen_bases: Mapped[Optional[int]] = mapped_column(Integer)
    batting_average: Mapped[Optional[float]] = mapped_column(Float)
    era: Mapped[Optional[float]] = mapped_column(Float)
    k_per_9: Mapped[Optional[float]] = mapped_column(Float)
    whip: Mapped[Optional[float]] = mapped_column(Float)

    profile: Mapped["ShowProfile"] = relationship(back_populates="online_stats")

message_likes = Table(
    "message_likes",
    Base.metadata,
    Column("user_id", ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
    Column("message_id", ForeignKey("messages.id", ondelete="CASCADE"), primary_key=True),
)

class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    content: Mapped[str] = mapped_column()
    timestamp: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    user: Mapped["Users"] = relationship(back_populates="messages")

    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("messages.id", ondelete="SET NULL"), nullable=True)
    parent: Mapped[Optional["Message"]] = relationship(remote_side=[id])
    edited_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    liked_by_users: Mapped[List["Users"]] = relationship(
        secondary=message_likes, backref="liked_messages"
    )

    def __repr__(self) -> str:
        return f"MESSAGE (id={self.id}, user={self.user_id})"

class ShowGameSummary(Base):
    __tablename__ = "show_game_summary"

    id: Mapped[str] = mapped_column(String, primary_key=True)

    home_profile_username: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("show_profiles.username", ondelete="RESTRICT"),
        index=True,
        nullable=False,
    )
    away_profile_username: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("show_profiles.username", ondelete="RESTRICT"),
        index=True,
        nullable=False,
    )

    home_name: Mapped[str] = mapped_column(String(128), nullable=False)
    away_name: Mapped[str] = mapped_column(String(128), nullable=False)

    home_full_name: Mapped[str] = mapped_column(String(128), nullable=False)
    away_full_name: Mapped[str] = mapped_column(String(128), nullable=False)

    home_result: Mapped[str] = mapped_column(String(1), nullable=False)
    away_result: Mapped[str] = mapped_column(String(1), nullable=False)

    home_runs: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    away_runs: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    home_hits: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    away_hits: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    home_errors: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    away_errors: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    innings: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    date: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), index=True, nullable=False)

    ball_park_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("show_ball_parks.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    difficulty: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    is_online: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    weather_degrees: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    weather_description: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    weather_wind: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    home_profile: Mapped["ShowProfile"] = relationship(
        "ShowProfile",
        foreign_keys=[home_profile_username],
    )
    away_profile: Mapped["ShowProfile"] = relationship(
        "ShowProfile",
        foreign_keys=[away_profile_username],
    )
    ball_park: Mapped[Optional["ShowBallParks"]] = relationship()

    def __repr__(self) -> str:
        return f"SHOW_GAME_SUMMARY (id={self.id}, home={self.home_full_name}, away={self.away_full_name}, date={self.date})"
    
class ShowBallParks(Base):
    __tablename__ = "show_ball_parks"
    __table_args__ = (
        UniqueConstraint("name", name="uq_show_ball_parks_name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    elevation: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"SHOW_BALL_PARKS (id={self.id}, name={self.name})"
    
class ShowGameHalfInning(Base):
    __tablename__ = "show_game_half_innings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    game_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_game_summary.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    inning: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    is_home_batting: Mapped[bool] = mapped_column(Boolean, nullable=False)

    runs: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    hits: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    walks: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    errors: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    pitches: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    runners_left_on: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    __table_args__ = (
        UniqueConstraint("game_id", "inning", "is_home_batting", name="uq_half_inning_game_inning_side"),
        Index("ix_half_inning_game_inning", "game_id", "inning"),
    )
    
class ShowBatterBoxscore(Base):
    __tablename__ = "show_batter_boxscore"

    game_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_game_summary.id", ondelete="CASCADE"),
        primary_key=True,
    )
    is_home: Mapped[bool] = mapped_column(Boolean, primary_key=True)

    appearance_idx: Mapped[int] = mapped_column(Integer, primary_key=True)
    replaced_apperance_idx: Mapped[Optional[int]] = mapped_column(Integer)

    player_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    ab: Mapped[int] = mapped_column(Integer)
    h: Mapped[int] = mapped_column(Integer)
    r: Mapped[int] = mapped_column(Integer)
    rbi: Mapped[int] = mapped_column(Integer)
    bb: Mapped[int] = mapped_column(Integer)
    so: Mapped[int] = mapped_column(Integer)
    doubles: Mapped[int] = mapped_column(Integer)
    triples: Mapped[int] = mapped_column(Integer)
    hr: Mapped[int] = mapped_column(Integer)
    sh: Mapped[int] = mapped_column(Integer)
    sf: Mapped[int] = mapped_column(Integer)
    gidp: Mapped[int] = mapped_column(Integer)
    e: Mapped[int] = mapped_column(Integer)
    pb: Mapped[int] = mapped_column(Integer)
    hbp: Mapped[int] = mapped_column(Integer)
    sb: Mapped[int] = mapped_column(Integer)
    cs: Mapped[int] = mapped_column(Integer)

    innings: Mapped[int] = mapped_column(Integer)
    pos: Mapped[int] = mapped_column(Integer)

    __table_args__ = (
        Index("ix_show_batter_boxscore_game_side", "game_id", "is_home"),
        Index("ix_show_batter_boxscore_mlb_id", "mlb_id"),
    )

class ShowBatterBoxscoreCardCandidate(Base):
    __tablename__ = "show_batter_boxscore_card_candidates"

    game_id: Mapped[str] = mapped_column(String, primary_key=True)
    is_home: Mapped[bool] = mapped_column(Boolean, primary_key=True)
    appearance_idx: Mapped[int] = mapped_column(Integer, primary_key=True)

    card_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("cards.id", ondelete="CASCADE"),
        primary_key=True,
    )

    score: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        ForeignKeyConstraint(
            ["game_id", "is_home", "appearance_idx"],
            ["show_batter_boxscore.game_id", "show_batter_boxscore.is_home", "show_batter_boxscore.appearance_idx"],
            ondelete="CASCADE",
        ),
        Index("ix_show_batter_boxscore_candidates_card", "card_id"),
    )


class ShowPitcherBoxscore(Base):
    __tablename__ = "show_pitcher_boxscore"

    game_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_game_summary.id", ondelete="CASCADE"),
        primary_key=True,
    )
    is_home: Mapped[bool] = mapped_column(Boolean, primary_key=True)

    appearance_idx: Mapped[int] = mapped_column(SmallInteger, primary_key=True)

    player_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    ip_raw: Mapped[str] = mapped_column(String(8), nullable=False)
    outs_pitched: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    r: Mapped[int] = mapped_column(Integer)
    h: Mapped[int] = mapped_column(Integer)
    er: Mapped[int] = mapped_column(Integer)
    bb: Mapped[int] = mapped_column(Integer)
    so: Mapped[int] = mapped_column(Integer)
    era: Mapped[Optional[float]] = mapped_column(Float)

    wp: Mapped[int] = mapped_column(Integer)
    win: Mapped[int] = mapped_column(Integer)
    loss: Mapped[int] = mapped_column(Integer)
    save: Mapped[int] = mapped_column(Integer)
    b_save: Mapped[int] = mapped_column(Integer)
    hold: Mapped[int] = mapped_column(Integer)

    s_wins: Mapped[int] = mapped_column(Integer)
    s_losses: Mapped[int] = mapped_column(Integer)
    s_saves: Mapped[int] = mapped_column(Integer)
    s_b_saves: Mapped[int] = mapped_column(Integer)
    s_holds: Mapped[int] = mapped_column(Integer)

    __table_args__ = (
        Index("ix_show_pitcher_boxscore_game_side", "game_id", "is_home"),
        Index("ix_show_pitcher_boxscore_mlb_id", "mlb_id"),
    )

class ShowPitcherBoxscoreCardCandidate(Base):
    __tablename__ = "show_pitcher_boxscore_card_candidates"

    game_id: Mapped[str] = mapped_column(String, primary_key=True)
    is_home: Mapped[bool] = mapped_column(Boolean, primary_key=True)
    appearance_idx: Mapped[int] = mapped_column(SmallInteger, primary_key=True)

    card_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("cards.id", ondelete="CASCADE"),
        primary_key=True,
    )

    score: Mapped[Optional[float]] = mapped_column(Float)

    __table_args__ = (
        ForeignKeyConstraint(
            ["game_id", "is_home", "appearance_idx"],
            ["show_pitcher_boxscore.game_id", "show_pitcher_boxscore.is_home", "show_pitcher_boxscore.appearance_idx"],
            ondelete="CASCADE",
        ),
        Index("ix_show_pitcher_boxscore_candidates_card", "card_id"),
    )

class ShowGameEvent(Base):
    __tablename__ = "show_game_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    game_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_game_summary.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    half_inning_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("show_game_half_innings.id", ondelete="CASCADE"),
        index=True,
        nullable=True,
    )

    seq: Mapped[int] = mapped_column(Integer, nullable=False)  # strict order in entire game
    inning: Mapped[Optional[int]] = mapped_column(SmallInteger, nullable=True)
    is_home_batting: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Snapshots (so you don't need to replay to answer basic questions)
    outs_before: Mapped[Optional[int]] = mapped_column(SmallInteger)
    outs_after: Mapped[Optional[int]] = mapped_column(SmallInteger)

    home_score_before: Mapped[Optional[int]] = mapped_column(SmallInteger)
    away_score_before: Mapped[Optional[int]] = mapped_column(SmallInteger)
    home_score_after: Mapped[Optional[int]] = mapped_column(SmallInteger)
    away_score_after: Mapped[Optional[int]] = mapped_column(SmallInteger)

    # Base occupancy snapshots (pre/post)
    pre_on_1b: Mapped[Optional[bool]] = mapped_column(Boolean)
    pre_on_2b: Mapped[Optional[bool]] = mapped_column(Boolean)
    pre_on_3b: Mapped[Optional[bool]] = mapped_column(Boolean)
    post_on_1b: Mapped[Optional[bool]] = mapped_column(Boolean)
    post_on_2b: Mapped[Optional[bool]] = mapped_column(Boolean)
    post_on_3b: Mapped[Optional[bool]] = mapped_column(Boolean)

    # What is this event?
    event_type: Mapped[str] = mapped_column(String(24), nullable=False)  
    # e.g. "pa", "pitching_change", "pinch_hit", "substitution", "balk", "wild_pitch", "passed_ball", "runner_out", "pickoff", etc.

    # Always store the raw snippet that produced the event (replay/debug)
    event_text: Mapped[str] = mapped_column(Text, nullable=False)

    # Color-code markers / context flags
    is_critical_play: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_critical_situation: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_go_ahead_play: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_simulated_play: Mapped[Optional[bool]] = mapped_column(Boolean)

    event_seq_in_half: Mapped[Optional[int]] = mapped_column(SmallInteger)

    parser_version: Mapped[Optional[str]] = mapped_column(String(32))

    __table_args__ = (
        UniqueConstraint("game_id", "seq", name="uq_show_game_events_game_seq"),
        Index("ix_show_game_events_half_seq", "half_inning_id", "seq"),
        Index("ix_show_game_events_type", "game_id", "event_type"),
    )

class ShowGamePlateAppearance(Base):
    __tablename__ = "show_game_plate_appearances"

    event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("show_game_events.id", ondelete="CASCADE"),
        primary_key=True,
    )

    batter_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    pitcher_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)

    # If you can resolve to your Player table, store mlb_id here
    batter_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    pitcher_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    # Outcome basics
    result: Mapped[Optional[str]] = mapped_column(String(32))          # single/double/hr/so/flyout/etc
    batted_ball_type: Mapped[Optional[str]] = mapped_column(String(32))  # ground/fly/line/popup
    fielder_pos: Mapped[Optional[str]] = mapped_column(String(8))      # CF/SS/1B etc
    putout_code: Mapped[Optional[str]] = mapped_column(String(32))     # F7, 6-3, SF8, 6-4-3 DP, etc

    is_out: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_double_play: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_sac_fly: Mapped[Optional[bool]] = mapped_column(Boolean)
    is_sac_bunt: Mapped[Optional[bool]] = mapped_column(Boolean)

    runs_scored: Mapped[Optional[int]] = mapped_column(SmallInteger)
    rbi: Mapped[Optional[int]] = mapped_column(SmallInteger)

    # HR / contact quality
    hr_distance_ft: Mapped[Optional[int]] = mapped_column(SmallInteger)
    is_perfect_perfect: Mapped[Optional[bool]] = mapped_column(Boolean)
    exit_vel_mph: Mapped[Optional[int]] = mapped_column(SmallInteger)

    # Strikeout extras (only when result is some kind of K)
    is_strikeout: Mapped[Optional[bool]] = mapped_column(Boolean)
    k_pitch_type: Mapped[Optional[str]] = mapped_column(String(32))
    k_loc_height: Mapped[Optional[str]] = mapped_column(String(8))     # high/middle/low
    k_loc_width: Mapped[Optional[str]] = mapped_column(String(8))      # inside/center/outside or left/center/right
    k_is_chase: Mapped[Optional[bool]] = mapped_column(Boolean)
    k_is_looking: Mapped[Optional[bool]] = mapped_column(Boolean)
    k_timing: Mapped[Optional[str]] = mapped_column(String(32))        # early/late

    batter_side: Mapped[Optional[str]] = mapped_column(String(8))      # L/R
    pitcher_throws: Mapped[Optional[str]] = mapped_column(String(8))   # L/R

    is_scoring_play: Mapped[Optional[bool]] = mapped_column(Boolean)
    hit_direction: Mapped[Optional[str]] = mapped_column(String(32))
    is_error: Mapped[Optional[bool]] = mapped_column(Boolean)
    error_pos: Mapped[Optional[str]] = mapped_column(String(32))

    event: Mapped["ShowGameEvent"] = relationship()

class ShowEventCardCandidate(Base):
    __tablename__ = "show_event_card_candidates"

    event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("show_game_events.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role: Mapped[str] = mapped_column(String(8), primary_key=True)  # "batter" | "pitcher" | "runner"
    card_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("cards.id", ondelete="CASCADE"),
        primary_key=True,
    )

    score: Mapped[Optional[float]] = mapped_column(Float)  # optional ranking/confidence

class ShowEventRunnerMove(Base):
    __tablename__ = "show_event_runner_moves"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("show_game_events.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    runner_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    runner_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    from_base: Mapped[Optional[int]] = mapped_column(SmallInteger)  # 0=batter, 1/2/3
    to_base: Mapped[Optional[int]] = mapped_column(SmallInteger)    # 1/2/3/4(home), -1=out

    move_type: Mapped[str] = mapped_column(String(24), nullable=False)
    # "advance", "score", "out", "stolen_base", "caught_stealing", "pickoff",
    # "extra_bases_out", "wild_pitch", "passed_ball", etc.

    note: Mapped[Optional[str]] = mapped_column(String(128))

class ShowGamePitchingChange(Base):
    __tablename__ = "show_game_pitching_changes"

    event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("show_game_events.id", ondelete="CASCADE"),
        primary_key=True,
    )

    pitcher_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    pitcher_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    replaced_pitcher_name_raw: Mapped[Optional[str]] = mapped_column(String(128))

class ShowGameSubstitution(Base):
    __tablename__ = "show_game_substitutions"

    event_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("show_game_events.id", ondelete="CASCADE"),
        primary_key=True,
    )

    sub_type: Mapped[str] = mapped_column(String(24), nullable=False)   # "pinch_hit", "defensive", etc (start with pinch_hit)

    incoming_player_name_raw: Mapped[str] = mapped_column(String(128), nullable=False)
    outgoing_player_name_raw: Mapped[Optional[str]] = mapped_column(String(128))

    incoming_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)
    outgoing_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

class ShowGamePitcherGameScore(Base):
    __tablename__ = "show_game_pitcher_game_scores"

    game_id: Mapped[str] = mapped_column(
        String,
        ForeignKey("show_game_summary.id", ondelete="CASCADE"),
        primary_key=True,
    )
    pitcher_name_raw: Mapped[str] = mapped_column(String(128), primary_key=True)
    is_home: Mapped[bool] = mapped_column(Boolean, primary_key=True)
    game_score: Mapped[int] = mapped_column(Integer, nullable=False)
    pitcher_mlb_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

class UserPrediction(Base):
    __tablename__ = "user_predictions"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), primary_key=True)
    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id"), primary_key=True)
    predicted_ovr: Mapped[int] = mapped_column(Integer) 

    def __repr__(self) -> str:
        return f"USER_PREDICTION (user_id={self.user_id}, card_id={self.card_id}, predicted_ovr={self.predicted_ovr})"
    

class Comment(Base):
    __tablename__ = "comments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    updated_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), onupdate=_utcnow)
    edited_at: Mapped[Optional[datetime.datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    content: Mapped[str] = mapped_column(Text)
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    user: Mapped["Users"] = relationship(back_populates="comments")
    
    card_id: Mapped[str] = mapped_column(ForeignKey("cards.id", ondelete="CASCADE"), index=True)
    card: Mapped["Card"] = relationship(back_populates="comments")
    
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("comments.id", ondelete="CASCADE"), nullable=True, index=True)
    replicas: Mapped[List["Comment"]] = relationship(back_populates="parent", cascade="all, delete-orphan")
    parent: Mapped[Optional["Comment"]] = relationship(back_populates="replicas", remote_side=[id])

    likes: Mapped[List["CommentLike"]] = relationship(back_populates="comment", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"COMMENT(id={self.id}, user_id={self.user_id}, card_id={self.card_id})"

class CommentLike(Base):
    __tablename__ = "comment_likes"

    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    comment_id: Mapped[int] = mapped_column(ForeignKey("comments.id", ondelete="CASCADE"), primary_key=True)
    created_at: Mapped[datetime.datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    user: Mapped["Users"] = relationship(back_populates="comment_likes")
    comment: Mapped["Comment"] = relationship(back_populates="likes")
        
