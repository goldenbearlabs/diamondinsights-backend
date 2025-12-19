from typing import List, Optional
import datetime
from sqlalchemy import Column, ForeignKey, Table, Date, ForeignKeyConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.database.database import Base

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

    id: Mapped[str] = mapped_column(primary_key=True)
    type: Mapped[str] = mapped_column()
    year: Mapped[int] = mapped_column()
    img: Mapped[str] = mapped_column()
    baked_img: Mapped[Optional[str]] = mapped_column()
    name: Mapped[str] = mapped_column(index=True)
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