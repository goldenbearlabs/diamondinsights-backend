import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
import requests
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import apps.jobs.card_sync as card_sync
from shared.db.database import Base, SQLALCHEMY_DATABASE_URL
from shared.db.models import Card, Series, Quirk, Location


def _get_test_db_url() -> str:
    return (
        os.getenv("CARD_SYNC_TEST_DATABASE_URL")
        or os.getenv("TEST_DATABASE_URL")
        or os.getenv("DATABASE_URL")
        or SQLALCHEMY_DATABASE_URL
    )


@pytest.fixture()
def db_session():
    db_url = _get_test_db_url()
    engine = create_engine(db_url, pool_pre_ping=True)
    if engine.dialect.name != "postgresql":
        pytest.skip("card_sync integration tests require a PostgreSQL database")

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.skip(f"PostgreSQL is not reachable: {exc}")

    schema = f"test_card_sync_{uuid.uuid4().hex}"
    conn = engine.connect()
    session = None
    try:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        conn.execute(text(f'SET search_path TO "{schema}"'))
        Base.metadata.create_all(bind=conn)
    except Exception as exc:
        conn.close()
        pytest.skip(f"Unable to create integration test schema: {exc}")

    try:
        Session = sessionmaker(bind=conn)
        session = Session()
        yield session
    finally:
        if session is not None:
            session.close()
        try:
            conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        except Exception:
            pass
        conn.close()


class _CardSyncHTTPServer(HTTPServer):
    def __init__(self, server_address, handler_cls, payloads):
        super().__init__(server_address, handler_cls)
        self.payloads = payloads
        self.fail_status = None


class _CardSyncHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.server.fail_status:
            self.send_response(self.server.fail_status)
            self.end_headers()
            return

        parsed = urlparse(self.path)
        if not parsed.path.endswith("/apis/items.json"):
            self.send_response(404)
            self.end_headers()
            return

        parts = parsed.path.strip("/").split("/")
        if not parts or not parts[0].startswith("mlb"):
            self.send_response(404)
            self.end_headers()
            return

        year = parts[0][3:]
        params = parse_qs(parsed.query)
        page = int(params.get("page", ["1"])[0])

        items_by_page = self.server.payloads.get(year, {})
        items = items_by_page.get(page, [])
        total_pages = max(items_by_page.keys(), default=0)

        body = json.dumps({"total_pages": total_pages, "items": items}).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args, **_kwargs):
        return


@pytest.fixture()
def show_api_server():
    payloads = {}
    server = _CardSyncHTTPServer(("127.0.0.1", 0), _CardSyncHandler, payloads)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        yield server, payloads, base_url
    finally:
        server.shutdown()
        thread.join()


def _card_item(
    uuid_value,
    name="Test Player",
    series="Live",
    ovr=90,
    quirks=None,
    locations=None,
    pitches=None,
    **overrides,
):
    item = {
        "uuid": uuid_value,
        "name": name,
        "series": series,
        "ovr": ovr,
        "type": "mlb_card",
        "team": "Team A",
        "team_short_name": "TA",
        "display_position": "P",
        "bat_hand": "R",
        "throw_hand": "R",
        "is_hitter": False,
        "quirks": quirks or [],
        "locations": locations or [],
        "pitches": pitches or [],
    }
    item.update(overrides)
    return item


def test_card_sync_integration_inserts_records(db_session, show_api_server, monkeypatch):
    server, payloads, base_url = show_api_server

    payloads["2024"] = {
        1: [
            _card_item(
                "uuid-1",
                name="José Álvarez",
                series="Live",
                ovr=91,
                quirks=[{"name": "Q1", "description": "desc", "img": "q1.png"}],
                locations=["LOC1", "LOC2"],
                pitches=[{"name": "4SFB", "speed": 99, "control": 80, "movement": 90}],
            ),
            _card_item("uuid-1", name="José Álvarez", series="Live", ovr=91),
            {"uuid": None, "name": "Missing"},
        ],
        2: [
            _card_item("uuid-2", name="Second", series="", ovr=80),
        ],
    }

    monkeypatch.setattr(card_sync, "THE_SHOW_YEARS", [2024])
    sync = card_sync.CardSync(reload_all_years=False, base_url_template=f"{base_url}/mlb{{year}}")
    sync.run(db_session)

    series = db_session.execute(select(Series)).scalars().all()
    assert {s.name for s in series} == {"UNKNOWN", "Live"}

    quirks = db_session.execute(select(Quirk)).scalars().all()
    assert {q.name for q in quirks} == {"Q1"}

    locations = db_session.execute(select(Location)).scalars().all()
    assert {l.name for l in locations} == {"LOC1", "LOC2"}

    cards = db_session.execute(select(Card)).scalars().all()
    assert len(cards) == 2

    by_uuid = {c.source_uuid: c for c in cards}
    card1 = by_uuid["uuid-1"]
    assert card1.id == "2024:uuid-1"
    assert card1.series_name == "Live"
    assert card1.search_name == "jose alvarez"

    card2 = by_uuid["uuid-2"]
    assert card2.id == "2024:uuid-2"
    assert card2.series_name == "UNKNOWN"


def test_card_sync_integration_upserts_updates(db_session, show_api_server, monkeypatch):
    _server, payloads, base_url = show_api_server

    payloads["2024"] = {
        1: [
            _card_item("uuid-1", name="Name A", series="Live", ovr=88),
        ]
    }

    monkeypatch.setattr(card_sync, "THE_SHOW_YEARS", [2024])
    sync = card_sync.CardSync(reload_all_years=False, base_url_template=f"{base_url}/mlb{{year}}")
    sync.run(db_session)

    card = db_session.execute(select(Card).where(Card.id == "2024:uuid-1")).scalar_one()
    assert card.ovr == 88

    payloads["2024"][1] = [
        _card_item("uuid-1", name="Name A", series="Live", ovr=92),
    ]

    sync.run(db_session)

    card = db_session.execute(select(Card).where(Card.id == "2024:uuid-1")).scalar_one()
    assert card.ovr == 92


def test_card_sync_integration_http_error_bubbles(db_session, show_api_server, monkeypatch):
    server, payloads, base_url = show_api_server
    payloads["2024"] = {1: [_card_item("uuid-1")]}

    monkeypatch.setattr(card_sync, "THE_SHOW_YEARS", [2024])
    sync = card_sync.CardSync(reload_all_years=False, base_url_template=f"{base_url}/mlb{{year}}")

    server.fail_status = 500
    with pytest.raises(requests.exceptions.HTTPError):
        sync.run(db_session)
