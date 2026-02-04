import apps.jobs.card_sync as card_sync


class DummyCard:
    def __init__(self):
        self.__dict__ = {}

    def __repr__(self):
        return f"DummyCard(id={getattr(self, 'id', None)})"


class DummyPitch:
    def __init__(self, card_id, name, speed, control, movement):
        self.card_id = card_id
        self.name = name
        self.speed = speed
        self.control = control
        self.movement = movement


def test_normalize_search_removes_diacritics_and_trims():
    assert card_sync._normalize_search(" José Álvarez ") == "jose alvarez"
    assert card_sync._normalize_search(None) == ""


def test_card_adapter_json_get_defaults():
    adapter = card_sync.CardAdapter({}, {}, {})

    assert adapter._json_get(None, "x", 123) == 123
    assert adapter._json_get({}, "x", 123) == 123
    assert adapter._json_get({"x": None}, "x", 123) is None


def test_card_adapter_run_builds_cards_and_relationships(monkeypatch):
    monkeypatch.setattr(card_sync, "Card", DummyCard)
    monkeypatch.setattr(card_sync, "Pitch", DummyPitch)

    series = object()
    quirk = object()
    location = object()

    adapter = card_sync.CardAdapter(
        series_map={"Live": series},
        quirk_map={"Quirk1": quirk},
        location_map={"LOC1": location},
    )

    data = [
        {
            "source_uuid": "u1",
            "year": 2024,
            "name": "José Álvarez",
            "series": " Live ",
            "quirks": [{"name": "Quirk1"}, {"name": "Unknown"}],
            "locations": ["LOC1", "", None, "OTHER"],
            "pitches": [{"name": "FB", "speed": 99, "control": 50, "movement": 60}],
            "jersey_number": None,
        },
        {
            "uuid": "u2",
            "year": 2023,
            "name": "No Source UUID Key",
            "series": "UnknownSeries",
        },
        {
            "source_uuid": "missing-year",
            "year": 0,
        },
    ]

    cards = adapter.run(data)

    assert len(cards) == 2

    first = cards[0]
    assert first.id == "2024:u1"
    assert first.source_uuid == "u1"
    assert first.search_name == "jose alvarez"
    assert first.series is series
    assert first.series_name == "Live"
    assert first.quirks == [quirk]
    assert first.locations == [location]
    assert len(first.pitches) == 1
    assert first.pitches[0].card_id == "2024:u1"
    assert first.jersey_number == 0

    second = cards[1]
    assert second.id == "2023:u2"
    assert "series_name" not in second.__dict__
