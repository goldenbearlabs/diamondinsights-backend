import pytest

from apps.jobs.job import Job


class DummyJob(Job):
    def run(self, db_session):
        return None


class SuperRunJob(Job):
    def run(self, db_session):
        return super().run(db_session)


def test_json_get_defaults_and_missing():
    job = DummyJob()

    assert job._json_get(None, "key", "default") == "default"
    assert job._json_get({}, "key", "default") == "default"
    assert job._json_get({"other": 1}, "key", "default") == "default"
    assert job._json_get({"key": "value"}, "key", "default") == "value"


def test_json_get_returns_none_when_key_present_with_none():
    job = DummyJob()

    assert job._json_get({"key": None}, "key", "default") is None


def test_run_raises_not_implemented():
    job = SuperRunJob()

    with pytest.raises(NotImplementedError):
        job.run(db_session=None)


def test_fetch_paginated_data_zero_pages():
    job = DummyJob()
    calls = []

    def fake_get(url, params):
        calls.append((url, params.copy()))
        return {"total_pages": 0, "items": [{"id": 1}]}

    job._api_client.get = fake_get

    params = {"type": "mlb_card"}
    items = job._fetch_paginated_data("/items", params)

    assert items == []
    assert calls == [("/items", {"type": "mlb_card", "page": 1})]
    assert params["page"] == 1


def test_fetch_paginated_data_single_page():
    job = DummyJob()
    calls = []

    def fake_get(url, params):
        calls.append((url, params.copy()))
        return {"total_pages": 1, "items": [{"id": 1}, {"id": 2}]}

    job._api_client.get = fake_get

    params = {"type": "mlb_card"}
    items = job._fetch_paginated_data("/items", params)

    assert items == [{"id": 1}, {"id": 2}]
    assert calls == [("/items", {"type": "mlb_card", "page": 1})]
    assert params["page"] == 1


def test_fetch_paginated_data_multi_page():
    job = DummyJob()
    calls = []
    responses = [
        {"total_pages": 3, "items": [{"id": 1}]},
        {"total_pages": 3, "items": [{"id": 2}]},
        {"total_pages": 3, "items": [{"id": 3}]},
    ]

    def fake_get(url, params):
        calls.append((url, params.copy()))
        return responses.pop(0)

    job._api_client.get = fake_get

    params = {"type": "mlb_card"}
    items = job._fetch_paginated_data("/items", params)

    assert items == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert calls == [
        ("/items", {"type": "mlb_card", "page": 1}),
        ("/items", {"type": "mlb_card", "page": 2}),
        ("/items", {"type": "mlb_card", "page": 3}),
    ]
    assert params["page"] == 3
