import logging

import pytest

import shared.core.logging_config as logging_config


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch):
    monkeypatch.setattr(logging_config, "_CURRENT_SERVICE", None)


def test_record_factory_injects_unknown_service_when_missing(monkeypatch):
    def _fake_factory(*_args, **_kwargs):
        return logging.LogRecord("test", logging.INFO, __file__, 1, "msg", (), None)

    monkeypatch.setattr(logging_config, "_BASE_FACTORY", _fake_factory)

    record = logging_config._record_factory("x", "y")

    assert record.service == "unknown"


def test_record_factory_keeps_existing_service(monkeypatch):
    def _fake_factory(*_args, **_kwargs):
        record = logging.LogRecord("test", logging.INFO, __file__, 1, "msg", (), None)
        record.service = "already-set"
        return record

    monkeypatch.setattr(logging_config, "_BASE_FACTORY", _fake_factory)
    monkeypatch.setattr(logging_config, "_CURRENT_SERVICE", "new-service")

    record = logging_config._record_factory("x", "y")

    assert record.service == "already-set"


def test_configure_logging_uses_explicit_service_level_and_force(monkeypatch):
    captured = {}

    def _fake_set_factory(factory):
        captured["factory"] = factory

    def _fake_basic_config(**kwargs):
        captured["basic_config"] = kwargs

    monkeypatch.setattr(logging_config.logging, "setLogRecordFactory", _fake_set_factory)
    monkeypatch.setattr(logging_config.logging, "basicConfig", _fake_basic_config)

    logging_config.configure_logging(service_name="jobs", level="warning", force=True)

    assert logging_config._CURRENT_SERVICE == "jobs"
    assert captured["factory"] is logging_config._record_factory
    assert captured["basic_config"]["level"] == logging.WARNING
    assert captured["basic_config"]["force"] is True
    assert "[service=%(service)s]" in captured["basic_config"]["format"]


def test_configure_logging_uses_env_level_and_no_service_format(monkeypatch):
    captured = {}

    monkeypatch.setenv("LOG_LEVEL", "error")
    monkeypatch.setattr(logging_config.logging, "setLogRecordFactory", lambda factory: captured.setdefault("factory", factory))
    monkeypatch.setattr(logging_config.logging, "basicConfig", lambda **kwargs: captured.setdefault("basic_config", kwargs))

    logging_config.configure_logging()

    assert captured["factory"] is logging_config._record_factory
    assert captured["basic_config"]["level"] == logging.ERROR
    assert captured["basic_config"]["force"] is False
    assert "[service=%(service)s]" not in captured["basic_config"]["format"]


def test_configure_logging_falls_back_to_info_and_keeps_existing_service(monkeypatch):
    captured = {}

    monkeypatch.setattr(logging_config, "_CURRENT_SERVICE", "backend")
    monkeypatch.setattr(logging_config.logging, "setLogRecordFactory", lambda factory: captured.setdefault("factory", factory))
    monkeypatch.setattr(logging_config.logging, "basicConfig", lambda **kwargs: captured.setdefault("basic_config", kwargs))

    logging_config.configure_logging(service_name=None, level="not-a-real-level")

    assert logging_config._CURRENT_SERVICE == "backend"
    assert captured["factory"] is logging_config._record_factory
    assert captured["basic_config"]["level"] == logging.INFO
    assert "[service=%(service)s]" in captured["basic_config"]["format"]
