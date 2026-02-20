import logging
import pytest

pytest.importorskip("firebase_admin")

import shared.core.firebase_admin as firebase_admin_module

def test_returns_existing_app(monkeypatch):
    existing_app = object()

    monkeypatch.setattr(
        firebase_admin_module.firebase_admin,
        "get_app",
        lambda: existing_app
    )

    def _should_not_be_called(*args, **kwargs):
        raise AssertionError("unexpected call")
    
    monkeypatch.setattr(firebase_admin_module.credentials, "Certificate", _should_not_be_called)
    monkeypatch.setattr(firebase_admin_module.firebase_admin, "initialize_app", _should_not_be_called)

    assert firebase_admin_module.init_firebase_admin() is existing_app


def test_initializes_when_missing(monkeypatch):
    def _raise_value_error():
        raise ValueError("no default app")

    monkeypatch.setattr(firebase_admin_module.firebase_admin, "get_app", _raise_value_error)

    seen = {}

    def _fake_certificate(path):
        seen["path"] = path
        return object()

    new_app = object()

    monkeypatch.setattr(firebase_admin_module.credentials, "Certificate", _fake_certificate)
    monkeypatch.setattr(firebase_admin_module.firebase_admin, "initialize_app", lambda cred: new_app)

    assert firebase_admin_module.init_firebase_admin() is new_app
    assert seen["path"] == "/run/secrets/firebase_service_account.json"


def test_logs_and_raises_if_certificate_ioerror(monkeypatch, caplog):
    def _raise_value_error():
        raise ValueError("no default app")

    monkeypatch.setattr(firebase_admin_module.firebase_admin, "get_app", _raise_value_error)

    def _raise_ioerror(path):
        raise FileNotFoundError(path)

    called = {"init": 0}

    def _init_should_not_run(_cred):
        called["init"] += 1
        return object()

    monkeypatch.setattr(firebase_admin_module.credentials, "Certificate", _raise_ioerror)
    monkeypatch.setattr(firebase_admin_module.firebase_admin, "initialize_app", _init_should_not_run)

    caplog.set_level(logging.ERROR)

    with pytest.raises(FileNotFoundError):
        firebase_admin_module.init_firebase_admin()

    assert called["init"] == 0
    assert any("Missing FIREBASE_SERVICE_ACCOUNT_PATH" in r.message for r in caplog.records)


def test_propagates_certificate_value_error(monkeypatch):
    def _raise_value_error():
        raise ValueError("no default app")

    monkeypatch.setattr(firebase_admin_module.firebase_admin, "get_app", _raise_value_error)
    monkeypatch.setattr(firebase_admin_module.credentials, "Certificate", lambda _path: (_ for _ in ()).throw(ValueError("bad cert")))

    with pytest.raises(ValueError):
        firebase_admin_module.init_firebase_admin()


def test_propagates_initialize_app_value_error(monkeypatch):
    def _raise_value_error():
        raise ValueError("no default app")

    monkeypatch.setattr(firebase_admin_module.firebase_admin, "get_app", _raise_value_error)
    monkeypatch.setattr(firebase_admin_module.credentials, "Certificate", lambda _path: object())
    monkeypatch.setattr(firebase_admin_module.firebase_admin, "initialize_app", lambda _cred: (_ for _ in ()).throw(ValueError("init failed")))

    with pytest.raises(ValueError):
        firebase_admin_module.init_firebase_admin()