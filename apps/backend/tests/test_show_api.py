from shared.core.show_api import (
    SHOW_PROXY_SECRET_HEADER,
    build_show_search_request,
    default_show_search_url,
)


def test_build_show_search_request_defaults_to_direct_upstream(monkeypatch) -> None:
    monkeypatch.delenv("SHOW_SEARCH_PROXY_URL", raising=False)
    monkeypatch.delenv("SHOW_PROXY_SHARED_SECRET", raising=False)
    monkeypatch.delenv("SHOW_SEARCH_URL", raising=False)

    url, params, headers = build_show_search_request("wizzy47911779")

    assert url == default_show_search_url()
    assert params == {"username": "wizzy47911779"}
    assert headers == {}


def test_build_show_search_request_uses_configured_proxy(monkeypatch) -> None:
    monkeypatch.setenv("SHOW_SEARCH_PROXY_URL", "https://diamondinsights.app/api/show-proxy/player-search")
    monkeypatch.setenv("SHOW_PROXY_SHARED_SECRET", "shared-secret")
    monkeypatch.setenv("SHOW_SEARCH_URL", "https://example.com/ignored.json")

    url, params, headers = build_show_search_request("wizzy47911779")

    assert url == "https://diamondinsights.app/api/show-proxy/player-search"
    assert params == {"username": "wizzy47911779"}
    assert headers == {SHOW_PROXY_SECRET_HEADER: "shared-secret"}


def test_build_show_search_request_omits_secret_header_when_unset(monkeypatch) -> None:
    monkeypatch.setenv("SHOW_SEARCH_PROXY_URL", "https://diamondinsights.app/api/show-proxy/player-search")
    monkeypatch.delenv("SHOW_PROXY_SHARED_SECRET", raising=False)

    _, _, headers = build_show_search_request("wizzy47911779")

    assert headers == {}
