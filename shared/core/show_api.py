from __future__ import annotations

import os
from typing import Any

from shared.core.config import CURRENT_SHOW_YEAR


SHOW_SEARCH_URL_ENV = "SHOW_SEARCH_URL"
SHOW_SEARCH_PROXY_URL_ENV = "SHOW_SEARCH_PROXY_URL"
SHOW_PROXY_SHARED_SECRET_ENV = "SHOW_PROXY_SHARED_SECRET"
SHOW_PROXY_SECRET_HEADER = "x-show-proxy-secret"


def default_show_search_url() -> str:
    return f"https://mlb{CURRENT_SHOW_YEAR}.theshow.com/apis/player_search.json"


def show_search_url() -> str:
    configured = (os.getenv(SHOW_SEARCH_URL_ENV) or "").strip()
    return configured or default_show_search_url()


def build_show_search_request(username: str) -> tuple[str, dict[str, Any], dict[str, str]]:
    proxy_url = (os.getenv(SHOW_SEARCH_PROXY_URL_ENV) or "").strip()
    params: dict[str, Any] = {"username": username}
    headers: dict[str, str] = {}

    if proxy_url:
        secret = (os.getenv(SHOW_PROXY_SHARED_SECRET_ENV) or "").strip()
        if secret:
            headers[SHOW_PROXY_SECRET_HEADER] = secret
        return proxy_url, params, headers

    return show_search_url(), params, headers
