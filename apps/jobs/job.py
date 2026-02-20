from abc import ABC, abstractmethod
import logging
from typing import Any, List

from sqlalchemy.orm import Session

from shared.core.http_client import APIClient

class Job(ABC):

    def __init__(self):
        self._api_client = APIClient("")
        self.logger = logging.getLogger(f"{self.__class__.__module__}.{self.__class__.__name__}")

    @abstractmethod
    def run(self, db_session: Session):
        raise NotImplementedError("Job.run must be implemented")

    def _log(self, level: int, message: str, **context) -> None:
        if context:
            ctx = " ".join(f"{k}={v}" for k, v in context.items())
            message = f"{message} {ctx}"
        self.logger.log(level, message)

    def _log_start(self, **context) -> None:
        self._log(logging.INFO, "job start", **context)

    def _log_end(self, **context) -> None:
        self._log(logging.INFO, "job complete", **context)

    def _json_get(self, json: dict, key: str, default=None) -> Any:
        if not json:
            return default
        if key not in json:
            return default
        return json[key]

    def _fetch_paginated_data(self, url: str, params: dict, items_key: str = "items") -> List:
        page = 1
        params["page"] = page
        res = self._api_client.get(url, params)

        max_pages = self._json_get(res, "total_pages", default=0)
        fetched_objects = []

        while page <= max_pages:
            items = self._json_get(res, items_key, default=[])
            fetched_objects.extend(items)

            page += 1
            if page > max_pages:
                break

            params["page"] = page
            res = self._api_client.get(url, params)

        return fetched_objects
