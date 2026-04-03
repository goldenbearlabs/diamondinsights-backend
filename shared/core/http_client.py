import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import random
import time
from typing import Any, Dict, Optional

class APIClient:
    def __init__(self, 
                base_url: str = "",
                backoff: float = 0.5,
                rate_limit_retries: int = 12,
                rate_limit_cap_s: float = 30.0):
        
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.rate_limit_retries = rate_limit_retries
        self.rate_limit_cap_s = rate_limit_cap_s
        self.rate_limit_backoff = backoff
        
        self.session = requests.Session()

        retry_strategy = Retry(
            total=0,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=20, pool_maxsize=20)
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        headers: Optional[Dict[str, str]] = None,
        retry_statuses: Optional[set[int]] = None,
        retry_count: Optional[int] = None,
        return_none_on_statuses: Optional[set[int]] = None,
    ):
        url = f"{self.base_url}{endpoint}"
        retry_on = retry_statuses or {429}
        return_none_on = return_none_on_statuses or set()
        max_retries = self.rate_limit_retries if retry_count is None else max(0, retry_count)

        last_response: Optional[requests.Response] = None
        for attempt in range(max_retries + 1):
            response = self.session.get(url, params=params, headers=headers, timeout=40)
            last_response = response

            if response.status_code in retry_on:
                if attempt >= max_retries:
                    break

                retry_after = response.headers.get("Retry-After")
                retry_after_s = None
                if retry_after:
                    try:
                        retry_after_s = float(retry_after)
                    except ValueError:
                        retry_after_s = None

                delay = min(self.rate_limit_cap_s, self.rate_limit_backoff * (2 ** attempt))
                delay *= random.uniform(0.85, 1.15)
                if retry_after_s is not None:
                    delay = max(delay, retry_after_s + random.uniform(0.0, 1.0))

                self.logger.warning(
                    f"{response.status_code} for {url}. Sleeping {delay:.2f}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(delay)
                continue

            if response.status_code in return_none_on:
                self.logger.warning(f"HTTP {response.status_code} for {url}. Returning None.")
                return None

            try:
                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError:
                self.logger.error(f"HTTP Error: {response.status_code} for {url}")
                raise
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Network Error: {e} for {url}")
                raise

        if last_response is not None:
            if last_response.status_code in return_none_on:
                self.logger.warning(
                    f"HTTP {last_response.status_code} for {url} (max retries exceeded). Returning None."
                )
                return None
            self.logger.error(f"HTTP Error: {last_response.status_code} for {url} (max retries exceeded)")
            last_response.raise_for_status()

        raise RuntimeError("unreachable")
            
    def close(self):
        self.session.close()
