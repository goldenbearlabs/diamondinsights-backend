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
                retries: int = 3, 
                backoff: float = 0.5,
                rate_limit_retries: int = 7,
                rate_limit_cap_s: float = 30.0):
        
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        self.rate_limit_retries = rate_limit_retries
        self.rate_limit_cap_s = rate_limit_cap_s
        self.rate_limit_backoff = backoff
        
        self.session = requests.Session()

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(("HEAD", "GET", "OPTIONS")),
            respect_retry_after_header=True,
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None):
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.rate_limit_retries + 1):
            response = self.session.get(url, params=params, timeout=40)

            if response.status_code != 429:
                try:
                    response.raise_for_status()
                    return response.json()
                except requests.exceptions.HTTPError as e:
                    self.logger.error(f"HTTP Error: {response.status_code} for {url}")
                    raise
                except requests.exceptions.RequestException as e:
                    self.logger.error(f"Network Error: {e} for {url}")
                    raise

            if attempt >= self.rate_limit_retries:
                self.logger.error(f"HTTP Error: 429 for {url} (max retries exceeded)")
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After")
            retry_after_s = None
            if retry_after:
                try:
                    retry_after_s = float(retry_after)
                except ValueError:
                    retry_after_s = None

            delay = min(self.rate_limit_cap_s, self.rate_limit_backoff * (2 ** attempt))
            if retry_after_s is not None:
                delay = max(delay, retry_after_s)

            delay *= random.uniform(0.85, 1.15)
            self.logger.warning(f"429 for {url}. Sleeping {delay:.2f}s (attempt {attempt + 1}/{self.rate_limit_retries})")
            time.sleep(delay)

        raise RuntimeError("unreachable")
            
    def close(self):
        self.session.close()