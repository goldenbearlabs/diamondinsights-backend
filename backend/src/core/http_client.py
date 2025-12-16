import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

class APIClient:
    def __init__(self, base_url: str = "", retries: int = 3, backoff: float = 0.5):
        self.base_url = base_url
        self.logger = logging.getLogger(__name__)
        
        self.session = requests.Session()

        retry_strategy = Retry(
            total=retries,
            backoff_factor=backoff,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, endpoint: str, params: dict = None):
        """Wrapper for GET requests with timeout and error handling."""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, params=params, timeout=25)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP Error: {e.response.status_code} for {url}")
            raise e 
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network Error: {e} for {url}")
            raise e
            
    def close(self):
        self.session.close()