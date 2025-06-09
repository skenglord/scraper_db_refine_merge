import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry # Corrected import path

class RequestsFetcherMBH:
    def __init__(self):
        self.session = None
        self._setup_session()

    def _setup_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/115.0.0.0 Safari/537.36"
                )
            }
        )
        return session

    def fetch_page(self, url: str) -> str | None:
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - network errors
            print(f"Error fetching {url}: {exc}", file=sys.stderr)
            return None
