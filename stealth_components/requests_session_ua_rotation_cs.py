import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    # Adding a few more from common lists to increase variety
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

class RequestsSessionManagerCS:
    """
    Manages a requests.Session with User-Agent rotation capabilities.
    """
    def __init__(self, user_agents: list[str] | None = None, default_retry_total: int = 3, default_backoff_factor: float = 1.0):
        self.user_agents = user_agents if user_agents else MODERN_USER_AGENTS
        if not self.user_agents: # Fallback if provided list was also empty
            self.user_agents = ["Mozilla/5.0 (compatible; DefaultScraper/1.0; +http://example.com/bot)"] # Absolute fallback

        self.current_user_agent: str = random.choice(self.user_agents)
        self.retry_total = default_retry_total
        self.backoff_factor = default_backoff_factor
        self.session: requests.Session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Creates a new requests.Session with the current User-Agent and retry settings.
        Adapted from classy_skkkrapey.BaseEventScraper._create_session.
        """
        session = requests.Session()
        session.headers.update({"User-Agent": self.current_user_agent})

        retries = Retry(
            total=self.retry_total,
            backoff_factor=self.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504], # Common server errors and rate limiting
            allowed_methods=["GET", "POST"] # Allow for more versatile use
        )

        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Common headers to make requests look more like a browser
        session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1", # Do Not Track
        })
        return session

    def rotate_user_agent(self) -> str:
        """
        Selects a new random User-Agent from the list and re-initializes the session
        with the new User-Agent.
        Adapted from classy_skkkrapey.BaseEventScraper.rotate_user_agent.
        Returns the new User-Agent string.
        """
        if not self.user_agents:
            # This should ideally not happen if __init__ has a fallback
            # print("Warning: No user agents available for rotation.", file=sys.stderr)
            return self.current_user_agent # No change

        new_ua = random.choice(self.user_agents)
        # Ensure a different UA is chosen if possible (for lists with more than one UA)
        if len(self.user_agents) > 1:
            while new_ua == self.current_user_agent:
                new_ua = random.choice(self.user_agents)

        self.current_user_agent = new_ua
        self.session = self._create_session() # Re-create session with new UA and headers
        # print(f"[INFO] Rotated User-Agent to: {self.current_user_agent}") # Optional logging
        return self.current_user_agent

    def get_session(self) -> requests.Session:
        """Returns the current session."""
        return self.session

# Example Usage (optional, can be removed or kept for testing)
if __name__ == "__main__": # pragma: no cover
    manager = RequestsSessionManagerCS()
    print(f"Initial User-Agent: {manager.current_user_agent}")

    try:
        response = manager.get_session().get("https://httpbin.org/user-agent")
        response.raise_for_status()
        print(f"Response with initial UA: {response.json()}")
    except requests.RequestException as e:
        print(f"Error during initial request: {e}")

    manager.rotate_user_agent()
    print(f"Rotated User-Agent: {manager.current_user_agent}")

    try:
        response = manager.get_session().get("https://httpbin.org/user-agent")
        response.raise_for_status()
        print(f"Response with rotated UA: {response.json()}")
    except requests.RequestException as e:
        print(f"Error during rotated request: {e}")

    # Test with custom UAs and retry settings
    custom_uas = ["MyCustomUA/1.0", "AnotherCustomUA/2.0"]
    custom_manager = RequestsSessionManagerCS(user_agents=custom_uas, default_retry_total=1)
    print(f"Custom Initial User-Agent: {custom_manager.current_user_agent}")
    custom_manager.rotate_user_agent()
    print(f"Custom Rotated User-Agent: {custom_manager.current_user_agent}")
    assert custom_manager.current_user_agent in custom_uas
    assert custom_manager.session.adapters['https://'].max_retries.total == 1
    print("Custom manager test passed.")
