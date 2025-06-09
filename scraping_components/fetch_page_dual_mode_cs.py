import random
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup # Though not directly used in fetch, for consistency

# Playwright imports
try:
    from playwright.sync_api import sync_playwright, Page, Browser
    # Consider adding PlaywrightException if specific error handling is needed.
    # from playwright.sync_api import Error as PlaywrightError
    # from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError: # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False
    # Define dummy types for when Playwright is not available, to allow type hinting
    class Page: pass
    class Browser: pass
    class PlaywrightContextManager: # Dummy for sync_playwright()
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
        def chromium(self): return None # Dummy
    def sync_playwright(): return PlaywrightContextManager() # Dummy


MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5.2 Safari/605.1.15",
]

class DualModeFetcherCS:
    def __init__(self, use_browser_default: bool = False, headless: bool = True):
        self.use_browser_default = use_browser_default
        self.headless = headless
        self.browser: Browser | None = None
        self.playwright_context = None # Stores the Playwright manager object
        self.current_user_agent: str = random.choice(MODERN_USER_AGENTS)
        self.session: requests.Session | None = None
        self.pages_scraped_since_ua_rotation: int = 0
        self.rotate_ua_after_pages: int = random.randint(30, 70)

        self._create_session()

        if self.use_browser_default:
            if not PLAYWRIGHT_AVAILABLE:
                raise RuntimeError(
                    "Playwright is not installed, but use_browser_default=True. "
                    "Please install playwright (e.g., pip install playwright) and browser drivers (playwright install)."
                )
            try:
                self.playwright_context = sync_playwright().start()
                self.browser = self.playwright_context.chromium.launch(headless=self.headless)
            except Exception as e: # pragma: no cover
                # Fallback or error logging if browser launch fails
                print(f"Playwright browser launch failed: {e}. Consider running 'playwright install'.")
                self.browser = None # Ensure browser is None if launch fails
                # Potentially re-raise or handle as a critical failure depending on requirements
                # For now, let's allow it to proceed without a browser if launch fails,
                # fetch_page will then rely on requests or fail if browser was mandatory.
                if self.playwright_context: # Ensure playwright_context is cleaned up if it was started.
                    try:
                        self.playwright_context.stop()
                    except Exception as stop_e: # pragma: no cover
                         print(f"Error stopping playwright_context after launch failure: {stop_e}")
                    self.playwright_context = None

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({"User-Agent": self.current_user_agent})
        # More robust retry strategy
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"] # Allow for potential future POST requests
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def rotate_user_agent(self):
        self.current_user_agent = random.choice(MODERN_USER_AGENTS)
        if self.session: # Ensure session exists before updating headers
            self.session.headers.update({"User-Agent": self.current_user_agent})
        self.pages_scraped_since_ua_rotation = 0
        # Optional: Log rotation
        # print(f"[INFO] Rotated User-Agent to: {self.current_user_agent}")

    def fetch_page(self, url: str, use_browser_override: bool = False) -> str:
        """Fetches page content using requests or Playwright."""
        self.pages_scraped_since_ua_rotation += 1
        if self.pages_scraped_since_ua_rotation >= self.rotate_ua_after_pages:
            self.rotate_user_agent()

        effective_use_browser = self.use_browser_default or use_browser_override

        if effective_use_browser:
            if not PLAYWRIGHT_AVAILABLE:
                raise RuntimeError("Playwright is not installed, but browser use was requested.")

            if not self.browser: # Initialize browser if not already done (e.g. if use_browser_default was false but override is true)
                if not self.playwright_context:
                    # This assumes sync_playwright() was already handled for PLAYWRIGHT_AVAILABLE
                    self.playwright_context = sync_playwright().start()
                try:
                    self.browser = self.playwright_context.chromium.launch(headless=self.headless)
                except Exception as e: # pragma: no cover
                    # Clean up playwright_context if browser launch fails here
                    if self.playwright_context:
                        try:
                            self.playwright_context.stop()
                        except Exception as stop_e: # pragma: no cover
                            print(f"Error stopping playwright_context after on-demand launch failure: {stop_e}")
                        self.playwright_context = None
                    raise RuntimeError(f"Playwright browser launch failed for on-demand use: {e}")


            if not self.browser: # Should not happen if logic above is correct, but as a safeguard
                 raise RuntimeError("Browser instance not available despite request for browser use.")

            pw_page: Page | None = None # Explicitly None, using the potentially dummied Page type
            try:
                # print(f"[INFO] Fetching with Playwright: {url}")
                pw_page = self.browser.new_page(user_agent=self.current_user_agent)
                pw_page.goto(url, wait_until="networkidle", timeout=45000)
                content = pw_page.content()
            except Exception as e: # Catch Playwright-specific errors if possible, else general Exception
                # print(f"[ERROR] Playwright fetch failed for {url}: {e}")
                # Consider more specific Playwright error handling if needed
                # from playwright.sync_api import Error as PlaywrightError
                # from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
                # if isinstance(e, PlaywrightTimeoutError): ...
                raise # Re-raise the exception
            finally:
                if pw_page:
                    pw_page.close()
            return content
        else:
            if not self.session: # Should be created by __init__
                raise RuntimeError("Requests session not initialized.")
            # print(f"[INFO] Fetching with Requests: {url}")
            time.sleep(random.uniform(0.5, 1.5)) # Basic rate limiting
            response = self.session.get(url, timeout=20)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response.text

    def close(self):
        """Closes the Playwright browser and context if they were initialized."""
        if self.browser:
            try:
                self.browser.close()
            except Exception as e: # pragma: no cover
                print(f"Error closing browser: {e}")
            self.browser = None
        if self.playwright_context:
            try:
                self.playwright_context.stop()
            except Exception as e: # pragma: no cover
                print(f"Error stopping playwright_context: {e}")
            self.playwright_context = None
        # print("[INFO] DualModeFetcherCS resources closed.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
