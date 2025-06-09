import argparse
import json
import logging
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, TypedDict, Any, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from playwright.sync_api import (
        sync_playwright,
        Page as PlaywrightPage,
        Browser as PlaywrightBrowser,
        Locator,
        TimeoutError as PlaywrightTimeoutError,
    )
    # from playwright_stealth import stealth_sync # Recommended for enhanced stealth
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    sync_playwright, PlaywrightPage, PlaywrightBrowser, Locator, PlaywrightTimeoutError = (None,) * 5 # type: ignore
    # stealth_sync = None # type: ignore
    PLAYWRIGHT_AVAILABLE = False

# --- Basic Configuration ---
DEFAULT_SNAPSHOT_DIR = "debug_snapshots"
DEFAULT_OUTPUT_DIR = "output"

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MegaEventScraper")

# --- User Agents ---
MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# --- Type Definitions for Event Schema (Refined) ---
class CoordinatesTypedDict(TypedDict, total=False):
    lat: Optional[float]
    lng: Optional[float]

class LocationTypedDict(TypedDict, total=False):
    venue: Optional[str]
    address: Optional[str]
    city: Optional[str]
    country: Optional[str]
    postalCode: Optional[str] # Added
    region: Optional[str] # Added (e.g. state/province)
    coordinates: Optional[CoordinatesTypedDict]

class ParsedDateTimeTypedDict(TypedDict, total=False):
    startDate: Optional[datetime]
    endDate: Optional[datetime]
    doorTime: Optional[datetime] # Changed to datetime for consistency
    timeZone: Optional[str]

class DateTimeInfoTypedDict(TypedDict, total=False):
    displayText: Optional[str]
    parsed: Optional[ParsedDateTimeTypedDict]
    dayOfWeek: Optional[str]

class ArtistTypedDict(TypedDict, total=False):
    name: str
    role: Optional[str] # e.g. "DJ", "Band"
    headliner: Optional[bool]
    # socialLinks: Optional[Dict[str, str]] # Artist-specific social links

class TicketTierTypedDict(TypedDict, total=False):
    name: Optional[str]
    price: Optional[float]
    currency: Optional[str]
    availability: Optional[str] # e.g. "InStock", "SoldOut"
    available: Optional[bool] # Derived from availability

class TicketInfoTypedDict(TypedDict, total=False):
    displayText: Optional[str]
    startingPrice: Optional[float]
    currency: Optional[str]
    tiers: Optional[List[TicketTierTypedDict]]
    status: Optional[str]
    url: Optional[str]

class OrganizerTypedDict(TypedDict, total=False):
    name: Optional[str]
    url: Optional[str] # Organizer's website
    # socialLinks: Optional[Dict[str, str]]

class EventSchemaTypedDict(TypedDict, total=False):
    # Core
    url: str
    title: Optional[str]
    extractionMethod: Optional[str]
    scrapedAt: datetime
    updatedAt: Optional[datetime] # When this specific record was last modified by the scraper
    lastCheckedAt: Optional[datetime] # When the source URL was last checked for any changes

    # Details
    location: Optional[LocationTypedDict]
    dateTime: Optional[DateTimeInfoTypedDict]
    lineUp: Optional[List[ArtistTypedDict]]
    artistCount: Optional[int]
    eventType: Optional[List[str]]
    genres: Optional[List[str]]
    fullDescription: Optional[str] # Can be HTML or Markdown
    descriptionFormat: Optional[str] # "html" or "markdown"
    promoter: Optional[OrganizerTypedDict] # Using Organizer schema for promoter

    # Tickets
    ticketInfo: Optional[TicketInfoTypedDict]
    ticketsUrl: Optional[str] # Most direct purchase link
    hasTicketInfo: Optional[bool]
    isFree: Optional[bool]
    isSoldOut: Optional[bool]

    # Misc
    organizer: Optional[OrganizerTypedDict] # Primary event organizer
    ageRestriction: Optional[str]
    images: Optional[List[str]] # URLs to images
    imageCount: Optional[int]
    videos: Optional[List[str]] # URLs to videos
    socialLinks: Optional[Dict[str, str]] # Event-specific social media links
    tags: Optional[List[str]] # Keywords or tags associated with the event

    # Debug / Admin
    siteProfileUsed: Optional[str] # Name or path of the site profile
    htmlSnapshotPath: Optional[str]
    screenshotPath: Optional[str]
    rawExtractedData: Optional[Dict[str, Any]] # Store pre-mapping data from various layers

# --- Helper Functions ---
def datetime_serializer(obj: Any) -> str:
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def is_data_sufficient(event_data: EventSchemaTypedDict, profile: Dict) -> bool:
    """Checks if extracted data is sufficient based on profile or defaults."""
    if not event_data or not event_data.get("title"):
        return False

    sufficiency_rules = profile.get("sufficiency_rules", {
        "require_title": True,
        "require_at_least_one_of": ["location.venue", "dateTime.displayText", "ticketInfo.startingPrice", "fullDescription"]
    })

    if sufficiency_rules.get("require_title") and not event_data.get("title"):
        return False

    if sufficiency_rules.get("require_at_least_one_of"):
        found_one = False
        for field_path in sufficiency_rules["require_at_least_one_of"]:
            parts = field_path.split('.')
            temp_data: Any = event_data
            valid_path = True
            for part in parts:
                if isinstance(temp_data, dict) and part in temp_data:
                    temp_data = temp_data[part]
                else:
                    valid_path = False
                    break
            if valid_path and temp_data is not None: # Check for non-None value
                # Specific check for price > 0
                if field_path == "ticketInfo.startingPrice" and isinstance(temp_data, (int, float)) and temp_data <= 0:
                    continue # Price must be > 0 to count for sufficiency here
                found_one = True
                break
        if not found_one:
            return False
    return True

# --- MegaEventScraper Class ---
class MegaEventScraper:
    def __init__(
        self,
        site_profile: Dict,
        global_config: Dict,
        user_agents: Optional[List[str]] = None,
    ):
        self.site_profile = site_profile
        self.config = global_config # Global scraper settings
        self.user_agents = user_agents or MODERN_USER_AGENTS
        
        self.current_user_agent: Optional[str] = None
        self.session: Optional[requests.Session] = None
        self.pages_scraped_since_ua_rotation: int = 0
        self.rotate_ua_after_pages: int = random.randint(5, 10)

        self.playwright_context: Any = None
        self.browser: Optional[PlaywrightBrowser] = None
        
        self.snapshot_dir = Path(self.config.get("snapshot_dir", DEFAULT_SNAPSHOT_DIR))
        self.snapshot_dir.mkdir(parents=True, exist_ok=True)

        self.rotate_user_agent() # Initializes session and first UA
        logger.info(f"MegaEventScraper initialized. Profile: {site_profile.get('name', 'Unnamed')}")

    def _ensure_playwright_started(self) -> bool:
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright is not available/installed.")
            return False
        if self.browser and self.browser.is_connected():
            return True
        try:
            logger.info("Starting Playwright browser...")
            if self.playwright_context: self.playwright_context.stop() # Stop if exists but browser is dead
            
            self.playwright_context = sync_playwright().start()
            launch_options: Dict[str, Any] = {
                'headless': self.config.get("headless", True),
                'slow_mo': self.site_profile.get("playwright_slow_mo", self.config.get("playwright_slow_mo", 50)),
                def scrape_event_list(self, url: str) -> List[EventSchemaTypedDict]:
        \"\"\"Scrapes an event list page and returns a list of events (with at least the URL and possibly other fields).\"\"\"
        html_content, html_snapshot, ss_snapshot = self.fetch_page(
            url, 
            use_browser_for_this_fetch=self.config.get("use_browser_for_event_lists", False)
        )
        if not html_content:
            logger.error(f"Failed to fetch event list page: {url}")
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        base_url = url

        event_list_rules = self.site_profile.get("event_list_rules", {})
        event_container_selector = event_list_rules.get("event_container_selector")
        event_url_selector = event_list_rules.get("event_url_selector", "a")
        event_title_selector = event_list_rules.get("event_title_selector")

        events = []

        if event_container_selector:
            event_containers = soup.select(event_container_selector)
            for container in event_containers:
                event_data: EventSchemaTypedDict = {
                    "url": "",
                    "scrapedAt": datetime.now(),
                }

                # Extract URL
                if event_url_selector:
                    link_elem = container.select_one(event_url_selector)
                    if link_elem and link_elem.get('href'):
                        event_url = urljoin(base_url, link_elem['href'])
                        event_data["url"] = event_url

                # Extract title
                if event_title_selector:
                    title_elem = container.select_one(event_title_selector)
                    if title_elem:
                        event_data["title"] = title_elem.get_text(strip=True)

                # Extract other fields from the container as defined in event_list_rules
                field_mappings = event_list_rules.get("field_mappings", {})
                for field_name, selector in field_mappings.items():
                    elem = container.select_one(selector)
                    if elem:
                        event_data[field_name] = elem.get_text(strip=True)

                events.append(event_data)
        else:
            logger.error("No event_container_selector defined in event_list_rules. Cannot scrape event list.")

        return events

}
            if self.config.get("proxy"):
                launch_options['proxy'] = {'server': self.config["proxy"]    def scrape_event_list(self, url: str) -> List[EventSchemaTypedDict]:
        \"\"\"Scrapes an event list page and returns a list of events (with at least the URL and possibly other fields).\"\"\"
        html_content, html_snapshot, ss_snapshot = self.fetch_page(
            url, 
            use_browser_for_this_fetch=self.config.get("use_browser_for_event_lists", False)
        )
        if not html_content:
            logger.error(f"Failed to fetch event list page: {url}")
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        base_url = url

        event_list_rules = self.site_profile.get("event_list_rules", {})
        event_container_selector = event_list_rules.get("event_container_selector")
        event_url_selector = event_list_rules.get("event_url_selector", "a")
        event_title_selector = event_list_rules.get("event_title_selector")

        events = []

        if event_container_selector:
            event_containers = soup.select(event_container_selector)
            for container in event_containers:
                event_data: EventSchemaTypedDict = {
                    "url": "",
                    "scrapedAt": datetime.now(),
                }

                # Extract URL
                if event_url_selector:
                    link_elem = container.select_one(event_url_selector)
                    if link_elem and link_elem.get('href'):
                        event_url = urljoin(base_url, link_elem['href'])
                        event_data["url"] = event_url

                # Extract title
                if event_title_selector:
                    title_elem = container.select_one(event_title_selector)
                    if title_elem:
                        event_data["title"] = title_elem.get_text(strip=True)

                # Extract other fields from the container as defined in event_list_rules
                field_mappings = event_list_rules.get("field_mappings", {})
                for field_name, selector in field_mappings.items():
                    elem = container.select_one(selector)
                    if elem:
                        event_data[field_name] = elem.get_text(strip=True)

                events.append(event_data)
        else:
            logger.error("No event_container_selector defined in event_list_rules. Cannot scrape event list.")

        return events

}
            
            # Channel for specific browser (e.g., 'chrome', 'msedge')
            browser_channel = self.site_profile.get("playwright_browser_channel")
            if browser_channel:
                launch_options['channel'] = browser_channel

            self.browser = self.playwright_context.chromium.launch(**launch_options)
            logger.info(f"Playwright browser started (Headless: {launch_options['headless']}).")
            return True
        except Exception as e:
            logger.exception(f"Failed to start Playwright browser: {e}")
            self.browser = None
            self.playwright_context = None
            return False

    def rotate_user_agent(self):
        self.current_user_agent = random.choice(self.user_agents)
        self.session = self._setup_session()
        self.pages_scraped_since_ua_rotation = 0
        self.rotate_ua_after_pages = random.randint(
            self.config.get("ua_rotation_min_pages", 5),
            self.config.get("ua_rotation_max_pages", 10)
        )
        logger.debug(f"Rotated User-Agent to: {self.current_user_agent}")

    def _setup_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=self.site_profile.get("requests_retries", self.config.get("requests_retries", 3)),
            backoff_factor=self.site_profile.get("requests_backoff_factor", self.config.get("requests_backoff_factor", 1)),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        headers = self.config.get("default_headers", {}).copy()
        headers["User-Agent"] = self.current_user_agent or MODERN_USER_AGENTS[0]
        if self.site_profile.get("custom_headers"):
            headers.update(self.site_profile["custom_headers"])
        
        session.headers.update(headers)
        if self.config.get("proxy"):
            session.proxies = {"http": self.config["proxy"], "https": self.config["proxy"]}
        return session

    def _save_debug_snapshot(self, page: PlaywrightPage, url: str, stage: str = "error"):
        """Saves HTML and screenshot for debugging."""
        try:
            filename_base = re.sub(r'[^\w\-_\.]', '_', url.split("://")[1])
            ts = int(time.time())
            html_path = self.snapshot_dir / f"{stage}_{filename_base}_{ts}.html"
            ss_path = self.snapshot_dir / f"{stage}_{filename_base}_{ts}.png"
            
            page_content = page.content()
            html_path.write_text(page_content, encoding='utf-8', errors='replace')
            logger.debug(f"Saved debug HTML snapshot to: {html_path}")
            
            page.screenshot(path=str(ss_path), full_page=True)
            logger.debug(f"Saved debug screenshot to: {ss_path}")
            return str(html_path), str(ss_path)
        except Exception as e:
            logger.error(f"Could not save debug snapshot for {url}: {e}")
            return None, None

    def _execute_playwright_actions(self, page: PlaywrightPage, actions: List[Dict]):
        """Executes a list of Playwright actions defined in the site profile."""
        for action_config in actions:
            action_type = action_config.get("type")
            selector = action_config.get("selector")
            value = action_config.get("value")
            timeout = action_config.get("timeout", 5000)
            delay_after_s = action_config.get("delay_after_s", random.uniform(0.5, 1.5))
            
            logger.info(f"Performing Playwright action: {action_type} on '{selector}'")
            try:
                target_locator: Optional[Locator] = page.locator(selector).first if selector else None
                
                if action_type == "click":
                    if target_locator: self._human_click(page, target_locator)
                elif action_type == "fill" and value is not None:
                    if target_locator: target_locator.fill(value, timeout=timeout)
                elif action_type == "wait_for_selector":
                    if selector: page.wait_for_selector(selector, timeout=timeout, state=action_config.get("state", "visible"))
                elif action_type == "scroll_to_bottom":
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                elif action_type == "wait_for_timeout": # value is milliseconds
                    page.wait_for_timeout(float(value) if value else 2000)
                elif action_type == "hover":
                    if target_locator: target_locator.hover(timeout=timeout)
                elif action_type == "press_key": # value is the key, e.g. "Escape", "Enter"
                    if value: page.keyboard.press(value)
                # Add more actions as needed (e.g., select_option, check, evaluate script)
                else:
                    logger.warning(f"Unknown Playwright action type: {action_type}")
                
                time.sleep(delay_after_s)
            except PlaywrightTimeoutError:
                logger.warning(f"Timeout performing Playwright action '{action_type}' on '{selector}'")
            except Exception as e:
                logger.error(f"Error performing Playwright action '{action_type}' on '{selector}': {e}")

    def _human_click(self, page: PlaywrightPage, locator: Locator, timeout: int = 10000):
        """Refined human-like click."""
        try:
            locator.wait_for(state="visible", timeout=timeout / 2)
            locator.scroll_into_view_if_needed(timeout=timeout / 2) # Ensure it's in view
            locator.hover(timeout=timeout / 2)
            time.sleep(random.uniform(0.1, 0.3)) # Small pause after hover
            
            # Bounding box click logic from Scraper 0
            bb = locator.bounding_box()
            if bb:
                x = bb['x'] + bb['width'] * random.uniform(0.25, 0.75)
                y = bb['y'] + bb['height'] * random.uniform(0.25, 0.75)
                page.mouse.move(x, y, steps=random.randint(5, 15))
                time.sleep(random.uniform(0.05, 0.15))
                page.mouse.down()
                time.sleep(random.uniform(0.05, 0.1)) # Simulate click duration
                page.mouse.up()
                logger.debug(f"Human-like click performed on locator.")
            else: # Fallback to Playwright's robust click if bounding_box fails
                locator.click(timeout=timeout / 2, force=True) # force can help with overlays
                logger.debug(f"Standard Playwright click performed on locator.")
            time.sleep(random.uniform(0.3, 0.8)) # Wait for action to take effect
        except Exception as e:
            logger.warning(f"Human-like click failed: {e}. Attempting direct click.")
            try:
                locator.click(timeout=timeout / 2, force=True)
            except Exception as e2:
                logger.error(f"Direct click also failed: {e2}")
                raise # Re-raise if all attempts fail

    def _handle_overlays(self, page: PlaywrightPage):
        """Handles overlays based on site profile."""
        overlay_configs = self.site_profile.get("overlay_handling", [])
        if not overlay_configs:
            logger.debug("No overlay handling configured in site profile.")
            return

        logger.info("Checking for overlays/cookie banners...")
        time.sleep(self.site_profile.get("overlay_initial_wait_s", random.uniform(0.8, 1.5)))

        for config in overlay_configs:
            selector = config.get("selector")
            iframe_selector = config.get("iframe_selector") # Optional: if overlay is in an iframe
            max_attempts = config.get("max_attempts", 1)
            delay_between_attempts_s = config.get("delay_between_attempts_s", 1)

            for attempt in range(max_attempts):
                try:
                    target_page_or_frame: Union[PlaywrightPage, Any] = page # Any for Frame
                    if iframe_selector:
                        iframe_loc = page.locator(iframe_selector).first
                        if iframe_loc.is_visible(timeout=2000):
                            target_page_or_frame = iframe_loc.content_frame()
                            if not target_page_or_frame:
                                logger.warning(f"Could not get content frame for iframe: {iframe_selector}")
                                continue
                            logger.debug(f"Switched to iframe context: {iframe_selector}")
                        else:
                            logger.debug(f"Iframe for overlay not visible: {iframe_selector}")
                            continue # Try next config or attempt

                    overlay_locator = target_page_or_frame.locator(selector).first
                    if overlay_locator.is_visible(timeout=config.get("visibility_timeout", 3000)):
                        logger.info(f"Found overlay: '{selector}' (Attempt {attempt + 1}). Clicking...")
                        self._human_click(target_page_or_frame if isinstance(target_page_or_frame, PlaywrightPage) else page, overlay_locator) # Pass page for mouse ops
                        
                        # Wait for overlay to disappear (optional, based on profile)
                        if config.get("wait_for_disappearance", True):
                            try:
                                overlay_locator.wait_for(state="hidden", timeout=config.get("disappearance_timeout", 5000))
                                logger.info(f"Overlay '{selector}' disappeared.")
                            except PlaywrightTimeoutError:
                                logger.warning(f"Overlay '{selector}' did not disappear after click.")
                        return # Successfully handled one overlay configuration
                    else:
                        logger.debug(f"Overlay '{selector}' not visible (Attempt {attempt + 1}).")
                
                except PlaywrightTimeoutError:
                    logger.debug(f"Timeout finding/interacting with overlay '{selector}' (Attempt {attempt + 1}).")
                except Exception as e:
                    logger.error(f"Error handling overlay '{selector}': {e} (Attempt {attempt + 1}).")
                
                if attempt < max_attempts - 1:
                    time.sleep(delay_between_attempts_s)
            
        logger.info("Overlay handling complete.")


    def fetch_page(self, url: str, use_browser_for_this_fetch: bool = False) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Fetches page content. Returns (html_content, html_snapshot_path, screenshot_path)."""
        self.pages_scraped_since_ua_rotation += 1
        if self.pages_scraped_since_ua_rotation >= self.rotate_ua_after_pages:
            self.rotate_user_agent()

        effective_use_browser = use_browser_for_this_fetch and self.config.get("allow_browser_use", True)
        html_snapshot, ss_snapshot = None, None

        if effective_use_browser:
            if not self._ensure_playwright_started(): return None, None, None
            
            page: Optional[PlaywrightPage] = None
            page_content: Optional[str] = None
            try:
                context_options: Dict[str, Any] = {'user_agent': self.current_user_agent}
                if self.site_profile.get("playwright_viewport"): context_options['viewport'] = self.site_profile["playwright_viewport"]
                if self.site_profile.get("playwright_locale"): context_options['locale'] = self.site_profile["playwright_locale"]
                if self.site_profile.get("playwright_timezone_id"): context_options['timezone_id'] = self.site_profile["playwright_timezone_id"]
                if self.site_profile.get("playwright_geolocation"): context_options['geolocation'] = self.site_profile["playwright_geolocation"]
                # Add more context options from profile as needed (permissions, color_scheme, etc.)

                page = self.browser.new_page(**context_options)
                
                # Apply playwright-stealth if integrated and enabled
                # if stealth_sync and self.site_profile.get("use_playwright_stealth", True):
                #    logger.debug("Applying playwright-stealth.")
                #    stealth_sync(page)

                if self.site_profile.get("playwright_init_scripts"):
                    for script in self.site_profile["playwright_init_scripts"]:
                        page.add_init_script(script)
                
                logger.info(f"Fetching with Playwright: {url}")
                page.goto(url, timeout=self.site_profile.get("playwright_goto_timeout", 60000),
                               wait_until=self.site_profile.get("playwright_wait_until_load", "domcontentloaded"))
                
                self._handle_overlays(page)
                
                if self.site_profile.get("playwright_pre_extraction_actions"):
                    self._execute_playwright_actions(page, self.site_profile["playwright_pre_extraction_actions"])

                final_content_selector = self.site_profile.get("playwright_wait_for_final_content_selector")
                if final_content_selector:
                    logger.info(f"Waiting for final content selector: {final_content_selector}")
                    page.wait_for_selector(final_content_selector, 
                                           timeout=self.site_profile.get("playwright_final_wait_timeout", 20000),
                                           state=self.site_profile.get("playwright_final_wait_state", "visible"))
                
                page_content = page.content()
                if self.config.get("debug_mode", False) or self.site_profile.get("always_snapshot_playwright", False):
                    html_snapshot, ss_snapshot = self._save_debug_snapshot(page, url, stage="fetch_success")

            except Exception as e:
                logger.exception(f"Playwright fetch failed for {url}: {e}")
                if page: html_snapshot, ss_snapshot = self._save_debug_snapshot(page, url, stage="fetch_error")
                return None, html_snapshot, ss_snapshot
            finally:
                if page: page.close()
            return page_content, html_snapshot, ss_snapshot
        else: # Use requests
            delay = random.uniform(
                self.site_profile.get("requests_min_delay_s", self.config.get("requests_min_delay_s", 0.7)),
                self.site_profile.get("requests_max_delay_s", self.config.get("requests_max_delay_s", 1.5))
            )
            time.sleep(delay)
            logger.info(f"Fetching with Requests: {url} (delay: {delay:.2f}s)")
            try:
                if not self.session: self.session = self._setup_session() # Should always be set by init
                response = self.session.get(url, timeout=self.site_profile.get("requests_timeout_s", self.config.get("requests_timeout_s", 20)))
                response.raise_for_status()
                return response.text, None, None # No snapshots for requests
            except Exception as e:
                logger.error(f"Requests fetch failed for {url}: {e}")
                return None, None, None

    def _extract_from_html_selectors(self, soup: BeautifulSoup, base_url: str) -> Dict[str, Any]:
        """Extracts data using CSS selectors defined in the site_profile."""
        data: Dict[str, Any] = {}
        html_extract_rules = self.site_profile.get("html_extract_rules", {})

        for field_name, rules_for_field in html_extract_rules.items():
            if not isinstance(rules_for_field, list): rules_for_field = [rules_for_field]
            
            for rule in rules_for_field:
                if isinstance(rule, str): # Simple selector string
                    selector_config = {"selector": rule}
                elif isinstance(rule, dict):
                    selector_config = rule
                else:
                    logger.warning(f"Invalid rule format for field '{field_name}': {rule}")
                    continue

                css_selector = selector_config.get("selector")
                if not css_selector: continue

                attribute = selector_config.get("attribute") # e.g., "href", "src", "content"
                extract_all = selector_config.get("all", False) # True to get a list of all matches
                text_contains_regex = selector_config.get("text_contains_regex") # For filtering elements by text
                
                try:
                    elements = soup.select(css_selector)
                    if text_contains_regex:
                        elements = [el for el in elements if re.search(text_contains_regex, el.get_text(strip=True), re.IGNORECASE)]

                    if not elements: continue

                    if extract_all:
                        values = []
                        for elem in elements:
                            val = elem.get(attribute) if attribute else elem.get_text(strip=True)
                            if attribute == "href" or attribute == "src": val = urljoin(base_url, val) # Resolve relative URLs
                            if val: values.append(val)
                        if values: data[field_name] = values; break # Found for this field
                    else: # Extract first
                        elem = elements[0]
                        val = elem.get(attribute) if attribute else elem.get_text(strip=True)
                        if attribute == "href" or attribute == "src": val = urljoin(base_url, val)
                        if val: data[field_name] = val; break
                except Exception as e:
                    logger.error(f"Error applying selector for '{field_name}' ('{css_selector}'): {e}")
        return data

    def extract_jsonld_data(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts all JSON-LD blocks, prioritizing MusicEvent."""
        found_json_ld_data = []
        music_events = []
        for script_tag in soup.find_all("script", type="application/ld+json"):
            try:
                content = script_tag.string or script_tag.get_text()
                if not content.strip(): continue
                data = json.loads(content)
                
                if isinstance(data, list): # Handle array of JSON-LD objects
                    for item in data:
                        if isinstance(item, dict):
                            found_json_ld_data.append(item)
                            if item.get("@type") == "MusicEvent" or (isinstance(item.get("@type"), list) and "MusicEvent" in item.get("@type")):
                                music_events.append(item)
                elif isinstance(data, dict):
                    found_json_ld_data.append(data)
                    if data.get("@type") == "MusicEvent" or (isinstance(data.get("@type"), list) and "MusicEvent" in data.get("@type")):
                        music_events.append(data)
                    # Check for @graph
                    if "@graph" in data and isinstance(data["@graph"], list):
                        for graph_item in data["@graph"]:
                            if isinstance(graph_item, dict):
                                found_json_ld_data.append(graph_item) # Add all graph items
                                if graph_item.get("@type") == "MusicEvent" or \
                                   (isinstance(graph_item.get("@type"), list) and "MusicEvent" in graph_item.get("@type")):
                                    music_events.append(graph_item)
            except json.JSONDecodeError:
                logger.warning(f"Failed to decode JSON-LD content: {content[:100]}...")
            except Exception as e:
                logger.error(f"Error processing JSON-LD script: {e}")
        
        return music_events if music_events else found_json_ld_data # Prioritize MusicEvent

    def extract_meta_data(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Extracts common meta tags (OpenGraph, Twitter, basic)."""
        data: Dict[str, str] = {}
        # Profile can override these mappings
        og_mappings = self.site_profile.get("meta_og_mappings", {
            "og:title": "og_title", "og:description": "og_description", "og:image": "og_image", 
            "og:url": "og_url", "og:type": "og_type", "event:start_time": "og_event_start_time", 
            "event:end_time": "og_event_end_time", "place:location:latitude": "og_place_lat", 
            "place:location:longitude": "og_place_lon"
        })
        for prop, key in og_mappings.items():
            tag = soup.find("meta", property=prop)
            if tag and tag.get("content"): data[key] = tag["content"]

        twitter_mappings = self.site_profile.get("meta_twitter_mappings", {
            "twitter:card": "twitter_card", "twitter:title": "twitter_title", 
            "twitter:description": "twitter_description", "twitter:image": "twitter_image"
        })
        for name, key in twitter_mappings.items():
            tag = soup.find("meta", attrs={"name": name}) # Twitter uses 'name' not 'property'
            if tag and tag.get("content"): data[key] = tag["content"]
        
        basic_meta_mappings = self.site_profile.get("meta_basic_mappings", {
            "description": "meta_description", "keywords": "meta_keywords"
        })
        for name, key in basic_meta_mappings.items():
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"): data[key] = tag["content"]
        
        # Canonical URL
        canonical_tag = soup.find("link", rel="canonical")
        if canonical_tag and canonical_tag.get("href"):
            data["canonical_url"] = canonical_tag["href"]
            
        return data

    def extract_regex_patterns(self, html_content: str) -> Dict[str, Any]:
        data = {}
        regex_patterns = self.site_profile.get("regex_patterns", {})
        for field, pattern in regex_patterns.items():
            try:
                match = re.search(pattern, html_content)
                if match:
                    data[field] = match.group(1)
                else:
                    data[field] = None
            except Exception as e:
                logger.error(f"Error applying regex for '{field}': {e}")
                data[field] = None
        return data

    def scrape_event_list(self, url: str) -> List[EventSchemaTypedDict]:
        """Scrapes an event list page and returns a list of events (with at least the URL and possibly other fields)."""
        html_content, html_snapshot, ss_snapshot = self.fetch_page(
            url, 
            use_browser_for_this_fetch=self.config.get("use_browser_for_event_lists", False)
        )
        if not html_content:
            logger.error(f"Failed to fetch event list page: {url}")
            return []

        soup = BeautifulSoup(html_content, 'html.parser')
        base_url = url

        event_list_rules = self.site_profile.get("event_list_rules", {})
        event_container_selector = event_list_rules.get("event_container_selector")
        event_url_selector = event_list_rules.get("event_url_selector", "a")
        event_title_selector = event_list_rules.get("event_title_selector")

        events = []

        if event_container_selector:
            event_containers = soup.select(event_container_selector)
            for container in event_containers:
                event_data: EventSchemaTypedDict = {
                    "url": "",
                    "scrapedAt": datetime.now(),
                }

                # Extract URL
                if event_url_selector:
                    link_elem = container.select_one(event_url_selector)
                    if link_elem and link_elem.get('href'):
                        event_url = urljoin(base_url, link_elem['href'])
                        event_data["url"] = event_url

                # Extract title
                if event_title_selector:
                    title_elem = container.select_one(event_title_selector)
                    if title_elem:
                        event_data["title"] = title_elem.get_text(strip=True)

                # Extract other fields from the container as defined in event_list_rules
                field_mappings = event_list_rules.get("field_mappings", {})
                for field_name, selector in field_mappings.items():
                    elem = container.select_one(selector)
                    if elem:
                        event_data[field_name] = elem.get_text(strip=True)

                events.append(event_data)
        else:
            logger.error("No event_container_selector defined in event_list_rules. Cannot scrape event list.")

        return events


    def scrape_event_data(self, url: str, use_browser_for_this_fetch: bool = False) -> EventSchemaTypedDict:
        """Main scraping method with multiple fallback strategies."""
        start_time = time.monotonic()
        logger.info(f"Starting scrape for URL: {url}")
        
        html_content, html_snapshot, ss_snapshot = self.fetch_page(url, use_browser_for_this_fetch)
        
        # Initialize with minimal data
        event_data_result: EventSchemaTypedDict = {
            "url": url,
            "scrapedAt": datetime.utcnow(),
            "extractionMethod": "failed", # Default if nothing works
            "siteProfileUsed": self.site_profile.get("name", "Unnamed Profile"),
            "htmlSnapshotPath": html_snapshot,
            "screenshotPath": ss_snapshot,
            "rawExtractedData": {} # Initialize raw data collector
        }

        if not html_content:
            logger.error(f"Failed to fetch HTML content for {url}.")
            return event_data_result

        soup = BeautifulSoup(html_content, self.site_profile.get("parser", "lxml"))
        
        # --- Extraction Layers ---
        # Layer 1: JSON-LD
        jsonld_items = self.extract_jsonld_data(soup)
        primary_jsonld_node = jsonld_items[0] if jsonld_items else None # Use the first (MusicEvent prioritized)
        if primary_jsonld_node:
            logger.info("JSON-LD data found. Attempting to map.")
            event_data_result.update(self._map_jsonld_to_event_schema(primary_jsonld_node, url, soup))
            event_data_result["extractionMethod"] = "jsonld"
            event_data_result["rawExtractedData"]["jsonld"] = primary_jsonld_node # type: ignore
            # Optionally augment with other layers if profile specifies
            if self.site_profile.get("augment_jsonld", False):
                logger.info("Augmenting JSON-LD with other extraction layers.")
                # (Augmentation logic would go here, e.g., filling missing fields)
        else:
            logger.info("No primary JSON-LD (MusicEvent) found. Proceeding to fallback layers.")

        # Fallback Layers (if JSON-LD failed or for augmentation)
        # These are collected into rawExtractedData and then mapped by _map_fallback_to_event_schema
        # if JSON-LD wasn't primary or if augmentation is on.
        
        meta_data = self.extract_meta_data(soup)
        event_data_result["rawExtractedData"]["meta"] = meta_data # type: ignore

        html_selector_data = self._extract_from_html_selectors(soup, url)
        event_data_result["rawExtractedData"]["html_selectors"] = html_selector_data # type: ignore

        regex_data = self.extract_regex_patterns(html_content)
        event_data_result["rawExtractedData"]["regex"] = regex_data # type: ignore

        # If JSON-LD was not the primary source, use fallback mapping
        if event_data_result["extractionMethod"] != "jsonld":
            logger.info("Mapping data using combined fallback (HTML/Meta/Regex).")
            # The _map_fallback_to_event_schema will use the collected rawExtractedData
            event_data_result.update(self._map_fallback_to_event_schema(event_data_result["rawExtractedData"], url, soup)) # type: ignore
            event_data_result["extractionMethod"] = "fallback_multi_layer"
        
        # Final derived fields population
        self._populate_derived_fields(event_data_result)
        event_data_result["updatedAt"] = datetime.utcnow()
        
        logger.info(f"Scraping for {url} completed in {time.monotonic() - start_time:.2f}s. Method: {event_data_result['extractionMethod']}")
        return event_data_result

    def scrape_event_strategically(self, url: str) -> EventSchemaTypedDict:
        """Orchestrates scraping: requests-first, then Playwright if needed/allowed."""
        logger.info(f"Strategically scraping: {url}")
        
        # Attempt 1: Requests only
        event_data_req = self.scrape_event_data(url, use_browser_for_this_fetch=False)
        if is_data_sufficient(event_data_req, self.site_profile):
            logger.info(f"Sufficient data obtained via requests for {url}.")
            return event_data_req

        # Attempt 2: Playwright (if allowed and requests was insufficient)
        if self.config.get("allow_browser_use", True) and PLAYWRIGHT_AVAILABLE:
            logger.info(f"Requests data insufficient for {url}. Attempting with Playwright.")
            event_data_pw = self.scrape_event_data(url, use_browser_for_this_fetch=True)
            
            # Prefer Playwright data if it's sufficient, otherwise stick with requests if it had a title
            if is_data_sufficient(event_data_pw, self.site_profile):
                logger.info(f"Sufficient data obtained via Playwright for {url}.")
                return event_data_pw
            elif event_data_req.get("title"): # If PW failed but req got something basic
                logger.warning(f"Playwright data also insufficient for {url}. Reverting to requests data (title: {event_data_req.get('title')}).")
                return event_data_req
            else: # Both failed to get even a title
                logger.error(f"Both requests and Playwright attempts failed to yield sufficient data for {url}.")
                return event_data_pw # Return PW attempt as it might have snapshots
        else:
            logger.info(f"Browser use not enabled or Playwright not available. Using requests-only data for {url}.")
            return event_data_req

    def _parse_datetime_flexible(self, date_str: Optional[str], time_str: Optional[str] = None) -> Optional[datetime]:
        """Flexibly parses date/time strings. Needs more robust library for production."""
        if not date_str: return None
        # This is a very basic parser. Consider dateutil.parser for real-world use.
        full_str = date_str
        if time_str: full_str += " " + time_str
        
        formats_to_try = [
            "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", # ISO with/without TZ
            "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
            "%d %b %Y %H:%M", "%d %B %Y %H:%M",
            "%m/%d/%Y %I:%M %p", "%m/%d/%Y %H:%M",
            "%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%m/%d/%Y"
        ]
        for fmt in formats_to_try:
            try:
                # Handle timezone offset like +01:00 by removing colon if present
                if '%z' in fmt and '+' in full_str and ':' in full_str[full_str.rfind('+',1):]:
                    tz_part_match = re.search(r'([+-]\d{2}):(\d{2})$', full_str)
                    if tz_part_match:
                        parsable_str = full_str[:tz_part_match.start()] + tz_part_match.group(1) + tz_part_match.group(2)
                        return datetime.strptime(parsable_str, fmt)
                return datetime.strptime(full_str, fmt)
            except ValueError:
                continue
        logger.warning(f"Could not parse datetime string: '{full_str}'")
        return None

    def _map_jsonld_to_event_schema(self, node: Dict, url: str, soup: BeautifulSoup) -> EventSchemaTypedDict:
        """Maps a JSON-LD node (expected to be MusicEvent) to EventSchemaTypedDict."""
        # This function would be very detailed, similar to input_file_3.py's version,
        # parsing location, offers, performer, organizer, etc.
        # For brevity, a simplified mapping is shown here.
        
        event: EventSchemaTypedDict = {"url": url} # type: ignore # Base fields

        event["title"] = node.get("name")
        event["fullDescription"] = node.get("description")
        event["descriptionFormat"] = "text" # JSON-LD descriptions are usually text

        # Location
        loc_node = node.get("location", {})
        if isinstance(loc_node, list): loc_node = loc_node[0] if loc_node else {}
        if isinstance(loc_node, dict):
            addr_node = loc_node.get("address", {})
            addr_str = ""
            if isinstance(addr_node, str): addr_str = addr_node
            elif isinstance(addr_node, dict):
                addr_str = ", ".join(filter(None, [
                    addr_node.get("streetAddress"), addr_node.get("addressLocality"),
                    addr_node.get("postalCode"), addr_node.get("addressCountry")
                ]))
            
            geo = loc_node.get("geo", {})
            coords = None
            if geo.get("latitude") and geo.get("longitude"):
                try: coords = {"lat": float(geo["latitude"]), "lng": float(geo["longitude"])}
                except ValueError: pass

            event["location"] = {
                "venue": loc_node.get("name"), "address": addr_str or None,
                "city": addr_node.get("addressLocality") if isinstance(addr_node, dict) else None,
                "country": addr_node.get("addressCountry") if isinstance(addr_node, dict) else None,
                "postalCode": addr_node.get("postalCode") if isinstance(addr_node, dict) else None,
                "coordinates": coords
            }

        # DateTime
        start_dt_str = node.get("startDate")
        end_dt_str = node.get("endDate")
        door_time_str = node.get("doorTime") # Often just time
        
        parsed_start = self._parse_datetime_flexible(start_dt_str)
        parsed_end = self._parse_datetime_flexible(end_dt_str)
        # Door time might need combining with start_date if it's just a time
        parsed_doors = self._parse_datetime_flexible(door_time_str if 'T' in str(door_time_str) else (str(start_dt_str).split('T')[0] + 'T' + str(door_time_str) if start_dt_str and door_time_str else door_time_str) )


        event["dateTime"] = {
            "displayText": f"From {start_dt_str}" + (f" to {end_dt_str}" if end_dt_str else ""),
            "parsed": {
                "startDate": parsed_start, "endDate": parsed_end, "doorTime": parsed_doors,
                "timeZone": parsed_start.tzname() if parsed_start and parsed_start.tzinfo else None
            },
            "dayOfWeek": parsed_start.strftime("%A") if parsed_start else None
        }
        
        # LineUp
        performers = node.get("performer", [])
        if isinstance(performers, dict): performers = [performers]
        event["lineUp"] = [{"name": p.get("name"), "headliner": (idx == 0)} for idx, p in enumerate(performers) if isinstance(p, dict) and p.get("name")]
        
        # Offers (Tickets)
        offers = node.get("offers", [])
        if isinstance(offers, dict): offers = [offers]
        if offers and isinstance(offers[0], dict):
            first_offer = offers[0]
            prices = [float(o["price"]) for o in offers if isinstance(o,dict) and o.get("price") and str(o["price"]).replace('.','',1).isdigit()]
            
            event["ticketInfo"] = {
                "startingPrice": min(prices) if prices else None,
                "currency": first_offer.get("priceCurrency"),
                "status": first_offer.get("availability"),
                "url": first_offer.get("url"),
                "tiers": [{"name": o.get("name"), "price": float(o["price"]) if o.get("price") else None, "currency": o.get("priceCurrency")} for o in offers if isinstance(o,dict)]
            }
            event["ticketsUrl"] = first_offer.get("url")

        # Images
        img = node.get("image")
        if isinstance(img, str): event["images"] = [img]
        elif isinstance(img, list): event["images"] = [i for i in img if isinstance(i, str)]
        
        # Organizer
        org = node.get("organizer")
        if isinstance(org, list): org = org[0] if org else {}
        if isinstance(org, dict):
            event["organizer"] = {"name": org.get("name"), "url": org.get("url")}
            event["promoter"] = event["organizer"] # Often the same

        return event # type: ignore

    def _map_fallback_to_event_schema(self, raw_data: Dict[str, Any], url: str, soup: BeautifulSoup) -> EventSchemaTypedDict:
        """Maps combined raw data from fallback layers to EventSchemaTypedDict."""
        # This function would intelligently combine data from raw_data['meta'], 
        # raw_data['html_selectors'], raw_data['regex']
        event: EventSchemaTypedDict = {"url": url} # type: ignore

        # Title: Prioritize HTML selector, then OG, then basic H1
        event["title"] = raw_data.get("html_selectors", {}).get("title") or \
                         raw_data.get("meta", {}).get("og_title") or \
                         (soup.find("h1").get_text(strip=True) if soup.find("h1") else None)

        # Description: Prioritize HTML selector, then OG description
        event["fullDescription"] = raw_data.get("html_selectors", {}).get("description") or \
                                   raw_data.get("meta", {}).get("og_description") or \
                                   raw_data.get("meta", {}).get("meta_description")
        event["descriptionFormat"] = "html" if event["fullDescription"] and "<" in event["fullDescription"] else "text"


        # Location (example - needs robust combination)
        event["location"] = {
            "venue": raw_data.get("html_selectors", {}).get("venue") or raw_data.get("regex", {}).get("venue"),
            "address": raw_data.get("html_selectors", {}).get("address"),
            "city": raw_data.get("html_selectors", {}).get("city"),
        }

        # DateTime (example)
        dt_text = raw_data.get("html_selectors", {}).get("date_text") or raw_data.get("regex", {}).get("date_text")
        parsed_start = self._parse_datetime_flexible(dt_text)
        event["dateTime"] = {
            "displayText": dt_text,
            "parsed": {"startDate": parsed_start},
            "dayOfWeek": parsed_start.strftime("%A") if parsed_start else None
        }
        
        # Lineup (example from HTML selectors if defined)
        lineup_raw = raw_data.get("html_selectors", {}).get("lineup_artists") # Expects a list
        if isinstance(lineup_raw, list):
            event["lineUp"] = [{"name": str(artist_name), "headliner": (idx==0)} for idx, artist_name in enumerate(lineup_raw)]
        elif isinstance(lineup_raw, str): # Single artist found
             event["lineUp"] = [{"name": lineup_raw, "headliner": True}]


        # Tickets (example)
        price_raw = raw_data.get("html_selectors", {}).get("price") or raw_data.get("regex", {}).get("price")
        ticket_url_raw = raw_data.get("html_selectors", {}).get("ticket_url")
        
        starting_price, currency = None, None
        if price_raw:
            price_match = re.search(r'([\d\.,]+)', str(price_raw).replace(',',''))
            if price_match:
                try: starting_price = float(price_match.group(1).replace(',','.'))
                except ValueError: pass
            if "€" in str(price_raw): currency = "EUR" # Basic currency detection
            elif "$" in str(price_raw): currency = "USD"
            elif "£" in str(price_raw): currency = "GBP"

        event["ticketInfo"] = {"startingPrice": starting_price, "currency": currency, "url": ticket_url_raw}
        event["ticketsUrl"] = ticket_url_raw

        # Images (from meta or HTML selectors)
        img_url = raw_data.get("html_selectors", {}).get("image_url") or raw_data.get("meta", {}).get("og_image")
        if img_url: event["images"] = [img_url] if isinstance(img_url, str) else img_url

        return event # type: ignore

    def _populate_derived_fields(self, event_data: EventSchemaTypedDict) -> None:
        """Populates fields derived from other data (e.g., counts, booleans)."""
        event_data["lastCheckedAt"] = datetime.utcnow() # Always update this

        # Artist Count
        event_data["artistCount"] = len(event_data.get("lineUp", []))
        # Image Count
        event_data["imageCount"] = len(event_data.get("images", []))

        # Ticket Booleans
        ti = event_data.get("ticketInfo")
        has_ti_flag = False
        is_free_flag = False
        is_sold_out_flag = False

        if ti:
            has_price = ti.get("startingPrice") is not None
            has_url = bool(ti.get("url","").strip())
            has_tiers = bool(ti.get("tiers"))
            has_display_text = bool(ti.get("displayText","").strip())
            has_ti_flag = has_price or has_url or has_tiers or has_display_text

            if has_ti_flag:
                if ti.get("startingPrice") == 0 and not (ti.get("startingPrice") is not None and ti.get("startingPrice",0) > 0) : # Price is explicitly 0
                    is_free_flag = True
                
                status_lower = (ti.get("status") or "").lower()
                display_lower = (ti.get("displayText") or "").lower()
                if "free" in status_lower or "free" in display_lower:
                    if not (ti.get("startingPrice") is not None and ti.get("startingPrice",0) > 0): # Text says free, and no price > 0
                        is_free_flag = True
                
                # If there's a price > 0, it's not free, regardless of text
                if ti.get("startingPrice") is not None and ti.get("startingPrice",0) > 0:
                    is_free_flag = False

                sold_out_kws = ["soldout", "sold out", "unavailable", "off-sale", "offsale", "agotado"]
                if any(kw in status_lower for kw in sold_out_kws) or \
                   any(kw in display_lower for kw in sold_out_kws):
                    is_sold_out_flag = True
        
        event_data["hasTicketInfo"] = has_ti_flag
        event_data["isFree"] = is_free_flag
        event_data["isSoldOut"] = is_sold_out_flag


    def close(self):
        if self.browser:
            try: self.browser.close()
            except Exception as e: logger.debug(f"Error closing browser: {e}")
            self.browser = None
        if self.playwright_context:
            try: self.playwright_context.stop()
            except Exception as e: logger.debug(f"Error stopping Playwright context: {e}")
            self.playwright_context = None
        logger.info("Scraper resources closed.")

# --- Crawling Function ---
def crawl_listing_page(
    listing_url: str,
    scraper: "MegaEventScraper",
    max_events_to_scrape: int
) -> List[EventSchemaTypedDict]:
    
    if not scraper.config.get("allow_browser_use", True) or not PLAYWRIGHT_AVAILABLE:
        logger.error("Playwright is required for crawling listings but not available/enabled.")
        return []
    if not scraper._ensure_playwright_started(): return []

    logger.info(f"Starting crawl of listing page: {listing_url}")
    page: Optional[PlaywrightPage] = None
    collected_event_links: Set[str] = set() # Use a set to store unique links

    try:
        context_options: Dict[str, Any] = {'user_agent': scraper.current_user_agent}
        # Add other context options from profile if needed
        page = scraper.browser.new_page(**context_options) # type: ignore
        
        page.goto(listing_url, 
                  timeout=scraper.site_profile.get("playwright_goto_timeout", 60000),
                  wait_until=scraper.site_profile.get("playwright_wait_until_listing_load", "networkidle"))
        
        scraper._handle_overlays(page)
        
        # Execute initial actions on listing page if any
        if scraper.site_profile.get("listing_page_initial_actions"):
            scraper._execute_playwright_actions(page, scraper.site_profile["listing_page_initial_actions"])

        # Pagination/Scrolling logic
        pagination_config = scraper.site_profile.get("listing_pagination", {})
        max_pages_or_scrolls = pagination_config.get("max_iterations", 5)
        
        for i in range(max_pages_or_scrolls + 1): # +1 for initial page
            if len(collected_event_links) >= max_events_to_scrape:
                logger.info(f"Reached max_events_to_scrape limit ({max_events_to_scrape}).")
                break

            logger.info(f"Processing listing page iteration {i+1}...")
            
            # Extract links
            link_rules = scraper.site_profile.get("listing_event_link_rules", [])
            current_page_links: Set[str] = set()

            for rule in link_rules:
                selector = rule.get("selector")
                if not selector: continue
                
                try:
                    page.wait_for_selector(selector, timeout=rule.get("wait_timeout", 10000), state="visible")
                    elements = page.locator(selector).all()
                    for elem in elements:
                        href_attr = elem.get_attribute("href")
                        if href_attr:
                            full_url = urljoin(page.url, href_attr)
                            # Apply filtering rules
                            include_regex = rule.get("include_if_matches_regex")
                            exclude_regex = rule.get("exclude_if_matches_regex")
                            
                            if exclude_regex and re.search(exclude_regex, full_url):
                                logger.debug(f"Link excluded by regex '{exclude_regex}': {full_url}")
                                continue
                            if include_regex and not re.search(include_regex, full_url):
                                logger.debug(f"Link did not match include_regex '{include_regex}': {full_url}")
                                continue
                            current_page_links.add(full_url)
                except PlaywrightTimeoutError:
                    logger.warning(f"Timeout waiting for event link selector '{selector}' on iteration {i+1}.")
                except Exception as e:
                    logger.error(f"Error extracting links with selector '{selector}': {e}")
            
            newly_found_links = len(current_page_links - collected_event_links)
            collected_event_links.update(current_page_links)
            logger.info(f"Found {newly_found_links} new links this iteration. Total unique links: {len(collected_event_links)}.")

            if i >= max_pages_or_scrolls: break # Reached max iterations

            # Perform pagination action
            pagination_action = pagination_config.get("action")
            if not pagination_action: break # No more pagination

            pagination_selector = pagination_action.get("selector")
            action_type = pagination_action.get("type", "click") # click, scroll_to_bottom
            
            try:
                if action_type == "click" and pagination_selector:
                    next_button = page.locator(pagination_selector).first
                    if next_button.is_visible(timeout=5000) and next_button.is_enabled(timeout=5000):
                        logger.info(f"Paginating: Clicking '{pagination_selector}'")
                        scraper._human_click(page, next_button)
                        page.wait_for_load_state(pagination_action.get("wait_after", "networkidle"), timeout=30000)
                    else: logger.info("Pagination element not interactable. Ending crawl."); break
                elif action_type == "scroll_to_bottom":
                    logger.info("Paginating: Scrolling to bottom")
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(pagination_action.get("wait_after_s", 3)) # Wait for content to load
                else: logger.warning(f"Unknown pagination action type: {action_type}"); break
            except Exception as e:
                logger.error(f"Error during pagination: {e}"); break
            
            time.sleep(random.uniform(1,3)) # Pause after pagination

    except Exception as e:
        logger.exception(f"Critical error during listing crawl of {listing_url}: {e}")
        if page: scraper._save_debug_snapshot(page, listing_url, stage="crawl_error")
    finally:
        if page: page.close()

    scraped_events_data: List[EventSchemaTypedDict] = []
    links_to_scrape = list(collected_event_links)[:max_events_to_scrape]
    logger.info(f"Proceeding to scrape details for {len(links_to_scrape)} event URLs.")

    for idx, event_url in enumerate(links_to_scrape):
        logger.info(f"\n--- Scraping Event {idx+1}/{len(links_to_scrape)}: {event_url} ---")
        event_data = scraper.scrape_event_strategically(event_url)
        if event_data.get("title"):
            scraped_events_data.append(event_data)
        else:
            logger.warning(f"No meaningful data (title) extracted for {event_url}.")
        
        # Delay between scraping individual event pages
        time.sleep(random.uniform(
            scraper.site_profile.get("requests_min_delay_s", scraper.config.get("requests_min_delay_s", 0.7)) * 0.8, # Slightly less delay during crawl
            scraper.site_profile.get("requests_max_delay_s", scraper.config.get("requests_max_delay_s", 1.5)) * 0.8
        ))
    return scraped_events_data

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Mega Event Scraper")
    parser.add_argument("target_url", help="URL to scrape (single event or listing if --crawl).")
    parser.add_argument("--crawl-listing", action="store_true", help="Crawl target_url as a listing page.")
    parser.add_argument("--site-profile", type=str, required=True, help="Path to JSON site profile.")
    parser.add_argument("--output-base", type=str, default="scraped_events", help="Base name for output files.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR, help="Directory for output files.")
    parser.add_argument("--snapshot-dir", type=str, default=DEFAULT_SNAPSHOT_DIR, help="Directory for debug snapshots.")
    parser.add_argument("--max-events-from-crawl", type=int, default=20, help="Max events to scrape from a listing.")
    
    # Global Configs / Overrides
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Logging level.")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True, help="Run Playwright headless.")
    parser.add_argument("--no-browser", action="store_true", help="Disable Playwright usage entirely.")
    parser.add_argument("--proxy", type=str, default=None, help="Proxy server (e.g., http://host:port).")
    parser.add_argument("--debug-mode", action="store_true", help="Enable extra debugging features like more snapshots.")

    args = parser.parse_args()

    # Set log level
    logger.setLevel(getattr(logging, args.log_level.upper()))

    # Load site profile
    try:
        with open(args.site_profile, 'r', encoding='utf-8') as f:
            site_profile_data = json.load(f)
        logger.info(f"Successfully loaded site profile: {args.site_profile}")
    except FileNotFoundError:
        logger.error(f"Site profile file not found: {args.site_profile}"); sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from site profile: {args.site_profile}"); sys.exit(1)
    
    # Prepare global config for the scraper instance
    global_scraper_config = {
        "headless": args.headless,
        "allow_browser_use": not args.no_browser,
        "proxy": args.proxy,
        "snapshot_dir": args.snapshot_dir,
        "debug_mode": args.debug_mode,
        # Add other global defaults from argparse if needed (e.g. playwright_slow_mo, delays)
        "default_headers": { # Example default headers
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
    }

    scraper = MegaEventScraper(
        site_profile=site_profile_data,
        global_config=global_scraper_config,
        user_agents=MODERN_USER_AGENTS # Could also be loaded from a file via args
    )

    all_events_data: List[EventSchemaTypedDict] = []
    try:
        if args.crawl_listing:
            all_events_data = crawl_listing_page(args.target_url, scraper, args.max_events_from_crawl)
        else:
            event_data = scraper.scrape_event_strategically(args.target_url)
            if event_data.get("title"):
                all_events_data.append(event_data)
        
        # Output results
        if all_events_data:
            output_path = Path(args.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            json_file = output_path / f"{args.output_base}_{timestamp}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump({"source_info": {"url": args.target_url, "profile": args.site_profile}, 
                           "events": all_events_data}, 
                          f, indent=2, default=datetime_serializer, ensure_ascii=False)
            logger.info(f"Saved {len(all_events_data)} events to {json_file}")

            # Optional: Markdown output (can be very long for many events)
            # md_file = output_path / f"{args.output_base}_{timestamp}.md"
            # with open(md_file, "w", encoding="utf-8") as f:
            #    f.write(f"# Scraped Events: {args.target_url} ({timestamp})\n\n")
            #    for event in all_events_data:
            #        f.write(format_event_to_markdown(event)) # You'd need a format_event_to_markdown function
            # logger.info(f"Saved Markdown summary to {md_file}")
        else:
            logger.info("No events were successfully scraped or processed.")

    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user.")
    except Exception as e:
        logger.exception(f"An unhandled error occurred in main execution: {e}")
    finally:
        scraper.close()
        logger.info("Script finished.")

if __name__ == "__main__":
    main()
