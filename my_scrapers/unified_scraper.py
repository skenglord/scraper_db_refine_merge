#!/usr/bin/env python3
"""
Unified Ibiza Spotlight Event Scraper (v1.2 - Refined)

This script provides two modes for scraping ibiza-spotlight.com:
1. 'scrape': Scrapes a single event detail page for comprehensive data.
2. 'crawl': Crawls a monthly calendar (e.g., for May 2025), handling 
             weekly pagination, and scrapes details for all events found.

It uses Playwright with stealth for dynamic content and robust parsing.
Incorporates insights from user-provided XPaths for better targeting.
"""
import argparse
import csv
import json
import random
import re
import time
import traceback
from dataclasses import dataclass, asdict, fields
from datetime import datetime, date, time as dt_time
from pathlib import Path
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Tag
import mistune
from classy_skkkrapey.utils.cleanup_html import cleanup_html
from classy_skkkrapey.config import settings
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure
from schema_adapter import map_to_unified_schema # Assuming schema_adapter.py is in project root or PYTHONPATH
import logging

# Setup logger for this module
logger = logging.getLogger(__name__)
# Basic logging configuration (if not configured globally)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


try:
    from playwright.sync_api import sync_playwright, Browser, Locator, TimeoutError as PlaywrightTimeoutError
    from playwright.sync_api._generated import Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    sync_playwright, Page, Browser, Locator, PlaywrightTimeoutError = (None,) * 5  # type: ignore
    PLAYWRIGHT_AVAILABLE = False

# --- Configuration ---
SNAPSHOT_DIR = Path("debug_snapshots")
OUTPUT_DIR = Path("output")
BASE_URL = "https://www.ibiza-spotlight.com"

MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# Ensure directories exist
SNAPSHOT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# --- Data Model ---
@dataclass
class Event:
    """Dataclass for a single event, designed for comprehensive detail page scraping."""
    url: str
    title: Optional[str] = None
    venue: Optional[str] = None
    date_text: Optional[str] = None # Raw date string from page
    start_date: Optional[date] = None # Parsed start date
    end_date: Optional[date] = None   # Parsed end date (if range)
    start_time: Optional[dt_time] = None
    end_time: Optional[dt_time] = None
    price_text: Optional[str] = None # Raw price string
    price_value: Optional[float] = None
    currency: Optional[str] = "EUR"
    lineup: Optional[List[str]] = None
    description: Optional[str] = None
    promoter: Optional[str] = None
    categories: Optional[List[str]] = None
    scraped_at: Optional[datetime] = None
    extraction_method: Optional[str] = "detail_page_html"

    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow()
        if self.lineup is None:
            self.lineup = []
        if self.categories is None:
            self.categories = []

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to a dictionary for serialization."""
        data = {}
        for field_info in fields(self): # Use fields() for dataclasses
            value = getattr(self, field_info.name)
            if isinstance(value, (date, dt_time, datetime)):
                data[field_info.name] = value.isoformat()
            else:
                data[field_info.name] = value
        return data

# --- Scraper Class ---
class IbizaSpotlightUnifiedScraper:
    """A stealthy, robust scraper for ibiza-spotlight.com with scrape and crawl modes."""
    
    def __init__(self, headless: bool = True, min_delay: float = 2.5, max_delay: float = 6.0):
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is not installed. Run: pip install playwright beautifulsoup4 requests && playwright install")
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.browser: Any = None
        self.playwright_context: Any = None
        self._ensure_browser()

        # Initialize MongoDB client
        try:
            # Use settings for MONGODB_URI and DB_NAME
            self.mongo_client = MongoClient(settings.MONGODB_URI)
            self.db = self.mongo_client[settings.DB_NAME]
            self.events_collection = self.db.events # Assuming collection name is 'events'
            logger.info(f"Successfully connected to MongoDB: {settings.DB_NAME} on {settings.MONGODB_URI}")
        except ConnectionFailure as e:
            logger.error(f"Could not connect to MongoDB: {e}")
            self.mongo_client = None
            self.db = None
            self.events_collection = None
        except AttributeError as e:
            logger.error(f"MongoDB settings (MONGODB_URI or DB_NAME) not found in config: {e}")
            self.mongo_client = None
            self.db = None
            self.events_collection = None


    def _get_random_delay(self, multiplier: float = 1.0) -> None:
        time.sleep(random.uniform(self.min_delay * multiplier, self.max_delay * multiplier))

    def _ensure_browser(self):
        if not self.browser or not self.browser.is_connected():
            print("[INFO] Starting/Re-starting Playwright browser...")
            if self.playwright_context:
                try: self.playwright_context.stop()
                except Exception as e: print(f"[DEBUG] Non-critical error stopping old Playwright context: {e}")
            self.playwright_context = sync_playwright().start()
            self.browser = self.playwright_context.chromium.launch(headless=self.headless)
            print("[INFO] Playwright browser started.")


    def _human_click(self, page: Any, locator: Any, timeout: int = 10000):
        try:
            print(f"[DEBUG] Attempting human_click on locator...")
            locator.wait_for(state="visible", timeout=timeout)
            bounding_box = locator.bounding_box()
            if not bounding_box:
                print(f"[WARNING] Could not get bounding box for locator. Using direct click.")
                locator.click(timeout=timeout) # Use Playwright's click which waits
                return

            # Add small random offsets to click point
            target_x = bounding_box['x'] + (bounding_box['width'] * random.uniform(0.25, 0.75))
            target_y = bounding_box['y'] + (bounding_box['height'] * random.uniform(0.25, 0.75))
            
            print(f"[DEBUG] Moving mouse to element area: ({target_x:.0f}, {target_y:.0f})")
            page.mouse.move(target_x, target_y, steps=random.randint(8, 15))
            self._get_random_delay(0.15) # Shorter pause before click
            
            print(f"[DEBUG] Performing mouse click at ({target_x:.0f}, {target_y:.0f})")
            page.mouse.click(target_x, target_y) # Using mouse.click
            print(f"[INFO] Human-like click potentially successful.")
            self._get_random_delay(0.6) # Pause after click for page to react
        except PlaywrightTimeoutError:
            print(f"[WARNING] Locator not visible for human_click within {timeout/1000}s. Trying direct click.")
            try: locator.click(timeout=timeout)
            except Exception as direct_click_err: print(f"[ERROR] Direct click also failed: {direct_click_err}")
        except Exception as e:
            print(f"[WARNING] Human-like click failed: {e}. Falling back to direct click.")
            try: locator.click(timeout=timeout)
            except Exception as click_err: print(f"[ERROR] Direct click also failed: {click_err}")


    def _handle_overlays(self, page: Page):
        overlay_selectors = [
            'a.cb-seen-accept', 'button#onetrust-accept-btn-handler',
            'button[data-testid="accept-all-cookies"]', 'button:has-text("Accept all")',
            'button:has-text("Accept Cookies")', 'button:has-text("I agree")',
            'button:has-text("No problem")', '[aria-label="close"]', '[aria-label="Close"]',
            '.modal-close', 'button.close', '.cookie-banner-accept-button' # Added a generic one
        ]
        print("[INFO] Checking for overlays and cookie banners...")
        self._get_random_delay(0.3) # Slightly shorter delay before check
        overlay_handled = False
        for selector in overlay_selectors:
            try:
                # Use page.query_selector to check existence without strict timeout initially
                if page.query_selector(selector):
                    button_locator = page.locator(selector).first
                    if button_locator.is_visible(timeout=2500): # Quick check for visibility
                        print(f"[INFO] Found overlay button: '{selector}'. Attempting click...")
                        self._human_click(page, button_locator, timeout=5000)
                        overlay_handled = True
                        print(f"[INFO] Clicked overlay with selector: {selector}.")
                        # Wait a bit for overlay to disappear
                        time.sleep(random.uniform(1.0, 2.0)) 
                        break # Assume one primary overlay
            except PlaywrightTimeoutError: continue # Not visible within quick check
            except Exception as e: print(f"[DEBUG] Error trying overlay selector '{selector}': {e}"); continue
        
        if not overlay_handled:
            print("[INFO] No primary overlays found or handled on main page. Checking iframes (basic check).")
            for frame in page.frames[1:]: # Skip main frame
                for selector in overlay_selectors:
                    try:
                        if frame.query_selector(selector): # Check existence in frame
                            button_locator = frame.locator(selector).first
                            if button_locator.is_visible(timeout=2000):
                                print(f"[INFO] Found overlay button in iframe: '{selector}'. Clicking...")
                                self._human_click(page, button_locator, timeout=5000) # Use page context for click
                                overlay_handled = True
                                time.sleep(random.uniform(1.0, 2.0))
                                break
                    except Exception: continue
                if overlay_handled: break
        
        if not overlay_handled:
            print("[INFO] No overlays actively handled.")
        else:
            print("[INFO] Overlay handling complete.")


    def fetch_page_html(self, url: str, wait_for_content_selector: Optional[str] = None) -> str:
        self._ensure_browser()
        page: Any = None  # Changed type to `Any` to avoid type expression error
        try:
            page = self.browser.new_page(user_agent=random.choice(MODERN_USER_AGENTS))
            print("[INFO] Navigating to:", url)
            page.goto(url, wait_until="domcontentloaded", timeout=75000)  # Increased timeout
            
            self._handle_overlays(page)
            
            content_ready_selector = wait_for_content_selector if wait_for_content_selector else "body"
            print(f"[INFO] Waiting for main content ('{content_ready_selector}')...")
            page.wait_for_selector(content_ready_selector, timeout=45000, state="visible")
            return page.content()
        except Exception as e:
            print(f"[ERROR] Playwright fetch failed for {url}: {e}")
            raise
        finally:
            if page:
                page.close()

    def _get_raw_details_from_html(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parses the HTML content of an event detail page and extracts raw information
        into a dictionary. This dictionary will serve as raw_data for map_to_unified_schema.
        """
        logger.info(f"Parsing event detail page for raw data: {url}") # Use logger
        soup = BeautifulSoup(html_content, "lxml")
        raw_details: Dict[str, Any] = {"url": url} # Start with the URL

        # --- SELECTORS (same as before, but will populate a dict) ---
        selectors = {
            "title": "h1.eventTitle, h1.article-title, main h1, article header h1",
            "venue_name": "a[href*='/club/'], .promoter-info a[href*='/night/clubs/'], .venue-name-class", # Key changed
            "raw_date_string": ".event-date-time-class, .event-info .date, time[datetime]", # Key changed
            "raw_time_string": ".event-date-time-class, .event-info .time", # Key changed
            "raw_price_string": ".price-info-class, .ticket-price-class, .buy-tickets .price", # Key changed
            "lineup_container": ".lineup-section, .dj-list-container, #lineup",
            "dj_item_selector": "li, .artist-name, .dj-name",
            "full_description_html": "div.event-description-text, article div.article-content, section#description", # Key changed for clarity
            "promoter_name": ".promoter-link a, .event-by-promoter", # Key changed
            "categories_container": ".event-tags, .category-list",
            "category_item_selector": "a, .tag-item"
        }
        # --- END OF SELECTORS ---

        title_elem = soup.select_one(selectors["title"])
        if title_elem: raw_details["title"] = title_elem.get_text(strip=True)
        else: logger.warning(f"No title found on detail page: {url}")

        venue_elem = soup.select_one(selectors["venue_name"])
        if venue_elem: raw_details["venue"] = venue_elem.get_text(strip=True) # Changed key to 'venue' for adapter
        
        date_text_elem = soup.select_one(selectors["raw_date_string"])
        if date_text_elem:
            raw_details["raw_date_string"] = date_text_elem.get('datetime') or date_text_elem.get_text(strip=True)
            try:
                if raw_details["raw_date_string"]:
                    parsed_dt = None
                    try: parsed_dt = datetime.fromisoformat(raw_details["raw_date_string"].replace('Z', '+00:00'))
                    except ValueError:
                        for fmt in ("%d %b %Y", "%d %B %Y", "%A %d %B %Y", "%a %d %b %Y"): # Added short day format
                            try:
                                year_in_url_match = re.search(r'/(\d{4})/', url)
                                year_context = str(datetime.now().year) # Default to current year
                                if year_in_url_match : year_context = year_in_url_match.group(1)

                                date_to_parse = raw_details["raw_date_string"]
                                if not re.search(r'\d{4}', date_to_parse): date_to_parse += f" {year_context}"
                                parsed_dt = datetime.strptime(date_to_parse, fmt)
                                break
                            except ValueError: continue
                    if parsed_dt:
                        raw_details["datetime_obj"] = parsed_dt
            except Exception as e_date:
                logger.debug(f"Could not parse date from raw_date_string '{raw_details.get('raw_date_string')}': {e_date}")

        time_text_elem = soup.select_one(selectors["raw_time_string"])
        if time_text_elem:
             raw_details["raw_time_string"] = time_text_elem.get_text(strip=True)

        price_elem = soup.select_one(selectors["raw_price_string"])
        if price_elem: 
            raw_details["price_text"] = price_elem.get_text(strip=True) # Changed key to 'price_text'
            
        lineup_container = soup.select_one(selectors["lineup_container"])
        if lineup_container:
            dj_elements = lineup_container.select(selectors["dj_item_selector"])
            # The adapter expects a list of dicts for artists if possible.
            # Creating basic artist dicts here.
            artists_list = []
            for dj_elem in dj_elements:
                dj_name = dj_elem.get_text(strip=True)
                if dj_name:
                    artists_list.append({"name": dj_name, "role": "dj"}) # Basic structure
            if artists_list: raw_details["artists"] = artists_list # Changed key to 'artists'
        
        desc_elem = soup.select_one(selectors["full_description_html"])
        # Pass HTML string for description; adapter can handle cleaning or full text.
        if desc_elem: raw_details["full_description"] = str(desc_elem)

        promoter_elem = soup.select_one(selectors["promoter_name"])
        if promoter_elem: raw_details["promoter"] = promoter_elem.get_text(strip=True) # Changed key to 'promoter'

        categories_container = soup.select_one(selectors["categories_container"])
        if categories_container:
            cat_elements = categories_container.select(selectors["category_item_selector"])
            raw_details["genres"] = [cat.get_text(strip=True) for cat in cat_elements if cat.get_text(strip=True)] # Changed key to 'genres'
            
        # Attempt to extract JSON-LD data
        json_ld_script = soup.find("script", type="application/ld+json")
        if json_ld_script and json_ld_script.string:
            try:
                json_ld_content = json.loads(json_ld_script.string)
                # The adapter might expect the full JSON-LD or specific parts.
                # For now, let's pass the description if available.
                current_event_ld = None
                if isinstance(json_ld_content, list):
                    for item in json_ld_content:
                        if isinstance(item, dict) and item.get("@type") in ["Event", "MusicEvent"]:
                            current_event_ld = item
                            break
                elif isinstance(json_ld_content, dict) and json_ld_content.get("@type") in ["Event", "MusicEvent"]:
                     current_event_ld = json_ld_content

                if current_event_ld and current_event_ld.get("description"):
                    raw_details["json_ld_description"] = current_event_ld["description"]
                if current_event_ld : # Pass the whole JSON LD for the event if found
                    raw_details["json_ld_data"] = current_event_ld

            except json.JSONDecodeError:
                logger.warning(f"Could not parse JSON-LD from {url}")

        if not raw_details.get("title") and not raw_details.get("venue") and not raw_details.get("raw_date_string"):
             logger.warning(f"Very little raw data found for {url}. Adapter might struggle.")

        return raw_details

    def _parse_html_to_markdown_fallback(self, html_content: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Fallback method to extract all text from HTML and convert it to Markdown.
        Used when structured parsing fails to yield high-quality data.
        """
        print(f"[INFO] Attempting markdown fallback for {url}")
        try:
            # Clean up HTML before extracting text
            # Clean up HTML before extracting text
            _title, minimized_body, _link_urls, _image_urls, _script_content = cleanup_html(html_content, url)
            
            # Extract all visible text
            soup = BeautifulSoup(minimized_body, "lxml")
            full_text = soup.get_text(separator="\n", strip=True)

            # Convert to Markdown using mistune
            markdown_parser = mistune.create_markdown()
            markdown_content = markdown_parser(full_text)

            # Create a simplified Event object with the markdown content
            # This assumes 'description' can hold the full markdown text
            fallback_event = Event(
                url=url,
                title=f"Fallback Content for {url}", # Generic title
                description=markdown_content,
                scraped_at=datetime.utcnow(),
                extraction_method="markdown_fallback"
            )
            print(f"[INFO] Successfully generated markdown fallback for {url}")
            return fallback_event
        except Exception as e:
            print(f"[ERROR] Markdown fallback failed for {url}: {e}")
            traceback.print_exc()
            return None

    def _extract_event_links_from_calendar(self, html_content: str, base_url: str, calendar_page_url: str) -> List[str]:
        soup = BeautifulSoup(html_content, "lxml")
        links = set()
        # Primary selector based on observed structure and user XPaths
        primary_event_links = soup.select("li.partyCal-day div.card-ticket.partyCal-ticket a.trackEventSpotlight")
        # Fallback if primary doesn't yield results
        fallback_event_links = soup.select("a.trackEventSpotlight[href*='/night/events/']")

        candidate_links = primary_event_links if primary_event_links else fallback_event_links

        for link_tag in candidate_links:
            href = link_tag.get('href')
            if href:
                full_url = urljoin(base_url, href)
                # Filter out links that are likely calendar navigation rather than event details
                # Event detail URLs usually have a non-numeric slug after /year/month/ or /year/
                path_part = urlparse(full_url).path
                if "/night/events/" in path_part:
                    # Regex to check if the path ends like /YYYY/MM or /YYYY/MM/DD or /YYYY
                    # or contains 'daterange=' query parameter
                    if not re.search(r'/night/events/\d{4}(?:/\d{1,2}){0,2}/?$', path_part) and \
                       "daterange=" not in urlparse(full_url).query:
                        links.add(full_url)
        
        if not links:
            print(f"[DEBUG] No event links extracted from {calendar_page_url} using current selectors. Saving snapshot.")
            safe_page_name = urlparse(calendar_page_url).path.replace('/', '_').strip('_') if calendar_page_url else "unknown_calendar_page"
            snap_path = SNAPSHOT_DIR / f"no_links_on_calendar_{safe_page_name}_{int(time.time())}.html"
            try:
                Path(snap_path).write_text(html_content, encoding="utf-8", errors="replace")
                print(f"[DEBUG] Saved snapshot (no links found on calendar page) to: {snap_path}")
            except Exception as e: print(f"[ERROR] Could not save no_links_found snapshot: {e}")
        else:
            print(f"[INFO] Extracted {len(links)} potential event detail links from {calendar_page_url}.")
        return list(links)

    def _handle_calendar_pagination(self, page: Page) -> bool:
        print("[INFO] Checking for calendar weekly pagination...")
        try:
            # Mobile "Next week" button (more specific selector)
            mobile_next_button_locator = page.locator("ul.nav-week li.nav-next a.calendarNav").first
            if mobile_next_button_locator.is_visible(timeout=3000):
                print("[INFO] Found mobile 'Next week' link. Clicking...")
                self._human_click(page, mobile_next_button_locator)
                page.wait_for_load_state("networkidle", timeout=30000) # Increased wait
                print(f"[INFO] Paginated (mobile) to: {page.url}")
                return True

            # Desktop weekly navigation
            # Get all week navigation tab links: div.calendar-nav-container.weeknav > a.calendarNav
            all_week_nav_links = page.locator("div.calendar-nav-container.weeknav a.calendarNav").all()
            if not all_week_nav_links:
                print("[DEBUG] No desktop week navigation links found with 'div.calendar-nav-container.weeknav a.calendarNav'.")
                return False

            current_page_url_path = urlparse(page.url).path # Compare paths to ignore queries like daterange
            active_link_index = -1

            for i, link_locator in enumerate(all_week_nav_links):
                href_attr = link_locator.get_attribute("href")
                if href_attr:
                    link_path = urlparse(urljoin(BASE_URL, href_attr)).path
                    # Check if the parent container has 'active' class
                    parent_container = link_locator.locator("xpath=ancestor::div[contains(@class, 'calendar-nav-container') and contains(@class, 'weeknav')]")
                    if "active" in (parent_container.get_attribute("class") or ""):
                         active_link_index = i
                         print(f"[DEBUG] Found active week tab at index {i}: {link_locator.text_content(timeout=1000)}")
                         break
                    # Fallback if direct active class on parent not found, compare URL paths
                    if link_path == current_page_url_path and active_link_index == -1: # Only set if not already found by class
                        active_link_index = i
                        print(f"[DEBUG] Matched current URL to week tab at index {i} by path: {link_locator.text_content(timeout=1000)}")


            if active_link_index != -1 and active_link_index + 1 < len(all_week_nav_links):
                next_week_link_locator = all_week_nav_links[active_link_index + 1]
                print(f"[INFO] Found desktop 'Next week' link (index {active_link_index + 1}). Text: '{next_week_link_locator.text_content(timeout=1000)}'. Clicking...")
                self._human_click(page, next_week_link_locator)
                page.wait_for_load_state("networkidle", timeout=30000) # Increased wait
                print(f"[INFO] Paginated (desktop) to: {page.url}")
                return True
            else:
                if active_link_index == -1: print("[DEBUG] Could not determine active week for desktop pagination.")
                elif active_link_index + 1 >= len(all_week_nav_links) : print("[DEBUG] Active week is the last displayed week, no further desktop pagination via this method.")
                else: print("[DEBUG] Unknown state in desktop pagination.")


            print("[INFO] No further weekly pagination links found or applicable.")
            return False
        except PlaywrightTimeoutError:
            print("[INFO] No weekly pagination link found (timeout).")
            return False
        except Exception as e:
            print(f"[ERROR] Error during calendar pagination: {e}")
            traceback.print_exc()
            return False

    def scrape_single_event(self, event_url: str) -> Optional[str]: # Returns event_id or None
        logger.info(f"[MODE: SCRAPE] Scraping single event: {event_url}")
        try:
            html_content = self.fetch_page_html(event_url, wait_for_content_selector="main article, main div.content-article, #main-content article")
            
            raw_event_details_dict = self._get_raw_details_from_html(html_content, event_url)

            if raw_event_details_dict:
                # Ensure 'url' is in raw_event_details_dict if not already added by _get_raw_details_from_html
                if 'url' not in raw_event_details_dict:
                    raw_event_details_dict['url'] = event_url

                unified_event_doc = map_to_unified_schema(
                    raw_data=raw_event_details_dict,
                    source_platform="ibiza-spotlight-unified", # Or derive dynamically if needed
                    source_url=event_url
                )

                if unified_event_doc and unified_event_doc.get("event_id"):
                    self.save_event_to_db(unified_event_doc)
                    logger.info(f"Successfully processed and initiated save for event: {event_url}")
                    return unified_event_doc.get("event_id")
                else:
                    logger.error(f"Schema mapping failed for {event_url}. No event_id generated.")
                    # Optionally, attempt markdown fallback here if unified_event_doc is None or lacks critical info
                    # For now, just logging error.
                    return None
            else:
                logger.warning(f"Could not extract sufficient raw details from {event_url} to process with schema adapter.")
                # Consider a more robust fallback here, e.g., saving raw HTML or a screenshot
                # For example, try the markdown fallback if that's still desired for some cases:
                # markdown_data = self._parse_html_to_markdown_fallback(html_content, event_url)
                # if markdown_data:
                #     # This markdown_data is a dict, not an Event object.
                #     # Need to decide how to save this. For now, we're not saving it directly to DB via adapter.
                #     logger.info(f"Generated markdown fallback for {event_url}, but not saving to DB via this path.")
                return None

        except Exception as e:
            logger.error(f"Failed to scrape event {event_url}: {e}", exc_info=True)
            traceback.print_exc()
            return None

    def crawl_calendar(self, year: int, month: int) -> List[Optional[str]]: # Returns list of event_ids or Nones
        self._ensure_browser()
        page: Any = None
        processed_event_ids: List[Optional[str]] = []
        scraped_event_urls_this_session = set()

        try:
            page = self.browser.new_page(user_agent=random.choice(MODERN_USER_AGENTS))
            logger.info(f"Starting calendar crawl for {year}-{month:02d}")

            current_calendar_url = f"{BASE_URL}/night/events/{year}/{month:02d}"
            page.goto(current_calendar_url, wait_until="domcontentloaded", timeout=75000)
            self._get_random_delay()
            self._handle_overlays(page)

            page_count = 0
            max_pages_to_crawl = 30 # Safety break for pagination

            while page_count < max_pages_to_crawl:
                page_count += 1
                logger.info(f"Processing calendar page {page_count}: {page.url}")
                html_content = page.content()

                # Save snapshot of the calendar page for debugging link extraction
                # snap_path = SNAPSHOT_DIR / f"calendar_page_{year}_{month:02d}_week_{page_count}_{int(time.time())}.html"
                # try:
                #     Path(snap_path).write_text(html_content, encoding="utf-8", errors="replace")
                #     logger.debug(f"Saved calendar snapshot to: {snap_path}")
                # except Exception as e: logger.error(f"Could not save calendar snapshot: {e}")

                event_urls_on_page = self._extract_event_links_from_calendar(html_content, BASE_URL, page.url)

                if not event_urls_on_page:
                    logger.info(f"No event links found on calendar page: {page.url}. This might be the end of the calendar or an issue.")

                for event_url in event_urls_on_page:
                    if event_url not in scraped_event_urls_this_session:
                        self._get_random_delay() # Delay before scraping each detail page
                        event_id = self.scrape_single_event(event_url) # This now saves to DB
                        processed_event_ids.append(event_id)
                        scraped_event_urls_this_session.add(event_url)
                    else:
                        logger.info(f"Already scraped {event_url} in this session, skipping.")

                self._get_random_delay() # Delay after processing a calendar page's events
                if not self._handle_calendar_pagination(page):
                    logger.info("No further pagination found or pagination limit reached.")
                    break
                self._get_random_delay() # Delay after pagination

            logger.info(f"Finished crawling calendar for {year}-{month:02d}. Processed {len(processed_event_ids)} events.")
            return processed_event_ids

        except Exception as e:
            logger.error(f"Error during calendar crawl for {year}-{month:02d}: {e}", exc_info=True)
            return processed_event_ids # Return what was processed so far
        finally:
            if page:
                page.close()
    
    def close(self):
        if self.browser:
            try: self.browser.close()
            except Exception as e: print(f"[DEBUG] Error closing browser: {e}")
        if self.playwright_context:
            try: self.playwright_context.stop()
            except Exception as e: print(f"[DEBUG] Error stopping Playwright context: {e}")
        # Close MongoDB client
        if self.mongo_client:
            try:
                self.mongo_client.close()
                logger.info("MongoDB connection closed.")
            except Exception as e:
                logger.error(f"Error closing MongoDB connection: {e}")
        print("[INFO] Scraper resources closed.")

    def save_event_to_db(self, unified_event_doc: Dict[str, Any]):
        if not self.events_collection:
            logger.error("MongoDB not connected. Cannot save event.")
            # Optionally, print to console as a fallback
            # print(json.dumps(unified_event_doc, indent=2, default=str))
            return

        if not unified_event_doc or not unified_event_doc.get("event_id"):
            logger.error("Attempted to save an event with missing data or event_id.")
            return

        try:
            update_key = {"event_id": unified_event_doc["event_id"]}
            self.events_collection.update_one(
                update_key,
                {"$set": unified_event_doc},
                upsert=True
            )
            logger.info(f"Successfully saved/updated event to DB: {unified_event_doc.get('title', unified_event_doc['event_id'])}")
        except Exception as e:
            logger.error(f"Error saving event {unified_event_doc.get('event_id')} to MongoDB: {e}", exc_info=True)

# Commenting out for now, primary save path is DB
# def save_events_to_file(events: List[Event], filepath_base: Path, formats: List[str]):
#     if not events: print("[INFO] No events to save."); return
    
#     for event in events:
#         if event.extraction_method == "markdown_fallback" and "md" in formats:
#             # Use the specific path provided by the user for markdown fallback output
#             md_path = Path("/home/creekz/Projects/skrrraped_graph/single_event_test_output/scraped_event_www_ibizaspotlight_com_001.md")
#             md_path.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists
#             with md_path.open("w", encoding="utf-8") as f:
#                 f.write(event.description if event.description else "")
#             print(f"[INFO] Saved markdown fallback content to {md_path}")
#             # Do not save this event as JSON/CSV if it's a markdown fallback, as it's not structured data
#             continue

#     if "json" in formats:
#         json_path = filepath_base.with_suffix(".json")
#         # Filter out markdown fallback events from JSON/CSV output
#         json_events = [e for e in events if e.extraction_method != "markdown_fallback"]
#         if json_events:
#             with json_path.open("w", encoding="utf-8") as f:
#                 json.dump([e.to_dict() for e in json_events], f, indent=2, ensure_ascii=False)
#             print(f"[INFO] Saved {len(json_events)} structured events to {json_path}")
#         else:
#             print("[INFO] No structured events to save to JSON.")

#     if "csv" in formats and events:
#         csv_path = filepath_base.with_suffix(".csv")
#         # Filter out markdown fallback events from JSON/CSV output
#         csv_events = [e for e in events if e.extraction_method != "markdown_fallback"]
#         if csv_events:
#             # Ensure all possible keys are included in header, even if some events don't have them
#             all_keys = set()
#             for event in csv_events:
#                 all_keys.update(event.to_dict().keys())
#             fieldnames = sorted(list(all_keys))

#             with csv_path.open("w", newline="", encoding="utf-8") as f:
#                 writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
#                 writer.writeheader()
#                 for event in csv_events: writer.writerow(event.to_dict())
#             print(f"[INFO] Saved {len(csv_events)} structured events to {csv_path}")
#         else:
#             print("[INFO] No structured events to save to CSV.")

def main():
    parser = argparse.ArgumentParser(description="Unified Ibiza Spotlight Scraper v1.2 - Refined")
    parser.add_argument("action", choices=["scrape", "crawl"], help="Action: 'scrape' a single URL, or 'crawl' a monthly calendar.")
    parser.add_argument("--url", type=str, help="URL of single event detail page (for 'scrape' mode).")
    parser.add_argument("--month", type=int, help="Month (1-12) (for 'crawl' mode, e.g., 5 for May).")
    parser.add_argument("--year", type=int, help="Year (e.g., 2025) (for 'crawl' mode).")
    # Headless, output-dir, min-delay, max-delay are now handled by settings
    parser.add_argument("--format", nargs='+', choices=["json", "csv", "md"], default=[], help="Output format(s) (Primarily for non-DB saving, less relevant now).")
    args = parser.parse_args()

    if args.action == "scrape":
        if not args.url: parser.error("--url is required for 'scrape' mode.")
        if not urlparse(args.url).scheme or not urlparse(args.url).netloc: parser.error("--url must be a full URL.")
    elif args.action == "crawl":
        if args.month is None or args.year is None: parser.error("--month and --year are required for 'crawl' mode.")
        if not (1 <= args.month <= 12): parser.error("Month must be 1-12.")
        if args.year < 2000 or args.year > datetime.now().year + 5: parser.error(f"Year seems invalid ({args.year}). Please provide a realistic year.")

    # Output directory from settings is used for snapshots, not primary data now
    Path(settings.SCRAPER_DEFAULT_OUTPUT_DIR).mkdir(exist_ok=True, parents=True)

    scraper = None
    try:
        scraper = IbizaSpotlightUnifiedScraper(
            headless=settings.SCRAPER_DEFAULT_HEADLESS,
            min_delay=settings.SCRAPER_DEFAULT_MIN_DELAY,
            max_delay=settings.SCRAPER_DEFAULT_MAX_DELAY
        )

        if not scraper.events_collection:
            logger.critical("MongoDB connection failed. Aborting script.")
            return

        if args.action == "scrape":
            event_id = scraper.scrape_single_event(args.url)
            if event_id:
                logger.info(f"Scrape successful for event URL {args.url}. Event ID: {event_id}")
            else:
                logger.warning(f"Scrape failed or no data processed for event URL {args.url}.")
        elif args.action == "crawl":
            processed_event_ids = scraper.crawl_calendar(args.year, args.month)
            successful_saves = sum(1 for _id in processed_event_ids if _id is not None)
            logger.info(f"Crawl completed for {args.year}-{args.month:02d}. Successfully processed and saved {successful_saves} events to DB.")
            if not processed_event_ids:
                logger.info("No events were processed during the crawl.")
            
    except KeyboardInterrupt: logger.info("\n[INFO] Scraping interrupted by user.")
    except ImportError as e:
        logger.critical(f"[FATAL ERROR] A required library is missing: {e}. Please install dependencies.")
        print("Try: pip install playwright beautifulsoup4 requests")
        print("And then: playwright install") # Remind user to install browser drivers
    except Exception as e:
        print(f"[FATAL ERROR] An unhandled error occurred: {e}")
        traceback.print_exc()
    finally:
        if scraper: scraper.close()
        print("[INFO] Script finished.")

if __name__ == "__main__":
    main()
