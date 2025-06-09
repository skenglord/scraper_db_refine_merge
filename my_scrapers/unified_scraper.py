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
        self.browser: Any = None  # Changed type to `Any` to avoid type expression error
        self.playwright_context: Any = None  # Changed type to `Any` to avoid type expression error
        self._ensure_browser()

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

    def _parse_event_detail_page_content(self, html_content: str, url: str) -> Optional[Event]:
        print(f"[INFO] Parsing event detail page: {url}")
        soup = BeautifulSoup(html_content, "lxml")
        event_data = Event(url=url)

        # --- SELECTORS FOR IBIZA SPOTLIGHT EVENT DETAIL PAGE ---
        # !!! IMPORTANT: YOU MUST INSPECT LIVE IBIZA SPOTLIGHT EVENT DETAIL PAGES !!!
        # !!! AND REFINE THESE SELECTORS FOR ACCURATE DATA EXTRACTION.       !!!
        # The selectors below are EDUCATED GUESSES based on common patterns.
        selectors = {
            "title": "h1.eventTitle, h1.article-title, main h1, article header h1", 
            "venue": "a[href*='/club/'], .promoter-info a[href*='/night/clubs/'], .venue-name-class", # Look for links to club pages
            "date_text": ".event-date-time-class, .event-info .date, time[datetime]", 
            "time_text": ".event-date-time-class, .event-info .time", # Often date and time are together
            "price_text": ".price-info-class, .ticket-price-class, .buy-tickets .price",
            "lineup_container": ".lineup-section, .dj-list-container, #lineup", 
            "dj_item": "li, .artist-name, .dj-name", 
            "description": "div.event-description-text, article div.article-content, section#description",
            "promoter": ".promoter-link a, .event-by-promoter",
            "categories_container": ".event-tags, .category-list",
            "category_item": "a, .tag-item"
        }
        # --- END OF SELECTOR REFINEMENT NOTE ---

        title_elem = soup.select_one(selectors["title"])
        if title_elem: event_data.title = title_elem.get_text(strip=True)
        else: print(f"[WARNING] No title found on detail page: {url}. This event might be skipped if title is critical."); # Not returning None yet, other fields might exist

        venue_elem = soup.select_one(selectors["venue"])
        if venue_elem: event_data.venue = venue_elem.get_text(strip=True)
        
        date_text_elem = soup.select_one(selectors["date_text"])
        if date_text_elem:
            event_data.date_text = date_text_elem.get('datetime') or date_text_elem.get_text(strip=True)
            # Attempt to parse start_date and start_time (basic example)
            try:
                if event_data.date_text:
                    # More robust parsing needed here for various date formats
                    # Example: "Thursday 01 May 2025" or "01/05/2025"
                    # This is a placeholder - real parsing needs to handle Ibiza Spotlight's specific format
                    parsed_dt = None
                    # Try ISO format first
                    try: parsed_dt = datetime.fromisoformat(event_data.date_text.replace('Z', '+00:00'))
                    except ValueError:
                        # Try common European format "DD MMM YYYY" or "DD Month YYYY"
                        for fmt in ("%d %b %Y", "%d %B %Y", "%A %d %B %Y"):
                            try:
                                # Extract year from URL if not in text
                                year_in_url_match = re.search(r'/(\d{4})/', url)
                                year_context = year_in_url_match.group(1) if year_in_url_match else str(datetime.now().year)
                                # Append year if not present
                                date_to_parse = event_data.date_text
                                if not re.search(r'\d{4}', date_to_parse): # If year is not in text
                                    date_to_parse += f" {year_context}"
                                parsed_dt = datetime.strptime(date_to_parse, fmt)
                                break
                            except ValueError:
                                continue
                    if parsed_dt:
                        event_data.start_date = parsed_dt.date()
                        # Time might be separate or part of a longer string
            except Exception as e_date:
                print(f"[DEBUG] Could not parse date from text '{event_data.date_text}': {e_date}")

        time_text_elem = soup.select_one(selectors["time_text"])
        if time_text_elem:
            time_full_text = time_text_elem.get_text(strip=True)
            time_matches = re.findall(r'(\d{1,2}:\d{2})', time_full_text) # Finds HH:MM
            if time_matches:
                try:
                    event_data.start_time = dt_time.fromisoformat(time_matches[0])
                    if len(time_matches) > 1:
                        event_data.end_time = dt_time.fromisoformat(time_matches[1])
                except ValueError: print(f"[WARNING] Could not parse time(s) from: {time_full_text}")

        price_elem = soup.select_one(selectors["price_text"])
        if price_elem: 
            event_data.price_text = price_elem.get_text(strip=True)
            price_match = re.search(r'(\d[\d,.]*\d)', event_data.price_text.replace(',', '.')) # Normalize comma to dot for float
            if price_match:
                try: event_data.price_value = float(price_match.group(1))
                except ValueError: print(f"[WARNING] Could not parse price from: {event_data.price_text}")
            if "€" in event_data.price_text or "eur" in event_data.price_text.lower(): event_data.currency = "EUR"
            elif "$" in event_data.price_text or "usd" in event_data.price_text.lower(): event_data.currency = "USD"
            elif "£" in event_data.price_text or "gbp" in event_data.price_text.lower(): event_data.currency = "GBP"
            
        lineup_container = soup.select_one(selectors["lineup_container"])
        if lineup_container:
            dj_elements = lineup_container.select(selectors["dj_item"])
            event_data.lineup = [dj.get_text(strip=True) for dj in dj_elements if dj.get_text(strip=True)]
        
        desc_elem = soup.select_one(selectors["description"])
        if desc_elem: event_data.description = desc_elem.get_text(strip=True, separator="\n")

        promoter_elem = soup.select_one(selectors["promoter"])
        if promoter_elem: event_data.promoter = promoter_elem.get_text(strip=True)

        categories_container = soup.select_one(selectors["categories_container"])
        if categories_container:
            cat_elements = categories_container.select(selectors["category_item"])
            event_data.categories = [cat.get_text(strip=True) for cat in cat_elements if cat.get_text(strip=True)]
            
        if not event_data.title and not event_data.venue and not event_data.date_text:
             print(f"[WARNING] Very little data found for {url}, likely not a valid event detail page or selectors need major update.")
             return None
        return event_data

    def _parse_html_to_markdown_fallback(self, html_content: str, url: str) -> Optional[Event]:
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

    def scrape_single_event(self, event_url: str) -> Optional[Event]:
        print(f"[MODE: SCRAPE] Scraping single event: {event_url}")
        try:
            # Wait for a general content area of an event detail page.
            # Common patterns: <main>, <article>, <div id="content-column">, <div class="event-detail-wrapper">
            # This needs to be specific enough for Ibiza Spotlight detail pages.
            html_content = self.fetch_page_html(event_url, wait_for_content_selector="main article, main div.content-article, #main-content article")
            
            # Attempt rigid JSON schema parsing first
            event = self._parse_event_detail_page_content(html_content, event_url)

            # Check if the event data is of high quality or sufficient
            # Define "high quality" as having at least a title, venue, and date_text
            if event and (event.title or event.venue or event.date_text):
                print(f"[INFO] Successfully scraped event {event_url} with structured data.")
                return event
            else:
                print(f"[INFO] Structured parsing failed or yielded low-quality data for {event_url}. Attempting markdown fallback.")
                # If structured parsing fails or yields low quality data, try markdown fallback
                return self._parse_html_to_markdown_fallback(html_content, event_url)

        except Exception as e:
            print(f"[ERROR] Failed to scrape event {event_url}: {e}")
            traceback.print_exc()
            return None

    def crawl_calendar(self, year: int, month: int) -> List[Event]:
        self._ensure_browser()
        page: Any = None  # Changed type to `Any` to avoid type expression error
        try:
            page = self.browser.new_page(user_agent=random.choice(MODERN_USER_AGENTS))
            print("[INFO] Starting crawl session...")
            page.goto(f"{BASE_URL}/night/events/{year}/{month:02d}", wait_until="domcontentloaded", timeout=75000)
            # ...existing code...
        finally:
            if page:
                page.close()
    # ...existing code...
    
    def close(self):
        if self.browser:
            try: self.browser.close()
            except Exception as e: print(f"[DEBUG] Error closing browser: {e}")
        if self.playwright_context:
            try: self.playwright_context.stop()
            except Exception as e: print(f"[DEBUG] Error stopping Playwright context: {e}")
        print("[INFO] Scraper resources closed.")

def save_events_to_file(events: List[Event], filepath_base: Path, formats: List[str]):
    if not events: print("[INFO] No events to save."); return
    
    for event in events:
        if event.extraction_method == "markdown_fallback" and "md" in formats:
            # Use the specific path provided by the user for markdown fallback output
            md_path = Path("/home/creekz/Projects/skrrraped_graph/single_event_test_output/scraped_event_www_ibizaspotlight_com_001.md")
            md_path.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists
            with md_path.open("w", encoding="utf-8") as f:
                f.write(event.description if event.description else "")
            print(f"[INFO] Saved markdown fallback content to {md_path}")
            # Do not save this event as JSON/CSV if it's a markdown fallback, as it's not structured data
            continue 

    if "json" in formats:
        json_path = filepath_base.with_suffix(".json")
        # Filter out markdown fallback events from JSON/CSV output
        json_events = [e for e in events if e.extraction_method != "markdown_fallback"]
        if json_events:
            with json_path.open("w", encoding="utf-8") as f:
                json.dump([e.to_dict() for e in json_events], f, indent=2, ensure_ascii=False)
            print(f"[INFO] Saved {len(json_events)} structured events to {json_path}")
        else:
            print("[INFO] No structured events to save to JSON.")

    if "csv" in formats and events:
        csv_path = filepath_base.with_suffix(".csv")
        # Filter out markdown fallback events from JSON/CSV output
        csv_events = [e for e in events if e.extraction_method != "markdown_fallback"]
        if csv_events:
            # Ensure all possible keys are included in header, even if some events don't have them
            all_keys = set()
            for event in csv_events:
                all_keys.update(event.to_dict().keys())
            fieldnames = sorted(list(all_keys))

            with csv_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                for event in csv_events: writer.writerow(event.to_dict())
            print(f"[INFO] Saved {len(csv_events)} structured events to {csv_path}")
        else:
            print("[INFO] No structured events to save to CSV.")

def main():
    parser = argparse.ArgumentParser(description="Unified Ibiza Spotlight Scraper v1.2 - Refined")
    parser.add_argument("action", choices=["scrape", "crawl"], help="Action: 'scrape' a single URL, or 'crawl' a monthly calendar.")
    parser.add_argument("--url", type=str, help="URL of single event detail page (for 'scrape' mode).")
    parser.add_argument("--month", type=int, help="Month (1-12) (for 'crawl' mode, e.g., 5 for May).")
    parser.add_argument("--year", type=int, help="Year (e.g., 2025) (for 'crawl' mode).")
    # Headless, output-dir, min-delay, max-delay are now handled by settings
    parser.add_argument("--format", nargs='+', choices=["json", "csv", "md"], default=["json", "csv"], help="Output format(s).")
    args = parser.parse_args()

    if args.action == "scrape":
        if not args.url: parser.error("--url is required for 'scrape' mode.")
        if not urlparse(args.url).scheme or not urlparse(args.url).netloc: parser.error("--url must be a full URL.")
    elif args.action == "crawl":
        if args.month is None or args.year is None: parser.error("--month and --year are required for 'crawl' mode.")
        if not (1 <= args.month <= 12): parser.error("Month must be 1-12.")
        if args.year < 2000 or args.year > datetime.now().year + 5: parser.error(f"Year seems invalid ({args.year}). Please provide a realistic year.")

    Path(settings.SCRAPER_DEFAULT_OUTPUT_DIR).mkdir(exist_ok=True, parents=True)
    scraper = None
    all_events_data: List[Event] = []
    try:
        scraper = IbizaSpotlightUnifiedScraper(
            headless=settings.SCRAPER_DEFAULT_HEADLESS,
            min_delay=settings.SCRAPER_DEFAULT_MIN_DELAY,
            max_delay=settings.SCRAPER_DEFAULT_MAX_DELAY
        )
        if args.action == "scrape":
            event = scraper.scrape_single_event(args.url)
            if event: all_events_data.append(event)
        elif args.action == "crawl":
            all_events_data = scraper.crawl_calendar(args.year, args.month)
            
        if not all_events_data: print("[INFO] No events were successfully scraped.")
        else:
            print(f"[SUCCESS] Completed. Total events processed/scraped: {len(all_events_data)}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            action_part = args.action
            name_part = ""
            if args.action == "scrape" and args.url:
                 url_path = urlparse(args.url).path
                 name_part = url_path.replace('/','_').strip('_')[:50] if url_path else "single_event"
            elif args.action == "crawl" and args.year and args.month:
                 name_part = f"ibiza_spotlight_{args.year}_{args.month:02d}"
            else:
                name_part = "unknown_operation"

            base_name = f"{action_part}_{name_part}_{timestamp}"
            filepath_base = Path(settings.SCRAPER_DEFAULT_OUTPUT_DIR) / base_name
            save_events_to_file(all_events_data, filepath_base, args.format)
    except KeyboardInterrupt: print("\n[INFO] Scraping interrupted by user.")
    except ImportError as e:
        print(f"[FATAL ERROR] A required library is missing: {e}. Please install dependencies.")
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
