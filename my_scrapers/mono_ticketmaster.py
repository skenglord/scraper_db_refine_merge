import argparse
from datetime import datetime
import json
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, TypedDict, Any

sys.path.insert(0, str(Path(__file__).parent))

import logging
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, Page, Browser, ElementHandle, TimeoutError as PlaywrightTimeoutError

from my_scrapers.utils.scraper_utils import (
    setup_logger,
    save_to_mongodb,
    save_to_json_file,
    save_to_csv_file,
    save_to_markdown_file
)
from schema_adapter import map_to_unified_schema
try:
    from classy_skkkrapey.config import settings
except ImportError:
    project_root_for_imports = Path(__file__).resolve().parent.parent
    if str(project_root_for_imports) not in sys.path:
        sys.path.insert(0, str(project_root_for_imports))
    try:
        from classy_skkkrapey.config import settings
    except ImportError:
        class DummySettings: # Fallback if settings cannot be imported
            MONGODB_URI = os.getenv("MONGODB_URI_FALLBACK", "mongodb://localhost:27017/fallback_db")
            DB_NAME = urlparse(MONGODB_URI).path.lstrip('/') or "fallback_db_name"
            SCRAPER_DEFAULT_MIN_DELAY = 0.5
            SCRAPER_DEFAULT_MAX_DELAY = 1.5
        settings = DummySettings()
        logging.warning("classy_skkkrapey.config.settings not found. Using dummy settings.")

from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import pytz
from tqdm import tqdm # Retained as per original, though may be removed if not desired
import html2text


DEFAULT_TARGET_URL = "https://www.ticketmaster.com/discover/concerts"
MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Type Definitions
class CoordinatesTypedDict(TypedDict, total=False): lat: Optional[float]; lng: Optional[float]
class LocationTypedDict(TypedDict, total=False): venue: Optional[str]; address: Optional[str]; coordinates: Optional[CoordinatesTypedDict]
class ParsedDateTimeTypedDict(TypedDict, total=False): startDate: Optional[datetime]; endDate: Optional[datetime]; doors: Optional[str]
class DateTimeInfoTypedDict(TypedDict, total=False): displayText: Optional[str]; parsed: Optional[ParsedDateTimeTypedDict]; dayOfWeek: Optional[str]
class ArtistTypedDict(TypedDict, total=False): name: str; affiliates: Optional[List[str]]; genres: Optional[List[str]]; headliner: Optional[bool]
class TicketTierTypedDict(TypedDict, total=False): name: Optional[str]; price: Optional[float]; available: Optional[bool]
class TicketInfoTypedDict(TypedDict, total=False): displayText: Optional[str]; startingPrice: Optional[float]; currency: Optional[str]; tiers: Optional[List[TicketTierTypedDict]]; status: Optional[str]; url: Optional[str]
class OrganizerTypedDict(TypedDict, total=False): name: Optional[str]; affiliates: Optional[List[str]]; socialLinks: Optional[Dict[str, str]]
class EventSchemaTypedDict(TypedDict, total=False): title: Optional[str]; url: str; location: Optional[LocationTypedDict]; dateTime: Optional[DateTimeInfoTypedDict]; lineUp: Optional[List[ArtistTypedDict]]; eventType: Optional[List[str]]; genres: Optional[List[str]]; ticketInfo: Optional[TicketInfoTypedDict]; promos: Optional[List[str]]; organizer: Optional[OrganizerTypedDict]; ageRestriction: Optional[str]; images: Optional[List[str]]; socialLinks: Optional[Dict[str, str]]; fullDescription: Optional[str]; hasTicketInfo: Optional[bool]; isFree: Optional[bool]; isSoldOut: Optional[bool]; artistCount: Optional[int]; imageCount: Optional[int]; scrapedAt: datetime; updatedAt: Optional[datetime]; lastCheckedAt: Optional[datetime]; extractionMethod: Optional[str]; html: Optional[str]; extractedData: Optional[Dict]; ticketsUrl: Optional[str]

def is_data_sufficient(event_data: Dict) -> bool:
    if not event_data: return False
    if event_data.get("extractionMethod") == "jsonld" and event_data.get("title"): return True
    if event_data.get("extractionMethod") == "fallback":
        if event_data.get("title") and (
            event_data.get("location", {}).get("venue") or
            event_data.get("dateTime", {}).get("displayText") or
            (event_data.get("ticketInfo", {}).get("startingPrice") or 0) > 0 or
            event_data.get("fullDescription")
        ): return True
    return False

class MultiLayerEventScraper:
    def __init__(
        self, use_browser: bool = True, headless: bool = True, playwright_slow_mo: int = 50,
        random_delay_range: tuple = (0.5, 1.5), user_agents: Optional[List[str]] = None,
        output_dir: str = "output/mono_ticketmaster", log_dir: str = "scraper_logs/mono_ticketmaster",
        db_name: Optional[str] = None, db_collection: Optional[str] = "ticketmaster_events_pw"
    ):
        self.logger = setup_logger("MonoTicketmaster", "mono_ticketmaster_run", log_dir=log_dir)
        self.use_browser = use_browser and sync_playwright is not None
        if use_browser and sync_playwright is None:
            self.logger.warning("Playwright not installed/imported. Browser usage disabled.")

        self.headless = headless
        self.playwright_slow_mo = playwright_slow_mo
        self.random_delay_range = random_delay_range
        self.user_agents = user_agents or MODERN_USER_AGENTS
        self.current_user_agent: Optional[str] = random.choice(self.user_agents)

        self.playwright_instance: Optional[sync_playwright] = None
        self.browser: Optional[Browser] = None

        self.config = ScraperConfig(
            url="",
            min_delay=random_delay_range[0], max_delay=random_delay_range[1],
            save_to_db=True,
            headless=headless, slow_mo=playwright_slow_mo, user_agent=self.current_user_agent or "",
            output_dir=output_dir, log_dir=log_dir,
            mongodb_uri=settings.MONGODB_URI if hasattr(settings, 'MONGODB_URI') else os.getenv("MONGODB_URI_FALLBACK", "mongodb://localhost:27017/fallback_db"),
            db_name=db_name if db_name else (settings.DB_NAME if hasattr(settings, 'DB_NAME') else (urlparse(os.getenv("MONGODB_URI_FALLBACK", "mongodb://localhost:27017/fallback_db")).path.lstrip('/') or "ticketmaster_db_fallback")),
            collection_name=db_collection
        )

        try:
            self.mongo_client = MongoClient(self.config.mongodb_uri)
            self.db = self.mongo_client[self.config.db_name]
            self.events_collection = self.db[self.config.collection_name]
            self.logger.info(f"MongoDB setup: URI {self.config.mongodb_uri}, DB {self.config.db_name}, Collection {self.config.collection_name}")
            self.db.command('ping')
            self.logger.info("Successfully connected to MongoDB.")
        except Exception as e:
            self.logger.error(f"MongoDB connection/setup failed: {e}", exc_info=True)
            self.mongo_client = None; self.db = None; self.events_collection = None

        self.all_scraped_data: List[Dict[str, Any]] = []
        self.stats = {"venues_scraped": 0, "promoters_scraped": 0, "events_scraped": 0, "pages_processed": 0, "errors": 0}


    def __enter__(self):
        self.logger.info("Starting Playwright...")
        if self.use_browser:
            self.playwright_instance = sync_playwright().start()
            try:
                self.browser = self.playwright_instance.chromium.launch(
                    headless=self.headless, slow_mo=self.playwright_slow_mo
                )
                self.logger.info("Playwright browser launched.")
            except Exception as e:
                self.logger.critical(f"Playwright browser launch failed: {e}", exc_info=True)
                if self.playwright_instance: self.playwright_instance.stop()
                self.use_browser = False
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("Cleaning up Playwright resources...")
        if self.browser and self.browser.is_connected():
            try: self.browser.close()
            except Exception as e: self.logger.error(f"Error closing browser: {e}", exc_info=True)
        if self.playwright_instance:
            try: self.playwright_instance.stop()
            except Exception as e: self.logger.error(f"Error stopping Playwright: {e}", exc_info=True)
        self.logger.info("Playwright resources cleaned up.")

    def _quick_delay(self):
        time.sleep(random.uniform(self.random_delay_range[0], self.random_delay_range[1]))

    def _handle_cookie_popup_playwright(self, page: Page):
        self.logger.debug(f"Checking for cookie popup on {page.url}")
        selectors = [
            "button:has-text('Accept All Cookies')", "button:has-text('Allow All')",
            "button:has-text('Accept')", "button:has-text('Agree')",
            'button[id*="cookie"][id*="accept"]', '#onetrust-accept-btn-handler',
            'button[data-testid="cookie-policy-manage-dialog-accept-button"]',
            'button#onetrust-accept-btn-handler'
        ]
        try:
            page.wait_for_timeout(random.randint(700, 1800))
            for selector in selectors:
                button = page.locator(selector).first
                if button.is_visible(timeout=1200):
                    self.logger.info(f"Cookie popup found with: '{selector}'. Clicking.")
                    button.click(timeout=3000)
                    self.logger.info("Clicked cookie button.")
                    page.wait_for_timeout(random.randint(500, 1200))
                    return True
            self.logger.debug("No cookie popup handled.")
            return False
        except PlaywrightTimeoutError: self.logger.debug("Cookie selector check timed out."); return False
        except Exception as e: self.logger.error(f"Cookie handling error: {e}", exc_info=True); return False

    def fetch_page(self, url: str, use_browser_for_this_fetch: bool = False) -> Optional[str]:
        self._quick_delay()
        if self.use_browser and use_browser_for_this_fetch:
            if not self.browser or not self.browser.is_connected():
                self.logger.error("Playwright browser not available for fetch_page.")
                return None
            page: Optional[Page] = None
            try:
                page = self.browser.new_page(user_agent=self.current_user_agent)
                self.logger.info(f"Fetching with Playwright: {url}")
                page.goto(url, timeout=45000, wait_until="domcontentloaded")
                self._handle_cookie_popup_playwright(page)
                for _ in range(2):
                    page.evaluate("window.scrollBy(0, window.innerHeight)")
                    time.sleep(0.3 + random.uniform(0.1, 0.3))
                content = page.content()
                return content
            except Exception as e:
                self.logger.error(f"Playwright fetch failed for {url}: {e}", exc_info=True)
                return None
            finally:
                if page: page.close()
        else:
            self.logger.info(f"Playwright not used for {url}. Requests fallback not implemented in this refactor phase.")
            return None

    def extract_jsonld_data(self, soup: BeautifulSoup) -> Optional[Dict]:
        """Extract JSON-LD structured data."""
        self.logger.debug("Attempting to extract JSON-LD data.")
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw_ld = script.string or script.get_text()
                data_ld = json.loads(raw_ld)
                graph = data_ld.get("@graph", []) if isinstance(data_ld, dict) else []
                for node in graph:
                    if node.get("@type") == "MusicEvent":
                        self.logger.info("Found MusicEvent in JSON-LD @graph.")
                        return node
                if isinstance(data_ld, dict) and data_ld.get("@type") == "MusicEvent":
                    self.logger.info("Found MusicEvent in JSON-LD root.")
                    return data_ld
                if isinstance(data_ld, list): # Check if data_ld itself is a list of events
                    for item in data_ld:
                        if isinstance(item, dict) and item.get("@type") == "MusicEvent":
                            self.logger.info("Found MusicEvent in JSON-LD list.")
                            return item
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON-LD decoding error: {e}. Content: {raw_ld[:100]}...")
            except Exception as e:
                self.logger.error(f"Error processing JSON-LD script: {e}", exc_info=True)
                continue
        self.logger.debug("No MusicEvent JSON-LD data found.")
        return None

    def extract_wordpress_data(self, soup: BeautifulSoup) -> Dict:
        self.logger.debug("Attempting to extract WordPress/WooCommerce data.")
        data: Dict[str, str] = {}
        title_selectors = ["h1.entry-title", ".product_title", "h1.product-title", ".event-title", "h1"]
        for selector in title_selectors:
            if (elem := soup.select_one(selector)): data["title"] = elem.get_text(strip=True); break
        date_selectors = [".event-date", ".wcs-event-date", ".event-time", '[class*="date"]', '[class*="time"]']
        for selector in date_selectors:
            if (elem := soup.select_one(selector)): data["date_text"] = elem.get_text(strip=True); break
        venue_selectors = [".event-venue", ".venue", ".location", '[class*="venue"]', '[class*="location"]']
        for selector in venue_selectors:
            if (elem := soup.select_one(selector)): data["venue"] = elem.get_text(strip=True); break
        price_selectors = [".price", ".woocommerce-price-amount", ".amount", '[class*="price"]']
        for selector in price_selectors:
            if (elem := soup.select_one(selector)): data["price_text"] = elem.get_text(strip=True); break
        desc_selectors = [".entry-content", ".product-description", ".event-description", ".description"]
        for selector in desc_selectors:
            if (elem := soup.select_one(selector)): data["description"] = elem.get_text(strip=True)[:500]; break
        return data

    def extract_meta_data(self, soup: BeautifulSoup) -> Dict:
        self.logger.debug("Attempting to extract meta tag data.")
        data: Dict[str, str] = {}
        og_mappings = {"og:title": "title", "og:description": "description", "og:image": "image", "og:url": "canonical_url"}
        for og_prop, key in og_mappings.items():
            if (meta := soup.find("meta", property=og_prop)) and meta.get("content"): data[key] = meta["content"]
        meta_mappings = {"description": "meta_description", "keywords": "keywords"}
        for name, key in meta_mappings.items():
            if (meta := soup.find("meta", attrs={"name": name})) and meta.get("content"): data[key] = meta["content"]
        return data

    def extract_text_patterns(self, html: str) -> Dict:
        self.logger.debug("Attempting to extract data using regex text patterns.")
        data: Dict[str, str] = {}
        date_patterns = [r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})", r"(\d{4}-\d{2}-\d{2})", r"(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)[,\s]+(\d{1,2}[/-]\d{1,2}[/-]\d{4})"]
        for pattern in date_patterns:
            if (match := re.search(pattern, html)): data["date_pattern"] = match.group(0); break
        price_patterns = [r"[€$£](\d+(?:\.\d{2})?)", r"(\d+(?:\.\d{2})?)\s*[€$£]", r"Price[:\s]+[€$£]?(\d+(?:\.\d{2})?)"]
        for pattern in price_patterns:
            if (match := re.search(pattern, html, re.IGNORECASE)): data["price_pattern"] = match.group(0); break
        return data
    
    def extract_lineup_from_html(self, soup: BeautifulSoup) -> List[str]:
        self.logger.debug("Attempting to extract lineup from HTML.")
        artists = []
        lineup_headers = soup.find_all(['h3', 'h4', 'h5'], string=re.compile(r'Line\s*Up', re.IGNORECASE))
        for header in lineup_headers:
            next_elem = header.find_next_sibling()
            while next_elem and next_elem.name not in ['h3', 'h4', 'h5', 'div']:
                if next_elem.name == 'p':
                    text = next_elem.get_text(separator='\n', strip=True)
                    artists.extend([line.strip() for line in text.split('\n') if line.strip()]); break
                elif next_elem.name == 'ul':
                    for li in next_elem.find_all('li'):
                        if (artist_text := li.get_text(strip=True)): artists.append(artist_text)
                    break
                next_elem = next_elem.find_next_sibling()
        seen = set(); unique_artists = [x for x in artists if not (x in seen or seen.add(x))] # type: ignore
        return unique_artists
    
    def extract_ticket_url_from_html(self, soup: BeautifulSoup) -> Optional[str]:
        self.logger.debug("Attempting to extract ticket URL from HTML.")
        ticket_links = soup.find_all('a', string=re.compile(r'Buy\s*Tickets|Tickets|Get\s*Tickets', re.IGNORECASE))
        for link in ticket_links:
            href = link.get('href')
            if href and any(keyword in href for keyword in ['ticket', 'fourvenues', 'eventbrite', 'ra.co']): # Added more keywords
                return urljoin(self.config.url, href) # Ensure absolute URL
        if (ticket_link := soup.find('a', class_=re.compile(r'ticket|btn-ticket|buy-now', re.IGNORECASE))): # Regex for class
             if (href := ticket_link.get('href')): return urljoin(self.config.url, href)
        return None

    def _map_jsonld_to_event_schema(self, node: Dict, url: str, html: Optional[str], now_iso: str) -> EventSchemaTypedDict:
        self.logger.debug(f"Mapping JSON-LD data for {url}")
        scraped_at_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00")) if now_iso else datetime.now(timezone.utc)
        event_data: EventSchemaTypedDict = {"url": url, "scrapedAt": scraped_at_dt, "extractionMethod": "jsonld", "html": (html or "")[:5000], "lineUp": []}
        event_data["title"] = node.get("name")
        loc_node = node.get("location", {}) or {}; addr_node = loc_node.get("address", {}) or {}; geo_node = loc_node.get("geo", {}) or {}
        addr_parts = [addr_node.get(k) for k in ["streetAddress", "addressLocality", "addressRegion", "postalCode", "addressCountry"] if addr_node.get(k)]
        coords = {"lat": float(geo_node["latitude"]), "lng": float(geo_node["longitude"])} if geo_node.get("latitude") and geo_node.get("longitude") else None
        event_data["location"] = {"venue": loc_node.get("name"), "address": " ".join(addr_parts) or None, "coordinates": coords}
        start_date = node.get("startDate"); end_date = node.get("endDate"); door_time = node.get("doorTime")
        parsed_start = datetime.fromisoformat(start_date.replace("Z", "+00:00")) if start_date else None
        parsed_end = datetime.fromisoformat(end_date.replace("Z", "+00:00")) if end_date else None
        event_data["dateTime"] = {"displayText": f"{start_date}" + (f" - {end_date}" if end_date else ""),
                                "parsed": {"startDate": parsed_start, "endDate": parsed_end, "doors": door_time},
                                "dayOfWeek": parsed_start.strftime("%A") if parsed_start else None}
        performers = node.get("performer", []); performers = [performers] if isinstance(performers, dict) else performers
        for idx, p_node in enumerate(performers):
            if isinstance(p_node, dict) and p_node.get("name"):
                event_data["lineUp"].append({"name": str(p_node["name"]), "headliner": idx == 0}) # Simplified
        event_type = node.get("@type", []); event_data["eventType"] = [event_type] if isinstance(event_type, str) else [str(et) for et in event_type if isinstance(et, str)]
        genres = node.get("genre", []); event_data["genres"] = [genres] if isinstance(genres, str) else [str(g) for g in genres if isinstance(g, str)]
        offers = node.get("offers", []); offers = [offers] if isinstance(offers, dict) else (offers if isinstance(offers, list) else [])
        if offers and isinstance(offers[0], dict):
            first_offer = offers[0]; prices = [float(o["price"]) for o in offers if isinstance(o, dict) and o.get("price") is not None and isinstance(o.get("price"), (int,float,str)) and str(o.get("price")).replace('.','',1).isdigit()]
            event_data["ticketInfo"] = {"startingPrice": min(prices) if prices else None, "currency": first_offer.get("priceCurrency"), "url": first_offer.get("url")}
            event_data["ticketsUrl"] = first_offer.get("url") or event_data.get("ticketsUrl") # Prioritize offer URL
        organizer = node.get("organizer", {}); organizer = organizer[0] if isinstance(organizer, list) and organizer else (organizer if isinstance(organizer, dict) else {})
        if isinstance(organizer, dict): event_data["organizer"] = {"name": organizer.get("name")}
        event_data["ageRestriction"] = node.get("typicalAgeRange")
        images = node.get("image", []); event_data["images"] = [images] if isinstance(images, str) else ([str(img) for img in images if isinstance(img, str)] if isinstance(images, list) else [])
        event_data["fullDescription"] = node.get("description")
        if html: # Re-parse with BS for lineup and ticket URL if not in JSON-LD
            soup = BeautifulSoup(html, "html.parser")
            if not event_data["lineUp"]: event_data["lineUp"] = [{"name": art_name, "headliner": i==0} for i, art_name in enumerate(self.extract_lineup_from_html(soup))]
            if not event_data.get("ticketsUrl"): event_data["ticketsUrl"] = self.extract_ticket_url_from_html(soup)
        self._populate_derived_fields(event_data)
        return event_data

    def _map_fallback_to_event_schema(self, data: Dict, url: str, html: str, now_iso: str) -> EventSchemaTypedDict:
        self.logger.debug(f"Mapping fallback data for {url}")
        scraped_at_dt = datetime.fromisoformat(now_iso.replace("Z", "+00:00")) if now_iso else datetime.now(timezone.utc)
        event_data: EventSchemaTypedDict = {"url": url, "scrapedAt": scraped_at_dt, "extractionMethod": "fallback", "html": html[:5000], "extractedData": data, "lineUp": []}
        event_data["title"] = data.get("title")
        event_data["location"] = {"venue": data.get("venue"), "address": data.get("address")}
        date_text = data.get("date_text") or data.get("date_pattern")
        # Simplified date/time for fallback
        event_data["dateTime"] = {"displayText": date_text} if date_text else None
        price_text = data.get("price_text") or data.get("price_pattern"); starting_price = None; currency = None
        if price_text:
            price_match = re.search(r"(\d+(?:\.\d{2})?)", price_text)
            if price_match: try: starting_price = float(price_match.group(1)) catch: pass
            if "€" in price_text: currency = "EUR"
            elif "$" in price_text: currency = "USD"
            elif "£" in price_text: currency = "GBP"
        event_data["ticketInfo"] = {"displayText": price_text, "startingPrice": starting_price, "currency": currency}
        og_image = data.get("image"); event_data["images"] = [og_image] if og_image and isinstance(og_image, str) else []
        description_html = data.get("description") or data.get("meta_description")
        event_data["fullDescription"] = str(description_html) if description_html else None
        # For fallback, lineup and ticket URL are extracted again from full HTML by default
        soup = BeautifulSoup(html, "html.parser")
        event_data["lineUp"] = [{"name": art_name, "headliner": i==0} for i, art_name in enumerate(self.extract_lineup_from_html(soup))]
        event_data["ticketsUrl"] = self.extract_ticket_url_from_html(soup)
        self._populate_derived_fields(event_data)
        return event_data

    def _populate_derived_fields(self, event_data: EventSchemaTypedDict) -> None:
        now_utc = datetime.now(timezone.utc)
        event_data["updatedAt"] = now_utc; event_data["lastCheckedAt"] = now_utc
        ticket_info = event_data.get("ticketInfo")
        if ticket_info:
            has_price = (ticket_info.get("startingPrice") or 0) > 0; display_text = ticket_info.get("displayText", ""); url = ticket_info.get("url", "")
            event_data["hasTicketInfo"] = bool(has_price or (display_text and display_text.strip()) or (url and url.strip()) or ticket_info.get("tiers"))
            is_free_price = ticket_info.get("startingPrice") == 0; status_text = (ticket_info.get("status") or "").lower(); display_text_lower = display_text.lower()
            event_data["isFree"] = bool(event_data["hasTicketInfo"] and (is_free_price or "free" in status_text or "free" in display_text_lower) and not has_price)
            event_data["isSoldOut"] = any(keyword in status_text for keyword in ["sold out", "unavailable", "off-sale", "offsale"])
            if has_price: event_data["isFree"] = False # Price overrides free status
        else: event_data["hasTicketInfo"] = False; event_data["isFree"] = False; event_data["isSoldOut"] = False
        event_data["artistCount"] = len(event_data.get("lineUp") or [])
        event_data["imageCount"] = len(event_data.get("images") or [])

    def scrape_event_data(self, url: str, attempt_with_browser: bool = False) -> Tuple[Dict, Optional[str]]:
        self.logger.info(f"Scraping event data for {url}, browser attempt: {attempt_with_browser}")
        html_content = self.fetch_page(url, use_browser_for_this_fetch=attempt_with_browser)
        if not html_content:
            self.logger.error(f"Failed to fetch HTML for {url}")
            return {}, None # Return empty dict and None for method if fetch fails

        soup = BeautifulSoup(html_content, "html.parser")
        now_iso = datetime.now(timezone.utc).isoformat() # Use timezone.utc

        # Try JSON-LD first
        jsonld_data = self.extract_jsonld_data(soup)
        if jsonld_data:
            self.logger.info(f"Successfully extracted JSON-LD data from {url}")
            # Pass html_content to _map_jsonld_to_event_schema for additional HTML-based extraction
            return self._map_jsonld_to_event_schema(jsonld_data, url, html_content, now_iso), "jsonld"

        # Fallback to other methods if JSON-LD fails or is not present
        self.logger.info(f"No JSON-LD data found or it was insufficient for {url}. Using fallback methods.")
        wp_data = self.extract_wordpress_data(soup)
        meta_data = self.extract_meta_data(soup)
        pattern_data = self.extract_text_patterns(html_content) # Pass html_content string

        # Combine all fallback data sources; ensure keys from more reliable sources (like meta) might take precedence if needed
        combined_data = {**pattern_data, **wp_data, **meta_data} # Order can matter if keys overlap

        # If even basic title is missing from combined, it's likely a poor scrape
        if not combined_data.get("title"):
             self.logger.warning(f"Fallback methods failed to extract a title for {url}. Data: {combined_data}")
             # Return the combined_data anyway, schema mapping might handle it or mark as low quality

        return self._map_fallback_to_event_schema(combined_data, url, html_content, now_iso), "fallback"

    def save_event_pw(self, unified_event_doc: Dict[str, Any]):
       """Saves a unified event document to MongoDB and adds to list for batch file output."""
       if not unified_event_doc or not unified_event_doc.get("event_id"):
           self.logger.error("Unified event doc is invalid or missing event_id. Cannot process.")
           return
       self.all_scraped_data.append(unified_event_doc)
       if self.config.save_to_db and self.db and self.events_collection:
           try:
               save_to_mongodb(
                   [unified_event_doc],
                   mongodb_uri=self.config.mongodb_uri,
                   db_name=self.config.db_name,
                   collection_name=self.config.collection_name,
                   logger_obj=self.logger
               )
           except Exception as e:
                self.logger.error(f"Error saving event {unified_event_doc.get('event_id')} to MongoDB via utility: {e}", exc_info=True)
       elif self.config.save_to_db:
            self.logger.warning(f"DB saving enabled, but no DB connection for event: {unified_event_doc.get('title')}")

    def scrape_event_strategically(self, url: str) -> Optional[Dict[str, Any]]:
        self.logger.info(f"Strategically scraping: {url}")
        # Default to using browser if self.use_browser is true, otherwise use requests (which is currently None)
        # For this refactor, fetch_page is now primarily Playwright-based.
        # The `attempt_with_browser` flag in scrape_event_data will determine if Playwright is used by fetch_page.

        # First attempt: Use Playwright if self.use_browser is true.
        # If self.use_browser is false, fetch_page will return None (as requests part is removed).
        raw_event_data, extraction_method = self.scrape_event_data(url, attempt_with_browser=self.use_browser)

        if not raw_event_data.get("title"):
            self.logger.warning(f"No sufficient raw data obtained for {url} (title missing).")
            # Optionally, if a non-browser attempt was made first and failed, one could force a browser attempt here
            # if not self.use_browser: # if initial attempt was non-browser (which is not the case anymore)
            #    self.logger.info(f"Retrying with browser for {url}")
            #    raw_event_data, extraction_method = self.scrape_event_data(url, attempt_with_browser=True)
            if not raw_event_data.get("title"): # Check again
                 self.logger.error(f"Still no title for {url} after all attempts.")
                 return None

        raw_event_data["extraction_method_used"] = extraction_method

        unified_event_doc = map_to_unified_schema(
            raw_data=raw_event_data, source_platform="ticketmaster_multilayer_pw", source_url=url
        )
        if unified_event_doc:
            self.save_event_pw(unified_event_doc)
            return unified_event_doc
        self.logger.error(f"Failed to map to unified schema for {url}")
        return None

    def crawl_listing_for_events(self, listing_url: str, max_events: int = 10) -> List[Dict[str, Any]]:
        self.logger.info(f"Crawling listing page: {listing_url} for max {max_events} events.")
        if not self.use_browser or not self.browser or not self.browser.is_connected():
            self.logger.error("Browser required for crawl_listing_for_events but not available/connected.")
            return []

        page: Optional[Page] = None
        try:
            page = self.browser.new_page(user_agent=self.current_user_agent)
            self.logger.info(f"Navigating to listing page: {listing_url}")
            page.goto(listing_url, timeout=60000, wait_until="domcontentloaded")
            self._handle_cookie_popup_playwright(page)

            load_more_selector = 'button[data-testid="load-more-button"]' # Example for Ticketmaster
            for i in range(3):
                load_more_button = page.locator(load_more_selector).first
                if load_more_button.is_visible(timeout=3000):
                    self.logger.info(f"Clicking 'Load More' (attempt {i+1})")
                    load_more_button.click()
                    page.wait_for_timeout(random.randint(2000, 3500))
                else: self.logger.debug("'Load More' not found or not visible."); break

            event_link_selector = "a[href*='/event/']" # Generic for Ticketmaster
            link_elements = page.locator(event_link_selector).all() # Get all Locator objects

            event_urls = []
            for link_loc in link_elements: # Iterate through Locators
                href = link_loc.get_attribute("href")
                if href:
                    full_url = urljoin(listing_url, href)
                    if "/event/" in full_url and full_url not in event_urls:
                        event_urls.append(full_url)

            self.logger.info(f"Found {len(event_urls)} unique event URLs on {listing_url}.")

            for i, event_url in enumerate(event_urls):
                if i >= max_events: self.logger.info(f"Reached max_events ({max_events})."); break
                self.logger.info(f"Scraping event {i+1}/{len(event_urls)}: {event_url}")
                self.scrape_event_strategically(event_url)
                self._quick_delay()
        except Exception as e:
            self.logger.error(f"Error crawling listing {listing_url}: {e}", exc_info=True)
        finally:
            if page: page.close()
        # self.all_scraped_data is populated by save_event_pw via scrape_event_strategically
        return self.all_scraped_data

    def run(self, target_url: Optional[str] = None, crawl_listing: bool = False, max_crawl_events: int = 10):
        self.logger.info(f"Run started. Target: {target_url or self.config.url}, Crawl: {crawl_listing}")
        self.all_scraped_data = []

        with self:
            actual_target_url = target_url or self.config.url
            if crawl_listing:
                self.logger.info(f"Starting crawl for listing URL: {actual_target_url}")
                self.crawl_listing_for_events(actual_target_url, max_events=max_crawl_events)
            elif actual_target_url: # Ensure there's a URL if not crawling
                self.logger.info(f"Starting scrape for single URL: {actual_target_url}")
                self.scrape_event_strategically(actual_target_url)
            else:
                self.logger.warning("No target URL specified and not crawling. Exiting run.")
        
        if self.all_scraped_data:
            self.logger.info(f"Saving {len(self.all_scraped_data)} scraped and unified events to files.")
            output_prefix = "mono_ticketmaster_events"
            save_to_json_file(self.all_scraped_data, output_prefix, self.config.output_dir, self.logger)
            save_to_csv_file(self.all_scraped_data, output_prefix, self.config.output_dir, self.logger)
            save_to_markdown_file(self.all_scraped_data, output_prefix, self.config.output_dir, self.logger)
            if self.config.save_to_db and self.db:
                 self.logger.info(f"DB saving was handled per event. Total events for file output: {len(self.all_scraped_data)}.")
        else:
            self.logger.info("No events were collected during this run.")
        
        self.logger.info(f"Run finished. Total unified events: {len(self.all_scraped_data)}. Errors: {self.stats.get('errors',0)}")
        return self.all_scraped_data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Ticketmaster event pages using MultiLayerEventScraper.")
    parser.add_argument("target_url", nargs="?", default=DEFAULT_TARGET_URL, help="Event URL or Listing URL to start crawling.")
    parser.add_argument("--crawl", action="store_true", help="Enable crawling of a listing page.")
    parser.add_argument("--max-events", type=int, default=20, help="Max events to scrape when crawling.")
    parser.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True, help="Run browser in headless mode.")
    parser.add_argument("--slow-mo", type=int, default=50, help="Slow down Playwright operations by N ms.")
    parser.add_argument("--no-db", action="store_true", help="Disable saving to MongoDB.")
    parser.add_argument("--output-dir", type=str, default="output/mono_ticketmaster", help="Directory for output files.")
    parser.add_argument("--log-dir", type=str, default="scraper_logs/mono_ticketmaster", help="Directory for log files.")
    args = parser.parse_args()

    cfg = ScraperConfig(
        url=args.target_url, headless=args.headless, slow_mo=args.slow_mo,
        save_to_db=not args.no_db, output_dir=args.output_dir, log_dir=args.log_dir
    )

    Path(cfg.output_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.log_dir).mkdir(parents=True, exist_ok=True)

    main_logger = setup_logger("MonoTicketmasterMain", "mono_tm_main_run", log_dir=cfg.log_dir)
    main_logger.info(f"Initiating MonoTicketmaster Scraper. Config: {cfg}")

    try:
        with MultiLayerEventScraper(config=cfg) as scraper:
            scraper.run(target_url=args.target_url, crawl_listing=args.crawl, max_crawl_events=args.max_events)
            main_logger.info("Scraping process has concluded.")
    except KeyboardInterrupt:
        main_logger.warning("Scraper run interrupted.")
    except Exception as e_main:
        main_logger.critical(f"Scraper failed critically in main execution: {e_main}", exc_info=True)
    finally:
        main_logger.info("MonoTicketmaster Scraper shutdown sequence initiated.")
        logging.shutdown()
