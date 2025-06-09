#!/usr/bin/env python3
import sys
import pathlib

# Add project root to sys.path (if your structure needs it)
# PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
# sys.path.insert(0, str(PROJECT_ROOT))

"""
Enhanced Ibiza Spotlight Scraper with MongoDB Integration,
Targeted Element Extraction, Improved Logging/Export, and Revised Schema.
"""

import os
import json
import time
import random
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
import datetime as dt # For type hinting primarily
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Any, Set
from dataclasses import dataclass
import csv

# --- Dependency Imports ---
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
from pymongo.database import Database
from dateutil import parser as date_parser

# Placeholder for QualityScorer if not integrated
class QualityScorer:
    def calculate_event_quality(self, event: Dict[str, Any]) -> Dict[str, Any]:
        score = 0; completeness = {}
        if event.get("title"): score += 1
        if event.get("datetime_obj"): score += 2
        if event.get("tier_1"): score += 1 # Check for first tier
        if event.get("full_description"): score += 1
        if event.get("promoter"): score += 0.5
        if event.get("venue"): score += 0.5
        if event.get("genres"): score += 1
        
        completeness["title_present"] = bool(event.get("title"))
        completeness["date_parsed"] = bool(event.get("datetime_obj"))
        completeness["has_tickets_tier1"] = bool(event.get("tier_1"))
        completeness["has_genres"] = bool(event.get("genres"))
        return {"score": score, "completeness_metrics": completeness, "version": "0.2.0"}

# --- Constants ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

PREDEFINED_GENRES = {
    "techno": ["techno", "tech house", "minimal techno", "dark techno"],
    "house": ["house music", "deep house", "funky house", "acid house", "progressive house", "afro house"],
    "trance": ["trance", "psytrance", "progressive trance", "uplifting trance"],
    "edm": ["edm", "electronic dance music", "big room", "future house"],
    "drum & bass": ["d&b", "drum and bass", "jungle", "neurofunk"],
    "reggaeton": ["reggaeton", "latin urban"],
    "hip hop": ["hip hop", "rap", "trap", "urban"],
    "live music": ["live band", "live set", "live music", "acoustic", "concert", "live pa"],
    "disco": ["disco", "nu-disco"],
    "funk": ["funk", "g-funk"],
    "soul": ["soul", "neo-soul"],
    "r&b": ["r&b", "rnb"],
    "electronic": ["electronic", "electronica", "experimental electronic"],
    "ambient": ["ambient", "chillout"],
    "garage": ["uk garage", "garage"],
    "bass music": ["bass music", "dubstep"],
    "underground": ["underground scene", "underground music"] # Descriptor
}

# Configure global logger (console part)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    logger.addHandler(ch)


@dataclass
class ScraperConfig:
    url: str
    min_delay: float = 0.7
    max_delay: float = 1.8

# --- MongoDB Connection ---
def get_mongodb_connection(retries=3, delay=2) -> Optional[Database]:
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/classy_skkkrapey")
    for attempt in range(retries):
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000, connectTimeoutMS=10000)
            client.admin.command('ping')
            db_name = urlparse(MONGODB_URI).path.lstrip('/') or "classy_skkkrapey"
            logger.info(f"MongoDB connected to {db_name}")
            return client[db_name]
        except errors.ConnectionFailure as e:
            logger.warning(f"MongoDB connection failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1: time.sleep(delay); delay *= 1.5
            else: logger.error("MongoDB connection failed after all retries."); raise
        except Exception as e: logger.error(f"Unexpected MongoDB error: {e}"); raise
    return None

# --- Scraper Class ---
class IbizaSpotlightScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.db = get_mongodb_connection()
        self.scorer = QualityScorer()
        self.current_user_agent = random.choice(USER_AGENTS)
        self.session = self._create_session()
        self.visited_urls: Set[str] = set()
        self.url_queue: List[tuple[str, int]] = []
        self.max_depth = 3
        self.current_depth = 0
        self.current_venue_context: Optional[str] = None # For venue name

        self.log_dir = Path("/home/creekz/Projects/classy_skrrrapey_mvp/classy_skkkrapey/scrape_logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._configure_file_logging()

        self.csv_file_path = self.log_dir / f"scraped_events_{self.run_timestamp}.csv"
        self.csv_headers_written = False
        self.all_scraped_events_for_run: List[Dict[str, Any]] = []
        
        self._initialize_venue_context()

    def _initialize_venue_context(self):
        """Extracts venue from the initial config URL if it's a venue page."""
        parsed_url = urlparse(self.config.url)
        path_parts = parsed_url.path.strip('/').split('/')
        # Example: /night/venue/unvrs
        if len(path_parts) >= 3 and path_parts[-2] == 'venue':
            self.current_venue_context = path_parts[-1]
            logger.info(f"Initialized with venue context: {self.current_venue_context}")

    def _configure_file_logging(self):
        log_file_path = self.log_dir / f"scraper_run_{self.run_timestamp}.log"
        # Remove existing file handlers for this logger to avoid duplicate logs on re-init
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler) and handler.baseFilename == str(log_file_path): # type: ignore
                logger.removeHandler(handler)
        
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter) 
        logger.addHandler(fh)
        logger.info(f"File logging configured at: {log_file_path}")

    def _create_session(self) -> requests.Session:
        session = requests.Session(); session.headers.update({"User-Agent": self.current_user_agent})
        retry_strategy = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter); session.mount("https://", adapter)
        return session
        
    def fetch_page_with_scroll(self, url: str) -> Optional[str]:
        """Fetch page using Playwright and scroll to load dynamic content"""
        from playwright.sync_api import sync_playwright
        
        try:
            logger.info(f"Fetching with Playwright: {url}")
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.goto(url)
                
                # Add 2-second wait after initial page load
                time.sleep(2)
                
                # Scroll to bottom in increments
                for _ in range(3):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(0.5)
                
                # Get final page content
                html = page.content()
                browser.close()
                return html
        except Exception as e:
            logger.error(f"Playwright request failed for {url}: {e}")
            return None

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            # Use Playwright for pages that need scrolling
            if "/promoters/" in url or "/night/" in url:
                return self.fetch_page_with_scroll(url)
                
            logger.info(f"Fetching: {url}")
            time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

    def parse_event_date(self, date_str: str, year: Optional[int] = None, context_url: str = "") -> Optional[dt.datetime]:
        if not date_str: return None
        date_str = date_str.replace("Desde", "").replace("From", "").strip()
        parsed_dt = None
        try:
            parsed_dt = date_parser.parse(date_str)
            if year and parsed_dt and parsed_dt.year != year: # If dateutil picked a year but we have a specific one
                 parsed_dt = parsed_dt.replace(year=year)
            elif year and parsed_dt and parsed_dt.year == datetime.now().year and year != datetime.now().year:
                 # If dateutil picked current year by default, but explicit year context is different
                 parsed_dt = parsed_dt.replace(year=year)
            return parsed_dt
        except (ValueError, TypeError) as e_parse:
            if year:
                try:
                    parsed_dt = date_parser.parse(f"{date_str} {year}")
                    if parsed_dt and parsed_dt.year != year : parsed_dt = parsed_dt.replace(year=year) # type: ignore
                    return parsed_dt
                except Exception as e_year_add: logger.warning(f"Could not parse date '{date_str}' with year {year} on {context_url}. Errors: {e_parse}, {e_year_add}")
            else: logger.warning(f"Could not parse date '{date_str}' (no year context) on {context_url}. Error: {e_parse}")
        return None

    def extract_genres_from_text(self, text_content: Optional[str]) -> List[str]:
        if not text_content: return []
        found_genres: Set[str] = set()
        text_lower = text_content.lower()
        for genre, keywords in PREDEFINED_GENRES.items():
            for keyword in keywords:
                if re.search(r'\b' + re.escape(keyword) + r'\b', text_lower):
                    found_genres.add(genre)
                    break # Found this genre, move to next genre category
        return sorted(list(found_genres))

    def scrape_partycal_ticket_card(self, card_soup: BeautifulSoup, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        try:
            event_data = {
                "title": None, "datetime_obj": None, "raw_date_string": None,
                "artists": [], "tier_1": None, "tier_2": None, "tier_3": None, # New tier fields
                "event_card_all_text": card_soup.get_text(separator=' ', strip=True),
                "scrapedAt": datetime.utcnow().isoformat() + "Z",
                "extractionMethod": "html-partycal-ticket", "page_year_context": page_year
            }
            header = card_soup.select_one("div.ticket-header")
            if header:
                title_el = header.select_one("h3"); event_data["title"] = title_el.get_text(strip=True) if title_el else None
                date_el = header.select_one("div.ticket-date time") or header.select_one("div.ticket-date")
                if date_el:
                    raw_date = date_el.get_text(strip=True); event_data["raw_date_string"] = raw_date
                    event_data["datetime_obj"] = self.parse_event_date(raw_date, page_year, current_page_url)

            body = card_soup.select_one("div.ticket-body")
            if body:
                # Artist extraction placeholder (complex, site-specific)
                # Example: p_artist = body.select_one("p.artist-list"); if p_artist: event_data["artists"] = [a.strip() for a in p_artist.get_text().split(',')]

                ticket_items_raw = []
                for item_el in body.select("div.ticket-item"):
                    item_data: Dict[str, Optional[str]] = {}
                    price_el = item_el.select_one("div.ticket-price"); item_data["price"] = price_el.get_text(strip=True) if price_el else None
                    desc_el = item_el.select_one("div.ticket-name"); item_data["description"] = desc_el.get_text(strip=True) if desc_el else None
                    # button_el = item_el.select_one("div.addtobasket"); item_data["button_text"] = button_el.get_text(strip=True) if button_el else None
                    if item_data.get("price") or item_data.get("description"): ticket_items_raw.append(item_data)
                
                if ticket_items_raw:
                    event_data["tier_1"] = ticket_items_raw[0] if len(ticket_items_raw) > 0 else None
                    event_data["tier_2"] = ticket_items_raw[1] if len(ticket_items_raw) > 1 else None
                    event_data["tier_3"] = ticket_items_raw[2] if len(ticket_items_raw) > 2 else None
            
            return event_data if event_data.get("title") else None
        except Exception as e: logger.error(f"Error in scrape_partycal_ticket_card for {current_page_url}: {e}", exc_info=True); return None

    def parse_json_ld_event(self, event_data_json: dict, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        try:
            raw_date = event_data_json.get("startDate", "")
            parsed_datetime = self.parse_event_date(raw_date, page_year, current_page_url)
            performers = []; p_data = event_data_json.get("performer", [])
            p_list = p_data if isinstance(p_data, list) else [p_data] if p_data else []
            for p_item in p_list:
                if isinstance(p_item, dict) and p_item.get("name"): performers.append(p_item["name"])
                elif isinstance(p_item, str): performers.append(p_item)

            return {
                "title": event_data_json.get("name", ""), "datetime_obj": parsed_datetime, "raw_date_string": raw_date,
                "json_ld_description": event_data_json.get("description", ""), # Separate from full_description
                "location": event_data_json.get("location", {}).get("name", "") if isinstance(event_data_json.get("location"), dict) else "",
                "artists": performers, "tier_1": None, "tier_2": None, "tier_3": None, # Typically not in basic JSON-LD event
                "json_ld_url": event_data_json.get("url"), # URL from JSON-LD if available
                "scrapedAt": datetime.utcnow().isoformat() + "Z", "extractionMethod": "json-ld",
                "page_year_context": page_year
            }
        except Exception as e: logger.error(f"Error parsing JSON-LD event on {current_page_url}: {e}", exc_info=True); return None
            
    def is_duplicate(self, new_event: Dict[str, Any], existing_events: List[Dict[str, Any]]) -> bool: # Simplified
        if not new_event.get("title"): return False
        for ex_event in existing_events:
            if not ex_event.get("title"): continue
            if new_event["title"].strip().lower() == ex_event["title"].strip().lower():
                new_dt, ex_dt = new_event.get("datetime_obj"), ex_event.get("datetime_obj")
                if new_dt and ex_dt and new_dt.date() == ex_dt.date(): return True
                if not new_dt and not ex_dt and new_event.get("raw_date_string") == ex_event.get("raw_date_string"): return True
        return False

    def scrape_promoter_page(self, url: str) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []
        html = self.fetch_page(url)
        if not html: return []
        
        current_promoter_slug: Optional[str] = None
        parsed_promoter_url = urlparse(url)
        promoter_path_parts = parsed_promoter_url.path.strip('/').split('/')
        if len(promoter_path_parts) >= 3 and promoter_path_parts[-2] == 'promoters':
            current_promoter_slug = promoter_path_parts[-1]

        try:
            soup = BeautifulSoup(html, "html.parser")
            page_year_context: Optional[int] = None
            title_text = soup.title.string if soup.title else ""
            year_search_texts = [title_text] + [h.get_text() for h in soup.select('h1, h2, .page-title, .listing-title')]
            for text_area in year_search_texts:
                if text_area:
                    year_match = re.search(r'\b(202\d|203\d)\b', text_area)
                    if year_match: page_year_context = int(year_match.group(1)); break
            if page_year_context: logger.info(f"Year context {page_year_context} for {url}")
            else: logger.warning(f"No year context found for {url}")

            main_content_el = soup.select_one("article.article._wysiwyg")
            full_desc_text = ""
            if main_content_el:
                for unwanted in main_content_el.select('script, style, form, .button, .social-share, div.card-ticket, section.promoter-listings, .partycal-ticket, .ticket-header, .ticket-body, .ticket-footer'): # More aggressive cleaning
                    unwanted.decompose()
                full_desc_text = main_content_el.get_text(separator='\n', strip=True)
                full_desc_text = re.sub(r'\n\s*\n', '\n', full_desc_text).strip()
            
            page_genres = self.extract_genres_from_text(full_desc_text)

            # JSON-LD Parsing (Primary method)
            json_ld_events = []
            for script_tag in soup.find_all('script', type='application/ld+json'):
                try:
                    data = json.loads(script_tag.string or "")
                    items = data if isinstance(data, list) else [data]
                    for item_json in items:
                        if isinstance(item_json, dict):
                            event_types = item_json.get("@type", [])
                            if not isinstance(event_types, list):
                                event_types = [event_types]
                            
                            # Validate schema includes both Event and MusicEvent
                            if "Event" in event_types or "MusicEvent" in event_types:
                                ld_event = self.parse_json_ld_event(item_json, url, page_year_context)
                                if ld_event:
                                    json_ld_events.append(ld_event)
                except Exception as e:
                    logger.error(f"JSON-LD parsing error on {url}: {e}", exc_info=True)
            
            # Add JSON-LD events if found
            for ld_event in json_ld_events:
                if not self.is_duplicate(ld_event, events):
                    events.append(ld_event)
                    logger.info(f"Added JSON-LD event: {ld_event.get('title', 'Untitled')}") 

            # CSS Selector Fallback (if no JSON-LD events found)
            if not json_ld_events:
                logger.info(f"No JSON-LD events found, falling back to CSS selectors on {url}")
                try:
                    for card_s in soup.select("div.card-ticket.partycal-ticket"):
                        event = self.scrape_partycal_ticket_card(card_s, url, page_year_context)
                        if event and not self.is_duplicate(event, events):
                            events.append(event)
                            logger.info(f"Added CSS event: {event.get('title', 'Untitled')}")
                except Exception as e:
                    logger.error(f"CSS selector fallback failed on {url}: {e}", exc_info=True)
            else:
                # Still try to add CSS events even if JSON-LD was found
                for card_s in soup.select("div.card-ticket.partycal-ticket"):
                    event = self.scrape_partycal_ticket_card(card_s, url, page_year_context)
                    if event and not self.is_duplicate(event, events):
                        events.append(event)
                        logger.info(f"Added CSS event: {event.get('title', 'Untitled')}")

            final_events = []
            for event_item in events: # Add common page data to all events from this page
                event_item["full_description"] = full_desc_text # Renamed field
                event_item["genres"] = page_genres
                event_item["promoter"] = current_promoter_slug
                event_item["venue"] = self.current_venue_context # From initial config or None
                event_item["tickets_url"] = url # URL of the page where event was found
                final_events.append(event_item)

            if not final_events and full_desc_text : # Fallback if no structured events but page has content
                 final_events.append({
                    "title": f"Content Page: {soup.title.string if soup.title else url}", "datetime_obj": None,
                    "full_description": full_desc_text, "genres": page_genres, "promoter": current_promoter_slug,
                    "venue": self.current_venue_context, "tickets_url": url,
                    "scrapedAt": datetime.utcnow().isoformat() + "Z", "extractionMethod": "page-description-only",
                    "page_year_context": page_year_context, "tier_1":None, "tier_2":None, "tier_3":None
                })

            next_links = soup.select('a[rel="next"], a.next, li.next > a, a:contains("Next Page"), a:contains("Siguiente")') # Added more selectors
            for link_el in next_links:
                href = link_el.get('href')
                if href:
                    next_url = urljoin(url, href)
                    if next_url not in self.visited_urls and self.current_depth < self.max_depth:
                        self.url_queue.append((next_url, self.current_depth + 1))
                        logger.info(f"Queued next page: {next_url}")
            return final_events
        except Exception as e: logger.error(f"Error scraping promoter page {url}: {e}", exc_info=True); return []

    def extract_promoter_urls(self, url: str) -> List[str]:
        html = self.fetch_page(url);
        if not html: return []
        soup = BeautifulSoup(html, "html.parser"); promoter_links = set()
        for link in soup.select('a[href*="/night/promoters/"], a[href*="/promoter/"]'): # More generic
            if link.get('href'): promoter_links.add(urljoin(url, link['href']))
        return list(promoter_links)

    def save_event(self, event: Dict[str, Any]):
        try:
            event.setdefault("title", "Untitled Event"); event.setdefault("tickets_url", "Unknown URL")
            quality_data = self.scorer.calculate_event_quality(event)
            event["_quality"] = quality_data
            
            update_key = {"title": event["title"], "tickets_url": event["tickets_url"]}
            if event.get("datetime_obj"): update_key["event_date_key_part"] = event["datetime_obj"].strftime("%Y-%m-%d")
            elif event.get("raw_date_string"): update_key["raw_date_key_part"] = event["raw_date_string"][:30]

            if self.db is not None:
                self.db.events.update_one(update_key, {"$set": event}, upsert=True)
            else:
                logger.warning("MongoDB not available. Event not saved to DB.")
        except Exception as e: logger.error(f"Error saving event '{event.get('title', 'N/A')}': {e}", exc_info=True)

    def append_to_csv(self, events_batch: List[Dict[str, Any]]):
        if not events_batch: return
        all_keys = set(); preferred_order = [
            "title", "datetime_obj", "raw_date_string", "tickets_url", "promoter", "venue", "genres",
            "tier_1", "tier_2", "tier_3", "full_description", "event_card_all_text", 
            "_quality", "extractionMethod", "page_year_context", "scrapedAt"
        ]
        for event in events_batch: all_keys.update(event.keys())
        final_headers = preferred_order + sorted(list(all_keys - set(preferred_order)))
        file_exists_non_empty = self.csv_file_path.exists() and self.csv_file_path.stat().st_size > 0
        try:
            with open(self.csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=final_headers, extrasaction='ignore')
                if not self.csv_headers_written and not file_exists_non_empty :
                    writer.writeheader(); self.csv_headers_written = True
                for event_data in events_batch:
                    row = {k: (v.isoformat() if isinstance(v, dt.datetime) else 
                               json.dumps(v) if isinstance(v, (list, dict)) else v) 
                           for k, v in event_data.items()}
                    writer.writerow(row)
            logger.info(f"Appended {len(events_batch)} events to {self.csv_file_path.name}")
        except Exception as e: logger.error(f"Error CSV append: {e}", exc_info=True)

    def run(self):
        logger.info(f"Starting scraper run_id: {self.run_timestamp}. CSV: {self.csv_file_path.name}")
        self.all_scraped_events_for_run = []; self.csv_headers_written = False

        if "/promoters/" in self.config.url or "/night/" in self.config.url and "/venue/" not in self.config.url:
            self.url_queue = [(self.config.url, 0)] # Start with promoter/general night page
        else: # Assumed venue page or other index that needs promoter discovery
            self.url_queue = [(self.config.url, 0)]

        while self.url_queue:
            try:
                url, depth = self.url_queue.pop(0)
                if url in self.visited_urls or depth > self.max_depth : continue
                self.visited_urls.add(url); self.current_depth = depth
                logger.info(f"Processing [{depth}]: {url}")

                # Update venue context if navigating to a new venue page (more advanced)
                # For now, current_venue_context is set at init and remains fixed.

                if "/promoters/" in url or ("/night/" in url and not url.endswith("/venue") and not re.search(r'/venue/[^/]+$', url)):
                    page_events = self.scrape_promoter_page(url)
                    if page_events:
                        for event_item in page_events: self.save_event(event_item)
                        self.append_to_csv(page_events)
                        self.all_scraped_events_for_run.extend(page_events)
                else: # Assumed venue page or other index needing promoter URL extraction
                    promoter_urls = self.extract_promoter_urls(url)
                    logger.info(f"Found {len(promoter_urls)} promoter URLs on {url}")
                    for p_url in promoter_urls:
                        if p_url not in self.visited_urls: self.url_queue.append((p_url, depth + 1))
                
                time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
            except Exception as e:
                logger.error(f"Error processing URL {url}: {e}", exc_info=True)
        
        logger.info(f"Run {self.run_timestamp} completed. Visited {len(self.visited_urls)} URLs. Found {len(self.all_scraped_events_for_run)} total events.")

# --- Main Execution ---
def main():
    config = ScraperConfig(
        url="https://www.ibiza-spotlight.com/night/venue/unvrs", # Target venue URL
    )
    scraper = IbizaSpotlightScraper(config)
    try: scraper.run()
    except KeyboardInterrupt: logger.warning("Scraper run interrupted by user.")
    except Exception as e: logger.critical(f"Scraper failed: {e}", exc_info=True)
    finally: logger.info(f"Scraper shutdown for run_id: {scraper.run_timestamp if hasattr(scraper, 'run_timestamp') else 'N/A'}.")

if __name__ == "__main__":
    main()