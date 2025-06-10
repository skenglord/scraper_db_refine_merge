"""
Refactored Ibiza Spotlight Scraper using Playwright for fetching and primary parsing.
Date: 2025-06-10
"""

import os
import time
import random
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta, timezone
import datetime as dt
from dataclasses import dataclass, field

from playwright.sync_api import sync_playwright, Page, Browser, TimeoutError as PlaywrightTimeoutError, ElementHandle
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import pytz
from tqdm import tqdm
import html2text

from my_scrapers.utils.scraper_utils import (
    setup_logger,
    save_to_mongodb,
    save_to_json_file,
    save_to_csv_file,
    save_to_markdown_file
)
from schema_adapter import map_to_unified_schema
from config import settings
from database.quality_scorer import QualityScorer
from pymongo import MongoClient, errors
from pymongo.database import Database
import logging

@dataclass
class ScraperConfig:
    url: str
    min_delay: float = 0.3
    max_delay: float = 1.2
    save_to_db: bool = True
    headless: bool = True
    slow_mo: int = 30
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    viewport_width: int = 1280
    viewport_height: int = 720
    output_dir: str = "output/ibiza_spotlight_pw"
    log_dir: str = "scraper_logs/ibiza_spotlight_pw"
    mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017/ibiza_data")
    db_name: str = urlparse(mongodb_uri).path.lstrip('/') or "ibiza_data_pw"
    collection_name: str = "events_spotlight_refactored"
    EVENT_SELECTORS_SPOTLIGHT: Dict[str, str] = field(default_factory=lambda: {
        'primary_event_card': "div.card-ticket.partycal-ticket",
        'json_ld_script': 'script[type="application/ld+json"]'
        # Add more selectors as needed
    })

def get_mongodb_connection(mongodb_uri: str, db_name_cfg: str, retries=3, delay=2, logger_obj=None) -> Optional[Database]:
    log = logger_obj if logger_obj else logging.getLogger("MongoTempConnect")
    if not logger_obj and not log.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        log.addHandler(ch)
        log.setLevel(logging.INFO)
    effective_db_name = db_name_cfg
    for attempt in range(retries):
        try:
            client = MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000, connectTimeoutMS=10000)
            client.admin.command('ping')
            log.info(f"MongoDB connected to DB: {effective_db_name} via URI: {mongodb_uri}")
            return client[effective_db_name]
        except errors.ConnectionFailure as e:
            log.warning(f"MongoDB connection failed (attempt {attempt+1}/{retries}) to {mongodb_uri} (DB: {effective_db_name}): {e}")
            if attempt < retries - 1: time.sleep(delay); delay *= 1.5
            else: log.error("MongoDB connection failed after all retries."); return None
        except Exception as e:
            log.error(f"Unexpected MongoDB error with {mongodb_uri} (DB: {effective_db_name}): {e}"); return None
    return None

class IbizaSpotlightScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = setup_logger("IbizaSpotlightScraperPW", "ibiza_spotlight_pw_run", log_dir=self.config.log_dir)
        self.playwright_instance: Optional[sync_playwright] = None
        self.browser: Optional[Browser] = None
        self.url_queue: List[Tuple[str, int]] = []
        self.visited_urls: Set[str] = set()
        self.current_depth = 0
        self.max_depth = 3
        self.current_venue_context: Optional[str] = None
        self.db = get_mongodb_connection(self.config.mongodb_uri, self.config.db_name, logger_obj=self.logger) if self.config.save_to_db else None
        self.scorer = QualityScorer()
        self.run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.all_scraped_events_for_run: List[Dict[str, Any]] = []
        self.stats = {"venues_scraped": 0, "promoters_scraped": 0, "events_scraped": 0, "pages_processed": 0, "errors": 0}
        self.html_to_text_converter = html2text.HTML2Text()
        self.html_to_text_converter.ignore_links = False; self.html_to_text_converter.ignore_images = True
        self.html_to_text_converter.body_width = 0
        self.logger.info(f"Initialized IbizaSpotlightScraper (Playwright) with URL: {self.config.url}")

    def __enter__(self):
        self.logger.info("Starting Playwright and launching browser...")
        self.playwright_instance = sync_playwright().start()
        try:
            self.browser = self.playwright_instance.chromium.launch(
                headless=self.config.headless, slow_mo=self.config.slow_mo,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.logger.info("Playwright browser launched.")
        except Exception as e:
            self.logger.critical(f"Playwright browser launch failed: {e}", exc_info=True)
            if self.playwright_instance: self.playwright_instance.stop()
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("Closing browser and stopping Playwright...")
        if self.browser and self.browser.is_connected():
            try: self.browser.close()
            except Exception as e: self.logger.error(f"Error closing browser: {e}", exc_info=True)
        if self.playwright_instance:
            try: self.playwright_instance.stop()
            except Exception as e: self.logger.error(f"Error stopping Playwright: {e}", exc_info=True)
        self.logger.info("Playwright resources cleaned up.")

    def _quick_delay(self, min_s: Optional[float] = None, max_s: Optional[float] = None):
        min_d = min_s if min_s is not None else self.config.min_delay
        max_d = max_s if max_s is not None else self.config.max_delay
        delay = random.uniform(min(min_d, max_d), max(min_d, max_d))
        self.logger.debug(f"Applying delay: {delay:.2f}s")
        time.sleep(delay)

    def _handle_cookie_popup_playwright(self, page: Page):
        """Detects and closes cookie consent popups using Playwright."""
        self.logger.debug(f"Checking for cookie popup on {page.url}...")
        selectors = [
            "a.cb-seen.cta-secondary.sm.cb-seen-accept",
            "button:has-text('Accept')", "button:has-text('Agree')",
            "button:has-text('OK')", "button:has-text('I agree')",
            "button:has-text('Consent')", "button:has-text('Accept all')",
            "button:has-text('Accept All Cookies')", "button:has-text('Allow all cookies')",
            'button[id*="cookie"][id*="accept"]', '#onetrust-accept-btn-handler'
        ]
        try:
            page.wait_for_timeout(random.randint(300, 700))
            for selector in selectors:
                button = page.locator(selector).first
                if button.is_visible(timeout=1000):
                    self.logger.info(f"Cookie popup found with: '{selector}'. Clicking.")
                    button.click(timeout=2500)
                    self.logger.info(f"Clicked cookie consent button using: '{selector}'.")
                    page.wait_for_timeout(random.randint(500, 1000))
                    return True
            self.logger.debug(f"No cookie popup detected or handled with known selectors on {page.url}.")
            return False
        except PlaywrightTimeoutError:
            self.logger.debug(f"A specific cookie selector timed out on {page.url}.")
            return False
        except Exception as e:
            self.logger.error(f"Error during cookie handling on {page.url}: {e}", exc_info=True)
            return False

    def _scroll_page_robustly(self, page: Page, attempts: int = 5, scroll_delay_ms: int = 1200):
        """Scrolls the page to attempt loading dynamic content."""
        self.logger.debug(f"Starting robust scroll for {page.url}; attempts={attempts}, delay={scroll_delay_ms}ms.")
        previous_height = -1
        for i in range(attempts):
            current_height = page.evaluate("document.body.scrollHeight")
            self.logger.debug(f"Scroll attempt {i+1}/{attempts}. Current height: {current_height}px, Previous height: {previous_height}px.")
            if current_height == previous_height and i > 0:
                self.logger.info(f"Page height stabilized at {current_height}px after {i+1} scroll attempts for {page.url}.")
                break
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(scroll_delay_ms)
            previous_height = current_height
        else:
             self.logger.info(f"Completed all {attempts} scroll attempts for {page.url}. Final height: {previous_height}px.")
        page.wait_for_timeout(scroll_delay_ms // 2)
        self.logger.debug(f"Finished robust scroll for {page.url}.")

    def fetch_page_content(self, url: str, robust_scroll: bool = False, scroll_attempts: int = 3) -> Optional[str]:
        if not self.browser:
            self.logger.error("Browser not initialized. Cannot fetch page content.")
            return None
        temp_page: Optional[Page] = None
        try:
            temp_page = self.browser.new_page(
                user_agent=self.config.user_agent,
                viewport={"width": self.config.viewport_width, "height": self.config.viewport_height}
            )
            temp_page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.info(f"Navigating to: {url}")
            temp_page.goto(url, wait_until="domcontentloaded", timeout=45000)
            self._handle_cookie_popup_playwright(temp_page)
            if robust_scroll:
                self._scroll_page_robustly(temp_page, attempts=scroll_attempts)
            else:
                temp_page.wait_for_timeout(random.randint(500,1000))
            html_content = temp_page.content()
            self.logger.info(f"Successfully fetched content for: {url} (Length: {len(html_content)})")
            return html_content
        except PlaywrightTimeoutError as pte:
            self.logger.error(f"Playwright timeout while fetching {url}: {pte}")
            self.stats["errors"] +=1; return None
        except Exception as e:
            self.logger.error(f"General Playwright error fetching {url}: {e}", exc_info=True)
            self.stats["errors"] +=1; return None
        finally:
            if temp_page:
                try: temp_page.close()
                except Exception as e_close: self.logger.warning(f"Error closing temporary page for {url}: {e_close}")

    # --- Stubs for other methods ---
    def _update_venue_context_from_url(self, url: str): self.logger.debug(f"Stub: _update_venue_context_from_url for {url}")

    def extract_main_event_content(self, page_or_html_content: Any, source_url: str) -> Dict[str, Any]:
        """
        Extracts main event content. Accepts Playwright Page object or HTML string.
        Uses BeautifulSoup for internal parsing of the given content.
        """
        content_data = {"full_description": "", "markdown_content": "", "sections": {}, "key_info": {}}
        html_to_parse = ""

        if isinstance(page_or_html_content, str):
            html_to_parse = page_or_html_content
        elif hasattr(page_or_html_content, 'content'): # Check if it's a Playwright Page-like object
            try:
                # Ensure page object is not None and has a content method
                if page_or_html_content:
                    html_to_parse = page_or_html_content.content()
                else:
                    self.logger.error(f"Playwright Page object is None in extract_main_event_content for {source_url}.")
                    return content_data
            except Exception as e:
                self.logger.error(f"Failed to get content from Playwright Page object for {source_url}: {e}", exc_info=True)
                return content_data
        else:
            self.logger.error(f"Invalid type for page_or_html_content in extract_main_event_content for {source_url}. Expected Page or HTML string.")
            return content_data

        if not html_to_parse:
            self.logger.warning(f"No HTML content to parse in extract_main_event_content for {source_url}.")
            return content_data

        soup = BeautifulSoup(html_to_parse, "html.parser")
        
        # Original parsing logic using soup
        content_selectors = [
            "main article", "div.event-content", "div.event-description",
            "div.content-main", "article.post-content",
            "[class*='event-detail']", "[class*='event-info']"
        ]
        main_content_bs = None
        for selector in content_selectors:
            main_content_bs = soup.select_one(selector)
            if main_content_bs: break
        
        if not main_content_bs:
            self.logger.debug(f"No specific main content element found for {source_url}, falling back to body.")
            main_content_bs = soup.find('body')
        
        if main_content_bs:
            content_data["full_description"] = main_content_bs.get_text(separator=' ', strip=True)
            try:
                content_data["markdown_content"] = self.html_to_text_converter.handle(str(main_content_bs))
            except Exception as e:
                self.logger.warning(f"Markdown conversion failed for {source_url}: {e}", exc_info=True)
                content_data["markdown_content"] = content_data["full_description"] # Fallback
            
            for heading in main_content_bs.find_all(['h1', 'h2', 'h3', 'h4']):
                section_title = heading.get_text(strip=True)
                section_content_list = []
                for sibling in heading.find_next_siblings():
                    if sibling.name and sibling.name.startswith('h'): break
                    section_content_list.append(sibling.get_text(strip=True))
                if section_content_list: content_data["sections"][section_title] = ' '.join(filter(None, section_content_list))
            
            info_patterns = {
                "doors_open": r"doors\s*(?:open)?:\s*(\d{1,2}:\d{2})",
                "age_restriction": r"(\d{2}\+|ages?\s*\d{2}\+)",
                "dress_code": r"dress\s*code:\s*([^\.]+)", "capacity": r"capacity:\s*(\d+)",
            }
            full_text_lower = content_data["full_description"].lower()
            for key, pattern in info_patterns.items():
                match = re.search(pattern, full_text_lower, re.IGNORECASE)
                if match: content_data["key_info"][key] = match.group(1).strip()
        else:
            self.logger.warning(f"Could not find main content block (even body) for parsing on {source_url}")

        return content_data

    def extract_comprehensive_ticket_info(self, ticket_section_element_handle: ElementHandle, source_url: str) -> List[Dict[str, Any]]:
        """
        Extracts comprehensive ticket pricing structure from a given Playwright ElementHandle.
        """
        ticket_tiers = []
        if not ticket_section_element_handle:
            self.logger.warning(f"Received null element_handle in extract_comprehensive_ticket_info for {source_url}")
            return ticket_tiers

        # Playwright equivalent selectors for ticket containers
        ticket_selectors_pw = [
            ".ticket-tier", ".price-option", ".ticket-type",
            "[class*='ticket-price']", "[class*='price-tier']",
            # ":has(span.price)" is complex in CSS, might need XPath or careful Playwright chaining.
            # For simplicity, direct child/descendant selectors are more reliable if possible.
            # Example: "li span.price" (if price is a span inside li)
        ]
        
        all_ticket_elements_handles: List[ElementHandle] = []
        for selector in ticket_selectors_pw:
            try:
                elements = ticket_section_element_handle.query_selector_all(selector)
                if elements:
                     all_ticket_elements_handles.extend(elements)
            except Exception as e_sel:
                self.logger.debug(f"Selector '{selector}' for tickets failed in scope for {source_url}: {e_sel}")

        if not all_ticket_elements_handles:
            self.logger.debug(f"No structured ticket elements found with selectors for {source_url}. Falling back to text patterns.")
            text_content = ticket_section_element_handle.text_content()
            if not text_content:
                self.logger.warning(f"No text content in ticket_section_element_handle for {source_url} to parse with regex.")
                return ticket_tiers

            price_pattern = r'([^€£$]*?)\s*[€£$]\s*(\d+(?:\.\d{2})?)'
            matches = re.findall(price_pattern, text_content)
            for tier_name_match, price_match_str in matches: # Renamed for clarity
                tier_name = tier_name_match.strip()
                if tier_name: # Ensure tier_name is not empty
                    try:
                        price = float(price_match_str)
                        ticket_tiers.append({
                            "tier_name": tier_name, "price": price, "currency": "EUR", # Assuming EUR
                            "availability": "unknown", "benefits": []
                        })
                    except ValueError:
                        self.logger.warning(f"Could not parse price '{price_match_str}' to float for tier '{tier_name}' on {source_url}")
            if ticket_tiers: self.logger.info(f"Extracted {len(ticket_tiers)} tiers using regex fallback for {source_url}.")

        else:
            self.logger.info(f"Found {len(all_ticket_elements_handles)} potential ticket elements with Playwright selectors for {source_url}.")
            for ticket_el_handle in all_ticket_elements_handles:
                tier_info = {"tier_name": "", "price": None, "currency": "EUR", "availability": "available", "benefits": []}
                try:
                    name_el = ticket_el_handle.query_selector("h3, h4, .tier-name, .ticket-name")
                    if name_el: tier_info["tier_name"] = name_el.text_content().strip()

                    price_el = ticket_el_handle.query_selector(".price, .ticket-price, span[class*='price']")
                    if price_el:
                        price_text = price_el.text_content().strip()
                        price_match_re = re.search(r'(\d+(?:\.\d{2})?)', price_text) # Renamed var
                        if price_match_re: tier_info["price"] = float(price_match_re.group(1))

                    el_class_attr = ticket_el_handle.get_attribute("class") or ""
                    el_text_content_lower = (ticket_el_handle.text_content() or "").lower() # Ensure text_content is not None
                    if "sold-out" in el_class_attr or "soldout" in el_text_content_lower:
                        tier_info["availability"] = "sold_out"
                    elif "limited" in el_text_content_lower:
                        tier_info["availability"] = "limited"

                    benefit_elements = ticket_el_handle.query_selector_all("ul li, .benefit, .perk")
                    tier_info["benefits"] = [b.text_content().strip() for b in benefit_elements if b.text_content()]

                    if tier_info["tier_name"] and tier_info["price"] is not None:
                        ticket_tiers.append(tier_info)
                    elif tier_info["price"] is not None and not tier_info["tier_name"]: # If price found but no name, try parent text
                        parent_text_content = ticket_el_handle.evaluate("element => element.parentElement.textContent || ''").strip()
                        if parent_text_content: tier_info["tier_name"] = parent_text_content[:50] # Cap length
                        if tier_info["tier_name"]: ticket_tiers.append(tier_info)

                except Exception as e_tier:
                    self.logger.warning(f"Error parsing a specific ticket tier element on {source_url}: {e_tier}", exc_info=True)
        
        self.logger.info(f"Extracted {len(ticket_tiers)} ticket tiers for {source_url}.")
        return ticket_tiers[:3] # Return top 3 tiers

    def standardize_datetime(self, date_str: str, context_url: str = "") -> Optional[Dict[str, Any]]:
        """
        Unified date/time formatting with ISO 8601 compliance. Uses self.logger.
        """
        if not date_str:
            self.logger.debug(f"standardize_datetime received empty date_str for {context_url}")
            return None
        
        datetime_info = {
            "start_datetime": None, "end_datetime": None,
            "timezone": "Europe/Madrid", "is_recurring": False,
            "recurrence_pattern": None, "original_string": date_str
        }
        
        cleaned_date_str = date_str.strip()
        cleaned_date_str = re.sub(r'\s+', ' ', cleaned_date_str) # Normalize whitespace
        
        range_patterns = [
            r'from\s+(.+?)\s+to\s+(.+)', r'(.+?)\s*-\s*(.+)',
            r'(.+?)\s*–\s*(.+)', # en-dash
            r'(.+?)\s*—\s*(.+)'  # em-dash
        ]
        
        for pattern in range_patterns:
            match = re.search(pattern, cleaned_date_str, re.IGNORECASE)
            if match:
                start_str, end_str = match.groups()
                try:
                    # Attempt to parse start and end strings
                    start_dt_naive = date_parser.parse(start_str)
                    end_dt_naive = date_parser.parse(end_str)
                    
                    ibiza_tz = pytz.timezone(datetime_info["timezone"])
                    
                    # Localize if naive
                    start_dt_aware = ibiza_tz.localize(start_dt_naive) if start_dt_naive.tzinfo is None else start_dt_naive
                    end_dt_aware = ibiza_tz.localize(end_dt_naive) if end_dt_naive.tzinfo is None else end_dt_naive

                    datetime_info["start_datetime"] = start_dt_aware
                    datetime_info["end_datetime"] = end_dt_aware
                    self.logger.debug(f"Parsed date range for {context_url}: {start_dt_aware} to {end_dt_aware}")
                    return datetime_info
                except Exception as e:
                    self.logger.debug(f"Failed to parse date range '{cleaned_date_str}' using pattern '{pattern}' for {context_url}: {e}")
        
        # Single date parsing
        try:
            parsed_dt_naive = date_parser.parse(cleaned_date_str)
            ibiza_tz = pytz.timezone(datetime_info["timezone"])
            parsed_dt_aware = ibiza_tz.localize(parsed_dt_naive) if parsed_dt_naive.tzinfo is None else parsed_dt_naive
            
            datetime_info["start_datetime"] = parsed_dt_aware
            
            # Basic recurrence check (can be expanded)
            if re.search(r'every\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday|day|week)', cleaned_date_str, re.IGNORECASE):
                datetime_info["is_recurring"] = True
                # More sophisticated pattern detection could go here
                if "daily" in cleaned_date_str.lower() or "every day" in cleaned_date_str.lower() : datetime_info["recurrence_pattern"] = "daily"
                elif "weekly" in cleaned_date_str.lower() or "every week" in cleaned_date_str.lower() : datetime_info["recurrence_pattern"] = "weekly"
                else: datetime_info["recurrence_pattern"] = "other" # Default if keyword found but not specific
            
            self.logger.debug(f"Parsed single date for {context_url}: {parsed_dt_aware}")
            return datetime_info
            
        except Exception as e:
            self.logger.warning(f"Failed to parse single date '{cleaned_date_str}' for {context_url}: {e}", exc_info=True)
            return None

    def extract_contextual_genres(self, text: str, title: str = "") -> List[str]:
        """
        Extracts music genres from text content using contextual analysis. Uses self.logger.
        """
        if not text:
            self.logger.debug("extract_contextual_genres received empty text.")
            return []
        
        genres = set()
        genre_keywords = {
            "house": ["house", "tech-house", "deep house", "progressive house", "afro house"],
            "techno": ["techno", "minimal techno", "hard techno", "melodic techno"],
            "trance": ["trance", "psy-trance", "progressive trance", "uplifting trance"],
            "drum_and_bass": ["drum and bass", "dnb", "d&b", "jungle"],
            "disco": ["disco", "nu-disco", "disco house", "cosmic disco"],
            "funk": ["funk", "funky", "future funk"], "soul": ["soul", "neo-soul", "soulful"],
            "hip_hop": ["hip hop", "hip-hop", "rap", "trap"], "reggae": ["reggae", "dub", "dancehall"],
            "latin": ["latin", "reggaeton", "salsa", "bachata"],
            "electronic": ["electronic", "electronica", "idm", "ambient", "edm"],
            "pop": ["pop", "dance pop", "synth pop"], "rock": ["rock", "indie rock", "alternative rock"],
            "jazz": ["jazz", "acid jazz", "jazz fusion"], "world": ["world music", "afrobeat", "ethnic"],
            "experimental": ["experimental", "avant-garde", "noise"],
            "ukg": ["uk garage", "ukg"], "bassline": ["bassline"],
            "grime": ["grime"], "dubstep": ["dubstep", "brostep"],
            "amapiano": ["amapiano"], "hardstyle": ["hardstyle"],
            "hardcore": ["hardcore", "gabber"], "downtempo": ["downtempo", "chillout"]
        }
        
        text_lower = (text + " " + title).lower() # Combine text and title for broader context
        
        for genre_category, keywords_list in genre_keywords.items():
            for keyword in keywords_list:
                # Use word boundaries for more precise matching if keyword is a full word
                # but allow substring match if it's part of a compound genre (e.g., "tech" in "tech-house")
                pattern = r'\b' + re.escape(keyword) + r'\b' if len(keyword) > 3 else re.escape(keyword)
                if re.search(pattern, text_lower):
                    genres.add(genre_category)
                    # Add specific sub-genre if it's not the main category and is multi-word or specific
                    if keyword != genre_category and (len(keyword.split()) > 1 or "-" in keyword):
                         genres.add(keyword.replace(" ", "_").replace("-", "_"))

        # Remove generic "electronic" if more specific electronic genres are found
        specific_electronic_found = any(g in genres for g in ["house", "techno", "trance", "drum_and_bass", "dubstep", "ukg"])
        if "electronic" in genres and specific_electronic_found and len(genres) > 1:
            genres.discard("electronic")
            self.logger.debug("Removed generic 'electronic' due to presence of specific electronic subgenres.")

        if not genres:
             self.logger.debug(f"No distinct genres found in text: '{text_lower[:200]}...'")
        else:
             self.logger.info(f"Extracted genres: {sorted(list(genres))} from text.")
        return sorted(list(genres))

    def extract_performers_from_content(self, scope_element_handle_or_html: Any, fallback_text_content: str, source_url: str) -> List[Dict[str, Any]]:
        """
        Extracts performers. Uses Playwright ElementHandle for structured data if provided,
        otherwise BeautifulSoup on HTML string, then regex on fallback_text_content.
        Social media linking is a general attempt based on page HTML (passed to extract_social_media_links).
        """
        performers: List[Dict[str, Any]] = []
        seen_names: Set[str] = set()

        performer_selectors = [".artist", ".dj", ".performer", "[class*='lineup']", "[class*='artist']", "h3:contains('Line-up')", "h3:contains('Artists')"] # TODO: :contains is BS specific

        if hasattr(scope_element_handle_or_html, 'query_selector_all'): # Is Playwright ElementHandle
            element_handle = scope_element_handle_or_html
            self.logger.debug(f"Extracting performers using Playwright ElementHandle for {source_url}")
            for selector in performer_selectors:
                # Adapt selectors if they are BS specific (like :contains)
                pw_selector = selector.replace(":contains(", ":has-text(") # Basic adaptation
                try:
                    elements = element_handle.query_selector_all(pw_selector)
                    for el_handle in elements:
                        name = (el_handle.text_content() or "").strip()
                        if name and name not in seen_names:
                            seen_names.add(name)
                            # Role detection from Playwright element_handle is harder without complex JS/XPath
                            performers.append({"name": name, "role": "dj", "social_media": {}, "performance_time": None})
                except Exception as e_pw_perf:
                    self.logger.debug(f"PW performer selector '{pw_selector}' error for {source_url}: {e_pw_perf}")

        elif isinstance(scope_element_handle_or_html, str): # Is HTML string
            self.logger.debug(f"Extracting performers using BeautifulSoup on HTML snippet for {source_url}")
            soup = BeautifulSoup(scope_element_handle_or_html, "html.parser")
            for selector in performer_selectors:
                try:
                    elements = soup.select(selector)
                    for el in elements:
                        name = el.get_text(strip=True)
                        if name and name not in seen_names:
                            seen_names.add(name)
                            parent_text = (el.parent.get_text() if el.parent else "").lower()
                            role = "dj"
                            if "headline" in parent_text: role = "headliner"
                            elif "support" in parent_text: role = "support"
                            elif "live" in parent_text: role = "live_act"
                            performers.append({"name": name, "role": role, "social_media": {}, "performance_time": None})
                except Exception as e_bs_perf:
                    self.logger.debug(f"BS performer selector '{selector}' error for {source_url}: {e_bs_perf}")

        # Pattern-based extraction from fallback_text_content if no structured performers found
        if not performers and fallback_text_content:
            self.logger.debug(f"No structured performers found for {source_url}, trying regex on fallback text.")
            headliner_patterns = [
                r'(?:headline[rd]?|featuring|presents?|starring)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'([A-Z][A-Za-z\s&\-\.]+)\s*(?:headline[rs]?|presents?)',
            ]
            for pattern in headliner_patterns:
                matches = re.findall(pattern, fallback_text_content)
                for match_name in matches: # Renamed var
                    name = match_name.strip()
                    if len(name) > 2 and name not in seen_names:
                        seen_names.add(name); performers.append({"name": name, "role": "headliner", "social_media": {}, "performance_time": None})
            
            support_patterns = [
                r'(?:support|supported\s+by|with|alongside)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'(?:also\s+playing|residents?|djs?):\s*([A-Z][A-Za-z\s&\-\.,]+)'
            ]
            for pattern in support_patterns:
                matches = re.findall(pattern, fallback_text_content)
                for match_list_str in matches: # Renamed var
                    names = re.split(r'[,&]', match_list_str)
                    for name_item in names: # Renamed var
                        name = name_item.strip()
                        if len(name) > 2 and name not in seen_names:
                            seen_names.add(name); performers.append({"name": name, "role": "support", "social_media": {}, "performance_time": None})
        
        # Social media linking is complex here. It ideally needs page-level HTML.
        # If scope_element_handle_or_html was page HTML, extract_social_media_links could use it.
        # For now, this part is omitted from this specific function, assuming it's handled at a higher level
        # or extract_social_media_links is called with full page HTML.

        self.logger.info(f"Extracted {len(performers)} performers for {source_url}.")
        return performers

    def extract_social_media_links(self, html_content_str: str) -> Dict[str, Any]:
        """
        Extracts and validates social media links from an HTML string. Uses self.logger.
        """
        social_media: Dict[str, Any] = {}
        if not html_content_str:
            self.logger.debug("extract_social_media_links received empty html_content_str.")
            return social_media

        platform_patterns = {
            "facebook": r'(?:https?://)?(?:www\.)?facebook\.com/[\w\-\.]+',
            "instagram": r'(?:https?://)?(?:www\.)?instagram\.com/[\w\-\.]+',
            "twitter": r'(?:https?://)?(?:www\.)?twitter\.com/[\w\-\.]+',
            "soundcloud": r'(?:https?://)?(?:www\.)?soundcloud\.com/[\w\-\.]+',
            "spotify": r'(?:https?://)?open\.spotify\.com/(?:artist|user|playlist)/[\w]+', # Made more general
            "youtube": r'(?:https?://)?(?:www\.)?youtube\.com/(?:c/|channel/|user/|@)[\w\-]+', # Added @handle
            "mixcloud": r'(?:https?://)?(?:www\.)?mixcloud\.com/[\w\-\.]+',
            "beatport": r'(?:https?://)?(?:www\.)?beatport\.com/(?:artist|label)/[\w\-]+(?:/\d+)?', # Optional ID
            "tiktok": r'(?:https?://)?(?:www\.)?tiktok\.com/@[\w\-\.]+',
            "linktree": r'(?:https?://)?linktr\.ee/[\w\-\.]+'
            # Consider adding residentadvisor, telegram, etc. if relevant
        }
        
        for platform, pattern in platform_patterns.items():
            try:
                matches = re.findall(pattern, html_content_str, re.IGNORECASE)
                if matches:
                    cleaned_urls = []
                    for url_match in matches:
                        # Handle cases where regex might return tuples (e.g. from groups)
                        m_url = url_match if isinstance(url_match, str) else url_match[0]
                        if not m_url.startswith('http'):
                            m_url = 'https://' + m_url
                        # Basic validation: check if it looks like a plausible URL structure
                        parsed_url = urlparse(m_url)
                        if parsed_url.scheme and parsed_url.netloc:
                             if m_url not in cleaned_urls:
                                cleaned_urls.append(m_url)
                        else:
                            self.logger.debug(f"Skipping invalid-looking URL for {platform}: {m_url}")

                    if cleaned_urls:
                        # Store single URL as string, multiple as list (current behavior)
                        social_media[platform] = cleaned_urls[0] if len(cleaned_urls) == 1 else cleaned_urls
                        self.logger.debug(f"Found {platform} links: {social_media[platform]}")
            except Exception as e_regex:
                self.logger.error(f"Error during regex for {platform} in extract_social_media_links: {e_regex}", exc_info=True)

        if not social_media:
            self.logger.debug("No social media links found in the provided HTML content.")
        else:
            self.logger.info(f"Extracted social media links: {social_media}")
        return social_media

    def _identify_social_media_type(self, url: str) -> Optional[str]:
        """Identifies social media platform from URL. Uses self.logger."""
        if not url or not isinstance(url, str):
            self.logger.debug("_identify_social_media_type received invalid URL.")
            return None

        platform_domains = {
            "facebook.com": "facebook", "instagram.com": "instagram", "twitter.com": "twitter",
            "soundcloud.com": "soundcloud", "spotify.com": "spotify", "youtube.com": "youtube",
            "mixcloud.com": "mixcloud", "beatport.com": "beatport", "tiktok.com": "tiktok",
            "linktr.ee": "linktree", "t.me": "telegram", "wa.me": "whatsapp", # Added Telegram & WhatsApp
            "residentadvisor.net": "residentadvisor" # Added Resident Advisor
        }
        
        try:
            parsed_url = urlparse(url.lower()) # Work with lowercase for domain matching
            netloc = parsed_url.netloc

            # Handle cases like 'm.facebook.com' -> 'facebook.com'
            if netloc.startswith('www.'): netloc = netloc[4:]
            if netloc.startswith('m.'): netloc = netloc[2:]

            for domain, platform in platform_domains.items():
                if domain == netloc or netloc.endswith('.' + domain): # Check netloc or subdomains
                    self.logger.debug(f"Identified platform '{platform}' for URL: {url}")
                    return platform

            # Path-based checks for less distinct domains (e.g. link aggregators or generic shorteners)
            # This part would be more complex and heuristic based. For now, focusing on domain.
            # Example: if 't.co' (Twitter shortener) found, it's Twitter.

            self.logger.debug(f"Could not identify platform for URL: {url}")
            return None
        except Exception as e:
            self.logger.error(f"Error in _identify_social_media_type for URL '{url}': {e}", exc_info=True)
            return None

    def calculate_completeness_score(self, event_data: Dict[str, Any]) -> float:
        """Calculates completeness score based on essential field presence. Uses self.logger."""
        if not event_data: return 0.0
        essential_fields = {
            "title": 0.20, "datetime_obj": 0.25, "venue": 0.15, # Assuming 'venue' is a key in event_data
            "full_description": 0.10, "tier_1": 0.10, # Reduced weight for tier_1 slightly
            "genres": 0.10, "artists": 0.10 # Increased weight for artists
        }
        score = 0.0
        missing_fields = []
        for field, weight in essential_fields.items():
            if event_data.get(field): # Check if field exists and is not None/empty
                score += weight
            else:
                missing_fields.append(field)
        
        if missing_fields:
            self.logger.debug(f"Completeness score for '{event_data.get('title', 'N/A')}' is {score*100:.1f}%. Missing fields: {missing_fields}")
        else:
            self.logger.debug(f"Completeness score for '{event_data.get('title', 'N/A')}' is {score*100:.1f}%. All essential fields present.")
        return score * 100

    def calculate_accuracy_score(self, event_data: Dict[str, Any]) -> float:
        """Calculates accuracy score based on data validation rules. Uses self.logger."""
        if not event_data: return 0.0
        score = 100.0
        penalties: List[Tuple[str, float]] = [] # Ensure type hint for list of tuples

        dt_val = event_data.get("datetime_obj")
        if dt_val and isinstance(dt_val, datetime): # Check type
            now = datetime.now(pytz.UTC if dt_val.tzinfo else None) # Make now timezone-aware if dt_val is
            # Ensure dt_val is offset-aware if now is.
            # If dt_val is naive, but should be Europe/Madrid, this needs careful handling.
            # Assuming standardize_datetime correctly returns offset-aware datetime objects.
            if dt_val.tzinfo is None and now.tzinfo is not None: # dt_val is naive, now is aware
                 # This case should ideally be handled by ensuring standardize_datetime always returns aware objects
                 self.logger.warning(f"Naive datetime_obj '{dt_val}' being compared with aware 'now' for event '{event_data.get('title', 'N/A')}'")
                 # For safety, skip date-based accuracy checks if timezone mismatch is an issue, or localize dt_val here.
            else: # Both are naive or both are aware
                if dt_val < now - timedelta(days=30): penalties.append(("past_event", 20.0))
                elif dt_val > now + timedelta(days=365*2): penalties.append(("far_future_event", 15.0))

        for tier_key in ["tier_1", "tier_2", "tier_3"]:
            tier_data = event_data.get(tier_key)
            if tier_data and isinstance(tier_data, dict):
                price = tier_data.get("price")
                if price is not None:
                    try:
                        price_float = float(price)
                        if price_float < 0 or price_float > 5000:
                            penalties.append((f"{tier_key}_invalid_price", 15.0))
                    except ValueError:
                         penalties.append((f"{tier_key}_unparseable_price", 15.0))

        for reason, penalty_val in penalties:
            score -= penalty_val
            self.logger.debug(f"Accuracy penalty for '{event_data.get('title', 'N/A')}': {reason} (-{penalty_val})")

        final_score = max(0.0, score)
        self.logger.debug(f"Accuracy score for '{event_data.get('title', 'N/A')}': {final_score:.1f}%")
        return final_score

    def calculate_freshness_score(self, event_data: Dict[str, Any]) -> float:
        """Calculates freshness score based on 'scrapedAt'. Uses self.logger."""
        if not event_data: return 0.0
        score = 100.0
        scraped_at_str = event_data.get("scrapedAt") # Should be ISO format string
        
        if scraped_at_str and isinstance(scraped_at_str, str):
            try:
                # Ensure correct parsing of ISO format string, possibly with timezone
                scraped_time = datetime.fromisoformat(scraped_at_str.replace('Z', '+00:00'))
                if scraped_time.tzinfo is None: # If somehow still naive, make it UTC
                     scraped_time = pytz.utc.localize(scraped_time)

                age_hours = (datetime.now(timezone.utc) - scraped_time).total_seconds() / 3600
                
                if age_hours > 24: # Older than 1 day
                    score -= min(50.0, (age_hours / 24) * 10.0)  # Lose 10 points per day, max 50
                self.logger.debug(f"Freshness for '{event_data.get('title', 'N/A')}': {age_hours:.1f} hours old, score {score:.1f}%.")
            except Exception as e:
                self.logger.warning(f"Failed to parse scrapedAt time '{scraped_at_str}' for freshness calculation of '{event_data.get('title', 'N/A')}': {e}")
                score = 50.0 # Penalize if scrapedAt is unparseable
        else:
            self.logger.debug(f"No 'scrapedAt' field for freshness calculation of '{event_data.get('title', 'N/A')}'. Defaulting score.")
            score = 75.0 # Default score if no scrapedAt provided

        final_score = max(0.0, score)
        return final_score

    def trigger_fallback_methods(self, page_content_html: str, url: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Implements fallback methods when quality is low.
        Accepts HTML content string and parses it with BeautifulSoup internally. Uses self.logger.
        """
        # Calculate current quality (using the already refactored method)
        # Assuming QualityScorer.calculate_event_quality might be deprecated or also refactored.
        # For now, let's use completeness score as a proxy for triggering.
        current_completeness_score = self.calculate_completeness_score(event_data)

        # Check a more comprehensive quality score if available from QualityScorer, otherwise use completeness.
        # This part might need linking to a refactored QualityScorer if its methods change.
        # quality_details = self.scorer.calculate_event_quality(event_data) # Assuming scorer is available
        # current_overall_score = quality_details.get("score", current_completeness_score) # Fallback to completeness
        current_overall_score = current_completeness_score # Simplified for now

        if current_overall_score >= 75: # Threshold for triggering fallbacks
            self.logger.info(f"Initial quality score {current_overall_score:.1f}% for {url} is good. Skipping extensive fallbacks.")
            return event_data

        self.logger.info(f"Triggering fallback methods for {url} due to low quality score: {current_overall_score:.1f}%")
        
        if not page_content_html:
            self.logger.warning(f"No HTML content provided to trigger_fallback_methods for {url}. Cannot apply BS-based fallbacks.")
            return event_data

        soup = BeautifulSoup(page_content_html, "html.parser")
        fallback_data = event_data.copy()
        
        # Fallback 1: Check meta tags
        meta_mappings = {
            "description": ["description", "og:description", "twitter:description"],
            "title": ["og:title", "twitter:title"],
            # "image_url": ["og:image", "twitter:image"] # Assuming image_url is part of unified schema
        }
        for field, meta_names in meta_mappings.items():
            if not fallback_data.get(field) or (field == "description" and not fallback_data.get("full_description")):
                for meta_name in meta_names:
                    meta_tag = soup.find("meta", attrs={"property": meta_name}) or \
                               soup.find("meta", attrs={"name": meta_name})
                    if meta_tag and meta_tag.get("content"):
                        content_value = meta_tag["content"].strip()
                        if field == "description": fallback_data["full_description"] = content_value
                        # elif field == "image_url": fallback_data["image_url"] = content_value
                        else: fallback_data[field] = content_value
                        self.logger.debug(f"Fallback used meta tag '{meta_name}' for field '{field}' on {url}.")
                        break

        # Fallback 2: Schema.org microdata (if not already robustly parsed by primary methods)
        # This primarily targets title and dates if they are still missing.
        if not fallback_data.get("title") or not fallback_data.get("datetime_obj"):
            # Adjusted to find any itemtype that ends with 'Event'
            schema_events = soup.find_all(attrs={"itemtype": re.compile(r"schema\.org/(Event|MusicEvent)$", re.I)})
            if schema_events: self.logger.debug(f"Found {len(schema_events)} Schema.org Event items for fallback on {url}.")
            for schema_event in schema_events:
                if not fallback_data.get("title"):
                    title_el = schema_event.find(attrs={"itemprop": "name"})
                    if title_el and title_el.get_text(strip=True):
                        fallback_data["title"] = title_el.get_text(strip=True)
                        self.logger.debug(f"Fallback used Schema.org 'name' for title on {url}.")

                if not fallback_data.get("datetime_obj"):
                    date_el = schema_event.find(attrs={"itemprop": "startDate"})
                    if date_el and date_el.get("content"):
                        datetime_info = self.standardize_datetime(date_el["content"], context_url=url)
                        if datetime_info and datetime_info.get("start_datetime"):
                            fallback_data["datetime_obj"] = datetime_info["start_datetime"]
                            fallback_data["datetime_info"] = datetime_info # Store full info
                            self.logger.debug(f"Fallback used Schema.org 'startDate' for datetime on {url}.")
                # Break if essential fields are now filled by this schema_event item
                if fallback_data.get("title") and fallback_data.get("datetime_obj"): break

        # Fallback 3: Aggressive text pattern matching for artists if still missing
        if not fallback_data.get("artists"):
            page_text_content = soup.get_text() # Full text from the HTML snippet
            # Using the same patterns as in extract_performers_from_content's fallback
            headliner_patterns = [
                r'(?:headline[rd]?|featuring|presents?|starring)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'([A-Z][A-Za-z\s&\-\.]+)\s*(?:headline[rs]?|presents?)',
            ]
            found_artists_list = [] # Renamed for clarity
            for pattern in headliner_patterns:
                matches = re.findall(pattern, page_text_content)
                for name_match in matches: found_artists_list.append(name_match.strip())
            
            support_patterns = [
                r'(?:support|supported\s+by|with|alongside)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'(?:also\s+playing|residents?|djs?):\s*([A-Z][A-Za-z\s&\-\.,]+)'
            ]
            for pattern in support_patterns:
                matches = re.findall(pattern, page_text_content)
                for name_list_str in matches:
                    names_from_list = re.split(r'[,&]', name_list_str)
                    for name_item in names_from_list: found_artists_list.append(name_item.strip())
            
            if found_artists_list:
                unique_artist_names = sorted(list(set(name for name in found_artists_list if name and len(name) > 2)))
                if unique_artist_names:
                    fallback_data["artists"] = [{"name": art_name, "role": "dj", "social_media": {}, "performance_time": None}
                                               for art_name in unique_artist_names]
                    self.logger.debug(f"Fallback found {len(unique_artist_names)} artists via text patterns on {url}.")

        final_completeness_score = self.calculate_completeness_score(fallback_data)
        self.logger.info(f"Fallback methods for {url}: Completeness changed from {current_completeness_score:.1f}% to {final_completeness_score:.1f}%.")
        return fallback_data

    def parse_event_date(self, date_str: str, year: Optional[int] = None, context_url: str = "") -> Optional[dt.datetime]:
        """Parses a date string, potentially with year context. Uses self.logger."""
        if not date_str:
            self.logger.debug(f"parse_event_date received empty date_str for {context_url}")
            return None
        
        # Attempt to use the more comprehensive standardize_datetime first
        datetime_info = self.standardize_datetime(date_str, context_url=context_url)
        if datetime_info and datetime_info.get("start_datetime"):
            # Ensure it's a datetime object before returning
            start_dt = datetime_info["start_datetime"]
            if isinstance(start_dt, datetime): # Or dt.datetime
                # If a specific year context is provided and it differs, attempt to adjust
                if year and start_dt.year != year:
                    try:
                        start_dt = start_dt.replace(year=year)
                        self.logger.debug(f"Adjusted year in parse_event_date for {context_url} to {year}. Original was {datetime_info['start_datetime'].year}.")
                    except ValueError: # Handles cases like Feb 29 on a non-leap year
                        self.logger.warning(f"Could not replace year to {year} for date {start_dt} on {context_url} (possibly invalid date).")
                        # Decide if to return original or None. Returning original for now.
                return start_dt
            else: # Should not happen if standardize_datetime is correct
                self.logger.warning(f"standardize_datetime for '{date_str}' did not return a valid datetime object for start_datetime on {context_url}.")


        self.logger.debug(f"standardize_datetime did not yield a usable start_datetime for '{date_str}' on {context_url}. Trying direct date_parser logic.")
        cleaned_date_str = date_str.replace("Desde", "").replace("From", "").strip()
        parsed_dt_val = None
        
        try:
            # Try parsing without explicit year first
            parsed_dt_val = date_parser.parse(cleaned_date_str)
            # If year context is given and parsed year is different (or default current year by date_parser)
            if year and parsed_dt_val and parsed_dt_val.year != year:
                # Check if parsed_dt_val is current year, and year context is different, then it's likely a specific year event
                # Or if it's just a different year, override.
                if parsed_dt_val.year == datetime.now().year or parsed_dt_val.year != year :
                     try:
                        parsed_dt_val = parsed_dt_val.replace(year=year)
                     except ValueError:
                        self.logger.warning(f"Could not replace year to {year} for date {parsed_dt_val} (from '{cleaned_date_str}') on {context_url}.")
                        # Try parsing with year directly if replace failed
                        try:
                            parsed_dt_val = date_parser.parse(f"{cleaned_date_str} {year}")
                        except Exception as e_direct_year:
                             self.logger.warning(f"Direct parse with year '{year}' for '{cleaned_date_str}' failed on {context_url}: {e_direct_year}")
                             return None # Give up if direct parse with year also fails after replace error
            return parsed_dt_val
        except (ValueError, TypeError) as e_parse:
            self.logger.debug(f"Initial date_parser.parse failed for '{cleaned_date_str}' on {context_url}: {e_parse}.")
            if year: # If year context is available, try parsing with it directly
                try:
                    parsed_dt_val = date_parser.parse(f"{cleaned_date_str} {year}")
                    # No need to replace year here as it was explicitly part of the parse string
                    return parsed_dt_val
                except Exception as e_year_add:
                    self.logger.warning(f"Could not parse date '{cleaned_date_str}' even with explicit year {year} on {context_url}. Errors: initial: {e_parse}, with year: {e_year_add}.")
            else: # No year context, and initial parsing failed
                self.logger.warning(f"Could not parse date '{cleaned_date_str}' (no year context) on {context_url}. Error: {e_parse}.")

        return None # Return None if all parsing attempts fail

    def extract_genres_from_text(self, text_content: Optional[str]) -> List[str]:
        """Extracts genres by calling extract_contextual_genres. Uses self.logger."""
        if not text_content:
            self.logger.debug("extract_genres_from_text received empty text_content.")
            return []
        # extract_contextual_genres already uses self.logger
        return self.extract_contextual_genres(text_content)

    def scrape_partycal_ticket_card(self, card_element_handle: ElementHandle, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        """
        Scrapes data from an event card ElementHandle using Playwright locators.
        Calls other parsing helpers which might still use BeautifulSoup on snippets for now.
        """
        try:
            self.logger.debug(f"Scraping partycal ticket card for URL: {current_page_url}")
            # Basic event_data structure
            event_data: Dict[str, Any] = { # Explicitly typed
                "title": None, "datetime_obj": None, "raw_date_string": None,
                "artists": [], "tier_1": None, "tier_2": None, "tier_3": None,
                "event_card_all_text": (card_element_handle.text_content() or "").strip(),
                "scrapedAt": datetime.now(timezone.utc).isoformat(), # Use timezone.utc
                "extractionMethod": "playwright_card_parse",
                "page_year_context": page_year,
                "full_description": None, "markdown_content": None, # These are usually page-level
                "content_sections": {}, "key_info": {},
                "venue": None, # Will attempt to get from card image alt or page context
                "promoter": None, # Usually from page context
                "tickets_url": current_page_url # Default to current page, can be overridden by specific link
            }

            # Header: Title and Date/Time
            # Using Playwright's recommended way: locator chaining from the element handle
            header_loc = card_element_handle.locator("div.ticket-header").first
            if header_loc.is_visible(): # Check visibility before operations
                title_el_loc = header_loc.locator(self.config.EVENT_SELECTORS_SPOTLIGHT.get('title', 'h3 a')).first # Use config
                if title_el_loc.is_visible():
                    event_data["title"] = (title_el_loc.text_content() or "").strip()
                    # If the title link is also the event URL (common pattern)
                    href = title_el_loc.get_attribute('href')
                    if href: event_data["tickets_url"] = urljoin(current_page_url, href)

                date_el_loc = header_loc.locator(self.config.EVENT_SELECTORS_SPOTLIGHT.get('time', 'time')).first
                if not date_el_loc.is_visible(): # Fallback selector for date
                    date_el_loc = header_loc.locator("div.ticket-date").first

                if date_el_loc.is_visible():
                    raw_date = (date_el_loc.text_content() or "").strip()
                    event_data["raw_date_string"] = raw_date
                    # standardize_datetime uses self.logger internally
                    datetime_info = self.standardize_datetime(raw_date, context_url=current_page_url)
                    if datetime_info and datetime_info.get("start_datetime"):
                        event_data["datetime_obj"] = datetime_info["start_datetime"]
                        event_data["datetime_info"] = datetime_info
            else:
                self.logger.debug(f"Ticket header not found or not visible for a card on {current_page_url}")

            # Body: Ticket tiers, Artists
            body_loc = card_element_handle.locator("div.ticket-body").first
            if body_loc.is_visible():
                # Ticket Info: Pass the ElementHandle of the body to the (refactored) helper
                event_data_tiers = self.extract_comprehensive_ticket_info(body_loc.element_handle(), source_url=current_page_url)
                for i, tier in enumerate(event_data_tiers[:3]): # Max 3 tiers
                    event_data[f"tier_{i+1}"] = tier
                
                # Artists: Pass ElementHandle of body and its text content as fallback
                # extract_performers_from_content is already refactored to accept ElementHandle
                event_data["artists"] = self.extract_performers_from_content(
                    body_loc.element_handle(),
                    (body_loc.text_content() or "").strip(),
                    source_url=current_page_url
                )
            else:
                self.logger.debug(f"Ticket body not found or not visible for a card on {current_page_url}")

            # Venue name from image alt text if available (using configured selector)
            venue_img_loc = card_element_handle.locator(self.config.EVENT_SELECTORS_SPOTLIGHT.get('venue_image_alt_for_name', 'div.ticket-header-bottom img')).first
            if venue_img_loc.is_visible():
                alt_text = venue_img_loc.get_attribute('alt')
                if alt_text:
                    event_data["venue"] = alt_text.strip()
                    self.logger.debug(f"Extracted venue '{event_data['venue']}' from image alt text on {current_page_url}")

            # Social media links from this card (if any specific to this card)
            # This typically needs the HTML of the card.
            card_html_snippet = card_element_handle.inner_html()
            event_data["social_media_links"] = self.extract_social_media_links(card_html_snippet) # extract_social_media_links needs HTML string
            
            # Fallback methods are typically applied at a higher level (e.g., scrape_promoter_page)
            # after initial data from various sources (card, JSON-LD, page content) is merged.
            # So, trigger_fallback_methods is NOT called here directly on the card's data alone.

            if event_data.get("title"):
                self.logger.info(f"Successfully parsed card: {event_data['title']} from {current_page_url}")
                return event_data
            else:
                self.logger.warning(f"Card parsing resulted in no title for an event on {current_page_url}. Card text: {event_data['event_card_all_text'][:100]}...")
                return None
        except Exception as e:
            self.logger.error(f"Error in scrape_partycal_ticket_card for {current_page_url}: {e}", exc_info=True)
            return None

    def parse_json_ld_event(self, event_data_json: dict, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        """
        Parses JSON-LD event data (already loaded into a dict). Uses self.logger.
        """
        try:
            self.logger.debug(f"Parsing JSON-LD event from: {current_page_url}")
            raw_date = event_data_json.get("startDate", "")
            # standardize_datetime is already refactored and uses self.logger
            datetime_info = self.standardize_datetime(raw_date, context_url=current_page_url)

            performers = []
            p_data = event_data_json.get("performer", [])
            p_list = p_data if isinstance(p_data, list) else ([p_data] if p_data else []) # Ensure p_list is always a list

            for p_item in p_list:
                performer_obj = None
                if isinstance(p_item, dict):
                    performer_name = p_item.get("name", "").strip()
                    if performer_name: # Ensure name is not empty
                        performer_obj = {"name": performer_name, "role": "performer", "social_media": {}, "performance_time": None}
                        # _identify_social_media_type is already refactored
                        if (same_as_urls := p_item.get("sameAs")): # Walrus operator for conciseness
                            social_urls = same_as_urls if isinstance(same_as_urls, list) else [same_as_urls]
                            for url_link in social_urls:
                                if (social_type := self._identify_social_media_type(url_link)):
                                    performer_obj["social_media"][social_type] = url_link
                elif isinstance(p_item, str) and p_item.strip(): # Ensure string performer is not empty
                    performer_obj = {"name": p_item.strip(), "role": "performer", "social_media": {}, "performance_time": None}

                if performer_obj: performers.append(performer_obj)

            location_json = event_data_json.get("location")
            event_location_dict = {"venue": None, "address": None, "city": None, "postal_code": None, "country": None }
            if isinstance(location_json, dict):
                event_location_dict['venue'] = (location_json.get("name") or "").strip()
                address_obj = location_json.get("address")
                if isinstance(address_obj, dict):
                    event_location_dict['address'] = (address_obj.get("streetAddress") or "").strip()
                    event_location_dict['city'] = (address_obj.get("addressLocality") or "").strip()
                    event_location_dict['postal_code'] = (address_obj.get("postalCode") or "").strip()
                    event_location_dict['country'] = (address_obj.get("addressCountry") or "").strip()
                elif isinstance(address_obj, str):
                    event_location_dict['address'] = address_obj.strip()
            elif isinstance(location_json, str):
                event_location_dict['venue'] = location_json.strip() # Assume the string is the venue name if not a dict

            parsed_event_data = {
                "title": (event_data_json.get("name") or "").strip(),
                "datetime_obj": datetime_info.get("start_datetime") if datetime_info else None,
                "datetime_info": datetime_info,
                "raw_date_string": raw_date,
                "json_ld_description": (event_data_json.get("description") or "").strip(),
                "location": event_location_dict,
                "artists": performers,
                "tier_1": None, "tier_2": None, "tier_3": None, # Tiers usually not in JSON-LD
                "json_ld_url": event_data_json.get("url"),
                "scrapedAt": datetime.now(timezone.utc).isoformat(),
                "extractionMethod": "json-ld",
                "page_year_context": page_year,
                "social_media_links": {} # Page-level social links usually added later
            }
            if parsed_event_data["title"]:
                 self.logger.info(f"Successfully parsed JSON-LD event: {parsed_event_data['title']} from {current_page_url}")
                 return parsed_event_data
            else:
                 self.logger.warning(f"Parsed JSON-LD event from {current_page_url} but title is missing.")
                 return None
        except Exception as e:
            self.logger.error(f"Error parsing JSON-LD event data from {current_page_url}: {e}", exc_info=True)
            return None

    def is_duplicate(self, new_event: Dict[str, Any], existing_events: List[Dict[str, Any]]) -> bool:
        """Checks for duplicates based on title and date. Uses self.logger for debug."""
        if not new_event or not new_event.get("title"):
            self.logger.debug("is_duplicate check: new_event is None or has no title.")
            return False # Cannot determine duplicate without a title

        norm_new_title = new_event["title"].strip().lower()
        new_dt = new_event.get("datetime_obj")
        new_raw_date = new_event.get("raw_date_string", "").strip().lower()

        for ex_event in existing_events:
            if not ex_event or not ex_event.get("title"):
                continue # Skip invalid existing event

            norm_ex_title = ex_event["title"].strip().lower()
            if norm_new_title == norm_ex_title:
                ex_dt = ex_event.get("datetime_obj")
                # Case 1: Both have datetime objects
                if new_dt and ex_dt and isinstance(new_dt, datetime) and isinstance(ex_dt, datetime):
                    if new_dt.date() == ex_dt.date():
                        self.logger.debug(f"Duplicate found (title and datetime_obj): '{norm_new_title}'")
                        return True
                # Case 2: Neither has datetime_obj, compare raw_date_string (less reliable but a fallback)
                elif not new_dt and not ex_dt and new_raw_date and new_raw_date == (ex_event.get("raw_date_string", "").strip().lower()):
                    self.logger.debug(f"Potential duplicate found (title and raw_date_string): '{norm_new_title}'")
                    return True
                # Case 3: One has datetime_obj, other doesn't. Could try parsing raw_date_string of the other.
                # This can be complex. For now, if one has dt and other doesn't, consider them different to be safe.
                # Or, if raw_date_string matches even if one has dt_obj, it's a strong indicator.
                elif new_raw_date and new_raw_date == (ex_event.get("raw_date_string", "").strip().lower()):
                     self.logger.debug(f"Potential duplicate found (title and matching raw_date_string, one might have datetime_obj): '{norm_new_title}'")
                     return True


        return False

    def scrape_promoter_page(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrapes events from a promoter page using Playwright for fetching,
        and a hybrid approach (Playwright preferred, BS fallback) for parsing.
        """
        self.logger.info(f"Scraping promoter page: {url}")
        final_page_events: List[Dict[str, Any]] = []

        # Fetch page content using Playwright
        # Robust scroll is generally good for promoter pages that might have dynamic content.
        page_content_html = self.fetch_page_content(url, robust_scroll=True, scroll_attempts=4)
        
        if not page_content_html:
            self.logger.warning(f"Could not fetch HTML content for promoter page: {url}")
            return []

        # Determine current promoter slug from URL
        current_promoter_slug: Optional[str] = None
        parsed_url_obj = urlparse(url)
        path_parts = parsed_url_obj.path.strip('/').split('/')
        if len(path_parts) >= 2 and (path_parts[-2] == 'promoters' or path_parts[-2] == 'promoter'):
            current_promoter_slug = path_parts[-1]
            self.logger.debug(f"Current promoter slug: {current_promoter_slug} for {url}")

        # For operations requiring DOM inspection (JSON-LD, card elements, year context, pagination),
        # we'll set the fetched HTML content on a temporary Playwright page.
        # This allows using Playwright's locators and evaluation capabilities.
        temp_page_for_parsing: Optional[Page] = None
        if not self.browser:
            self.logger.error("Browser not available in scrape_promoter_page. Cannot parse with Playwright.")
            # As a fallback, could use BeautifulSoup for everything here if browser is gone.
            # For now, let's assume browser should be available if __enter__ was successful.
            soup = BeautifulSoup(page_content_html, "html.parser") # Fallback parsing with BS
            # ... (limited BS-only parsing - this path should ideally not be hit often) ...
            self.logger.warning(f"Proceeding with limited BS parsing for {url} due to no browser.")
            # (Call BS-based versions of sub-extraction logic - not fully implemented here for brevity)
            return [] # Or attempt BS parsing

        try:
            temp_page_for_parsing = self.browser.new_page()
            temp_page_for_parsing.set_content(page_content_html, wait_until="domcontentloaded")
            self.logger.debug(f"Set HTML content on temporary page for Playwright-based parsing of {url}")

            page_year_context: Optional[int] = None
            title_text_content = temp_page_for_parsing.title()
            
            # Try to find year from H1, H2, or specific title-like elements first
            year_context_elements_loc = temp_page_for_parsing.locator('h1, h2, .page-title, .listing-title').all()
            year_search_texts_list = [loc.text_content() for loc_idx, loc in enumerate(year_context_elements_loc) if loc.is_visible() and loc_idx < 5] # Limit checks
            year_search_texts_list.append(title_text_content)

            for text_item_content in year_search_texts_list: # Renamed var
                if text_item_content:
                    year_match_obj_re = re.search(r'\b(202\d|203\d)\b', text_item_content) # Renamed var
                    if year_match_obj_re: page_year_context = int(year_match_obj_re.group(1)); break
            
            if page_year_context: self.logger.info(f"Determined page year context: {page_year_context} for {url}")
            else: self.logger.warning(f"Could not determine year context for {url}")

            # Page-level general content extraction (using the refactored method that now takes Page or HTML)
            # Passing the temporary Playwright page here.
            page_level_main_content_dict = self.extract_main_event_content(temp_page_for_parsing, source_url=url)
            page_full_description = page_level_main_content_dict.get("full_description", "")
            
            page_genres_list = self.extract_contextual_genres(page_full_description, title_text_content)
            page_social_media_dict = self.extract_social_media_links(page_content_html) # Uses full HTML string

            # 1. JSON-LD Parsing (using Playwright on temp_page_for_parsing)
            json_ld_scripts_texts = temp_page_for_parsing.locator(self.config.EVENT_SELECTORS_SPOTLIGHT['json_ld_script']).all_inner_texts()
            for script_text_content in json_ld_scripts_texts: # Renamed var
                try:
                    data_obj_json = json.loads(script_text_content) # Renamed var
                    items_to_parse = data_obj_json if isinstance(data_obj_json, list) else [data_obj_json]
                    for item_json_data in items_to_parse: # Renamed var
                        if isinstance(item_json_data, dict):
                            event_types_json = item_json_data.get("@type", []) # Renamed var
                            if not isinstance(event_types_json, list): event_types_json = [event_types_json]
                            if "Event" in event_types_json or "MusicEvent" in event_types_json:
                                ld_event_parsed_data = self.parse_json_ld_event(item_json_data, url, page_year_context) # Renamed var
                                if ld_event_parsed_data and not self.is_duplicate(ld_event_parsed_data, final_page_events):
                                    final_page_events.append(ld_event_parsed_data)
                except json.JSONDecodeError as json_err:
                    self.logger.debug(f"JSON-LD script content decode error on {url}: {json_err}") # Debug for common empty/bad scripts
                except Exception as e_jsonld_proc:
                    self.logger.error(f"JSON-LD processing error on {url}: {e_jsonld_proc}", exc_info=True)
            
            self.logger.info(f"Found {len(final_page_events)} events via JSON-LD on {url}.")

            # 2. Playwright-based Event Card Extraction
            primary_card_selector = self.config.EVENT_SELECTORS_SPOTLIGHT['primary_event_card']
            card_element_handles = temp_page_for_parsing.locator(primary_card_selector).all_element_handles()
            
            if not card_element_handles and self.config.EVENT_SELECTORS_SPOTLIGHT.get('fallback_event_cards'):
                for fallback_selector in self.config.EVENT_SELECTORS_SPOTLIGHT['fallback_event_cards']:
                    self.logger.debug(f"Trying fallback card selector '{fallback_selector}' for {url}")
                    card_element_handles = temp_page_for_parsing.locator(fallback_selector).all_element_handles()
                    if card_element_handles:
                        self.logger.info(f"Found {len(card_element_handles)} cards with fallback selector '{fallback_selector}' for {url}")
                        break
            
            if card_element_handles:
                self.logger.info(f"Processing {len(card_element_handles)} event cards found with Playwright for {url}.")
                for card_handle in card_element_handles:
                    event_from_card_data = self.scrape_partycal_ticket_card(card_handle, url, page_year_context)
                    if event_from_card_data and not self.is_duplicate(event_from_card_data, final_page_events):
                        final_page_events.append(event_from_card_data)
            else:
                self.logger.info(f"No event cards found with Playwright locators on {url}.")

            # Consolidate and add page-level info to all events found so far
            processed_events_list = [] # Renamed var
            for event_data_item in final_page_events:
                event_data_item.setdefault("full_description", page_full_description)
                event_data_item.setdefault("markdown_content", self.html_to_text_converter.handle(event_data_item.get("full_description","")))
                event_data_item.setdefault("genres", page_genres_list)
                event_data_item.setdefault("promoter", current_promoter_slug)
                event_data_item.setdefault("venue", self.current_venue_context)
                event_data_item.setdefault("tickets_url", url)
                event_data_item.setdefault("page_social_media", page_social_media_dict)

                # If artists still missing, try to extract from main page description (less reliable)
                if not event_data_item.get("artists") and page_full_description:
                     # extract_performers_from_content expects ElementHandle or HTML string.
                     # Here, we only have text. This part of logic might need to be simpler regex on text.
                     # For now, passing None as element handle, relying on its text fallback.
                    event_data_item["artists"] = self.extract_performers_from_content(None, page_full_description, source_url=url)
                
                event_after_fallback_data = self.trigger_fallback_methods(page_content_html, url, event_data_item)
                
                quality_info_dict = self.scorer.calculate_event_quality(event_after_fallback_data) # Renamed var
                event_after_fallback_data["data_quality"] = {
                    "overall_score": quality_info_dict.get("score",0),
                    "completeness_score": self.calculate_completeness_score(event_after_fallback_data),
                    "accuracy_score": self.calculate_accuracy_score(event_after_fallback_data),
                    "freshness_score": self.calculate_freshness_score(event_after_fallback_data),
                    "issues": quality_info_dict.get("issues", [])
                }
                processed_events_list.append(event_after_fallback_data)
            
            final_page_events = processed_events_list # Update with processed events

            # Pagination using Playwright on the temp_page_for_parsing
            pagination_pw_selectors = [
                'a[rel="next"]', 'a.next', 'li.next > a',
                'a:has-text("Next")', 'a:has-text("Siguiente")',
                '.pagination a[href*="page"]', 'a[href*="offset"]', '.pager .next a'
            ]
            next_page_q_found_flag = False # Renamed var
            for pag_selector_str in pagination_pw_selectors: # Renamed var
                try:
                    next_link_locators = temp_page_for_parsing.locator(pag_selector_str).all() # Get all matching
                    for next_link_loc_item in next_link_locators: # Renamed var
                        if next_link_loc_item.is_visible(timeout=500):
                            href_attribute = next_link_loc_item.get_attribute('href') # Renamed var
                            if href_attribute:
                                next_page_url_val = urljoin(url, href_attribute) # Renamed var
                                if next_page_url_val not in self.visited_urls and self.current_depth < self.max_depth:
                                    self.url_queue.append((next_page_url_val, self.current_depth + 1))
                                    self.logger.info(f"Queued next page (PW parsed): {next_page_url_val}")
                                    next_page_q_found_flag = True; break
                    if next_page_q_found_flag: break
                except PlaywrightTimeoutError: self.logger.debug(f"PW Pagination selector '{pag_selector_str}' not visible on {url}")
                except Exception as e_pag_pw_exc: self.logger.warning(f"PW Pagination selector '{pag_selector_str}' error on {url}: {e_pag_pw_exc}")

            return final_page_events
        except Exception as e_main_scrape_promoter:
            self.logger.error(f"Major error in scrape_promoter_page for {url}: {e_main_scrape_promoter}", exc_info=True)
            self.stats["errors"] +=1
            return []
        finally:
            if temp_page_for_parsing:
                try: temp_page_for_parsing.close()
                except Exception as e_close_temp: self.logger.warning(f"Error closing temp parsing page for {url}: {e_close_temp}")

    def extract_venue_club_urls(self, url: str) -> List[str]:
        """Extracts venue/club/promoter URLs from a page using Playwright."""
        self.logger.info(f"Extracting venue/club/promoter URLs from: {url}")
        page_content_html = self.fetch_page_content(url, robust_scroll=False) # No need for deep scroll for link finding usually
        if not page_content_html:
            self.logger.warning(f"No content from {url} for venue/club URL extraction.")
            return []
        
        venue_club_links = set()
        temp_page: Optional[Page] = None
        if not self.browser:
            self.logger.error("Browser not available for Playwright parsing in extract_venue_club_urls.")
            return [] # Cannot proceed without browser

        try:
            temp_page = self.browser.new_page()
            temp_page.set_content(page_content_html, wait_until="domcontentloaded")

            # More specific and combined selectors
            # Prioritize selectors that are less likely to pick up unwanted event detail links
            link_selectors = [
                'a[href*="/night/venue/"]', 'a[href*="/venue/"]', 'a[href*="/club/"]', # Venue/Club specific
                'a[href*="/night/promoters/"]', 'a[href*="/promoter/"]',             # Promoter specific
                '.venue-link a', '.club-item a', '.promoter-listing a',            # Links within specific container classes
                'nav[aria-label*="footer"] a[href*="/night/"]', # Footer navigation links under /night/
                'a[href*="/events"]' # General events, check path depth later
            ]

            found_urls: Set[str] = set() # To store hrefs from locators

            for selector in link_selectors:
                link_elements = temp_page.locator(selector).all_element_handles()
                for link_el_handle in link_elements:
                    href = link_el_handle.get_attribute('href')
                    if href:
                        full_url = urljoin(url, href.strip())
                        # Basic validation and filtering
                        parsed_url = urlparse(full_url)
                        if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc == urlparse(self.config.url).netloc: # Stay on same domain
                            # Filter out very generic links or specific unwanted patterns
                            path = parsed_url.path
                            if path and path.count('/') >= 2 and \
                               not (path.endswith("/events/") and path.count('/') <= 3) and \
                               not (path.endswith("/night/") and path.count('/') <= 2) and \
                               not (path.endswith("/magazine")) and \
                               not (path.endswith("/photos")):
                                found_urls.add(full_url)

            venue_club_links.update(found_urls) # Add all unique URLs found
            self.logger.info(f"Extracted {len(venue_club_links)} potential venue/club/promoter URLs from {url} using Playwright.")

        except Exception as e:
            self.logger.error(f"Error extracting venue/club URLs from {url} with Playwright: {e}", exc_info=True)
        finally:
            if temp_page:
                try: temp_page.close()
                except Exception as e_close: self.logger.warning(f"Error closing temp page for venue/club URL extraction from {url}: {e_close}")
        
        return list(venue_club_links)

    def extract_promoter_urls(self, url: str) -> List[str]:
        """Extracts promoter URLs from a venue page using Playwright."""
        self.logger.info(f"Extracting promoter URLs from venue page: {url}")
        page_content_html = self.fetch_page_content(url, robust_scroll=False)
        if not page_content_html:
            self.logger.warning(f"No content from {url} for promoter URL extraction.")
            return []

        promoter_links = set()
        temp_page: Optional[Page] = None
        if not self.browser:
            self.logger.error("Browser not available for Playwright parsing in extract_promoter_urls.")
            return []

        try:
            temp_page = self.browser.new_page()
            temp_page.set_content(page_content_html, wait_until="domcontentloaded")
            
            # Selectors for promoter links
            promoter_selectors = [
                'a[href*="/night/promoters/"]',
                'a[href*="/promoter/"]',
                '.promoter-link a', '.event-promoter a' # More specific if available
            ]
            found_promoter_urls: Set[str] = set()

            for selector in promoter_selectors:
                link_elements = temp_page.locator(selector).all_element_handles()
                for link_el_handle in link_elements:
                    href = link_el_handle.get_attribute('href')
                    if href:
                        full_url = urljoin(url, href.strip())
                        parsed_url = urlparse(full_url)
                        # Ensure it's a specific promoter, not just the category page, and on the same domain
                        if parsed_url.scheme in ['http', 'https'] and \
                           parsed_url.netloc == urlparse(self.config.url).netloc and \
                           urlparse(full_url).path.count('/') >= 3: # e.g., /night/promoters/some-promoter
                            found_promoter_urls.add(full_url)
            
            promoter_links.update(found_promoter_urls)
            self.logger.info(f"Extracted {len(promoter_links)} promoter URLs from {url} using Playwright.")

        except Exception as e:
            self.logger.error(f"Error extracting promoter URLs from {url} with Playwright: {e}", exc_info=True)
        finally:
            if temp_page:
                try: temp_page.close()
                except Exception as e_close: self.logger.warning(f"Error closing temp page for promoter URL extraction from {url}: {e_close}")

        return list(promoter_links)

    def save_event_pw(self, raw_event_data: Dict[str, Any], current_page_url: str):
       """
       Maps raw event data to unified schema, saves to DB (if enabled),
       and adds the unified document to self.all_scraped_events_for_run for batch file output.
       """
       try:
           if not raw_event_data or not raw_event_data.get("title"):
               self.logger.warning(f"Skipping save for event with no title or empty data from {current_page_url}.")
               return

           event_specific_url = raw_event_data.get('tickets_url', raw_event_data.get('json_ld_url', current_page_url))
           unified_event_doc = map_to_unified_schema(
               raw_data=raw_event_data, source_platform="ibiza-spotlight-pw", source_url=event_specific_url
           )
           if not unified_event_doc:
               self.logger.warning(f"Mapping to unified schema returned None for event: {raw_event_data.get('title')} from {current_page_url}")
               return

           self.all_scraped_events_for_run.append(unified_event_doc)

           if self.db and self.config.save_to_db:
               if unified_event_doc.get("event_id"):
                   update_key = {"event_id": unified_event_doc["event_id"]}
                   self.db.events.update_one(update_key, {"$set": unified_event_doc}, upsert=True)
                   self.logger.debug(f"Saved/Updated event to DB: {unified_event_doc.get('title', 'N/A')[:50]}... (ID: {unified_event_doc['event_id']})")
               else:
                    self.logger.warning(f"Cannot save event to DB due to missing event_id: {unified_event_doc.get('title', 'N/A')}")
           elif not self.db and self.config.save_to_db:
               self.logger.warning(f"DB save enabled in config, but DB connection is not available for event: {unified_event_doc.get('title', 'N/A')}")
           else:
                self.logger.debug(f"DB saving not enabled. Unified event '{unified_event_doc.get('title', 'N/A')}' added to list for file output.")
       except Exception as e_save:
           self.logger.error(f"Error in save_event_pw for '{raw_event_data.get('title', 'N/A')}' from {current_page_url}: {e_save}", exc_info=True)
           self.stats["errors"] += 1
    
    def run(self):
        """Main scraper execution logic using Playwright."""
        self.logger.info(f"Starting Playwright-enhanced scraper run. Timestamp: {self.run_timestamp}")
        self.all_scraped_events_for_run = []
        self.stats = {"venues_scraped": 0, "promoters_scraped": 0, "events_scraped": 0, "pages_processed": 0, "errors": 0}
        
        Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.config.log_dir).mkdir(parents=True, exist_ok=True)

        self.url_queue = [(self.config.url, 0)]
        self.logger.info(f"Initial URL: {self.config.url}")

        # tqdm progress bar can be re-added here if desired.
        # if self.progress_bar: self.progress_bar.close()
        # self.progress_bar = tqdm(desc="Scraping Progress", unit="pages", postfix=self.stats, leave=True)

        with self: # Manages Playwright browser lifecycle via __enter__ and __exit__
            while self.url_queue:
                current_url_for_loop, depth = self.url_queue.pop(0)
                try:
                    if current_url_for_loop in self.visited_urls or depth > self.max_depth:
                        self.logger.debug(f"Skipping URL: {current_url_for_loop} (visited or max_depth)")
                        continue
                    
                    self.visited_urls.add(current_url_for_loop)
                    self.current_depth = depth
                    self.stats["pages_processed"] += 1
                    self.logger.info(f"Processing [{depth}]: {current_url_for_loop}")

                    self._update_venue_context_from_url(current_url_for_loop)

                    is_main_page = (depth == 0 and ("/night/events" in current_url_for_loop or current_url_for_loop.endswith("/night")))
                    is_venue = "/venue/" in current_url_for_loop or "/clubs/" in current_url_for_loop
                    is_promoter = "/promoters/" in current_url_for_loop or "/promoter/" in current_url_for_loop
                    
                    page_events_data = []

                    if is_main_page:
                        self.logger.info(f"Main page: extracting venue/club/promoter URLs from {current_url_for_loop}")
                        discovered_nav_urls = self.extract_venue_club_urls(current_url_for_loop)
                        self.stats["venues_scraped"] += len(discovered_nav_urls)
                        for nav_url in discovered_nav_urls:
                            if nav_url not in self.visited_urls and (depth + 1) <= self.max_depth:
                                self.url_queue.append((nav_url, depth + 1))
                    elif is_venue:
                        self.logger.info(f"Venue page: extracting promoter URLs from {current_url_for_loop}")
                        promoter_page_urls = self.extract_promoter_urls(current_url_for_loop)
                        for p_page_url in promoter_page_urls:
                            if p_page_url not in self.visited_urls and (depth + 1) <= self.max_depth:
                                self.url_queue.append((p_page_url, depth + 1))
                    elif is_promoter:
                        self.logger.info(f"Promoter page: scraping events from {current_url_for_loop}")
                        self.stats["promoters_scraped"] += 1
                        page_events_data = self.scrape_promoter_page(current_url_for_loop)
                    else:
                        self.logger.info(f"General page: attempting event scrape from {current_url_for_loop}")
                        page_events_data = self.scrape_promoter_page(current_url_for_loop)
                    
                    if page_events_data:
                        self.logger.info(f"Found {len(page_events_data)} raw events on {current_url_for_loop}")
                        for raw_event_item in page_events_data:
                            raw_event_item.setdefault('page_url_event_was_scraped_from', current_url_for_loop)
                            self.save_event_pw(raw_event_item, current_url_for_loop)

                    # if self.progress_bar: self.progress_bar.set_postfix(self.stats); self.progress_bar.update(1)
                    self._quick_delay()

                except Exception as e_loop_exc:
                    self.logger.error(f"Error processing URL {current_url_for_loop}: {e_loop_exc}", exc_info=True)
                    self.stats["errors"] += 1
                    continue
            
            self.stats["events_scraped"] = len(self.all_scraped_events_for_run)

            if self.all_scraped_events_for_run:
                self.logger.info(f"Consolidating {len(self.all_scraped_events_for_run)} scraped and unified events for file output.")
                output_prefix = "ibiza_spotlight_final_pw_events"
                save_to_json_file(self.all_scraped_events_for_run, output_prefix, self.config.output_dir, self.logger)
                save_to_csv_file(self.all_scraped_events_for_run, output_prefix, self.config.output_dir, self.logger)
                save_to_markdown_file(self.all_scraped_events_for_run, output_prefix, self.config.output_dir, self.logger)
                self.logger.info(f"Unified data saved to files in {self.config.output_dir}.")
            else:
                self.logger.info("No events were collected in all_scraped_events_for_run to save to files.")

        # if self.progress_bar: self.progress_bar.close()

        self.logger.info("=" * 80)
        self.logger.info("PLAYWRIGHT-ENHANCED SCRAPING COMPLETE - SUMMARY")
        self.logger.info(f"Total pages processed: {self.stats['pages_processed']}")
        self.logger.info(f"Total unique URLs visited: {len(self.visited_urls)}")
        self.logger.info(f"Venues/Clubs links discovered (approx): {self.stats['venues_scraped']}")
        self.logger.info(f"Promoter pages processed: {self.stats['promoters_scraped']}")
        self.logger.info(f"Total unified events collected for output: {len(self.all_scraped_events_for_run)}")
        self.logger.info(f"Total errors encountered: {self.stats['errors']}")
        self.logger.info("=" * 80)
        return self.all_scraped_events_for_run

if __name__ == "__main__":
    config = ScraperConfig(url="https://www.ibiza-spotlight.com/night/events")
    Path(config.output_dir).mkdir(parents=True, exist_ok=True)
    Path(config.log_dir).mkdir(parents=True, exist_ok=True)
    
    main_exec_logger = setup_logger("MainSpotlightPWExec", "spotlight_main_pw_exec", log_dir=config.log_dir)
    main_exec_logger.info(f"Initiating Ibiza Spotlight Scraper with Playwright. Config: {config}")

    try:
        with IbizaSpotlightScraper(config=config) as scraper_instance:
            scraper_instance.run()
            main_exec_logger.info("Scraping process has concluded.")
    except KeyboardInterrupt:
        main_exec_logger.warning("Scraper run interrupted by user.")
    except Exception as e_main_exc:
        main_exec_logger.critical(f"Scraper failed critically during main execution: {e_main_exc}", exc_info=True)
    finally:
        main_exec_logger.info("Ibiza Spotlight Scraper (Playwright) shutdown complete.")
        logging.shutdown()
