#!/usr/bin/env python3
import sys
import pathlib

# Add project root to sys.path (if your structure needs it)
# PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
# sys.path.insert(0, str(PROJECT_ROOT))

"""
Enhanced Ibiza Spotlight Scraper with MongoDB Integration,
Targeted Element Extraction, Improved Logging/Export, and Revised Schema.

Version 0506 - Enhanced with:
- Updated main URL to June 2025 events
- Improved scroll-to-bottom logic with content detection
- Enhanced venue discovery and URL queue handling
- Dynamic venue context extraction
- Comprehensive error handling for new flow
- Updated pagination logic with better CSS selectors
- Advanced main body text extraction with Markdown conversion
- Comprehensive ticket pricing structure extraction
- Unified date/time formatting with ISO 8601 compliance
- Advanced performer & genre extraction with social media
- Social media link extraction and validation
- Comprehensive data quality system with fallback methods
- Enhanced error handling with progress bar and ETA
"""

import os
import json
import time
import random
import re
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
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

# Import enhanced dependencies
try:
    import html2text
    from tqdm import tqdm
    import pytz
except ImportError as e:
    print(f"Missing dependency: {e}. Please install with: pip install html2text tqdm pytz")
    sys.exit(1)

# Import or define enhanced QualityScorer
try:
    from classy_skkkrapey.database.quality_scorer import QualityScorer
except ImportError:
    # Enhanced placeholder with comprehensive scoring
    class QualityScorer:
        def calculate_event_quality(self, event: Dict[str, Any]) -> Dict[str, Any]:
            """Calculate comprehensive quality scores for event data"""
            scores = {
                "title": self._score_field(event.get("title"), min_length=5),
                "datetime": self._score_datetime(event.get("datetime_obj")),
                "ticketing": self._score_ticketing(event),
                "content": self._score_content(event.get("full_description", "")),
                "artists": self._score_artists(event.get("artists", [])),
                "venue": self._score_field(event.get("venue"), min_length=2),
                "social_media": self._score_social_media(event.get("social_media_links", {}))
            }
            
            # Calculate weighted overall score
            weights = {
                "title": 0.20,
                "datetime": 0.25,
                "ticketing": 0.15,
                "content": 0.10,
                "artists": 0.15,
                "venue": 0.10,
                "social_media": 0.05
            }
            
            overall_score = sum(scores[field] * weights[field] for field in weights)
            
            completeness = {
                f"{field}_present": scores[field] > 0 for field in scores
            }
            
            return {
                "score": overall_score * 100,  # Convert to 0-100 scale
                "field_scores": scores,
                "completeness_metrics": completeness,
                "version": "0.3.0",
                "calculated_at": datetime.utcnow().isoformat() + "Z"
            }
        
        def _score_field(self, value: Any, min_length: int = 1) -> float:
            if not value:
                return 0.0
            if isinstance(value, str) and len(value) >= min_length:
                return 1.0
            if isinstance(value, list) and len(value) > 0:
                return 1.0
            return 0.5
        
        def _score_datetime(self, dt_obj: Optional[datetime]) -> float:
            if not dt_obj:
                return 0.0
            if dt_obj.tzinfo:  # Has timezone info
                return 1.0
            return 0.8
        
        def _score_ticketing(self, event: Dict) -> float:
            score = 0.0
            if event.get("tier_1"):
                score += 0.4
            if event.get("tier_2"):
                score += 0.3
            if event.get("tier_3"):
                score += 0.3
            return score
        
        def _score_content(self, content: str) -> float:
            if not content:
                return 0.0
            word_count = len(content.split())
            if word_count >= 100:
                return 1.0
            elif word_count >= 50:
                return 0.7
            elif word_count >= 20:
                return 0.5
            return 0.3
        
        def _score_artists(self, artists: List) -> float:
            if not artists:
                return 0.0
            if len(artists) >= 3:
                return 1.0
            elif len(artists) >= 1:
                return 0.7
            return 0.0
        
        def _score_social_media(self, social_links: Dict) -> float:
            if not social_links:
                return 0.0
            link_count = len([v for v in social_links.values() if v])
            if link_count >= 3:
                return 1.0
            elif link_count >= 1:
                return 0.6
            return 0.0

# --- Constants ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

PREDEFINED_GENRES = {
    "techno": ["techno", "tech house", "minimal techno", "dark techno", "melodic techno", "hard techno"],
    "house": ["house music", "deep house", "funky house", "acid house", "progressive house", "afro house", "tech house", "vocal house"],
    "trance": ["trance", "psytrance", "progressive trance", "uplifting trance", "goa trance", "vocal trance"],
    "edm": ["edm", "electronic dance music", "big room", "future house", "electro house"],
    "drum & bass": ["d&b", "drum and bass", "jungle", "neurofunk", "liquid funk", "jump up"],
    "reggaeton": ["reggaeton", "latin urban", "perreo", "dembow"],
    "hip hop": ["hip hop", "rap", "trap", "urban", "boom bap", "conscious rap"],
    "live music": ["live band", "live set", "live music", "acoustic", "concert", "live pa", "live performance"],
    "disco": ["disco", "nu-disco", "disco house", "cosmic disco"],
    "funk": ["funk", "g-funk", "future funk", "funky breaks"],
    "soul": ["soul", "neo-soul", "northern soul"],
    "r&b": ["r&b", "rnb", "rhythm and blues", "contemporary r&b"],
    "electronic": ["electronic", "electronica", "experimental electronic", "idm", "glitch"],
    "ambient": ["ambient", "chillout", "downtempo", "lounge"],
    "garage": ["uk garage", "garage", "2-step", "speed garage"],
    "bass music": ["bass music", "dubstep", "future bass", "trap", "bass house"],
    "underground": ["underground scene", "underground music", "underground party"],
    "commercial": ["commercial", "mainstream", "chart music", "pop dance"],
    "hardcore": ["hardcore", "gabber", "happy hardcore", "frenchcore"],
    "latin": ["latin", "salsa", "bachata", "merengue", "cumbia"]
}

# Social media patterns
SOCIAL_MEDIA_PATTERNS = {
    "instagram": [
        r'(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9_.]+)',
        r'(?:https?://)?(?:www\.)?instagr\.am/([A-Za-z0-9_.]+)',
        r'@([A-Za-z0-9_.]+)(?:\s|$)'  # Instagram handle
    ],
    "facebook": [
        r'(?:https?://)?(?:www\.)?facebook\.com/([A-Za-z0-9.]+)',
        r'(?:https?://)?(?:www\.)?fb\.com/([A-Za-z0-9.]+)'
    ],
    "twitter": [
        r'(?:https?://)?(?:www\.)?twitter\.com/([A-Za-z0-9_]+)',
        r'(?:https?://)?(?:www\.)?x\.com/([A-Za-z0-9_]+)'
    ],
    "tiktok": [
        r'(?:https?://)?(?:www\.)?tiktok\.com/@([A-Za-z0-9_.]+)'
    ],
    "youtube": [
        r'(?:https?://)?(?:www\.)?youtube\.com/(?:c/|channel/|user/|@)([A-Za-z0-9_-]+)'
    ],
    "soundcloud": [
        r'(?:https?://)?(?:www\.)?soundcloud\.com/([A-Za-z0-9_-]+)'
    ]
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

# --- Enhanced Scraper Class ---
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
        
        # Progress tracking
        self.progress_bar: Optional[tqdm] = None
        self.stats = {
            "venues_scraped": 0,
            "promoters_scraped": 0,
            "events_scraped": 0,
            "start_time": datetime.now()
        }
        
        # HTML to Markdown converter
        self.html2text_converter = html2text.HTML2Text()
        self.html2text_converter.ignore_links = False
        self.html2text_converter.ignore_images = True
        self.html2text_converter.body_width = 0  # Don't wrap lines
        
        self._initialize_venue_context()

    def _initialize_venue_context(self):
        """Extracts venue from the initial config URL if it's a venue page."""
        parsed_url = urlparse(self.config.url)
        path_parts = parsed_url.path.strip('/').split('/')
        # Example: /night/venue/unvrs or /night/events/2025/06
        if len(path_parts) >= 3 and path_parts[-2] == 'venue':
            self.current_venue_context = path_parts[-1]
            logger.info(f"Initialized with venue context: {self.current_venue_context}")
        elif len(path_parts) >= 4 and path_parts[1] == 'night' and path_parts[2] == 'events':
            # For events pages like /night/events/2025/06, extract year/month context
            year_month = "/".join(path_parts[3:5]) if len(path_parts) >= 5 else path_parts[3]
            self.current_venue_context = f"events_{year_month}"
            logger.info(f"Initialized with events context: {self.current_venue_context}")

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
    
    def extract_main_event_content(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Enhanced main body text extraction with aggressive cleaning and markdown conversion.
        Returns structured content with sections and cleaned text.
        """
        content_data = {
            "full_description": "",
            "markdown_content": "",
            "sections": {},
            "key_info": []
        }
        
        # Target primary content containers
        content_selectors = [
            "article.article._wysiwyg",
            "div.event-content",
            "main.event-details",
            "div.content-main",
            ".event-description",
            "[itemprop='description']"
        ]
        
        main_content = None
        for selector in content_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                break
        
        if not main_content:
            # Fallback to body content
            main_content = soup.find('body')
        
        if main_content:
            # Clone to avoid modifying original
            content_copy = main_content.__copy__()
            
            # Aggressive content cleaning
            remove_selectors = [
                'script', 'style', 'nav', '.navigation', '.social-share',
                '.ticket-widget', '.advertisement', '.ad', '.banner',
                'form', '.newsletter', '.cookie-notice', '.popup',
                '.related-events', '.sidebar', 'aside', '.comments',
                '.share-buttons', '[class*="social"]', '[class*="share"]',
                '.breadcrumb', '.pagination', 'footer', 'header'
            ]
            
            for selector in remove_selectors:
                for element in content_copy.select(selector):
                    element.decompose()
            
            # Convert to markdown
            html_str = str(content_copy)
            markdown_content = self.html2text_converter.handle(html_str)
            
            # Clean up markdown
            markdown_content = re.sub(r'\n{3,}', '\n\n', markdown_content)
            markdown_content = markdown_content.strip()
            
            content_data["markdown_content"] = markdown_content
            content_data["full_description"] = content_copy.get_text(separator='\n', strip=True)
            
            # Extract key sections
            section_patterns = {
                "about": r"(?:About|Description|Overview)[\s\S]*?(?=\n#{1,3}\s|\Z)",
                "lineup": r"(?:Line\s*up|Artists?|Performers?)[\s\S]*?(?=\n#{1,3}\s|\Z)",
                "venue_info": r"(?:Venue|Location|Where)[\s\S]*?(?=\n#{1,3}\s|\Z)",
                "tickets": r"(?:Tickets?|Pricing|Entry)[\s\S]*?(?=\n#{1,3}\s|\Z)",
                "rules": r"(?:Rules?|Policy|Terms)[\s\S]*?(?=\n#{1,3}\s|\Z)"
            }
            
            for section_name, pattern in section_patterns.items():
                match = re.search(pattern, markdown_content, re.IGNORECASE)
                if match:
                    content_data["sections"][section_name] = match.group(0).strip()
            
            # Extract key information bullets
            key_info_patterns = [
                r'(?:•|\*|-)\s*([^\n]+)',  # Bullet points
                r'(?:(?:Time|Date|Price|Age|Dress\s*code):\s*[^\n]+)',  # Key-value pairs
            ]
            
            for pattern in key_info_patterns:
                matches = re.findall(pattern, content_data["full_description"], re.IGNORECASE)
                content_data["key_info"].extend(matches)
        
        return content_data
    
    def extract_comprehensive_ticket_info(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Extract comprehensive ticket pricing structure with multiple tiers.
        Returns structured ticket objects matching the ticketing.ticket_tiers schema.
        """
        ticket_tiers = []
        
        # Multiple selectors for different ticket structures
        ticket_selectors = [
            "div.ticket-item",
            "tr.price-row",
            "div.pricing-tier",
            ".ticket-option",
            "[class*='ticket-type']",
            "div.tier",
            ".price-item"
        ]
        
        for selector in ticket_selectors:
            ticket_elements = soup.select(selector)
            if ticket_elements:
                for elem in ticket_elements:
                    tier_info = self._extract_ticket_tier(elem)
                    if tier_info and tier_info not in ticket_tiers:
                        ticket_tiers.append(tier_info)
        
        # If no structured tickets found, try to extract from text
        if not ticket_tiers:
            ticket_tiers = self._extract_tickets_from_text(soup.get_text())
        
        return ticket_tiers
    
    def _extract_ticket_tier(self, element: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract individual ticket tier information"""
        tier = {
            "name": "",
            "price": None,
            "currency": "EUR",  # Default to EUR for Ibiza
            "description": "",
            "availability": "available",
            "conditions": []
        }
        
        # Extract name
        name_selectors = ["div.ticket-name", "td.tier-name", "h4.price-title", ".ticket-title", "[class*='name']"]
        for selector in name_selectors:
            name_elem = element.select_one(selector)
            if name_elem:
                tier["name"] = name_elem.get_text(strip=True)
                break
        
        # Extract price
        price_selectors = ["div.ticket-price", "span.price", "td.price-amount", ".amount", "[class*='price']"]
        for selector in price_selectors:
            price_elem = element.select_one(selector)
            if price_elem:
                price_text = price_elem.get_text(strip=True)
                price_info = self._parse_price(price_text)
                if price_info:
                    tier["price"] = price_info["amount"]
                    tier["currency"] = price_info["currency"]
                break
        
        # Extract description
        desc_selectors = ["div.ticket-description", "td.tier-details", ".description", "p"]
        for selector in desc_selectors:
            desc_elem = element.select_one(selector)
            if desc_elem:
                tier["description"] = desc_elem.get_text(strip=True)
                break
        
        # Check availability
        if re.search(r'sold\s*out|unavailable|expired', element.get_text(), re.IGNORECASE):
            tier["availability"] = "sold_out"
        elif re.search(r'limited|few\s*left|hurry', element.get_text(), re.IGNORECASE):
            tier["availability"] = "limited"
        
        # Extract conditions
        conditions_text = element.get_text()
        if "21+" in conditions_text or "18+" in conditions_text:
            tier["conditions"].append("Age restriction")
        if re.search(r'dress\s*code', conditions_text, re.IGNORECASE):
            tier["conditions"].append("Dress code applies")
        
        # Only return if we have at least a name or price
        if tier["name"] or tier["price"]:
            return tier
        return None
    
    def _parse_price(self, price_text: str) -> Optional[Dict[str, Any]]:
        """Parse price string and extract amount and currency"""
        # Currency patterns
        currency_patterns = {
            "EUR": [r'€', r'EUR', r'euros?'],
            "GBP": [r'£', r'GBP', r'pounds?'],
            "USD": [r'\$', r'USD', r'dollars?']
        }
        
        # Clean the price text
        price_text = price_text.replace(',', '.')
        
        # Try to extract number
        number_match = re.search(r'(\d+(?:\.\d{1,2})?)', price_text)
        if not number_match:
            return None
        
        amount = float(number_match.group(1))
        
        # Detect currency
        currency = "EUR"  # Default
        for curr, patterns in currency_patterns.items():
            for pattern in patterns:
                if re.search(pattern, price_text, re.IGNORECASE):
                    currency = curr
                    break
        
        return {"amount": amount, "currency": currency}
    
    def _extract_tickets_from_text(self, text: str) -> List[Dict[str, Any]]:
        """Fallback method to extract ticket info from unstructured text"""
        tickets = []
        
        # Common ticket patterns
        ticket_patterns = [
            r'(early\s*bird|advance|door|vip|general\s*admission)[:\s]*(?:€|£|\$)?(\d+(?:\.\d{2})?)',
            r'(?:€|£|\$)(\d+(?:\.\d{2})?)\s*(?:for\s*)?(early\s*bird|advance|door|vip|general)',
            r'tickets?[:\s]*(?:from\s*)?(?:€|£|\$)?(\d+(?:\.\d{2})?)'
        ]
        
        for pattern in ticket_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                if len(match.groups()) >= 2:
                    name = match.group(1) if not match.group(1).isdigit() else match.group(2)
                    price = match.group(2) if match.group(2).isdigit() else match.group(1)
                else:
                    name = "General Admission"
                    price = match.group(1)
                
                try:
                    tickets.append({
                        "name": name.title(),
                        "price": float(price),
                        "currency": "EUR",
                        "description": "",
                        "availability": "available",
                        "conditions": []
                    })
                except ValueError:
                    continue
        
        return tickets
    
    def standardize_datetime(self, raw_date: str, timezone_str: Optional[str] = None, 
                           context_url: str = "") -> Optional[Dict[str, Any]]:
        """
        Unified date/time formatting that returns standardized ISO 8601 datetime.
        Returns a dict with multiple datetime fields.
        """
        if not raw_date:
            return None
        
        # Default timezone for Ibiza (CEST in summer)
        default_tz = pytz.timezone('Europe/Madrid')
        
        # Clean the date string
        raw_date = raw_date.strip()
        raw_date = re.sub(r'(from|desde|a\s*partir\s*de)\s*', '', raw_date, flags=re.IGNORECASE)
        
        try:
            # Parse the date
            parsed_dt = date_parser.parse(raw_date, fuzzy=True)
            
            # Apply timezone
            if parsed_dt.tzinfo is None:
                if timezone_str:
                    try:
                        tz = pytz.timezone(timezone_str)
                        parsed_dt = tz.localize(parsed_dt)
                    except:
                        parsed_dt = default_tz.localize(parsed_dt)
                else:
                    parsed_dt = default_tz.localize(parsed_dt)
            
            # Extract time components for doors_open, event_start, last_entry
            time_patterns = {
                "doors_open": [r'doors?\s*(?:open)?\s*(?:at\s*)?(\d{1,2}[:\.]?\d{0,2}\s*(?:am|pm)?)', 
                              r'apertura\s*(?:puertas)?\s*(\d{1,2}[:\.]?\d{0,2})'],
                "last_entry": [r'last\s*entry\s*(?:at\s*)?(\d{1,2}[:\.]?\d{0,2}\s*(?:am|pm)?)',
                             r'última\s*entrada\s*(\d{1,2}[:\.]?\d{0,2})']
            }
            
            datetime_info = {
                "start_datetime": parsed_dt,
                "end_datetime": None,
                "doors_open": None,
                "last_entry": None,
                "timezone": str(parsed_dt.tzinfo) if parsed_dt.tzinfo else "Europe/Madrid",
                "raw_date_string": raw_date
            }
            
            # Try to extract additional times
            full_text = raw_date
            for time_type, patterns in time_patterns.items():
                for pattern in patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        time_str = match.group(1)
                        try:
                            time_parsed = date_parser.parse(time_str, fuzzy=True)
                            # Combine with event date
                            combined_dt = parsed_dt.replace(
                                hour=time_parsed.hour,
                                minute=time_parsed.minute
                            )
                            datetime_info[time_type] = combined_dt
                        except:
                            pass
            
            # Validate dates are reasonable (not too far in past/future)
            now = datetime.now(default_tz)
            if parsed_dt < now - timedelta(days=365):
                logger.warning(f"Date seems too far in the past: {parsed_dt}")
                return None
            if parsed_dt > now + timedelta(days=730):  # 2 years
                logger.warning(f"Date seems too far in the future: {parsed_dt}")
                return None
            
            return datetime_info
            
        except Exception as e:
            logger.warning(f"Could not parse date '{raw_date}' on {context_url}: {e}")
            return None
    
    def extract_performers_from_content(self, soup: BeautifulSoup, content_text: str) -> List[Dict[str, Any]]:
        """
        Advanced performer extraction with role identification and social media.
        Returns structured performer objects.
        """
        performers = []
        seen_names = set()
        
        # Check structured data first
        lineup_selectors = [
            ".lineup-artist", ".performer", ".artist-name",
            "[itemprop='performer']", ".dj-name", ".act"
        ]
        
        for selector in lineup_selectors:
            artist_elems = soup.select(selector)
            for elem in artist_elems:
                artist_info = self._extract_artist_info(elem)
                if artist_info and artist_info["name"] not in seen_names:
                    performers.append(artist_info)
                    seen_names.add(artist_info["name"])
        
        # Extract from content using patterns
        if content_text:
            # Headliner patterns
            headliner_patterns = [
                r'(?:headline[rd]?|featuring|presents?|starring)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'([A-Z][A-Za-z\s&\-\.]+)\s*(?:headline[rs]?|presents?)',
            ]
            
            for pattern in headliner_patterns:
                matches = re.finditer(pattern, content_text, re.IGNORECASE)
                for match in matches:
                    name = match.group(1).strip()
                    if name and len(name) > 2 and name not in seen_names:
                        performer = {
                            "name": name,
                            "role": "headliner",
                            "social_media": self._extract_social_media_near_text(content_text, name),
                            "performance_time": None
                        }
                        performers.append(performer)
                        seen_names.add(name)
            
            # Supporting acts patterns
            support_patterns = [
                r'(?:support(?:ed)?\s*by|with|plus)\s*([A-Z][A-Za-z\s&\-\.]+)',
                r'(?:special\s*guest|opening\s*act)s?\s*([A-Z][A-Za-z\s&\-\.]+)'
            ]
            
            for pattern in support_patterns:
                matches = re.finditer(pattern, content_text, re.IGNORECASE)
                for match in matches:
                    name = match.group(1).strip()
                    if name and len(name) > 2 and name not in seen_names:
                        performer = {
                            "name": name,
                            "role": "support",
                            "social_media": self._extract_social_media_near_text(content_text, name),
                            "performance_time": None
                        }
                        performers.append(performer)
                        seen_names.add(name)
        
        return performers
    
    def _extract_artist_info(self, element: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Extract artist information from an element"""
        artist = {
            "name": element.get_text(strip=True),
            "role": "performer",
            "social_media": {},
            "performance_time": None
        }
        
        # Check for role indicators
        parent_text = element.parent.get_text() if element.parent else ""
        if re.search(r'headline', parent_text, re.IGNORECASE):
            artist["role"] = "headliner"
        elif re.search(r'support|opening', parent_text, re.IGNORECASE):
            artist["role"] = "support"
        
        # Extract social media links
        for link in element.find_all('a', href=True):
            social_type = self._identify_social_media_type(link['href'])
            if social_type:
                artist["social_media"][social_type] = link['href']
        
        return artist if artist["name"] else None
    
    def _extract_social_media_near_text(self, text: str, artist_name: str) -> Dict[str, str]:
        """Extract social media mentions near an artist name"""
        social_links = {}
        
        # Find text around artist name (±100 chars)
        pattern = re.escape(artist_name)
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        
        for match in matches:
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)
            context = text[start:end]
            
            # Extract social media from context
            extracted = self.extract_social_media_links(context)
            social_links.update(extracted)
        
        return social_links
    
    def extract_contextual_genres(self, full_text: str, event_title: str = "") -> List[str]:
        """
        Advanced genre extraction with weighted scoring and context analysis.
        """
        genres_scores = {}
        
        # Combine all text for analysis
        combined_text = f"{event_title} {full_text}".lower()
        
        # Score genres based on keyword matches
        for genre, keywords in PREDEFINED_GENRES.items():
            score = 0
            for keyword in keywords:
                # Exact word boundary matches get higher score
                exact_matches = len(re.findall(r'\b' + re.escape(keyword) + r'\b', combined_text))
                score += exact_matches * 2
                
                # Partial matches get lower score
                partial_matches = combined_text.count(keyword) - exact_matches
                score += partial_matches * 0.5
            
            if score > 0:
                genres_scores[genre] = score
        
        # Sort by score and return top genres
        sorted_genres = sorted(genres_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Return genres with significant scores
        threshold = max(genres_scores.values()) * 0.3 if genres_scores else 0
        return [genre for genre, score in sorted_genres if score >= threshold]
    
    def extract_social_media_links(self, text: str) -> Dict[str, List[str]]:
        """
        Extract and validate social media links from text.
        Returns dict with platform names as keys and lists of handles/URLs as values.
        """
        social_links = {
            "instagram": [],
            "facebook": [],
            "twitter": [],
            "tiktok": [],
            "youtube": [],
            "soundcloud": []
        }
        
        for platform, patterns in SOCIAL_MEDIA_PATTERNS.items():
            for pattern in patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    if match.groups():
                        handle = match.group(1)
                        # Validate handle
                        if handle and len(handle) > 2 and not handle.startswith('.'):
                            if platform == "instagram" and handle.startswith('@'):
                                handle = handle[1:]  # Remove @ from Instagram handles
                            social_links[platform].append(handle)
        
        # Remove duplicates and empty lists
        return {k: list(set(v)) for k, v in social_links.items() if v}
    
    def _identify_social_media_type(self, url: str) -> Optional[str]:
        """Identify social media platform from URL"""
        url_lower = url.lower()
        if 'instagram.com' in url_lower or 'instagr.am' in url_lower:
            return 'instagram'
        elif 'facebook.com' in url_lower or 'fb.com' in url_lower:
            return 'facebook'
        elif 'twitter.com' in url_lower or 'x.com' in url_lower:
            return 'twitter'
        elif 'tiktok.com' in url_lower:
            return 'tiktok'
        elif 'youtube.com' in url_lower:
            return 'youtube'
        elif 'soundcloud.com' in url_lower:
            return 'soundcloud'
        return None
    
    def trigger_fallback_methods(self, soup: BeautifulSoup, url: str,
                               initial_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Comprehensive fallback system when primary extraction fails or quality is low.
        """
        logger.info(f"Triggering fallback methods for {url}")
        fallback_data = initial_data.copy()
        
        # Check current quality score
        quality_result = self.scorer.calculate_event_quality(fallback_data)
        current_score = quality_result.get("score", 0)
        
        if current_score >= 90:
            logger.info(f"Quality score {current_score} is already high, skipping fallbacks")
            return fallback_data
        
        # 1. Try alternative CSS selectors
        if not fallback_data.get("title") or current_score < 70:
            alternative_title_selectors = [
                "h1", "h2", ".event-title", "[itemprop='name']",
                ".title", "meta[property='og:title']", "title"
            ]
            
            for selector in alternative_title_selectors:
                elem = soup.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True) if selector != "meta" else elem.get("content", "")
                    if title and len(title) > 5:
                        fallback_data["title"] = title
                        logger.info(f"Found title using fallback selector: {selector}")
                        break
        
        # 2. Look for JSON-LD structured data
        if current_score < 80:
            json_ld_scripts = soup.find_all('script', type='application/ld+json')
            for script in json_ld_scripts:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict):
                        # Extract any available fields
                        if not fallback_data.get("title") and data.get("name"):
                            fallback_data["title"] = data["name"]
                        if not fallback_data.get("full_description") and data.get("description"):
                            fallback_data["full_description"] = data["description"]
                        if data.get("location") and isinstance(data["location"], dict):
                            if not fallback_data.get("venue"):
                                fallback_data["venue"] = data["location"].get("name", "")
                except:
                    continue
        
        # 3. Text mining from unstructured content
        if current_score < 85:
            # Extract dates from anywhere in the page
            date_patterns = [
                r'\b(\d{1,2}[-/]\d{1,2}[-/]\d{2,4})\b',
                r'\b(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{2,4})\b',
                r'\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{2,4})\b'
            ]
            
            page_text = soup.get_text()
            for pattern in date_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                if matches and not fallback_data.get("datetime_obj"):
                    for date_str in matches[:3]:  # Try first 3 matches
                        datetime_info = self.standardize_datetime(date_str)
                        if datetime_info:
                            fallback_data["datetime_obj"] = datetime_info["start_datetime"]
                            fallback_data["raw_date_string"] = date_str
                            logger.info(f"Found date using text mining: {date_str}")
                            break
        
        # 4. Cross-reference multiple sections
        if current_score < 90:
            # Look for ticket info in various formats
            if not fallback_data.get("tier_1"):
                ticket_info = self.extract_comprehensive_ticket_info(soup)
                if ticket_info:
                    for i, tier in enumerate(ticket_info[:3]):
                        fallback_data[f"tier_{i+1}"] = tier
        
        # 5. Content inference
        if not fallback_data.get("genres"):
            # Infer genres from all available text
            all_text = f"{fallback_data.get('title', '')} {fallback_data.get('full_description', '')} {page_text[:2000]}"
            inferred_genres = self.extract_contextual_genres(all_text)
            if inferred_genres:
                fallback_data["genres"] = inferred_genres
                logger.info(f"Inferred genres: {inferred_genres}")
        
        # 6. Meta tag extraction
def fetch_page_with_scroll(self, url: str) -> Optional[str]:
        """
        Enhanced fetch page using Playwright with improved scroll-to-bottom logic.
        Increases initial wait to 5 seconds and implements scroll until no new content loads.
        """
        from playwright.sync_api import sync_playwright
        
        try:
            logger.info(f"Fetching with enhanced Playwright scrolling: {url}")
            with sync_playwright() as p:
                browser = p.chromium.launch()
                page = browser.new_page()
                page.goto(url)
                
                # Enhanced: Increase initial wait to 5 seconds
                logger.info("Waiting 5 seconds for initial page load...")
                time.sleep(5)
                
                # Enhanced: Implement scroll-to-bottom logic until no new content loads
                previous_height = 0
                scroll_attempts = 0
                max_scroll_attempts = 10
                
                while scroll_attempts < max_scroll_attempts:
                    # Get current page height
                    current_height = page.evaluate("document.body.scrollHeight")
                    
                    # If height hasn't changed, we've likely loaded all content
                    if current_height == previous_height:
                        logger.info(f"No new content detected after scroll attempt {scroll_attempts}")
                        break
                    
                    previous_height = current_height
                    
                    # Scroll to bottom
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    logger.info(f"Scroll attempt {scroll_attempts + 1}: height {current_height}")
                    
                    # Wait for potential new content to load
                    time.sleep(2)
                    scroll_attempts += 1
                
                logger.info(f"Completed scrolling after {scroll_attempts} attempts")
                
                # Get final page content
                html = page.content()
                browser.close()
                return html
        except Exception as e:
            logger.error(f"Enhanced Playwright request failed for {url}: {e}")
            return None

    def fetch_page(self, url: str) -> Optional[str]:
        try:
            # Use enhanced Playwright for pages that need scrolling
            if "/promoters/" in url or "/night/" in url or "/events/" in url:
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
        
        # Use the new standardize_datetime method
        datetime_info = self.standardize_datetime(date_str, context_url=context_url)
        if datetime_info:
            return datetime_info["start_datetime"]
        
        # Fallback to original logic if needed
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
        return self.extract_contextual_genres(text_content)

    def scrape_partycal_ticket_card(self, card_soup: BeautifulSoup, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        try:
            # Extract comprehensive content
            content_data = self.extract_main_event_content(card_soup)
            
            event_data = {
                "title": None, "datetime_obj": None, "raw_date_string": None,
                "artists": [], "tier_1": None, "tier_2": None, "tier_3": None, # New tier fields
                "event_card_all_text": card_soup.get_text(separator=' ', strip=True),
                "scrapedAt": datetime.utcnow().isoformat() + "Z",
                "extractionMethod": "html-partycal-ticket", "page_year_context": page_year,
                "full_description": content_data["full_description"],
                "markdown_content": content_data["markdown_content"],
                "content_sections": content_data["sections"],
                "key_info": content_data["key_info"]
            }
            
            header = card_soup.select_one("div.ticket-header")
            if header:
                title_el = header.select_one("h3"); event_data["title"] = title_el.get_text(strip=True) if title_el else None
                date_el = header.select_one("div.ticket-date time") or header.select_one("div.ticket-date")
                if date_el:
                    raw_date = date_el.get_text(strip=True); event_data["raw_date_string"] = raw_date
                    datetime_info = self.standardize_datetime(raw_date, context_url=current_page_url)
                    if datetime_info:
                        event_data["datetime_obj"] = datetime_info["start_datetime"]
                        event_data["datetime_info"] = datetime_info

            body = card_soup.select_one("div.ticket-body")
            if body:
                # Extract comprehensive ticket info
                ticket_tiers = self.extract_comprehensive_ticket_info(body)
                for i, tier in enumerate(ticket_tiers[:3]):
                    event_data[f"tier_{i+1}"] = tier
                
                # Extract performers
                event_data["artists"] = self.extract_performers_from_content(
                    body, content_data["full_description"]
                )
            
            # Extract social media links
            event_data["social_media_links"] = self.extract_social_media_links(
                card_soup.get_text()
            )
            
            # Apply fallback methods if quality is low
            event_data = self.trigger_fallback_methods(card_soup, current_page_url, event_data)
            
            return event_data if event_data.get("title") else None
        except Exception as e: 
            logger.error(f"Error in scrape_partycal_ticket_card for {current_page_url}: {e}", exc_info=True)
            return None

    def parse_json_ld_event(self, event_data_json: dict, current_page_url: str, page_year: Optional[int]) -> Optional[Dict[str, Any]]:
        try:
            raw_date = event_data_json.get("startDate", "")
            datetime_info = self.standardize_datetime(raw_date, context_url=current_page_url)
            
            performers = []
            p_data = event_data_json.get("performer", [])
            p_list = p_data if isinstance(p_data, list) else [p_data] if p_data else []
            for p_item in p_list:
                if isinstance(p_item, dict):
                    performer_obj = {
                        "name": p_item.get("name", ""),
                        "role": "performer",
                        "social_media": {},
                        "performance_time": None
                    }
                    # Check for social media in performer data
                    if p_item.get("sameAs"):
                        social_urls = p_item["sameAs"] if isinstance(p_item["sameAs"], list) else [p_item["sameAs"]]
                        for url in social_urls:
                            social_type = self._identify_social_media_type(url)
                            if social_type:
                                performer_obj["social_media"][social_type] = url
                    performers.append(performer_obj)
                elif isinstance(p_item, str):
                    performers.append({
                        "name": p_item,
                        "role": "performer",
                        "social_media": {},
                        "performance_time": None
                    })

            return {
                "title": event_data_json.get("name", ""), 
                "datetime_obj": datetime_info["start_datetime"] if datetime_info else None,
                "datetime_info": datetime_info,
                "raw_date_string": raw_date,
                "json_ld_description": event_data_json.get("description", ""), # Separate from full_description
                "location": event_data_json.get("location", {}).get("name", "") if isinstance(event_data_json.get("location"), dict) else "",
                "artists": performers, 
                "tier_1": None, "tier_2": None, "tier_3": None, # Typically not in basic JSON-LD event
                "json_ld_url": event_data_json.get("url"), # URL from JSON-LD if available
                "scrapedAt": datetime.utcnow().isoformat() + "Z", 
                "extractionMethod": "json-ld",
                "page_year_context": page_year,
                "social_media_links": {}
            }
        except Exception as e: 
            logger.error(f"Error parsing JSON-LD event on {current_page_url}: {e}", exc_info=True)
            return None
            
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
            
            # Extract comprehensive content
            content_data = self.extract_main_event_content(soup)
            
            page_year_context: Optional[int] = None
            title_text = soup.title.string if soup.title else ""
            year_search_texts = [title_text] + [h.get_text() for h in soup.select('h1, h2, .page-title, .listing-title')]
            for text_area in year_search_texts:
                if text_area:
                    year_match = re.search(r'\b(202\d|203\d)\b', text_area)
                    if year_match: page_year_context = int(year_match.group(1)); break
            if page_year_context: logger.info(f"Year context {page_year_context} for {url}")
            else: logger.warning(f"No year context found for {url}")

            # Extract genres from full content
            page_genres = self.extract_contextual_genres(
                content_data["full_description"], 
                title_text
            )
            
            # Extract social media links from page
            page_social_media = self.extract_social_media_links(html)

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

            # Enhanced CSS Selector Fallback with multiple selectors for robustness
            css_selectors = [
                "div.card-ticket.partycal-ticket",  # Original selector
                "div.event-card",                   # Alternative event card selector
                "article.event-item",               # Article-based event items
                ".event-listing",                   # Generic event listing
                "[data-event-id]"                   # Data attribute based selection
            ]
            
            css_events_found = False
            for selector in css_selectors:
                try:
                    cards = soup.select(selector)
                    if cards:
                        logger.info(f"Found {len(cards)} events using selector '{selector}' on {url}")
                        for card_s in cards:
                            event = self.scrape_partycal_ticket_card(card_s, url, page_year_context)
                            if event and not self.is_duplicate(event, events):
                                events.append(event)
                                logger.info(f"Added CSS event: {event.get('title', 'Untitled')}")
                                css_events_found = True
                        break  # Stop after finding events with first working selector
                except Exception as e:
                    logger.warning(f"CSS selector '{selector}' failed on {url}: {e}")
                    continue
            
            if not css_events_found and not json_ld_events:
                logger.warning(f"No events found with any extraction method on {url}")

            final_events = []
            for event_item in events:
                # Add common page data to all events from this page
                event_item["full_description"] = content_data["full_description"]
                event_item["markdown_content"] = content_data["markdown_content"]
                event_item["content_sections"] = content_data["sections"]
                event_item["genres"] = page_genres
                event_item["promoter"] = current_promoter_slug
                event_item["venue"] = self.current_venue_context
                event_item["tickets_url"] = url
                event_item["page_social_media"] = page_social_media
                
                # Extract additional performers from content
                if not event_item.get("artists"):
                    event_item["artists"] = self.extract_performers_from_content(
                        soup, content_data["full_description"]
                    )
                
                # Calculate data quality scores
                event_item["data_quality"] = {
                    "completeness_score": self.calculate_completeness_score(event_item),
                    "accuracy_score": self.calculate_accuracy_score(event_item),
                    "freshness_score": self.calculate_freshness_score(event_item)
                }
                
                final_events.append(event_item)

            if not final_events and content_data["full_description"]:
                # Fallback if no structured events but page has content
                fallback_event = {
                    "title": f"Content Page: {soup.title.string if soup.title else url}",
                    "datetime_obj": None,
                    "full_description": content_data["full_description"],
                    "markdown_content": content_data["markdown_content"],
                    "content_sections": content_data["sections"],
                    "genres": page_genres,
                    "promoter": current_promoter_slug,
                    "venue": self.current_venue_context,
                    "tickets_url": url,
                    "scrapedAt": datetime.utcnow().isoformat() + "Z",
                    "extractionMethod": "page-description-only",
                    "page_year_context": page_year_context,
                    "tier_1": None, "tier_2": None, "tier_3": None,
                    "social_media_links": page_social_media,
                    "data_quality": {
                        "completeness_score": 30.0,
                        "accuracy_score": 50.0,
                        "freshness_score": 100.0
                    }
                }
                final_events.append(fallback_event)

            # Enhanced pagination logic with better CSS selectors
            pagination_selectors = [
                'a[rel="next"]',           # Standard next link
                'a.next',                  # Class-based next
                'li.next > a',             # List item next
                'a:contains("Next")',      # Text-based next (English)
                'a:contains("Siguiente")', # Text-based next (Spanish)
                '.pagination a[href*="page"]', # Pagination with page parameter
                'a[href*="offset"]',       # Offset-based pagination
                '.pager .next a'           # Pager-based next
            ]
            
            for selector in pagination_selectors:
                try:
                    next_links = soup.select(selector)
                    for link_el in next_links:
                        href = link_el.get('href')
                        if href:
                            next_url = urljoin(url, href)
                            if next_url not in self.visited_urls and self.current_depth < self.max_depth:
                                self.url_queue.append((next_url, self.current_depth + 1))
                                logger.info(f"Queued next page: {next_url}")
                    if next_links:
                        break  # Stop after finding pagination with first working selector
                except Exception as e:
                    logger.warning(f"Pagination selector '{selector}' failed on {url}: {e}")
                    continue
                    
            return final_events
        except Exception as e:
            logger.error(f"Error scraping promoter page {url}: {e}", exc_info=True)
            return []

    def extract_venue_club_urls(self, url: str) -> List[str]:
        """
        Enhanced method to extract venue and club URLs from sidebar/navigation.
        Implements the sidebar-focused extraction pattern.
        """
        html = self.fetch_page(url)
        if not html: return []
        
        soup = BeautifulSoup(html, "html.parser")
        venue_club_links = set()
        
        # First, try to find sidebar/navigation containers
        sidebar_containers = soup.select(
            'aside, nav, .sidebar, .navigation, .side-nav, .venue-list, '
            '.club-list, [class*="sidebar"], [class*="navigation"], '
            '.widget, .menu-venues, #venues-menu'
        )
        
        if sidebar_containers:
            logger.info(f"Found {len(sidebar_containers)} sidebar/navigation containers")
            for container in sidebar_containers:
                # Extract venue/club links from sidebar
                venue_links = container.select('a[href*="/venue/"], a[href*="/clubs/"]')
                for link in venue_links:
                    href = link.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        venue_club_links.add(full_url)
                        logger.debug(f"Found venue URL in sidebar: {full_url}")
        
        # Fallback to broader search if no sidebar found
        if not venue_club_links:
            logger.info("No sidebar found, using fallback venue extraction")
            venue_selectors = [
                'a[href*="/night/venue/"]',     # Direct venue links
                'a[href*="/venue/"]',           # Alternative venue pattern
                'a[href*="/club/"]',            # Club links
                'a[href*="/night/clubs/"]',     # Alternative club pattern
                '.venue-link',                  # Class-based venue links
                '.club-link',                   # Class-based club links
                '[data-venue-id]',              # Data attribute venue links
            ]
            
            for selector in venue_selectors:
                try:
                    links = soup.select(selector)
                    for link in links:
                        href = link.get('href')
                        if href:
                            full_url = urljoin(url, href)
                            venue_club_links.add(full_url)
                except Exception as e:
                    logger.warning(f"Venue selector '{selector}' failed on {url}: {e}")
                    continue
        
        # Validate expected venue count (26 for Ibiza Spotlight)
        venue_count = len(venue_club_links)
        if venue_count != 26:
            logger.warning(f"Expected 26 venue URLs but found {venue_count}")
        else:
            logger.info(f"Successfully found expected 26 venue URLs")
        
        logger.info(f"Extracted {venue_count} venue/club URLs from {url}")
        return list(venue_club_links)

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
            "_quality", "extractionMethod", "page_year_context", "scrapedAt", "artists",
            "social_media_links", "data_quality"
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
        """
        Enhanced run method implementing the proper navigation flow with progress bar.
        1. On main events page: ONLY extract venue URLs (no event scraping)
        2. For each venue URL: extract promoter URLs
        3. For each promoter URL: scrape events
        """
        logger.info(f"Starting enhanced scraper run_id: {self.run_timestamp}. CSV: {self.csv_file_path.name}")
        self.all_scraped_events_for_run = []; self.csv_headers_written = False
        
        # Statistics tracking
        venue_count = 0
        promoter_count_by_venue = {}
        event_count_by_promoter = {}

        # Initialize URL queue
        self.url_queue = [(self.config.url, 0)]
        logger.info(f"Starting with URL: {self.config.url}")
        
        # Initialize progress bar
        self.progress_bar = tqdm(
            desc="Scraping Progress",
            unit="pages",
            position=0,
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] "
                      "Venues: {postfix[venues]} | Promoters: {postfix[promoters]} | Events: {postfix[events]}",
            postfix={'venues': 0, 'promoters': 0, 'events': 0}
        )

        while self.url_queue:
            try:
                url, depth = self.url_queue.pop(0)
                if url in self.visited_urls or depth > self.max_depth:
                    continue
                self.visited_urls.add(url)
                self.current_depth = depth
                logger.info(f"Processing [{depth}]: {url}")
                
                # Update progress bar
                self.progress_bar.update(1)

                # Dynamic venue context update based on current URL
                self._update_venue_context_from_url(url)

                # Determine page type and process accordingly
                if depth == 0 and "/events/" in url:
                    # Main events page - ONLY extract venue URLs, no event scraping
                    logger.info("Main events page detected - extracting venue URLs only")
                    venue_club_urls = self.extract_venue_club_urls(url)
                    venue_count = len(venue_club_urls)
                    
                    self.stats["venues_scraped"] = venue_count
                    self.progress_bar.set_postfix({'venues': venue_count, 'promoters': 0, 'events': 0})
                    
                    logger.info(f"DEBUG: Found {venue_count} venue URLs from main events page")
                    
                    if venue_club_urls:
                        for vc_url in venue_club_urls:
                            if vc_url not in self.visited_urls and depth + 1 <= self.max_depth:
                                self.url_queue.append((vc_url, depth + 1))
                                logger.debug(f"Added venue URL to queue: {vc_url}")
                    else:
                        logger.warning("No venue URLs found on main events page!")
                        
                elif "/venue/" in url or "/clubs/" in url:
                    # Venue page - extract promoter URLs only
                    logger.info(f"Venue page detected - extracting promoter URLs from: {url}")
                    promoter_urls = self.extract_promoter_urls(url)
                    
                    # Track promoter count for this venue
                    venue_name = url.split('/')[-1]
                    promoter_count_by_venue[venue_name] = len(promoter_urls)
                    
                    self.stats["promoters_scraped"] += len(promoter_urls)
                    self.progress_bar.set_postfix({
                        'venues': self.stats["venues_scraped"],
                        'promoters': self.stats["promoters_scraped"],
                        'events': self.stats["events_scraped"]
                    })
                    
                    logger.info(f"DEBUG: Found {len(promoter_urls)} promoter URLs from venue '{venue_name}'")
                    
                    if promoter_urls:
                        for p_url in promoter_urls:
                            if p_url not in self.visited_urls and depth + 1 <= self.max_depth:
                                self.url_queue.append((p_url, depth + 1))
                                logger.debug(f"Added promoter URL to queue: {p_url}")
                    else:
                        logger.warning(f"No promoter URLs found on venue page: {url}")
                        
                elif "/promoters/" in url or "/promoter/" in url:
                    # Promoter page - scrape events
                    logger.info(f"Promoter page detected - scraping events from: {url}")
                    page_events = self.scrape_promoter_page(url)
                    
                    # Track event count for this promoter
                    promoter_name = url.split('/')[-1]
                    event_count
        meta_mappings = {
            "description": ["description", "og:description", "twitter:description"],
            "title": ["og:title", "twitter:title"],
            "image": ["og:image", "twitter:image"]
        }
        
        for field, meta_names in meta_mappings.items():
            if not fallback_data.get(field):
                for meta_name in meta_names:
                    meta_tag = soup.find("meta", attrs={"property": meta_name}) or \
                              soup.find("meta", attrs={"name": meta_name})
                    if meta_tag and meta_tag.get("content"):
                        if field == "description":
                            fallback_data["full_description"] = meta_tag["content"]
                        else:
                            fallback_data[field] = meta_tag["content"]
                        break
        
        # Calculate final quality score
        final_quality = self.scorer.calculate_event_quality(fallback_data)
        logger.info(f"Quality improved from {current_score} to {final_quality.get('score', 0)}")
        
        return fallback_data
    
    def calculate_completeness_score(self, event_data: Dict[str, Any]) -> float:
        """Calculate completeness score based on essential field presence"""
        essential_fields = {
            "title": 0.20,
            "datetime_obj": 0.25,
            "venue": 0.15,
            "full_description": 0.10,
            "tier_1": 0.15,
            "genres": 0.10,
            "artists": 0.05
        }
        
        score = 0.0
        for field, weight in essential_fields.items():
            if event_data.get(field):
                score += weight
        
        return score * 100
    
    def calculate_accuracy_score(self, event_data: Dict[str, Any]) -> float:
        """Calculate accuracy score based on data validation rules"""
        score = 100.0
        penalties = []
        
        # Check date validity
        if event_data.get("datetime_obj"):
            dt = event_data["datetime_obj"]
            now = datetime.now(pytz.UTC)
            if dt < now - timedelta(days=30):
                penalties.append(("past_event", 20))
            elif dt > now + timedelta(days=365):
                penalties.append(("far_future_event", 10))
        
        # Check price validity
        for tier in ["tier_1", "tier_2", "tier_3"]:
            if event_data.get(tier) and isinstance(event_data[tier], dict):
                price = event_data[tier].get("price")
                if price and (price < 0 or price > 10000):
                    penalties.append((f"{tier}_invalid_price", 10))
        
        # Apply penalties
        for reason, penalty in penalties:
            score -= penalty
            logger.debug(f"Accuracy penalty: {reason} (-{penalty})")
        
        return max(0, score)
    
    def calculate_freshness_score(self, event_data: Dict[str, Any]) -> float:
        """Calculate freshness score based on content update timestamps"""
        score = 100.0
        
        # Check if scraped recently
        if event_data.get("scrapedAt"):
            scraped_time = datetime.fromisoformat(event_data["scrapedAt"].replace('Z', '+00:00'))
            age_hours = (datetime.now(timezone.utc) - scraped_time).total_seconds() / 3600
            
            if age_hours < 24:
                score = 100.0
            elif age_hours < 72:
                score = 90.0
            elif age_hours < 168:  # 1 week
                score = 70.0
            else:
                score = 50.0
        
        return score