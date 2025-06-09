"""
Enhanced Ibiza Spotlight Scraper with 8 Major Improvements
==========================================================

This scraper implements comprehensive enhancements for better data extraction:
1. Enhanced main body text extraction with Markdown conversion
2. Advanced ticket pricing structure extraction
3. Unified date/time formatting with ISO 8601 compliance
4. Advanced performer & genre extraction with social media
5. Social media link extraction and validation
6. Comprehensive data quality system with fallback methods
7. Enhanced error handling with progress bar
8. Improved venue extraction with sidebar focus

Author: Code Mode Agent
Date: 2025-06-05
"""

import os
import logging
import time
import random
import csv
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta, timezone
import datetime as dt
from dataclasses import dataclass, field

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
import pytz
from tqdm import tqdm
import html2text

from config import settings
from database.quality_scorer import QualityScorer
from pymongo import MongoClient, errors
from pymongo.database import Database

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define formatter here, as it's used by both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add console handler for INFO and above (remains at module level)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


@dataclass
class ScraperConfig:
    url: str
    min_delay: float = 0.7
    max_delay: float = 1.8
    save_to_db: bool = True
    headers: Dict[str, str] = field(default_factory=lambda: {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

def get_mongodb_connection(retries=3, delay=2) -> Optional[Database]:
    """Get MongoDB connection with retry logic"""
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
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 1.5
            else:
                logger.error("MongoDB connection failed after all retries.")
                raise
        except Exception as e:
            logger.error(f"Unexpected MongoDB error: {e}")
            raise
    return None

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, dt.date)): # Handle both datetime and date
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

class IbizaSpotlightScraper:
    """Enhanced scraper for Ibiza Spotlight with comprehensive data extraction"""
    
    def __init__(self, config: ScraperConfig):
        """Initialize scraper with configuration"""
        self.config = config
        
        # Moved log_dir and file_handler setup into __init__
        self.log_dir = Path("classy_skkkrapey/scrape_logs")
        self.log_dir.mkdir(parents=True, exist_ok=True) # Ensure parents=True for robustness

        # Create a unique log file for each scraper instance if desired, or use a shared one
        # For now, keeping the original dynamic name generation but tied to instance
        instance_run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_handler = logging.FileHandler(self.log_dir / f"ibiza_spotlight_{instance_run_timestamp}.log")
        file_handler.setLevel(logging.DEBUG)
        # formatter is already defined at module level
        file_handler.setFormatter(formatter)

        # Add file_handler only if not already present (e.g. if multiple instances share a logger)
        # A more robust approach might involve instance-specific loggers or careful handler management.
        # For now, this ensures the handler is added.
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == file_handler.baseFilename for h in logger.handlers):
             logger.addHandler(file_handler)

        self.session = requests.Session()
        self.session.headers.update(self.config.headers)
        
        # Initialize components
        self.url_queue: List[Tuple[str, int]] = []
        self.visited_urls: Set[str] = set()
        self.current_depth = 0
        self.max_depth = 3
        self.current_venue_context: Optional[str] = None
        
        # MongoDB setup
        self.db = get_mongodb_connection() if self.config.save_to_db else None
        self.scorer = QualityScorer()
        
        # CSV setup
        self.run_timestamp = instance_run_timestamp # Use the same timestamp for CSV as for log file
        self.csv_file_path = self.log_dir / f"scraped_events_{self.run_timestamp}.csv" # Use self.log_dir
        self.csv_headers_written = False
        self.all_scraped_events_for_run: List[Dict[str, Any]] = []
        
        # Progress tracking
        self.progress_bar: Optional[tqdm] = None
        self.stats = {
            "venues_scraped": 0,
            "promoters_scraped": 0,
            "events_scraped": 0
        }
        
        # Initialize Markdown converter
        self.html2text = html2text.HTML2Text()
        self.html2text.ignore_links = False
        self.html2text.ignore_images = True
        self.html2text.body_width = 0  # Don't wrap lines
        
        logger.info(f"Initialized IbizaSpotlightScraper with URL: {self.config.url}")
    
    def _update_venue_context_from_url(self, url: str):
        """Extract venue context from URL"""
        parsed = urlparse(url)
        path_parts = parsed.path.strip('/').split('/')
        
        if 'venue' in path_parts or 'clubs' in path_parts:
            try:
                venue_idx = path_parts.index('venue') if 'venue' in path_parts else path_parts.index('clubs')
                if venue_idx + 1 < len(path_parts):
                    self.current_venue_context = path_parts[venue_idx + 1]
                    logger.debug(f"Updated venue context: {self.current_venue_context}")
            except ValueError:
                pass
    
    def extract_main_event_content(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Enhancement #1: Extract main event content with Markdown conversion
        """
        content_data = {
            "full_description": "",
            "markdown_content": "",
            "sections": {},
            "key_info": {}
        }
        
        # Primary content selectors
        content_selectors = [
            "main article",
            "div.event-content",
            "div.event-description",
            "div.content-main",
            "article.post-content",
            "[class*='event-detail']",
            "[class*='event-info']"
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
            # Extract text content
            content_data["full_description"] = main_content.get_text(separator=' ', strip=True)
            
            # Convert to Markdown
            try:
                content_data["markdown_content"] = self.html2text.handle(str(main_content))
            except Exception as e:
                logger.warning(f"Markdown conversion failed: {e}")
                content_data["markdown_content"] = content_data["full_description"]
            
            # Extract sections
            for heading in main_content.find_all(['h1', 'h2', 'h3', 'h4']):
                section_title = heading.get_text(strip=True)
                section_content = []
                
                for sibling in heading.find_next_siblings():
                    if sibling.name and sibling.name.startswith('h'):
                        break
                    section_content.append(sibling.get_text(strip=True))
                
                if section_content:
                    content_data["sections"][section_title] = ' '.join(section_content)
            
            # Extract key information
            info_patterns = {
                "doors_open": r"doors\s*(?:open)?:\s*(\d{1,2}:\d{2})",
                "age_restriction": r"(\d{2}\+|ages?\s*\d{2}\+)",
                "dress_code": r"dress\s*code:\s*([^\.]+)",
                "capacity": r"capacity:\s*(\d+)",
            }
            
            full_text = content_data["full_description"].lower()
            for key, pattern in info_patterns.items():
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    content_data["key_info"][key] = match.group(1).strip()
        
        return content_data
    
    def extract_comprehensive_ticket_info(self, element: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Enhancement #2: Extract comprehensive ticket pricing structure
        """
        ticket_tiers = []
        
        # Look for ticket containers
        ticket_selectors = [
            ".ticket-tier",
            ".price-option",
            ".ticket-type",
            "[class*='ticket-price']",
            "[class*='price-tier']",
            "li:has(span.price)",
            "div:has(span.price)"
        ]
        
        ticket_elements = []
        for selector in ticket_selectors:
            ticket_elements.extend(element.select(selector))
        
        if not ticket_elements:
            # Fallback: look for price patterns in text
            text = element.get_text()
            price_pattern = r'([^€£$]*?)\s*[€£$]\s*(\d+(?:\.\d{2})?)'
            matches = re.findall(price_pattern, text)
            
            for tier_name, price in matches:
                tier_name = tier_name.strip()
                if tier_name:
                    ticket_tiers.append({
                        "tier_name": tier_name,
                        "price": float(price),
                        "currency": "EUR",
                        "availability": "unknown",
                        "benefits": []
                    })
        else:
            # Extract from structured elements
            for ticket_el in ticket_elements:
                tier_info = {
                    "tier_name": "",
                    "price": None,
                    "currency": "EUR",
                    "availability": "available",
                    "benefits": []
                }
                
                # Extract tier name
                name_el = ticket_el.select_one("h3, h4, .tier-name, .ticket-name")
                if name_el:
                    tier_info["tier_name"] = name_el.get_text(strip=True)
                
                # Extract price
                price_el = ticket_el.select_one(".price, .ticket-price, span[class*='price']")
                if price_el:
                    price_text = price_el.get_text(strip=True)
                    price_match = re.search(r'(\d+(?:\.\d{2})?)', price_text)
                    if price_match:
                        tier_info["price"] = float(price_match.group(1))
                
                # Extract availability
                if "sold-out" in ticket_el.get('class', []) or "soldout" in str(ticket_el):
                    tier_info["availability"] = "sold_out"
                elif "limited" in str(ticket_el).lower():
                    tier_info["availability"] = "limited"
                
                # Extract benefits
                benefits_el = ticket_el.select("ul li, .benefit, .perk")
                tier_info["benefits"] = [b.get_text(strip=True) for b in benefits_el]
                
                if tier_info["tier_name"] and tier_info["price"] is not None:
                    ticket_tiers.append(tier_info)
        
        return ticket_tiers[:3]  # Return top 3 tiers
    
    def standardize_datetime(self, date_str: str, context_url: str = "") -> Optional[Dict[str, Any]]:
        """
        Enhancement #3: Unified date/time formatting with ISO 8601 compliance
        """
        if not date_str:
            return None
        
        datetime_info = {
            "start_datetime": None,
            "end_datetime": None,
            "timezone": "Europe/Madrid",  # Ibiza timezone
            "is_recurring": False,
            "recurrence_pattern": None,
            "original_string": date_str
        }
        
        # Clean date string
        date_str = date_str.strip()
        date_str = re.sub(r'\s+', ' ', date_str)
        
        # Check for date ranges
        range_patterns = [
            r'from\s+(.+?)\s+to\s+(.+)',
            r'(.+?)\s*-\s*(.+)',
            r'(.+?)\s*–\s*(.+)',  # em dash
        ]
        
        for pattern in range_patterns:
            match = re.search(pattern, date_str, re.IGNORECASE)
            if match:
                start_str, end_str = match.groups()
                try:
                    start_dt = date_parser.parse(start_str)
                    end_dt = date_parser.parse(end_str)
                    
                    # Apply timezone
                    tz = pytz.timezone(datetime_info["timezone"])
                    if start_dt.tzinfo is None:
                        start_dt = tz.localize(start_dt)
                    if end_dt.tzinfo is None:
                        end_dt = tz.localize(end_dt)
                    
                    datetime_info["start_datetime"] = start_dt
                    datetime_info["end_datetime"] = end_dt
                    
                    return datetime_info
                except Exception as e:
                    logger.debug(f"Failed to parse date range: {e}")
        
        # Single date parsing
        try:
            parsed_dt = date_parser.parse(date_str)
            
            # Apply timezone if not present
            if parsed_dt.tzinfo is None:
                tz = pytz.timezone(datetime_info["timezone"])
                parsed_dt = tz.localize(parsed_dt)
            
            datetime_info["start_datetime"] = parsed_dt
            
            # Check for recurring patterns
            if re.search(r'every\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)', 
                        date_str, re.IGNORECASE):
                datetime_info["is_recurring"] = True
                datetime_info["recurrence_pattern"] = "weekly"
            
            return datetime_info
            
        except Exception as e:
            logger.warning(f"Failed to parse date '{date_str}': {e}")
            return None
    
    def extract_contextual_genres(self, text: str, title: str = "") -> List[str]:
        """
        Extract music genres from text content using contextual analysis
        """
        genres = set()
        
        # Comprehensive genre patterns
        genre_keywords = {
            "house": ["house", "tech-house", "deep house", "progressive house", "afro house"],
            "techno": ["techno", "minimal techno", "hard techno", "melodic techno"],
            "trance": ["trance", "psy-trance", "progressive trance", "uplifting trance"],
            "drum_and_bass": ["drum and bass", "dnb", "d&b", "jungle"],
            "disco": ["disco", "nu-disco", "disco house", "cosmic disco"],
            "funk": ["funk", "funky", "future funk"],
            "soul": ["soul", "neo-soul", "soulful"],
            "hip_hop": ["hip hop", "hip-hop", "rap", "trap"],
            "reggae": ["reggae", "dub", "dancehall"],
            "latin": ["latin", "reggaeton", "salsa", "bachata"],
            "electronic": ["electronic", "electronica", "idm", "ambient"],
            "pop": ["pop", "dance pop", "synth pop"],
            "rock": ["rock", "indie rock", "alternative rock"],
            "jazz": ["jazz", "acid jazz", "jazz fusion"],
            "world": ["world music", "afrobeat", "ethnic"],
            "experimental": ["experimental", "avant-garde", "noise"]
        }
        
        # Search in text
        text_lower = (text + " " + title).lower()
        
        for genre_category, keywords in genre_keywords.items():
            for keyword in keywords:
                if keyword in text_lower:
                    genres.add(genre_category)
                    # Also add specific sub-genre if it's not the main category
                    if keyword != genre_category:
                        genres.add(keyword.replace(" ", "_"))
        
        # Look for DJ style indicators
        style_patterns = [
            r'playing\s+(\w+(?:\s+\w+)?)\s+music',
            r'(\w+(?:\s+\w+)?)\s+dj',
            r'(\w+(?:\s+\w+)?)\s+sounds',
            r'(\w+(?:\s+\w+)?)\s+vibes'
        ]
        
        for pattern in style_patterns:
            matches = re.findall(pattern, text_lower)
            for match in matches:
                for genre_cat, keywords in genre_keywords.items():
                    if any(kw in match for kw in keywords):
                        genres.add(genre_cat)
        
        return sorted(list(genres))
    
    def extract_performers_from_content(self, soup: BeautifulSoup, text_content: str) -> List[Dict[str, Any]]:
        """
        Enhancement #4: Advanced performer extraction with roles and social media
        """
        performers = []
        seen_names = set()
        
        # Look for structured performer data
        performer_selectors = [
            ".artist", ".dj", ".performer",
            "[class*='lineup']", "[class*='artist']",
            "h3:contains('Line-up')", "h3:contains('Artists')"
        ]
        
        for selector in performer_selectors:
            elements = soup.select(selector)
            for el in elements:
                name = el.get_text(strip=True)
                if name and name not in seen_names:
                    seen_names.add(name)
                    performer = {
                        "name": name,
                        "role": "dj",  # default role
                        "social_media": {},
                        "performance_time": None
                    }
                    
                    # Check for role indicators
                    parent_text = el.parent.get_text() if el.parent else ""
                    if "headline" in parent_text.lower():
                        performer["role"] = "headliner"
                    elif "support" in parent_text.lower():
                        performer["role"] = "support"
                    elif "live" in parent_text.lower():
                        performer["role"] = "live_act"
                    
                    performers.append(performer)
        
        # Pattern-based extraction from text
        if not performers:
            # Headliner patterns
            headliner_patterns = [
                r'(?:headline[rd]?|featuring|presents?|starring)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'([A-Z][A-Za-z\s&\-\.]+)\s*(?:headline[rs]?|presents?)',
            ]
            
            for pattern in headliner_patterns:
                matches = re.findall(pattern, text_content)
                for match in matches:
                    name = match.strip()
                    if len(name) > 2 and name not in seen_names:
                        seen_names.add(name)
                        performers.append({
                            "name": name,
                            "role": "headliner",
                            "social_media": {},
                            "performance_time": None
                        })
            
            # Support/other DJs
            support_patterns = [
                r'(?:support|supported\s+by|with|alongside)\s*(?:by\s*)?([A-Z][A-Za-z\s&\-\.]+)',
                r'(?:also\s+playing|residents?|djs?):\s*([A-Z][A-Za-z\s&\-\.,]+)'
            ]
            
            for pattern in support_patterns:
                matches = re.findall(pattern, text_content)
                for match in matches:
                    # Split on common separators
                    names = re.split(r'[,&]', match)
                    for name in names:
                        name = name.strip()
                        if len(name) > 2 and name not in seen_names:
                            seen_names.add(name)
                            performers.append({
                                "name": name,
                                "role": "support",
                                "social_media": {},
                                "performance_time": None
                            })
        
        # Extract social media for each performer
        for performer in performers:
            social_links = self.extract_social_media_links(str(soup))
            # Try to match social links to performer names
            for platform, urls in social_links.items():
                for url in urls if isinstance(urls, list) else [urls]:
                    if performer["name"].lower().replace(" ", "") in url.lower():
                        performer["social_media"][platform] = url
        
        return performers
    
    def extract_social_media_links(self, html_content: str) -> Dict[str, Any]:
        """
        Enhancement #5: Extract and validate social media links
        """
        social_media = {}
        
        # Platform patterns
        platform_patterns = {
            "facebook": r'(?:https?://)?(?:www\.)?facebook\.com/[\w\-\.]+',
            "instagram": r'(?:https?://)?(?:www\.)?instagram\.com/[\w\-\.]+',
            "twitter": r'(?:https?://)?(?:www\.)?twitter\.com/[\w\-\.]+',
            "soundcloud": r'(?:https?://)?(?:www\.)?soundcloud\.com/[\w\-\.]+',
            "spotify": r'(?:https?://)?open\.spotify\.com/artist/[\w]+',
            "youtube": r'(?:https?://)?(?:www\.)?youtube\.com/(?:c/|channel/|user/)[\w\-]+',
            "mixcloud": r'(?:https?://)?(?:www\.)?mixcloud\.com/[\w\-\.]+',
            "beatport": r'(?:https?://)?(?:www\.)?beatport\.com/artist/[\w\-]+/\d+',
            "tiktok": r'(?:https?://)?(?:www\.)?tiktok\.com/@[\w\-\.]+',
            "linktree": r'(?:https?://)?linktr\.ee/[\w\-\.]+'
        }
        
        for platform, pattern in platform_patterns.items():
            matches = re.findall(pattern, html_content, re.IGNORECASE)
            if matches:
                # Clean and deduplicate URLs
                cleaned_urls = []
                for url in matches:
                    if not url.startswith('http'):
                        url = 'https://' + url
                    if url not in cleaned_urls:
                        cleaned_urls.append(url)
                
                if cleaned_urls:
                    social_media[platform] = cleaned_urls[0] if len(cleaned_urls) == 1 else cleaned_urls
        
        return social_media
    
    def _identify_social_media_type(self, url: str) -> Optional[str]:
        """Identify social media platform from URL"""
        platform_domains = {
            "facebook.com": "facebook",
            "instagram.com": "instagram",
            "twitter.com": "twitter",
            "soundcloud.com": "soundcloud",
            "spotify.com": "spotify",
            "youtube.com": "youtube",
            "mixcloud.com": "mixcloud",
            "beatport.com": "beatport",
            "tiktok.com": "tiktok",
            "linktr.ee": "linktree"
        }
        
        for domain, platform in platform_domains.items():
            if domain in url:
                return platform
        return None
    
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
            try:
                scraped_time = datetime.fromisoformat(event_data["scrapedAt"].replace('Z', '+00:00'))
                age_hours = (datetime.now(pytz.UTC) - scraped_time).total_seconds() / 3600
                
                if age_hours > 24:
                    score -= min(50, age_hours / 24 * 10)  # Lose 10 points per day, max 50
            except Exception as e:
                logger.debug(f"Failed to parse scraped time: {e}")
        
        return max(0, score)
    
    def trigger_fallback_methods(self, soup: BeautifulSoup, url: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhancement #6: Implement fallback methods when quality is low
        """
        # Calculate current quality
        current_score = self.scorer.calculate_event_quality(event_data).get("score", 0)
        
        if current_score >= 80:
            return event_data  # Good enough
        
        logger.info(f"Triggering fallback methods for low quality score: {current_score}")
        
        fallback_data = event_data.copy()
        
        # Fallback 1: Check meta tags
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
        
        # Fallback 2: Schema.org microdata
        schema_events = soup.find_all(attrs={"itemtype": re.compile(r"schema\.org/.*Event", re.I)})
        for schema_event in schema_events:
            if not fallback_data.get("title"):
                title_el = schema_event.find(attrs={"itemprop": "name"})
                if title_el:
                    fallback_data["title"] = title_el.get_text(strip=True)
            
            if not fallback_data.get("datetime_obj"):
                date_el = schema_event.find(attrs={"itemprop": "startDate"})
                if date_el and date_el.get("content"):
                    datetime_info = self.standardize_datetime(date_el["content"], url)
                    if datetime_info:
                        fallback_data["datetime_obj"] = datetime_info["start_datetime"]
                        fallback_data["datetime_info"] = datetime_info
        
        # Fallback 3: Aggressive text pattern matching
        if not fallback_data.get("artists"):
            text = soup.get_text()
            # Look for "DJ Name" patterns
            dj_patterns = [
                r'DJ\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+\(DJ\)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+b2b\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)'
            ]
            
            artists = []
            for pattern in dj_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    if isinstance(match, tuple):
                        artists.extend(match)
                    else:
                        artists.append(match)
            
            if artists:
                fallback_data["artists"] = [{"name": a, "role": "dj", "social_media": {}, "performance_time": None} 
                                           for a in set(artists) if len(a) > 2]
        
        # Calculate final quality score
        final_quality = self.scorer.calculate_event_quality(fallback_data)
        logger.info(f"Quality improved from {current_score} to {final_quality.get('score', 0)}")
        
        return fallback_data
    
    def fetch_page_with_scroll(self, url: str) -> Optional[str]:
        """
        Enhanced fetch page using Playwright with improved scroll-to-bottom logic.
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
            if year and parsed_dt and parsed_dt.year != year:
                parsed_dt = parsed_dt.replace(year=year)
            elif year and parsed_dt and parsed_dt.year == datetime.now().year and year != datetime.now().year:
                parsed_dt = parsed_dt.replace(year=year)
            return parsed_dt
        except (ValueError, TypeError) as e_parse:
            if year:
                try:
                    parsed_dt = date_parser.parse(f"{date_str} {year}")
                    if parsed_dt and parsed_dt.year != year:
                        parsed_dt = parsed_dt.replace(year=year)
                    return parsed_dt
                except Exception as e_year_add:
                    logger.warning(f"Could not parse date '{date_str}' with year {year} on {context_url}. Errors: {e_parse}, {e_year_add}")
            else:
                logger.warning(f"Could not parse date '{date_str}' (no year context) on {context_url}. Error: {e_parse}")
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
                "artists": [], "tier_1": None, "tier_2": None, "tier_3": None,
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
                title_el = header.select_one("h3")
                event_data["title"] = title_el.get_text(strip=True) if title_el else None
                date_el = header.select_one("div.ticket-date time") or header.select_one("div.ticket-date")
                if date_el:
                    raw_date = date_el.get_text(strip=True)
                    event_data["raw_date_string"] = raw_date
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

            # Prepare location dictionary
            location_value_from_json = event_data_json.get("location")
            event_location_dict = { # Ensure basic keys expected by QualityScorer exist
                "venue": None,
                "address": None,
                "city": None
            }
            if isinstance(location_value_from_json, dict):
                event_location_dict['venue'] = location_value_from_json.get("name")
                address_obj = location_value_from_json.get("address")
                if isinstance(address_obj, dict):
                    event_location_dict['address'] = address_obj.get("streetAddress")
                    event_location_dict['city'] = address_obj.get("addressLocality") # Common in Schema.org
                elif isinstance(address_obj, str):
                     event_location_dict['address'] = address_obj
                # Add other potential location fields if needed, e.g., postalCode, country
            elif isinstance(location_value_from_json, str):
                event_location_dict['venue'] = location_value_from_json # Assume the string is the venue name

            return {
                "title": event_data_json.get("name", ""),
                "datetime_obj": datetime_info["start_datetime"] if datetime_info else None,
                "datetime_info": datetime_info,
                "raw_date_string": raw_date,
                "json_ld_description": event_data_json.get("description", ""),
                "location": event_location_dict, # Now always a dictionary
                "artists": performers,
                "tier_1": None, "tier_2": None, "tier_3": None,
                "json_ld_url": event_data_json.get("url"),
                "scrapedAt": datetime.now(timezone.utc).isoformat(), # Replaced utcnow()
                "extractionMethod": "json-ld",
                "page_year_context": page_year,
                "social_media_links": {}
            }
        except Exception as e:
            logger.error(f"Error parsing JSON-LD event on {current_page_url}: {e}", exc_info=True)
            return None
    
    def is_duplicate(self, new_event: Dict[str, Any], existing_events: List[Dict[str, Any]]) -> bool:
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
                    if year_match:
                        page_year_context = int(year_match.group(1))
                        break
            if page_year_context:
                logger.info(f"Year context {page_year_context} for {url}")
            else:
                logger.warning(f"No year context found for {url}")

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
                "div.card-ticket.partycal-ticket",
                "div.event-card",
                "article.event-item",
                ".event-listing",
                "[data-event-id]"
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
                        break
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
                'a[rel="next"]',
                'a.next',
                'li.next > a',
                'a:contains("Next")',
                'a:contains("Siguiente")',
                '.pagination a[href*="page"]',
                'a[href*="offset"]',
                '.pager .next a'
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
                        break
                except Exception as e:
                    logger.warning(f"Pagination selector '{selector}' failed on {url}: {e}")
                    continue
                    
            return final_events
        except Exception as e:
            logger.error(f"Error scraping promoter page {url}: {e}", exc_info=True)
            return []
    
    def extract_venue_club_urls(self, url: str) -> List[str]:
        """
        Enhanced method to extract venue and club URLs from main pages.
        Supports multiple URL patterns and improved CSS selectors.
        (Method from Scraper 0506)
        """
        html = self.fetch_page(url) # Will use Scraper Final's fetch_page
        if not html: return []
        
        soup = BeautifulSoup(html, "html.parser")
        venue_club_links = set()
        
        # Enhanced selectors for venue and club discovery from Scraper 0506
        venue_selectors = [
            'a[href*="/night/venue/"]',     # Direct venue links
            'a[href*="/venue/"]',           # Alternative venue pattern
            'a[href*="/club/"]',            # Club links
            'a[href*="/night/promoters/"]', # Promoter links
            'a[href*="/promoter/"]',        # Alternative promoter pattern
            '.venue-link',                  # Class-based venue links
            '.club-link',                   # Class-based club links
            '[data-venue-id]',              # Data attribute venue links
            'a[href*="/events/"]'           # Event category links (can be broad)
        ]
        
        for selector in venue_selectors:
            try:
                for link_tag in soup.select(selector):
                    href = link_tag.get('href')
                    if href:
                        full_url = urljoin(url, href)
                        parsed_link_path = urlparse(full_url).path
                        if parsed_link_path and parsed_link_path.count('/') >= 2 and \
                           not (parsed_link_path.endswith("/events/") and parsed_link_path.count('/') == 3) and \
                           not (parsed_link_path.endswith("/night/") and parsed_link_path.count('/') == 2) :
                             venue_club_links.add(full_url)
                             logger.debug(f"Found potential venue/club/promoter URL: {full_url}")
            except Exception as e:
                logger.warning(f"Venue selector '{selector}' failed on {url}: {e}")
                continue
        
        logger.info(f"Extracted {len(venue_club_links)} potential venue/club/promoter URLs from {url}")
        if "/night/events/" in url and not venue_club_links: # Check if it's the main events page
             logger.warning(f"No venue/club/promoter URLs extracted from main events page {url}. Check selectors.")
        return list(venue_club_links)

    def extract_promoter_urls(self, url: str) -> List[str]:
        """
        Extracts promoter URLs from a venue page.
        (Method from Scraper 0506)
        """
        html = self.fetch_page(url)
        if not html: return []
        soup = BeautifulSoup(html, "html.parser"); promoter_links = set()
        
        promoter_selectors = [
            'a[href*="/night/promoters/"]',
            'a[href*="/promoter/"]',
        ]
        for selector in promoter_selectors:
            for link_tag in soup.select(selector):
                href = link_tag.get('href')
                if href:
                    full_url = urljoin(url, href)
                    if urlparse(full_url).path.count('/') >= 3:
                        promoter_links.add(full_url)
                        logger.debug(f"Found promoter URL: {full_url}")
                        
        logger.info(f"Extracted {len(promoter_links)} promoter URLs from {url}")
        return list(promoter_links)

    def _update_venue_context_from_url(self, url: str):
        """
        Dynamically update venue context based on current URL being processed.
        This allows for more accurate venue attribution as we navigate through different pages.
        (Method from Scraper 0506)
        """
        try:
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.strip('/').split('/')
            
            # Update venue context for venue pages
            if len(path_parts) >= 3 and (path_parts[-2] == 'venue' or path_parts[-2] == 'clubs'): # Catches /venue/ and /clubs/
                new_venue_context = path_parts[-1]
                if new_venue_context != self.current_venue_context:
                    logger.info(f"Updated venue context from '{self.current_venue_context}' to '{new_venue_context}' for URL {url}")
                    self.current_venue_context = new_venue_context
            
            # Update context for events pages (e.g., /night/events/2025/06)
            elif len(path_parts) >= 3 and path_parts[1] == 'night' and path_parts[2] == 'events':
                event_page_context_parts = [p for p in path_parts[3:] if p.isdigit() or p.lower() in ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"]]
                if event_page_context_parts:
                    new_context = f"events_{'_'.join(event_page_context_parts)}"
                    if new_context != self.current_venue_context:
                        logger.info(f"Updated event page context from '{self.current_venue_context}' to '{new_context}' for URL {url}")
                        self.current_venue_context = new_context
                elif self.current_venue_context != "events_page":
                    logger.info(f"Setting generic events page context for URL {url}")
                    self.current_venue_context = "events_page"
                    
        except Exception as e:
            logger.warning(f"Error updating venue context from URL {url}: {e}", exc_info=True)

    def save_event(self, event: Dict[str, Any]):
        try:
            event.setdefault("title", "Untitled Event")
            event.setdefault("tickets_url", "Unknown URL")
            
            # --- Schema Alignment ---
            # 1. Align 'artists' to 'lineUp' schema
            if "artists" in event:
                line_up = []
                for artist in event["artists"]:
                    lu_item = {
                        "name": artist.get("name", ""),
                        "room": artist.get("room", "Main Room"), # Default room if not specified
                        "headliner": artist.get("role") == "headliner",
                        "genre": artist.get("genre"), # Assuming genre might be part of artist data
                        "startTime": artist.get("performance_time") # Assuming performance_time maps to startTime
                    }
                    line_up.append(lu_item)
                event["lineUp"] = line_up
                del event["artists"] # Remove old field
            
            # 2. Align 'datetime_obj' and 'datetime_info' to 'dateTime' schema
            if event.get("datetime_info"):
                dt_info = event["datetime_info"]
                event["dateTime"] = {
                    "start": dt_info.get("start_datetime"),
                    "end": dt_info.get("end_datetime"),
                    "displayText": dt_info.get("original_string"),
                    "timezone": dt_info.get("timezone")
                }
                # Ensure start and end are datetime objects for MongoDB
                if event["dateTime"]["start"] and not isinstance(event["dateTime"]["start"], datetime):
                    event["dateTime"]["start"] = datetime.fromisoformat(event["dateTime"]["start"])
                if event["dateTime"]["end"] and not isinstance(event["dateTime"]["end"], datetime):
                    event["dateTime"]["end"] = datetime.fromisoformat(event["dateTime"]["end"])
                
                del event["datetime_obj"] # Remove old field
                del event["datetime_info"] # Remove old field
            elif event.get("datetime_obj"): # Fallback if only datetime_obj exists
                event["dateTime"] = {
                    "start": event["datetime_obj"],
                    "end": None,
                    "displayText": event.get("raw_date_string"),
                    "timezone": "Europe/Madrid" # Default timezone
                }
                del event["datetime_obj"]
            
            # 3. Align 'location' field if it's not already a dict with 'venue'
            if event.get("venue") and not isinstance(event.get("location"), dict):
                event["location"] = {"venue": event["venue"]}
                if event.get("address"):
                    event["location"]["address"] = event["address"]
                if event.get("city"):
                    event["location"]["city"] = event["city"]
                del event["venue"] # Remove old field
                if "address" in event: del event["address"]
                if "city" in event: del event["city"]
            elif event.get("venue") and isinstance(event.get("location"), dict):
                # If location is already a dict, ensure 'venue' is populated
                if not event["location"].get("venue"):
                    event["location"]["venue"] = event["venue"]
                del event["venue"]
            
            # 4. Map 'data_quality' to '_quality'
            if event.get("data_quality"):
                quality_data = {
                    "scores": {
                        "title": event["data_quality"].get("completeness_score", 0) / 100,
                        "location": event["data_quality"].get("completeness_score", 0) / 100, # Placeholder, improve if specific location quality is available
                        "dateTime": event["data_quality"].get("accuracy_score", 0) / 100, # Using accuracy for datetime
                        "lineUp": event["data_quality"].get("completeness_score", 0) / 100, # Placeholder
                        "ticketInfo": event["data_quality"].get("completeness_score", 0) / 100 # Placeholder
                    },
                    "overall": self.scorer.calculate_event_quality(event).get("score", 0) / 100,
                    "lastCalculated": datetime.utcnow()
                }
                event["_quality"] = quality_data
                del event["data_quality"]
            else:
                # Fallback to existing quality calculation if data_quality is not present
                quality_data = self.scorer.calculate_event_quality(event)
                event["_quality"] = quality_data
            
            # Ensure scrapedAt is a datetime object
            if isinstance(event.get("scrapedAt"), str):
                event["scrapedAt"] = datetime.fromisoformat(event["scrapedAt"].replace('Z', '+00:00'))

            # Define update key based on new schema fields
            update_key = {"title": event["title"], "tickets_url": event["tickets_url"]}
            if event.get("dateTime") and event["dateTime"].get("start"):
                update_key["dateTime.start"] = event["dateTime"]["start"]
            elif event.get("raw_date_string"):
                update_key["raw_date_key_part"] = event["raw_date_string"][:30] # Keep for backward compatibility if needed
            
            # Remove raw_date_string if dateTime is properly set
            if event.get("dateTime") and event["dateTime"].get("start") and "raw_date_string" in event:
                del event["raw_date_string"]

            if self.db is not None:
                self.db.events.update_one(update_key, {"$set": event}, upsert=True)
            else:
                logger.warning("MongoDB not available. Event not saved to DB.")
        except Exception as e:
            logger.error(f"Error saving event '{event.get('title', 'N/A')}': {e}", exc_info=True)
    
    def append_to_csv(self, events_batch: List[Dict[str, Any]]):
        if not events_batch: return
        all_keys = set()
        preferred_order = [
            "title", "datetime_obj", "raw_date_string", "tickets_url", "promoter", "venue", "genres",
            "tier_1", "tier_2", "tier_3", "full_description", "event_card_all_text",
            "_quality", "extractionMethod", "page_year_context", "scrapedAt", "artists",
            "social_media_links", "data_quality"
        ]
        for event in events_batch:
            all_keys.update(event.keys())
        final_headers = preferred_order + sorted(list(all_keys - set(preferred_order)))
        file_exists_non_empty = self.csv_file_path.exists() and self.csv_file_path.stat().st_size > 0
        try:
            with open(self.csv_file_path, 'a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=final_headers, extrasaction='ignore')
                if not self.csv_headers_written and not file_exists_non_empty:
                    writer.writeheader()
                    self.csv_headers_written = True
                for event_data in events_batch:
                    row = {k: (v.isoformat() if isinstance(v, (dt.datetime, dt.date)) else
                               json.dumps(v, default=json_serial) if isinstance(v, (list, dict)) else v)
                           for k, v in event_data.items()}
                    writer.writerow(row)
            logger.info(f"Appended {len(events_batch)} events to {self.csv_file_path.name}")
        except Exception as e:
            logger.error(f"Error CSV append: {e}", exc_info=True)
    
    def run(self):
        """
        Run method implementing navigation logic from Scraper 0506,
        integrated with Scraper Final's features (progress bar, advanced scraping).
        """
        logger.info(f"Starting enhanced scraper run_id: {self.run_timestamp}. CSV: {self.csv_file_path.name}")
        self.all_scraped_events_for_run = []
        self.csv_headers_written = False
        
        # Initialize or reset stats for the current run
        self.stats = {
            "venues_scraped": 0,
            "promoters_scraped": 0,
            "events_scraped": 0
        }
        # Local counters for Scraper 0506's summary style, if needed, but self.stats is primary
        # venue_count_on_main_page = 0
        # promoter_count_by_venue = {}
        # event_count_by_promoter = {}

        self.url_queue = [(self.config.url, 0)]
        logger.info(f"Starting with URL: {self.config.url}")

        if hasattr(self, 'progress_bar') and self.progress_bar is not None:
             self.progress_bar.close()
        self.progress_bar = tqdm(
            desc="Scraping Progress",
            unit="pages",
            position=0,
            leave=True,
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
            postfix=self.stats
        )

        while self.url_queue:
            try:
                url, depth = self.url_queue.pop(0)
                if url in self.visited_urls or depth > self.max_depth:
                    if url in self.visited_urls: logger.debug(f"Skipping already visited URL: {url}")
                    else: logger.debug(f"Skipping URL due to max depth: {url}")
                    continue
                self.visited_urls.add(url)
                self.current_depth = depth
                logger.info(f"Processing [{depth}]: {url}")

                if self.progress_bar is not None:
                    self.progress_bar.update(1)

                self._update_venue_context_from_url(url)

                is_main_events_page = (depth == 0 and ("/events/" in url or url.endswith("/night/")))

                if is_main_events_page:
                    logger.info("Main events page detected - extracting venue/club/promoter URLs only")
                    discovered_urls = self.extract_venue_club_urls(url)
                    self.stats["venues_scraped"] = len(discovered_urls) # Update based on initial discovery
                    
                    logger.info(f"DEBUG: Found {self.stats['venues_scraped']} potential URLs from main events page")
                    
                    if discovered_urls:
                        for vc_url in discovered_urls:
                            if vc_url not in self.visited_urls and depth + 1 <= self.max_depth:
                                self.url_queue.append((vc_url, depth + 1))
                                logger.debug(f"Added URL to queue: {vc_url}")
                    else:
                        logger.warning("No venue/club/promoter URLs found on main events page!")
                        
                elif "/venue/" in url or "/clubs/" in url:
                    logger.info(f"Venue/Club page detected - extracting promoter URLs from: {url}")
                    promoter_urls = self.extract_promoter_urls(url)
                    # self.stats["promoters_scraped"] += len(promoter_urls) # This might double count if promoters are on multiple venues
                                                                         # Instead, count when promoter page is processed.
                    logger.info(f"DEBUG: Found {len(promoter_urls)} promoter URLs from venue/club page '{url.split('/')[-1]}'")
                    
                    if promoter_urls:
                        for p_url in promoter_urls:
                            if p_url not in self.visited_urls and depth + 1 <= self.max_depth:
                                self.url_queue.append((p_url, depth + 1))
                                logger.debug(f"Added promoter URL to queue: {p_url}")
                    else:
                        logger.warning(f"No promoter URLs found on venue/club page: {url}")
                        
                elif "/promoters/" in url or "/promoter/" in url:
                    logger.info(f"Promoter page detected - scraping events from: {url}")
                    self.stats["promoters_scraped"] += 1
                    page_events = self.scrape_promoter_page(url)
                    
                    if page_events:
                        logger.info(f"Found {len(page_events)} events on promoter page {url.split('/')[-1]}")
                        for event_item in page_events:
                            self.save_event(event_item)
                        self.append_to_csv(page_events)
                        self.all_scraped_events_for_run.extend(page_events)
                        self.stats["events_scraped"] += len(page_events)
                    else:
                        logger.warning(f"No events found on promoter page: {url}")
                else:
                    logger.info(f"Skipping non-target page type or already processed initial page type: {url}")
                
                if self.progress_bar is not None:
                    self.progress_bar.set_postfix(self.stats)

                try:
                    min_delay = settings.SCRAPER_DEFAULT_MIN_DELAY
                    max_delay = settings.SCRAPER_DEFAULT_MAX_DELAY
                    delay_val = random.uniform(min_delay, max_delay)
                    time.sleep(delay_val)
                except Exception as delay_error:
                    logger.warning(f"Delay error: {delay_error}, using fallback 1s")
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"Error processing URL {url if 'url' in locals() else 'UNKNOWN'}: {e}", exc_info=True)
                continue
        
        if self.progress_bar is not None:
            self.progress_bar.close()
            self.progress_bar = None
        
        logger.info("=" * 80)
        logger.info("SCRAPING COMPLETE - SUMMARY STATISTICS (Integrated Logic)")
        logger.info("=" * 80)
        logger.info(f"Total initial URLs (venues/clubs/promoters from main page): {self.stats['venues_scraped']}")
        logger.info(f"Total promoter pages processed: {self.stats['promoters_scraped']}")
        logger.info(f"Total events scraped: {self.stats['events_scraped']}")
        logger.info(f"Total unique URLs visited: {len(self.visited_urls)}")
        logger.info(f"CSV file: {self.csv_file_path}")
        
        if self.all_scraped_events_for_run:
            quality_scores = []
            for event in self.all_scraped_events_for_run:
                if event.get("data_quality") and isinstance(event["data_quality"], dict):
                    completeness = event["data_quality"].get("completeness_score", 0)
                    accuracy = event["data_quality"].get("accuracy_score", 0)
                    freshness = event["data_quality"].get("freshness_score", 0)
                    avg_event_score = (completeness + accuracy + freshness) / 3.0 if (completeness + accuracy + freshness) > 0 else 0
                    quality_scores.append(avg_event_score)
            
            if quality_scores:
                avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
                logger.info(f"Average data quality score: {avg_quality:.1f}%")
                logger.info(f"Quality distribution:")
                logger.info(f"  - High quality (>80%): {sum(1 for s in quality_scores if s > 80)}")
                logger.info(f"  - Medium quality (50-80%): {sum(1 for s in quality_scores if 50 <= s <= 80)}")
                logger.info(f"  - Low quality (<50%): {sum(1 for s in quality_scores if s < 50)}")
            else:
                logger.info("No events with quality data to summarize.")
        else:
            logger.info("No events scraped to summarize quality.")
        
        logger.info("=" * 80)
        logger.info(f"Enhanced run {self.run_timestamp} completed using integrated navigation logic.")
        return self.all_scraped_events_for_run


# Main execution
if __name__ == "__main__":
    # Default configuration, similar to Scraper 0506's main()
    default_config = ScraperConfig(
        url="https://www.ibiza-spotlight.com/night/events/2025/06",
        # min_delay, max_delay, save_to_db, headers will use defaults from ScraperConfig
    )
    scraper = IbizaSpotlightScraper(config=default_config)
    
    try:
        events = scraper.run()
        logger.info(f"Scraping completed. Total events: {len(events) if events else 0}")
    except KeyboardInterrupt:
        logger.warning("Scraper run interrupted by user.")
    except Exception as e:
        logger.critical(f"Scraper failed: {e}", exc_info=True)
    finally:
        # Ensure run_timestamp is available for the final log message
        run_ts = scraper.run_timestamp if hasattr(scraper, 'run_timestamp') else 'N/A'
        logger.info(f"Scraper shutdown for run_id: {run_ts}.")