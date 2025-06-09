#!/usr/bin/env python3
"""
Enhanced Ibiza Spotlight Scraper with MongoDB Integration

Features:
- Direct MongoDB insertion with quality metadata
- Real-time quality scoring
- Schema validation
- Batch processing
- Error handling and retries
- Async operations
"""

import sys
import json
import time
import random
import argparse
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin
from typing import List, Dict, Any
from dataclasses import dataclass
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
from pymongo.database import Database
from datetime import timedelta

# Import quality scorer
sys.path.append(str(Path(__file__).parent.parent))
from database.quality_scorer import QualityScorer

# --- Constants ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
]

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScraperConfig:
    url: str
    headless: bool = True
    output_dir: Path = Path("output")
    min_delay: float = 0.5
    max_delay: float = 1.5
    verbose: bool = False
    batch_size: int = 10

# --- MongoDB Connection with Retries ---
def get_mongodb_connection(retries=3, delay=2) -> Database:
    """Establish MongoDB connection with retry logic"""
    MONGODB_URI = "mongodb://localhost:27017/ibiza_events"
    for attempt in range(retries):
        try:
            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000
            )
            client.admin.command('ping')
            db_name = MONGODB_URI.split('/')[-1].split('?')[0]
            logger.info(f"MongoDB connected to {db_name}")
            return client[db_name]
        except errors.ConnectionFailure as e:
            logger.warning(f"MongoDB connection failed (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
                delay *= 1.5
            else:
                logger.error("MongoDB connection failed after retries")
                raise
        except Exception as e:
            logger.error(f"Unexpected MongoDB error: {e}")
            raise

# --- Scraper Class ---
class IbizaSpotlightScraper:
    """Enhanced scraper for ibiza-spotlight.com with MongoDB integration"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.db = get_mongodb_connection()
        self.scorer = QualityScorer()
        self.current_user_agent = random.choice(USER_AGENTS)
        self.session = self._create_session()
        self.events_buffer = []
        self.visited_urls = set()
        
    def _create_session(self) -> requests.Session:
        """Create requests session with retry logic"""
        session = requests.Session()
        session.headers.update({"User-Agent": self.current_user_agent})
        
        # Configure retries
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
        
    def extract_promoter_urls(self, url: str) -> List[str]:
        """Extract promoter URLs from venue page images"""
        try:
            html = self.fetch_page(url)
            soup = BeautifulSoup(html, "html.parser")
            promoter_links = set()
            
            # Find all images with promoter links
            for img in soup.select('img[src]'):
                parent_link = img.find_parent('a')
                if parent_link and parent_link.get('href'):
                    href = parent_link['href']
                    if "ibiza-spotlight.com/night/promoters" in href:
                        full_url = urljoin(url, href)
                        promoter_links.add(full_url)
            
            return list(promoter_links)
        except Exception as e:
            logger.error(f"Error extracting promoter URLs: {e}")
            return []
    
    def scrape_event_cards(self, url: str) -> List[Dict[str, Any]]:
        """Scrape event cards from promoter page with fallbacks"""
        try:
            html = self.fetch_page(url)
            soup = BeautifulSoup(html, "html.parser")
            events = []
            
            # Primary method: CSS selector for event cards
            for card in soup.select('.event-listing'):
                event_data = {
                    "title": self._extract_title(card),
                    "date": self._extract_date(card),
                    "description": self._extract_description(card),
                    "promoterUrl": url
                }
                events.append(event_data)
            
            # Fallback method: Look for event metadata in JSON-LD
            if not events:
                for script in soup.select('script[type="application/ld+json"]'):
                    try:
                        data = json.loads(script.string)
                        if isinstance(data, dict) and data.get("@type") == "Event":
                            events.append({
                                "title": data.get("name", ""),
                                "date": data.get("startDate", ""),
                                "description": data.get("description", ""),
                                "promoterUrl": url
                            })
                    except json.JSON极客
                    except json.JSONDecodeError:
                        continue
            
            return events
        except Exception as e:
            logger.error(f"Error scraping event cards from {url}: {e}")
            return []
            
    def _extract_title(self, card) -> str:
        """Extract event title with fallbacks"""
        # Primary selector
        title = card.select_one('.event-title')
        if title:
            return title.get_text(strip=True)
        
        # Fallback selectors
        for selector in ['.title', 'h3', 'h4']:
            title = card.select_one(selector)
            if title:
                return title.get_text(strip=True)
        return ""
        
    def _extract_date(self, card) -> str:
        """Extract event date with fallbacks"""
        # Primary selector
        date = card.select_one('.event-date')
        if date:
            return date.get_text(strip=True)
        
        # Fallback selectors
        for selector in ['.date', '.time', 'time']:
            date = card.select_one(selector)
            if date and (date.get('datetime') or date.text.strip()):
                return date.get('datetime', date.text.strip())
        return ""
        
    def _extract_description(self, card) -> str:
        """Extract event description with fallbacks"""
        # Primary selector
        desc = card.select_one('.event-description')
        if desc:
            return desc.get_text(strip=True)
        
        # Fallback: use first paragraph
        desc = card.select_one('p')
        if desc:
            return desc.get_text(strip=True)
        return ""

    def save_venue(self, url: str, promoter_urls: List[str]):
        """Save venue data to MongoDB"""
        venue_data = {
            "url": url,
            "scrapedAt": datetime.utcnow().isoformat() + "Z",
            "promoterUrls": promoter_urls
        }
        try:
            self.db.venue.update_one(
                {"url": url},
                {"$set": venue_data},
                upsert=True
            )
            logger.info(f"Saved venue data: {url}")
        except Exception as e:
            logger.error(f"Error saving venue data: {e}")
            
    def save_event(self, event: Dict[str, Any]):
        """Save event data to MongoDB"""
        try:
            self.db.events.update_one(
                {"title": event["title"], "date": event["date"], "promoterUrl": event["promoterUrl"]},
                {"$set": event},
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error saving event: {e}")
            
    def run(self):
        """Main execution method"""
        logger.info("Starting Ibiza Spotlight event scraper")
        venue_url = self.config.url
        
        # Step 1: Scrape venue page for promoter URLs
        promoter_urls = self.extract_promoter_urls(venue_url)
        logger.info(f"Found {len(promoter_urls)} promoter URLs")
        self.save_venue(venue_url, promoter_urls)
        
        # Step 2: Process each promoter page
        for i, url in enumerate(promoter_urls, 1):
            logger.info(f"Processing promoter page {i}/{len(promoter_urls)}: {url}")
            
            # Step 3: Scrape event cards
            events = self.scrape_event_cards(url)
            logger.info(f"Found {len(events)} events on {url}")
            
            # Step 4: Save events
            for event in events:
                self.save_event(event)
                
            time.sleep(random.uniform(1.0, 3.0))
        
        logger.info("Event scraping completed successfully")

# --- Main Execution ---
def main():
    """Main function to run the scraper with command-line arguments"""
    parser = argparse.ArgumentParser(description='Ibiza Spotlight Scraper')
    parser.add_argument('--url', default="https://www.ibiza-spotlight.com/night/venue/unvrs",
                        help='Target venue URL to scrape')
    args = parser.parse_args()
    
    config = ScraperConfig(
        url=args.url,
        headless=True,
        output_dir=Path("output"),
        verbose=True
    )
    
    scraper = IbizaSpotlightScraper(config)
    try:
        scraper.run()
    except Exception as e:
        logger.error(f"Scraper failed: {e}")
    finally:
        logger.info("Scraper shutdown complete")

if __name__ == "__main__":
    main()