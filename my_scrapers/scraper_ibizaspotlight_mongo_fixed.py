#!/usr/bin/env python3
import sys
import pathlib

# Add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
#!/usr/bin/env python3
"""
Enhanced Ibiza Spotlight Scraper with MongoDB Integration
"""

import os
import sys
import json
import time
import random
import re
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urljoin, urlparse
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

# --- Constants ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
]

# --- Dependency Imports ---
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors
from classy_skkkrapey.utils.html_to_md import html_to_md
from pymongo.database import Database
# Import quality scorer
sys.path.append(str(Path(__file__).parent.parent))
from database.quality_scorer import QualityScorer

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
                delay *= 1.5  # Exponential backoff
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
        
    def fetch_page(self, url: str) -> str:
        """Fetch page content with rate limiting and error handling"""
        try:
            logger.info(f"Fetching: {url}")
            time.sleep(random.uniform(self.config.min_delay, self.config.max_delay))
            response = self.session.get(url, timeout=20)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
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
    
    def scrape_promoter_page(self, url: str) -> List[Dict[str, Any]]:
        """Scrape event data from promoter page using Markdown parsing"""
        events = []
        try:
            html = self.fetch_page(url)
            md_content = html_to_md(html)
            
            # Extract JSON-LD events from Markdown
            json_ld_events = self.extract_json_ld_from_md(md_content)
            events.extend(json_ld_events)
            
            # If no JSON-LD found, try to extract basic event info
            if not events:
                event_data = {
                    "title": "Event at " + url,
                    "date": "",
                    "description": md_content[:500] + "..." if len(md_content) > 500 else md_content,
                    "promoterUrl": url,
                    "scrapedAt": datetime.utcnow().isoformat() + "Z",
                    "extractionMethod": "markdown-fallback"
                }
                events.append(event_data)
                
        except Exception as e:
            logger.error(f"Error scraping promoter page {url} with Markdown: {e}")
        
        return events
        
    def extract_json_ld_from_md(self, md_content: str) -> List[Dict[str, Any]]:
        """Extract JSON-LD event data from Markdown content"""
        events = []
        pattern = r'```json(.+?)```'
        matches = re.findall(pattern, md_content, re.DOTALL)
        
        for match in matches:
            try:
                data = json.loads(match.strip())
                if isinstance(data, list):
                    for item in data:
                        if item.get("@type") == "Event":
                            events.append(self.parse_json_ld_event(item))
                elif isinstance(data, dict) and data.get("@type") == "Event":
                    events.append(self.parse_json_ld_event(data))
            except json.JSONDecodeError:
                continue
                
        return events
        
    def parse_json_ld_event(self, event_data: dict) -> Dict[str, Any]:
        """Parse JSON-LD event data into our format"""
        return {
            "title": event_data.get("name", ""),
            "date": event_data.get("startDate", ""),
            "description": event_data.get("description", ""),
            "location": event_data.get("location", {}).get("name", "") if isinstance(event_data.get("location"), dict) else "",
            "lineup": [performer["name"] for performer in event_data.get("performer", [])],
            "promoterUrl": event_data.get("url", ""),
            "scrapedAt": datetime.utcnow().isoformat() + "Z",
            "extractionMethod": "json-ld"
        }
    
    def scrape_event_card(self, card) -> Dict[str, Any]:
        """Extract event details from card element with robust selectors"""
        try:
            # Extract title using multiple selectors
            title = ""
            for selector in ['.event-title', '.title', 'h3', 'h4', 'h2', 'h1', '.event-name', '.name']:
                elem = card.select_one(selector)
                if elem:
                    title = elem.get_text(strip=True)
                    break
            
            # Extract date using multiple selectors
            date_str = ""
            for selector in ['.event-date', '.date', '.time', 'time', '.event-time', '.start-date']:
                elem = card.select_one(selector)
                if elem:
                    date_str = elem.get('datetime', elem.get_text(strip=True))
                    break
            
            # Extract description using multiple selectors
            description = ""
            for selector in ['.event-description', '.description', '.event-desc', '.desc', 'p']:
                elem = card.select_one(selector)
                if elem:
                    description = elem.get_text(strip=True)
                    break
            
            # Extract lineup if available
            lineup = []
            for artist in card.select('.artist, .lineup-artist, .event-artist'):
                lineup.append(artist.get_text(strip=True))
            
            return {
                "title": title,
                "date": date_str,
                "description": description,
                "lineup": lineup,
                "scrapedAt": datetime.utcnow().isoformat() + "Z",
                "extractionMethod": "html-dynamic"
            }
        except Exception as e:
            logger.error(f"Error scraping event card: {e}")
            return None
    
    def save_event(self, event: Dict[str, Any]):
        """Save event data to MongoDB with quality scoring"""
        try:
            # Calculate quality score
            quality_data = self.scorer.calculate_event_quality(event)
            event["_quality"] = quality_data
            
            # Save to MongoDB
            self.db.events.update_one(
                {"title": event["title"], "date": event["date"], "promoterUrl": event["promoterUrl"]},
                {"$set": event},
                upsert=True
            )
            logger.info(f"Saved event: {event['title']}")
        except Exception as e:
            logger.error(f"Error saving event: {e}")
    
    def scrape_venue_events(self, url: str) -> List[Dict[str, Any]]:
        """Scrape events directly from venue page with pagination"""
        events = []
        current_url = url
        
        while current_url:
            if current_url in self.visited_urls:
                break
                
            self.visited_urls.add(current_url)
            try:
                html = self.fetch_page(current_url)
                soup = BeautifulSoup(html, "html.parser")
                
                # Extract event listings
                event_listings = soup.select('.event-listing, .event-item, .views-row, .event-card')
                
                for listing in event_listings:
                    event_data = self.scrape_event_card(listing)
                    if event_data:
                        event_data["venueUrl"] = current_url
                        events.append(event_data)
                
                # Handle pagination
                next_link = soup.select_one('a.pagination-next[href], a.next[href], a.page-next[href]')
                current_url = urljoin(current_url, next_link['href']) if next_link else None
                
                # Anti-bot delay
                time.sleep(random.uniform(1.0, 2.5))
                
            except Exception as e:
                logger.error(f"Error scraping venue page {current_url}: {e}")
                break
                
        return events

    def run(self):
        """Main execution method"""
        logger.info("Starting Ibiza Spotlight scraper")
        venue_url = self.config.url
        
        # Scrape events directly from venue page
        events = self.scrape_venue_events(venue_url)
        logger.info(f"Found {len(events)} events on venue page")
        
        # Save events
        for event in events:
            self.save_event(event)
        
        logger.info("Event scraping completed successfully")

# --- Main Execution ---
def main():
    """Main function to run the scraper"""
    config = ScraperConfig(
        url="https://www.ibiza-spotlight.com/night/venue/unvrs",
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
    try:
        main()
        # Generate sitemaps after successful scraping
        urls = get_sitemap_urls()
        xml_content = generate_xml_sitemap(urls)
        html_content = generate_html_sitemap(urls)
        
        with open("../output/sitemap.xml", "w") as f:
            f.write(xml_content)
        with open("../output/sitemap.html", "w") as f:
            f.write(html_content)
            
        print(f"Generated sitemaps with {len(urls)} URLs")
    except Exception as e:
        print(f"Sitemap generation failed: {str(e)}")