#!/usr/bin/env python3
"""
ClubTickets.com Scraper using Requests (Headless)
A lightweight alternative to browser-based scraping for static content
"""

import re
import random
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ClubTicketsRequestsScraper:
    """Scrapes ClubTickets.com using requests and BeautifulSoup"""
    
    def __init__(self):
        self.session = requests.Session()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        ]
        self.rotate_user_agent()
        
    def rotate_user_agent(self):
        """Rotate user agent for request diversity"""
        self.session.headers.update({
            "User-Agent": random.choice(self.user_agents),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a page"""
        try:
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
            
    def extract_events(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        """Extract event URLs from page HTML"""
        event_urls = []
        event_cards = soup.select('.content-text-card a[href]')
        
        for card in event_cards:
            href = card.get('href')
            if href and '/event/' in href:
                full_url = urljoin(base_url, href)
                event_urls.append(full_url)
                
        return event_urls
        
    def crawl(self, start_url: str, max_pages: int = 10) -> List[str]:
        """
        Crawl ClubTickets.com to extract event URLs
        :param start_url: Starting URL for crawling
        :param max_pages: Maximum number of pages to process
        :return: List of event URLs
        """
        event_urls = []
        page_count = 0
        current_url = start_url
        
        while current_url and page_count < max_pages:
            soup = self.get_page(current_url)
            if not soup:
                break
                
            # Extract events from current page
            event_urls.extend(self.extract_events(soup, current_url))
            page_count += 1
            
            # Find next page link
            next_page = soup.select_one('a[rel="next"]')
            current_url = urljoin(current_url, next_page['href']) if next_page else None
            
            # Random delay between pages
            time.sleep(random.uniform(1.0, 2.5))
            
        return event_urls

# Example usage
if __name__ == "__main__":
    scraper = ClubTicketsRequestsScraper()
    event_urls = scraper.crawl(
        "https://www.clubtickets.com/search?dates=31%2F05%2F25+-+01%2F11%2F25",
        max_pages=10
    )
    
    print(f"\nFound {len(event_urls)} event URLs:")
    for url in event_urls[:10]:  # Print first 10 URLs
        print(f"- {url}")
    if len(event_urls) > 10:
        print(f"- ... and {len(event_urls) - 10} more")