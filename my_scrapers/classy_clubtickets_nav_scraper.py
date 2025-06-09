#!/usr/bin/env python3
"""
Advanced ClubTickets.com Scraper with Enhanced Navigation and Stealth Features

This scraper implements robust navigation logic and human-like interaction patterns
specifically for clubtickets.com, incorporating techniques from club_tickets_crawl_logic.py
"""

import re
import random
import logging
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
from typing import List, Optional, Dict, Any
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configuration Parameters ---
CONFIG = {
    "headless": True,  # Run browser in headless mode by default
    "slow_mo": 0,       # Slows down Playwright operations (milliseconds)
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "viewport_width": 1366,
    "viewport_height": 768,
    "max_retries": 3,      # Max attempts for an action before failing
    "retry_delay_sec": 0.5, # Delay between retries (seconds)
    
    # Randomized delays for human-like interaction (seconds)
    "random_short_delay_sec_min": 0.5,
    "random_short_delay_sec_max": 1.5,
    "random_long_delay_sec_min": 2,
    "random_long_delay_sec_max": 4,
    
    # XPath selectors for clubtickets.com
    # More specific selector for "Show more events" button
    "show_more_xpath": "//button[contains(concat(' ', normalize-space(@class), ' '), ' btn-more-events ') and contains(concat(' ', normalize-space(@class), ' '), ' more-events ') and text()='Show more events']",
    "date_tab_xpath": "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"btn-custom-day-tab\", \" \" ))]"
}

class ClubTicketsScraper:
    """Advanced scraper for clubtickets.com with enhanced navigation and stealth features"""
    
    def __init__(self, headless: bool = True):
        self.config = {**CONFIG, "headless": headless}
        self.playwright = None
        self.browser = None
        self.page = None
        
    def __enter__(self):
        """Context manager entry"""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.config["headless"],
            slow_mo=self.config["slow_mo"],
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        self.page = self.browser.new_page(
            user_agent=self.config["user_agent"],
            viewport={"width": self.config["viewport_width"], "height": self.config["viewport_height"]}
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        if self.page:
            self.page.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
            
    def retry_action(self, action, description, is_critical=True):
        """
        Attempts to perform an action with retries and delays
        :param action: Callable to execute
        :param description: Action description for logging
        :param is_critical: Whether failure is critical
        :return: True if succeeded, False otherwise
        """
        for attempt in range(1, self.config["max_retries"] + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.config['max_retries']}: {description}...")
                action()
                logger.info(f"Successfully performed: {description}")
                return True
            except PlaywrightTimeoutError as e:
                logger.warning(f"Timeout on attempt {attempt} for {description}: {e}")
                if attempt < self.config["max_retries"]:
                    time.sleep(self.config["retry_delay_sec"])
            except Exception as e:
                logger.warning(f"Error on attempt {attempt} for {description}: {e}")
                if attempt < self.config["max_retries"]:
                    time.sleep(self.config["retry_delay_sec"])
        
        if is_critical:
            logger.critical(f"Failed to perform {description} after {self.config['max_retries']} attempts")
        else:
            logger.error(f"Failed to perform {description} after {self.config['max_retries']} attempts")
        return False
    
    def random_delay(self, short=True):
        """Apply random delay for human-like behavior"""
        if short:
            min_delay = self.config["random_short_delay_sec_min"]
            max_delay = self.config["random_short_delay_sec_max"]
        else:
            min_delay = self.config["random_long_delay_sec_min"]
            max_delay = self.config["random_long_delay_sec_max"]
            
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
        
    def navigate_to(self, url: str):
        """Navigate to URL with retry logic and human-like delays"""
        success = self.retry_action(
            lambda: self.page.goto(url, wait_until="domcontentloaded", timeout=60000),
            f"Navigate to {url}"
        )
        if success:
            # Handle cookies immediately after navigation
            self.handle_cookie_popup()
        return success
        
    def handle_cookie_popup(self):
        """Detect and close cookie consent popup if present"""
        try:
            # Wait for page to stabilize after navigation
            self.page.wait_for_timeout(1500)
            
            # Common cookie consent selectors
            selectors = [
                'button#cookie-accept',  # ID-based
                'button.cookie-accept',  # Class-based
                'button:has-text("Accept")',  # Text-based
                'button:has-text("Agree")',
                'button:has-text("OK")',
                'button:has-text("I agree")',
                'button:has-text("Consent")',
                'button:has-text("Accept all")',
                'button:has-text("Accept cookies")'
            ]
            
            for selector in selectors:
                try:
                    if self.page.locator(selector).is_visible(timeout=3000):
                        self.retry_action(
                            lambda: self.page.locator(selector).click(timeout=5000),
                            f"Click cookie consent: {selector}",
                            is_critical=False
                        )
                        logger.info(f"Closed cookie popup using: {selector}")
                        self.random_delay()
                        return True
                except Exception:
                    continue
                    
            logger.debug("No cookie popup detected")
            return False
        except Exception as e:
            logger.error(f"Error handling cookie popup: {e}")
            return False
    
    def crawl_events(self, url: str, max_pages: int = 10) -> List[str]:
        """
        Crawl clubtickets.com to extract event URLs using advanced navigation
        :param url: Starting URL for crawling
        :param max_pages: Maximum number of date pages to process
        :return: List of event URLs
        """
        event_urls = []
        
        # 1. Navigate to target URL and handle cookies
        if not self.navigate_to(url):
            return event_urls
            
        # 2. Ensure cookie popup was handled
        if not self.handle_cookie_popup():
            logger.info("Re-checking for cookie popup after initial handling")
            self.handle_cookie_popup()
            
        # Apply initial delay after navigation and cookie handling
        self.random_delay(short=False)
        
        # 2. Optimized scrolling to ensure all elements are visible
        # First scroll: Slightly shorter scroll (300-400px)
        scroll1 = random.randint(300, 400)
        self.page.evaluate(f"window.scrollBy(0, {scroll1})")
        logger.info(f"Initial scroll: {scroll1}px")
        self.random_delay()
        
        # Second scroll: Additional 400-500px
        scroll2 = random.randint(400, 500)
        self.page.evaluate(f"window.scrollBy(0, {scroll2})")
        logger.info(f"Secondary scroll: {scroll2}px")
        self.random_delay()
        
        # Third scroll: Final 200-300px
        scroll3 = random.randint(200, 300)
        self.page.evaluate(f"window.scrollBy(0, {scroll3})")
        logger.info(f"Final scroll: {scroll3}px")
        
        # Additional pause before interacting with elements
        time.sleep(1)
        
        # 3. Click "Show more events" if available (using class-based selector)
        try:
            # Use class-based selector as per the JavaScript snippet
            show_more_button = self.page.locator('button.btn-more-events.more-events')
            if show_more_button.is_visible(timeout=5000):
                time.sleep(1)  # Additional pause before interaction
                if self.retry_action(
                    lambda: show_more_button.click(timeout=10000),
                    "Click 'Show more events'"
                ):
                    logger.info("Clicked 'Show more events'")
                    
                    # Additional scroll after clicking to ensure all content loads
                    scroll_after = random.randint(300, 500)
                    self.page.evaluate(f"window.scrollBy(0, {scroll_after})")
                    logger.info(f"Post-click scroll: {scroll_after}px")
                    self.random_delay()
        except Exception as e:
            logger.error(f"Error locating 'Show more events' button: {e}")
        
        # 4. Process initial events
        event_urls.extend(self.process_current_events())
        
        # 5. Process date tabs with page limit
        date_tabs = self.page.locator(self.config["date_tab_xpath"]).all()
        logger.info(f"Found {len(date_tabs)} date tabs")
        
        # Calculate how many pages to process (max_pages + 1 because we already processed page 0)
        pages_to_process = min(len(date_tabs), max_pages + 1)
        
        for i in range(1, pages_to_process):
            date_tab = date_tabs[i]
            date_text = date_tab.text_content().strip()
            logger.info(f"Processing date {i}/{pages_to_process-1}: {date_text}")
            
            # Randomized horizontal scroll
            scroll_amount = random.randint(90, 120)
            self.page.evaluate(f"window.scrollBy({scroll_amount}, 0)")
            logger.info(f"Scrolled {scroll_amount}px horizontally")
            self.random_delay()
            
            # Scroll into view and click
            if self.retry_action(
                lambda: date_tab.scroll_into_view_if_needed(timeout=10000),
                f"Scroll date tab into view: {date_text}",
                is_critical=False
            ):
                if self.retry_action(
                    lambda: date_tab.click(timeout=10000),
                    f"Click date tab: {date_text}"
                ):
                    # Process events after date selection
                    event_urls.extend(self.process_current_events(date_text))
            
            # Long delay between dates
            self.random_delay(short=False)
            
        return event_urls
    
    def process_current_events(self, date_context="current") -> List[str]:
        """
        Extract event URLs from the current page using browser context
        :param date_context: Context for logging
        :return: List of event URLs
        """
        # Wait for event cards
        if not self.retry_action(
            lambda: self.page.wait_for_selector(
                ".content-text-card",
                state="visible",
                timeout=20000
            ),
            f"Wait for event cards ({date_context})",
            is_critical=False
        ):
            return []
            
        try:
            # Extract all event URLs in one browser context execution
            urls = self.page.evaluate('''() => {
                const cards = document.querySelectorAll('.content-text-card');
                const urls = [];
                
                cards.forEach(card => {
                    const link = card.querySelector('a');
                    if (link && link.href) {
                        // Use resolved absolute URL
                        urls.push(link.href);
                    }
                });
                
                return urls;
            }''')
            
            logger.info(f"Found {len(urls)} events for {date_context}")
            return urls
        except Exception as e:
            logger.error(f"Error extracting event URLs: {e}")
            return []

# Example usage
if __name__ == "__main__":
    with ClubTicketsScraper(headless=False) as scraper:
        event_urls = scraper.crawl_events(
            "https://www.clubtickets.com/search?dates=31%2F05%2F25+-+01%2F11%2F25",
            max_pages=10
        )
        print(f"Found {len(event_urls)} event URLs (max 10 pages processed):")
        for url in event_urls:
            print(f"- {url}")