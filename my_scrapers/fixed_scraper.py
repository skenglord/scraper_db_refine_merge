#!/usr/bin/env python3
"""
Enhanced Ibiza Spotlight Event Scraper (v4.0) - Professional Grade

This version integrates advanced scraping techniques for robustness and stealth:
- Rewritten parsing logic to match the site's row-based structure.
- `playwright-stealth` to avoid headless browser detection.
- Human-like interactions (mouse movements, clicks) to mimic user behavior.
- A robust, multi-layered overlay handler for cookie banners and pop-ups.
- Greatly improved accuracy for extracting Venue and Date information.
"""
import argparse
import csv
import json
import random
import re
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, date, time as dt_time
from pathlib import Path
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from playwright.sync_api import sync_playwright, Page, Browser, Route, Locator
    from playwright_stealth import stealth_sync
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    sync_playwright, Page, Browser, Route, Locator, stealth_sync = (None,) * 6
    PLAYWRIGHT_AVAILABLE = False

# --- Configuration ---
SNAPSHOT_DIR = Path("debug_snapshots")
OUTPUT_DIR = Path("output")

MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
]

# Ensure directories exist
SNAPSHOT_DIR.mkdir(exist_ok=True, parents=True)
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# --- Data Models ---

@dataclass
class Event:
    """Dataclass for a single event."""
    title: str
    date: date
    venue: str
    url: str
    start_time: Optional[dt_time] = None
    price: Optional[float] = None
    currency: str = "EUR"
    djs: Optional[List[str]] = None
    extraction_method: str = "unknown"
    scraped_at: Optional[datetime] = None

    def __post_init__(self):
        if self.scraped_at is None:
            self.scraped_at = datetime.utcnow()

    @classmethod
    def from_html(cls, card_soup: BeautifulSoup, venue: str, event_date: date, base_url: str) -> Optional["Event"]:
        """
        Create an Event object from a single event card's HTML (BeautifulSoup object).
        This method now assumes venue and date are provided from the parent context.
        """
        try:
            title_elem = card_soup.select_one("h3.h3 a.trackEventSpotlight")
            if not title_elem:
                return None

            title = title_elem.get_text(strip=True)
            url = urljoin(base_url, title_elem.get("href", ""))

            time_elem = card_soup.select_one("time")
            start_time = cls._parse_time(time_elem.get_text(strip=True)) if time_elem else None

            price_elem = card_soup.select_one(".price, .ticket-price")
            price = cls._parse_price(price_elem.get_text(strip=True)) if price_elem else None
            
            djs = [dj.get_text(strip=True) for dj in card_soup.select(".partyDj a")]

            return cls(
                title=title,
                date=event_date,
                venue=venue,
                url=url,
                start_time=start_time,
                price=price,
                djs=djs or None,
                extraction_method="html_parsing"
            )
        except Exception as e:
            print(f"[WARNING] Failed to parse event card: {e}")
            print(f"[DEBUG] Card content: {str(card_soup)[:300]}...")
            return None

    @staticmethod
    def _parse_time(ts: Optional[str]) -> Optional[dt_time]:
        if not ts: return None
        match = re.search(r'(\d{1,2}):(\d{2})', ts)
        if match:
            return dt_time(hour=int(match.group(1)), minute=int(match.group(2)))
        return None

    @staticmethod
    def _parse_price(price_str: Optional[str]) -> Optional[float]:
        if not price_str: return None
        numbers = re.findall(r'\d+\.?\d*', re.sub(r'[€$£,]', '', price_str))
        return float(numbers[0]) if numbers else None

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to a dictionary for serialization."""
        data = asdict(self)
        for key, value in data.items():
            if isinstance(value, (date, dt_time, datetime)):
                data[key] = value.isoformat()
        return data

# --- Scraper Class ---

class IbizaSpotlightScraper:
    """A stealthy, robust scraper for ibiza-spotlight.com."""
    
    def __init__(self, headless: bool = True, min_delay: float = 1.0, max_delay: float = 3.0):
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.browser: Optional[Browser] = None
        self.playwright_context = None

    def _get_random_delay(self, multiplier=1.0) -> None:
        """Sleep for a random duration."""
        time.sleep(random.uniform(self.min_delay * multiplier, self.max_delay * multiplier))

    def _ensure_browser(self):
        """Initializes Playwright browser if not already running."""
        if not PLAYWRIGHT_AVAILABLE:
            raise ImportError("Playwright is not installed. Run: pip install playwright playwright-stealth && playwright install")
        if not self.browser:
            print("[INFO] Starting Playwright browser...")
            self.playwright_context = sync_playwright().start()
            self.browser = self.playwright_context.chromium.launch(headless=self.headless)

    def human_click(self, page: Page, locator: Locator, timeout=10000):
        """Moves mouse over an element then clicks it like a human."""
        try:
            locator.wait_for(state="visible", timeout=timeout)
            box = locator.bounding_box()
            if not box:
                raise Exception("Could not get bounding box for element to click.")

            target_x = box['x'] + box['width'] * random.uniform(0.2, 0.8)
            target_y = box['y'] + box['height'] * random.uniform(0.2, 0.8)

            page.mouse.move(target_x, target_y, steps=random.randint(15, 30))
            self._get_random_delay(0.1)
            page.mouse.click(target_x, target_y)
            print(f"[INFO] Human-like click successful.")
        except Exception as e:
            print(f"[WARNING] Human-like click failed: {e}")
            # Fallback to a direct click
            locator.click(timeout=timeout)

    def handle_overlays(self, page: Page):
        """Robustly finds and closes any cookie banners or pop-up overlays."""
        selectors = [
            'a.cb-seen-accept',
            'button:has-text("Accept all")',
            'button:has-text("Accept")',
            'button:has-text("No problem")',
        ]
        print("[INFO] Checking for overlays...")
        self._get_random_delay(0.5)

        for selector in selectors:
            try:
                button_locator = page.locator(selector).first
                if button_locator.is_visible(timeout=5000):
                    print(f"[INFO] Found overlay button with selector: '{selector}'. Clicking it...")
                    self.human_click(page, button_locator)
                    time.sleep(2)
                    return
            except Exception:
                continue
        print("[INFO] No overlays were found or handled.")

    def fetch_page_html(self, url: str) -> str:
        """Fetch page HTML using Playwright with stealth and robust waits."""
        self._ensure_browser()
        page = None
        try:
            page = self.browser.new_page(user_agent=random.choice(MODERN_USER_AGENTS))
    print("[INFO] Applying stealth modifications to the browser page...")
    stealth_sync(page)
    print(f"[INFO] Fetching page with Playwright: {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    self.handle_overlays(page)
    main_container = "div#PartyCalBody"
    print(f"[INFO] Waiting for event container: {main_container}...")
    try:
        page.wait_for_selector(main_container, timeout=30000, state="visible")
        print("[INFO] Event container is visible. Content has loaded.")
        self._get_random_delay()
    except Exception as e:
        print(f"[ERROR] Timed out waiting for the main event container.")
        snap_path = SNAPSHOT_DIR / f"error_timeout_{int(time.time())}.html"
        snap_path.write_text(page.content(), encoding="utf-8")
        print(f"[DEBUG] Saved timeout snapshot to: {snap_path}")
        raise e
    return page.content()
finally:
    if page:
        page.close()

    def parse_html_events(self, html: str, base_url: str) -> List[Event]:
        """
        A completely rewritten parser that processes the page structure logically:
        by venue row, then by day, then by event card.
        """
        soup = BeautifulSoup(html, "lxml")
        events = []
        
        # The top-level container for all calendar content
        calendar_body = soup.select_one("#PartyCalBody")
        if not calendar_body:
            print("[ERROR] Could not find #PartyCalBody container in HTML. Aborting parse.")
            return []

        # Find all venue rows
        venue_rows = calendar_body.select(".partyCal-row")
        print(f"[INFO] Found {len(venue_rows)} venue rows to process.")

        # Find the date headers for the week
        date_headers = [th.get_text(strip=True) for th in soup.select(".partyCal-head li a")]

        for row in venue_rows:
            # 1. Extract the Venue for this entire row
            venue_name = "TBA"
            venue_elem = row.select_one(".partyCal-venue span")
            if venue_elem:
                venue_name = venue_elem.get_text(strip=True)
            
            # 2. Process each day within this venue's row
            day_cells = row.select("li.partyCal-day")
            for i, day_cell in enumerate(day_cells):
                # 3. Determine the date for this cell
                event_date = None
                try:
                    # Attempt to parse date from the header corresponding to this column
                    date_text = date_headers[i]
                    # Regex to find something like "Mon 28 Apr"
                    match = re.search(r'(\d{1,2})\s(\w{3})', date_text)
                    if match:
                        day, month_str = match.groups()
                        # This assumes the year is the one we are scraping.
                        # For a more robust solution, you'd pass the year down.
                        dt_obj = datetime.strptime(f"{day} {month_str}", "%d %b")
                        event_date = date(datetime.now().year, dt_obj.month, dt_obj.day)
                except (IndexError, ValueError) as e:
                    print(f"[WARNING] Could not determine date for cell {i}. Error: {e}")

                if not event_date:
                    continue # Skip if we can't figure out the date

                # 4. Find the event card inside this day's cell
                card = day_cell.select_one(".card-ticket")
                if not card:
                    continue # This day is empty for this venue

                # 5. Parse the event card with full context
                event = Event.from_html(card, venue=venue_name, event_date=event_date, base_url=base_url)
                if event:
                    events.append(event)
                    print(f"[DEBUG] Extracted: {event.title} at {event.venue} on {event.date}")

        return events

    def crawl_month(self, month: int, year: int) -> List[Event]:
        """Crawl events for a specific month."""
        print("[INFO] This scraper uses HTML parsing as the primary method.")
        events = []
        try:
            url = f"https://www.ibiza-spotlight.com/night/events/{year}/{month:02d}"
            html = self.fetch_page_html(url)
            events = self.parse_html_events(html, "https://www.ibiza-spotlight.com")
        except Exception as e:
            print(f"[FATAL] HTML crawl failed: {e}")
            traceback.print_exc()
        
        print(f"[SUCCESS] Found {len(events)} events for {month:02d}/{year}")
        return events

    def close(self):
        """Cleans up scraper resources."""
        if self.browser:
            self.browser.close()
        if self.playwright_context:
            self.playwright_context.stop()
        print("[INFO] Scraper resources closed")

# --- Export and Utility Functions ---

def save_events(events: List[Event], filepath_base: Path, formats: List[str]):
    """Saves events to specified formats."""
    if "json" in formats:
        with (filepath_base.with_suffix(".json")).open("w", encoding="utf-8") as f:
            json.dump([e.to_dict() for e in events], f, indent=2)
        print(f"[INFO] Saved {len(events)} events to {filepath_base.with_suffix('.json')}")
    if "csv" in formats:
        if events:
            with (filepath_base.with_suffix(".csv")).open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=events[0].to_dict().keys())
                writer.writeheader()
                for event in events:
                    writer.writerow(event.to_dict())
            print(f"[INFO] Saved {len(events)} events to {filepath_base.with_suffix('.csv')}")

def celebrate():
    """A small celebration for a successful scrape."""
    print("\n" + "✨" * 10 + " SCRAPE SUCCESSFUL! " + "✨" * 10)

# --- Main CLI ---

def main():
    """Main Command Line Interface function."""
    parser = argparse.ArgumentParser(description="Ibiza Spotlight Event Scraper v4.0 (Professional Grade)")
    parser.add_argument("--month", type=int, required=True, help="Month to scrape (1-12)")
    parser.add_argument("--year", type=int, required=True, help="Year to scrape")
    parser.add_argument("--no-headless", action="store_false", dest="headless", help="Show browser window for debugging")
    parser.add_argument("--format", nargs='+', choices=["json", "csv"], default=["json", "csv"], help="Output format(s)")
    
    args = parser.parse_args()
    
    if not (1 <= args.month <= 12):
        print("[ERROR] Month must be between 1 and 12.")
        return 1
    
    scraper = IbizaSpotlightScraper(headless=args.headless)
    
    try:
        events = scraper.crawl_month(args.month, args.year)
        
        if not events:
            print("[WARNING] No events were found.")
            return 0
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = f"ibiza_events_{args.year}_{args.month:02d}_{timestamp}"
        filepath_base = OUTPUT_DIR / base_name
        
        save_events(events, filepath_base, args.format)
        celebrate()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n[INFO] Scraping interrupted by user.")
        return 1
    except Exception:
        print(f"[FATAL] An unhandled error occurred.")
        traceback.print_exc()
        return 1
    finally:
        scraper.close()

if __name__ == "__main__":
    exit(main())
