"""
Optimized Playwright-based scraper for Ibiza Spotlight Calendar.

This version focuses on performance optimization while maintaining robust data extraction:
- Reduced delays and timeouts
- Parallel processing where possible
- Efficient selector strategies
- Streamlined interception logic
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

from playwright.async_api import (Browser, BrowserContext, Page,
                                Playwright, async_playwright, TimeoutError, Response)
from playwright_stealth import stealth_async

# --- Optimized Configuration ---
BASE_URL = "https://www.ibiza-spotlight.com"
CALENDAR_URL = f"{BASE_URL}/night/events"
OUTPUT_FILE = "ibiza_spotlight_calendar_events.json"
LOG_FILE = "ibiza_spotlight_calendar_scraper.log"

USER_AGENT = "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1"
VIEWPORT_SIZE = {"width": 390, "height": 844}

# Reduced delays for speed
MIN_NAV_DELAY = 0.5
MAX_NAV_DELAY = 1.5
MIN_ACTION_DELAY = 0.1
MAX_ACTION_DELAY = 0.3
QUICK_DELAY = 0.1

# Reduced timeouts
DEFAULT_TIMEOUT = 5000
QUICK_TIMEOUT = 2000
ELEMENT_TIMEOUT = 3000

# --- Streamlined Selectors (prioritized by likely success) ---
SELECT_DATE_BUTTON_SELECTOR = "a[data-modal='calendar-dp-modal']"
CALENDAR_MODAL_SELECTOR = "#calendar-dp-modal"
CONFIRM_DATE_BUTTON_SELECTOR = "button:has-text('Confirm')"

RELATIVE_MODAL_TITLE_SELECTOR = ".ui-datepicker-title"
RELATIVE_MODAL_NEXT_MONTH_BUTTON_SELECTOR = ".ui-datepicker-next"
RELATIVE_MODAL_DAY_LINK_SELECTOR = "td[data-handler='selectDay'] a"

INITIAL_MODAL_CONTENT_CHECK_SELECTOR = f"{CALENDAR_MODAL_SELECTOR} {RELATIVE_MODAL_TITLE_SELECTOR}"

# Primary selectors (most likely to work)
PRIMARY_EVENT_CARD_SELECTOR = "div.card-ticket.partycal-ticket"
FALLBACK_EVENT_SELECTORS = [".partycal-ticket", ".card-ticket", ".event-card"]

# Optimized detail selectors (most common first)
EVENT_SELECTORS = {
    'title': "div.ticket-header-bottom h3 a",
    'time': "div.ticket-header time",
    'lineup_container': "div.partyDjs",
    'artists': "div.djlist div.partyDj a",
    'venue_img': "div.ticket-header-bottom img",
    'price': ".price"
}

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FastEventExtractor:
    """High-performance event data extractor"""
    
    def __init__(self):
        self.intercepted_events: List[Dict] = []
        self.setup_complete = False
    
    async def setup_lightweight_interception(self, page: Page) -> None:
        """Lightweight interception focusing only on essential data"""
        if self.setup_complete:
            return
            
        logger.info("Setting up lightweight network interception...")
        
        # Minimal interception - only capture obvious event data
        async def handle_response(response: Response):
            try:
                url = response.url.lower()
                if ('event' in url or 'calendar' in url or 'api' in url) and response.status == 200:
                    content_type = response.headers.get('content-type', '')
                    if 'application/json' in content_type:
                        try:
                            data = await response.json()
                            if self.is_event_data(data):
                                self.intercepted_events.append({
                                    'url': response.url,
                                    'data': data,
                                    'timestamp': datetime.utcnow().isoformat()
                                })
                                logger.info(f"Captured event data from: {response.url}")
                        except:
                            pass  # Ignore JSON parsing errors
            except:
                pass  # Ignore all other errors
        
        page.on("response", handle_response)
        
        # Minimal JavaScript injection
        await page.add_init_script("""
            window.eventDataCapture = [];
            if (window.fetch) {
                const originalFetch = window.fetch;
                window.fetch = async function(url, options) {
                    const response = await originalFetch(url, options);
                    if (url.includes('event') || url.includes('calendar')) {
                        try {
                            const data = await response.clone().json();
                            if (Array.isArray(data) || (data && data.events)) {
                                window.eventDataCapture.push(data);
                            }
                        } catch(e) {}
                    }
                    return response;
                };
            }
        """)
        
        self.setup_complete = True
    
    def is_event_data(self, data: Any) -> bool:
        """Quick check if data contains events"""
        if isinstance(data, list) and len(data) > 0:
            return True
        if isinstance(data, dict):
            return 'events' in data or 'title' in data or 'name' in data
        return False
    
    async def get_captured_data(self, page: Page) -> List[Dict]:
        """Quickly retrieve captured data"""
        all_data = list(self.intercepted_events)
        
        try:
            js_data = await page.evaluate("window.eventDataCapture || []")
            for item in js_data:
                all_data.append({'data': item, 'source': 'js'})
        except:
            pass
        
        return all_data

async def quick_delay(min_s: float = QUICK_DELAY, max_s: float = MIN_ACTION_DELAY) -> None:
    await asyncio.sleep(random.uniform(min_s, max_s))

async def init_fast_browser(playwright: Playwright, headless: bool = True) -> Tuple[Optional[Browser], Optional[BrowserContext]]:
    try:
        browser = await playwright.chromium.launch(
            headless=headless,
            args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport=VIEWPORT_SIZE,
            is_mobile=True,
            java_script_enabled=True
        )
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("Fast browser initialized.")
        return browser, context
    except Exception as e:
        logger.error(f"Browser init error: {e}")
        return None, None

async def fast_navigate(page: Page, url: str) -> bool:
    try:
        await stealth_async(page)
        await page.goto(url, timeout=15000, wait_until="domcontentloaded")  # Faster load
        await quick_delay(0.3, 0.8)  # Much shorter delay
        logger.info(f"Fast navigation to {url}")
        return True
    except Exception as e:
        logger.error(f"Navigation error: {e}")
        return False

async def quick_cookie_handle(page: Page) -> None:
    """Fast cookie handling with minimal timeout"""
    selectors = [
        "a.cb-seen.cta-secondary.sm.cb-seen-accept",
        "button:has-text('Accept')",
        "#cookie-accept"
    ]
    for selector in selectors:
        try:
            if await page.locator(selector).is_visible(timeout=1000):
                await page.locator(selector).click()
                logger.info("Cookie handled")
                return
        except:
            continue

async def find_events_fast(page: Page) -> List[Any]:
    """Fast event card detection"""
    try:
        # Try primary selector first
        cards = await page.locator(PRIMARY_EVENT_CARD_SELECTOR).all()
        if cards:
            logger.info(f"Found {len(cards)} events with primary selector")
            return cards
        
        # Quick fallback check
        for selector in FALLBACK_EVENT_SELECTORS:
            cards = await page.locator(selector).all()
            if cards:
                logger.info(f"Found {len(cards)} events with fallback: {selector}")
                return cards
    except:
        pass
    
    return []

async def parse_event_fast(card, index: int) -> Optional[Dict[str, Any]]:
    """Fast event parsing with minimal error handling"""
    try:
        event = {
            "scraped_at": datetime.utcnow().isoformat(),
            "index": index
        }
        
        # Title and URL
        try:
            title_el = card.locator(EVENT_SELECTORS['title']).first
            if await title_el.is_visible(timeout=QUICK_TIMEOUT):
                event["title"] = (await title_el.text_content() or "").strip()
                href = await title_el.get_attribute("href")
                if href:
                    event["url"] = urljoin(BASE_URL, href)
        except:
            pass
        
        # Time
        try:
            time_el = card.locator(EVENT_SELECTORS['time']).first
            if await time_el.is_visible(timeout=QUICK_TIMEOUT):
                event["time"] = (await time_el.text_content() or "").strip()
        except:
            pass
        
        # Lineup (simplified)
        try:
            lineup = []
            artists = await card.locator(EVENT_SELECTORS['artists']).all()
            for artist in artists[:10]:  # Limit to first 10 artists for speed
                name = await artist.text_content()
                if name and name.strip():
                    lineup.append({"name": name.strip(), "room": "Main"})
            if lineup:
                event["lineup"] = lineup
        except:
            pass
        
        # Venue
        try:
            img = card.locator(EVENT_SELECTORS['venue_img']).first
            if await img.is_visible(timeout=QUICK_TIMEOUT):
                alt = await img.get_attribute("alt")
                if alt:
                    event["venue"] = alt
        except:
            pass
        
        return event if event.get("title") else None
        
    except Exception as e:
        logger.debug(f"Parse error for card {index}: {e}")
        return None

async def process_intercepted_fast(data_list: List[Dict]) -> List[Dict]:
    """Fast processing of intercepted data"""
    events = []
    
    for item in data_list:
        try:
            data = item.get('data', {})
            
            if isinstance(data, list):
                for event in data[:20]:  # Limit processing
                    if isinstance(event, dict) and event.get('title'):
                        events.append({
                            "title": event.get('title'),
                            "url": event.get('url'),
                            "time": event.get('time', event.get('date')),
                            "venue": event.get('venue', event.get('location')),
                            "scraped_at": datetime.utcnow().isoformat(),
                            "source": "intercepted"
                        })
            elif isinstance(data, dict) and data.get('title'):
                events.append({
                    "title": data.get('title'),
                    "url": data.get('url'),
                    "time": data.get('time', data.get('date')),
                    "venue": data.get('venue', data.get('location')),
                    "scraped_at": datetime.utcnow().isoformat(),
                    "source": "intercepted"
                })
        except:
            continue
    
    return events

async def open_modal_fast(page: Page) -> Optional[Any]:
    """Fast modal opening"""
    try:
        if not await page.locator(INITIAL_MODAL_CONTENT_CHECK_SELECTOR).is_visible(timeout=1000):
            await page.locator(SELECT_DATE_BUTTON_SELECTOR).click()
            await page.locator(INITIAL_MODAL_CONTENT_CHECK_SELECTOR).wait_for(state="visible", timeout=5000)
        return page.locator(CALENDAR_MODAL_SELECTOR)
    except Exception as e:
        logger.error(f"Modal error: {e}")
        return None

async def scrape_fast(page: Page) -> List[Dict[str, Any]]:
    """High-speed scraping with minimal delays"""
    extractor = FastEventExtractor()
    await extractor.setup_lightweight_interception(page)
    
    all_events: List[Dict[str, Any]] = []
    seen_events: Set[str] = set()
    
    if not await fast_navigate(page, CALENDAR_URL):
        return []
    
    await quick_cookie_handle(page)
    await page.wait_for_load_state("domcontentloaded", timeout=10000)
    
    scraped_months = set()
    
    while True: # Iterate indefinitely until no more months are available
        modal = await open_modal_fast(page)
        if not modal:
            logger.error(f"Modal failed for month {month_num + 1}")
            break
        
        month_text = (await modal.locator(RELATIVE_MODAL_TITLE_SELECTOR).text_content() or "").strip()
        if month_text in scraped_months:
            logger.warning(f"Month loop detected: {month_text}")
            break
        scraped_months.add(month_text)
        logger.info(f"Processing month: {month_text}")
        
        day_count = await modal.locator(RELATIVE_MODAL_DAY_LINK_SELECTOR).count()
        logger.info(f"Days to process: {day_count}")
        
        # Process days with minimal delays
        for day_idx in range(min(day_count, 31)):  # Safety limit
            modal = await open_modal_fast(page)
            if not modal:
                break
            
            try:
                day_links = await modal.locator(RELATIVE_MODAL_DAY_LINK_SELECTOR).all()
                if day_idx >= len(day_links):
                    break
                
                day_link = day_links[day_idx]
                day_text = (await day_link.text_content() or "").strip()
                
                # Fast day selection
                await day_link.click()
                await modal.locator(CONFIRM_DATE_BUTTON_SELECTOR).click()
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
                
                # Quick scroll
                await page.mouse.wheel(0, 500)
                await quick_delay()
                
                # Fast event extraction
                intercepted = await extractor.get_captured_data(page)
                if intercepted:
                    processed = await process_intercepted_fast(intercepted)
                    for event in processed:
                        key = event.get("url") or f"{event.get('title')}_{day_text}"
                        if key and key not in seen_events:
                            all_events.append(event)
                            seen_events.add(key)
                
                # DOM parsing fallback
                cards = await find_events_fast(page)
                if cards:
                    logger.info(f"Day {day_text}: {len(cards)} cards found")
                    
                    # Process cards in parallel batches
                    tasks = []
                    for i, card in enumerate(cards[:20]):  # Limit for speed
                        tasks.append(parse_event_fast(card, i))
                    
                    # Process in batches of 5
                    for i in range(0, len(tasks), 5):
                        batch = tasks[i:i+5]
                        results = await asyncio.gather(*batch, return_exceptions=True)
                        
                        for result in results:
                            if isinstance(result, dict) and result.get("title"):
                                key = result.get("url") or f"{result.get('title')}_{day_text}"
                                if key and key not in seen_events:
                                    all_events.append(result)
                                    seen_events.add(key)
                
                # Minimal delay between days
                await quick_delay()
                
            except Exception as e:
                logger.debug(f"Day {day_idx} error: {e}")
                continue
        
        # Next month
        modal = await open_modal_fast(page)
        if modal:
            try:
                next_btn = modal.locator(RELATIVE_MODAL_NEXT_MONTH_BUTTON_SELECTOR)
                if await next_btn.is_enabled():
                    await next_btn.click()
                    await page.wait_for_timeout(500)  # Very short wait
                else:
                    logger.info("No more next month buttons found. Ending month iteration.")
                    break # No more next month buttons
            except Exception as e:
                logger.info(f"Error navigating to next month or no more months: {e}")
                break # Break if there's an error or no more months
    
    logger.info(f"Total events collected: {len(all_events)}")
    return all_events

def save_fast(events: List[Dict[str, Any]], filename: str = OUTPUT_FILE) -> None:
    try:
        output = {
            "metadata": {
                "total_events": len(events),
                "scraped_at": datetime.utcnow().isoformat(),
                "version": "fast_v1.0"
            },
            "events": events
        }
        
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(events)} events to {filename}")
    except Exception as e:
        logger.error(f"Save error: {e}")

async def main():
    logger.info("Starting FAST Ibiza Spotlight Scraper...")
    start_time = time.time()
    
    async with async_playwright() as playwright:
        browser, context = await init_fast_browser(playwright, headless=False)
        if not browser or not context:
            return
        
        page = await context.new_page()
        try:
            events = await scrape_fast(page)
            if events:
                save_fast(events)
                logger.info(f"SUCCESS: {len(events)} events in {time.time() - start_time:.1f}s")
            else:
                logger.warning("No events collected")
        finally:
            await browser.close()
    
    logger.info(f"Finished in {time.time() - start_time:.1f} seconds")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)