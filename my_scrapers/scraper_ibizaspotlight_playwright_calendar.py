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
from pymongo import MongoClient # Added
from pymongo.errors import ConnectionFailure # Added

# Assuming schema_adapter is in project root, and classy_skkkrapey is project root or accessible
# Path manipulation for imports might be needed if this script is run directly
# For now, direct imports are attempted.
try:
    from schema_adapter import map_to_unified_schema # Added
    from classy_skkkrapey.config import settings # Added
except ImportError as e:
    # Fallback for local execution if imports fail
    # This assumes schema_adapter.py and config.py are in the parent directory
    # or that the project root has been added to sys.path by an external runner
    import sys
    from pathlib import Path
    project_root_for_imports = Path(__file__).resolve().parent.parent
    if str(project_root_for_imports) not in sys.path:
        sys.path.insert(0, str(project_root_for_imports))
    try:
        from schema_adapter import map_to_unified_schema
        from classy_skkkrapey.config import settings
    except ImportError:
        # Define dummy settings if import still fails, for script to run without full functionality
        class DummySettings:
            MONGODB_URI = "mongodb://localhost:27017/"
            DB_NAME = "fallback_db_calendar"
        settings = DummySettings()
        # map_to_unified_schema will remain undefined if import fails, leading to errors later if not handled.
        # This is acceptable for the task, as the focus is on refactoring assuming components are available.
        logging.error(f"Failed to import schema_adapter or settings even after path modification: {e}")


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
    
    logger.info(f"Total raw events collected by scrape_fast: {len(all_events)}")

    unified_events_list: List[Dict[str, Any]] = []
    if 'map_to_unified_schema' not in globals():
        logger.error("map_to_unified_schema is not available. Cannot process events for unified schema.")
        return unified_events_list # Return empty list

    for raw_event_dict in all_events:
        source_url_extracted = raw_event_dict.get("url")
        if not source_url_extracted:
            # Try to find URL in 'data' if it's from intercepted_events
            if raw_event_dict.get('source') == 'intercepted' and isinstance(raw_event_dict.get('data'), dict):
                source_url_extracted = raw_event_dict.get('data', {}).get('url')

            if not source_url_extracted:
                 # Fallback if URL is truly missing, use a placeholder or log error
                logger.warning(f"Missing source_url for raw event: {raw_event_dict.get('title', 'Unknown event')}. Skipping.")
                continue # Skip this event if URL is crucial and missing

        try:
            unified_event_doc = map_to_unified_schema(
                raw_data=raw_event_dict,
                source_platform="ibiza-spotlight-calendar", # Specific platform name
                source_url=source_url_extracted
            )
            if unified_event_doc:
                unified_events_list.append(unified_event_doc)
            else:
                logger.warning(f"Mapping to unified schema returned None for event from: {source_url_extracted}")
        except Exception as e_map:
            logger.error(f"Error mapping event data from {source_url_extracted}: {e_map}", exc_info=True)

    logger.info(f"Successfully mapped {len(unified_events_list)} events to unified schema.")
    return unified_events_list

def save_fast(unified_events: List[Dict[str, Any]], events_collection: Any) -> None: # Added events_collection type hint
    """Saves a list of unified event documents to MongoDB."""
    if not events_collection:
        logger.warning("MongoDB collection not available. Cannot save events to DB.")
        if unified_events: # Fallback to print if DB not available but events exist
            logger.info("Printing unified events to console as DB fallback:")
            for event_doc in unified_events[:5]: # Print first 5 as sample
                 print(json.dumps(event_doc, indent=2, default=str))
            if len(unified_events) > 5:
                print(f"... and {len(unified_events) - 5} more events.")
        return

    if not unified_events:
        logger.info("No unified events to save.")
        return

    saved_count = 0
    for unified_event_doc in unified_events:
        if not unified_event_doc or not unified_event_doc.get("event_id"):
            logger.warning(f"Skipping save for event due to missing data or event_id: {unified_event_doc.get('title', 'N/A')}")
            continue
        try:
            update_key = {"event_id": unified_event_doc["event_id"]}
            events_collection.update_one(
                update_key,
                {"$set": unified_event_doc},
                upsert=True
            )
            saved_count += 1
            # logger.debug(f"Saved/Updated event to DB: {unified_event_doc.get('title')}") # Can be too noisy
        except Exception as e:
            logger.error(f"Error saving event {unified_event_doc.get('event_id')} to MongoDB: {e}", exc_info=True)

    logger.info(f"Successfully saved/updated {saved_count} events to MongoDB.")


async def main():
    logger.info("Starting FAST Ibiza Spotlight Calendar Scraper...")
    start_time = time.time()

    mongo_client = None
    events_collection = None
    try:
        if hasattr(settings, 'MONGODB_URI') and hasattr(settings, 'DB_NAME'):
            mongo_client = MongoClient(settings.MONGODB_URI)
            db = mongo_client[settings.DB_NAME]
            events_collection = db.events # Assuming collection name is 'events'
            logger.info(f"Successfully connected to MongoDB: {settings.DB_NAME} on {settings.MONGODB_URI}")
        else:
            logger.warning("MongoDB settings (MONGODB_URI or DB_NAME) not found. Will fallback if saving is attempted.")
    except ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
    except AttributeError: # If settings is a DummySettings without these attributes
        logger.error("MongoDB URI/DB_NAME not found in settings configuration.")

    
    async with async_playwright() as playwright:
        browser, context = await init_fast_browser(playwright, headless=True) # Usually headless=True for prod
        if not browser or not context:
            if mongo_client: mongo_client.close()
            return
        
        page = await context.new_page()
        try:
            # scrape_fast now returns list of unified_event_docs
            unified_events_list = await scrape_fast(page)

            if unified_events_list:
                save_fast(unified_events_list, events_collection) # Pass collection to save_fast
                logger.info(f"SUCCESS: Processed {len(unified_events_list)} events in {time.time() - start_time:.1f}s")
            else:
                logger.warning("No events collected or mapped.")
        finally:
            await browser.close()
            if mongo_client:
                mongo_client.close()
                logger.info("MongoDB connection closed.")
    
    logger.info(f"Finished in {time.time() - start_time:.1f} seconds")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)