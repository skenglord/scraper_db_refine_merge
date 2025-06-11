import asyncio
import json
import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Set
from urllib.parse import urljoin

from playwright.async_api import Browser, BrowserContext, Page, Playwright, TimeoutError as PlaywrightTimeoutError, Locator
from playwright_stealth import stealth_async
from bs4 import BeautifulSoup # Added for hybrid parsing

# Placeholder for actual schema mapping
try:
    from scrapers_v2.schema_adapter import map_to_unified_schema
    logger.info("Successfully imported map_to_unified_schema from scrapers_v2.schema_adapter")
except ImportError:
    logger = logging.getLogger(__name__) # Define logger here if import fails early
    logger.warning("Could not import map_to_unified_schema from scrapers_v2.schema_adapter. Using DUMMY version.")
    def map_to_unified_schema(raw_data: Dict, source_platform: str, source_url: str) -> Dict:
        logger.debug(f"Used DUMMY map_to_unified_schema for {source_platform} from {source_url}")
        unique_str = f"{raw_data.get('title', 'untitled')}_{raw_data.get('time_string', '')}_{source_url}"
        event_id = f"dummy_{hash(unique_str)}"
        return {**raw_data, "event_id": event_id, "unified": True, "source_platform": source_platform, "source_url": source_url}

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class IbizaSpotlightCalendarScraper:
    def __init__(self, config_path: str = "config.yaml"):
        self.config = self._load_dummy_config(config_path)
        logger.info(f"IbizaSpotlightCalendarScraper initialized with config: {self.config.get('scraper_name')}")
        self.base_url = self.config.get("base_url", "https://www.ibiza-spotlight.com")
        self.calendar_url = urljoin(self.base_url, self.config.get("calendar_path", "/night/events"))
        self.user_agent = self.config.get("user_agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1")
        self.viewport_size = self.config.get("viewport_size", {"width": 390, "height": 844})
        self.playwright_instance: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None

    def _load_dummy_config(self, config_file_name: str) -> Dict:
        logger.info(f"Loading dummy config (as {config_file_name} loading is not implemented yet).")
        return {
            "scraper_name": "IbizaSpotlightCalendar",
            "base_url": "https://www.ibiza-spotlight.com",
            "calendar_path": "/night/events",
            "max_months_to_scrape": 2,
            "db_collection": "events_ibiza_spotlight",
            "selectors": { # CSS selectors for BeautifulSoup parsing primarily
                "select_date_button": "a[data-modal='calendar-dp-modal']", # Playwright
                "calendar_modal": "#calendar-dp-modal", # Playwright
                "confirm_date_button": "button:has-text('Confirm')", # Playwright
                "modal_title": ".ui-datepicker-title", # Playwright
                "modal_next_month_button": ".ui-datepicker-next", # Playwright
                "modal_day_link": "td[data-handler='selectDay'] a", # Playwright
                "initial_modal_content_check": "#calendar-dp-modal .ui-datepicker-title", # Playwright
                "primary_event_card": "div.card-ticket.partycal-ticket", # Playwright (to get card HTML)
                "fallback_event_cards": [".partycal-ticket", ".card-ticket", ".event-card"], # Playwright
                # BS selectors, to be used on card_html:
                "event_title_bs": "div.ticket-header-bottom h3 a",
                "event_time_bs": "div.ticket-header time",
                "event_lineup_container_bs": "div.partyDjs", # Container for artists
                "event_artists_bs": "div.djlist div.partyDj a", # Actual artist links within container
                "event_venue_img_bs": "div.ticket-header-bottom img",
                "event_price_bs": ".price"
            },
            "timeouts": {"default": 10000, "navigation": 20000, "element": 7000, "quick_element": 3000},
            "delays": {"navigation_min": 0.8, "navigation_max": 1.8, "action_min": 0.3, "action_max": 0.7, "quick_min": 0.1, "quick_max": 0.2}
        }

    # --- Playwright Interaction Helpers ---
    async def _quick_delay(self, min_s: Optional[float] = None, max_s: Optional[float] = None) -> None:
        min_d = min_s if min_s is not None else self.config["delays"]["quick_min"]
        max_d = max_s if max_s is not None else self.config["delays"]["quick_max"]
        await asyncio.sleep(random.uniform(min_d, max_d))

    async def _init_browser(self, playwright: Playwright, headless: bool = True) -> bool:
        try:
            self.browser = await playwright.chromium.launch(headless=headless, args=['--no-sandbox', '--disable-blink-features=AutomationControlled'])
            self.context = await self.browser.new_context(user_agent=self.user_agent, viewport=self.viewport_size, is_mobile=True, java_script_enabled=True)
            await self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("Browser initialized successfully.")
            return True
        except Exception as e:
            logger.error(f"Browser initialization error: {e}", exc_info=True)
            return False

    async def _navigate_to_page(self, page: Page, url: str) -> bool:
        try:
            await stealth_async(page)
            await page.goto(url, timeout=self.config["timeouts"]["navigation"], wait_until="domcontentloaded")
            await self._quick_delay(self.config["delays"]["navigation_min"], self.config["delays"]["navigation_max"])
            logger.info(f"Successfully navigated to {url}")
            return True
        except PlaywrightTimeoutError: logger.error(f"Timeout navigating to {url}"); return False
        except Exception as e: logger.error(f"Navigation error to {url}: {e}", exc_info=True); return False

    async def _handle_cookies(self, page: Page) -> None:
        selectors = ["a.cb-seen.cta-secondary.sm.cb-seen-accept", "button:has-text('Accept')", "button:has-text('OK')", "#cookie-accept", "button[id*='cookie'][id*='accept']"]
        for selector in selectors:
            try:
                cookie_button = page.locator(selector)
                if await cookie_button.is_visible(timeout=self.config["timeouts"]["quick_element"]):
                    await cookie_button.click(timeout=self.config["timeouts"]["quick_element"])
                    logger.info(f"Cookie banner handled with selector: {selector}"); await self._quick_delay(); return
            except PlaywrightTimeoutError: logger.debug(f"Cookie selector not visible or timed out: {selector}")
            except Exception as e: logger.warning(f"Error handling cookie with selector {selector}: {e}")
        logger.info("No cookie banner found or handled.")

    async def _open_date_modal(self, page: Page) -> Optional[Locator]:
        try:
            modal_main_selector = self.config["selectors"]["calendar_modal"]
            modal_check_selector = self.config["selectors"]["initial_modal_content_check"]
            if not await page.locator(modal_check_selector).is_visible(timeout=1000):
                logger.info("Date modal not visible, attempting to click date select button.")
                await page.locator(self.config["selectors"]["select_date_button"]).click(timeout=self.config["timeouts"]["element"])
            await page.locator(modal_check_selector).wait_for(state="visible", timeout=self.config["timeouts"]["element"])
            logger.info("Date modal opened/confirmed visible.")
            return page.locator(modal_main_selector)
        except PlaywrightTimeoutError: logger.error("Timeout opening or finding date modal."); return None
        except Exception as e: logger.error(f"Error opening date modal: {e}", exc_info=True); return None

    async def _find_event_card_locators(self, page: Page) -> List[Locator]:
        primary_selector = self.config["selectors"]["primary_event_card"]
        card_locators = await page.locator(primary_selector).all()
        if card_locators:
            logger.info(f"Found {len(card_locators)} event cards with primary selector: {primary_selector}")
            return card_locators
        logger.warning(f"Primary selector {primary_selector} found no cards. Trying fallbacks.")
        for fallback_selector in self.config["selectors"]["fallback_event_cards"]:
            card_locators = await page.locator(fallback_selector).all()
            if card_locators:
                logger.info(f"Found {len(card_locators)} event cards with fallback: {fallback_selector}")
                return card_locators
        logger.warning("No event cards found with any selectors."); return []

    # --- Parsing Helper Functions (using BeautifulSoup on HTML snippets) ---
    def _parse_event_title_and_url_bs(self, card_html: str, base_url: str) -> Tuple[Optional[str], Optional[str]]:
        soup = BeautifulSoup(card_html, "html.parser")
        title_el = soup.select_one(self.config["selectors"]["event_title_bs"])
        if title_el:
            title = title_el.get_text(strip=True)
            url = title_el.get("href")
            return title, urljoin(base_url, url.strip()) if url else None
        return None, None

    def _parse_event_time_string_bs(self, card_html: str) -> Optional[str]:
        soup = BeautifulSoup(card_html, "html.parser")
        time_el = soup.select_one(self.config["selectors"]["event_time_bs"])
        return time_el.get_text(strip=True) if time_el else None

    def _parse_event_artists_bs(self, card_html: str) -> List[Dict[str, str]]:
        soup = BeautifulSoup(card_html, "html.parser")
        artists = []
        # First, find the container, then the artist links within it.
        lineup_container = soup.select_one(self.config["selectors"]["event_lineup_container_bs"])
        if lineup_container:
            artist_elements = lineup_container.select(self.config["selectors"]["event_artists_bs"])
            for artist_el in artist_elements[:10]: # Limit for performance
                name = artist_el.get_text(strip=True)
                if name: artists.append({"name": name, "role": "DJ"}) # Assuming role
        return artists

    def _parse_event_venue_name_bs(self, card_html: str) -> Optional[str]:
        soup = BeautifulSoup(card_html, "html.parser")
        venue_img_el = soup.select_one(self.config["selectors"]["event_venue_img_bs"])
        if venue_img_el:
            alt_text = venue_img_el.get("alt")
            return alt_text.strip() if alt_text else None
        return None

    def _parse_event_price_bs(self, card_html: str) -> Optional[str]:
        soup = BeautifulSoup(card_html, "html.parser")
        price_el = soup.select_one(self.config["selectors"]["event_price_bs"])
        return price_el.get_text(strip=True) if price_el else None

    async def _parse_event_card(self, card_locator: Locator, index: int) -> Optional[Dict[str, Any]]:
        event_data = {"scraped_at": datetime.utcnow().isoformat(), "card_index": index}
        try:
            card_html = await card_locator.inner_html()

            title, url = self._parse_event_title_and_url_bs(card_html, self.base_url)
            if not title: return None # Title is essential
            event_data["title"] = title
            if url: event_data["url"] = url

            event_data["time_string"] = self._parse_event_time_string_bs(card_html)
            event_data["artists"] = self._parse_event_artists_bs(card_html)
            event_data["venue_name"] = self._parse_event_venue_name_bs(card_html)
            event_data["price_string"] = self._parse_event_price_bs(card_html)

            logger.debug(f"Successfully parsed event card (hybrid): {event_data.get('title')}")
            return event_data
        except Exception as e:
            logger.error(f"Error parsing event card index {index} (hybrid): {e}", exc_info=True)
            return None

    async def _scrape_day_events(self, page: Page, day_str: str, seen_events_keys: Set[str]) -> List[Dict[str, Any]]:
        day_events = []
        await page.wait_for_load_state("domcontentloaded", timeout=self.config["timeouts"]["navigation"])
        await self._quick_delay(0.2, 0.5); await page.mouse.wheel(0, 500); await self._quick_delay(0.2, 0.5)

        event_card_locators = await self._find_event_card_locators(page)
        logger.info(f"Found {len(event_card_locators)} potential event cards for day {day_str}.")

        parse_tasks = [self._parse_event_card(card_loc, i) for i, card_loc in enumerate(event_card_locators)]
        parsed_results = await asyncio.gather(*parse_tasks, return_exceptions=True)

        for result in parsed_results:
            if isinstance(result, Exception): logger.error(f"Exception during event card parsing: {result}", exc_info=result)
            elif result and result.get("title"):
                event_key = f"{result.get('title')}_{result.get('venue_name', '')}_{result.get('time_string', '')}_{day_str}"
                if event_key not in seen_events_keys:
                    result["scraped_day_context"] = day_str
                    day_events.append(result); seen_events_keys.add(event_key)
        return day_events

    async def save_events_v2_style(self, events: List[Dict[str, Any]]):
        if not events: logger.info("No events to save via v2_style."); return {"status": "no_events", "saved_count": 0}
        logger.info(f"Attempting to save {len(events)} events using 'scrapers_v2' database components (placeholder).")
        try:
            logger.info(f"Placeholder: Would call a 'scrapers_v2.database.save_batch' like function here.")
            for i, event in enumerate(events[:2]): logger.info(f"Sample event {i+1} that would be saved: {{'event_id': event.get('event_id'), 'title': event.get('title'), 'source_url': event.get('source_url')}}")
            logger.info(f"Successfully 'saved/updated' {len(events)} events via placeholder 'scrapers_v2' database utility.")
            return {"status": "success_placeholder", "saved_count": len(events)}
        except Exception as e:
            logger.error(f"Error during placeholder save_events_v2_style: {e}", exc_info=True)
            return {"status": "error_placeholder", "message": str(e), "saved_count": 0}

    async def run(self) -> List[Dict[str, Any]]:
        from playwright.async_api import async_playwright # Import here to ensure it's part of async context
        self.playwright_instance = await async_playwright().start()

        if not await self._init_browser(self.playwright_instance, headless=True):
            if self.playwright_instance: await self.playwright_instance.stop()
            return []

        page = await self.context.new_page()
        all_unified_events: List[Dict[str, Any]] = []
        seen_event_keys_for_day: Set[str] = set()

        try:
            if not await self._navigate_to_page(page, self.calendar_url): return []
            await self._handle_cookies(page)
            await page.wait_for_load_state("domcontentloaded", timeout=self.config["timeouts"]["navigation"])

            scraped_month_titles = set()
            max_months = self.config.get("max_months_to_scrape", 12)

            for month_idx in range(max_months):
                logger.info(f"Starting month {month_idx + 1}/{max_months}")
                modal_locator = await self._open_date_modal(page)
                if not modal_locator: logger.error("Failed to open date modal. Ending month iteration."); break

                current_month_title = (await modal_locator.locator(self.config["selectors"]["modal_title"]).text_content() or "").strip()
                if not current_month_title: logger.warning("Could not get month title.")

                if current_month_title and current_month_title in scraped_month_titles:
                    logger.warning(f"Already scraped month '{current_month_title}'. Loop detected. Stopping."); break
                if current_month_title: scraped_month_titles.add(current_month_title)
                logger.info(f"Processing month: {current_month_title if current_month_title else 'Unknown Title'}")

                day_links_locators = await modal_locator.locator(self.config["selectors"]["modal_day_link"]).all()
                logger.info(f"Found {len(day_links_locators)} active days in month: {current_month_title}")

                for day_idx_loop in range(len(day_links_locators)):
                    modal_loop = await self._open_date_modal(page)
                    if not modal_loop: logger.error(f"Failed to re-open/validate date modal for day {day_idx_loop + 1}. Skipping day."); continue

                    current_day_links_in_loop = await modal_loop.locator(self.config["selectors"]["modal_day_link"]).all()
                    if day_idx_loop >= len(current_day_links_in_loop): logger.warning(f"Day index {day_idx_loop} out of bounds. Skipping."); break

                    day_link_loc = current_day_links_in_loop[day_idx_loop]
                    day_text = (await day_link_loc.text_content() or "").strip()
                    logger.info(f"Selecting day: {day_text} in month '{current_month_title}'")

                    try:
                        await day_link_loc.click(timeout=self.config["timeouts"]["element"])
                        await modal_loop.locator(self.config["selectors"]["confirm_date_button"]).click(timeout=self.config["timeouts"]["element"])

                        raw_day_events = await self._scrape_day_events(page, f"{current_month_title}-{day_text}", seen_event_keys_for_day)
                        logger.info(f"Collected {len(raw_day_events)} new raw events for day {day_text}.")

                        for raw_event in raw_day_events:
                            source_url = raw_event.get("url", self.calendar_url)
                            if not source_url.startswith("http"): source_url = urljoin(self.base_url, source_url)
                            unified_event = map_to_unified_schema(raw_data=raw_event, source_platform=self.config.get("scraper_name"), source_url=source_url)
                            if unified_event: all_unified_events.append(unified_event)
                        seen_event_keys_for_day.clear()
                    except PlaywrightTimeoutError: logger.error(f"Timeout selecting/confirming day {day_text}. Skipping.")
                    except Exception as e_day: logger.error(f"Error processing day {day_text}: {e_day}", exc_info=True)
                    await self._quick_delay(0.3, 0.6)

                modal_nav = await self._open_date_modal(page)
                if not modal_nav: logger.error("Failed to open modal for next month navigation. Stopping."); break

                next_month_btn = modal_nav.locator(self.config["selectors"]["modal_next_month_button"])
                if await next_month_btn.is_enabled(timeout=self.config["timeouts"]["element"]):
                    logger.info("Clicking next month button.")
                    prev_title_check = (await modal_nav.locator(self.config["selectors"]["modal_title"]).text_content() or "").strip()
                    await next_month_btn.click(); await self._quick_delay(0.5, 1.0)
                    new_title_check = (await modal_nav.locator(self.config["selectors"]["modal_title"]).text_content() or "").strip()
                    if new_title_check == prev_title_check: logger.info(f"Month title '{new_title_check}' did not change. Likely last month. Stopping."); break
                else: logger.info("Next month button not enabled/found. End of calendar."); break

            logger.info(f"Scraping finished. Total unique unified events collected: {len(all_unified_events)}")
            save_result = await self.save_events_v2_style(all_unified_events)
            logger.info(f"Save operation result: {save_result}")
        except Exception as e: logger.critical(f"Unhandled error in main run loop: {e}", exc_info=True)
        finally:
            if self.browser: await self.browser.close(); logger.info("Browser closed.")
            if self.playwright_instance: await self.playwright_instance.stop(); logger.info("Playwright instance stopped.")
        return all_unified_events

async def main_test_run():
    scraper = IbizaSpotlightCalendarScraper(config_path="scrapers_v2/scrapers/ibiza_spotlight_calendar/config.yaml")
    if "selectors" not in scraper.config :
         scraper.config = scraper._load_dummy_config("fallback_dummy_config")
         scraper.base_url = scraper.config.get("base_url"); scraper.calendar_url = urljoin(scraper.base_url, scraper.config.get("calendar_path"))
         scraper.user_agent = scraper.config.get("user_agent"); scraper.viewport_size = scraper.config.get("viewport_size")
         logger.info("Re-loaded DUMMY config for test run.")
    results = await scraper.run()
    logger.info(f"Scraper run completed. Number of events returned by run(): {len(results)}")
    if results: logger.info(f"Sample of first event returned: {json.dumps(results[0], indent=2, default=str)}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] - %(filename)s:%(lineno)d - %(message)s")
    logger.info("Starting Ibiza Spotlight Calendar Scraper directly for testing...")
    asyncio.run(main_test_run())
