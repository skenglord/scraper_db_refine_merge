#!/usr/bin/env python3
"""
ClubTickets.com Scraper - Scrapers_V2 Adaptation
"""

import re
import random
import time
from datetime import datetime, timezone as dt_timezone
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any
import logging
import json

from playwright.sync_api import sync_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError, ElementHandle

# --- V2 Imports ---
from scrapers_v2.config import settings
from scrapers_v2.utils import (
    setup_logger,
    save_unified_events_to_mongodb,
    save_to_json_file,
    save_to_csv_file,
    # save_to_markdown_file # Markdown saving can be verbose, uncomment if needed
)
from scrapers_v2.schema_adapter import map_to_unified_schema, UnifiedEvent
from scrapers_v2.data_quality.cleaning import clean_and_normalize_text
from scrapers_v2.data_quality.scoring import calculate_basic_quality_score
from scrapers_v2.sentry_setup import init_sentry # Import Sentry initialization function

class ClubTicketsV2Scraper:
    def __init__(self):
        self.logger = setup_logger(
            "ClubTicketsV2Scraper",
            "clubtickets_v2_scrape_run"
        )
        self.logger.info("Initializing ClubTicketsV2Scraper using centralized settings.")

        ct_settings = settings.scrapers_specific.clubtickets

        self.headless = settings.scraper_globals.default_headless_browser
        self.slow_mo = ct_settings.slow_mo_ms
        self.user_agent = settings.scraper_globals.default_user_agent
        self.viewport_width = ct_settings.viewport_width
        self.viewport_height = ct_settings.viewport_height

        self.max_retries = ct_settings.max_retries
        self.retry_delay_sec = ct_settings.retry_delay_sec
        self.random_short_delay_sec_min = settings.scraper_globals.min_delay_ms / 1000.0
        self.random_short_delay_sec_max = settings.scraper_globals.max_delay_ms / 1000.0
        self.random_long_delay_sec_min = (settings.scraper_globals.min_delay_ms * 1.5) / 1000.0
        self.random_long_delay_sec_max = (settings.scraper_globals.max_delay_ms * 1.5) / 1000.0

        self.event_card_selector = ct_settings.event_card_selector
        self.date_tab_xpath = ct_settings.date_tab_xpath
        self.show_more_xpath = ct_settings.show_more_xpath
        self.event_detail_selectors = ct_settings.event_selectors

        self.playwright_instance: Optional[sync_playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.logger.info(f"ClubTicketsV2Scraper initialized. Headless: {self.headless}, Target URL (from settings): {ct_settings.target_url}")

    def __enter__(self):
        self.logger.info("Starting Playwright...")
        self.playwright_instance = sync_playwright().start()
        try:
            self.browser = self.playwright_instance.chromium.launch(
                headless=self.headless,
                slow_mo=self.slow_mo,
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.logger.info(f"Playwright browser launched (headless: {self.headless}).")
        except Exception as e:
            self.logger.critical(f"Browser launch failed: {e}", exc_info=True)
            if self.playwright_instance: self.playwright_instance.stop()
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.info("Closing Playwright resources...")
        if self.page and not self.page.is_closed():
            try: self.page.close()
            except Exception as e: self.logger.error(f"Page close error: {e}", exc_info=True)
        if self.browser and self.browser.is_connected():
            try: self.browser.close()
            except Exception as e: self.logger.error(f"Browser close error: {e}", exc_info=True)
        if self.playwright_instance:
            try: self.playwright_instance.stop()
            except Exception as e: self.logger.error(f"Playwright stop error: {e}", exc_info=True)
        self.logger.info("Playwright resources cleaned.")

    def _quick_delay(self, min_s: Optional[float] = None, max_s: Optional[float] = None):
        _min = min_s if min_s is not None else self.random_short_delay_sec_min
        _max = max_s if max_s is not None else self.random_short_delay_sec_max
        time.sleep(random.uniform(_min, _max))

    def random_delay(self, short: bool = True):
        min_d, max_d = ((self.random_short_delay_sec_min, self.random_short_delay_sec_max)
                        if short else (self.random_long_delay_sec_min, self.random_long_delay_sec_max))
        time.sleep(random.uniform(min_d, max_d))

    def retry_action(self, action, description, is_critical=True) -> bool:
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.debug(f"Attempt {attempt}/{self.max_retries}: {description}")
                action()
                self.logger.debug(f"Successfully performed: {description}")
                return True
            except PlaywrightTimeoutError as e:
                self.logger.warning(f"Timeout on attempt {attempt} for {description}: {e}")
            except Exception as e:
                self.logger.warning(f"Error on attempt {attempt} for {description}: {e}", exc_info=True)
            if attempt < self.max_retries:
                time.sleep(self.retry_delay_sec)

        log_func = self.logger.critical if is_critical else self.logger.error
        log_func(f"Failed to perform {description} after {self.max_retries} attempts")
        return False

    def navigate_to(self, url: str) -> bool:
        self.logger.info(f"Navigating to: {url}")
        if not self.browser:
            self.logger.error("Browser not initialized. Cannot navigate.")
            return False
        try:
            if self.page and not self.page.is_closed():
                self.page.close()
                self.logger.debug("Closed existing page before navigation.")
            self.page = self.browser.new_page(
                user_agent=self.user_agent,
                viewport={"width": self.viewport_width, "height": self.viewport_height}
            )
            self.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.debug(f"New page created for navigation to {url}.")
        except Exception as e:
            self.logger.error(f"Failed to create new page for {url}: {e}", exc_info=True)
            return False

        request_timeout = settings.scraper_globals.default_request_timeout_ms
        success = self.retry_action(
            lambda: self.page.goto(url, wait_until="domcontentloaded", timeout=request_timeout),
            f"Navigate to {url}"
        )
        if success:
            self.logger.info(f"Successfully navigated to {url}.")
            self.handle_cookie_popup()
            self._quick_delay(min_s=0.5, max_s=1.0)
        else:
            self.logger.error(f"Failed to navigate to {url}.")
        return success

    def handle_cookie_popup(self):
        if not self.page:
            self.logger.warning("No page available to handle cookie popup.")
            return False
        self.logger.debug("Checking for cookie consent popup...")
        self.page.wait_for_timeout(random.randint(1000, 2000))
        selectors = [
            'button#cookie-accept', 'button.cookie-accept', "button:has-text('Accept All')",
            "button:has-text('Accept all cookies')", "button:has-text('Accept Cookies')",
            "button:has-text('Agree')", "button:has-text('OK')", "button:has-text('I agree')",
            "button:has-text('Consent')", 'button[data-testid="cookie-accept-button"]',
            'button#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll'
        ]
        for selector in selectors:
            try:
                button = self.page.locator(selector).first
                if button.is_visible(timeout=2000):
                    self.logger.info(f"Cookie popup found with selector: '{selector}'. Attempting to click.")
                    if self.retry_action(lambda: button.click(timeout=3000), f"Click cookie button: '{selector}'", is_critical=False):
                        self.logger.info(f"Clicked cookie consent button using: '{selector}'.")
                        self.page.wait_for_timeout(random.randint(500, 1000))
                        return True
                    else:
                        self.logger.warning(f"Failed to click cookie button '{selector}' after retries.")
            except PlaywrightTimeoutError:
                self.logger.debug(f"Cookie selector '{selector}' not visible or timed out.")
            except Exception as e:
                self.logger.warning(f"Error interacting with cookie selector '{selector}': {e}", exc_info=True)
        self.logger.debug("No known cookie popup detected or handled.")
        return False

    def parse_event_card_details(self, card_element: ElementHandle, base_url: str) -> Optional[Dict[str, Any]]:
        raw_card_text_for_debug = clean_and_normalize_text(card_element.text_content())
        self.logger.debug(f"Attempting to parse event card. Base URL: {base_url}. Card text preview: '{raw_card_text_for_debug[:150]}'")

        event_details: Dict[str, Any] = {
            "scraped_at": datetime.now(dt_timezone.utc).isoformat(),
            "source_page_url": base_url,
            "title": None, "event_specific_url": None, "date_text": None,
            "venue_name": None, "image_url": None, "price_info": None
        }

        cfg_selectors = self.event_detail_selectors
        if not cfg_selectors or not isinstance(cfg_selectors, dict):
            self.logger.error("Event detail selectors (self.event_detail_selectors) is not defined or not a dict. Cannot parse card details.")
            return None

        for field_key, selector_str in cfg_selectors.items():
            if not selector_str:
                self.logger.debug(f"No selector provided for field '{field_key}'. Skipping.")
                continue
            try:
                element = card_element.query_selector(selector_str)
                if element:
                    raw_text_content = element.text_content()

                    if field_key == "url_anchor":
                        raw_url = element.get_attribute('href')
                        if raw_url: event_details["event_specific_url"] = urljoin(base_url, raw_url)
                    elif field_key == "image":
                        raw_img_src = element.get_attribute('src')
                        if raw_img_src: event_details["image_url"] = urljoin(base_url, raw_img_src)
                    elif field_key == "price":
                         cleaned_price_text = clean_and_normalize_text(raw_text_content)
                         if cleaned_price_text:
                             price_match = re.search(r'(?:From\s*)?€?\$?\£?([\d\.,]+)(?:\s*-\s*€?\$?\£?[\d\.,]+)?', cleaned_price_text, re.IGNORECASE)
                             event_details["price_info"] = price_match.group(1) if price_match else cleaned_price_text
                         else:
                             event_details["price_info"] = None
                    else:
                        event_details[field_key] = clean_and_normalize_text(raw_text_content)
                else:
                    self.logger.debug(f"Selector for field '{field_key}' ('{selector_str}') not found.")
            except PlaywrightTimeoutError:
                self.logger.warning(f"Timeout for selector '{selector_str}' for field '{field_key}'.")
            except Exception as e:
                self.logger.error(f"Error extracting field '{field_key}' with selector '{selector_str}': {e}", exc_info=True)

        if not event_details.get("title"):
            self.logger.warning(f"Event card parsed with no title. Base URL: {base_url}. Skipping.")
            return None

        event_details["source_url"] = event_details["event_specific_url"] if event_details["event_specific_url"] else base_url
        self.logger.info(f"Parsed event card: '{event_details.get('title', 'N/A')}'")
        return event_details

    def process_current_page_events(self, date_context="current") -> List[Dict[str, Any]]:
        if not self.page: self.logger.error("Page not available."); return []
        current_page_url = self.page.url
        self.logger.info(f"Processing events for: {date_context}, URL: {current_page_url}")

        if not self.retry_action( lambda: self.page.wait_for_selector(self.event_card_selector, state="attached", timeout=10000),
            f"Wait for event cards '{self.event_card_selector}' for {date_context}", is_critical=False):
            self.logger.warning(f"Event cards not found for '{date_context}'.")
            return []

        card_elements = self.page.locator(self.event_card_selector).all()
        self.logger.info(f"Found {len(card_elements)} potential event cards for '{date_context}'.")
        parsed_events: List[Dict[str, Any]] = []
        for i, card_locator in enumerate(card_elements):
            self._quick_delay(0.05, 0.15)
            try:
                card_handle = card_locator.element_handle()
                if card_handle:
                    event_data = self.parse_event_card_details(card_handle, base_url=current_page_url)
                    if event_data: event_data["page_context"] = date_context; parsed_events.append(event_data)
            except Exception as e: self.logger.error(f"Error parsing card {i}: {e}", exc_info=True)
        self.logger.info(f"Parsed {len(parsed_events)} events from page context '{date_context}'.")
        return parsed_events

    def crawl_events(self, url: str, max_pages_to_process: int) -> List[Dict[str, Any]]:
        self.logger.info(f"Starting crawl of: {url}, Max date tabs: {max_pages_to_process}")
        all_raw_events: List[Dict[str, Any]] = []
        if not self.navigate_to(url) or not self.page: return all_raw_events

        self.random_delay(short=False)
        for _i in range(random.randint(1,2)):
            self.page.evaluate(f"window.scrollBy(0, {random.randint(200, 350)})")
            self.logger.debug(f"Performed scroll on {self.page.url}")
            self._quick_delay(0.2, 0.4)

        if self.show_more_xpath:
            try:
                show_more_button = self.page.locator(self.show_more_xpath)
                if show_more_button.is_visible(timeout=5000):
                    self.logger.info("'Show more events' button is visible. Attempting click.")
                    if self.retry_action(lambda: show_more_button.click(timeout=8000), "Click 'Show more events' button"):
                        self.page.wait_for_load_state("networkidle", timeout=10000)
                        self.random_delay(short=True)
                else: self.logger.info("'Show more events' button not found/visible.")
            except Exception as e: self.logger.warning(f"Issue with 'Show more events': {e}", exc_info=True)

        all_raw_events.extend(self.process_current_page_events(date_context="initial_page"))

        if not self.date_tab_xpath:
            self.logger.warning("Date tab XPath not configured. Skipping tab processing.")
            return all_raw_events

        date_tabs_locators = self.page.locator(self.date_tab_xpath).all()
        self.logger.info(f"Found {len(date_tabs_locators)} date tabs. Will process up to {max_pages_to_process} tabs.")

        processed_tabs_count = 0
        for i, tab_locator in enumerate(date_tabs_locators):
            if processed_tabs_count >= max_pages_to_process:
                self.logger.info(f"Reached max_pages_to_process ({max_pages_to_process}) for date tabs.")
                break

            raw_tab_text = tab_locator.text_content()
            tab_text_content = clean_and_normalize_text(raw_tab_text) or f"Tab_{i+1}"

            self.logger.info(f"Processing date tab {i+1}/{len(date_tabs_locators)}: '{tab_text_content}'")

            if self.retry_action(lambda: tab_locator.click(timeout=10000), f"Click date tab: {tab_text_content}"):
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    self.page.wait_for_timeout(random.randint(1500,2500))
                    all_raw_events.extend(self.process_current_page_events(date_context=f"date_tab_{tab_text_content}"))
                    processed_tabs_count +=1
                except Exception as e_tab_proc:
                    self.logger.error(f"Error processing events for tab '{tab_text_content}': {e_tab_proc}", exc_info=True)
            else: self.logger.warning(f"Failed to click date tab: {tab_text_content}. Skipping.")
            self.random_delay(short=False)

        self.logger.info(f"Crawl_events completed. Total raw events: {len(all_raw_events)}")
        return all_raw_events

def run_clubtickets_scraper(
    target_url_override: Optional[str] = None,
    max_pages_override: Optional[int] = None,
    collection_name_override: Optional[str] = None
) -> List[UnifiedEvent]:

    # Initialize Sentry at the beginning of the main execution function
    init_sentry() # Sentry will only initialize if DSN is in settings

    run_logger = setup_logger("ClubTicketsV2Run", "clubtickets_v2_main_run")
    run_logger.info("Starting ClubTickets V2 Scraper execution...")

    actual_target_url = target_url_override if target_url_override else str(settings.scrapers_specific.clubtickets.target_url)
    actual_max_pages = max_pages_override if max_pages_override is not None else settings.scrapers_specific.clubtickets.max_pages_to_process

    run_logger.info(f"Targeting URL: {actual_target_url}, Max pages: {actual_max_pages}")

    all_unified_events: List[UnifiedEvent] = []
    raw_events_data: List[Dict[str, Any]] = []

    try:
        with ClubTicketsV2Scraper() as scraper:
            raw_events_data = scraper.crawl_events(
                url=actual_target_url,
                max_pages_to_process=actual_max_pages
            )
        run_logger.info(f"Crawling complete. Raw event entries: {len(raw_events_data)}")

        if raw_events_data:
            run_logger.info("Mapping raw events to unified schema and calculating quality scores...")
            for raw_event in raw_events_data:
                if not isinstance(raw_event, dict):
                    run_logger.warning(f"Skipping non-dictionary raw event: {type(raw_event)}")
                    continue
                event_url_for_schema = raw_event.get("event_specific_url", raw_event.get("source_page_url", actual_target_url))

                try:
                    unified_event_item = map_to_unified_schema(
                        raw_data=raw_event,
                        source_platform="clubtickets.com-v2",
                        source_url=str(event_url_for_schema)
                    )
                    if unified_event_item:
                        quality_assessment_result = calculate_basic_quality_score(unified_event_item)
                        unified_event_item.quality_assessment = quality_assessment_result
                        run_logger.debug(f"Event '{unified_event_item.event_details.title}' quality: {quality_assessment_result['overall_score']}")

                        all_unified_events.append(unified_event_item)
                    else:
                        run_logger.warning(f"Mapping to unified schema returned None for raw event: {raw_event.get('title', 'N/A')}")
                except Exception as mapping_error:
                    run_logger.error(f"Error mapping or scoring event '{raw_event.get('title', 'N/A')}': {mapping_error}", exc_info=True)
            run_logger.info(f"Successfully mapped and scored {len(all_unified_events)} events.")

            if all_unified_events:
                run_logger.info(f"Saving {len(all_unified_events)} unified events to MongoDB...")
                save_unified_events_to_mongodb(
                    events=all_unified_events,
                    collection_name_override=collection_name_override,
                    logger_obj=run_logger
                )

                output_prefix = "clubtickets_v2_events"
                output_subfolder = settings.scrapers_specific.clubtickets.output_subfolder
                events_as_dicts = [event.model_dump(exclude_none=True) for event in all_unified_events]

                save_to_json_file(
                    data_to_save=events_as_dicts,
                    filename_prefix=output_prefix,
                    sub_folder=output_subfolder,
                    logger_obj=run_logger
                )
                save_to_csv_file(
                    data_to_save=events_as_dicts,
                    filename_prefix=output_prefix,
                    sub_folder=output_subfolder,
                    logger_obj=run_logger
                )
        else:
            run_logger.info("No raw events collected by the scraper.")

    except Exception as main_exec_err:
        run_logger.critical(f"Main execution error in run_clubtickets_scraper: {main_exec_err}", exc_info=True)
        # Sentry will capture this if initialized and the error is not caught by a more specific try-except
        # that doesn't re-raise or explicitly capture to Sentry.
    finally:
        run_logger.info(f"ClubTickets V2 Scraper execution finished. Collected {len(all_unified_events)} unified events.")
    return all_unified_events


if __name__ == "__main__":
    # init_sentry() # Call Sentry init here as well if running script directly for testing
    # Note: If run_clubtickets_scraper is the main entry point for Prefect, init_sentry() inside it is sufficient.
    # For direct script execution, calling it here ensures Sentry is active for the __main__ block too.
    # However, the current structure calls it inside run_clubtickets_scraper, which is fine.

    main_run_logger = setup_logger("ClubTicketsV2MainDirectRun", "clubtickets_v2_direct_run")
    main_run_logger.info("Starting ClubTicketsV2Scraper directly for testing...")

    results = run_clubtickets_scraper()

    main_run_logger.info(f"Direct test run completed. {len(results)} events processed.")
    if results:
        main_run_logger.info("Sample of first event (if any, including quality assessment):")
        main_run_logger.info(json.dumps(results[0].model_dump(exclude_none=True), indent=2, default=str))
    logging.shutdown()
```
