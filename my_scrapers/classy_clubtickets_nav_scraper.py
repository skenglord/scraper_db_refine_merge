#!/usr/bin/env python3
"""
Refactored ClubTickets.com Scraper - Minimal Shell for Restoration
Date: 2025-06-10
"""

import re
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, ElementHandle

# Unified utilities and schema adapter
# Ensure these are importable from the execution context
from my_scrapers.utils.scraper_utils import (
    setup_logger,
    save_to_mongodb,
    save_to_json_file,
    save_to_csv_file,
    save_to_markdown_file
)
from schema_adapter import map_to_unified_schema


# Default Configuration (Minimal for now)
DEFAULT_CONFIG = {
    "headless": True, "slow_mo": 30,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "viewport_width": 1280, "viewport_height": 720,
    "max_retries": 2, "retry_delay_sec": 0.8,
    "random_short_delay_sec_min": 0.1, "random_short_delay_sec_max": 0.4,
    "random_long_delay_sec_min": 0.8, "random_long_delay_sec_max": 2.0,
    "output_dir": "output/clubtickets_test",
    "log_dir": "scraper_logs/clubtickets_test", # Default log dir for the class
    "mongodb_uri": "mongodb://localhost:27017/",
    "db_name": "clubtickets_test_db",
    "collection_name": "clubtickets_events_test",
    "event_card_selector": ".content-text-card",
    "date_tab_xpath": "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"btn-custom-day-tab\", \" \" ))]", # Restored
    "show_more_xpath": "//button[contains(concat(' ', normalize-space(@class), ' '), ' btn-more-events ') and contains(concat(' ', normalize-space(@class), ' '), ' more-events ') and text()='Show more events']", # Added from original
    "EVENT_SELECTORS_CLUBTICKETS": {
        "title": "h3.title-event",
        "url_anchor": "a",
        "date_text": "span.date-day-month",
        "venue_name": "span.club-name",
        "image": "img.img-responsive",
        "price": "span.price"
    }
}

class ClubTicketsScraper:
    def __init__(self, config_overrides: Optional[Dict[str, Any]] = None):
        temp_config = DEFAULT_CONFIG.copy()
        if config_overrides: temp_config.update(config_overrides)
        self.config = temp_config
        
        self.logger = setup_logger("ClubTicketsScraper", "clubtickets_scrape_run", log_dir=self.config["log_dir"])

        self.playwright_instance: Optional[sync_playwright] = None
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.logger.info(f"ClubTicketsScraper initialized. Effective config (output_dir): {self.config['output_dir']}")

    def __enter__(self):
        self.logger.info("Starting Playwright...")
        self.playwright_instance = sync_playwright().start()
        try:
            self.browser = self.playwright_instance.chromium.launch(
                headless=self.config.get("headless", True),
                slow_mo=self.config.get("slow_mo", 50),
                args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"]
            )
            self.logger.info("Playwright browser launched.")
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

    def _quick_delay(self, min_s: float = 0.1, max_s: float = 0.4):
        time.sleep(random.uniform(min_s, max_s))

    def random_delay(self, short: bool = True):
        min_d, max_d = ((self.config["random_short_delay_sec_min"], self.config["random_short_delay_sec_max"])
                        if short else (self.config["random_long_delay_sec_min"], self.config["random_long_delay_sec_max"]))
        time.sleep(random.uniform(min_d, max_d))

    def retry_action(self, action, description, is_critical=True) -> bool:
        for attempt in range(1, self.config.get("max_retries", 3) + 1):
            try:
                self.logger.debug(f"Attempt {attempt}/{self.config.get('max_retries', 3)}: {description}")
                action()
                self.logger.debug(f"Successfully performed: {description}")
                return True
            except PlaywrightTimeoutError as e:
                self.logger.warning(f"Timeout on attempt {attempt} for {description}: {e}")
            except Exception as e:
                self.logger.warning(f"Error on attempt {attempt} for {description}: {e}", exc_info=True)
            if attempt < self.config.get('max_retries', 3):
                time.sleep(self.config.get("retry_delay_sec", 1.0))
        
        log_func = self.logger.critical if is_critical else self.logger.error
        log_func(f"Failed to perform {description} after {self.config.get('max_retries', 3)} attempts")
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
                user_agent=self.config.get("user_agent"),
                viewport={
                    "width": self.config.get("viewport_width", 1280),
                    "height": self.config.get("viewport_height", 720)
                }
            )
            self.page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.logger.debug(f"New page created for navigation to {url}.")
        except Exception as e:
            self.logger.error(f"Failed to create new page for {url}: {e}", exc_info=True)
            return False

        success = self.retry_action(
            lambda: self.page.goto(url, wait_until="domcontentloaded", timeout=60000),
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
        card_debug_text = (card_element.text_content() or "").strip()[:150].replace("\n", " ")
        self.logger.debug(f"Attempting to parse event card. Base URL: {base_url}. Card text preview: '{card_debug_text}'")
        
        event_details: Dict[str, Any] = {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_page_url": base_url, # URL of the page where the card was found
            # Initialize all expected fields from selectors to None for consistent structure
            "title": None,
            "event_specific_url": None,
            "date_text": None,
            "venue_name": None,
            "image_url": None,
            "price_info": None
        }
        
        cfg_selectors = self.config.get("EVENT_SELECTORS_CLUBTICKETS", {})
        if not cfg_selectors:
            self.logger.error("EVENT_SELECTORS_CLUBTICKETS is not defined in config. Cannot parse card details.")
            return None

        for field, selector_str in cfg_selectors.items():
            if not selector_str:
                self.logger.debug(f"No selector provided for field '{field}'. Skipping.")
                continue
            try:
                element = card_element.query_selector(selector_str)
                if element:
                    text_content_value = (element.text_content() or "").strip()
                    if field == "url_anchor":
                        raw_url = element.get_attribute('href')
                        if raw_url:
                            event_details["event_specific_url"] = urljoin(base_url, raw_url)
                            self.logger.debug(f"Extracted 'event_specific_url': {event_details['event_specific_url']}")
                        else:
                            self.logger.debug(f"Field 'url_anchor': found element but 'href' attribute was empty or None.")
                    elif field == "image":
                        raw_img_src = element.get_attribute('src')
                        if raw_img_src:
                            event_details["image_url"] = urljoin(base_url, raw_img_src)
                            self.logger.debug(f"Extracted 'image_url': {event_details['image_url']}")
                        else:
                            self.logger.debug(f"Field 'image': found element but 'src' attribute was empty or None.")
                    elif field == "price":
                         price_text = text_content_value
                         # Improved regex to capture various price formats, including ranges or "From X"
                         price_match = re.search(r'(?:From\s*)?€?\$?\£?([\d\.,]+)(?:\s*-\s*€?\$?\£?[\d\.,]+)?', price_text, re.IGNORECASE)
                         if price_match:
                             event_details["price_info"] = price_match.group(1) # Store the first price found, or full range as text
                         else:
                             event_details["price_info"] = price_text # Store original text if no clear number
                         self.logger.debug(f"Extracted 'price_info': {event_details['price_info']} (from raw: '{price_text}')")
                    else:
                        event_details[field] = text_content_value
                        self.logger.debug(f"Extracted '{field}': {text_content_value[:100]}")
                else:
                    self.logger.debug(f"Selector for field '{field}' ('{selector_str}') did not find any element on this card.")
            except PlaywrightTimeoutError:
                self.logger.warning(f"Timeout while querying selector '{selector_str}' for field '{field}' on card.")
            except Exception as e:
                self.logger.error(f"Error extracting field '{field}' with selector '{selector_str}': {e}", exc_info=True)
        
        # Basic validation: A title is usually essential.
        if not event_details.get("title"):
            self.logger.warning(f"Event card parsed with no title. Base URL: {base_url}. Card text preview: '{card_debug_text}'. Skipping this card.")
            return None

        # Set a more generic 'source_url' for the event, preferring specific event URL.
        event_details["source_url"] = event_details["event_specific_url"] if event_details["event_specific_url"] else base_url

        self.logger.info(f"Successfully parsed event card: '{event_details.get('title', 'N/A')}' from {event_details['source_url']}")
        return event_details

    def process_current_page_events(self, date_context="current") -> List[Dict[str, Any]]:
        if not self.page:
            self.logger.error("Page not available for processing current page events.")
            return []
        
        current_page_url = self.page.url
        self.logger.info(f"Processing current page events for context: {date_context}, URL: {current_page_url}")
        card_selector = self.config.get("event_card_selector", ".event-card-class-fallback")
        
        if not self.retry_action(
            lambda: self.page.wait_for_selector(card_selector, state="attached", timeout=10000),
            f"Wait for event cards using selector '{card_selector}' for context '{date_context}'",
            is_critical=False):
            self.logger.warning(f"Event cards not found for '{date_context}' using selector '{card_selector}'.")
            return []
        
        try:
            card_elements = self.page.locator(card_selector).all() # Corrected from all_element_handles()
        except Exception as e:
            self.logger.error(f"Error locating event cards with selector '{card_selector}': {e}", exc_info=True)
            return []

        self.logger.info(f"Found {len(card_elements)} potential event card elements for '{date_context}'.")
        parsed_events_on_page: List[Dict[str, Any]] = []
        for i, card_locator in enumerate(card_elements): # Iterating over Locators
            self._quick_delay(min_s=0.05, max_s=0.15)
            try:
                card_handle = card_locator.element_handle() # Get ElementHandle from Locator
                if card_handle:
                    event_data = self.parse_event_card_details(card_handle, base_url=current_page_url)
                    if event_data:
                        event_data["page_context"] = date_context
                        parsed_events_on_page.append(event_data)
                else:
                    self.logger.warning(f"Could not get ElementHandle for card {i} in context '{date_context}'")
            except Exception as e_card_handle:
                 self.logger.error(f"Error getting ElementHandle or parsing card {i} in context '{date_context}': {e_card_handle}", exc_info=True)
        
        self.logger.info(f"Successfully parsed {len(parsed_events_on_page)} events from page context '{date_context}'.")
        return parsed_events_on_page

    def crawl_events(self, url: str, max_pages_to_process: int = 1) -> List[Dict[str, Any]]:
        self.logger.info(f"Starting crawl of: {url}, Max date tabs to process: {max_pages_to_process}")
        all_scraped_raw_events: List[Dict[str, Any]] = []
        
        if not self.navigate_to(url) or not self.page:
            self.logger.error(f"Initial navigation to {url} failed or page became unavailable.")
            return all_scraped_raw_events

        self.random_delay(short=False)
        for i in range(random.randint(1,2)):
            self.page.evaluate(f"window.scrollBy(0, {random.randint(200, 350)})")
            self.logger.debug(f"Performed scroll {i+1} on {self.page.url}")
            self._quick_delay(0.2, 0.4)
        
        show_more_xpath = self.config.get("show_more_xpath")
        if show_more_xpath:
            try:
                show_more_button = self.page.locator(show_more_xpath)
                if show_more_button.is_visible(timeout=5000):
                    self.logger.info("'Show more events' button is visible. Attempting click.")
                    if self.retry_action(lambda: show_more_button.click(timeout=8000), "Click 'Show more events' button"):
                        self.page.wait_for_load_state("networkidle", timeout=10000)
                        self.random_delay(short=True)
                else: self.logger.info("'Show more events' button not found/visible.")
            except Exception as e: self.logger.warning(f"Issue with 'Show more events': {e}", exc_info=True)
        
        all_scraped_raw_events.extend(self.process_current_page_events(date_context="initial_page"))

        date_tab_xpath = self.config.get("date_tab_xpath")
        if not date_tab_xpath: # Check if the key exists and is not empty
            self.logger.warning("Date tab XPath not configured or empty. Skipping tab processing.")
            return all_scraped_raw_events

        date_tabs_locators = self.page.locator(date_tab_xpath).all() # Get list of Locators
        self.logger.info(f"Found {len(date_tabs_locators)} date tabs. Will process up to {max_pages_to_process} tabs.")

        processed_tabs_count = 0
        for i, tab_locator in enumerate(date_tabs_locators):
            if processed_tabs_count >= max_pages_to_process:
                self.logger.info(f"Reached max_pages_to_process ({max_pages_to_process}) for date tabs.")
                break
            tab_text_content = (tab_locator.text_content() or f"Tab_{i+1}").strip()
            self.logger.info(f"Processing date tab {i+1}/{len(date_tabs_locators)}: '{tab_text_content}'")
            
            if self.retry_action(lambda: tab_locator.click(timeout=10000), f"Click date tab: {tab_text_content}"):
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=20000)
                    self.page.wait_for_timeout(random.randint(1500,2500))
                    all_scraped_raw_events.extend(self.process_current_page_events(date_context=f"date_tab_{tab_text_content}"))
                    processed_tabs_count +=1
                except Exception as e_tab_proc:
                    self.logger.error(f"Error processing events for tab '{tab_text_content}': {e_tab_proc}", exc_info=True)
            else: self.logger.warning(f"Failed to click date tab: {tab_text_content}. Skipping.")
            self.random_delay(short=False)
            
        self.logger.info(f"Crawl_events completed. Total raw events: {len(all_scraped_raw_events)}")
        return all_scraped_raw_events

if __name__ == "__main__":
    main_logger = setup_logger("ClubTicketsMainExecution", "clubtickets_main_refactored_run",
                               log_dir=DEFAULT_CONFIG["log_dir"])
    main_logger.info("Starting ClubTickets Scraper refactored main execution...")

    test_run_config_overrides = {
        "headless": True,
        "max_pages_to_process": 1,
        "mongodb_uri": "mongodb://localhost:27017/",
        "db_name": "clubtickets_test_db",
        "collection_name": "clubtickets_test_output",
        "output_dir": "output/clubtickets_test_run",
        "log_dir": "scraper_logs/clubtickets_test_run"
    }
    main_logger.info(f"Test run configuration overrides: {test_run_config_overrides}")
    
    Path(test_run_config_overrides["output_dir"]).mkdir(parents=True, exist_ok=True)
    Path(test_run_config_overrides["log_dir"]).mkdir(parents=True, exist_ok=True)
    main_logger = setup_logger("ClubTicketsMainExecution", "clubtickets_main_refactored_run",
                               log_dir=test_run_config_overrides["log_dir"])


    all_event_data_unified: List[Dict[str, Any]] = []
    try:
        with ClubTicketsScraper(config_overrides=test_run_config_overrides) as scraper:
            target_url = "https://www.clubtickets.com/search?dates=31%2F05%2F25+-+01%2F11%2F25"
            main_logger.info(f"Targeting URL: {target_url}")
            
            raw_events_data = scraper.crawl_events(
                url=target_url,
                max_pages_to_process=scraper.config.get("max_pages_to_process", 1)
            )
            main_logger.info(f"Crawling complete. Raw event entries: {len(raw_events_data)}")

            if raw_events_data:
                main_logger.info("Mapping raw events to unified schema...")
                for raw_event in raw_events_data:
                    if not isinstance(raw_event, dict):
                        main_logger.warning(f"Skipping non-dictionary raw event: {type(raw_event)}")
                        continue
                    event_url_for_schema = raw_event.get("event_specific_url", raw_event.get("source_page_url", target_url))
                    try:
                        unified_event_item = map_to_unified_schema(
                            raw_data=raw_event, source_platform="clubtickets.com", source_url=event_url_for_schema
                        )
                        if unified_event_item: all_event_data_unified.append(unified_event_item)
                        else: main_logger.warning(f"Mapping returned None for: {raw_event.get('title')}")
                    except Exception as mapping_error:
                        main_logger.error(f"Error mapping event '{raw_event.get('title')}': {mapping_error}", exc_info=True)
                main_logger.info(f"Successfully mapped {len(all_event_data_unified)} events.")

                if all_event_data_unified:
                    main_logger.info("Saving unified event data...")
                    output_prefix = "clubtickets_events_test"
                    current_output_dir = scraper.config["output_dir"]

                    save_to_json_file(all_event_data_unified, output_prefix, output_dir=current_output_dir, logger_obj=main_logger)
                    save_to_csv_file(all_event_data_unified, output_prefix, output_dir=current_output_dir, logger_obj=main_logger)
                    save_to_markdown_file(all_event_data_unified, output_prefix, output_dir=current_output_dir, logger_obj=main_logger)

                    if scraper.config["mongodb_uri"] and scraper.config["mongodb_uri"] != "YOUR_MONGODB_URI": # Check against placeholder
                        main_logger.info(f"Saving {len(all_event_data_unified)} events to MongoDB...")
                        save_to_mongodb(
                            data_list=all_event_data_unified, mongodb_uri=scraper.config["mongodb_uri"],
                            db_name=scraper.config["db_name"], collection_name=scraper.config["collection_name"],
                            logger_obj=main_logger
                        )
                    else: main_logger.warning("MongoDB URI is placeholder/not set or is 'YOUR_MONGODB_URI'. Skipping MongoDB save.")
            else: main_logger.info("No raw events collected.")
    except Exception as main_exec_err:
        main_logger.critical(f"Main execution error: {main_exec_err}", exc_info=True)
    finally:
        main_logger.info(f"Execution finished. Unified events: {len(all_event_data_unified)}")
        import logging as pylogging_main
        pylogging_main.shutdown()
