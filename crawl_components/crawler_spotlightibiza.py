import argparse
import json
from datetime import datetime
from bs4 import BeautifulSoup
import re # For any regex needs in simplified parsing
from dataclasses import dataclass, asdict, field # field for default_factory
from typing import List, Optional

# Attempt to import from previously created components
import sys
from pathlib import Path
import logging # Added
from pymongo import MongoClient # Added
from pymongo.errors import ConnectionFailure # Added
from dataclasses import asdict # Added

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


if __name__ == '__main__': # Guard this path manipulation
    try:
        current_dir = Path(__file__).resolve().parent
        project_root = current_dir.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            logger.debug(f"Added {project_root} to sys.path for crawler_spotlightibiza.py")
    except NameError:
        logger.debug("__file__ not defined, skipping sys.path modification for crawler_spotlightibiza.py.")
        project_root = Path.cwd() # Fallback

try:
    from schema_adapter import map_to_unified_schema # Added
    from classy_skkkrapey.config import settings # Added
except ImportError as e:
    logger.error(f"Failed to import schema_adapter or settings: {e}. Ensure schema_adapter.py is in project root and classy_skkkrapey/config.py is accessible.")
    class DummySettings: # Fallback settings
        MONGODB_URI = "mongodb://localhost:27017/"
        DB_NAME = "fallback_db_spotlight"
    settings = DummySettings()
    # map_to_unified_schema will be missing if this fails, handle downstream.

try:
    from stealth_components.playwright_stealth_integration_fs import launch_stealth_browser
    from stealth_components.playwright_setup_enhancements_us import setup_enhanced_playwright_page, MODERN_USER_AGENTS_SUBSET
    from stealth_components.playwright_handle_overlays_us import handle_overlays
    # human_click might not be directly called by this simplified scraper, but handle_overlays might use it.
    # from stealth_components.playwright_human_click_us import human_click
    from stealth_components.random_delay_util_us import get_random_delay

    # For Playwright types if not fully covered by launch_stealth_browser return types
    from playwright.sync_api import Page as PlaywrightPage, Browser as PlaywrightBrowser, PlaywrightException
    # Playwright instance type is not explicitly exported by playwright.sync_api but is implicitly Playwright an object from sync_playwright()
    # Let's use 'Any' or a placeholder if needed for pw_instance type hint.
    from typing import Any as PlaywrightInstanceType

    print("Successfully imported spotlight crawler components.") # Debug print
    STEALTH_COMPONENTS_AVAILABLE = True
except ImportError as e:
    print(f"Error importing stealth components for Spotlight crawler: {e}")
    print("Falling back: Stealth component imports failed. Script will likely not run correctly.")
    STEALTH_COMPONENTS_AVAILABLE = False
    # Define dummy versions or let it fail
    def launch_stealth_browser(headless: bool = True, browser_type: str = "chromium", **kwargs): return None, None, None
    def setup_enhanced_playwright_page(page, **kwargs): pass
    MODERN_USER_AGENTS_SUBSET = ["Mozilla/5.0 Dummy UA"]
    def handle_overlays(page, **kwargs): return False
    def get_random_delay(min_s, max_s, **kwargs): import time; time.sleep(0.1)
    class PlaywrightPage: pass # Dummy
    class PlaywrightBrowser: pass # Dummy
    class PlaywrightException(Exception): pass
    PlaywrightInstanceType = None


# --- Simple Event Dataclass ---
@dataclass
class SpotlightEvent:
    url: str
    title: Optional[str] = None
    date_text: Optional[str] = None # Raw date string from page
    venue: Optional[str] = None
    raw_description: Optional[str] = None # Store a chunk of text as description
    main_content_text: Optional[str] = None # Alternative to raw_description
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    extraction_method: str = "spotlight_simplified_html"

    def to_dict(self) -> dict:
        data = asdict(self)
        # Ensure datetime is ISO format string for JSON serialization and adapter consumption
        if isinstance(self.scraped_at, datetime):
            data['scraped_at'] = self.scraped_at.isoformat() + "Z"
        return data

# --- Core Scraping Logic ---
def scrape_spotlight_event_page(page: PlaywrightPage, url: str) -> dict | None:
    """
    Scrapes a single event detail page from Ibiza Spotlight using Playwright.
    Extracts raw data, then maps it using schema_adapter.
    Returns a unified event document (dictionary) or None.
    """
    logger.info(f"Attempting to scrape Spotlight event from: {url}")

    try:
        logger.info(f"Navigating to {url}...")
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        get_random_delay(0.5, 1.5)

        logger.info("Handling overlays...")
        handle_overlays(page)

        get_random_delay(1.0, 2.5)

        logger.info("Extracting page content...")
        html_content = page.content()
        soup = BeautifulSoup(html_content, "html.parser")

        # Populate a temporary SpotlightEvent dataclass or a simple dict with raw fields
        # For this refactor, we'll use the existing SpotlightEvent dataclass then convert to dict
        temp_event_data = SpotlightEvent(url=url)

        title_tag = soup.select_one("h1.eventTitle, h1.article-title, article header h1, main h1")
        if title_tag:
            temp_event_data.title = title_tag.get_text(strip=True)
        else:
            logger.warning(f"No title found for {url}")

        date_info_tag = soup.select_one(".event-date, .date-display, time[datetime], .eventInfo .date") # Added another common selector
        if date_info_tag:
            temp_event_data.date_text = date_info_tag.get_text(strip=True)
            if not temp_event_data.date_text and date_info_tag.has_attr('datetime'):
                temp_event_data.date_text = date_info_tag['datetime']

        venue_link_tag = soup.select_one("a[href*='/club/'], .venue-name, .eventInfo .club") # Added another common selector
        if venue_link_tag:
            temp_event_data.venue = venue_link_tag.get_text(strip=True)

        description_area = soup.select_one("article .content, .event-details .description, #main_content, .articleText") # Added .articleText
        if description_area:
            temp_event_data.raw_description = description_area.get_text(separator="\n", strip=True)[:2000] # Increased limit slightly
        else:
            body_text = soup.body.get_text(separator="\n", strip=True) if soup.body else ""
            temp_event_data.main_content_text = body_text[:2000] if body_text else None
            if not body_text: logger.warning(f"Could not find description area or body text for {url}")


        if not temp_event_data.title:
            logger.warning(f"Essential data (title) missing for {url}. Cannot process for unified schema.")
            return None

        logger.info(f"Successfully extracted some raw data for: {temp_event_data.title or url}")

        # Convert SpotlightEvent dataclass instance to dictionary
        raw_event_dict = temp_event_data.to_dict()

        # Add any other fields needed by the adapter that are not in SpotlightEvent
        # For example, the adapter might expect 'promoter' or 'artists' list directly if available.
        # The current SpotlightEvent is very basic, so adapter might need to infer a lot.
        # For now, we pass what we have.

        # Call the adapter
        # Check if map_to_unified_schema was imported successfully
        if 'map_to_unified_schema' not in globals():
            logger.error("map_to_unified_schema is not available. Cannot map to unified schema.")
            return None # Or handle differently, e.g., return raw_event_dict for basic saving

        unified_event_doc = map_to_unified_schema(
            raw_data=raw_event_dict,
            source_platform="ibiza-spotlight-stealth", # Specific platform name
            source_url=url
        )

        if unified_event_doc:
            logger.info(f"Successfully mapped '{unified_event_doc.get('title')}' to unified schema.")
            return unified_event_doc
        else:
            logger.error(f"Schema mapping failed for {url}. unified_event_doc is None.")
            return None

    except PlaywrightException as e:
        logger.error(f"A Playwright error occurred while scraping {url}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while scraping {url}: {e}", exc_info=True)

    return None


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Scrape a single event page from Ibiza-Spotlight.com and save to MongoDB.")
    parser.add_argument("url", help="The URL of the event detail page to scrape.")
    parser.add_argument(
        "--headless",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run browser in headless mode. Use --no-headless to show browser."
    )
    args = parser.parse_args()

    mongo_client = None
    events_collection = None
    pw_instance = None
    browser = None

    try:
        # Establish MongoDB connection
        # Attempt to import settings if available (already handled at module level, this is for clarity)
        if hasattr(settings, 'MONGODB_URI') and hasattr(settings, 'DB_NAME'):
            try:
                mongo_client = MongoClient(settings.MONGODB_URI)
                db = mongo_client[settings.DB_NAME]
                events_collection = db.events
                logger.info(f"Successfully connected to MongoDB: {settings.DB_NAME}")
            except ConnectionFailure as e:
                logger.error(f"Could not connect to MongoDB: {e}")
            except AttributeError: # Should be caught by hasattr check, but as a safeguard
                 logger.error("MongoDB URI/DB_NAME not found in settings. Cannot connect to DB.")
        else:
            logger.warning("MongoDB settings not found (likely due to import error or dummy settings). Will print to console.")


        if not STEALTH_COMPONENTS_AVAILABLE:
            logger.fatal("Required stealth components are not available. Cannot proceed.")
            return

        logger.info(f"Launching browser (headless: {args.headless})...")
        pw_instance, browser, page = launch_stealth_browser(headless=args.headless)

        if not page or not browser:
            logger.error("Failed to launch Playwright browser or page. Exiting.")
            return

        logger.info("Setting up enhanced Playwright page...")
        setup_enhanced_playwright_page(
            page,
            user_agents_list=MODERN_USER_AGENTS_SUBSET,
            resource_types_to_block=["image", "font", "media"]
        )

        unified_event_doc = scrape_spotlight_event_page(page, args.url)

        if unified_event_doc:
            if events_collection:
                try:
                    if not unified_event_doc.get("event_id"):
                        logger.error("Unified event document is missing 'event_id'. Cannot save to DB.")
                        # Fallback to print if event_id is missing for some reason
                        logger.info("Printing unified_event_doc to console due to missing event_id.")
                        print(json.dumps(unified_event_doc, indent=2, default=str))
                    else:
                        update_key = {"event_id": unified_event_doc["event_id"]}
                        events_collection.update_one(
                            update_key,
                            {"$set": unified_event_doc},
                            upsert=True
                        )
                        logger.info(f"Successfully saved/updated event to DB: {unified_event_doc.get('title', unified_event_doc['event_id'])}")
                except Exception as e:
                    logger.error(f"Error saving event {unified_event_doc.get('event_id')} to MongoDB: {e}", exc_info=True)
                    logger.info("Printing unified_event_doc to console due to DB save error.")
                    print(json.dumps(unified_event_doc, indent=2, default=str)) # Ensure json is imported for this
            else:
                logger.warning("DB not connected. Printing unified_event_doc to console instead.")
                # Ensure json is imported if this path is taken
                import json
                print(json.dumps(unified_event_doc, indent=2, default=str))
        else:
            logger.info(f"No data scraped or processed for {args.url}.")

    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        logger.info("\nClosing browser and Playwright...")
        if browser:
            try:
                browser.close()
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
        if pw_instance:
            try:
                pw_instance.stop()
            except Exception as e:
                logger.error(f"Error stopping Playwright: {e}")

        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")
        logger.info("Cleanup finished.")

if __name__ == "__main__":
    main()
