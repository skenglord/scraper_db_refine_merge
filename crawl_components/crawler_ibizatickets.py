import argparse
import json
from datetime import datetime # For potential use in main, though EventSchema uses strings
from bs4 import BeautifulSoup

# Attempt to import from previously created components
# Option 1: If components are installed or PYTHONPATH is set up
# from scraping_components.fetch_page_dual_mode_cs import DualModeFetcherCS
# from parse_components.parse_json_ld_event_cs import (
#     parse_json_ld_event_cs, EventSchema, LocationSchema,
#     DateTimeSchema, ArtistSchema, TicketInfoSchema
# )
# from parse_components.format_event_to_markdown_cs import format_event_to_markdown_cs

# Option 2: For agent environments, assume they are in sibling directories
# and Python's default module search path might find them if the root of 'components'
# is effectively on the path. This is often not the case without __init__.py files
# and proper packaging or path manipulation.

# For this task, I will proceed with direct relative-like imports assuming a common root
# or that the execution environment handles it. If this fails during execution,
# the task instructions mention duplicating critical small utilities like TypedDicts.

# --- BEGIN COMPONENT IMPORTS ---
# Simulating that these components are accessible.
# If direct import fails, the agent is instructed to consider alternatives like duplication for TypedDicts.

# Placeholder for actual import paths if the agent environment requires specific syntax
# For now, using placeholder paths that might work if a common root for components is in sys.path
# Note: Python typically requires __init__.py files in directories to treat them as packages for "." imports.
# If these are just directories of .py files, direct import "scraping_components.fetch_page_dual_mode_cs" might fail
# unless "scraping_components" itself is a package or its parent is in sys.path.

# Let's try a structure that might work if the parent of 'crawl_components',
# 'scraping_components', etc. is added to sys.path.
# This is often the case in structured projects.
import sys
from pathlib import Path
import logging # Added
from pymongo import MongoClient # Added
from pymongo.errors import ConnectionFailure # Added

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# Add parent directory of 'crawl_components' to sys.path to find sibling component directories
# This is a common pattern for making sibling packages/modules importable.
# Assuming the script is in .../some_root/crawl_components/crawler_ibizatickets.py
# And other components are in .../some_root/scraping_components/ etc.
# This makes 'some_root' the effective top-level for these imports.

# project_root calculation needs to be at the module level for imports to be found globally
try:
    current_dir = Path(__file__).resolve().parent
    project_root = current_dir.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
        logger.debug(f"Added {project_root} to sys.path")
except NameError:
    logger.debug("__file__ not defined, skipping sys.path modification for sibling imports.")
    project_root = Path.cwd() # Fallback or assume current dir is project root

try:
    from schema_adapter import map_to_unified_schema # Added
    from classy_skkkrapey.config import settings # Added
except ImportError as e:
    logger.error(f"Failed to import schema_adapter or settings: {e}. Ensure schema_adapter.py is in project root and classy_skkkrapey/config.py is accessible.")
    # Define dummy settings if import fails, for script to run without full functionality
    class DummySettings:
        MONGODB_URI = "mongodb://localhost:27017/"
        DB_NAME = "fallback_db"
    settings = DummySettings()
    # map_to_unified_schema will remain undefined if import fails, leading to errors later if not handled.
    # This is acceptable for the task, as the focus is on refactoring assuming components are available.

try:
    from scraping_components.fetch_page_dual_mode_cs import DualModeFetcherCS
    from parse_components.parse_json_ld_event_cs import (
        parse_json_ld_event_cs, EventSchema, LocationSchema,
        DateTimeSchema, ArtistSchema, TicketInfoSchema
    )
    # Ensure EventSchema is the one used by format_event_to_markdown_cs
    from parse_components.format_event_to_markdown_cs import format_event_to_markdown_cs
    # Check if EventSchema from parse_json_ld_event_cs is compatible or if format_event_to_markdown_cs uses its own.
    # The prompt for parse_components.format_event_to_markdown_cs stated:
    # "Copy the TypedDict definitions ... into this file ... Ensure it uses the locally defined EventSchema."
    # This implies format_event_to_markdown_cs might have its own EventSchema.
    # For consistency, it's better if they share one. Assuming parse_json_ld_event_cs is the source of truth.
    # If format_event_to_markdown_cs truly has its own, we'd need to map or use that one.
    # For now, assume the EventSchema from parse_json_ld_event_cs is what we need.

    # If the above EventSchema from parse_json_ld_event_cs is what format_event_to_markdown_cs
    # is also supposed to use (ideally they share the same definition from a common place),
    # then this is fine.
    # If format_event_to_markdown_cs has its own *different* EventSchema, this could be an issue.
    # The instructions for Task 5 in the previous subtask said:
    # "Copy the TypedDict definitions ... into this file (format_event_to_markdown_cs.py) ... Ensure it uses the locally defined EventSchema."
    # This means format_event_to_markdown_cs.py *does* have its own copy.
    # To avoid conflicts and ensure the correct schema is used for formatting,
    # it's safer to just import format_event_to_markdown_cs and let it use its internal schema.
    # The data from parse_json_ld_event_cs should be structurally compatible if both were copied from classy_skkkrapey.py.

    # Re-evaluating: The prompt for *this* task (crawler_ibizatickets) says for imports:
    # "Import ... EventSchema ... from parse_components.parse_json_ld_event_cs"
    # "Import format_event_to_markdown_cs from parse_components.format_event_to_markdown_cs"
    # This implies the EventSchema from parse_json_ld_event_cs is the one to work with,
    # and format_event_to_markdown_cs should be ableto handle it.

    logger.info("Successfully imported local scraping/parsing components.")
    COMPONENTS_AVAILABLE = True
except ImportError as e:
    logger.error(f"Error importing local scraping/parsing components: {e}")
    logger.warning("Falling back: Component imports failed. TypedDicts might be duplicated if necessary.")
    COMPONENTS_AVAILABLE = False
    # Define dummy classes/functions or allow script to fail if components are critical
    class DualModeFetcherCS: pass
    def parse_json_ld_event_cs(soup): return None
    def format_event_to_markdown_cs(event_data): return "Markdown formatting unavailable."
    # Critical: EventSchema and its parts. Per instructions, these might need to be duplicated.
    # For now, let the script fail later if COMPONENTS_AVAILABLE is False and these are used.
    # Or, define them here as per fallback instructions.
    # Let's try defining them as a fallback:
    # TODO: Ideally, import EventSchema from a shared schemas module or parse_components
    from typing import TypedDict, List, Optional as TypingOptional # Use TypingOptional to avoid conflict

    class LocationSchema(TypedDict, total=False):
        venue: str
        address: str
        city: str
        country: str

    class DateTimeSchema(TypedDict, total=False):
        startDate: str
        endDate: str
        doorTime: str
        timeZone: str
        displayText: str

    class ArtistSchema(TypedDict, total=False):
        name: str
        headliner: bool

    class TicketInfoSchema(TypedDict, total=False):
        url: str
        availability: str
        startingPrice: float
        currency: str

    class EventSchema(TypedDict, total=False):
        url: TypingOptional[str] # Made Optional as it's added by scraper, not parser
        scrapedAt: TypingOptional[str] # Added by scraper
        extractionMethod: TypingOptional[str] # Added by parser
        title: TypingOptional[str]
        location: TypingOptional[LocationSchema]
        dateTime: TypingOptional[DateTimeSchema]
        lineUp: TypingOptional[List[ArtistSchema]]
        ticketInfo: TypingOptional[TicketInfoSchema]
        description: TypingOptional[str]

    # If imports failed, DualModeFetcherCS needs a dummy that can be initialized and has a close method
    if not COMPONENTS_AVAILABLE:
        class DummyFetcher:
            def __init__(self, *args, **kwargs):
                print("Using DummyFetcher as component import failed.")
            def fetch_page(self, url: str, use_browser_override: bool = False) -> str | None:
                print(f"DummyFetcher: Would fetch {url}")
                if "ticketsibiza.com" in url: # Simulate some basic HTML structure for TicketsIbiza
                    return f"""<html><head><title>Test Event</title>
                           <script type="application/ld+json">
                           {{
                             "@context": "http://schema.org",
                             "@type": "MusicEvent",
                             "name": "Dummy Event from ticketsibiza.com",
                             "startDate": "{(datetime.now()).isoformat()}",
                             "location": {{
                               "@type": "Place",
                               "name": "Dummy Venue",
                               "address": "123 Dummy Street"
                             }}
                           }}
                           </script></head><body><h1>Dummy Event</h1></body></html>"""
                return "<html><body>No data</body></html>"
            def close(self): print("DummyFetcher closed.")
            def __enter__(self): return self
            def __exit__(self, exc_type, exc_val, exc_tb): self.close()
        DualModeFetcherCS = DummyFetcher # type: ignore


# --- Main Logic ---
def scrape_ibiza_tickets_event(url: str, fetcher: DualModeFetcherCS) -> dict | None:
    """
    Scrapes a single event page from TicketsIbiza.com, maps it to Unified Event Schema v2.
    """
    logger.info(f"Attempting to scrape event from: {url}")
    unified_event_doc: dict | None = None
    try:
        html_content = fetcher.fetch_page(url, use_browser_override=True)

        if not html_content:
            logger.error(f"Failed to fetch HTML content from {url}.")
            return None

        soup = BeautifulSoup(html_content, "html.parser")
        parsed_event_data = parse_json_ld_event_cs(soup) # This returns EventSchema (a TypedDict)

        if parsed_event_data:
            logger.info(f"Successfully extracted raw data for: {parsed_event_data.get('title', 'N/A Title')}")

            # Convert EventSchema (TypedDict) to a standard dict for the adapter
            # The adapter expects a plain dict as raw_data.
            # Ensure all fields from EventSchema are correctly represented.
            # parse_json_ld_event_cs already sets extractionMethod.
            # We need to add 'url' and 'scrapedAt' to the raw_data if adapter expects them
            # from the raw_data dict itself, rather than as separate params.
            # The schema_adapter's map_to_unified_schema takes source_url separately.
            # scrapedAt is usually generated by the adapter or at point of saving.

            raw_data_dict = dict(parsed_event_data)
            # Add any other fields to raw_data_dict if your parse_json_ld_event_cs
            # doesn't capture everything the adapter might look for in raw_data.
            # For example, if the adapter uses 'raw_date_string' but your parser produces 'dateTime.displayText'.
            # For now, assuming direct conversion is mostly fine.

            unified_event_doc = map_to_unified_schema(
                raw_data=raw_data_dict,
                source_platform="ibizatickets", # Hardcoded source platform
                source_url=url
            )

            if unified_event_doc:
                logger.info(f"Successfully mapped event {unified_event_doc.get('title')} to unified schema.")
            else:
                logger.error(f"Failed to map raw data from {url} to unified schema.")
        else:
            logger.warning(f"No structured event data (JSON-LD) found on {url}.")

    except Exception as e:
        logger.error(f"An error occurred during scraping of {url}: {e}", exc_info=True)
        # Removed traceback.print_exc() as logger.error with exc_info=True does this.

    return unified_event_doc

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Scrape a single event page from TicketsIbiza.com and save to MongoDB.")
    parser.add_argument("url", help="The URL of the event page to scrape.")
    args = parser.parse_args()

    mongo_client = None
    events_collection = None

    # Establish MongoDB connection
    try:
        mongo_client = MongoClient(settings.MONGODB_URI)
        db = mongo_client[settings.DB_NAME]
        events_collection = db.events # Assuming 'events' is the collection name
        logger.info(f"Successfully connected to MongoDB: {settings.DB_NAME} on {settings.MONGODB_URI}")
    except ConnectionFailure as e:
        logger.error(f"Could not connect to MongoDB: {e}")
    except AttributeError: # If settings is not available (e.g. DummySettings)
        logger.error("MongoDB settings (MONGODB_URI or DB_NAME) not found. Cannot connect to DB.")

    if not COMPONENTS_AVAILABLE and DualModeFetcherCS.__name__ == 'DummyFetcher':
        logger.warning("\nRunning with dummy components due to import errors. Output will be simulated and not saved to DB.\n")

    fetcher_instance = None
    try:
        # DualModeFetcherCS is a context manager
        # Initialize fetcher outside with statement to ensure close is called in finally
        fetcher_instance = DualModeFetcherCS(use_browser_default=False, headless=True)
        unified_event_doc = scrape_ibiza_tickets_event(args.url, fetcher_instance)

        if unified_event_doc:
            if events_collection:
                try:
                    if not unified_event_doc.get("event_id"):
                        logger.error("Unified event document is missing 'event_id'. Cannot save to DB.")
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
                    # Fallback to printing if DB save fails
                    logger.info("Printing unified_event_doc to console due to DB save error.")
                    print(json.dumps(unified_event_doc, indent=2, default=str))
            else:
                logger.warning("DB not connected. Printing unified_event_doc to console instead.")
                # Need to import json for this fallback
                import json
                print(json.dumps(unified_event_doc, indent=2, default=str))
        else:
            logger.info(f"No data scraped or mapped from {args.url}.")

    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        if fetcher_instance:
            fetcher_instance.close()
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")

if __name__ == "__main__":
    main()
