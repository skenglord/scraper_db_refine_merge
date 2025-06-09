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

# Add parent directory of 'crawl_components' to sys.path to find sibling component directories
# This is a common pattern for making sibling packages/modules importable.
# Assuming the script is in .../some_root/crawl_components/crawler_ibizatickets.py
# And other components are in .../some_root/scraping_components/ etc.
# This makes 'some_root' the effective top-level for these imports.
if __name__ == '__main__': # Guard this path manipulation
    try:
        # Path to the 'crawl_components' directory
        current_dir = Path(__file__).resolve().parent
        # Path to the parent of 'crawl_components' (e.g., 'some_root')
        project_root = current_dir.parent
        if str(project_root) not in sys.path:
            sys.path.insert(0, str(project_root))
            # print(f"DEBUG: Added {project_root} to sys.path") # Optional debug
    except NameError: # __file__ is not defined (e.g. in a REPL or non-file execution)
        # print("DEBUG: __file__ not defined, skipping sys.path modification for sibling imports.")
        pass


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

    print("Successfully imported components.") # Debug print
    COMPONENTS_AVAILABLE = True
except ImportError as e:
    print(f"Error importing components: {e}")
    print("Falling back: Component imports failed. TypedDicts might be duplicated if necessary.")
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
def scrape_ibiza_tickets_event(url: str, fetcher: DualModeFetcherCS) -> EventSchema | None:
    """
    Scrapes a single event page from TicketsIbiza.com.
    """
    print(f"Attempting to scrape event from: {url}")
    event_data: EventSchema | None = None
    try:
        # Use browser for TicketsIbiza as dynamic content might be involved for some pages.
        # The original TicketsIbizaScraper in classy_skkkrapey used requests by default,
        # but BaseEventScraper (which DualModeFetcherCS is based on) had Playwright.
        # Forcing browser use here for robustness.
        html_content = fetcher.fetch_page(url, use_browser_override=True)

        if not html_content:
            print(f"Failed to fetch HTML content from {url}.")
            return None

        soup = BeautifulSoup(html_content, "html.parser")

        # parse_json_ld_event_cs is designed to find and parse EventSchema
        parsed_event_data = parse_json_ld_event_cs(soup)

        if parsed_event_data:
            # Add URL and scrapedAt time, as the parser component doesn't do this.
            # The EventSchema from parse_json_ld_event_cs.py already includes these fields (as optional).
            event_data = parsed_event_data
            event_data['url'] = url
            event_data['scrapedAt'] = datetime.utcnow().isoformat() + "Z"
            # extractionMethod is already set by parse_json_ld_event_cs

            print(f"Successfully extracted data for: {event_data.get('title', 'N/A Title')}")
            if COMPONENTS_AVAILABLE : # Only try to format if the real component is there
                try:
                    markdown_output = format_event_to_markdown_cs(event_data)
                    print("\n--- Markdown Output ---")
                    print(markdown_output)
                    print("-----------------------\n")
                except Exception as e_markdown:
                    print(f"Error formatting to markdown: {e_markdown}")
            else:
                 print("(Skipping markdown formatting due to component import failure)")
        else:
            print(f"No structured event data (JSON-LD) found on {url}.")

    except Exception as e:
        print(f"An error occurred during scraping of {url}: {e}")
        import traceback
        traceback.print_exc()

    return event_data

# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Scrape a single event page from TicketsIbiza.com.")
    parser.add_argument("url", help="The URL of the event page to scrape.")
    # Example: "https://www.ticketsibiza.com/en/ibiza-calendar-2024/amnesia/amnesia-closing-party-october-5th"
    # Add more arguments as needed, e.g., --output-file

    args = parser.parse_args()

    if not COMPONENTS_AVAILABLE and DualModeFetcherCS.__name__ == 'DummyFetcher':
        print("\nWARNING: Running with dummy components due to import errors. Output will be simulated.\n")

    # DualModeFetcherCS is a context manager
    with DualModeFetcherCS(use_browser_default=False, headless=True) as fetcher: # Default to not using browser unless specified by fetch_page
        scraped_data = scrape_ibiza_tickets_event(args.url, fetcher)

        if scraped_data:
            print("\n--- JSON Output ---")
            # Ensure all parts of EventSchema are serializable if they contain complex types not handled by default.
            # The current EventSchema uses basic types or lists/dicts of them, so json.dumps should be fine.
            try:
                print(json.dumps(scraped_data, indent=2, ensure_ascii=False))
            except TypeError as te:
                print(f"Error serializing to JSON: {te}. Ensure all EventSchema fields are JSON serializable.")
                print("Scraped data (raw):", scraped_data)

        else:
            print(f"No data scraped from {args.url}.")

if __name__ == "__main__":
    main()
