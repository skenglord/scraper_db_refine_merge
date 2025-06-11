# Ibiza Spotlight Scraper Documentation

## Purpose and Functionality

The Ibiza Spotlight Scraper is designed to extract event information from the Ibiza Spotlight website (ibiza-spotlight.com). It navigates through calendar pages, venue pages, and promoter pages to gather comprehensive data about events, including titles, dates, artists, and pricing.

This scraper is an enhanced and unified version that leverages Playwright for robust browser automation and stealth techniques to minimize detection. The collected data is then processed and can be stored in a MongoDB database, mapped to a unified schema for consistency across different data sources.

Key features include:
-   **Stealthy Scraping**: Utilizes User-Agent spoofing and JavaScript modifications to avoid common anti-bot measures.
-   **Resilient Navigation**: Implements configurable retries and exponential back-off for network requests.
-   **Structured Data Extraction**: Parses HTML content using BeautifulSoup to extract specific event details.
-   **Configurable**: Settings like headless mode, proxy usage, and target dates can be easily configured via environment variables.
-   **Data Storage**: Integrates with MongoDB for storing scraped event data.
-   **Schema Unification**: Adapts scraped data to a common event schema (details in `common.schema_adapter`).

## Scraper Structure

The scraper is organized into several key files:

-   **`scrapers/base/playwright_base.py`**:
    -   Provides a reusable `BrowserSession` class that wraps Playwright functionality.
    -   Manages browser instances (Firefox by default) with configurable options like headless mode, proxy, and User-Agent.
    -   Implements basic stealth techniques (e.g., modifying `navigator.webdriver`) and retry logic for page navigation (`safe_goto`).
    -   Offers convenience methods like `open_page` for managing page lifecycles.

-   **`scrapers/spotlight/settings.py`**:
    -   Defines the `SpotlightSettings` class using `pydantic-settings`.
    -   Manages all configuration parameters for the scraper, such as target year/month, proxy settings, and MongoDB connection details.
    -   Allows settings to be overridden by environment variables (e.g., `SPOTLIGHT_HEADLESS`, `SPOTLIGHT_PROXY`).

-   **`scrapers/spotlight/page_extractors.py`**:
    -   Contains functions responsible for fetching and parsing data from specific page types on the Ibiza Spotlight website.
    -   `fetch_calendar_html()`: Retrieves the HTML content of the main event calendar page for a given year and month.
    -   `extract_venue_urls()`: Finds and returns URLs for individual venue pages from the calendar or other relevant pages.
    -   `extract_promoter_urls()`: Finds and returns URLs for promoter pages, typically from venue pages.
    -   `extract_event_cards()`: Locates event card elements on a page (usually a promoter page) and extracts raw event data (title, date, artists, price) by parsing their HTML content with BeautifulSoup.
    -   `_parse_card()`: A helper function that takes the HTML of a single event card and extracts structured information.

-   **`scrapers/spotlight/ibizaspotlight_combo.py`**:
    -   This is the main executable script for the scraper.
    -   Orchestrates the scraping process:
        1.  Initializes `BrowserSession` using settings from `SpotlightSettings`.
        2.  Navigates to the calendar page using `fetch_calendar_html()`.
        3.  Discovers venue URLs using `extract_venue_urls()`.
        4.  For each venue, discovers promoter URLs using `extract_promoter_urls()`.
        5.  For each promoter page, extracts event card data using `extract_event_cards()`.
        6.  Collects all raw event data.
        7.  Maps the raw data to a unified schema (via `common.schema_adapter`).
        8.  Inserts the unified event data into the configured MongoDB collection, handling duplicates.
    -   Includes logging to monitor the scraper's progress.

-   **`scrapers/tests/`**:
    -   `test_extractors.py`: Contains unit tests for the parsing logic in `page_extractors.py`.
    -   `test_spotlight_scraper.py`: Contains integration tests for the overall scraping process.

## Setup and Execution

Follow these steps to set up and run the Ibiza Spotlight scraper locally:

### 1. Install Dependencies

Ensure you have Python and Poetry installed. Then, install the required packages:

```bash
# Navigate to your project's root directory if you're not already there
poetry install
```

Alternatively, if you are using `pip` and have a `requirements.txt` file:

```bash
# pip install -r requirements.txt
```

You also need to install the Playwright browser binaries. The scraper is configured to use Firefox:

```bash
playwright install firefox
```

### 2. Configure Environment Variables

The scraper's behavior can be customized using environment variables. These override the default values defined in `scrapers/spotlight/settings.py`.

Key variables include:

-   `SPOTLIGHT_HEADLESS`: Set to `true` (default) to run the browser in headless mode, or `false` to watch the browser UI.
    ```bash
    export SPOTLIGHT_HEADLESS=true
    ```
-   `SPOTLIGHT_PROXY`: Specify an HTTP proxy if needed.
    ```bash
    export SPOTLIGHT_PROXY="http://your-proxy-address:port"
    ```
-   `SPOTLIGHT_YEAR`: The year for which to scrape events (default: `2025`).
    ```bash
    export SPOTLIGHT_YEAR=2025
    ```
-   `SPOTLIGHT_MONTH`: The month (1-12) for which to scrape events (default: `8` for August).
    ```bash
    export SPOTLIGHT_MONTH=8
    ```
-   `SPOTLIGHT_MONGO_DSN`: The MongoDB connection string.
    ```bash
    export SPOTLIGHT_MONGO_DSN="mongodb://localhost:27017/?replicaSet=rs0"
    ```
-   `SPOTLIGHT_MONGO_DB`: The MongoDB database name (default: `events`).
    ```bash
    export SPOTLIGHT_MONGO_DB="events"
    ```
-   `SPOTLIGHT_MONGO_COLLECTION`: The MongoDB collection name (default: `unified_events`).
    ```bash
    export SPOTLIGHT_MONGO_COLLECTION="unified_events"
    ```

Set these variables in your shell session or using a `.env` file (if your setup supports it, `pydantic-settings` can automatically load it).

### 3. Run the Scraper

Execute the main scraper script:

```bash
python -m scrapers.spotlight.ibizaspotlight_combo
```

The scraper will log its progress to the console. The first run might take a bit longer as Playwright initializes Firefox.

### 4. Validate Data in MongoDB

After the scraper completes, you can check if the data was successfully inserted into MongoDB. Connect to your MongoDB instance (e.g., using `mongosh`) and query the collection:

```bash
mongosh --eval 'db.unified_events.countDocuments({"scraping_metadata.source_platform":"ibiza-spotlight"})'
```

For a typical run targeting August 2025, you should expect a significant number of events (e.g., >= 150, though this can vary).

### 5. Run Tests

To ensure the scraper components are working correctly, run the provided tests:

```bash
pytest -q
```

This will execute tests from `scrapers/tests/`, covering both individual extractor functions and the integrated scraping process.

## Key Component Implementations

This section provides more detail on the core components of the scraper.

### `scrapers/base/playwright_base.py`

This module offers a robust foundation for web scraping with Playwright.

-   **`BrowserSession` Class**:
    -   Manages the Playwright browser lifecycle (`__aenter__`, `__aexit__`).
    -   Configurable via parameters like `user_agent`, `proxy`, `headless`, `timeout_ms`, and `max_retries`.
    -   Randomly selects a User-Agent from `_DEFAULT_UA_POOL` if none is provided, helping to mimic real user traffic.
    -   Launches a Firefox browser instance by default, as it's often better at evading bot detection.
    -   The `new_page()` method creates a new browser page and applies stealth measures.
        -   Sets a realistic viewport size and locale.
        -   Injects JavaScript (`_stealth_js()`) to remove common Playwright and headless browser fingerprints (e.g., `navigator.webdriver`, `window.chrome`).

-   **Stealth Measures**:
    -   `_pick_random_ua()`: Selects a User-Agent string from a predefined list of common desktop UAs.
    -   `_stealth_js()`: A small JavaScript snippet that modifies browser properties to make automated browsing less detectable. This includes:
        -   Setting `navigator.webdriver` to `undefined`.
        -   Mocking `window.chrome.runtime` for headless Chrome detection.
        -   Simulating browser plugins and languages.

-   **Navigation and Helpers**:
    -   `safe_goto()`: A wrapper around `page.goto()` that includes retry logic with exponential back-off. This handles transient network errors or page load issues, making the scraper more resilient.
    -   `open_page()`: An asynchronous context manager that simplifies creating, using, and then automatically closing a browser page.

### `scrapers/spotlight/settings.py`

Configuration is managed by the `SpotlightSettings` class, which inherits from `pydantic-settings.BaseSettings`.

-   **Type-Safe Settings**: Defines expected data types for all settings (e.g., `headless: bool`, `year: int`).
-   **Default Values**: Provides sensible defaults for most settings (e.g., `headless=True`, `year=2025`, `month=8`).
-   **Environment Variable Overrides**:
    -   Automatically reads values from environment variables if they are set.
    -   The `env_prefix = "SPOTLIGHT_"` in `Config` means it looks for variables like `SPOTLIGHT_HEADLESS`, `SPOTLIGHT_PROXY`, etc. This is a clean way to manage different configurations for development, testing, and production without code changes.

### `scrapers/spotlight/page_extractors.py`

This module is central to fetching and parsing data from Ibiza Spotlight.

-   **Calendar Fetching**:
    -   `CALENDAR_URL_TMPL`: A string template for constructing the URL of the monthly event calendar.
    -   `fetch_calendar_html(page: Page, year: int, month: int) -> str`: Navigates to the specified calendar page and returns its full HTML content. It waits for network activity to cease (`networkidle`) to ensure dynamic content is loaded.

-   **URL Extraction**:
    -   `extract_venue_urls(page: Page) -> list[str]`:
        -   Uses Playwright's `page.eval_on_selector_all()` with CSS selectors (`a[href*="/night/venue/"] >> img, img[src*="/night/venue/"]`) to find links to venue pages.
        -   The JavaScript expression `els => els.map(e => e.closest('a')?.href || e.src)` extracts the `href` from the closest anchor tag or the image's `src` attribute if it directly contains the venue link.
        -   Deduplicates URLs and canonicalizes them by removing query parameters.
    -   `extract_promoter_urls(page: Page) -> list[str]`: Similar to venue URL extraction, but targets promoter links (e.g., `a[href*="/night/promoters/"]`). Includes a fallback for images within Slick carousels.

-   **Event Card Extraction**:
    -   `_CARD_CONTAINER_CSS`: A CSS selector string targeting various HTML structures that represent event cards on the website. This makes the scraper adaptable to slight variations in page layout.
    -   `_TITLE_CSS`, `_DATE_CSS`, `_ARTIST_CSS`, `_PRICE_CSS`: Specific CSS selectors for extracting individual pieces of data (title, date, artists, price) from within an event card.
    -   `extract_event_cards(page: Page) -> list[dict[str, Any]]`:
        -   Locates all event card containers on the current page using `page.locator(_CARD_CONTAINER_CSS)`.
        -   Iterates through each found element, extracts its `inner_html` (a small, self-contained portion of the DOM), and passes it to `_parse_card()`.
    -   `_parse_card(html: str) -> dict[str, Any]`:
        -   Uses `BeautifulSoup` to parse the HTML of a single event card.
        -   Extracts text and attributes using the predefined CSS selectors for title, date, artists, and price.
        -   Converts the date string to a timezone-aware `datetime` object (UTC).
        -   Cleans and structures artist names.
        -   Parses prices, extracts numerical values, and determines the lowest price in EUR.
        -   Returns a dictionary containing the structured event data.

### `scrapers/spotlight/ibizaspotlight_combo.py`

The main script that ties everything together.

-   **Initialization**:
    -   Sets up basic logging.
    -   Loads settings using `SpotlightSettings()`.

-   **`collect_raw_events() -> list[dict]`**:
    -   The core asynchronous function for the scraping workflow.
    -   Initializes a `BrowserSession` with the configured settings.
    -   **Orchestration Logic**:
        1.  Opens the main calendar page and fetches its HTML using `fetch_calendar_html()`.
        2.  Extracts all venue URLs from the calendar page using `extract_venue_urls()`.
        3.  Logs the number of venue URLs found.
        4.  Iterates through each venue URL:
            -   Opens the venue page.
            -   Extracts promoter URLs from the venue page using `extract_promoter_urls()`.
            -   Iterates through each promoter URL:
                -   Opens the promoter page.
                -   Extracts event card data using `extract_event_cards()`.
                -   For each extracted event card (raw dictionary), it adds `source_url` (the promoter page URL) and `promoter_page` metadata.
                -   Appends the augmented event data to a list.
    -   Returns the list of all collected raw event dictionaries.

-   **`insert_into_mongo(events: list[dict])`**:
    -   Connects to MongoDB using the DSN from settings.
    -   Selects the target database and collection.
    -   Iterates through the raw events:
        -   Transforms each raw event dictionary into a "unified schema" using `map_to_unified_schema()` (this function is assumed to be defined in `common.schema_adapter`). This step is crucial for data consistency if you're integrating data from multiple sources.
        -   The `source_platform` is set to `"ibiza-spotlight"`, and the `source_url` from the raw event is passed along.
    -   Performs a bulk upsert operation into MongoDB:
        -   Uses `UpdateOne` with `{"event_id": doc["event_id"]}` as the filter and `{"$setOnInsert": doc}` as the update, with `upsert=True`. This means:
            -   If an event with the same `event_id` already exists, it's not modified (matched).
            -   If no such event exists, the new document is inserted.
        -   Logs the number of new events inserted and existing events matched.

-   **`main()` Function**:
    -   The main asynchronous entry point.
    -   Calls `collect_raw_events()` to get the data.
    -   Logs the total number of raw events collected.
    -   Calls `insert_into_mongo()` to store the data.

-   **Execution Guard (`if __name__ == "__main__":`)**:
    -   Ensures `asyncio.run(main())` is called only when the script is executed directly.

## Examples and Usage Tips

### Example: Running for a Different Date

To scrape events for January 2026, you would set the environment variables before running the scraper:

```bash
export SPOTLIGHT_YEAR=2026
export SPOTLIGHT_MONTH=1
python -m scrapers.spotlight.ibizaspotlight_combo
```

### Example: Running with Visible Browser

For debugging or to observe the scraper's behavior, you can run it with headless mode disabled:

```bash
export SPOTLIGHT_HEADLESS=false
python -m scrapers.spotlight.ibizaspotlight_combo
```
The Firefox browser window will open and show the pages being navigated.

### Tip: Proxy Configuration

If you are behind a corporate proxy or need to rotate IPs for extensive scraping, ensure `SPOTLIGHT_PROXY` is correctly set:

```bash
export SPOTLIGHT_PROXY="http://user:password@your-proxy-server:port"
```
The `BrowserSession` will automatically use this proxy for all browser requests.

### Tip: Understanding `common.schema_adapter`

The module `common.schema_adapter` (not detailed in this document but referenced in `ibizaspotlight_combo.py`) is responsible for transforming the raw scraped data into a standardized format. If you need to customize the final data structure or add new fields, this is the module you would typically modify. The `map_to_unified_schema` function within it likely takes the raw event dictionary, `source_platform`, and `source_url` as inputs.

### Tip: Timeout and Retry Adjustments

If you encounter frequent timeouts, especially on slower connections or when the target site is slow to respond, you might need to adjust the default timeout and retry settings in `playwright_base.py`:

-   `timeout_ms` in `BrowserSession` constructor (default: 30,000 ms)
-   `max_retries` in `BrowserSession` constructor (default: 3)
-   The delay logic within `safe_goto()`

However, changing these directly in `playwright_base.py` would alter the base behavior. For scraper-specific overrides, consider modifying `ibizaspotlight_combo.py` to pass different values when instantiating `BrowserSession`, or enhance `SpotlightSettings` to include these parameters so they can be set via environment variables.

### Troubleshooting:

-   **Low Event Count**:
    -   Verify the `SPOTLIGHT_YEAR` and `SPOTLIGHT_MONTH` are set to a period where events are expected.
    -   Check the Ibiza Spotlight website manually for events during the target month to ensure data exists.
    -   Run with `SPOTLIGHT_HEADLESS=false` to observe if pages are loading correctly or if there are unexpected pop-ups/banners interfering.
    -   Examine logs for any errors during navigation or data extraction.
-   **Playwright Errors**:
    -   Ensure `playwright install firefox` was successful.
    -   Make sure no other Playwright instances are conflicting.
-   **MongoDB Connection Issues**:
    -   Verify your MongoDB server is running and accessible at the DSN specified by `SPOTLIGHT_MONGO_DSN`.
    -   Check MongoDB logs for connection errors.
-   **CSS Selector Changes**:
    -   Websites change their structure over time. If the scraper stops extracting certain data, the CSS selectors in `page_extractors.py` (e.g., `_CARD_CONTAINER_CSS`, `_TITLE_CSS`) might need updating. Use browser developer tools to inspect the website HTML and find the new selectors.
