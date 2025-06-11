# Ticketmaster Scraper (`scrapers_v2`)

## Overview
This scraper is responsible for collecting event data from Ticketmaster.com. It extracts information such as event titles, dates, venues, locations, ticket prices, performers, and event URLs.

The scraper is designed to:
- Fetch event information starting from configured URLs (e.g., general concert discovery pages).
- Parse event data primarily from embedded JSON-LD structured data, which is often comprehensive.
- Fall back to HTML parsing for event cards if JSON-LD is insufficient or missing for some details.
- Optionally use a headless browser (Playwright, if enabled in the configuration) to handle dynamically loaded content, "Load More" pagination, and cookie consent popups.
- Transform and validate the extracted data against a defined Pydantic schema.

This scraper is a migration of the previous `mono_ticketmaster.py` scraper into the `scrapers_v2` framework, incorporating structured configuration, Pydantic data models, and more robust parsing strategies.

## Configuration
The primary configuration for this scraper is located in `ticketmaster_config.yaml` within the same directory. This file defines how the scraper behaves and targets data.

Key configuration parameters include:

-   **`target_urls`**:
    *   Defines the starting URLs for scraping.
    *   Example: `concerts: "https://www.ticketmaster.com/discover/concerts"`

-   **`selectors`**:
    *   Contains CSS selectors used for finding data within the HTML and for identifying specific script tags. These are crucial for adapting to website structure changes.
    *   Examples:
        *   `event_card: ".event-card-container"`: Selector for individual event listings in HTML.
        *   `event_title_in_card: ".event-title-in-card"`: Selector for the event title within an HTML card.
        *   `json_ld_script: "script[type='application/ld+json']"`: Selector for finding JSON-LD structured data script tags.
        *   `load_more_button: "#load-more-events"`: Selector for the "Load More" button for pagination (used with Playwright).
        *   `cookie_popup_selectors`: A list of selectors for cookie consent popups that Playwright will attempt to click.

-   **`playwright_settings`**:
    *   Controls the behavior of the headless browser (Playwright).
    *   `enabled: true|false`: Whether to use Playwright for fetching pages. If `false`, a basic HTTP client is used (which may not work well with JavaScript-heavy sites like Ticketmaster).
    *   `headless: true|false`: Whether to run the browser in headless mode (no UI).
    *   `user_agent`: Custom User-Agent string for browser requests.
    *   `default_timeout_ms`: Default timeout for Playwright operations.
    *   `slow_mo_ms`: Slows down Playwright operations by the specified milliseconds, useful for debugging or mimicking human interaction.

-   **`scraping_settings`**:
    *   General parameters for the scraping process.
    *   `delays`: Contains sub-keys for delays at different stages:
        *   `request_min_ms`, `request_max_ms`: Min/max delay before making a page request.
        *   `post_interaction_min_ms`, `post_interaction_max_ms`: Min/max delay after an interaction like a click (used with Playwright).
    *   `max_load_more_clicks`: The maximum number of times the scraper will attempt to click a "Load More" button during pagination with Playwright.

**Note on Sensitive Information:** Sensitive data like API keys (if Ticketmaster were to offer/require one for this type of access) should **not** be stored in `ticketmaster_config.yaml`. Such information should be managed through the global `scrapers_v2` configuration system, typically using `.env` files at the root of the `scrapers_v2` project.

## Data Schema
The data extracted and transformed by this scraper adheres to a Pydantic model structure defined in `ticketmaster_datamodels.py`. This ensures that the output is typed and validated. The schema was initially based on `scrapers_v2/ticketmaster_event_schema.md`.

Key fields in the `TicketmasterEventModel` include:
-   `event_id` (str, generated from URL)
-   `event_title` (str)
-   `event_url` (HttpUrl)
-   `event_start_datetime` (Optional[datetime])
-   `venue_name` (Optional[str])
-   `lineup` (Optional[List[ArtistInfoModel]])
-   `ticket_min_price` (Optional[float])
-   `ticket_currency` (Optional[str])
-   `ticket_prices_detailed` (Optional[List[PriceDetailModel]])
-   `description` (Optional[str])
-   `scraped_at` (datetime)
-   `source_platform` (str, defaults to "ticketmaster_mock" in current test version)

Refer to `ticketmaster_datamodels.py` for the complete schema details, including nested models and validation rules.

## Setup
1.  **Standard `scrapers_v2` Environment:** Ensure the `scrapers_v2` framework is installed and configured according to its documentation. This typically involves setting up a Python virtual environment and installing dependencies from a main `requirements.txt` file.
2.  **Key Dependencies:** This scraper relies on:
    *   `PyYAML` for loading its YAML configuration.
    *   `beautifulsoup4` for HTML parsing.
    *   `python-dateutil` for robust date string parsing.
    *   `pydantic` for data validation and modeling.
    *   `playwright` (if `playwright_settings.enabled` is true in the config) for browser automation.
    These should be included in the `scrapers_v2` project's main `requirements.txt`.

## Running the Scraper
This scraper is designed to be invoked by the `scrapers_v2` orchestration system (e.g., via a scheduler or a central command).

For **local development and testing**, the `ticketmaster_scraper.py` script can be run directly:
```bash
python scrapers_v2/scrapers/ticketmaster/ticketmaster_scraper.py
```
When run directly, its `if __name__ == "__main__":` block executes a test function (`main()`) that initializes the scraper with placeholder settings and runs the `scrape_live_events()` method. This test function uses mock HTML content embedded in the placeholder client classes, allowing for testing of parsing and transformation logic without actual web requests.

## Output
The `scrape_live_events()` method, when successfully executed, returns a list of `TicketmasterEventModel` Pydantic objects. Each object represents a validated scraped event.

When the `main()` test function in `ticketmaster_scraper.py` is run, it prints a sample of these Pydantic model instances (as JSON) to the console for inspection. In a production `scrapers_v2` environment, this data would typically be passed to a data storage component (e.g., a database, a JSON/CSV file writer).

## Known Issues/Limitations
-   **Selector Stability:** The scraper's reliability for HTML parsing is highly dependent on Ticketmaster's website structure remaining consistent with the defined CSS selectors in `ticketmaster_config.yaml`. Changes to the website layout may require updating these selectors. JSON-LD parsing is generally more robust to layout changes but depends on the continued presence and structure of that data.
-   **Dynamic Content:** While Playwright helps with dynamic content, complex anti-scraping measures (like advanced CAPTCHAs or browser fingerprinting) are not explicitly handled by the current placeholder implementation and would require more sophisticated solutions if encountered.
-   **Performance:** Extensive use of Playwright (if enabled for many pages or deep pagination) can be resource-intensive and slower than direct HTTP requests. The configuration should balance the need for Playwright with performance considerations.
-   **Rate Limiting:** The current delay mechanism is basic. Aggressive scraping could lead to IP blocks from Ticketmaster. A more robust proxy and User-Agent rotation strategy, managed by the `scrapers_v2` framework, would be beneficial for large-scale scraping.
-   **Mock Data Only (Current State):** The scraper, as developed through these subtasks, primarily uses mock clients and embedded mock HTML for testing. It does not perform live web requests yet.

## Maintainer(s)
-   (To be assigned)
```
