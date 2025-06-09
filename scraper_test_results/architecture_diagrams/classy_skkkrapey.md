# Classy Skkkrapey Architecture Analysis

## System Architecture
```
BaseEventScraper (Base Class)
├── __init__(use_browser: bool, headless: bool)
│   └── _create_session() -> requests.Session
├── Core Methods
│   ├── rotate_user_agent()
│   ├── fetch_page(url, use_browser_override)
│   └── close()
└── Abstract Methods
    ├── scrape_event_data(url)
    └── crawl_listing_for_events(url)

TicketsIbizaScraper (Subclass)
├── Data Extraction Methods
│   ├── _parse_json_ld(soup)
│   ├── _parse_microdata(soup)
│   └── _parse_html_fallback(soup)
├── Implementation Methods
│   ├── scrape_event_data(url)
│   └── crawl_listing_for_events(url)
└── Uses requests by default

IbizaSpotlightScraper (Subclass)
├── Implementation Methods
│   ├── scrape_event_data(url)
│   └── crawl_listing_for_events(url)
└── Uses Playwright for JavaScript rendering

Data Models
├── LocationSchema
├── DateTimeSchema
├── ArtistSchema
├── TicketInfoSchema
└── EventSchema

Utility Functions
├── format_event_to_markdown()
├── datetime_serializer()
└── get_scraper_class()
```

## Features Analysis

1. Core Features:
   - Multi-site support through factory pattern
   - Automatic scraper selection based on URL
   - Flexible browser/request switching
   - Comprehensive data schemas
   - Robust error handling

2. Functions Used:
   - _create_session(): ✓ Used for HTTP requests
   - fetch_page(): ✓ Used for content retrieval
   - rotate_user_agent(): ✓ Used for request rotation
   - All parsing methods: ✓ Used for data extraction
   - format_event_to_markdown(): ✓ Used for output formatting

3. Popup Handling:
   - Browser-based popup handling for IbizaSpotlightScraper
   - No popup handling needed for TicketsIbizaScraper (static content)

## Test Results with Target URL

Target URL: https://www.ibiza-spotlight.com/night/events/2025/05?daterange=26/05/2025-01/06/2025

### Test Execution
1. Scraper Selection:
   - Status: ✓ Success
   - Correctly selected IbizaSpotlightScraper
   - Initialized with browser support

2. Browser Automation:
   - Status: ✓ Success
   - Headless mode enabled
   - JavaScript execution working

3. Data Extraction:
   - Status: In Progress
   - Using browser-based extraction
   - Handling dynamic content loading

### Strengths
1. Architecture:
   - Clean separation of concerns
   - Extensible design
   - Factory pattern for scraper selection

2. Features:
   - Multi-site support
   - Flexible content retrieval
   - Comprehensive data models
   - Multiple output formats

### Limitations
1. Dependencies:
   - Requires Playwright for JavaScript sites
   - Higher resource usage with browser automation

### Conclusion
The classy_skkkrapey.py is the most robust and well-structured scraper in the codebase:
1. Handles multiple sites through a unified interface
2. Provides flexible content retrieval methods
3. Has comprehensive data modeling
4. Implements proper error handling and logging
