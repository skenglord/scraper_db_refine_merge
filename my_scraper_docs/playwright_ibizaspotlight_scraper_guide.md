# Playwright Ibiza Spotlight Scraper Guide

This document provides a comprehensive overview of the new Playwright-based scraper for Ibiza Spotlight, detailing its architecture, key features, and the rationale behind its development.

## 1. Technology Transition

### From `requests`/`BeautifulSoup` to Playwright

The scraping methodology for Ibiza Spotlight has undergone a significant transition from a combination of `requests` for HTTP requests and `BeautifulSoup` for HTML parsing to **Playwright**.

### Reasons for the Upgrade

The primary motivations for this strategic shift are:

*   **Dynamic Content Handling**: The previous `requests`/`BeautifulSoup` approach was limited in its ability to effectively scrape content loaded via JavaScript, AJAX calls, and other client-side rendering techniques. Playwright enables full browser automation, allowing the scraper to interact with web elements and execute JavaScript, thus accessing dynamically loaded content.
*   **Improved Anti-Detection**: Modern websites employ sophisticated anti-bot measures. Playwright, by mimicking human browser behavior more closely, significantly reduces the likelihood of the scraper being detected and blocked.
*   **Interaction Capabilities**: Many crucial data points on dynamic websites are only accessible after user interaction (e.g., clicking "Load More" buttons, navigating through calendars). Playwright allows programmatic interaction with page elements (clicking buttons, filling forms, scrolling) to uncover such hidden or dynamically loaded data.

## 2. Scraper Architecture

The Playwright-based scraper incorporates advanced architectural decisions to enhance its effectiveness and stealth.

### Mobile Emulation Strategy

*   **Benefit**: Accessing mobile-specific versions of websites can sometimes reveal different data structures, simpler layouts, or less stringent anti-scraping measures. This also diversifies the scraper's fingerprint, making it harder to identify as a bot.
*   **Implementation**: Playwright's robust device emulation capabilities are utilized to simulate various mobile devices (e.g., iPhone, Android). This is achieved by setting appropriate user agents, viewport sizes, and enabling touch event capabilities, ensuring the website renders as it would for a genuine mobile user.

### Calendar-First Crawling Approach

*   **Benefit**: For event-based websites like Ibiza Spotlight, navigating directly through calendar interfaces often provides a more structured and comprehensive way to discover events compared to traditional link crawling. This strategy leads to higher data completeness and more efficient discovery of new or updated event listings.
*   **Implementation**: The scraper prioritizes locating and interacting with calendar elements (e.g., date pickers, month navigators). It programmatically iterates through days, weeks, or months, extracting event links or data directly from the calendar views, ensuring all events within a specified range are discovered.

## 3. Calendar Navigation Implementation

### Batch Processing Parameters
- **Month Chunk Size**: 3 months per batch
- **Events per Batch**: 50 events
- **Batch Timeout**: 120 seconds
- **Retry Policy**: 3 attempts with exponential backoff
- **Parallelism**: 3 concurrent batches
- **Date Range**: Current month + 3 months forward
- **Event Cache Size**: 500 events

### Stealth Techniques for Calendar Interaction
- **Randomized Click Coordinates**: Offsets within element bounds
- **Variable Scroll Speeds**: 100px/s to 800px/s
- **Human-like Delay Patterns**: Weibull distribution (shape=1.5, scale=3.2)
- **Input Obfuscation**: Mimics keyboard/mouse accelerations
- **Headless Detection Evasion**: Overrides `navigator.webdriver` and `chrome.runtime`
- **Viewport Rotation**: Randomizes between portrait/landscape modes
- **Touch Event Simulation**: Generates touch sequences for mobile devices

## 4. Key Features

The new Playwright scraper is equipped with several key features designed to maximize data extraction and minimize detection.

### Dynamic Web Page Interaction

The scraper can:
*   **Click elements**: Simulate clicks on buttons, links, and other interactive elements to reveal hidden content or navigate.
*   **Fill forms**: Input text into search bars or forms to trigger dynamic content loads.
*   **Scroll**: Mimic human scrolling behavior to load lazy-loaded content.
*   **Wait for network requests**: Ensure all necessary data is loaded before attempting to extract information.

### Human-like Interaction Techniques

To avoid bot detection, the scraper integrates:
*   **Randomized delays**: Introduces variable pauses between actions to simulate human browsing patterns.
*   **Human-like mouse movements**: Utilizes Playwright's capabilities to move the mouse cursor in a non-linear, human-like fashion before clicking.
*   **Viewport interactions**: Randomizes viewport sizes and scrolls to mimic diverse user environments.
*   **`playwright-stealth` integration**: Patches Playwright against common bot detection scripts, making the automated browser appear more like a regular user.

### Data Enrichment Logic

The scraper is designed to capture richer data points:
*   **JSON-LD parsing**: Prioritizes extracting structured data directly from JSON-LD scripts embedded in the HTML.
*   **HTML parsing fallback**: If JSON-LD is unavailable or incomplete, robust CSS selectors and XPath expressions are used to extract data from the rendered HTML.
*   **Full content capture**: Capable of capturing fully rendered descriptions, including those loaded by "Read More" buttons, and images loaded dynamically or within carousels.
*   **Real-time data**: Aims to capture real-time pricing and ticket availability if displayed dynamically on the page.

## 5. MongoDB Schema Changes

The adoption of Playwright allows for the capture of richer and more dynamic data. The MongoDB `events` schema has been reviewed and potentially updated to accommodate these new data points.

### Updated `lineUp` Field Structure

While the core `lineUp` field structure remains an array of strings for artist names, the Playwright scraper ensures more reliable and complete extraction, especially if lineup information is dynamically loaded after initial page render.

### Rationale for Schema Modification

Potential new fields and their rationale:

*   **`raw_html_snapshot` (Optional, String)**: Stores a snapshot of the fully rendered HTML at the time of scraping.
    *   **Rationale**: Useful for debugging, re-processing data offline, or analyzing changes in website structure over time without re-scraping.
*   **`interaction_metadata` (Object, Optional)**: Logs details about interactions performed to retrieve the data.
    *   **Rationale**: Provides an audit trail of how data was accessed, crucial for understanding complex scraping flows and replicating issues.
    *   Example: `{"actions_taken": ["clicked_date_2025-07-15", "scrolled_to_event_list"]}`
*   **`dynamic_content_fields` (Object, Optional)**: Stores fields populated specifically through JavaScript execution or user interaction, previously inaccessible.
    *   **Rationale**: Captures data that is only visible after client-side rendering, such as `live_ticket_availability_status` or `user_review_count_dynamic`.
*   **`mobile_specific_data` (Object, Optional)**: Contains data points that are only available or differ on the mobile version of the site.
    *   **Rationale**: Leverages the mobile emulation strategy to capture unique mobile-only content.
*   **`event_timestamps.last_verified_dynamic` (DateTime)**: Timestamp indicating when dynamic elements of the event were last verified/scraped.
    *   **Rationale**: Provides a more granular understanding of data freshness for dynamically loaded content.

These schema changes aim to enhance the richness and accuracy of the event data while maintaining data integrity and backward compatibility where possible.

## 6. Performance and Reliability

The Playwright scraper is designed with performance and reliability in mind, particularly concerning anti-bot measures and comprehensive event discovery.

### Anti-bot Detection Avoidance

Beyond human-like interactions, the scraper employs:
*   **IP Rotation (if configured)**: Integration with proxy services to rotate IP addresses, preventing IP-based blocks.
*   **User-Agent Rotation**: Randomly selects from a pool of legitimate user-agent strings.
*   **Referer and Header Management**: Sets realistic HTTP headers to mimic genuine browser requests.
*   **Error Handling with Retries**: Implements robust error handling with exponential backoff and retry logic for network failures or temporary blocks.

### Comprehensive Event Discovery

The calendar-first approach, combined with Playwright's ability to interact with dynamic elements, ensures:
*   **Full date range coverage**: Systematically iterates through all available dates on the calendar to discover events.
*   **Hidden event revelation**: Clicks on "Load More" buttons or similar elements to uncover events that are not immediately visible.
*   **Robust navigation**: Handles complex pagination and infinite scrolling mechanisms effectively.
*   **Debug Instrumentation**: Comprehensive statistics tracking (e.g., venue count, promoters per venue, events per promoter) and summary report generation aid in monitoring and ensuring completeness.

## 7. Future Improvements and Recommendations

*   **Headless vs. Headed Mode Optimization**: Further analysis on when to use headless mode for performance versus headed mode for debugging and complex interactions.
*   **Distributed Scraping**: Implement a distributed scraping architecture using tools like Celery or Kubernetes to scale operations and handle larger volumes of data more efficiently.
*   **Machine Learning for Anomaly Detection**: Develop ML models to detect unusual website behavior (e.g., CAPTCHAs, sudden layout changes) and adapt scraping strategies automatically.
*   **Enhanced Data Validation**: Implement more sophisticated post-scraping data validation routines to ensure data quality and consistency, especially for dynamically extracted fields.
*   **Integration with Monitoring Tools**: Integrate with external monitoring and alerting systems (e.g., Prometheus, Grafana) for real-time performance tracking and incident response.
*   **Dynamic Proxy Management**: Implement a more intelligent proxy management system that can dynamically select the best proxy based on performance and success rates.