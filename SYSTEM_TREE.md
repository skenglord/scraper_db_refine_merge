# System Tree

This document outlines the hierarchical structure of the Web Scraping and Event Data API project, detailing its major components and their primary functions.

## 1. Project Root (`skrrraped_graph/`)

- **`my_scrapers/`**: Contains the core web scraping logic.
  - **`classy_skkkrapey.py`**: Main executable script for scraping.
    - `BaseEventScraper`: Abstract base class for scrapers.
      - Responsibilities: Common scraping utilities (HTTP requests, Playwright browser management, User-Agent rotation).
    - `TicketsIbizaScraper(BaseEventScraper)`: Scraper for `ticketsibiza.com`.
      - Extraction Methods: JSON-LD, Microdata, HTML fallback.
      - Content Handling: Primarily static content.
    - `IbizaSpotlightScraper(BaseEventScraper)`: Scraper for `ibiza-spotlight.com`.
      - Extraction Methods: HTML parsing of dynamically rendered content.
      - Content Handling: JavaScript-heavy, requires Playwright.
    - Argument Parsing (`argparse`): Handles CLI commands (`scrape`, `crawl`) and options.
    - Output Generation: Saves data to JSON and Markdown.
- **`database/`**: Manages data storage, quality, and API access.
  - **`api_server.py`**: FastAPI application for exposing event data.
    - Endpoints:
      - `/api/events`: Get filtered events (quality, venue, future_only).
      - `/api/events/{event_id}`: Get specific event details.
      - `/api/events/search/{search_term}`: Full-text search.
      - `/api/venues`: List venues.
      - `/api/venues/{venue_name}/events`: Events by venue.
      - `/api/stats/quality`: Data quality statistics.
      - `/api/upcoming`: Upcoming events.
      - `/api/events/{event_id}/refresh`: Mark event for re-scraping.
    - Pydantic Models: Define data structures for API requests/responses.
    - MongoDB Interaction (`pymongo`): Synchronous queries to MongoDB.
  - **`mongodb_setup.py`**: Script for initializing MongoDB schema.
    - Responsibilities: Creates collections (`events`, `quality_scores`, etc.) and defines indexes.
  - **`quality_scorer.py`**: Module for calculating data quality scores.
    - Logic: Evaluates fields like title, location, dateTime, lineUp, ticketInfo based on predefined rules.
  - **`data_migration.py`**: Script for migrating existing JSON data to MongoDB.
    - Includes quality scoring for migrated data.
  - **`export_to_sheets.py`**: Utility to export data to Google Sheets.
  - `requirements.txt`: (Note: also a root `requirements.txt`, this one might be specific or duplicated, typically dependencies are managed at root)
- **`utils/`**: Contains helper scripts and utility functions.
  - Examples: `cleanup_code.py`, `cleanup_html.py`, `model_costs.py` (actual utility depends on their content).
  - Purpose: Support various tasks like data cleaning, text processing, etc.
- **`tests/` & `test_cases/`**: Automated tests for the project.
  - `pytest` framework used.
  - Contains unit and integration tests for different modules (e.g., `test_mono_ticketmaster.py`).
- **`prompts/`**: Likely contains prompt templates if an LLM is used for data extraction or generation.
- **`models/`**: Contains data models or machine learning model integration (e.g., `deepseek.py` suggests LLM integration, perhaps for advanced data processing or classification).
- **`docloaders/`**: Might contain utilities for loading or processing documents (e.g., `chromium.py`, `scrape_do.py`).
- **`adapters/`**: Could be for interfacing with different services or data formats.
- **Documentation & Configuration Files (Root Level):**
  - `README.md`: Main project overview, setup, and usage guide.
  - `CONTRIBUTING.md`: Guidelines for contributors.
  - `DEPLOYMENT.md`: High-level deployment guidance.
  - `SYSTEM_TREE.md`: This document.
  - `requirements.txt`: Python package dependencies for the project.
  - `requirements-dev.txt`: Development-specific dependencies (testing, docs).
  - `Makefile`: Contains build/utility commands (e.g., for setup, cleaning).
  - `.github/workflows/`: CI/CD pipeline configurations (e.g., `python-package-conda.yml`).

## 2. Key Data Structures (Conceptual)

- **`EventSchema` (and related `TypedDict`s):** Defines the structure of event data within the scraper.
  - Fields: `url`, `title`, `location`, `dateTime`, `lineUp`, `ticketInfo`, `description`, `scrapedAt`, `extractionMethod`.
- **MongoDB Event Document:** The representation of an event in the database.
  - Includes fields from `EventSchema`.
  - Augmented with `_quality` (overall score, field-specific scores) and `_validation` (validation flags, confidence) sub-documents.

## 3. Core Processes / Workflows

- **Scraping Workflow:**
  1. CLI triggers `classy_skkkrapey.py` (`crawl` or `scrape`).
  2. Appropriate scraper selected (TicketsIbiza or IbizaSpotlight).
  3. Page content fetched (Requests or Playwright).
  4. Data extracted using site-specific logic.
  5. (Ideally) Data passed to `quality_scorer.py`.
  6. Data stored in MongoDB.
- **API Request Workflow:**
  1. Client sends HTTP request to `api_server.py`.
  2. FastAPI routes request to appropriate endpoint function.
  3. Endpoint function queries MongoDB via `pymongo`.
  4. Data serialized using Pydantic models.
  5. JSON response sent to client.
- **Data Quality Scoring Workflow:**
  1. Event data (dictionary) passed to `QualityScorer`.
  2. Scorer applies rules to each relevant field.
  3. Individual and overall quality scores calculated.
  4. Scores and validation details returned for storage.

This tree provides a high-level map of the project's architecture and how its different parts interact.
