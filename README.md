# Ibiza Event Scraper & API

## Description

This project is designed to scrape event information from key Ibiza event websites: "ticketsibiza.com" (a primarily static site) and "ibiza-spotlight.com" (a JavaScript-heavy, dynamic site). It processes this information, scores it for quality and completeness, stores the curated data in a MongoDB database, and provides a FastAPI-based API for easy access to the event data.

## Key Features

*   **Multi-Site Scraping:** Capable of scraping data from multiple websites with different structures.
*   **Dynamic Content Handling:** Utilizes Playwright to effectively scrape JavaScript-rendered content.
*   **Static Content Handling:** Uses Requests and BeautifulSoup for efficient scraping of static sites.
*   **Data Quality Scoring:** Implements a system to score the quality of scraped data, ensuring reliability.
*   **MongoDB Storage:** Stores structured event data in a MongoDB database.
*   **FastAPI Powered API:** Provides a robust and well-documented API for accessing event data, with filtering capabilities (including by quality score).
*   **Configurable Scraper:** Offers CLI options for targeted scraping and crawling.
*   **User-Agent Rotation & Delays:** Implements polite scraping practices.

## Technologies Used

*   **Programming Language:** Python 3.9+
*   **Web Scraping:**
    *   Playwright (for dynamic sites)
    *   Requests (for static sites)
    *   BeautifulSoup4 (for HTML parsing)
*   **API Development:**
    *   FastAPI
    *   Pydantic (for data validation and settings management via pydantic-settings)
*   **Database:**
    *   MongoDB
    *   Pymongo (driver)
*   **CLI:** Argparse
*   **Logging:** Python `logging` module

## Prerequisites

Before you begin, ensure you have the following installed:

*   Python 3.9 or higher
*   MongoDB
*   Access to a terminal or command prompt
*   A `.env` file (copied from `.env.example`) for custom configuration (e.g., MongoDB URI, scraper settings).

## Setup and Installation

1.  **Clone the Repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Create and Activate a Python Virtual Environment:**
    ```bash
    python -m venv venv
    # On macOS/Linux
    source venv/bin/activate
    # On Windows
    # venv\Scripts\activate
    ```

3.  **Install Dependencies:**
    Install all required packages from `requirements.txt`:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Install Playwright Browser Drivers:**
    Playwright requires browser binaries to operate. Install them with:
    ```bash
    playwright install
    ```
    This will download the default browsers (Chromium, Firefox, WebKit).

5.  **MongoDB Setup:**
    *   Ensure your MongoDB instance is running.
    *   MongoDB connection URI and other settings are managed via environment variables. Copy the `.env.example` file to `.env` and customize it with your settings (e.g., MongoDB URI).
    *   The project includes scripts and guidelines for setting up the necessary collections and potentially indexes. Refer to `database/README.md` and `database/mongodb_setup.py` for detailed instructions on configuring the database schema.

## Usage

### Scraper (`my_scrapers/unified_scraper.py`)

The main scraper script (`my_scrapers/unified_scraper.py`) is used to fetch and process event data from Ibiza Spotlight. Default scraper settings (like output directory, headless mode, and request delays) are configured via environment variables in the `.env` file. The command-line options for the scraper now focus on operational parameters like the action (scrape/crawl) and target URLs/dates.

**Basic Command Structure (for `unified_scraper.py`):**
```bash
python my_scrapers/unified_scraper.py <action> [options]
```

**Actions:**
*   `scrape`: Scrapes a single event detail page. Requires `--url`.
*   `crawl`: Crawls a monthly calendar. Requires `--month` and `--year`.

**Retained Command-Line Options:**
*   `--url URL`: URL of the single event detail page (for 'scrape' action).
*   `--month MONTH`: Month (1-12) for 'crawl' action.
*   `--year YEAR`: Year (e.g., 2025) for 'crawl' action.
*   `--format {json,csv,md}`: Output format(s). Default: json csv.

**Example Commands (for `unified_scraper.py`):**

*   **Crawl Ibiza Spotlight for May 2025 events:**
    ```bash
    python my_scrapers/unified_scraper.py crawl --month 5 --year 2025
    ```
*   **Scrape a specific event from Ibiza Spotlight:**
    ```bash
    python my_scrapers/unified_scraper.py scrape --url https://www.ibiza-spotlight.com/night/events/2024/09/some-event-slug
    ```
*Note: Other scrapers might exist in `my_scrapers/` with different CLI options. The above refers to `unified_scraper.py` which has been updated for the new configuration system.*

**Output:**
The scraper saves output to the directory specified by `SCRAPER_DEFAULT_OUTPUT_DIR` in your `.env` file (default is `output/`).
*   JSON files (containing structured event data).
*   CSV files (containing structured event data).
*   Markdown files (for fallback content if detailed parsing fails, or as specified).

### API Server (`database/api_server.py`)

The API server provides access to the scraped event data stored in MongoDB.

1.  **Run the API Server:**
    Navigate to the `database` directory and run:
    ```bash
    python api_server.py
    ```
    By default, the server will run on `http://localhost:8000`.

2.  **Access API Documentation:**
    Once the server is running, you can access the interactive API documentation (Swagger UI) by navigating to:
    `http://localhost:8000/docs`
    This interface allows you to explore all available endpoints, view their request/response models, and even test them directly from your browser.

## Directory Structure Overview

*   `my_scrapers/`: Contains the core web scraping logic, including the base scraper and site-specific scraper classes.
*   `database/`: Includes scripts for API server, database setup, data migration, quality scoring, and example queries.
*   `utils/`: Contains various utility modules used across the project.
*   `models/`: Contains data models or machine learning model integration (e.g., `deepseek.py`).
*   `output/`: Default directory for scraped data (JSON, Markdown).
*   `tests/`: Contains test scripts for the project.
*   `test_cases/`: May contain sample HTML files or data for testing purposes.

## Data Quality

A key aspect of this project is its data quality scoring system. After scraping, event data is evaluated based on completeness, format correctness, and other heuristics. This score is stored alongside the event information and can be used via the API to filter for high-quality, reliable event listings.

## Contributing

Contributions are welcome! If you'd like to contribute, please consider the following:
*   Check for open issues or propose new features.
*   Follow the existing code style and add tests for new functionality.
*   A `CONTRIBUTING.md` file exists with more detailed guidelines.

---

This README provides a basic guide to getting the project set up and running. For more detailed information on specific components, please refer to the documentation within the respective directories and code files.
