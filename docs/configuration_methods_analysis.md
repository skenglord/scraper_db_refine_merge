# Analysis of Configuration Methods in Scraper Files

This document outlines the various configuration methods and sources identified across the project's scraper files.

## 1. Configuration Methods Observed

Several distinct approaches to configuration are used within the repository:

### a. Central `Settings` Module (`config.py`)
*   **Description**: A dedicated Python module (`config.py` at the project root) defines a `Settings` class, typically using `pydantic-settings`. This class loads values from environment variables (via `os.getenv`) and can also read from a `.env` file. It provides type hinting and default values for configuration parameters. An instance of this `Settings` class (e.g., `settings`) is then imported by other modules.
*   **Source**: Environment variables, `.env` file, default values in the `Settings` class.
*   **Example Snippet (from `config.py`)**:
    ```python
    import os
    from pydantic_settings import BaseSettings

    class Settings(BaseSettings):
        MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
        # ... other settings ...
        class Config:
            env_file = ".env"
            extra = "ignore"
    settings = Settings()
    ```
*   **Consumed by**:
    *   `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`
    *   `my_scrapers/unified_scraper.py` (via `classy_skkkrapey.config`)
    *   `my_scrapers/mono_ticketmaster.py` (attempts to, with fallback)
    *   `crawl_components/crawler_ibizatickets.py` (attempts to, with fallback)
    *   `crawl_components/crawler_spotlightibiza.py` (attempts to, with fallback)
    *   `database/etl_sqlite_to_mongo.py` (uses `os.getenv` directly, mimicking `config.py` style for DB settings).

### b. Internal `DEFAULT_CONFIG` Dictionaries
*   **Description**: A Python dictionary, often named `DEFAULT_CONFIG`, is defined at the module level within a scraper file. This dictionary holds hardcoded default values for various scraper parameters. These defaults are typically merged with overrides provided during class instantiation.
*   **Source**: Hardcoded values within the Python file.
*   **Example Snippet (from `my_scrapers/ventura_crawler.py`)**:
    ```python
    DEFAULT_CONFIG = {
        "db_path": "serpentscale_scraper_data.db",
        "browser_pool_size": 3,
        # ... other settings ...
    }
    # In class __init__:
    # self.config = {**DEFAULT_CONFIG, **(config_overrides or {})}
    ```
*   **Used by**:
    *   `my_scrapers/ventura_crawler.py`
    *   `my_scrapers/classy_clubtickets_nav_scraper.py`

### c. Constructor Overrides / Direct Parameter Passing
*   **Description**: Scraper classes are designed to accept configuration values directly as parameters to their `__init__` method. These can be individual values or a dictionary of overrides. This method is often used in conjunction with `DEFAULT_CONFIG` dictionaries.
*   **Source**: Programmatic instantiation of scraper classes.
*   **Example**:
    *   `my_scrapers/ventura_crawler.py` (`config_overrides` dict)
    *   `my_scrapers/unified_scraper.py` (individual parameters like `headless`)

### d. Local Configuration Dataclasses
*   **Description**: A dataclass is defined within a scraper module to structure its specific configuration. This provides type hints and can include default values. Instances of this dataclass are used to configure the scraper. This is distinct from the central `Settings` class in `config.py`.
*   **Source**: Hardcoded defaults in the dataclass, or values passed during instantiation.
*   **Example Snippet (from `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`)**:
    ```python
    @dataclass
    class ScraperConfig:
        url: str
        min_delay: float = 0.3
        # ... other settings ...
        mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017/ibiza_data")
    ```
*   **Used by**:
    *   `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`
    *   `my_scrapers/mono_ticketmaster.py` (defines its own `ScraperConfig` dataclass locally)

### e. Hardcoded Constants and Values
*   **Description**: Configuration values (e.g., specific URLs, selectors, file paths, fixed delays, logging formats) are directly embedded as constants or literal values within the scraper logic. While some are intrinsic to the scraper's target, others could be externalized.
*   **Source**: Directly in the code.
*   **Examples**:
    *   `my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`: `BASE_URL`, `CALENDAR_URL`, `USER_AGENT`, various selectors.
    *   `crawl_components/club_tickets_crawl_logic.py`: Global `config` dictionary with hardcoded URLs, selectors, and behavior flags.
    *   Most scrapers use hardcoded CSS/XPath selectors either directly or within configuration dictionaries/dataclasses.

### f. Environment Variables (Direct `os.getenv`)
*   **Description**: Some modules or local configuration objects (like dataclasses) might directly use `os.getenv()` to fetch specific configuration values, providing a default if the environment variable is not set. This is a more targeted use than the centralized `pydantic-settings` approach.
*   **Source**: Environment variables, with in-code defaults.
*   **Used by**:
    *   `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py` (within its `ScraperConfig` dataclass for `MONGODB_URI`)
    *   `database/etl_sqlite_to_mongo.py` (for `SQLITE_DB_PATH`, `MONGO_URI`, etc.)
    *   Fallback `DummySettings` in `my_scrapers/mono_ticketmaster.py` and `crawl_components/crawler_ibizatickets.py`.

### g. Command-Line Arguments (`argparse`)
*   **Description**: For scripts intended to be run directly from the command line, Python's `argparse` module is used to define and parse command-line arguments. These arguments often serve as the primary configuration source or override hardcoded defaults.
*   **Source**: User input via command-line interface.
*   **Used by**:
    *   `my_scrapers/mono_ticketmaster.py`
    *   `my_scrapers/unified_scraper.py`
    *   `crawl_components/crawler_ibizatickets.py`
    *   `crawl_components/crawler_spotlightibiza.py`

## 2. Summary of Configuration Usage by Scraper/Component

*   **`my_scrapers/ventura_crawler.py`**: Primarily uses an internal `DEFAULT_CONFIG` dictionary, supplemented by constructor overrides. Configuration is largely self-contained.
*   **`my_scrapers/classy_clubtickets_nav_scraper.py`**: Similar to `ventura_crawler`, uses an internal `DEFAULT_CONFIG` and constructor overrides. Selectors are part of this `DEFAULT_CONFIG`.
*   **`my_scrapers/mono_ticketmaster.py`**: Uses `argparse` for command-line execution. Defines a local `ScraperConfig` dataclass. Attempts to import a central `settings` object (from `classy_skkkrapey.config`) but has a fallback to a dummy settings class that uses `os.getenv` or hardcoded values. Contains hardcoded default URLs and selectors.
*   **`my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`**: Relies heavily on module-level hardcoded constants for URLs, file paths, user agents, and selectors. Imports `settings` from `classy_skkkrapey.config` (or a fallback) for database details.
*   **`my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`**: Imports the central `settings` object from `config.py`. Also defines its own `ScraperConfig` dataclass which draws values from `os.getenv` or the central `settings`.
*   **`my_scrapers/unified_scraper.py`**: Uses `argparse` for command-line invocation. Accepts some configuration (headless mode, delays) via constructor parameters. Imports a central `settings` object from `classy_skkkrapey.config`.
*   **`crawl_components/club_tickets_crawl_logic.py`**: Uses a global `config` dictionary defined within the file, containing hardcoded values for behavior, delays, and target URL.
*   **`crawl_components/crawler_ibizatickets.py`**: Uses `argparse`. Attempts to import a central `settings` object (from `classy_skkkrapey.config`) with a fallback mechanism similar to `mono_ticketmaster.py`.
*   **`crawl_components/crawler_spotlightibiza.py`**: Uses `argparse`. Attempts to import a central `settings` object (from `classy_skkkrapey.config`) with a fallback.
*   **`database/etl_sqlite_to_mongo.py`**: Uses `os.getenv` for database paths and connection URIs, with hardcoded defaults.

## 3. Observations

*   There is a trend towards using a central `settings` object (from `config.py` or a similar `classy_skkkrapey.config`) in more recent or refactored scrapers, which promotes consistency.
*   Older or more standalone scrapers often rely on internal `DEFAULT_CONFIG` dictionaries or hardcoded constants.
*   Command-line runnable scripts consistently use `argparse`.
*   Hardcoded values, especially for CSS/XPath selectors and specific target URLs/patterns, are prevalent across many files, sometimes within config dictionaries and sometimes directly in the logic.
*   Fallback mechanisms for importing central settings (e.g., in `mono_ticketmaster.py`) indicate an awareness of different execution contexts or a transition phase.
*   Dataclasses are used both for central settings (`pydantic-settings` in `config.py`) and for local, scraper-specific configuration structures.

This mixed approach suggests an ongoing evolution in configuration management within the project. Consolidating towards the central `pydantic-settings` approach for most parameters, while still allowing for scraper-specific configurations (perhaps also as typed dataclasses that can be composed), could be a future direction. Externalizing more hardcoded values (like base URLs or common selectors where appropriate) into the configuration system would also enhance maintainability.
