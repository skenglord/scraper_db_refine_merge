# Assessment of File Outputs in Scraper Scripts

## 1. Introduction

This document assesses the current usage and perceived necessity of file outputs (JSON, CSV, Markdown) across various scraper scripts. It identifies which scrapers use utility functions from `my_scrapers/utils/scraper_utils.py` for file generation, the apparent purposes of these outputs, and notes any inconsistencies.

## 2. Overview of File Output Utilities in `my_scrapers/utils/scraper_utils.py`

The `scraper_utils.py` module provides the following primary functions for generating file outputs:

*   **`save_to_json_file(data_list, filename_prefix, output_dir, logger_obj)`**: Saves a list of dictionaries to a timestamped JSON file. Handles serialization of complex types like `datetime`.
*   **`save_to_csv_file(data_list, filename_prefix, output_dir, logger_obj)`**: Saves a list of dictionaries to a timestamped CSV file. Serializes complex types (lists/dicts) into JSON strings within cells.
*   **`save_to_markdown_file(data_list, filename_prefix, output_dir, logger_obj)`**: Converts a list of dictionaries into a formatted Markdown report and saves it to a timestamped `.md` file.

All these utilities save files to a specified `output_dir` (defaulting to "output") relative to the project root and include a timestamp in the filename for versioning. They also integrate with Python's `logging`.

Additionally, `scraper_utils.py` contains `setup_logger`, which configures logging to timestamped files, and `save_to_mongodb` for database output.

## 3. Assessment of Individual Scraper Scripts

### a. `my_scrapers/classy_clubtickets_nav_scraper.py`
*   **File Output Types**: JSON, CSV, Markdown.
*   **Method of Generation**: Uses `save_to_json_file`, `save_to_csv_file`, and `save_to_markdown_file` from `scraper_utils.py`.
*   **Configuration**:
    *   Output directory is configurable via `DEFAULT_CONFIG["output_dir"]` (default "output/clubtickets_test") and can be overridden.
    *   Filename prefix is hardcoded as "clubtickets_events_test".
*   **Data Saved**: List of unified event data (`all_event_data_unified`).
*   **Apparent Purpose**: Primarily for testing, local analysis, and providing example outputs during development, as indicated by the "test" in directory/db names.

### b. `my_scrapers/mono_ticketmaster.py`
*   **File Output Types**: JSON, CSV, Markdown.
*   **Method of Generation**: Uses `save_to_json_file`, `save_to_csv_file`, and `save_to_markdown_file` from `scraper_utils.py`.
*   **Configuration**:
    *   Output directory is configurable via its local `ScraperConfig` dataclass, which can be set by command-line arguments (`--output-dir`, default "output/mono_ticketmaster").
    *   Filename prefix is hardcoded as "mono_ticketmaster_events".
*   **Data Saved**: List of unified event data (`self.all_scraped_data`).
*   **Apparent Purpose**: Debugging, ad-hoc analysis, and providing a persistent file-based record of scraped data alongside MongoDB storage.

### c. `my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`
*   **File Output Types**: Defines `OUTPUT_FILE = "ibiza_spotlight_calendar_events.json"` and `LOG_FILE` constants.
*   **Method of Generation**:
    *   **Log File**: Uses standard Python `logging.FileHandler` to save logs to the path defined by `LOG_FILE`.
    *   **JSON Data File**: The provided code **does not** show this script actually writing to `OUTPUT_FILE`. The `save_fast` function, which handles data persistence, saves directly to MongoDB. There's a fallback to `print(json.dumps(event_doc...))` if the database isn't available, but no direct utility call or file write operation for general event data output to `OUTPUT_FILE`.
*   **Configuration**: File paths are hardcoded constants.
*   **Data Saved**: Logs are saved. Event data is intended for MongoDB.
*   **Apparent Purpose**:
    *   Logging: For debugging and tracing execution.
    *   The `OUTPUT_FILE` constant suggests an original intent for JSON file output, which might have been superseded by direct MongoDB saving or is part of an unobserved section of the code. As is, JSON file output for events is not actively implemented in the reviewed code.

### d. `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`
*   **File Output Types**: JSON, CSV, Markdown.
*   **Method of Generation**: Uses `save_to_json_file`, `save_to_csv_file`, and `save_to_markdown_file` from `scraper_utils.py`.
*   **Configuration**:
    *   Output directory is configurable via its `ScraperConfig` dataclass (`output_dir`, default "output/ibiza_spotlight_pw").
    *   Filename prefix is hardcoded as "ibiza_spotlight_final_pw_events".
*   **Data Saved**: List of unified event data (`self.all_scraped_events_for_run`).
*   **Apparent Purpose**: Similar to `mono_ticketmaster.py`; providing file-based outputs for debugging, local analysis, and archival in addition to MongoDB.

### e. `my_scrapers/unified_scraper.py`
*   **File Output Types**: Potentially JSON, CSV, Markdown.
*   **Method of Generation**:
    *   The `main()` function has an `args.format` parameter.
    *   It calls a function `save_events_to_file(events, filepath_base, args.format)`. However, the definition of `save_events_to_file` within this script is **commented out**.
    *   The commented-out `save_events_to_file` function appears to handle direct file writing for JSON and CSV, not using the `scraper_utils.py` utilities. The Markdown part specifically mentions writing to a hardcoded path for a single event.
*   **Configuration**: Output path seems to be constructed using `OUTPUT_DIR` (a global constant) and a generated filename.
*   **Data Saved**: Intended to save a list of `Event` dataclass objects.
*   **Apparent Purpose**: The file output functionality seems to be deprecated, partially implemented, or in a transitional state. The primary saving mechanism in this script is `save_event_to_db`, which saves to MongoDB.

### f. `my_scrapers/ventura_crawler.py`
*   **File Output Types**:
    *   Error screenshots (PNG images).
    *   Log files (via `setup_logging`).
*   **Method of Generation**:
    *   Screenshots: Uses Playwright's `page.screenshot()` method directly.
    *   Logs: Standard Python logging configured by `setup_logging`.
    *   It does **not** use the `scraper_utils.py` functions for JSON, CSV, or Markdown output of its primary event data.
*   **Configuration**:
    *   Error screenshot directory is configured via `DEFAULT_CONFIG["error_screenshot_dir"]`.
    *   Log file name (`serpentscale_scraper.log`) is hardcoded in `setup_logging` but path comes from config.
    *   `DEFAULT_CONFIG` has an `"output_data_format": "json"` key, but this doesn't seem to be actively used to produce JSON event data files.
*   **Data Saved**: Error screenshots, execution logs. Event data is saved to an SQLite database.
*   **Apparent Purpose**:
    *   Screenshots: Debugging errors.
    *   Logs: Tracing execution and debugging.
    *   The primary data output is to SQLite, not flat files.

## 4. Inconsistencies Observed

*   **Utility Usage**: Some scrapers (`classy_clubtickets`, `mono_ticketmaster`, `scraper_ibizaspotlight_revised`) consistently use `scraper_utils.py` for JSON/CSV/Markdown. Others (`unified_scraper` has commented-out custom logic, `scraper_ibizaspotlight_playwright_calendar` lacks event file output, `ventura_crawler` doesn't produce event data files).
*   **Configuration of Outputs**: Output directories and filename prefixes are sometimes hardcoded within the scraper script (even if passed to the utility) versus being fully managed by a central configuration system.
*   **Enable/Disable File Outputs**: There's no consistent mechanism to easily enable or disable file outputs per scraper; it's often embedded in the main execution block.
*   **Data Consistency in Files**: While `scraper_utils.py` tries to handle data consistently (e.g., `default=str` for JSON, serializing complex types for CSV), the actual content and structure depend on the `data_list` provided by each scraper. If scrapers produce slightly different "unified" structures, the files will reflect that.
*   **Purpose Clarity**: The purpose of file outputs (debug, archive, ad-hoc query) isn't always explicitly stated or managed by configuration.

## 5. Perceived Necessity and Recommendations

Given the project's use of MongoDB as the primary data store for unified events, and Prefect for orchestration (which has its own logging and artifact storage):

*   **Primary Data Store**: MongoDB should remain the single source of truth for queryable event data.
*   **Debugging**:
    *   JSON outputs can be very useful for debugging individual scraper runs or inspecting the structure of data before it's saved to MongoDB.
    *   Error screenshots (as used by `ventura_crawler`) are valuable for UI-heavy scrapers.
*   **Ad-hoc Analysis/Reporting**:
    *   CSV outputs are convenient for quick analysis in spreadsheet software or by data analysts not directly querying MongoDB.
    *   Markdown outputs can be useful for generating human-readable reports from specific scraper runs, perhaps as Prefect artifacts.
*   **Archival/Backup**: If MongoDB backups are regularly performed, separate file-based archival of all scraped data might be redundant unless there's a specific requirement for long-term, immutable flat file storage.

**Recommendations**:

1.  **Standardize on `scraper_utils.py`**: All scrapers that need to produce JSON, CSV, or Markdown outputs should use the functions from `scraper_utils.py` for consistency.
2.  **Centralize Configuration for File Outputs**:
    *   Add global settings in `config.py` to enable/disable each file type output (e.g., `settings.scraper_globals.enable_json_output: bool`).
    *   Allow scraper-specific overrides for these flags and for their output directories/prefixes within their respective Pydantic config models (e.g., `settings.scrapers["myscraper"].output_dir_json`).
3.  **Default to OFF for Production**: In a production environment orchestrated by Prefect, routine generation of multiple large JSON/CSV/MD files for every scraper run might be excessive and consume unnecessary disk space. Consider having these outputs disabled by default in production configuration and enable them on-demand for specific debugging runs or ad-hoc needs (e.g., via Prefect parameters).
4.  **Debugging Tool**: Position file outputs primarily as debugging tools or for specific ad-hoc analysis tasks rather than a core part of the data persistence strategy.
5.  **`scraper_ibizaspotlight_playwright_calendar.py`**: If JSON output is desired, refactor it to use `scraper_utils.save_to_json_file` instead of relying on `print` statements or unimplemented constants.
6.  **`unified_scraper.py`**: Either complete or remove its commented-out file saving logic. If kept, it should use `scraper_utils.py`.
7.  **`ventura_crawler.py`**: Its current focus on SQLite for primary data and screenshots for errors is reasonable. If ad-hoc file dumps of its SQLite data are needed, a separate utility or script could handle that, rather than integrating general file outputs into its core scraping loop. Its log files are already managed.

By standardizing the mechanism and configuration of file outputs, their generation can be more controlled and their purpose better aligned with debugging and specific analytical needs, complementing the primary MongoDB data store.
