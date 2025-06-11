# File Output Standardization Plan

## 1. Introduction

This document outlines a plan to standardize the implementation and configuration of file outputs (JSON, CSV, Markdown, logs, screenshots) across all scraper scripts. The goal is to improve consistency, make file generation more configurable, and align it with the project's overall data management strategy, referencing findings from `docs/file_output_assessment.md`.

## 2. Centralizing File Output Configuration in `config.py`

To manage file outputs centrally, we will enhance the main `config.py` by adding a dedicated Pydantic model for file output settings.

**Proposed `FileOutputSettings` model in `config.py`**:

```python
# In config.py

from pydantic import DirectoryPath, Field # Assuming pydantic v2 for DirectoryPath
from pydantic_settings import BaseSettings
from pathlib import Path

# ... other settings models ...

class FileOutputSettings(BaseSettings):
    # Global enable/disable flags
    enable_json_output: bool = Field(False, description="Enable JSON file output for scrapers.")
    enable_csv_output: bool = Field(False, description="Enable CSV file output for scrapers.")
    enable_markdown_output: bool = Field(False, description="Enable Markdown file output for scrapers.")
    enable_error_screenshots: bool = Field(True, description="Enable saving screenshots on scraper errors.")

    # Base directories (relative to project root)
    # Using Path for better path management. pydantic-settings handles Path conversion.
    base_output_directory: Path = Path("output_files")
    log_output_directory: Path = Path("scraper_logs")
    screenshot_directory: Path = Path("error_screenshots") # For VenturaCrawler & potentially others

    # model_config for env variable loading if needed, e.g., env_prefix = "FILE_OUTPUT_"
    # For now, these are primarily code-configurable with sensible defaults.
    # Environment variables can override them if the main Settings model_config allows.

# Add to the main Settings class:
class Settings(BaseSettings):
    # ... other existing settings ...
    file_outputs: FileOutputSettings = FileOutputSettings()
    # ... model_config ...

settings = Settings()
```

**Usage**: Scrapers and utilities will import `settings` from `config.py` and access these values (e.g., `settings.file_outputs.enable_json_output`, `settings.file_outputs.base_output_directory`).

## 3. Standardizing `scraper_utils.py` File Saving Functions

The file saving functions in `my_scrapers/utils/scraper_utils.py` (`save_to_json_file`, `save_to_csv_file`, `save_to_markdown_file`, and `setup_logger`) will be enhanced and standardized:

**General Enhancements for all file/log utilities**:

1.  **Use Central Configuration**:
    *   Each utility must import `settings` from `config.py`.
    *   **Enable/Disable Check**: At the beginning of each `save_to_*_file` function, check the corresponding global flag (e.g., `if not settings.file_outputs.enable_json_output: logger.debug("JSON output disabled by global config."); return`).
    *   **Base Output Directory**: The `output_dir` parameter in these functions should now be treated as a subdirectory relative to `settings.file_outputs.base_output_directory`. The utility should construct the full path. If `output_dir` is not provided, files can be saved directly in the base directory, or a scraper-specific subdirectory can be created by default.
        ```python
        # Example within save_to_json_file
        # base_dir = settings.file_outputs.base_output_directory
        # if output_dir_param: # output_dir_param is the function's 'output_dir' argument
        #     target_dir = base_dir / output_dir_param
        # else:
        #     target_dir = base_dir / filename_prefix # Default to a subdir named after prefix
        # target_dir.mkdir(parents=True, exist_ok=True)
        ```
        The `_ensure_output_dir_exists` helper should be updated to use `settings.file_outputs.base_output_directory` as its root.
2.  **Standardized Naming Convention**:
    *   Filenames should consistently follow the pattern: `settings.file_outputs.base_output_directory / [optional_scraper_specific_subdir] / filename_prefix_YYYYMMDD_HHMMSS.ext`.
    *   The `filename_prefix` should clearly identify the scraper or data source.
3.  **Consistent Error Handling & Logging**:
    *   Ensure robust `try-except` blocks for `IOError` and other relevant exceptions.
    *   Use the provided `logger_obj` consistently. If `None`, use a default logger defined within `scraper_utils.py`.
    *   Log key information: whether saving is skipped due to global config, filename being written, success, or specific errors.
4.  **Directory Creation**: The `_ensure_output_dir_exists` helper should be used by all file-saving functions to create the target directory if it doesn't exist, now relative to the configured `base_output_directory`.

**Specific Function Adjustments**:

*   **`setup_logger`**:
    *   Modify to use `settings.file_outputs.log_output_directory` as the base path for log files.
*   **`save_to_json_file`**:
    *   Adhere to general enhancements regarding config and naming.
*   **`save_to_csv_file`**:
    *   Adhere to general enhancements.
    *   **Header Generation**: To handle varying keys in `data_list` items more robustly, collect all unique keys from *all* dictionaries in `data_list` to form the CSV header. This ensures all data is captured, though it might result in some empty cells if records have different structures. (Alternatively, enforce uniform dictionaries as input).
*   **`save_to_markdown_file`**:
    *   Adhere to general enhancements. (Further recommendations in Section 5).

## 4. Refactoring Plan for Existing Scrapers

Each scraper script needs to be refactored to use the standardized utilities and centralized configuration.

**General Steps for Each Scraper**:

1.  **Remove Local File Output Config**: Delete any hardcoded output paths, enable/disable flags for file outputs, or similar configurations from local `DEFAULT_CONFIG` or class attributes.
2.  **Import `settings`**: Ensure `from config import settings` is present.
3.  **Update Calls to Utility Functions**:
    *   When calling `save_to_json_file`, `save_to_csv_file`, or `save_to_markdown_file`:
        *   The `output_dir` parameter should now specify a subdirectory relative to `settings.file_outputs.base_output_directory` (e.g., `"my_scraper_name"`), or be omitted if the utility defaults to a prefix-based subdirectory.
        *   The `filename_prefix` should be a concise identifier for the scraper (e.g., `"clubtickets"`, `"ticketmaster_events"`).
    *   The decision to call these functions can be wrapped in `if settings.file_outputs.enable_json_output:` etc., or this check can be solely within the utility function itself (preferred, to simplify scraper code).

**Specific Scraper Refactoring Notes**:

*   **`my_scrapers/classy_clubtickets_nav_scraper.py`**:
    *   Modify its `if __name__ == "__main__":` block.
    *   Remove `output_dir`, `mongodb_uri`, `db_name`, `collection_name` from `DEFAULT_CONFIG` if these are to be fully centralized (MongoDB parts are covered by DB centralization). File output path from `DEFAULT_CONFIG` should be removed.
    *   Pass a suitable `filename_prefix` (e.g., "clubtickets_nav") to file saving utilities.
*   **`my_scrapers/mono_ticketmaster.py`**:
    *   Update its `run()` method. Remove `output_dir` from its local `ScraperConfig` if it's to be globally managed.
    *   Pass `filename_prefix="mono_ticketmaster"` to file saving utilities.
*   **`my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`**:
    *   Update its `run()` method. Remove `output_dir` from its `ScraperConfig`.
    *   Pass `filename_prefix="ibiza_spotlight_revised"` to file saving utilities.
*   **`my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`**:
    *   Remove the `OUTPUT_FILE` constant.
    *   If JSON output for events is desired, after `unified_events_list = await scrape_fast(page)`, add a call:
        ```python
        # from my_scrapers.utils.scraper_utils import save_to_json_file # Ensure import
        # logger is defined in this file
        # if settings.file_outputs.enable_json_output: # Check can be here or inside utility
        #    save_to_json_file(unified_events_list, "ibiza_spotlight_calendar", logger_obj=logger)
        ```
    *   Its `LOG_FILE` constant should be replaced by logic using `setup_logger` which sources its path from `settings.file_outputs.log_output_directory`.
*   **`my_scrapers/unified_scraper.py`**:
    *   Remove the commented-out `save_events_to_file` function.
    *   If file outputs are needed, in its `main()` function, after data collection, call the standardized utilities from `scraper_utils.py`, passing an appropriate `filename_prefix`. The `args.format` logic would need to be re-evaluated; enabling/disabling via `settings` is preferred over command-line for consistency.
*   **`my_scrapers/ventura_crawler.py`**:
    *   **Log Files**: Modify its `setup_logging` function. The `log_file` parameter should be constructed using `settings.file_outputs.log_output_directory` as the base. The hardcoded `serpentscale_scraper.log` could become a prefix.
    *   **Error Screenshots**: The `SerpentScaleScraper.__init__` method, where `error_screenshot_dir` is configured from `self.config`, should be updated to use `settings.file_outputs.screenshot_directory` as the base or full path.
    *   No changes needed for event data files as it doesn't produce them.

## 5. Specific Recommendations for Markdown Output

*   **Purpose**: Markdown outputs are best for human-readable summaries, quick reports of a small number of events, or as artifacts in a Prefect flow for immediate review. They are not ideal for large datasets or machine parsing.
*   **Configuration**: The `settings.file_outputs.enable_markdown_output` flag should default to `False`, especially in production environments. It should be enabled explicitly for debugging runs or specific reporting needs.
*   **Content of `save_to_markdown_file`**:
    *   The current utility in `scraper_utils.py` generates a detailed breakdown of each event. This is suitable for small lists.
    *   Consider adding a parameter to `save_to_markdown_file` like `summary_mode: bool = False`. If `True`, it could generate a more concise report (e.g., total events, list of titles, or first N events only) which might be more practical for routine runs if enabled. For now, the detailed version is acceptable as long as its generation is controlled.
*   **Prefect Artifacts**: Markdown files are excellent candidates for being saved as Prefect artifacts, making them easily accessible from the Prefect UI for a given flow run.

## 6. Benefits of Standardization

*   **Consistency**: All scrapers will produce file outputs (if enabled) in a uniform manner, with consistent naming and directory structures.
*   **Centralized Control**: Enables/disables and base paths for all file outputs can be managed from `config.py`, simplifying environment configuration.
*   **Reduced Redundancy**: Consolidates file-saving logic into `scraper_utils.py`, making it easier to maintain and update (e.g., improving error handling or serialization for all scrapers at once).
*   **Clarity**: Scraper code becomes cleaner by offloading file output details to the utility.
*   **Maintainability**: Easier to manage disk space and locate output files when a standard structure is followed.

By implementing this plan, the project will have a more organized, maintainable, and configurable approach to generating supplementary file outputs.
