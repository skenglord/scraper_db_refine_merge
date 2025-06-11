# Proposal for a Unified Configuration System

This document proposes a unified configuration system for all scrapers and components within the project. The goal is to enhance consistency, maintainability, type safety, and ease of management for configurations, including secrets and runtime arguments. The existing `pydantic-settings`-based `config.py` will serve as the foundation.

## 1. Core Principles

*   **Single Source of Truth (as much as possible)**: Configuration should be loaded and accessed through a centralized mechanism.
*   **Type Safety**: Leverage Pydantic for typed configuration models.
*   **Environment Variable Driven**: Prioritize environment variables for deployment flexibility and secrets management, aligning with 12-factor app principles.
*   **File-Based Configuration**: Support `.env` files for local development and potentially other structured files (e.g., YAML) for more complex configurations.
*   **Runtime Overrides**: Allow command-line arguments or programmatic overrides for specific execution needs.
*   **Clear Separation**: Differentiate between global settings, scraper-specific settings, and secrets.

## 2. Leveraging `config.py` and `pydantic-settings`

The existing `config.py` with its `Settings(BaseSettings)` class is the starting point.

```python
# config.py
import os
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Dict, List, Optional

class GlobalScraperSettings(BaseSettings):
    """Settings applicable to all scrapers."""
    default_output_dir: str = "output"
    min_delay_ms: int = 1000
    max_delay_ms: int = 5000
    default_headless_browser: bool = True
    default_user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    # Add more global scraper defaults here

class DatabaseSettings(BaseSettings):
    mongodb_uri: str = "mongodb://localhost:27017/"
    mongodb_db_name: str = "scraper_db"
    sqlite_db_path: str = "scraper_data.db" # For components still using SQLite

class ApiKeySettings(BaseSettings):
    """Central place for API keys (secrets)."""
    captcha_solver_api_key: Optional[str] = Field(None, validation_alias=AliasChoices('CAPTCHA_SOLVER_API_KEY', '2CAPTCHA_API_KEY'))
    # Add other API keys as needed

    model_config = SettingsConfigDict(env_prefix='', extra='ignore', populate_by_name=True)


# Placeholder for scraper-specific settings (see section 3)
class VenturaCrawlerSpecificSettings(BaseSettings):
    browser_pool_size: int = 3
    max_concurrent_scrapes: int = 5
    # ... other ventura_crawler specific settings ...
    model_config = SettingsConfigDict(env_prefix='VENTURA_', extra='ignore')

class ClubTicketsSpecificSettings(BaseSettings):
    event_card_selector: str = ".content-text-card"
    date_tab_xpath: str = "//*[contains(@class, 'btn-custom-day-tab')]"
    # ... other clubtickets specific settings ...
    model_config = SettingsConfigDict(env_prefix='CLUBTICKETS_', extra='ignore')


class Settings(BaseSettings):
    """Main settings class, aggregates all other settings."""
    environment: str = Field("development", validation_alias=AliasChoices('ENV', 'ENVIRONMENT'))
    log_level: str = "INFO"

    # Global settings
    db: DatabaseSettings = DatabaseSettings()
    api_keys: ApiKeySettings = ApiKeySettings()
    scraper_globals: GlobalScraperSettings = GlobalScraperSettings()

    # Scraper-specific settings (can be extended)
    # Use a dictionary to hold scraper-specific configs, keyed by scraper name
    scrapers: Dict[str, BaseSettings] = {
        "ventura_crawler": VenturaCrawlerSpecificSettings(),
        "clubtickets": ClubTicketsSpecificSettings(),
        # Add other scrapers here
    }
    # Alternatively, directly embed them if fewer scrapers:
    # ventura_settings: VenturaCrawlerSpecificSettings = VenturaCrawlerSpecificSettings()
    # clubtickets_settings: ClubTicketsSpecificSettings = ClubTicketsSpecificSettings()

    model_config = SettingsConfigDict(
        env_file=".env",
        env_nested_delimiter='__', # For loading nested models, e.g., DB__MONGODB_URI
        env_prefix='', # No global prefix, prefixes are per-model if needed
        extra='ignore',
        populate_by_name=True # Allows use of AliasChoices for env vars
    )

settings = Settings()

# Example usage in a scraper:
# from config import settings
# db_uri = settings.db.mongodb_uri
# ventura_pool_size = settings.scrapers["ventura_crawler"].browser_pool_size
# or if directly embedded: ventura_pool_size = settings.ventura_settings.browser_pool_size
```

## 3. Scraper-Specific Configurations

To manage configurations that vary per scraper (e.g., target URLs, CSS selectors, specific behavior flags):

**Proposed Approach: Nested Pydantic Models with Environment Variable Prefixes**

*   Define a Pydantic `BaseSettings` model for each scraper (e.g., `VenturaCrawlerSpecificSettings`, `ClubTicketsSpecificSettings`).
*   These models will include fields specific to that scraper.
*   Use `SettingsConfigDict(env_prefix='SCRAPERNAME_')` within each scraper-specific model to load its values from environment variables prefixed accordingly (e.g., `VENTURA_BROWSER_POOL_SIZE`).
*   The main `Settings` class can then include instances of these scraper-specific models, either directly as fields or in a dictionary.

**Advantages**:
*   **Type Safety**: All configurations are typed.
*   **Clear Structure**: Configuration is organized and discoverable.
*   **Environment Variable Compatibility**: Easy to override any setting via environment variables (e.g., `DB__MONGODB_URI`, `VENTURA__BROWSER_POOL_SIZE`).
*   **Central Access**: All settings are accessible via the main `settings` object.

**Alternative for very numerous/complex scraper configs**:
If the number of scrapers becomes very large or their configurations are extensive and changed often by non-developers, consider loading scraper-specific configurations from YAML or JSON files. Pydantic can be customized to load settings from such files. The main `Settings` class could then have a path to a directory of scraper config files.

## 4. Secrets Management

*   **Environment Variables**: Secrets (API keys, database credentials) should primarily be loaded from environment variables. This is supported by `pydantic-settings`.
*   **`.env` File**: For local development, a `.env` file (gitignored) can store these secrets. `pydantic-settings` loads this automatically.
*   **Dedicated Model**: Group secrets like API keys into a specific nested Pydantic model (e.g., `ApiKeySettings` within `Settings`) for clarity. Use `Field(validation_alias=AliasChoices(...))` to allow flexibility in naming environment variables (e.g., `API_KEY_XYZ` or `XYZ_API_KEY`).
*   **Security**: Ensure `.env` files are in `.gitignore`. In production, inject environment variables directly. Avoid hardcoding secrets.

## 5. Runtime Arguments (e.g., `argparse`)

Runtime arguments (like a specific URL to scrape, mode of operation like `crawl` vs `scrape`, or a one-off override of a setting) should take precedence over configurations from files or environment variables.

**Integration Strategy**:

1.  **Load Pydantic Settings First**: Initialize the `settings` object from `config.py`. This loads defaults, `.env`, and environment variables.
2.  **Define `argparse` Arguments**:
    *   For arguments that correspond to settings in the Pydantic model, their *default* values in `argparse` can be set *from* the already loaded `settings` object.
    *   This ensures that if a command-line argument is not provided, the value from the environment or `.env` file (or the Pydantic default) is used.
3.  **Parse Arguments**: Let `argparse` parse the command-line arguments.
4.  **Update Pydantic Settings (Optional but Recommended for Consistency)**: After parsing, if `argparse` provides values that need to be globally accessible as part of the configuration, update the corresponding fields in the `settings` object.
    ```python
    # In main script, e.g., unified_scraper.py
    from config import settings
    import argparse

    # ... (load settings instance) ...

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=settings.scraper_globals.default_target_url, help="Target URL")
    parser.add_argument("--headless", type=bool, default=settings.scraper_globals.default_headless_browser, help="Run headless")
    # Add other arguments, potentially sourcing defaults from settings.scrapers["scraper_name"].some_field

    args = parser.parse_args()

    # Update settings instance if necessary, or use 'args' directly for runtime parameters
    # For parameters that are truly runtime decisions (like the specific URL to scrape),
    # using args directly is often clearer.
    # For parameters that are configuration overrides (like headless mode), updating settings can be useful.
    settings.scraper_globals.default_headless_browser = args.headless
    # target_url_to_scrape = args.url # Use directly

    # Scraper logic then uses 'settings' or 'args' as appropriate.
    ```

This approach makes the command line the ultimate source of override for applicable settings.

## 6. Migration of Other Configuration Methods

*   **Internal `DEFAULT_CONFIG` Dictionaries**:
    *   **Action**: Deprecate.
    *   **Migration**: Move all key-value pairs into the appropriate scraper-specific Pydantic model (e.g., `VenturaCrawlerSpecificSettings`). Default values are set directly in the Pydantic model fields.
*   **Constructor Overrides/Direct Parameter Passing for Configuration**:
    *   **Action**: Minimize for static configuration. Retain for essential runtime parameters (e.g., a list of URLs to process if not from config) or dependency injection (e.g., passing a database client instance).
    *   **Migration**: Static configuration values previously passed via constructor should now be part of the `settings` object, accessed within the class. If a class needs its own config block, it should expect its specific Pydantic config model instance.
*   **Local Configuration Dataclasses**:
    *   **Action**: Deprecate if they are simple data holders for static config.
    *   **Migration**: Convert these into Pydantic `BaseSettings` models, potentially nested within the main `Settings` class or as part of the `settings.scrapers` dictionary.
*   **Hardcoded Constants and Values**:
    *   **Action**: Review critically.
    *   **Migration**:
        *   Move configurable parameters (URLs, file paths, delays, common selectors, behavior flags) to the relevant Pydantic `Settings` model.
        *   Retain truly immutable constants (e.g., mathematical constants, fundamental regex patterns that aren't site-specific tweaks).
        *   For complex selectors, consider if they can be parameterized or if a different approach (e.g., selector learning database) is more appropriate than static config. Common, stable selectors can be in config.
*   **Environment Variables (Direct `os.getenv`)**:
    *   **Action**: Deprecate direct usage in scraper logic.
    *   **Migration**: All environment variable access should be consolidated through the `pydantic-settings` mechanism in `config.py`. Scrapers then access these values from the `settings` object. This ensures type checking, central documentation, and clear defaults.
*   **Global `config` Dictionaries (e.g., in `crawl_components/club_tickets_crawl_logic.py`)**:
    *   **Action**: Deprecate.
    *   **Migration**: Convert into a scraper-specific Pydantic model or integrate into an existing relevant model.

## 7. Benefits of the Unified System

*   **Centralization**: `config.py` and the `settings` object become the primary entry point for configuration.
*   **Type Safety**: Reduces errors caused by incorrect configuration types.
*   **Discoverability**: Configuration options are explicitly defined in Pydantic models, making them easier to find and understand.
*   **Consistency**: All scrapers and components access configuration in a uniform way.
*   **Flexibility**: Supports environment variables, `.env` files, and runtime overrides in a clear order of precedence.
*   **Maintainability**: Easier to manage and update configurations, especially for secrets and deployment-specific settings.
*   **Scalability**: The nested model approach or dictionary of scraper settings in `config.Settings` allows for adding new scraper configurations without excessive clutter in the main `Settings` model.

This unified system provides a robust and maintainable way to handle configurations across the project, building upon the strengths of the existing `pydantic-settings` implementation.
