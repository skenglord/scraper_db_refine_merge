# Configuration Migration Plan to Unified System

This document outlines a detailed plan for transitioning existing scrapers and components to the proposed unified configuration system, which leverages `pydantic-settings` via the central `config.py` module.

## 1. Introduction

The goal of this migration is to standardize how configuration is managed across the project, leading to improved maintainability, consistency, type safety, and easier deployment. This plan provides general principles, step-by-step refactoring guidance, and a suggested order for migrating components.

## 2. Pre-requisites

*   **Unified `config.py` Defined**: The structure of the central `config.py` (as proposed in `docs/unified_configuration_proposal.md`) should be implemented. This includes the main `Settings` class and placeholders for scraper-specific configuration models.
*   **Understanding of Pydantic**: Developers involved should have a basic understanding of Pydantic models and `pydantic-settings`.

## 3. General Principles

*   **Incremental Approach**: Migrate one scraper or a small group of related scrapers at a time. This minimizes risk and allows for learning and adjustments during the process.
*   **Version Control**: All changes must be done in separate Git branches. This allows for easy review, rollback, and parallel work if needed. Merge changes only after thorough testing.
*   **Backup Critical Data**: Although configuration changes shouldn't directly affect data, ensure any critical files or databases are backed up before starting, especially when modifying database-related configurations.
*   **Thorough Testing**: After refactoring each scraper, it must be tested thoroughly to ensure it operates correctly with the new configuration system. This includes testing default values, environment variable overrides, and command-line argument overrides.
*   **Documentation Updates**: Any scraper-specific README files or internal documentation regarding its configuration must be updated to reflect the changes. The central `config.py` should also be well-commented.
*   **Communication**: If multiple developers are working on the project, communicate clearly about which scrapers are being refactored to avoid conflicts.
*   **Phased Rollout**: For critical scrapers, consider a phased rollout in staging or a controlled environment before deploying to production.

## 4. Per-Scraper Refactoring Steps

The following steps should be applied to each scraper or component being migrated:

### Step 1: Analyze Current Configuration Usage
*   **Identify all sources**: For the target scraper, list every way it currently receives configuration. This includes:
    *   Hardcoded constants (URLs, paths, selectors, delays, API endpoints).
    *   Internal `DEFAULT_CONFIG` dictionaries.
    *   Parameters passed to its `__init__` method or other functions.
    *   Local dataclasses used for configuration.
    *   Direct calls to `os.getenv()`.
    *   `argparse` definitions if it's a command-line script.
*   **Categorize parameters**: Differentiate between:
    *   Secrets (API keys, credentials).
    *   Behavioral settings (headless mode, delays, retry counts).
    *   Target-specific settings (base URLs, specific selectors, paths).
    *   Runtime parameters (e.g., a specific URL to scrape given at runtime, not a default).

### Step 2: Update Central `config.py`
*   **Define Scraper-Specific Model**: If one doesn't exist, create a new Pydantic `BaseSettings` model in `config.py` for the scraper (e.g., `MyScraperSettings(BaseSettings)`).
    ```python
    # In config.py
    class MyScraperSettings(BaseSettings):
        target_url: str = "http://default.example.com"
        max_retries: int = 3
        specific_selector: str = ".default-selector"
        # Add other fields based on analysis from Step 1

        model_config = SettingsConfigDict(env_prefix='MYSCRAPER_', extra='ignore')
    ```
*   **Integrate into Main `Settings`**: Add an instance of this new model to the main `Settings` class. The recommended way is using a dictionary:
    ```python
    # In config.py, within the main Settings class
    class Settings(BaseSettings):
        # ... other settings ...
        scrapers: Dict[str, BaseSettings] = {
            # ... other existing scrapers ...
            "myscraper": MyScraperSettings(), # Add new scraper
        }
        # ... model_config ...
    ```
    Alternatively, for a few key scrapers, direct attributes can be used: `myscraper_settings: MyScraperSettings = MyScraperSettings()`. The dictionary approach is more scalable.
*   **Global Settings**: If any parameters from the scraper are actually global (e.g., a common delay setting), add them to `GlobalScraperSettings` or another relevant shared model if not already present.
*   **Secrets**: Ensure any secrets (API keys, etc.) are defined in `ApiKeySettings` or a similar dedicated model, using `Field(validation_alias=AliasChoices(...))` for flexible environment variable naming.

### Step 3: Refactor Scraper Code
*   **Import `settings`**: At the beginning of the scraper file, import the global `settings` object:
    ```python
    from config import settings # Assuming config.py is in the Python path
    ```
*   **Remove Old Configuration**: Delete or comment out the old `DEFAULT_CONFIG` dictionary, local configuration dataclasses, and direct `os.getenv()` calls that are now handled by `config.py`.
*   **Access New Configuration**: Replace all references to old configuration values with accesses via the `settings` object.
    *   Example: `old_value = DEFAULT_CONFIG["target_url"]` becomes `new_value = settings.scrapers["myscraper"].target_url`.
    *   If using direct attributes: `new_value = settings.myscraper_settings.target_url`.
*   **Update Constructor (`__init__`)**:
    *   Remove parameters from `__init__` that are now sourced from the central `settings`.
    *   The class should internally reference `settings` for these values.
    *   Retain parameters for true runtime inputs not part of static config (e.g., a list of URLs to process dynamically provided).
*   **Refactor `argparse` (if applicable)**:
    *   Modify `argparse` definitions so that default values for arguments are sourced from the `settings` object.
    *   The value parsed by `argparse` will then naturally override the value from `settings` if provided on the command line.
    ```python
    # Example for argparse
    parser.add_argument(
        "--target-url",
        default=settings.scrapers["myscraper"].target_url, # Default from settings
        help="Target URL for the scraper."
    )
    args = parser.parse_args()
    # Use args.target_url in the script. It will be the command-line value or the settings default.
    ```
*   **Hardcoded Values**: Replace hardcoded URLs, paths, delays, etc., with values from the `settings` object. For selectors, move common or configurable ones to the scraper's Pydantic model. Highly dynamic or very specific, one-off selectors might remain in code but should be reviewed.

### Step 4: Environment and Documentation
*   **`.env.example`**: Add new environment variables (e.g., `MYSCRAPER_TARGET_URL`, `MYSCRAPER_MAX_RETRIES`) to the `.env.example` file to document them for other users/deployments.
*   **Local `.env`**: Update local `.env` files with these new variables for testing.
*   **Scraper Documentation**: Update any READMEs or documentation specific to the scraper to reflect how its configuration is now managed (primarily via environment variables through `config.py`).

### Step 5: Testing
*   **Default Values**: Test the scraper without setting any specific environment variables or command-line arguments for it, ensuring it runs correctly with the defaults defined in `config.py`.
*   **Environment Variable Overrides**: Test by setting the scraper-specific environment variables (e.g., `export MYSCRAPER_TARGET_URL="http://new-target.com"`) and verify the scraper uses these new values.
*   **`.env` File Loading**: Test that values from the `.env` file are correctly loaded.
*   **`argparse` Overrides**: If applicable, test that command-line arguments correctly override values from environment variables or defaults.
*   **Functionality**: Perform a test run of the scraper to ensure its core logic remains unaffected and it achieves its scraping goals.

### Step 6: Review and Merge
*   Commit changes to the Git branch.
*   Create a Pull Request for review.
*   After approval and any CI checks pass, merge the branch into the main development line.

## 5. Suggested Migration Order

This order prioritizes foundational changes and simpler scrapers first.

1.  **`database/etl_sqlite_to_mongo.py`**:
    *   **Rationale**: Already uses `os.getenv` for DB settings. Formalizing this into `config.py` (e.g., under `settings.db`) is a small, good first step.
    *   **Action**: Update `etl_sqlite_to_mongo.py` to import `settings` from `config.py` and use `settings.db.sqlite_db_path`, `settings.db.mongodb_uri`, etc.

2.  **Scrapers Already Attempting to Use Central `settings`**:
    *   Files: `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`, `my_scrapers/unified_scraper.py`, `my_scrapers/mono_ticketmaster.py`, `crawl_components/crawler_ibizatickets.py`, `crawl_components/crawler_spotlightibiza.py`.
    *   **Rationale**: These are partially aligned. The main task is to ensure their specific configurations are properly modeled in `config.py` (e.g., in `settings.scrapers["scraper_name"]`) and fallbacks are removed or standardized.
    *   **Action**: For each, define its specific Pydantic model in `config.py`, refactor to use it, and remove dummy/fallback settings. Update `argparse` defaults.

3.  **`my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`**:
    *   **Rationale**: Currently uses many hardcoded constants and imports `settings` for DB.
    *   **Action**: Create a specific Pydantic model for it in `config.py`. Move its constants (BASE_URL, CALENDAR_URL, USER_AGENT, selectors if stable) into this model. Refactor the script to use these settings from the global `settings` object.

4.  **Scrapers with `DEFAULT_CONFIG` Dictionaries**:
    *   Files: `my_scrapers/ventura_crawler.py`, `my_scrapers/classy_clubtickets_nav_scraper.py`.
    *   **Rationale**: These have self-contained configurations. `ventura_crawler.py` is complex. `classy_clubtickets_nav_scraper.py` might be simpler to start with.
    *   **Action**: For each:
        *   Create its specific Pydantic model in `config.py`.
        *   Transfer all items from its `DEFAULT_CONFIG` into the new model, setting appropriate `env_prefix`.
        *   Refactor the scraper to remove `DEFAULT_CONFIG` and `config_overrides` logic related to static config, instead importing and using `settings.scrapers["scraper_name"]`. Constructor overrides should only be for runtime data.

5.  **`crawl_components/club_tickets_crawl_logic.py`**:
    *   **Rationale**: Uses a global `config` dictionary. It's more of a script than a class-based scraper.
    *   **Action**: Define a specific Pydantic model for it in `config.py`. Refactor the script to import `settings` and use the values from its specific model instead of the global `config` dict.

## 6. Handling Common Challenges

*   **Selectors**:
    *   Stable, common selectors can be moved to Pydantic configuration models.
    *   For highly dynamic selectors or those requiring complex logic, they might need to remain in code or be managed by a different system (e.g., a selector database if selector learning is robustly implemented). The goal is to externalize what *can* be reasonably configured.
*   **Shared Constants vs. Scraper-Specific**: Carefully decide if a value is truly global (e.g., a default user agent for all scrapers, put in `GlobalScraperSettings`) or specific to one scraper (put in its own model).
*   **Testing Utilities**: It might be beneficial to create helper functions or fixtures for tests that can easily mock or provide different `Settings` configurations.

## 7. Post-Migration Cleanup

*   Once all components are migrated, remove any old, unused configuration files or utility functions related to deprecated config methods.
*   Ensure the main project README and any developer setup guides are updated to reflect the unified configuration system (e.g., how to use `.env` files, common environment variables).

This migration plan provides a structured approach to standardizing configuration. Flexibility and careful testing at each step will be key to a successful transition.
