import os
from pathlib import Path
from typing import Dict, Optional, List, Any

from pydantic import Field, AliasChoices, HttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

class MongoDBSettings(BaseSettings):
    """MongoDB connection and database settings."""
    uri: str = Field("mongodb://localhost:27017/", validation_alias=AliasChoices('MONGODB_URI', 'MONGO_URI'))
    database: str = Field("scraper_data_v2", validation_alias=AliasChoices('MONGODB_DATABASE', 'MONGO_DATABASE'))
    default_unified_collection: str = Field("unified_events", validation_alias=AliasChoices('MONGODB_DEFAULT_UNIFIED_COLLECTION', 'MONGO_DEFAULT_UNIFIED_COLLECTION'))

    model_config = SettingsConfigDict(
        env_prefix='MONGODB_',
        extra='ignore',
        populate_by_name=True
    )

class GlobalScraperSettings(BaseSettings):
    """Global settings applicable to most scrapers."""
    default_user_agent: str = Field(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        validation_alias=AliasChoices('SCRAPER_GLOBAL_DEFAULT_USER_AGENT', 'DEFAULT_USER_AGENT')
    )
    default_request_timeout_ms: int = Field(30000, validation_alias=AliasChoices('SCRAPER_GLOBAL_DEFAULT_REQUEST_TIMEOUT_MS', 'DEFAULT_REQUEST_TIMEOUT_MS'))
    min_delay_ms: int = Field(1000, validation_alias=AliasChoices('SCRAPER_GLOBAL_MIN_DELAY_MS', 'MIN_DELAY_MS'))
    max_delay_ms: int = Field(5000, validation_alias=AliasChoices('SCRAPER_GLOBAL_MAX_DELAY_MS', 'MAX_DELAY_MS'))
    default_headless_browser: bool = Field(True, validation_alias=AliasChoices('SCRAPER_GLOBAL_DEFAULT_HEADLESS_BROWSER', 'DEFAULT_HEADLESS_BROWSER'))

    model_config = SettingsConfigDict(
        env_prefix='SCRAPER_GLOBAL_',
        extra='ignore',
        populate_by_name=True
    )

class FileOutputSettings(BaseSettings):
    """Settings for controlling file-based outputs."""
    base_output_directory: Path = Field(Path("output_data_v2"), validation_alias=AliasChoices('FILE_OUTPUT_BASE_OUTPUT_DIRECTORY', 'BASE_OUTPUT_DIRECTORY'))
    enable_json_output: bool = Field(False, validation_alias=AliasChoices('FILE_OUTPUT_ENABLE_JSON_OUTPUT', 'ENABLE_JSON_OUTPUT'))
    enable_csv_output: bool = Field(False, validation_alias=AliasChoices('FILE_OUTPUT_ENABLE_CSV_OUTPUT', 'ENABLE_CSV_OUTPUT'))
    enable_markdown_output: bool = Field(False, validation_alias=AliasChoices('FILE_OUTPUT_ENABLE_MARKDOWN_OUTPUT', 'ENABLE_MARKDOWN_OUTPUT'))
    log_output_directory: Path = Field(Path("scraper_logs_v2"), validation_alias=AliasChoices('FILE_OUTPUT_LOG_OUTPUT_DIRECTORY', 'LOG_OUTPUT_DIRECTORY'))
    enable_error_screenshots: bool = Field(True, validation_alias=AliasChoices('FILE_OUTPUT_ENABLE_ERROR_SCREENSHOTS', 'ENABLE_ERROR_SCREENSHOTS'))
    screenshot_directory: Path = Field(Path("error_screenshots_v2"), validation_alias=AliasChoices('FILE_OUTPUT_SCREENSHOT_DIRECTORY', 'SCREENSHOT_DIRECTORY'))

    model_config = SettingsConfigDict(
        env_prefix='FILE_OUTPUT_',
        extra='ignore',
        populate_by_name=True
    )

class SentrySettings(BaseSettings):
    """Configuration for Sentry error tracking."""
    dsn: Optional[HttpUrl] = Field(None, validation_alias=AliasChoices('SENTRY_DSN'))
    environment: Optional[str] = Field(None, description="Overrides main app environment for Sentry if needed. Typically inherits from Settings.environment.")
    traces_sample_rate: float = Field(0.2, ge=0.0, le=1.0, description="Sentry performance monitoring traces sample rate.")
    profiles_sample_rate: float = Field(0.2, ge=0.0, le=1.0, description="Sentry profiling sample rate.")
    enable_performance_monitoring: bool = Field(True, description="Enable Sentry performance monitoring.")

    model_config = SettingsConfigDict(
        env_prefix='SENTRY_', # e.g. SENTRY_DSN
        extra='ignore',
        populate_by_name=True
    )

# --- Scraper-Specific Settings Models ---

class ClubTicketsSettings(BaseSettings):
    """Configuration specific to the ClubTickets scraper."""
    target_url: HttpUrl = Field("https://www.clubtickets.com/search?dates=31%2F05%2F25+-+01%2F11%2F25")
    max_pages_to_process: int = Field(2, description="Max date tabs to process for ClubTickets.")
    slow_mo_ms: int = Field(30, description="Slow down Playwright operations by N ms for ClubTickets.")
    viewport_width: int = 1280
    viewport_height: int = 720
    max_retries: int = 2
    retry_delay_sec: float = 0.8
    event_selectors: Dict[str, str] = Field(default_factory=lambda: {
        "title": "h3.title-event", "url_anchor": "a", "date_text": "span.date-day-month",
        "venue_name": "span.club-name", "image": "img.img-responsive", "price": "span.price"
    })
    event_card_selector: str = ".content-text-card"
    date_tab_xpath: str = "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"btn-custom-day-tab\", \" \" ))]"
    show_more_xpath: str = "//button[contains(concat(' ', normalize-space(@class), ' '), ' btn-more-events ') and contains(concat(' ', normalize-space(@class), ' '), ' more-events ') and text()='Show more events']"
    output_subfolder: str = "clubtickets"

    model_config = SettingsConfigDict(
        env_prefix='CLUBTICKETS_',
        extra='ignore'
    )

class AllScraperSpecificSettings(BaseSettings):
    """Container for all scraper-specific configurations."""
    clubtickets: ClubTicketsSettings = ClubTicketsSettings()

    model_config = SettingsConfigDict(extra='ignore')


class Settings(BaseSettings):
    """Main application settings."""
    environment: str = Field("development", validation_alias=AliasChoices('APP_ENV', 'ENVIRONMENT'))
    log_level: str = Field("INFO", validation_alias=AliasChoices('APP_LOG_LEVEL', 'LOG_LEVEL'))

    mongodb: MongoDBSettings = MongoDBSettings()
    scraper_globals: GlobalScraperSettings = GlobalScraperSettings()
    file_outputs: FileOutputSettings = FileOutputSettings()
    sentry: SentrySettings = SentrySettings() # Added SentrySettings

    scrapers_specific: AllScraperSpecificSettings = AllScraperSpecificSettings()

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        env_nested_delimiter='__',
        extra='ignore',
        populate_by_name=True
    )

settings = Settings()

def ensure_directories_exist():
    # This function should ideally be called once at application startup.
    # For scrapers_v2, this might be in a main __init__.py or a startup script.
    if settings.file_outputs.base_output_directory and not settings.file_outputs.base_output_directory.exists():
        settings.file_outputs.base_output_directory.mkdir(parents=True, exist_ok=True)
    if settings.file_outputs.log_output_directory and not settings.file_outputs.log_output_directory.exists():
        settings.file_outputs.log_output_directory.mkdir(parents=True, exist_ok=True)
    if settings.file_outputs.screenshot_directory and not settings.file_outputs.screenshot_directory.exists():
        settings.file_outputs.screenshot_directory.mkdir(parents=True, exist_ok=True)

# Example: Call it if this config is run as a script, or import and call from main app entry point
# if __name__ == "__main__":
#     ensure_directories_exist()
#     print(settings.model_dump_json(indent=2))
```
