import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.pymongo import PymongoIntegration
from sentry_sdk.integrations.prefect import PrefectIntegration # Requires sentry-sdk[prefect]

from scrapers_v2.config import settings

logger = logging.getLogger(__name__)

def init_sentry():
    """
    Initializes the Sentry SDK if a DSN is configured.
    Uses settings from scrapers_v2.config.settings.
    """
    sentry_settings = settings.sentry
    app_environment = settings.environment # Main application environment

    if sentry_settings.dsn:
        logger.info(f"Sentry DSN found. Initializing Sentry SDK for environment: '{sentry_settings.environment or app_environment}'.")

        # Determine the environment for Sentry: use specific Sentry env if set, else fallback to main app env
        effective_environment = sentry_settings.environment if sentry_settings.environment else app_environment

        integrations = [
            LoggingIntegration(
                level=logging.INFO,        # Capture info and above as breadcrumbs
                event_level=logging.ERROR  # Send errors as Sentry events
            ),
            PymongoIntegration(),
            # Add PrefectIntegration if Prefect is a core part of the execution monitored by Sentry
            # This requires `sentry-sdk[prefect]` to be installed.
            PrefectIntegration(),
            # Add other integrations as needed e.g. FastApiIntegration, CeleryIntegration etc.
        ]

        try:
            sentry_sdk.init(
                dsn=str(sentry_settings.dsn), # Ensure DSN is a string
                environment=effective_environment,
                traces_sample_rate=sentry_settings.traces_sample_rate if sentry_settings.enable_performance_monitoring else 0.0,
                profiles_sample_rate=sentry_settings.profiles_sample_rate if sentry_settings.enable_performance_monitoring else 0.0,
                integrations=integrations,
                # You can add more Sentry options here, like release version, debug flags etc.
                # release="your-app-version@1.0.0" # Example
            )
            logger.info("Sentry SDK initialized successfully.")
        except Exception as e:
            logger.error(f"Failed to initialize Sentry SDK: {e}", exc_info=True)
    else:
        logger.info("Sentry DSN not found in settings. Sentry SDK will not be initialized.")

if __name__ == "__main__":
    # This block is for testing the Sentry initialization directly.
    # To test this, you would need to have a .env file in the scrapers_v2 directory
    # with SENTRY_DSN="YOUR_ACTUAL_SENTRY_DSN" and optionally APP_ENV.

    # Example of how settings would be loaded and used:
    print(f"Attempting to initialize Sentry based on settings...")
    print(f"Sentry DSN from settings: {settings.sentry.dsn}")
    print(f"App Environment from settings: {settings.environment}")
    print(f"Sentry Environment override from settings: {settings.sentry.environment}")

    init_sentry()

    # Test capturing an exception (if Sentry is initialized)
    if settings.sentry.dsn:
        logger.info("Testing Sentry error capture...")
        try:
            division_by_zero = 1 / 0
        except ZeroDivisionError as e:
            sentry_sdk.capture_exception(e)
            logger.info("Test exception captured and sent to Sentry (if DSN is valid).")

        # Test logging integration
        logger.error("This is a test error log that should also go to Sentry.")
        logger.info("This is an info log (should be breadcrumb in Sentry).")
    else:
        logger.info("Sentry not initialized, so not sending test error.")
```
