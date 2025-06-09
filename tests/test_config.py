import os
import unittest
from unittest import mock

# Temporarily add project root to sys.path for config import if tests are run directly
# and the project is not installed as a package.
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Now import settings from config
from config import Settings, settings as global_settings

class TestConfigLoading(unittest.TestCase):

    @mock.patch.dict(os.environ, {
        "MONGODB_URI": "mongodb://testuser:testpass@testhost:27017/testdb",
        "SCRAPER_DEFAULT_OUTPUT_DIR": "/tmp/test_output",
        "SCRAPER_DEFAULT_MIN_DELAY": "1.0",
        "SCRAPER_DEFAULT_MAX_DELAY": "2.0",
        "SCRAPER_DEFAULT_HEADLESS": "False",
    })
    def test_load_settings_from_env_variables(self):
        settings = Settings() # Load settings with mocked env
        self.assertEqual(settings.MONGODB_URI, "mongodb://testuser:testpass@testhost:27017/testdb")
        self.assertEqual(settings.SCRAPER_DEFAULT_OUTPUT_DIR, "/tmp/test_output")
        self.assertEqual(settings.SCRAPER_DEFAULT_MIN_DELAY, 1.0)
        self.assertEqual(settings.SCRAPER_DEFAULT_MAX_DELAY, 2.0)
        self.assertEqual(settings.SCRAPER_DEFAULT_HEADLESS, False)

    def test_default_settings_values(self):
        # Test with no env variables set (or relying on .env.example if it's loaded by default,
        # but Pydantic BaseSettings only loads .env by default if `env_file` is set and it exists)
        # For a clean test, ensure no interfering .env file is loaded or mock its absence.
        # Here, we assume default values are as defined in config.py if no .env or specific env vars are set.

        # Clear relevant env vars for this test if they were set globally by a previous test or shell
        with mock.patch.dict(os.environ, {}, clear=True):
            # Re-initialize settings to pick up cleared environment
            # Need to be careful if 'global_settings' is already imported and cached.
            # For truly isolated test of defaults, instantiate Settings directly.
            settings_with_defaults = Settings()

            self.assertEqual(settings_with_defaults.MONGODB_URI, "mongodb://localhost:27017/")
            self.assertEqual(settings_with_defaults.SCRAPER_DEFAULT_OUTPUT_DIR, "output")
            self.assertEqual(settings_with_defaults.SCRAPER_DEFAULT_MIN_DELAY, 2.5)
            self.assertEqual(settings_with_defaults.SCRAPER_DEFAULT_MAX_DELAY, 6.0)
            self.assertEqual(settings_with_defaults.SCRAPER_DEFAULT_HEADLESS, True)


class TestAPIServerConfigIntegration(unittest.TestCase):

    @mock.patch.dict(os.environ, {"MONGODB_URI": "mongodb://envvars.com/mydb"})
    @mock.patch("motor.motor_asyncio.AsyncIOMotorClient")
    def test_api_server_uses_env_mongodb_uri(self, mock_motor_client):
        # Reload api_server or its config dependency to pick up mocked env var
        # This is tricky because api_server.py might have already imported 'settings'
        # A robust way is to ensure the 'settings' object itself reflects the mock,
        # or to re-import modules under mock.

        # For simplicity, let's assume 'global_settings' (instance from config.py)
        # will be re-evaluated or we can patch it directly if needed.
        # Better: Re-initialize settings for the scope of this test.

        temp_settings = Settings()
        self.assertEqual(temp_settings.MONGODB_URI, "mongodb://envvars.com/mydb")

        # To test if api_server.py *uses* it, we'd typically need to
        # either run a part of api_server.py or check how it instantiates motor.
        # The current api_server.py instantiates motor at the module level.
        # So, we need to reload api_server.py after setting the mock.

        # Path the global 'settings' object that api_server.py imports
        with mock.patch('config.settings', temp_settings):
            if 'database.api_server' in sys.modules:
                del sys.modules['database.api_server'] # Remove to force re-import
            import database.api_server # Re-import to use mocked settings

            database.api_server.client # Access the client to trigger instantiation
            mock_motor_client.assert_called_once_with("mongodb://envvars.com/mydb")


# It's more complex to directly test if unified_scraper.py uses the settings
# without running its main() function or refactoring it to be more testable.
# However, we've tested that Settings class loads correctly.
# A more focused test for the scraper would mock `config.settings`
# and then call the scraper's initialization or relevant parts.

class TestScraperConfigIntegration(unittest.TestCase):

    @mock.patch.dict(os.environ, {
        "SCRAPER_DEFAULT_OUTPUT_DIR": "test_scraper_output",
        "SCRAPER_DEFAULT_MIN_DELAY": "0.1",
        "SCRAPER_DEFAULT_MAX_DELAY": "0.2",
        "SCRAPER_DEFAULT_HEADLESS": "True", # Note: Pydantic converts "False" to False, "True" to True
    })
    def test_scraper_initialization_uses_settings(self):
        # Re-initialize settings for this test's scope
        temp_scraper_settings = Settings()

        self.assertEqual(temp_scraper_settings.SCRAPER_DEFAULT_OUTPUT_DIR, "test_scraper_output")
        self.assertEqual(temp_scraper_settings.SCRAPER_DEFAULT_MIN_DELAY, 0.1)
        self.assertEqual(temp_scraper_settings.SCRAPER_DEFAULT_MAX_DELAY, 0.2)
        self.assertTrue(temp_scraper_settings.SCRAPER_DEFAULT_HEADLESS)

        # To test unified_scraper.py, we mock the 'settings' it imports
        with mock.patch('config.settings', temp_scraper_settings):
            # If unified_scraper is already imported, it might hold old settings.
            # For a clean test, ensure it's re-imported or its functions use the patched settings.
            if 'my_scrapers.unified_scraper' in sys.modules:
                del sys.modules['my_scrapers.unified_scraper']
            from my_scrapers.unified_scraper import IbizaSpotlightUnifiedScraper

            scraper_instance = IbizaSpotlightUnifiedScraper(
                # These args are now for operational params, defaults come from settings
            )

            # Assert that the instance was created with values from our mocked settings
            self.assertEqual(scraper_instance.min_delay, 0.1)
            self.assertEqual(scraper_instance.max_delay, 0.2)
            self.assertEqual(scraper_instance.headless, True)

            # Testing output_dir usage would require deeper integration or running parts of main()
            # For now, confirming constructor params are influenced by settings is a good step.

if __name__ == '__main__':
    unittest.main()
