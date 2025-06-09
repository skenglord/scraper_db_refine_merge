import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import os
import csv
import json
import re # Added for the new test case
from pathlib import Path
import sys

# Add project root to sys.path to allow importing project modules
# Assuming the test is run from the project root or the subtask environment handles paths
sys.path.insert(0, Path(__file__).resolve().parents[2].as_posix())

from my_scrapers.scraper_ibizaspotlight_revised_0506_final import IbizaSpotlightScraper, ScraperConfig, json_serial
from database.quality_scorer import QualityScorer

# Dummy config for the scraper
test_config = ScraperConfig(url="http://example.com", save_to_db=False)

class TestIbizaSpotlightScraperBugs(unittest.TestCase):

    def setUp(self):
        self.base_dir = Path(".").resolve() # Resolve to make it absolute
        self.log_dir = self.base_dir / "classy_skkkrapey" / "scrape_logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        self.mock_db_patch = patch('my_scrapers.scraper_ibizaspotlight_revised_0506_final.get_mongodb_connection', return_value=None)
        self.mock_db_conn = self.mock_db_patch.start()

        self.scraper = IbizaSpotlightScraper(config=test_config)
        self.scraper.scorer = QualityScorer()
        self.scraper.db = None

    def tearDown(self):
        self.mock_db_patch.stop()
        for f in self.log_dir.glob(f"scraped_events_{self.scraper.run_timestamp}*.csv"): # Match specific scraper instance's file
            if f.exists():
                try:
                    os.remove(f)
                except OSError as e:
                    print(f"Error removing test CSV {f}: {e}")
            else:
                print(f"Test CSV {f} not found for removal.")


    # Test has been refocused to directly test QualityScorer._score_location with string input
    def test_quality_scorer_score_location_handles_string_input(self):
        scorer = QualityScorer()
        location_string = "Some Venue Name As String"

        # No AttributeError should be raised
        try:
            score, details = scorer._score_location(location_string)
        except AttributeError:
            self.fail("_score_location raised AttributeError unexpectedly for string input.")

        self.assertIsInstance(score, float)
        self.assertIsInstance(details, dict)

        # For a non-empty string, score is 0.2 (base for string as venue) + 0.3 (for having a venue name) = 0.5
        self.assertEqual(score, 0.5, "Score for a string input (treated as venue name) should be 0.5.")

        self.assertIn("location_is_string_input", details.get("flags", []))
        self.assertIn("missing_address", details.get("flags", []))
        self.assertIn("missing_city", details.get("flags", []))

        # Test with an empty string
        score_empty, details_empty = scorer._score_location("")
        self.assertEqual(score_empty, 0.0)
        self.assertIn("location_is_string_input", details_empty.get("flags", []))
        self.assertIn("missing_location", details_empty.get("flags", [])) # or specific flag for empty string


    # Keep a test for save_event to ensure it DOESN'T raise the error due to its sanitization
    # This test also benefits from the QualityScorer being more robust,
    # ensuring save_event continues to work smoothly.
    def test_save_event_with_string_location_is_handled(self):
        event_data_problematic = {
            "title": "Test Event Location String Handled by save_event",
            "tickets_url": "http://example.com/event1_handled",
            "venue": "Some Venue Name",
            "location": "This is a string location for save_event handling test",
            "dateTime": {
                "start": datetime.now(timezone.utc),
                "displayText": "Some date",
                "timezone": "Europe/Madrid"
            },
            "scrapedAt": datetime.now(timezone.utc).isoformat(),
            "lineUp": [],
            "ticketInfo": {}
        }
        try:
            self.scraper.save_event(event_data_problematic)
        except AttributeError:
            self.fail("save_event should handle string location and not raise AttributeError from QualityScorer")

    def test_save_event_location_as_dict_works(self):
        event_data_correct = {
            "title": "Test Event Location Dict",
            "tickets_url": "http://example.com/event2",
            "location": {"venue": "Venue From Dict", "address": "123 Street", "city": "Ibiza"},
            "dateTime": {
                "start": datetime.now(timezone.utc),
                "displayText": "Some date",
                "timezone": "Europe/Madrid"
            },
            "scrapedAt": datetime.now(timezone.utc).isoformat(),
            "lineUp": [],
            "ticketInfo": {}
        }
        try:
            self.scraper.save_event(event_data_correct)
        except AttributeError as e:
            self.fail(f"save_event raised AttributeError unexpectedly for dict location: {e}")
        except Exception as e:
            self.fail(f"save_event raised an unexpected exception: {e}")

    def test_append_to_csv_with_nested_datetime_causes_type_error(self):
        event_with_nested_datetime = {
            "title": "Event with Nested DateTime",
            "tickets_url": "http://example.com/event3",
            "dateTime": {
                "start": datetime.now(timezone.utc),
                "end": datetime.now(timezone.utc) + timedelta(hours=2)
            },
            "scrapedAt": datetime.now(timezone.utc).isoformat(), # ensure it's a string for CSV
            "some_other_data": "test"
        }
        # Create a unique scraper instance for this test to have a unique CSV filename
        scraper_instance_csv_test = IbizaSpotlightScraper(config=test_config)
        scraper_instance_csv_test.db = None # Ensure no DB calls

        try:
            scraper_instance_csv_test.append_to_csv([event_with_nested_datetime])
        except TypeError:
            self.fail("append_to_csv raised TypeError unexpectedly, json_serial should handle nested datetimes.")

        # Cleanup the specific CSV file if created
        csv_file_to_remove = self.log_dir / f"scraped_events_{scraper_instance_csv_test.run_timestamp}.csv"
        if csv_file_to_remove.exists():
            os.remove(csv_file_to_remove)


    def test_append_to_csv_simple_data_works(self):
        event_simple = {
            "title": "Simple Event",
            "tickets_url": "http://example.com/event4",
            "description": "A plain event.",
            "scrapedAt": datetime.now(timezone.utc).isoformat()
        }
        scraper_instance_csv_test = IbizaSpotlightScraper(config=test_config)
        scraper_instance_csv_test.db = None
        try:
            scraper_instance_csv_test.append_to_csv([event_simple])
            csv_file_path = self.log_dir / f"scraped_events_{scraper_instance_csv_test.run_timestamp}.csv"
            self.assertTrue(csv_file_path.exists(), f"CSV file {csv_file_path.name} was not created")
            self.assertTrue(csv_file_path.stat().st_size > 0, f"CSV file {csv_file_path.name} is empty")
            if csv_file_path.exists(): # cleanup
                 os.remove(csv_file_path)
        except TypeError as e:
            self.fail(f"append_to_csv raised TypeError unexpectedly for simple data: {e}")

    def test_parse_json_ld_event_location_string(self):
        json_ld = {
            "@type": "MusicEvent", "name": "Event With String Location",
            "location": "Some Venue String", "startDate": "2025-07-01T19:00:00Z"
        }
        parsed_event = self.scraper.parse_json_ld_event(json_ld, "http://example.com", 2025)
        self.assertIsNotNone(parsed_event)
        self.assertIsInstance(parsed_event.get("location"), dict, "Location field should be a dict")
        self.assertEqual(parsed_event["location"].get("venue"), "Some Venue String")
        self.assertIsNone(parsed_event["location"].get("address"))
        self.assertIsNone(parsed_event["location"].get("city"))

    def test_parse_json_ld_event_location_dict(self):
        json_ld = {
            "@type": "MusicEvent", "name": "Event With Dict Location",
            "location": {"@type": "Place", "name": "Venue From Dict", "address": "123 Main St"},
            "startDate": "2025-07-01T19:00:00Z"
        }
        parsed_event = self.scraper.parse_json_ld_event(json_ld, "http://example.com", 2025)
        self.assertIsNotNone(parsed_event)
        self.assertIsInstance(parsed_event.get("location"), dict, "Location field should be a dict")
        self.assertEqual(parsed_event["location"].get("venue"), "Venue From Dict")
        self.assertEqual(parsed_event["location"].get("address"), "123 Main St")

    def test_parse_json_ld_event_location_missing(self):
        json_ld = {
            "@type": "MusicEvent", "name": "Event Missing Location",
            "startDate": "2025-07-01T19:00:00Z"
        }
        parsed_event = self.scraper.parse_json_ld_event(json_ld, "http://example.com", 2025)
        self.assertIsNotNone(parsed_event)
        self.assertIsInstance(parsed_event.get("location"), dict, "Location field should be a dict")
        self.assertIsNone(parsed_event["location"].get("venue"))
        self.assertIsNone(parsed_event["location"].get("address"))
        self.assertIsNone(parsed_event["location"].get("city"))

    def test_parse_json_ld_event_handles_timezone_correctly(self):
        minimal_event_ld = {
            "@type": "MusicEvent",
            "name": "Test Timezone Event",
            "startDate": "2025-08-01T20:00:00Z", # Needs a start date for standardize_datetime
            "location": {"name": "Test Venue"} # Needs a location for standardize_datetime
        }
        try:
            # Scraper is already initialized in setUp
            parsed_event = self.scraper.parse_json_ld_event(minimal_event_ld, "http://example.com/tz-test", 2025)
            self.assertIsNotNone(parsed_event)
            self.assertIn("scrapedAt", parsed_event)
            scraped_at_val = parsed_event["scrapedAt"]
            self.assertIsInstance(scraped_at_val, str)
            # Check if it's a valid ISO 8601 timestamp, ending with Z or +00:00
            self.assertTrue(re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+(Z|[+-]\d{2}:\d{2})$', scraped_at_val),
                            f"scrapedAt format is not valid ISO8601 UTC: {scraped_at_val}")
            # Attempt to parse it to further validate
            datetime.fromisoformat(scraped_at_val.replace('Z', '+00:00'))

        except NameError as e:
            self.fail(f"parse_json_ld_event raised NameError unexpectedly: {e}")
        except Exception as e:
            self.fail(f"parse_json_ld_event raised an unexpected exception: {e}")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
