import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
from pathlib import Path
import json
import importlib # Added for reloading

# Add project root to sys.path
sys.path.insert(0, Path(__file__).resolve().parents[2].as_posix())

# Import the target module
from crawl_components import crawler_ibizatickets

# Define a sample EventSchema structure for mocking (consistent with the one in crawler_ibizatickets)
# This helps in creating mock return values for the parser.
class LocationSchema(crawler_ibizatickets.LocationSchema, total=False): pass
class DateTimeSchema(crawler_ibizatickets.DateTimeSchema, total=False): pass
class ArtistSchema(crawler_ibizatickets.ArtistSchema, total=False): pass
class TicketInfoSchema(crawler_ibizatickets.TicketInfoSchema, total=False): pass
class EventSchema(crawler_ibizatickets.EventSchema, total=False): pass


class TestCrawlerIbizaTickets(unittest.TestCase):

    def setUp(self):
        # This will be used by the crawler when it tries to import components
        self.mock_dual_mode_fetcher = MagicMock(spec=crawler_ibizatickets.DualModeFetcherCS)
        self.mock_parse_json_ld = MagicMock(spec=crawler_ibizatickets.parse_json_ld_event_cs)
        self.mock_format_markdown = MagicMock(spec=crawler_ibizatickets.format_event_to_markdown_cs)

        # Create a dictionary for patching the component imports
        self.patcher_dict = {
            'scraping_components.fetch_page_dual_mode_cs.DualModeFetcherCS': self.mock_dual_mode_fetcher,
            'parse_components.parse_json_ld_event_cs.parse_json_ld_event_cs': self.mock_parse_json_ld,
            'parse_components.parse_json_ld_event_cs.EventSchema': EventSchema, # Use our defined schema
            'parse_components.parse_json_ld_event_cs.LocationSchema': LocationSchema,
            'parse_components.parse_json_ld_event_cs.DateTimeSchema': DateTimeSchema,
            'parse_components.parse_json_ld_event_cs.ArtistSchema': ArtistSchema,
            'parse_components.parse_json_ld_event_cs.TicketInfoSchema': TicketInfoSchema,
            'parse_components.format_event_to_markdown_cs.format_event_to_markdown_cs': self.mock_format_markdown,
        }

        # Patch COMPONENTS_AVAILABLE to True by default for most tests
        self.components_patch = patch('crawl_components.crawler_ibizatickets.COMPONENTS_AVAILABLE', True)
        self.components_patch.start()

        # This patcher will mock the imports *inside* crawler_ibizatickets
        self.module_patcher = patch.dict('sys.modules', {
            'scraping_components.fetch_page_dual_mode_cs': MagicMock(DualModeFetcherCS=self.mock_dual_mode_fetcher),
            'parse_components.parse_json_ld_event_cs': MagicMock(
                parse_json_ld_event_cs=self.mock_parse_json_ld,
                EventSchema=EventSchema,
                LocationSchema=LocationSchema,
                DateTimeSchema=DateTimeSchema,
                ArtistSchema=ArtistSchema,
                TicketInfoSchema=TicketInfoSchema
            ),
            'parse_components.format_event_to_markdown_cs': MagicMock(format_event_to_markdown_cs=self.mock_format_markdown),
        })
        self.module_patcher.start()

        # Reload the module to ensure it picks up our mocks for COMPONENTS_AVAILABLE and imports
        # unittest.mock.reloading.reload(crawler_ibizatickets) # Not available in stdlib
        # Instead, we will control COMPONENTS_AVAILABLE directly for fallback tests

        # Reload the module to apply the patches from module_patcher at import time for crawler_ibizatickets
        importlib.reload(crawler_ibizatickets)

    def tearDown(self):
        self.components_patch.stop()
        self.module_patcher.stop()
        # Optionally, reload again to reset to a completely fresh state,
        # though subsequent setUp should handle it.
        importlib.reload(crawler_ibizatickets)


    def test_scrape_event_successful_extraction(self):
        mock_fetcher_instance = self.mock_dual_mode_fetcher.return_value.__enter__.return_value
        mock_fetcher_instance.fetch_page.return_value = "<html><body>Mock HTML</body></html>"

        mock_event_data: EventSchema = {
            "title": "Test Event",
            "description": "Event description",
            "extractionMethod": "json-ld"
            # Add other fields as per EventSchema if parse_json_ld_event_cs returns them
        }
        self.mock_parse_json_ld.return_value = mock_event_data
        self.mock_format_markdown.return_value = "Formatted Markdown"

        test_url = "http://ticketsibiza.com/event/test-event"
        result = crawler_ibizatickets.scrape_ibiza_tickets_event(test_url, mock_fetcher_instance)

        mock_fetcher_instance.fetch_page.assert_called_once_with(test_url, use_browser_override=True)
        self.mock_parse_json_ld.assert_called_once()
        self.mock_format_markdown.assert_called_once_with(result) # result should be augmented event_data

        self.assertIsNotNone(result)
        self.assertEqual(result["title"], "Test Event")
        self.assertEqual(result["url"], test_url)
        self.assertIn("scrapedAt", result)
        self.assertEqual(result["extractionMethod"], "json-ld")

    def test_scrape_event_no_json_ld_found(self):
        mock_fetcher_instance = self.mock_dual_mode_fetcher.return_value.__enter__.return_value
        mock_fetcher_instance.fetch_page.return_value = "<html><body>No JSON-LD here</body></html>"
        self.mock_parse_json_ld.return_value = None # Simulate parser finding nothing

        test_url = "http://ticketsibiza.com/event/no-json"
        result = crawler_ibizatickets.scrape_ibiza_tickets_event(test_url, mock_fetcher_instance)

        mock_fetcher_instance.fetch_page.assert_called_once_with(test_url, use_browser_override=True)
        self.mock_parse_json_ld.assert_called_once()
        self.mock_format_markdown.assert_not_called() # Should not be called if no event data
        self.assertIsNone(result)

    def test_scrape_event_fetch_fails(self):
        mock_fetcher_instance = self.mock_dual_mode_fetcher.return_value.__enter__.return_value
        mock_fetcher_instance.fetch_page.return_value = None # Simulate fetch failure

        test_url = "http://ticketsibiza.com/event/fetch-fail"
        result = crawler_ibizatickets.scrape_ibiza_tickets_event(test_url, mock_fetcher_instance)

        mock_fetcher_instance.fetch_page.assert_called_once_with(test_url, use_browser_override=True)
        self.mock_parse_json_ld.assert_not_called()
        self.mock_format_markdown.assert_not_called()
        self.assertIsNone(result)

    def test_scrape_event_components_unavailable_fallback(self):
        # Use patch as a context manager to control its scope precisely with reloads
        with patch('crawl_components.crawler_ibizatickets.COMPONENTS_AVAILABLE', False):
            importlib.reload(crawler_ibizatickets) # Reload with COMPONENTS_AVAILABLE = False

            test_url = "http://ticketsibiza.com/event/fallback-test"

            # In the reloaded module:
            # - DualModeFetcherCS should now be the DummyFetcher.
            # - parse_json_ld_event_cs should be the dummy_parse_json_ld_event_cs.
            # - format_event_to_markdown_cs should be the dummy_format_event_to_markdown_cs.

            fetcher_instance = crawler_ibizatickets.DualModeFetcherCS() # This will be DummyFetcher

            # The DummyFetcher's fetch_page returns a string with some basic JSON-LD.
            # The dummy_parse_json_ld_event_cs (internal to the reloaded module) returns None.
            # The dummy_format_event_to_markdown_cs (internal) should not be called.

            # To verify that the internal dummy functions are indeed called (or not called),
            # we can further patch them *on the reloaded module object* if needed,
            # but for now, let's trust the reload sets them up as per the module's logic.
            # The main outcome is that result should be None.

            # crawler_ibizatickets.py uses print() for logging, not a logger object.
            # So, we cannot patch a logger here. We will rely on the functional outcome.
            result = crawler_ibizatickets.scrape_ibiza_tickets_event(test_url, fetcher_instance)

            self.assertIsNone(result, "scrape_ibiza_tickets_event should return None when dummy parser returns None.")

            # If we wanted to check print calls, it would be more involved:
            # with patch('builtins.print') as mock_print:
            #     result = crawler_ibizatickets.scrape_ibiza_tickets_event(test_url, fetcher_instance)
            #     self.assertIsNone(result)
            #     mock_print.assert_any_call("Using DummyFetcher as component import failed.") # Example check

        # After exiting the 'with patch' block, COMPONENTS_AVAILABLE is restored.
        # The reload in tearDown will ensure the module is reset for other tests.

    def test_main_function_flow(self):
        test_url = "http://ticketsibiza.com/event/main-flow-test"
        args = MagicMock()
        args.url = test_url

        mock_fetcher_instance = self.mock_dual_mode_fetcher.return_value.__enter__.return_value
        mock_fetcher_instance.fetch_page.return_value = "<html><body>Mock HTML for main</body></html>"

        mock_event_data: EventSchema = {"title": "Main Flow Event", "extractionMethod": "json-ld"}
        self.mock_parse_json_ld.return_value = mock_event_data
        self.mock_format_markdown.return_value = "Main Flow Markdown"

        with patch('crawl_components.crawler_ibizatickets.argparse.ArgumentParser') as mock_argparse:
            mock_argparse.return_value.parse_args.return_value = args
            with patch('builtins.print') as mock_print: # Suppress print output
                crawler_ibizatickets.main()

        mock_fetcher_instance.fetch_page.assert_called_with(test_url, use_browser_override=True)
        self.mock_parse_json_ld.assert_called_once()


        # Check that the event data (augmented with url, scrapedAt) was passed to format_event_to_markdown_cs
        # The actual argument to format_event_to_markdown_cs will be the dictionary *after* scrape_ibiza_tickets_event modifies it
        augmented_event_data_passed_to_markdown = self.mock_format_markdown.call_args[0][0]
        self.assertEqual(augmented_event_data_passed_to_markdown["title"], "Main Flow Event")
        self.assertEqual(augmented_event_data_passed_to_markdown["url"], test_url)
        self.assertIn("scrapedAt", augmented_event_data_passed_to_markdown)

        # Check that JSON output was attempted
        # The last call to print (or one of the last) should be the JSON dump
        json_output_found = False
        for call_args in mock_print.call_args_list:
            try:
                output_str = call_args[0][0]
                if isinstance(output_str, str) and '"title": "Main Flow Event"' in output_str:
                    json.loads(output_str) # Check if it's valid JSON
                    json_output_found = True
                    break
            except (json.JSONDecodeError, IndexError):
                continue
        self.assertTrue(json_output_found, "JSON output of scraped_data not found in print calls")

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
