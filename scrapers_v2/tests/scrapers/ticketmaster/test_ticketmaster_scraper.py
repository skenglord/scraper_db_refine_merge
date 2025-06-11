# scrapers_v2/tests/scrapers/ticketmaster/test_ticketmaster_scraper.py
import unittest
import pathlib
from bs4 import BeautifulSoup
import sys
import os
import asyncio
import logging
from datetime import datetime, timezone

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from scrapers_v2.scrapers.ticketmaster.ticketmaster_scraper import TicketmasterScraper, PlaceholderSettings
# Import Pydantic model and error for testing
from scrapers_v2.scrapers.ticketmaster.ticketmaster_datamodels import TicketmasterEventModel, PriceDetailModel, ArtistInfoModel
from pydantic import ValidationError, HttpUrl


MOCK_SCRAPER_CONFIG_UNIT_TESTS = {
    'target_urls': {'concerts': 'http://mockurl.com/concerts'},
    'selectors': {
        'event_card': '.event-card-container',
        'event_title_in_card': '.event-title-in-card',
        'event_date_in_card': '.event-date-in-card',
        'event_venue_in_card': '.event-venue-in-card',
        'event_url_in_card': 'a.event-link',
        'price_in_card': '.event-price-in-card',
        'json_ld_script': "script[type='application/ld+json']"
    },
    'playwright_settings': {'enabled': False},
    'scraping_settings': {'delays': { 'request_min_ms': 1, 'request_max_ms': 2}}
}

class TestTicketmasterScraperTransformations(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = PlaceholderSettings()
        cls.scraper = TicketmasterScraper(settings=cls.settings)
        cls.scraper.scraper_config = MOCK_SCRAPER_CONFIG_UNIT_TESTS
        cls.base_url = "http://mockevents.com"

    def test_transform_text(self):
        transform = TicketmasterScraper._transform_text
        self.assertEqual(transform("  text  "), "text")

    def test_transform_date_string(self):
        transform = TicketmasterScraper._transform_date_string
        # Returns datetime object now
        dt_obj = transform("2024-01-15T10:00:00-05:00")
        self.assertIsInstance(dt_obj, datetime)
        self.assertEqual(dt_obj.isoformat(), "2024-01-15T10:00:00-05:00")

        dt_obj_naive = transform("Jan 15, 2024 10:00 AM")
        self.assertIsInstance(dt_obj_naive, datetime)
        # Current logic in transform makes it UTC if naive
        # self.assertEqual(dt_obj_naive.tzinfo, timezone.utc)
        # self.assertEqual(dt_obj_naive.isoformat(), "2024-01-15T10:00:00+00:00")
        # Pydantic model will handle timezone localization to UTC if naive.
        # So, _transform_date_string should just parse it.
        self.assertIsNone(dt_obj_naive.tzinfo) # It's naive as per current _transform_date_string
        self.assertEqual(dt_obj_naive.isoformat(), "2024-01-15T10:00:00")


        self.assertIsNone(transform("Invalid Date"))

    def test_transform_price_string(self):
        transform = TicketmasterScraper._transform_price_string
        self.assertEqual(transform("From $19.99"), (19.99, "USD", "From $19.99"))

    def test_transform_url(self):
        transform = TicketmasterScraper._transform_url
        base = "http://example.com"
        # Returns HttpUrl object now
        self.assertEqual(str(transform("/event/123", base)), "http://example.com/event/123")
        self.assertEqual(str(transform("http://othersite.com/event", base)), "http://othersite.com/event")
        self.assertIsNone(transform("/invalid-url-no-base", None)) # Relative URL needs base

    def test_transform_event_data_to_pydantic_input_dict(self):
        # This tests the dictionary produced by _transform_event_data, ready for Pydantic
        raw_data = {
            'title': '  Test Event HTML ', 'date_text': 'Jan 16, 2025 7:00 PM',
            'price_text': '$25.00', 'url': '/test-event-html',
            'venue_name': 'The Test Venue HTML '
        }
        # _transform_event_data returns a dict, not a Pydantic model instance directly
        transformed_dict = self.scraper._transform_event_data(raw_data, self.base_url)

        self.assertIsNotNone(transformed_dict)
        self.assertEqual(transformed_dict.get('event_title'), 'Test Event HTML')
        self.assertIsInstance(transformed_dict.get('event_start_datetime'), datetime)
        self.assertEqual(transformed_dict.get('ticket_min_price'), 25.00)
        self.assertEqual(transformed_dict.get('ticket_currency'), 'USD')
        self.assertIsInstance(transformed_dict.get('event_url'), HttpUrl)
        self.assertEqual(str(transformed_dict.get('event_url')), f'{self.base_url}/test-event-html')
        self.assertEqual(transformed_dict.get('venue_name'), 'The Test Venue HTML')

    def test_pydantic_model_validation_from_transformed_data(self):
        # Test that the output of _transform_event_data can successfully create a Pydantic model
        raw_data = {
            'title': 'Valid Pydantic Event', 'date_text': '2025-08-15 10:00',
            'url': 'http://example.com/valid-pydantic', 'venue_name': 'Pydantic Place'
        }
        transformed_dict = self.scraper._transform_event_data(raw_data, self.base_url)
        self.assertIsNotNone(transformed_dict)

        try:
            event_model = TicketmasterEventModel(**transformed_dict)
            self.assertEqual(event_model.event_title, "Valid Pydantic Event")
            self.assertIsNotNone(event_model.event_id) # Should be generated
            self.assertEqual(event_model.event_url, HttpUrl("http://example.com/valid-pydantic"))
        except ValidationError as e:
            self.fail(f"Pydantic validation failed for valid data: {e.errors()}")

    def test_pydantic_model_fails_for_invalid_url_type(self):
        # Test that Pydantic validation fails if URL is not transformable to HttpUrl
        raw_data_invalid_url = {'title': 'Event With Invalid URL', 'url': "not a url string"}
        # _transform_url will return None for "not a url string"
        transformed_dict = self.scraper._transform_event_data(raw_data_invalid_url, self.base_url)

        # Pydantic model requires event_url, if it's None after transformation, it should fail
        with self.assertRaises(ValidationError):
            TicketmasterEventModel(**transformed_dict)

    def test_pydantic_model_generates_event_id(self):
        raw_data = {'event_title': "Test ID Gen", 'event_url': HttpUrl("http://example.com/id-gen-test")}
        # Simulate data that might come to Pydantic model directly (event_id not pre-filled)
        model = TicketmasterEventModel(**raw_data)
        self.assertIsNotNone(model.event_id)
        self.assertTrue(len(model.event_id) > 0)


class TestTicketmasterScraperIntegration(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.settings = PlaceholderSettings()
        self.scraper = TicketmasterScraper(settings=self.settings)
        logging.getLogger("scrapers_v2.ticketmaster").setLevel(logging.WARNING)

    async def test_scraper_initialization_loads_config_and_sets_client(self):
        # ... (same as before)
        self.assertIsNotNone(self.scraper.scraper_config)
        self.assertTrue(len(self.scraper.scraper_config.get('target_urls', {})) > 0)
        if self.scraper.scraper_config.get('playwright_settings', {}).get('enabled', False):
            self.assertIsInstance(self.scraper.client, PlaceholderPlaywrightClient)
        else:
            self.assertIsInstance(self.scraper.client, PlaceholderHttpClient)

    async def test_scrape_live_events_single_page_returns_pydantic_models(self):
        self.scraper.scraper_config['playwright_settings']['enabled'] = False
        self.scraper._init_client()
        self.assertIsInstance(self.scraper.client, PlaceholderHttpClient)

        mock_html_page1 = """
        <html><head><script type="application/ld+json">{
            "@context": "http://schema.org", "@type": "Event", "name": "Event Alpha (JSON)",
            "startDate": "2025-10-01T20:00:00Z", "url": "/event-alpha"}</script></head>
        <body><div class="event-card-container">
            <a class="event-link" href="/event-alpha"><h3 class="event-title-in-card">Event Alpha (HTML)</h3></a>
            <p class="event-date-in-card">Oct 1, 2025</p></div>
        <div class="event-card-container">
            <a class="event-link" href="/event-beta"><h3 class="event-title-in-card">Event Beta</h3></a>
            <p class="event-date-in-card">Oct 5, 2025</p></div>
        </body></html>"""

        # Ensure client is PlaceholderHttpClient and then set mock_html
        if isinstance(self.scraper.client, PlaceholderHttpClient):
            self.scraper.client.set_mock_html(mock_html_page1)

        results = await self.scraper.scrape_live_events() # Should now return List[TicketmasterEventModel]

        self.assertIsNotNone(results)
        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], TicketmasterEventModel)

        event_alpha = next((e for e in results if e.event_url.path == "/event-alpha"), None)
        event_beta = next((e for e in results if e.event_url.path == "/event-beta"), None)

        self.assertIsNotNone(event_alpha)
        self.assertEqual(event_alpha.event_title, "Event Alpha (JSON)")
        self.assertIsInstance(event_alpha.event_start_datetime, datetime)
        self.assertTrue(event_alpha.event_start_datetime.isoformat().startswith("2025-10-01"))

        self.assertIsNotNone(event_beta)
        self.assertEqual(event_beta.event_title, "Event Beta")

    async def test_scrape_live_events_with_pagination_returns_pydantic_models(self):
        self.scraper.scraper_config['playwright_settings']['enabled'] = True
        self.scraper.scraper_config['scraping_settings']['max_load_more_clicks'] = 1
        self.scraper._init_client()
        self.assertIsInstance(self.scraper.client, PlaceholderPlaywrightClient)

        initial_page_html = """<html><head><title>Page 1</title></head><body>
        <div class="event-card-container">
            <a class="event-link" href="/event-page1"><h3 class="event-title-in-card">Event Page 1</h3></a>
            <p class="event-date-in-card">Nov 1, 2025</p></div>
        <button id="load-more-events">Load More</button></body></html>"""

        load_more_page_html = """<div class="event-card-container">
            <a class="event-link" href="/event-page2"><h3 class="event-title-in-card">Event Page 2</h3></a>
            <p class="event-date-in-card">Nov 10, 2025</p></div>"""

        if isinstance(self.scraper.client, PlaceholderPlaywrightClient):
            self.scraper.client.set_mock_html_initial_page(initial_page_html)
            self.scraper.client.set_mock_html_load_more_pages([load_more_page_html])

        results = await self.scraper.scrape_live_events()

        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], TicketmasterEventModel)

        event_page1 = next((e for e in results if e.event_url.path == "/event-page1"), None)
        event_page2 = next((e for e in results if e.event_url.path == "/event-page2"), None)

        self.assertIsNotNone(event_page1)
        self.assertEqual(event_page1.event_title, "Event Page 1")

        self.assertIsNotNone(event_page2)
        self.assertEqual(event_page2.event_title, "Event Page 2")

if __name__ == '__main__':
    unittest.main()
