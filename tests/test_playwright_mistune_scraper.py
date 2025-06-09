import asyncio
import os
import sys
import pytest
from unittest.mock import patch, MagicMock # Added MagicMock for async context manager
from datetime import datetime, timezone # Added timezone

# Keep existing playwright importorskip
pytest.importorskip("playwright", reason="playwright not installed")
# Import Playwright's TimeoutError for specific exception testing if available
# If playwright is skipped, this import will also be skipped.
try:
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except ImportError:
    PlaywrightTimeoutError = RuntimeError # Fallback if playwright itself is skipped/not fully there


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import the module containing the function to test
import my_scrapers.playwright_mistune_scraper # To allow mocking its datetime
from my_scrapers.playwright_mistune_scraper import scrape_event_data


# Updated DummyLocator and DummyPage
class DummyLocator:
    def __init__(self, text="Event Title", should_raise=None):
        self._text = text
        self._should_raise = should_raise

    async def inner_text(self):
        if self._should_raise:
            raise self._should_raise
        return self._text

class DummyPage:
    def __init__(self, content_html="<html><h1 class='post-title'>Event Title</h1></html>", locator_instance=None):
        self.url = None
        self._content_html = content_html
        self._locator_instance = locator_instance if locator_instance else DummyLocator()
        self.goto_should_raise = None
        self.wait_for_selector_should_raise = None

    async def goto(self, url, timeout=30000): # Added default for timeout
        if self.goto_should_raise:
            raise self.goto_should_raise
        self.url = url
        return None # goto usually returns None or a Response object, adapt if Response needed

    async def wait_for_selector(self, selector, timeout=30000): # Added default for timeout
        if self.wait_for_selector_should_raise:
            raise self.wait_for_selector_should_raise
        return None # wait_for_selector usually returns an ElementHandle or None

    async def content(self):
        return self._content_html

    def locator(self, selector, timeout=None): # Added timeout argument
        if selector == 'h1.post-title':
            return self._locator_instance
        # Return a default locator that might raise if unexpected selector is used
        # For this test suite, we assume only 'h1.post-title' is critical from locator
        return DummyLocator(text="Unexpected Selector", should_raise=RuntimeError(f"Unexpected selector queried: {selector}"))


@pytest.mark.asyncio
async def test_scrape_event_data_success(): # Removed event_loop
    page = DummyPage()

    # Mock datetime.utcnow within the playwright_mistune_scraper module
    fixed_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc) # Make fixed_time offset-aware

    # Patching datetime directly within the module where it's imported and used.
    with patch('my_scrapers.playwright_mistune_scraper.datetime') as mock_dt:
        # Configure the utcnow() method of the mocked datetime object
        mock_dt.utcnow.return_value = fixed_time # utcnow() returns naive, so keep fixed_time naive if SUT expects naive
        # If SUT uses timezone.utc, then fixed_time should be aware.
        # The SUT's code: `datetime.utcnow().isoformat() + "Z"` implies utcnow() is used directly.
        # Let's adjust fixed_time to be naive UTC for utcnow() mock, then SUT adds 'Z'
        fixed_time_naive_for_utcnow = datetime(2023, 1, 1, 12, 0, 0)
        mock_dt.utcnow.return_value = fixed_time_naive_for_utcnow

        # For fromisoformat, it needs to return an aware object if string has 'Z'
        # This part is tricky as fromisoformat is a class method of datetime.datetime
        # If the SUT calls datetime.fromisoformat(), we need to mock that on the class mock_dt itself.
        # Let's assume now_iso_test in SUT is `datetime.utcnow().isoformat() + "Z"`
        # and then `datetime.fromisoformat(now_iso_test.replace("Z", "+00:00"))`
        # We only need to ensure mock_dt.utcnow is correct.

        data = await scrape_event_data(page, "http://example.com/success")

    assert data["title"] == "Event Title"
    assert data["url"] == "http://example.com/success"
    assert data["html"] == "<html><h1 class='post-title'>Event Title</h1></html>"

    # SUT creates now_iso_test = datetime.utcnow().isoformat() + "Z"
    # datetime.utcnow() is mocked to return fixed_time_naive_for_utcnow
    # So, now_iso_test becomes "2023-01-01T12:00:00Z"
    # Then SUT does scrapedAt = datetime.fromisoformat(now_iso_test.replace("Z", "+00:00"))
    # which results in an aware datetime: datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Corrected expectation based on SUT's use of naive datetime.isoformat() + "Z"
    expected_iso_time = fixed_time_naive_for_utcnow.isoformat() + "Z"

    assert data["scrapedAt"] == expected_iso_time
    assert data["updatedAt"] == expected_iso_time
    assert data["lastCheckedAt"] == expected_iso_time


@pytest.mark.asyncio
async def test_scrape_event_data_title_locator_fails(): # Removed event_loop
    error_to_raise = PlaywrightTimeoutError("Title not found") if "playwright.async_api" in sys.modules else RuntimeError("Title not found")
    locator_that_fails = DummyLocator(should_raise=error_to_raise)
    page = DummyPage(locator_instance=locator_that_fails)

    data = await scrape_event_data(page, "http://example.com/title_fail")
    assert data is None

@pytest.mark.asyncio
async def test_scrape_event_data_goto_fails(): # Removed event_loop
    page = DummyPage()
    error_to_raise = PlaywrightTimeoutError("Navigation failed") if "playwright.async_api" in sys.modules else RuntimeError("Navigation failed")
    page.goto_should_raise = error_to_raise

    data = await scrape_event_data(page, "http://example.com/goto_fail")
    assert data is None

@pytest.mark.asyncio
async def test_scrape_event_data_wait_for_selector_fails(): # Removed event_loop
    page = DummyPage()
    error_to_raise = PlaywrightTimeoutError("Body selector wait failed") if "playwright.async_api" in sys.modules else RuntimeError("Body selector wait failed")
    page.wait_for_selector_should_raise = error_to_raise

    data = await scrape_event_data(page, "http://example.com/wait_selector_fail")
    assert data is None
