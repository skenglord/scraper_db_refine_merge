import pytest
import os
import sys
from unittest.mock import patch, MagicMock

# Add project root to sys.path to allow direct imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from my_scrapers.mono_basic_html import BasicHTMLScraper, HAS_LXML
from lxml import etree # For catching specific ParserError
import requests.exceptions # For mocking network errors

# BasicHTMLScraper tests will go here

HTML_SIMPLE = "<html><body><h1>Title 1</h1><h1>Title 2</h1></body></html>"
HTML_CLASSES = '<div><p class="content">Hello</p><span class="content">World</span><p>Ignore</p></div>'
HTML_ID = '<div><p id="unique">Unique Content</p></div>'
HTML_ATTRIBUTE = '<div data-test="item1">Item 1</div><div data-test="item2">Item 2</div><div data-other="other">Other</div>'
HTML_MULTIPLE = '<h1>A Title</h1><p class="info">Some info</p><p class="info extra">More info</p>'
HTML_NO_ELEMENTS = '<div><p>Just some text</p></div>'
HTML_EMPTY = ""
HTML_CHILDREN_TEXT = '<div><p class="parent">Text <span>Child Text</span> Suffix</p></div>'

# HTML constants for XPath tests
HTML_XPATH_SIMPLE = "<html><body><h1>Title 1</h1><div><h1>Title 2</h1></div></body></html>"
HTML_XPATH_PREDICATE = '<div><p class="item">Item A</p><p class="other">Ignore</p><p class="item">Item B</p></div>'
HTML_XPATH_ATTRIBUTE = '<div><a href="link1.html">Link 1</a><a href="link2.html">Link 2</a></div>'
HTML_XPATH_MULTIPLE = '<h1>A Title</h1><div class="content"><p>Info</p></div><a href="#ref">Reference</a>'


@pytest.fixture
def basic_scraper():
    """Provides an instance of BasicHTMLScraper."""
    return BasicHTMLScraper()

# Example (will be expanded in next steps):
# def test_example_basic_scraper(basic_scraper):
#     assert basic_scraper is not None

# --- Tests for BasicHTMLScraper.extract_css ---

def test_extract_css_simple_tag(basic_scraper):
    selectors = ["h1"]
    expected = {"h1": ["Title 1", "Title 2"]}
    result = basic_scraper.extract_css(HTML_SIMPLE, selectors)
    assert result == expected

def test_extract_css_class_selector(basic_scraper):
    selectors = [".content"]
    expected = {".content": ["Hello", "World"]}
    result = basic_scraper.extract_css(HTML_CLASSES, selectors)
    assert result == expected

def test_extract_css_id_selector(basic_scraper):
    selectors = ["#unique"]
    expected = {"#unique": ["Unique Content"]}
    result = basic_scraper.extract_css(HTML_ID, selectors)
    assert result == expected

def test_extract_css_attribute_selector(basic_scraper):
    selectors = ["[data-test]"]
    expected = {"[data-test]": ["Item 1", "Item 2"]}
    result = basic_scraper.extract_css(HTML_ATTRIBUTE, selectors)
    assert result == expected

def test_extract_css_multiple_selectors(basic_scraper):
    selectors = ["h1", ".info"]
    expected = {"h1": ["A Title"], ".info": ["Some info", "More info"]}
    result = basic_scraper.extract_css(HTML_MULTIPLE, selectors)
    assert result == expected

def test_extract_css_no_elements_found(basic_scraper):
    selectors = [".nonexistent", "h2"]
    expected = {".nonexistent": [], "h2": []}
    result = basic_scraper.extract_css(HTML_NO_ELEMENTS, selectors)
    assert result == expected

def test_extract_css_empty_html(basic_scraper):
    selectors = ["h1"]
    expected = {"h1": []}
    result = basic_scraper.extract_css(HTML_EMPTY, selectors)
    assert result == expected

def test_extract_css_selector_with_children_text(basic_scraper):
    selectors = [".parent"]
    expected = {".parent": ["TextChild TextSuffix"]} # Adjusted to match SUT's get_text(strip=True) behavior
    result = basic_scraper.extract_css(HTML_CHILDREN_TEXT, selectors)
    assert result == expected

# --- Tests for BasicHTMLScraper.extract_xpath ---

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_simple_path(basic_scraper):
    # Note: SUT's extract_xpath gets text using node.text_content() if element is selected,
    # or node itself if it's a string result (e.g. from text() or attribute).
    # Then, strings are stripped.
    xpaths = ["//h1/text()"]
    expected = {"//h1/text()": ["Title 1", "Title 2"]}
    result = basic_scraper.extract_xpath(HTML_XPATH_SIMPLE, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_select_element_text_content(basic_scraper):
    # Test selecting the element itself and relying on .text_content()
    xpaths = ["//h1"]
    expected = {"//h1": ["Title 1", "Title 2"]} # .text_content() is applied by SUT
    result = basic_scraper.extract_xpath(HTML_XPATH_SIMPLE, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_path_with_predicate(basic_scraper):
    xpaths = ["//p[@class='item']/text()"]
    expected = {"//p[@class='item']/text()": ["Item A", "Item B"]}
    result = basic_scraper.extract_xpath(HTML_XPATH_PREDICATE, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_select_attribute(basic_scraper):
    xpaths = ["//a/@href"]
    expected = {"//a/@href": ["link1.html", "link2.html"]}
    result = basic_scraper.extract_xpath(HTML_XPATH_ATTRIBUTE, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_multiple_expressions(basic_scraper):
    xpaths = ["//h1/text()", "//div[@class='content']/p/text()", "//a/@href"]
    expected = {
        "//h1/text()": ["A Title"],
        "//div[@class='content']/p/text()": ["Info"],
        "//a/@href": ["#ref"]
    }
    result = basic_scraper.extract_xpath(HTML_XPATH_MULTIPLE, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_no_nodes_found(basic_scraper):
    # Using HTML_NO_ELEMENTS from CSS tests as it's suitable
    xpaths = ["//h2/text()", "//div[@id='nonexistent']"]
    expected = {"//h2/text()": [], "//div[@id='nonexistent']": []}
    result = basic_scraper.extract_xpath(HTML_NO_ELEMENTS, xpaths)
    assert result == expected

@pytest.mark.skipif(not HAS_LXML, reason="lxml not installed")
def test_extract_xpath_empty_html(basic_scraper):
    # Using HTML_EMPTY from CSS tests
    xpaths = ["//body/p/text()"]
    # Expected: lxml parser raises ParserError on empty string
    with pytest.raises(etree.ParserError, match="Document is empty"):
        basic_scraper.extract_xpath(HTML_EMPTY, xpaths)

def test_extract_xpath_lxml_not_available(basic_scraper):
    with patch('my_scrapers.mono_basic_html.HAS_LXML', False):
        with pytest.raises(RuntimeError, match="XPath extraction requires the 'lxml' package."):
            basic_scraper.extract_xpath("<html><body><p>test</p></body></html>", ["//p/text()"])

# --- Tests for BasicHTMLScraper.fetch_page ---

@patch('requests.Session.get')
def test_fetch_page_success(mock_requests_get, basic_scraper):
    mock_response = MagicMock()
    mock_response.text = "<html><body>Success</body></html>"
    mock_response.raise_for_status = MagicMock() # Does not raise
    mock_requests_get.return_value = mock_response

    url = "http://example.com"
    result = basic_scraper.fetch_page(url)

    assert result == "<html><body>Success</body></html>"
    mock_requests_get.assert_called_once_with(url, timeout=10)

@patch('requests.Session.get')
def test_fetch_page_request_exception(mock_requests_get, basic_scraper):
    url = "http://example.com/timeout"
    mock_requests_get.side_effect = requests.exceptions.Timeout("Connection timed out")

    result = basic_scraper.fetch_page(url)

    assert result is None
    mock_requests_get.assert_called_once_with(url, timeout=10)

@patch('requests.Session.get')
def test_fetch_page_http_error(mock_requests_get, basic_scraper):
    url = "http://example.com/404"
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Client Error")
    mock_requests_get.return_value = mock_response

    result = basic_scraper.fetch_page(url)

    assert result is None
    mock_requests_get.assert_called_once_with(url, timeout=10)
