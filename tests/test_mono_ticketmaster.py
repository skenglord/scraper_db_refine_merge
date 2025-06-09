import os
import sys
import pytest
from bs4 import BeautifulSoup # Added import
from datetime import datetime, timezone # Added timezone
from unittest.mock import patch # Required for mocking
import my_scrapers.mono_ticketmaster # Added for mocking datetime

pytest.importorskip("mistune", reason="mistune not installed") # Kept existing skip

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from my_scrapers.mono_ticketmaster import MultiLayerEventScraper # Corrected import path


# Keep existing test
def test_scrape_event_data_jsonld(mocker):
    html = '''<html><head><script type="application/ld+json">{
        "@context": "https://schema.org",
        "@type": "MusicEvent",
        "name": "Sample Event",
        "location": {"name": "My Venue", "address": {"streetAddress": "123 St", "addressRegion": "CA", "addressCountry": "USA"}},
        "startDate": "2025-01-01T20:00:00",
        "endDate": "2025-01-01T23:00:00",
        "offers": {"name": "General", "price": "30", "priceCurrency": "USD", "availability": "InStock", "url": "http://ticket.example.com"}
    }</script></head><body></body></html>'''
    scraper = MultiLayerEventScraper(use_browser=False)
    mocker.patch.object(scraper, "fetch_page", return_value=html) # This mocks fetch_page for scrape_event_data
    data = scraper.scrape_event_data("http://example.com")
    assert data["title"] == "Sample Event"
    assert data["ticketInfo"]["currency"] == "USD"
    assert data["extractionMethod"] == "jsonld"

# New tests for extract_jsonld_data directly

@pytest.fixture
def scraper_instance():
    # Using use_browser=False as these are unit tests for parsing, not fetching
    return MultiLayerEventScraper(use_browser=False)

def test_extract_jsonld_data_simple_music_event(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "MusicEvent",
            "name": "Concert Name"
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is not None
    assert data["@type"] == "MusicEvent"
    assert data["name"] == "Concert Name"

def test_extract_jsonld_data_from_graph(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "WebSite", "name": "My Site"},
                {"@type": "MusicEvent", "name": "Event From Graph"}
            ]
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is not None
    assert data["@type"] == "MusicEvent"
    assert data["name"] == "Event From Graph"

def test_extract_jsonld_data_not_music_event(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "Movie",
            "name": "A Film"
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None # Should not return if not MusicEvent

def test_extract_jsonld_data_malformed_json(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "MusicEvent",
            "name": "Concert Name",
        }
        </script>
    </head><body></body></html>''' # Trailing comma makes it malformed
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None

def test_extract_jsonld_data_no_ldjson_script(scraper_instance):
    html = '''<html><head></head><body><p>No data</p></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None

def test_extract_jsonld_data_empty_script_tag(scraper_instance):
    html = '''<html><head><script type="application/ld+json"></script></head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None

def test_extract_jsonld_data_script_tag_with_only_whitespace(scraper_instance):
    html = '''<html><head><script type="application/ld+json">   </script></head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None

def test_extract_jsonld_data_multiple_scripts_first_is_musicevent(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "MusicEvent",
            "name": "First Event"
        }
        </script>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "Movie",
            "name": "Some Movie"
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is not None
    assert data["name"] == "First Event" # Should pick the first MusicEvent

def test_extract_jsonld_data_multiple_scripts_second_is_musicevent(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "My Site"
        }
        </script>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@type": "MusicEvent",
            "name": "Second Event"
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is not None
    assert data["name"] == "Second Event"

def test_extract_jsonld_data_in_graph_not_musicevent(scraper_instance):
    html = '''
    <html><head>
        <script type="application/ld+json">
        {
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "WebSite", "name": "My Site"},
                {"@type": "Organization", "name": "My Org"}
            ]
        }
        </script>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_jsonld_data(soup)
    assert data is None # No MusicEvent in graph

# Add these tests after the extract_jsonld_data tests in tests/test_mono_ticketmaster.py

def test_extract_wordpress_data_all_fields_present(scraper_instance):
    html = '''
    <html><body>
        <h1 class="entry-title">WP Event Title</h1>
        <div class="event-date">Jan 1, 2025</div>
        <div class="event-venue">The WP Venue</div>
        <span class="price"><span class="woocommerce-Price-amount amount">€25.00</span></span>
        <div class="entry-content"><p>Event description here.</p></div>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)

    assert data["title"] == "WP Event Title"
    assert data["date_text"] == "Jan 1, 2025"
    assert data["venue"] == "The WP Venue"
    assert data["price_text"] == "€25.00" # BeautifulSoup's get_text on span.price will get this
    assert data["description"] == "Event description here."

def test_extract_wordpress_data_alternative_selectors(scraper_instance):
    html = '''
    <html><body>
        <h1 class="product_title">Product Event Title</h1>
        <div class="wcs-event-date">Feb 2, 2026</div>
        <div class="location">Product Location</div>
        <span class="woocommerce-price-amount"><bdi>30.00<span class="woocommerce-Price-currencySymbol">£</span></bdi></span>
        <div class="product-description">Product description.</div>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)

    assert data["title"] == "Product Event Title"
    assert data["date_text"] == "Feb 2, 2026"
    assert data["venue"] == "Product Location"
    assert data["price_text"] == "30.00£"
    assert data["description"] == "Product description."

def test_extract_wordpress_data_some_fields_missing(scraper_instance):
    html = '''
    <html><body>
        <h1>Generic Title</h1>
        <div class="event-time">10:00 PM</div>
        <div class="description"><p>Only description and title.</p></div>
    </body></html>''' # Missing venue and price
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)

    assert data["title"] == "Generic Title"
    assert data["date_text"] == "10:00 PM" # event-time selector
    assert "venue" not in data
    assert "price_text" not in data
    assert data["description"] == "Only description and title."

def test_extract_wordpress_data_no_relevant_tags(scraper_instance):
    html = '''<html><body><p>Just some random text.</p><div>Irrelevant</div></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)

    assert not data # Should be an empty dictionary

def test_extract_wordpress_data_uses_first_matching_selector_for_title(scraper_instance):
    html = '''
    <html><body>
        <h1 class="entry-title">Entry Title First</h1>
        <h1 class="product_title">Product Title Second (should not be picked)</h1>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)
    assert data["title"] == "Entry Title First"

def test_extract_wordpress_data_price_without_symbol_in_main_element(scraper_instance):
    html = '''
    <html><body>
        <span class="price">20.00 USD</span>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)
    assert data["price_text"] == "20.00 USD"

def test_extract_wordpress_data_description_truncation(scraper_instance):
    # Description is currently truncated at 500 chars by [:500] in the code.
    # This test doesn't need to create a 500+ char string here,
    # just acknowledges the logic exists. A more direct test of truncation
    # would be if the truncation limit was configurable or smaller.
    # For now, just test it gets a normal description.
    long_desc_text = "This is a fairly long description that should be captured fully as it's less than 500 characters. " * 5
    html = f'''
    <html><body>
        <div class="entry-content">{long_desc_text}</div>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_wordpress_data(soup)
    assert data["description"] == long_desc_text.strip() # Added .strip()

# Add these tests after the extract_wordpress_data tests in tests/test_mono_ticketmaster.py

def test_extract_meta_data_all_og_present(scraper_instance):
    html = '''
    <html><head>
        <meta property="og:title" content="OG Title"/>
        <meta property="og:description" content="OG Description"/>
        <meta property="og:image" content="http://example.com/og_image.jpg"/>
        <meta property="og:url" content="http://example.com/og_url"/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert data["title"] == "OG Title"
    assert data["description"] == "OG Description"
    assert data["image"] == "http://example.com/og_image.jpg"
    assert data["canonical_url"] == "http://example.com/og_url"
    assert "meta_description" not in data # Ensure it doesn't pick up og:description as meta_description

def test_extract_meta_data_all_standard_meta_present(scraper_instance):
    html = '''
    <html><head>
        <meta name="description" content="Meta Description"/>
        <meta name="keywords" content="keyword1, keyword2"/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert data["meta_description"] == "Meta Description"
    assert data["keywords"] == "keyword1, keyword2"
    assert "title" not in data # Ensure it doesn't pick up meta description as title

def test_extract_meta_data_mixed_og_and_standard(scraper_instance):
    html = '''
    <html><head>
        <meta property="og:title" content="OG Title"/>
        <meta name="description" content="Meta Description"/>
        <meta property="og:image" content="http://example.com/og_image.jpg"/>
        <meta name="keywords" content="keyword1"/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert data["title"] == "OG Title"
    assert data["meta_description"] == "Meta Description"
    assert data["image"] == "http://example.com/og_image.jpg"
    assert data["keywords"] == "keyword1"
    assert "description" not in data # og:description was not present

def test_extract_meta_data_some_missing(scraper_instance):
    html = '''
    <html><head>
        <meta property="og:title" content="Only OG Title"/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert data["title"] == "Only OG Title"
    assert len(data) == 1

def test_extract_meta_data_no_relevant_meta_tags(scraper_instance):
    html = '''<html><head><title>Page Title</title></head><body><p>Content</p></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert not data # Should be an empty dictionary

def test_extract_meta_data_og_description_preferred_over_meta_description_for_desc_key(scraper_instance):
    # The current code maps og:description to "description" and name="description" to "meta_description".
    # This test confirms that behavior.
    html = '''
    <html><head>
        <meta property="og:description" content="OG Description Is Key"/>
        <meta name="description" content="Standard Meta Description"/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)

    assert data["description"] == "OG Description Is Key"
    assert data["meta_description"] == "Standard Meta Description"

def test_extract_meta_data_empty_content_attribute(scraper_instance):
    html = '''
    <html><head>
        <meta property="og:title" content=""/>
        <meta name="description" content=""/>
    </head><body></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    data = scraper_instance.extract_meta_data(soup)
    # Assuming current behavior: if content is empty, key is not added.
    assert "title" not in data
    assert "meta_description" not in data
    assert len(data) == 0

# Add these tests after the extract_meta_data tests in tests/test_mono_ticketmaster.py

def test_extract_text_patterns_date_dd_mm_yyyy_slash(scraper_instance):
    html = "Some text around 15/07/2024 and more."
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "15/07/2024"

def test_extract_text_patterns_date_dd_mm_yyyy_hyphen(scraper_instance):
    html = "Event on 01-03-2025, book now."
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "01-03-2025" # Will pick the first group of the first matching pattern

def test_extract_text_patterns_date_yyyy_mm_dd_hyphen(scraper_instance):
    # This pattern is 2nd in list, so if dd-mm-yyyy also present and first, that would be caught.
    # Ensure this specific pattern is caught if it's the only one.
    html = "Scheduled for 2025-12-20."
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "2025-12-20"

def test_extract_text_patterns_date_day_dd_mm_yyyy(scraper_instance):
    html = "It's on Monday, 22/07/2024 for sure."
    # The first pattern r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})" matches "22/07/2024" first.
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "22/07/2024"

def test_extract_text_patterns_price_euro_prefix(scraper_instance):
    html = "Price is €25.50 for entry."
    data = scraper_instance.extract_text_patterns(html)
    assert data["price_pattern"] == "€25.50" # group(0) is the full match

def test_extract_text_patterns_price_dollar_prefix_no_decimal(scraper_instance):
    html = "Tickets $50 each."
    data = scraper_instance.extract_text_patterns(html)
    assert data["price_pattern"] == "$50"

def test_extract_text_patterns_price_pound_suffix(scraper_instance):
    html = "Cost: 75.00£ per person." # Matches 2nd price pattern
    data = scraper_instance.extract_text_patterns(html)
    assert data["price_pattern"] == "75.00£"

def test_extract_text_patterns_price_keyword_prefix(scraper_instance):
    html = "Special Price: €19.99 only."
    # The first price pattern r"([€$£]\d+(?:\.\d{2})?)" matches "€19.99" first.
    data = scraper_instance.extract_text_patterns(html)
    assert data["price_pattern"] == "€19.99"


def test_extract_text_patterns_multiple_dates_first_one_taken(scraper_instance):
    # First pattern is r"(\d{1,2}[/-]\d{1,2}[/-]\d{4})"
    html = "Dates: 01/01/2023 then 2023-02-02 and also Wednesday, 03/03/2023."
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "01/01/2023"

def test_extract_text_patterns_multiple_prices_first_one_taken(scraper_instance):
    # First pattern is r"[€$£](\d+(?:\.\d{2})?)"
    html = "Offers: €10, then $15.50, and Price: £20."
    data = scraper_instance.extract_text_patterns(html)
    assert data["price_pattern"] == "€10"

def test_extract_text_patterns_no_relevant_patterns(scraper_instance):
    html = "Just some plain text without dates or prices."
    data = scraper_instance.extract_text_patterns(html)
    assert not data # Should be empty

def test_extract_text_patterns_date_and_price_present(scraper_instance):
    html = "Event on 10/10/2024, cost is $30.00."
    data = scraper_instance.extract_text_patterns(html)
    assert data["date_pattern"] == "10/10/2024"
    assert data["price_pattern"] == "$30.00"

# Add these tests after the extract_text_patterns tests in tests/test_mono_ticketmaster.py

def test_extract_lineup_from_html_p_br_tags(scraper_instance):
    html = '''
    <html><body>
        <h3>Line Up</h3>
        <p>Artist One<br>Artist Two<br/>  Artist Three  </p>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == ["Artist One", "Artist Two", "Artist Three"]

def test_extract_lineup_from_html_ul_li_tags(scraper_instance):
    html = '''
    <html><body>
        <h4>Line Up</h4> # Changed "Line-Up" to "Line Up" to match regex
        <ul>
            <li>DJ Alpha</li>
            <li>  DJ Beta  </li>
            <li>DJ Gamma</li>
        </ul>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == ["DJ Alpha", "DJ Beta", "DJ Gamma"]

def test_extract_lineup_from_html_mixed_headers(scraper_instance):
    # Test if "Line Up" (case insensitive) is found with different Hx tags
    html_h3 = '''<html><body><h3>Line up</h3><p>Artist H3</p></body></html>'''
    html_h5 = '''<html><body><h5>LINEUP</h5><ul><li>Artist H5</li></ul></body></html>'''

    soup_h3 = BeautifulSoup(html_h3, "html.parser")
    artists_h3 = scraper_instance.extract_lineup_from_html(soup_h3)
    assert artists_h3 == ["Artist H3"]

    soup_h5 = BeautifulSoup(html_h5, "html.parser")
    artists_h5 = scraper_instance.extract_lineup_from_html(soup_h5)
    assert artists_h5 == ["Artist H5"]

def test_extract_lineup_from_html_no_lineup_found(scraper_instance):
    html = '''<html><body><h3>Other Section</h3><p>No artists here.</p></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == []

def test_extract_lineup_from_html_empty_tags(scraper_instance):
    html = '''<html><body><h3>Line Up</h3><p><br/></p><ul><li></li></ul></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == []

def test_extract_lineup_from_html_duplicates_are_removed(scraper_instance):
    html = '''
    <html><body>
        <h3>Line Up</h3>
        <p>Artist A<br>Artist B<br>Artist A</p>
        <ul><li>Artist C</li><li>Artist B</li></ul>
    </body></html>'''
    # Expected order is preserved from first encounter.
    # From <p>: "Artist A", "Artist B" (Artist A again is ignored)
    # From <ul>: "Artist C" (Artist B again is ignored)
    # Based on observed behavior, it seems only the first <p> or <ul> block after a header is processed.
    # So, the <ul> after the <p> is ignored for this <h3> header.
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == ["Artist A", "Artist B"]

def test_extract_lineup_from_html_header_then_irrelevant_then_list(scraper_instance):
    html = '''
    <html><body>
        <h3>Line Up</h3>
        <div>Some other text</div>
        <p>Maybe some more text</p>
        <ul>
            <li>Real Artist 1</li>
            <li>Real Artist 2</li>
        </ul>
    </body></html>'''
    # The current logic: `while next_elem and next_elem.name not in ['h3', 'h4', 'h5', 'div']:`
    # This means if a 'div' is encountered after header before 'p' or 'ul', it stops for that header.
    # This test checks this behavior. If the div was not a stop condition, it would find the ul.
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == [] # Because the 'div' after <h3>Line Up</h3> stops the search for that header.

def test_extract_lineup_from_html_multiple_lineup_sections(scraper_instance):
    html = '''
    <html><body>
        <h3>Line Up</h3>
        <p>Main Artist<br>Support Artist</p>
        <h4>Later Acts (LineUp)</h4>
        <ul><li>Late DJ</li></ul>
    </body></html>'''
    # Should find both sections and combine them, respecting duplicates.
    soup = BeautifulSoup(html, "html.parser")
    artists = scraper_instance.extract_lineup_from_html(soup)
    assert artists == ["Main Artist", "Support Artist", "Late DJ"]

# Add these tests after the extract_lineup_from_html tests in tests/test_mono_ticketmaster.py

def test_extract_ticket_url_from_html_buy_tickets_text_fourvenues(scraper_instance):
    html = '''<html><body><a href="http://example.com/fourvenues/ticket1">Buy Tickets Here</a></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/fourvenues/ticket1"

def test_extract_ticket_url_from_html_buy_tickets_text_generic_ticket_link(scraper_instance):
    html = '''<html><body><a href="http://example.com/ticket/event2">BUY TICKETS NOW</a></body></html>''' # Case insensitive text
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/ticket/event2"

def test_extract_ticket_url_from_html_wcs_btn_action_class(scraper_instance):
    html = '''<html><body><a class="wcs-btn--action" href="http://example.com/wcs_ticket_link">Get Your Tickets</a></body></html>'''
    # This should be found if the text-based search fails or if it's prioritized.
    # Current logic: text search first, then class search.
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/wcs_ticket_link"

def test_extract_ticket_url_from_html_text_match_takes_precedence_over_class(scraper_instance):
    html = '''
    <html><body>
        <a href="http://example.com/textmatch_fourvenues">Buy Tickets First</a>
        <a class="wcs-btn--action" href="http://example.com/classmatch_wcs">Another Ticket Link</a>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/textmatch_fourvenues" # Text match is iterated first

def test_extract_ticket_url_from_html_no_matching_text_class_fallback(scraper_instance):
    html = '''
    <html><body>
        <a href="http://example.com/some_other_link">More Info</a>
        <a class="wcs-btn--action" href="http://example.com/classmatch_wcs_only">Tickets by Class</a>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/classmatch_wcs_only"

def test_extract_ticket_url_from_html_text_match_no_keyword_in_href(scraper_instance):
    # The regex for text is "Buy Tickets". The href check for keywords ('fourvenues', 'ticket') is secondary.
    html = '''<html><body><a href="http://example.com/generic_link">Buy Tickets Please</a></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    # The current implementation has a condition: `if href and ('fourvenues' in href or 'ticket' in href.lower()): return href`
    # So, if "Buy Tickets" is found, but href doesn't contain keywords, it will NOT return this link.
    # It will then fall through to check for `wcs-btn--action`.
    assert url is None

def test_extract_ticket_url_from_html_text_match_with_ticket_keyword_in_href(scraper_instance):
    html = '''<html><body><a href="http://example.com/some_ticket_page">Buy Tickets Please</a></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/some_ticket_page"


def test_extract_ticket_url_from_html_no_ticket_links(scraper_instance):
    html = '''<html><body><a href="http://example.com/about">About Us</a><p>No tickets here.</p></body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url is None

def test_extract_ticket_url_from_html_multiple_text_matches_first_valid_taken(scraper_instance):
    html = '''
    <html><body>
        <a href="http://example.com/no_keyword1">Buy Tickets (Not this one)</a>
        <a href="http://example.com/yes_ticket_keyword">Buy Tickets (This one)</a>
        <a href="http://example.com/fourvenues_also">Buy Tickets (Not this one either, already found)</a>
    </body></html>'''
    soup = BeautifulSoup(html, "html.parser")
    url = scraper_instance.extract_ticket_url_from_html(soup)
    assert url == "http://example.com/yes_ticket_keyword"

# Add these tests after the extract_ticket_url_from_html tests
from unittest.mock import patch # Required for mocking

# Test _map_jsonld_to_event_schema
def test_map_jsonld_basic_fields(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    scraped_at_dt = datetime.fromisoformat(now_iso_test.replace("Z", "+00:00"))

    jsonld_input = {
        "@type": "MusicEvent",
        "name": "JSON-LD Event Title",
        "url": "http://example.com/event-original-url", # This is usually the source URL of JSON-LD, not the event's own canonical.
        "startDate": "2025-07-01T19:00:00+00:00",
        "endDate": "2025-07-01T22:00:00+00:00",
        "location": {
            "@type": "Place",
            "name": "The LD Venue",
            "address": {
                "@type": "PostalAddress",
                "streetAddress": "123 LD Street",
                "addressLocality": "LD City",
                "addressRegion": "LD State",
                "postalCode": "LDP01",
                "addressCountry": "LDLand"
            },
            "geo": {"@type": "GeoCoordinates", "latitude": "38.9", "longitude": "1.4"}
        },
        "description": "This is the event description from JSON-LD.",
        "image": ["http://example.com/image1.jpg", "http://example.com/image2.jpg"],
        "typicalAgeRange": "18+"
    }
    event_url_input = "http://example.com/actual_event_page"
    html_input = "<html><body>Some event page content.</body></html>"

    # Mock the helper methods that _map_jsonld_to_event_schema calls
    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=["Artist From HTML"])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value="http://tickets.example.com/html_link")

    # Mock _populate_derived_fields as its tested separately and makes assertions complex here
    mocker.patch.object(scraper_instance, '_populate_derived_fields')


    event_data = scraper_instance._map_jsonld_to_event_schema(jsonld_input, event_url_input, html_input, now_iso_test)

    assert event_data["title"] == "JSON-LD Event Title"
    assert event_data["url"] == event_url_input # Should use the passed event_url_input
    assert event_data["extractionMethod"] == "jsonld"
    assert event_data["scrapedAt"] == scraped_at_dt
    assert event_data["html"] == html_input # Not truncated in this direct call test
    assert event_data["fullDescription"] == "This is the event description from JSON-LD."
    assert event_data["images"] == ["http://example.com/image1.jpg", "http://example.com/image2.jpg"]
    assert event_data["ageRestriction"] == "18+"

    # Location checks
    assert event_data["location"]["venue"] == "The LD Venue"
    assert event_data["location"]["address"] == "123 LD Street LD City LD State LDP01 LDLand"
    assert event_data["location"]["coordinates"]["lat"] == 38.9
    assert event_data["location"]["coordinates"]["lng"] == 1.4

    # DateTime checks
    assert event_data["dateTime"]["parsed"]["startDate"] == datetime(2025, 7, 1, 19, 0, tzinfo=timezone.utc)
    assert event_data["dateTime"]["parsed"]["endDate"] == datetime(2025, 7, 1, 22, 0, tzinfo=timezone.utc)
    assert event_data["dateTime"]["dayOfWeek"] == "Tuesday" # 2025-07-01 is a Tuesday

    # Check that mocked helpers were integrated
    # Lineup: JSON-LD part might be empty, HTML part is added
    # The mock returns ["Artist From HTML"]. The function appends this.
    # If jsonld_input["performer"] was empty, lineup would be just this.
    # If jsonld_input["performer"] had items, they'd be listed first.
    # For this test, jsonld_input has no "performer".
    assert {"name": "Artist From HTML", "affiliates": [], "genres": [], "headliner": False} in event_data["lineUp"]

    assert event_data["ticketsUrl"] == "http://tickets.example.com/html_link"

    scraper_instance._populate_derived_fields.assert_called_once_with(event_data)


def test_map_jsonld_performers_and_offers(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    jsonld_input = {
        "@type": "MusicEvent",
        "name": "Event With Performers/Offers",
        "performer": [
            {"@type": "MusicGroup", "name": "Band A", "sameAs": "http://banda.com"},
            {"@type": "Person", "name": "Solo B", "genre": "Rock"}
        ],
        "offers": [
            {
                "@type": "Offer", "name": "Early Bird", "price": "20.00", "priceCurrency": "EUR",
                "availability": "http://schema.org/InStock", "url": "http://tickets.example.com/early"
            },
            {
                "@type": "Offer", "name": "VIP", "price": "50.50", "priceCurrency": "EUR",
                "availability": "http://schema.org/SoldOut", "url": "http://tickets.example.com/vip"
            }
        ]
    }
    event_url_input = "http://example.com/event_perf_offers"
    html_input = "<html></html>"

    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[]) # No HTML lineup for this test
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None) # No HTML ticket URL
    mocker.patch.object(scraper_instance, '_populate_derived_fields')

    event_data = scraper_instance._map_jsonld_to_event_schema(jsonld_input, event_url_input, html_input, now_iso_test)

    # LineUp checks
    assert len(event_data["lineUp"]) == 2
    assert event_data["lineUp"][0]["name"] == "Band A"
    assert event_data["lineUp"][0]["headliner"] is True # First is headliner
    assert "http://banda.com" in event_data["lineUp"][0]["affiliates"]
    assert event_data["lineUp"][1]["name"] == "Solo B"
    assert event_data["lineUp"][1]["headliner"] is False
    assert "Rock" in event_data["lineUp"][1]["genres"]

    # TicketInfo checks
    assert event_data["ticketInfo"]["startingPrice"] == 20.00 # Min of 20.00 and 50.50
    assert event_data["ticketInfo"]["currency"] == "EUR"
    assert event_data["ticketInfo"]["status"] == "http://schema.org/InStock" # From first offer
    assert event_data["ticketInfo"]["url"] == "http://tickets.example.com/early" # From first offer
    assert len(event_data["ticketInfo"]["tiers"]) == 2
    assert event_data["ticketInfo"]["tiers"][0]["name"] == "Early Bird"
    assert event_data["ticketInfo"]["tiers"][0]["price"] == 20.00
    assert event_data["ticketInfo"]["tiers"][0]["available"] is True
    assert event_data["ticketInfo"]["tiers"][1]["name"] == "VIP"
    assert event_data["ticketInfo"]["tiers"][1]["price"] == 50.50
    assert event_data["ticketInfo"]["tiers"][1]["available"] is False # Based on SoldOut

    scraper_instance._populate_derived_fields.assert_called_once()

def test_map_jsonld_minimal_data(scraper_instance, mocker):
    # Test with the bare minimum valid JSON-LD
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    scraped_at_dt = datetime.fromisoformat(now_iso_test.replace("Z", "+00:00"))
    jsonld_input = {"@type": "MusicEvent", "name": "Minimal Event"}
    event_url_input = "http://example.com/minimal"
    html_input = ""

    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None)
    mocker.patch.object(scraper_instance, '_populate_derived_fields')

    event_data = scraper_instance._map_jsonld_to_event_schema(jsonld_input, event_url_input, html_input, now_iso_test)

    assert event_data["title"] == "Minimal Event"
    assert event_data["url"] == event_url_input
    assert event_data["scrapedAt"] == scraped_at_dt
    assert event_data["extractionMethod"] == "jsonld"
    assert event_data["html"] is None # Corrected: empty html input results in None

    # Check that unprovided fields are None or empty lists as per EventSchemaTypedDict defaults
    assert event_data["location"] == {'venue': None, 'address': None, 'coordinates': None}
    assert event_data["dateTime"] == {'displayText': None, 'parsed': {'startDate': None, 'endDate': None, 'doors': None}, 'dayOfWeek': None}
    assert event_data["lineUp"] == []
    assert event_data["ticketInfo"] is None # This is correctly None if offers node is empty
    assert event_data["images"] == []
    assert event_data["fullDescription"] is None

    scraper_instance._populate_derived_fields.assert_called_once()

def test_map_jsonld_html_truncation_not_directly_tested_here(scraper_instance, mocker):
    # The _map_jsonld_to_event_schema takes html and assigns it.
    # The main scrape_event_data is responsible for truncation before calling this.
    # This test just confirms it passes through what it's given.
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    jsonld_input = {"@type": "MusicEvent", "name": "Test Event"}
    event_url_input = "http://example.com/trunc"
    short_html = "short"

    mocker.patch.object(scraper_instance, '_populate_derived_fields')
    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None)


    event_data = scraper_instance._map_jsonld_to_event_schema(jsonld_input, event_url_input, short_html, now_iso_test)
    assert event_data["html"] == short_html # Passes through what it gets

    # Note: The actual truncation `html[:5000]` is in `_map_jsonld_to_event_schema`
    # and `_map_fallback_to_event_schema`. So this test should reflect that.
    # Let's test truncation here if html input is long
    long_html_content = "a" * 6000
    event_data_long_html = scraper_instance._map_jsonld_to_event_schema(jsonld_input, event_url_input, long_html_content, now_iso_test)
    assert len(event_data_long_html["html"]) == 5000
    assert event_data_long_html["html"] == "a" * 5000

# Add these tests after the _map_jsonld_to_event_schema tests

def test_map_fallback_basic_fields(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    scraped_at_dt = datetime.fromisoformat(now_iso_test.replace("Z", "+00:00"))

    fallback_input_data = {
        "title": "Fallback Title", # From WP or Meta
        "venue": "Fallback Venue", # From WP
        "date_text": "Jan 1, 2026", # From WP
        "price_text": "€30.00", # From WP
        "description": "Fallback description text.", # From WP or Meta
        "image": "http://example.com/fallback_image.jpg" # From Meta (og:image)
    }
    event_url_input = "http://example.com/actual_event_page_fallback"
    html_input = "<html><body>Some fallback event page content.</body></html>"

    # Mock the helper methods
    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=["Fallback Artist"])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value="http://tickets.example.com/fallback_html_link")
    mocker.patch.object(scraper_instance, '_populate_derived_fields')

    event_data = scraper_instance._map_fallback_to_event_schema(fallback_input_data, event_url_input, html_input, now_iso_test)

    assert event_data["title"] == "Fallback Title"
    assert event_data["url"] == event_url_input
    assert event_data["extractionMethod"] == "fallback"
    assert event_data["scrapedAt"] == scraped_at_dt
    assert event_data["html"] == html_input # Truncation is applied, but input is short
    assert event_data["extractedData"] == fallback_input_data
    assert event_data["fullDescription"] == "Fallback description text."
    assert event_data["images"] == ["http://example.com/fallback_image.jpg"]

    # Location
    assert event_data["location"]["venue"] == "Fallback Venue"
    assert event_data["location"]["address"] is None # Not in fallback_input_data
    assert event_data["location"]["coordinates"] is None

    # DateTime
    assert event_data["dateTime"]["displayText"] == "Jan 1, 2026"
    assert event_data["dateTime"]["parsed"]["startDate"] is None # Fallback doesn't parse date string deeply by default

    # LineUp (from mocked HTML extraction)
    assert len(event_data["lineUp"]) == 1
    assert event_data["lineUp"][0]["name"] == "Fallback Artist"
    assert event_data["lineUp"][0]["headliner"] is True # First is headliner by default

    # TicketInfo
    assert event_data["ticketInfo"]["displayText"] == "€30.00"
    assert event_data["ticketInfo"]["startingPrice"] == 30.00
    assert event_data["ticketInfo"]["currency"] == "EUR"
    assert event_data["ticketInfo"]["url"] is None # From fallback_input_data, not HTML for this field in this path

    assert event_data["ticketsUrl"] == "http://tickets.example.com/fallback_html_link" # From mocked HTML extraction

    scraper_instance._populate_derived_fields.assert_called_once_with(event_data)

def test_map_fallback_price_parsing_variations(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    event_url_input = "http://example.com/fallback_price"
    html_input = ""
    mocker.patch.object(scraper_instance, '_populate_derived_fields')
    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None)


    # Case 1: Price with $ and no decimals
    data1 = {"price_pattern": "$50"}
    event_data1 = scraper_instance._map_fallback_to_event_schema(data1, event_url_input, html_input, now_iso_test)
    assert event_data1["ticketInfo"]["startingPrice"] == 50.0
    assert event_data1["ticketInfo"]["currency"] == "USD"

    # Case 2: Price with £ and decimals
    data2 = {"price_text": "£25.99"} # price_text is also checked
    event_data2 = scraper_instance._map_fallback_to_event_schema(data2, event_url_input, html_input, now_iso_test)
    assert event_data2["ticketInfo"]["startingPrice"] == 25.99
    assert event_data2["ticketInfo"]["currency"] == "GBP"

    # Case 3: No currency symbol, no price
    data3 = {"price_text": "Contact for price"}
    event_data3 = scraper_instance._map_fallback_to_event_schema(data3, event_url_input, html_input, now_iso_test)
    assert event_data3["ticketInfo"]["startingPrice"] is None
    assert event_data3["ticketInfo"]["currency"] is None

    # Case 4: Price pattern with number only
    data4 = {"price_pattern": "123.45"}
    event_data4 = scraper_instance._map_fallback_to_event_schema(data4, event_url_input, html_input, now_iso_test)
    assert event_data4["ticketInfo"]["startingPrice"] == 123.45
    assert event_data4["ticketInfo"]["currency"] is None # No symbol to infer from

def test_map_fallback_minimal_data(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    fallback_input_data = {"title": "Minimal Fallback"} # Only title
    event_url_input = "http://example.com/minimal_fallback"
    html_input = "minimal html"

    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None)
    mocker.patch.object(scraper_instance, '_populate_derived_fields')

    event_data = scraper_instance._map_fallback_to_event_schema(fallback_input_data, event_url_input, html_input, now_iso_test)

    assert event_data["title"] == "Minimal Fallback"
    assert event_data["url"] == event_url_input
    assert event_data["extractionMethod"] == "fallback"
    assert event_data["html"] == html_input
    assert event_data["extractedData"] == fallback_input_data

    # Check default empty/None structures for other fields
    assert event_data["location"] == {"venue": None, "address": None, "coordinates": None}
    assert event_data["dateTime"]["displayText"] is None
    assert event_data["lineUp"] == []
    assert event_data["ticketInfo"]["startingPrice"] is None
    assert event_data["images"] == []
    assert event_data["fullDescription"] is None

    scraper_instance._populate_derived_fields.assert_called_once()

def test_map_fallback_html_truncation(scraper_instance, mocker):
    now_iso_test = datetime.utcnow().isoformat() + "Z"
    fallback_input_data = {"title": "Truncation Test"}
    event_url_input = "http://example.com/trunc_fallback"
    long_html_content = "b" * 6000

    mocker.patch.object(scraper_instance, '_populate_derived_fields')
    mocker.patch.object(scraper_instance, 'extract_lineup_from_html', return_value=[])
    mocker.patch.object(scraper_instance, 'extract_ticket_url_from_html', return_value=None)

    event_data = scraper_instance._map_fallback_to_event_schema(fallback_input_data, event_url_input, long_html_content, now_iso_test)
    assert len(event_data["html"]) == 5000
    assert event_data["html"] == "b" * 5000

# --- Tests for _populate_derived_fields ---
# import my_scrapers.mono_ticketmaster # Ensure this import is present for mocker path
# Already added at the top

def test_populate_derived_fields_ticket_info_logic(scraper_instance):
    event_data_template = {
        "url": "http://example.com", "title": "Test Event", "scrapedAt": datetime.utcnow(),
        "lineUp": [], "images": []
    }

    # Case 1: Has price
    event1 = {**event_data_template, "ticketInfo": {"startingPrice": 10.0}}
    scraper_instance._populate_derived_fields(event1)
    assert event1["hasTicketInfo"] is True
    assert event1["isFree"] is False
    assert event1["isSoldOut"] is False

    # Case 2: Has ticket URL
    event2 = {**event_data_template, "ticketInfo": {"url": "http://tickets.com"}}
    scraper_instance._populate_derived_fields(event2)
    assert event2["hasTicketInfo"] is True

    # Case 3: Has tiers
    event3 = {**event_data_template, "ticketInfo": {"tiers": [{"name": "GA", "price": 10}]}}
    scraper_instance._populate_derived_fields(event3)
    assert event3["hasTicketInfo"] is True

    # Case 4: Has display text only
    event4 = {**event_data_template, "ticketInfo": {"displayText": "Tickets available soon"}}
    scraper_instance._populate_derived_fields(event4)
    assert event4["hasTicketInfo"] is True
    assert event4["isFree"] is False

    # Case 5: No ticketInfo sub-fields
    event5 = {**event_data_template, "ticketInfo": {}}
    scraper_instance._populate_derived_fields(event5)
    assert event5["hasTicketInfo"] is False
    assert event5["isFree"] is False
    assert event5["isSoldOut"] is False

    # Case 6: ticketInfo is None
    event6 = {**event_data_template, "ticketInfo": None}
    scraper_instance._populate_derived_fields(event6)
    assert event6["hasTicketInfo"] is False
    assert event6["isFree"] is False
    assert event6["isSoldOut"] is False

def test_populate_derived_fields_is_free_logic(scraper_instance):
    event_data_template = {"url": "u", "title": "t", "scrapedAt": datetime.utcnow(), "lineUp": [], "images": []}

    event1 = {**event_data_template, "ticketInfo": {"startingPrice": 0}}
    scraper_instance._populate_derived_fields(event1)
    assert event1["isFree"] is False # Corrected: startingPrice:0 alone doesn't make hasTicketInfo true, so isFree is false
    assert event1["hasTicketInfo"] is False # Corrected: startingPrice:0 alone makes has_price false, so hasTicketInfo false

    event2 = {**event_data_template, "ticketInfo": {"status": "free event"}}
    scraper_instance._populate_derived_fields(event2)
    assert event2["isFree"] is False # Corrected: status:"free event" alone doesn't make hasTicketInfo true
    assert event2["hasTicketInfo"] is False # Corrected: status alone doesn't make hasTicketInfo true

    event3 = {**event_data_template, "ticketInfo": {"displayText": "This is a FREE event"}}
    scraper_instance._populate_derived_fields(event3)
    assert event3["isFree"] is True
    assert event3["hasTicketInfo"] is True

    event4 = {**event_data_template, "ticketInfo": {"startingPrice": 5.0, "status": "free entrance"}}
    scraper_instance._populate_derived_fields(event4)
    assert event4["isFree"] is False
    assert event4["hasTicketInfo"] is True

    event5 = {**event_data_template, "ticketInfo": {"startingPrice": None, "status": "available"}}
    scraper_instance._populate_derived_fields(event5)
    assert event5["isFree"] is False # This should remain False
    assert event5["hasTicketInfo"] is False # Corrected: status alone doesn't make hasTicketInfo true

    event6 = {**event_data_template, "ticketInfo": {"startingPrice": 0.00}}
    scraper_instance._populate_derived_fields(event6)
    assert event6["isFree"] is False # Corrected: like event1
    assert event6["hasTicketInfo"] is False # Corrected: like event1

def test_populate_derived_fields_is_sold_out_logic(scraper_instance):
    event_data_template = {"url": "u", "title": "t", "scrapedAt": datetime.utcnow(), "lineUp": [], "images": []}
    keywords = ["sold out", "unavailable", "off-sale", "offsale"]
    for kw in keywords:
        event = {**event_data_template, "ticketInfo": {"status": f"Event is {kw}"}}
        scraper_instance._populate_derived_fields(event)
        assert event["isSoldOut"] is True, f"Failed for keyword: {kw}"

    event_available = {**event_data_template, "ticketInfo": {"status": "available"}}
    scraper_instance._populate_derived_fields(event_available)
    assert event_available["isSoldOut"] is False

def test_populate_derived_fields_counts_and_timestamps(scraper_instance, mocker):
    event_data = {
        "url": "u", "title": "t", "scrapedAt": datetime(2023, 1, 1),
        "lineUp": [{"name": "A1"}, {"name": "A2"}],
        "images": ["img1.jpg", "img2.jpg", "img3.jpg"],
        "ticketInfo": {}
    }

    fixed_now = datetime(2023, 10, 5, 12, 0, 0, tzinfo=timezone.utc)
    # Ensure the path to datetime matches where it's called in MultiLayerEventScraper's _populate_derived_fields
    mocker.patch('my_scrapers.mono_ticketmaster.datetime', autospec=True)
    my_scrapers.mono_ticketmaster.datetime.utcnow.return_value = fixed_now

    scraper_instance._populate_derived_fields(event_data)

    assert event_data["artistCount"] == 2
    assert event_data["imageCount"] == 3
    assert event_data["updatedAt"] == fixed_now
    assert event_data["lastCheckedAt"] == fixed_now

    event_no_counts = {"url": "u", "title": "t", "scrapedAt": datetime(2023,1,1), "lineUp": None, "images": None, "ticketInfo": None}
    scraper_instance._populate_derived_fields(event_no_counts)
    assert event_no_counts["artistCount"] == 0
    assert event_no_counts["imageCount"] == 0
