import os
from classy_clubtickets_nav_scraper import ClubTicketsScraper
from urllib.parse import urlparse
import markdownify

def extract_event_card_info(page):
    """
    Extract CSS selectors, XPath, and URL of the first event card
    """
    event_card_selector = ".content-text-card"
    event_cards = page.query_selector_all(event_card_selector)
    if not event_cards:
        return None, None, None
    first_card = event_cards[0]
    # Extract URL from first link inside card
    link = first_card.query_selector("a")
    url = link.get_attribute("href") if link else None
    if url:
        url = page.urljoin(url)
    # CSS selector is known
    css_selector = event_card_selector
    # XPath can be constructed or approximated
    xpath = page.evaluate("""
        (element) => {
            function getXPath(el) {
                if (el.id !== '') {
                    return 'id(\"' + el.id + '\")';
                }
                if (el === document.body) {
                    return el.tagName.toLowerCase();
                }
                var ix= 0;
                var siblings= el.parentNode.childNodes;
                for (var i= 0; i < siblings.length; i++) {
                    var sibling= siblings[i];
                    if (sibling===el) {
                        return getXPath(el.parentNode)+'/'+el.tagName.toLowerCase()+'['+(ix+1)+']';
                    }
                    if (sibling.nodeType===1 && sibling.tagName===el.tagName) {
                        ix++;
                    }
                }
            }
            return getXPath(element);
        }
    """, first_card)
    return css_selector, xpath, url

def save_markdown(filename, content):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

def main():
    url = "https://www.clubtickets.com/search?dates=31%2F05%2F25+-+01%2F11%2F25"
    output_file = "clubtickets_fetch_data.md"
    with ClubTicketsScraper(headless=False) as scraper:
        scraper.navigate_to(url)
        scraper.handle_cookie_popup()
        # Scroll down 3 times as per task
        for _ in range(3):
            scraper.page.evaluate("window.scrollBy(0, window.innerHeight / 3)")
            scraper.random_delay()
        # Click "Show more events" button if present
        show_more_button = scraper.page.locator("button.btn-more-events.more-events")
        if show_more_button.is_visible(timeout=5000):
            show_more_button.click()
            scraper.random_delay()
        # Extract first event card info
        css_selector, xpath, event_url = extract_event_card_info(scraper.page)
        # Extract all event URLs
        event_urls = scraper.process_current_events()
        # Fetch markdown content for each event URL using scraper.page.goto and page content
        markdown_contents = []
        for i, e_url in enumerate(event_urls):
            scraper.page.goto(e_url)
            scraper.random_delay(short=False)
            html_content = scraper.page.content()
            md_content = markdownify.markdownify(html_content, heading_style="ATX")
            markdown_contents.append(f"# Event {i+1}\nURL: {e_url}\n\n{md_content}\n\n---\n")
        # Save all markdown content to file
        save_markdown(output_file, "\n".join(markdown_contents))
    print(f"Saved markdown data for {len(event_urls)} events to {output_file}")
    print(f"First event card CSS selector: {css_selector}")
    print(f"First event card XPath: {xpath}")
    print(f"First event card URL: {event_url}")

if __name__ == "__main__":
    main()
