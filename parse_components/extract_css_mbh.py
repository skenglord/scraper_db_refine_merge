from bs4 import BeautifulSoup
from typing import List, Dict

def extract_css_mbh(html: str, selectors: List[str]) -> Dict[str, List[str]]:
    """
    Extracts text content from HTML using a list of CSS selectors.
    Adapted from mono_basic_html.BasicHTMLScraper.extract_css.

    Args:
        html: The HTML content as a string.
        selectors: A list of CSS selector strings.

    Returns:
        A dictionary where keys are the CSS selectors and values are lists of
        extracted text strings for each element found by that selector.
    """
    soup = BeautifulSoup(html, "html.parser")
    results: Dict[str, List[str]] = {}
    for sel in selectors:
        elements = soup.select(sel)
        results[sel] = [el.get_text(strip=True) for el in elements]
    return results
