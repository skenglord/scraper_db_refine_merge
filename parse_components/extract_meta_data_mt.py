from bs4 import BeautifulSoup
from typing import Dict

def extract_meta_data_mt(soup: BeautifulSoup) -> Dict[str, str]:
    """
    Extract Open Graph and other meta tag data from a BeautifulSoup object.
    Adapted from mono_ticketmaster.MultiLayerEventScraper.extract_meta_data.
    """
    data: Dict[str, str] = {}

    og_mappings = {
        "og:title": "title",
        "og:description": "description",
        "og:image": "image",
        "og:url": "canonical_url",
        "og:type": "type",
        "og:site_name": "site_name",
        # Add more Open Graph properties as needed
        "og:locale": "locale",
        "article:published_time": "article_published_time",
        "article:modified_time": "article_modified_time",
    }
    for og_prop, key in og_mappings.items():
        meta_tag = soup.find("meta", property=og_prop)
        if meta_tag and meta_tag.get("content"):
            data[key] = meta_tag["content"]

    meta_mappings = {
        "description": "meta_description", # Already have og:description, but this can be a fallback
        "keywords": "keywords",
        "author": "author",
        "viewport": "viewport",
        "robots": "robots",
        # Add more standard meta tags as needed
        "twitter:card": "twitter_card",
        "twitter:site": "twitter_site",
        "twitter:title": "twitter_title",
        "twitter:description": "twitter_description",
        "twitter:image": "twitter_image",
    }
    for name, key in meta_mappings.items():
        meta_tag = soup.find("meta", attrs={"name": name})
        if meta_tag and meta_tag.get("content"):
            data[key] = meta_tag["content"]

    # Extract title tag as a fallback if no og:title or twitter:title
    if "title" not in data and "twitter_title" not in data:
        title_tag = soup.find("title")
        if title_tag and title_tag.string:
            data["html_title"] = title_tag.string.strip()

    # Extract canonical link tag
    if "canonical_url" not in data: # Only if not found via og:url
        canonical_link_tag = soup.find("link", rel="canonical")
        if canonical_link_tag and canonical_link_tag.get("href"):
            data["canonical_url_link_tag"] = canonical_link_tag["href"]

    return data
