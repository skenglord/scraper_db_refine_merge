# scrapers_v2/scrapers/ticketmaster/ticketmaster_scraper.py
import asyncio
import logging
import json
import re
import yaml
import pathlib
from bs4 import BeautifulSoup
from datetime import datetime, timezone # Added timezone
from urllib.parse import urljoin
from dateutil import parser as date_parser
from typing import Any, Dict, List, Optional, Union, Tuple # Added Tuple

# Import Pydantic model and error class
from pydantic import ValidationError
from .ticketmaster_datamodels import TicketmasterEventModel # Assuming it's in the same directory

# --- Placeholder Framework Components (Simulating scrapers_v2) ---
class PlaceholderSettings:
    def __init__(self):
        self.DEFAULT_USER_AGENT = "ScrapersV2 Global Default User Agent/1.0"
        self.LOG_LEVEL = "DEBUG"

class PlaceholderHttpClient:
    def __init__(self, user_agent: str, scraper_instance: Optional['TicketmasterScraper'] = None):
        self.user_agent = user_agent
        self.scraper_instance = scraper_instance
        self._mock_html_content: str = "<html><body>Default HTTP Mock HTML</body></html>"

    def set_mock_html(self, html_content: str):
        self._mock_html_content = html_content
        logger.debug(f"PlaceholderHttpClient: Mock HTML set to: {html_content[:100]}...")

    async def get(self, url: str) -> str:
        logger.debug(f"PlaceholderHttpClient: GET {url} with User-Agent: {self.user_agent}")
        if self.scraper_instance:
            self.scraper_instance.current_page_url = url
        await asyncio.sleep(0.001)
        logger.debug(f"PlaceholderHttpClient: Returning mock HTML: {self._mock_html_content[:100]}...")
        return self._mock_html_content

class PlaceholderPlaywrightClient(PlaceholderHttpClient):
    def __init__(self, user_agent: str, headless: bool = True, scraper_instance: Optional['TicketmasterScraper'] = None):
        super().__init__(user_agent, scraper_instance)
        self.headless = headless
        self.scraper_config_for_click: Dict[str, Any] = {}
        self._initial_page_html: str = "<html><body>Default Playwright Initial Page Mock HTML</body></html>"
        self._load_more_html_pages: List[str] = []
        self._current_click_index: int = 0
        self._current_page_content_str: str = self._initial_page_html

    def set_mock_html_initial_page(self, html_content: str):
        self._initial_page_html = html_content
        self._current_page_content_str = html_content
        logger.debug(f"PlaceholderPlaywrightClient: Initial page HTML set: {html_content[:100]}...")

    def set_mock_html_load_more_pages(self, html_pages: List[str]):
        self._load_more_html_pages = html_pages
        self._current_click_index = 0
        logger.debug(f"PlaceholderPlaywrightClient: Load more pages HTML set (count: {len(html_pages)}).")

    async def get(self, url: str) -> str:
        logger.debug(f"PlaceholderPlaywrightClient: Initial GET for {url} (headless: {self.headless})")
        if self.scraper_instance:
            self.scraper_instance.current_page_url = url
        await asyncio.sleep(0.001)
        self._current_click_index = 0
        self._current_page_content_str = self._initial_page_html
        logger.debug(f"PlaceholderPlaywrightClient: Returning initial page mock HTML: {self._current_page_content_str[:100]}...")
        return self._current_page_content_str

    async def click_if_present(self, selector: str) -> bool:
        logger.debug(f"PlaceholderPlaywrightClient: Attempting to click '{selector}'")
        cookie_selectors = self.scraper_config_for_click.get('selectors', {}).get('cookie_popup_selectors', [])
        if selector in cookie_selectors:
             logger.info(f"PlaceholderPlaywrightClient: Clicked cookie button '{selector}'.")
             return True
        if selector == self.scraper_config_for_click.get('selectors', {}).get('load_more_button'):
            if self._current_click_index < len(self._load_more_html_pages):
                logger.info(f"PlaceholderPlaywrightClient: Clicked 'Load More' (click #{self._current_click_index + 1}).")
                new_content = self._load_more_html_pages[self._current_click_index]
                self._current_page_content_str += new_content
                self._current_click_index += 1
                await asyncio.sleep(0.001)
                logger.debug(f"PlaceholderPlaywrightClient: Updated page content after 'Load More': {new_content[:100]}...")
                return True
            else:
                logger.info("PlaceholderPlaywrightClient: 'Load More' clicked, but no more mock pages available.")
                return False
        logger.info(f"PlaceholderPlaywrightClient: Selector '{selector}' not a cookie or load_more, or no action defined. Not clicked.")
        return False

    def set_scraper_config_for_click(self, config: Dict[str, Any]):
        self.scraper_config_for_click = config

    async def get_page_content(self) -> str:
        logger.debug("PlaceholderPlaywrightClient: Getting current page content string.")
        return self._current_page_content_str

    async def wait_for_network_idle(self, timeout: int = 100):
        logger.debug(f"PlaceholderPlaywrightClient: Simulating wait_for_network_idle (timeout: {timeout}ms)")
        await asyncio.sleep(timeout / 1000.0)

logger = logging.getLogger("scrapers_v2.ticketmaster")

class TicketmasterScraper:
    DEFAULT_CONFIG_PATH = pathlib.Path(__file__).parent / "ticketmaster_config.yaml"

    def __init__(self, settings: PlaceholderSettings, config_path: Optional[pathlib.Path] = None):
        self.settings = settings
        self.config_path = config_path or self.DEFAULT_CONFIG_PATH
        self.scraper_config = self._load_specific_config()
        self.current_page_url: Optional[str] = None
        self._init_client()

    def _init_client(self):
        playwright_cfg = self.scraper_config.get('playwright_settings', {})
        if playwright_cfg.get('enabled', False):
            logger.info("Playwright is enabled. Initializing PlaceholderPlaywrightClient.")
            self.client: Union[PlaceholderHttpClient, PlaceholderPlaywrightClient] = PlaceholderPlaywrightClient(
                user_agent=playwright_cfg.get('user_agent', self.settings.DEFAULT_USER_AGENT),
                headless=playwright_cfg.get('headless', True),
                scraper_instance=self
            )
            if isinstance(self.client, PlaceholderPlaywrightClient):
                self.client.set_scraper_config_for_click(self.scraper_config)
        else:
            logger.info("Playwright is disabled. Initializing PlaceholderHttpClient.")
            http_user_agent = self.scraper_config.get('http_settings', {}).get('user_agent', self.settings.DEFAULT_USER_AGENT)
            self.client = PlaceholderHttpClient(user_agent=http_user_agent, scraper_instance=self)
        logger.info(f"Client initialized: {type(self.client).__name__}")

    def _load_specific_config(self) -> Dict[str, Any]:
        logger.info(f"Loading Ticketmaster-specific configuration from: {self.config_path}")
        try:
            with open(self.config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                if not isinstance(config_data, dict):
                    logger.error(f"YAML content from {self.config_path} did not parse to a dictionary.")
                    raise yaml.YAMLError("YAML content not a dictionary.")
                return config_data
        except FileNotFoundError: logger.error(f"Configuration file not found: {self.config_path}"); raise
        except yaml.YAMLError as e: logger.error(f"Error parsing YAML: {e}"); raise
        except Exception as e: logger.error(f"Unexpected error loading config: {e}", exc_info=True); raise

    async def _apply_random_delay(self, action_type: str = "request"):
        delay_config = self.scraper_config.get("scraping_settings", {}).get("delays", {})
        min_ms = delay_config.get(f"{action_type}_min_ms", 1)
        max_ms = delay_config.get(f"{action_type}_max_ms", 2)
        if action_type == "post_interaction":
             min_ms = delay_config.get("post_interaction_min_ms", min_ms)
             max_ms = delay_config.get("post_interaction_max_ms", max_ms)
        await asyncio.sleep(random.randint(min_ms, max_ms) / 1000.0)

    @staticmethod
    def _transform_text(text: Optional[str]) -> Optional[str]:
        return text.strip() if isinstance(text, str) else None

    @staticmethod
    def _transform_date_string(date_str: Optional[str]) -> Optional[datetime]: # Return datetime object
        if not isinstance(date_str, str) or date_str.lower() in ["coming soon", "tba", ""]: return None
        try:
            dt_object = date_parser.parse(date_str)
            # Pydantic will handle UTC conversion if the datetime object is naive during model validation
            # if dt_object.tzinfo is None:
            #     dt_object = dt_object.replace(tzinfo=timezone.utc)
            return dt_object
        except (ValueError, TypeError) as e: logger.warning(f"Could not parse date string '{date_str}': {e}"); return None

    @staticmethod
    def _transform_price_string(price_str: Optional[str]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
        if not isinstance(price_str, str): return None, None, price_str
        price_str_cleaned = price_str.lower().replace("from", "").strip()
        price_matches = re.findall(r"[\d\.]+", price_str_cleaned)
        min_price, currency = None, None
        if price_matches:
            try:
                min_price = float(price_matches[0])
                if "€" in price_str or "eur" in price_str_cleaned: currency = "EUR"
                elif "$" in price_str or "usd" in price_str_cleaned: currency = "USD"
                elif "£" in price_str or "gbp" in price_str_cleaned: currency = "GBP"
            except ValueError: logger.warning(f"Could not convert price '{price_matches[0]}' to float.")
        return min_price, currency, price_str

    @staticmethod
    def _transform_url(raw_url: Optional[str], base_url: Optional[str]) -> Optional[HttpUrl]:
        if isinstance(raw_url, str) and base_url:
            abs_url = urljoin(base_url, raw_url.strip())
            try: return HttpUrl(abs_url) # Validate and convert to HttpUrl
            except Exception: logger.warning(f"Generated URL '{abs_url}' is not a valid HttpUrl."); return None
        elif isinstance(raw_url, str):
             try: return HttpUrl(raw_url.strip())
             except Exception: logger.warning(f"Absolute URL '{raw_url}' is not a valid HttpUrl."); return None
        return None

    def _transform_event_data(self, raw_data: Dict[str, Any], base_url: Optional[str]) -> Dict[str, Any]:
        logger.debug(f"Preparing data for Pydantic model from raw: {raw_data}")

        # Data for Pydantic model instantiation
        model_input_data: Dict[str, Any] = {}

        model_input_data["event_title"] = self._transform_text(raw_data.get("title"))
        model_input_data["event_url"] = self._transform_url(raw_data.get("url"), base_url)

        start_date_obj = self._transform_date_string(raw_data.get("date_text") or raw_data.get("startDate"))
        if start_date_obj: model_input_data["event_start_datetime"] = start_date_obj

        raw_date_text = self._transform_text(raw_data.get("date_text") or raw_data.get("startDate"))
        if raw_date_text and not start_date_obj : model_input_data["raw_date_text"] = raw_date_text


        min_price, currency, raw_price = self._transform_price_string(raw_data.get("price_text"))
        if min_price is not None: model_input_data["ticket_min_price"] = min_price
        if currency: model_input_data["ticket_currency"] = currency
        if raw_price: model_input_data["raw_price_text"] = raw_price # Keep original text

        model_input_data["venue_name"] = self._transform_text(raw_data.get("venue_name"))

        if raw_data.get("_is_json_ld", False):
            model_input_data["description"] = self._transform_text(raw_data.get("description"))

            performers_raw = raw_data.get("performer")
            lineup_data = []
            if isinstance(performers_raw, dict) and performers_raw.get("name"):
                lineup_data.append({"name": self._transform_text(performers_raw["name"])})
            elif isinstance(performers_raw, list):
                for p in performers_raw:
                    if isinstance(p, dict) and p.get("name"):
                        artist_name = self._transform_text(p["name"])
                        if artist_name: lineup_data.append({"name": artist_name})
            if lineup_data: model_input_data["lineup"] = lineup_data

            offers_raw = raw_data.get("offers", [])
            detailed_prices_data = []
            if isinstance(offers_raw, list) and offers_raw:
                for offer in offers_raw:
                    if isinstance(offer, dict) and offer.get("price") and offer.get("priceCurrency"):
                        try:
                            price_val = float(offer["price"])
                            detailed_prices_data.append({
                                "value": price_val,
                                "currency": self._transform_text(offer.get("priceCurrency")),
                                "name": self._transform_text(offer.get("name"))
                            })
                        except ValueError: logger.warning(f"Could not parse offer price {offer.get('price')} to float.")
                if detailed_prices_data:
                    model_input_data["ticket_prices_detailed"] = detailed_prices_data
                    # Override min_price/currency if JSON-LD offers are more specific
                    if (model_input_data.get("ticket_min_price") is None) and detailed_prices_data:
                        valid_prices = [p['value'] for p in detailed_prices_data if p.get('value') is not None]
                        if valid_prices:
                            model_input_data["ticket_min_price"] = min(valid_prices)
                            model_input_data["ticket_currency"] = next((p['currency'] for p in detailed_prices_data if p.get('currency')), None)

        # event_id is not set here; Pydantic model will generate it from event_url if not provided
        # Pydantic model will also add scraped_at
        return model_input_data


    def _extract_json_ld(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        logger.debug("Extracting raw data from JSON-LD scripts.")
        raw_json_ld_event_data_list: List[Dict[str, Any]] = [] # Stores data prepped for Pydantic
        json_ld_selector = self.scraper_config.get("selectors", {}).get("json_ld_script")
        if not json_ld_selector: return raw_json_ld_event_data_list

        for script_tag in soup.select(json_ld_selector):
            try:
                json_content = json.loads(script_tag.string or "")
                events_to_process: List[Dict[str, Any]] = []
                if isinstance(json_content, list): events_to_process.extend(json_content)
                elif isinstance(json_content, dict) and json_content.get("@type"): events_to_process.append(json_content)
                elif isinstance(json_content, dict) and "@graph" in json_content: events_to_process.extend(json_content["@graph"])

                for raw_event_data in events_to_process:
                    if isinstance(raw_event_data, dict) and raw_event_data.get("@type") in ["Event", "MusicEvent", "SportsEvent"]:
                        # Map JSON-LD to common keys expected by _transform_event_data then Pydantic
                        mapped_data = {
                            "title": raw_event_data.get("name"), "url": raw_event_data.get("url"),
                            "startDate": raw_event_data.get("startDate"), "description": raw_event_data.get("description"),
                            "venue_name": raw_event_data.get("location", {}).get("name"),
                            "offers": raw_event_data.get("offers"), "performer": raw_event_data.get("performer"),
                            "_is_json_ld": True # Flag for transform logic
                        }
                        # Transform step now returns dict for Pydantic, not the model instance yet
                        transformed_dict = self._transform_event_data(mapped_data, self.current_page_url)
                        if transformed_dict: # Check if transformation was successful
                             raw_json_ld_event_data_list.append(transformed_dict)
            except Exception as e: logger.error(f"Error processing JSON-LD: {e}", exc_info=True)

        logger.info(f"Prepared {len(raw_json_ld_event_data_list)} data dicts from JSON-LD for Pydantic validation.")
        return raw_json_ld_event_data_list

    def _parse_single_event_card(self, event_card_soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        raw_card_data: Dict[str, Any] = {}
        selectors = self.scraper_config.get("selectors", {})
        def get_text(key: str) -> Optional[str]:
            s = selectors.get(key)
            return event_card_soup.select_one(s).get_text(strip=True) if s and event_card_soup.select_one(s) else None

        raw_card_data["title"] = get_text("event_title_in_card")
        raw_card_data["date_text"] = get_text("event_date_in_card")
        raw_card_data["venue_name"] = get_text("event_venue_in_card")
        raw_card_data["price_text"] = get_text("price_in_card")

        url_s = selectors.get("event_url_in_card")
        url_el = event_card_soup.select_one(url_s) if url_s else None
        raw_card_data["url"] = url_el["href"] if url_el and url_el.has_attr("href") else None

        if not raw_card_data.get("title") and not url_el : return None

        # Transform step returns dict for Pydantic
        return self._transform_event_data(raw_card_data, self.current_page_url)

    def parse_events_from_html(self, html_content: str) -> List[Dict[str, Any]]:
        logger.debug("Parsing HTML and preparing data dicts for Pydantic validation.")
        prepared_data_list: List[Dict[str, Any]] = []
        if not html_content: return prepared_data_list
        soup = BeautifulSoup(html_content, "html.parser")
        s = self.scraper_config.get("selectors", {}).get("event_card")
        if not s: logger.error("Event card selector not configured."); return prepared_data_list

        for card_soup in soup.select(s):
            prepared_dict = self._parse_single_event_card(card_soup)
            if prepared_dict: prepared_data_list.append(prepared_dict)
        logger.info(f"Prepared {len(prepared_data_list)} data dicts from HTML cards for Pydantic validation.")
        return prepared_data_list

    async def fetch_initial_page(self, url: str) -> Optional[str]:
        logger.info(f"Fetching initial page: {url}")
        self.current_page_url = url
        await self._apply_random_delay("request")
        try:
            html_content = await self.client.get(url)
            if isinstance(self.client, PlaceholderPlaywrightClient):
                cookie_s = self.scraper_config.get("selectors", {}).get("cookie_popup_selectors", [])
                for s_cookie in cookie_s:
                    if await self.client.click_if_present(s_cookie):
                        await self._apply_random_delay("post_interaction")
                        html_content = await self.client.get_page_content()
                        break
            return html_content
        except Exception as e: logger.error(f"Error fetching page {url}: {e}", exc_info=True); return None

    async def scrape_live_events(self) -> List[TicketmasterEventModel]: # Return list of Pydantic models
        start_url = self.scraper_config.get("target_urls", {}).get("concerts")
        if not start_url: logger.error("No 'concerts' target URL configured."); return []

        html_content = await self.fetch_initial_page(start_url)
        if not html_content: logger.error("Failed to fetch initial page."); return []

        # Get lists of dictionaries (already transformed, ready for Pydantic)
        html_event_dicts = self.parse_events_from_html(html_content)
        json_ld_event_dicts = self._extract_json_ld(BeautifulSoup(html_content, "html.parser"))

        # Combine and Deduplicate (based on event_url)
        combined_event_dicts: Dict[HttpUrl, Dict[str, Any]] = {} # Key by HttpUrl for Pydantic type
        for event_dict in html_event_dicts + json_ld_event_dicts: # Process all dicts
            url_val = event_dict.get("event_url") # This is HttpUrl or None after transformation
            if not url_val: continue # Skip if URL transformation failed

            if url_val in combined_event_dicts: # Merge, prioritizing latter (JSON-LD if it came second)
                for key, value in event_dict.items():
                    if value is not None or key not in combined_event_dicts[url_val] or combined_event_dicts[url_val][key] is None:
                        combined_event_dicts[url_val][key] = value
            else:
                combined_event_dicts[url_val] = event_dict

        # --- Pydantic Validation Step ---
        validated_events: List[TicketmasterEventModel] = []
        for url, event_dict in combined_event_dicts.items():
            try:
                # Ensure event_url is a string for Pydantic HttpUrl validation if it's somehow not
                if 'event_url' in event_dict and not isinstance(event_dict['event_url'], str):
                    event_dict['event_url'] = str(event_dict['event_url'])

                # Generate event_id from URL if not present before validation
                # The Pydantic model's validator will handle this if 'event_id' is missing
                if 'event_id' not in event_dict and 'event_url' in event_dict:
                     pass # Pydantic model will generate it

                validated_event = TicketmasterEventModel(**event_dict)
                validated_events.append(validated_event)
            except ValidationError as e:
                logger.warning(f"Event data validation failed for URL '{url}': {e.errors()}") # Log Pydantic errors
            except Exception as e_gen: # Catch any other unexpected error during model creation
                logger.error(f"Unexpected error creating Pydantic model for URL '{url}': {e_gen}", exc_info=True)


        logger.info(f"Successfully validated {len(validated_events)} events using Pydantic models.")

        # --- Pagination Logic (Conceptual for Playwright) ---
        # (If pagination occurs, new dicts should also go through the Pydantic validation loop above)
        # For simplicity, this example will not re-implement full pagination data merging here.
        # Assume `validated_events` is the final list for this stage.

        logger.info(f"Total validated events after all steps: {len(validated_events)}")
        return validated_events

# --- Main Execution (for testing) ---
async def main():
    log_format = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    global_settings = PlaceholderSettings()
    logging.getLogger().setLevel(getattr(logging, global_settings.LOG_LEVEL.upper(), logging.INFO))
    logger.info("--- Starting TicketmasterScraper Test Run (with Pydantic validation) ---")
    try:
        scraper = TicketmasterScraper(settings=global_settings)
        validated_event_models = await scraper.scrape_live_events()

        logger.info(f"--- Test Run Finished. Total Validated Pydantic Models: {len(validated_event_models)} ---")
        if validated_event_models:
            logger.info("--- Sample of Validated Pydantic Event Models (First 3): ---")
            for i, model_instance in enumerate(validated_event_models[:3]):
                 logger.info(f"Event Model {i+1}: {model_instance.model_dump_json(indent=2)}") # Use Pydantic's json export
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
    logger.info("--- TicketmasterScraper Test Run Concluded ---")

if __name__ == "__main__":
    import random
    asyncio.run(main())
