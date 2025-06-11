import hashlib
import json
import logging
import re
from datetime import datetime, timezone as dt_timezone, date, time as dt_time
from typing import List, Optional, Dict, Any, Tuple, Union
from urllib.parse import urlparse

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator
from dateutil import parser as dateutil_parser
import pytz # For timezone handling

# Setup logger
logger = logging.getLogger(__name__)
# Basic configuration if no handlers are attached yet (e.g., when run directly)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def _normalize_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    text = text.strip()
    text = re.sub(r'\s{2,}', ' ', text) # Replace multiple spaces with a single space
    return text if text else None

def _generate_event_id(composite_key_fields: List[Optional[str]]) -> str:
    """Generates a SHA256 hash based on a list of key fields."""
    # Normalize and concatenate fields, ensuring consistent order and handling of None
    key_string = "|".join(str(field).lower().strip() if field is not None else "none" for field in composite_key_fields)
    return hashlib.sha256(key_string.encode('utf-8')).hexdigest()

def _parse_date_to_utc_iso(
    date_str: Optional[str],
    default_timezone_str: str = "UTC",
    current_year: Optional[int] = None
) -> Optional[str]:
    if not date_str or not isinstance(date_str, str):
        return None

    cleaned_date_str = date_str.strip()
    if not cleaned_date_str:
        return None

    try:
        if current_year and not re.search(r'\b\d{4}\b', cleaned_date_str):
            cleaned_date_str += f" {current_year}"

        dt_obj = dateutil_parser.parse(cleaned_date_str)

        if dt_obj.tzinfo is None or dt_obj.tzinfo.utcoffset(dt_obj) is None:
            default_tz = pytz.timezone(default_timezone_str)
            dt_obj = default_tz.localize(dt_obj, is_dst=None)

        return dt_obj.astimezone(pytz.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, TypeError, pytz.exceptions.UnknownTimeZoneError) as e:
        logger.debug(f"Could not parse date string '{date_str}' with default_tz '{default_timezone_str}': {e}")
        if default_timezone_str == "UTC":
            for tz_str_alt in ["Europe/Madrid", "Europe/London", "Europe/Berlin"]:
                try:
                    dt_obj_alt = dateutil_parser.parse(cleaned_date_str)
                    alt_tz = pytz.timezone(tz_str_alt)
                    dt_obj_alt_aware = alt_tz.localize(dt_obj_alt, is_dst=None)
                    return dt_obj_alt_aware.astimezone(pytz.utc).isoformat().replace("+00:00", "Z")
                except (ValueError, TypeError, pytz.exceptions.UnknownTimeZoneError):
                    continue
        return None


def _extract_price_info(price_text: Optional[str]) -> Tuple[Optional[float], Optional[str]]:
    if not price_text or not isinstance(price_text, str):
        return None, None

    price_text = price_text.strip().lower()
    if "free" in price_text or "gratis" in price_text:
        return 0.0, "EUR"

    match = re.search(r'(?:from\s*)?([€$£])?\s*(\d+(?:[.,]\d{1,2})?)\s*([€$£]|[A-Z]{3})?', price_text, re.IGNORECASE)

    amount: Optional[float] = None
    currency: Optional[str] = None

    if match:
        try:
            amount_str = match.group(2).replace(',', '.')
            amount = float(amount_str)
            curr_sym_before = match.group(1)
            curr_sym_after = match.group(3)

            if curr_sym_before == '€' or curr_sym_after == '€' or (curr_sym_after and curr_sym_after.lower() == "eur"):
                currency = "EUR"
            elif curr_sym_before == '$' or curr_sym_after == '$' or (curr_sym_after and curr_sym_after.lower() == "usd"):
                currency = "USD"
            elif curr_sym_before == '£' or curr_sym_after == '£' or (curr_sym_after and curr_sym_after.lower() == "gbp"):
                currency = "GBP"
            elif curr_sym_after and len(curr_sym_after) == 3:
                currency = curr_sym_after.upper()
            else:
                currency = "EUR"
        except ValueError:
            logger.debug(f"Could not parse amount from price string: '{price_text}'")
            return None, None

    return amount, currency


# --- Pydantic Models for UnifiedEvent Schema ---

class EventTimestamps(BaseModel):
    scraped_at_utc: datetime = Field(default_factory=lambda: datetime.now(dt_timezone.utc))
    first_seen_at_utc: Optional[datetime] = None
    last_updated_at_utc: datetime = Field(default_factory=lambda: datetime.now(dt_timezone.utc))

class EventDateDetails(BaseModel):
    start_date_utc: Optional[str] = None
    end_date_utc: Optional[str] = None
    start_date_local: Optional[str] = None
    end_date_local: Optional[str] = None
    timezone: Optional[str] = None
    display_text: Optional[str] = None
    door_time_text: Optional[str] = None

class LocationCoordinates(BaseModel):
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @field_validator('latitude')
    def latitude_must_be_valid(cls, v):
        if v is not None and not (-90 <= v <= 90):
            raise ValueError('Latitude must be between -90 and 90')
        return v

    @field_validator('longitude')
    def longitude_must_be_valid(cls, v):
        if v is not None and not (-180 <= v <= 180):
            raise ValueError('Longitude must be between -180 and 180')
        return v

class EventLocation(BaseModel):
    venue_name: Optional[str] = None
    full_address: Optional[str] = None
    street_address: Optional[str] = None
    city: Optional[str] = None
    state_province: Optional[str] = None
    postal_code: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = None
    coordinates: Optional[LocationCoordinates] = None

class Artist(BaseModel):
    name: str
    role: Optional[str] = Field(None, description="e.g., headliner, support, DJ, band")
    source_artist_id: Optional[str] = Field(None, description="ID from the source platform, if available")

class TicketPrice(BaseModel):
    amount: Optional[float] = None
    currency: Optional[str] = None
    price_type: Optional[str] = Field(None, description="e.g., General Admission, VIP, Early Bird")

    @field_validator('amount')
    def amount_must_be_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError('Price amount cannot be negative')
        return v

    @field_validator('currency')
    def currency_code_format(cls, v):
        if v is not None and not re.fullmatch(r'^[A-Z]{3}$', v.upper()):
            logger.warning(f"Currency code '{v}' does not seem to be a standard 3-letter code.")
        return v.upper() if v else None


class TicketInfo(BaseModel):
    availability: Optional[str] = Field(None, description="e.g., on_sale, sold_out, off_sale, free")
    is_free: bool = False
    ticket_purchase_url: Optional[HttpUrl] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    price_display_text: Optional[str] = Field(None, description="Original price string, e.g., '€25 - €50', 'From $30'")
    price_tiers: Optional[List[TicketPrice]] = None

    @model_validator(mode='after')
    def check_prices_if_not_free(self):
        if not self.is_free and self.min_price is None and not self.price_display_text and not self.price_tiers:
            logger.debug("TicketInfo: Event is not free, but no price information provided.")
        if self.is_free and (self.min_price is not None and self.min_price > 0):
            logger.warning("TicketInfo: Event marked as free, but min_price is greater than 0.")
        return self

class EventMedia(BaseModel):
    image_urls: Optional[List[HttpUrl]] = None
    video_urls: Optional[List[HttpUrl]] = None

class Organizer(BaseModel):
    name: str
    organizer_url: Optional[HttpUrl] = None

class EventDetails(BaseModel):
    title: str
    subtitle: Optional[str] = None
    description_text: Optional[str] = None
    description_html: Optional[str] = None
    type: Optional[str] = Field(None, description="e.g., concert, festival, conference, online, club_night")
    categories_tags: Optional[List[str]] = None
    language: Optional[str] = Field(None, description="Primary language of the event content, ISO 639-1 code e.g. 'en', 'es'")

class SourceReference(BaseModel):
    source_platform: str
    source_url: HttpUrl
    source_event_id: Optional[str] = Field(None, description="ID from the source platform, if available")

# class DataQuality(BaseModel): # This was defined in the previous step, now adding it here
#     overall_score: Optional[float] = None
#     issues_found: Optional[List[Dict[str, Any]]] = None
#     last_assessed_utc: Optional[datetime] = None

class UnifiedEvent(BaseModel):
    event_id: str = Field(description="Primary Key. Generated hash of key event properties.")

    timestamps: EventTimestamps
    event_details: EventDetails
    event_dates: EventDateDetails
    location: EventLocation

    performers: Optional[List[Artist]] = None
    ticketing: Optional[TicketInfo] = None
    media: Optional[EventMedia] = None
    organizers: Optional[List[Organizer]] = None
    source_references: SourceReference

    quality_assessment: Optional[Dict[str, Any]] = Field(None, description="Results from basic DQ scoring, e.g. {'overall_score': 80, 'issues': []}")
    additional_properties: Optional[Dict[str, Any]] = Field(None, description="For extra data not fitting the main schema.")
    raw_data_snapshot: Optional[Dict[str, Any]] = Field(None, description="Snapshot of raw_data for debugging (use sparingly in prod).")

    model_config = SettingsConfigDict(validate_assignment=True)


# --- Main Mapping Function ---

def map_to_unified_schema(
    raw_data: Dict[str, Any],
    source_platform: str,
    source_url: str
) -> Optional[UnifiedEvent]:
    if not raw_data or not isinstance(raw_data, dict):
        logger.error("map_to_unified_schema: raw_data is empty or not a dictionary.")
        return None

    logger.info(f"Mapping event from source: {source_platform}, URL: {source_url}")

    title = _normalize_text(raw_data.get("title", raw_data.get("name")))

    raw_date_str = raw_data.get("raw_date_string", raw_data.get("date_text", raw_data.get("startDate")))
    current_year_context = raw_data.get("page_year_context", datetime.now().year)
    platform_timezone = "Europe/Madrid" if "ibiza" in source_platform.lower() else "UTC"
    start_date_utc_iso = _parse_date_to_utc_iso(raw_date_str, default_timezone_str=platform_timezone, current_year=current_year_context)

    venue_name_raw = raw_data.get("venue", raw_data.get("venue_name"))
    venue_name = _normalize_text(venue_name_raw.get("name")) if isinstance(venue_name_raw, dict) else _normalize_text(venue_name_raw)

    id_fields_for_hash = [
        title,
        start_date_utc_iso.split('T')[0] if start_date_utc_iso else None,
        venue_name,
        urlparse(source_url).netloc
    ]
    event_id = _generate_event_id(id_fields_for_hash)

    if not all([event_id, title, start_date_utc_iso, source_url, source_platform]):
        missing_fields_log = [f for f_name, f in [("event_id",event_id), ("title",title), ("start_date_utc",start_date_utc_iso), ("source_url",source_url), ("source_platform",source_platform)] if not f] # simplified
        logger.error(f"Essential data missing for event from {source_url}. Missing: {', '.join(missing_fields_log)}. Cannot create UnifiedEvent.")
        return None

    timestamps_data = EventTimestamps()
    if raw_data.get("scrapedAt"):
        parsed_scraped_at = _parse_date_to_utc_iso(raw_data.get("scrapedAt"))
        if parsed_scraped_at:
            try: timestamps_data.scraped_at_utc = datetime.fromisoformat(parsed_scraped_at.replace("Z", "+00:00"))
            except ValueError: pass

    event_details_data = EventDetails(
        title=title,
        description_text=_normalize_text(raw_data.get("description", raw_data.get("full_description", raw_data.get("json_ld_description")))),
        description_html=raw_data.get("description_html"),
        categories_tags=raw_data.get("genres", raw_data.get("categories", [])),
        type=_normalize_text(raw_data.get("eventType", raw_data.get("event_type")))
    )

    event_dates_data = EventDateDetails(
        start_date_utc=start_date_utc_iso,
        display_text=raw_date_str,
        timezone=platform_timezone
    )

    raw_location = raw_data.get("location", {})
    if not isinstance(raw_location, dict): raw_location = {}

    coordinates_data = None
    raw_coords = raw_location.get("coordinates", raw_location.get("geo"))
    if isinstance(raw_coords, dict) and raw_coords.get("latitude") is not None and raw_coords.get("longitude") is not None:
        try:
            coordinates_data = LocationCoordinates(
                latitude=float(raw_coords["latitude"]),
                longitude=float(raw_coords["longitude"])
            )
        except (ValueError, TypeError): logger.debug(f"Could not parse coordinates: {raw_coords}")

    location_data = EventLocation(
        venue_name=venue_name,
        full_address=_normalize_text(raw_location.get("full_address", raw_location.get("address"))),
        city=_normalize_text(raw_location.get("city", raw_location.get("addressLocality"))),
        country_code=_normalize_text(raw_location.get("country_code", raw_location.get("addressCountry"))),
        coordinates=coordinates_data
    )

    performers_list = []
    raw_performers = raw_data.get("artists", raw_data.get("lineUp", raw_data.get("performers", [])))
    if isinstance(raw_performers, list):
        for p_data in raw_performers:
            if isinstance(p_data, dict) and p_data.get("name"):
                performers_list.append(Artist(name=_normalize_text(p_data["name"]), role=_normalize_text(p_data.get("role"))))
            elif isinstance(p_data, str) and _normalize_text(p_data):
                 performers_list.append(Artist(name=_normalize_text(p_data)))

    ticket_info_data = None
    raw_ticket_info = raw_data.get("ticketInfo", raw_data.get("offers", raw_data.get("tickets_info")))
    if isinstance(raw_ticket_info, dict):
        min_price, currency = _extract_price_info(raw_ticket_info.get("price_text", raw_ticket_info.get("startingPrice", raw_ticket_info.get("price"))))
        is_free_val = raw_ticket_info.get("isFree", False)
        if min_price == 0.0 and not is_free_val: is_free_val = True
        ticket_info_data = TicketInfo(
            availability=_normalize_text(raw_ticket_info.get("availability", raw_ticket_info.get("status"))),
            is_free=is_free_val,
            ticket_purchase_url=raw_ticket_info.get("ticket_url", raw_ticket_info.get("url")),
            min_price=min_price, currency=currency,
            price_display_text=_normalize_text(raw_ticket_info.get("price_text", raw_ticket_info.get("displayText")))
        )
    elif isinstance(raw_data.get("price_text"), str):
        min_price, currency = _extract_price_info(raw_data.get("price_text"))
        is_free_val = raw_data.get("isFree", False)
        if min_price == 0.0 and not is_free_val: is_free_val = True
        ticket_info_data = TicketInfo(min_price=min_price, currency=currency, is_free=is_free_val, price_display_text=raw_data.get("price_text"))

    media_data = EventMedia(
        image_urls=[url for url in raw_data.get("image_urls", raw_data.get("images", [])) if isinstance(url, str) and url.startswith("http")]
    )

    source_references_data = SourceReference(
        source_platform=source_platform,
        source_url=source_url,
        source_event_id=raw_data.get("source_event_id")
    )

    try:
        unified_event = UnifiedEvent(
            event_id=event_id,
            timestamps=timestamps_data,
            event_details=event_details_data,
            event_dates=event_dates_data,
            location=location_data,
            performers=performers_list if performers_list else None,
            ticketing=ticket_info_data,
            media=media_data if media_data.image_urls or media_data.video_urls else None,
            source_references=source_references_data,
            quality_assessment=None # Initialize as None, to be filled later
        )
        logger.info(f"Successfully mapped event '{title}' (ID: {event_id}) to UnifiedEvent schema.")
        return unified_event

    except Exception as e:
        logger.error(f"Error creating UnifiedEvent instance for {source_url} (title: {title}): {e}", exc_info=True)
        try: logger.debug(f"Failed raw_data: {json.dumps(raw_data, default=str, indent=2)}")
        except Exception: logger.debug(f"Failed raw_data (could not serialize to JSON): {raw_data}")
        return None

if __name__ == '__main__':
    logger.info("Testing schema_adapter.py...")
    raw_data_1 = {
        "title": "  Summer Music Festival 2024  ", "date_text": "August 15-18, 2024",
        "startDate": "2024-08-15T18:00:00+02:00", "venue": {"name": "Central Park Amphitheatre"},
        "location": {"address": "123 Park Ave, New York, NY", "geo": {"latitude": "40.7829", "longitude": "-73.9654"}},
        "description": "An amazing music festival with various artists.",
        "artists": [{"name": "DJ Beats"}, {"name": "The Rockers", "role": "Headliner"}],
        "price_text": "From €50.00 - Tickets available", "categories": ["Music", "Festival", "Outdoor"],
        "source_event_id": "evt12345", "image_urls": ["http://example.com/image.jpg"]
    }
    unified_event_1 = map_to_unified_schema(raw_data_1, "example_platform", "http://example.com/event/123")
    if unified_event_1:
        print("\n--- Unified Event 1 ---"); print(unified_event_1.model_dump_json(indent=2))
    else: print("\n--- Unified Event 1: Mapping Failed ---")

    raw_data_2 = {"name": "Quick Gig"}
    unified_event_2 = map_to_unified_schema(raw_data_2, "another_platform", "http://another.com/gig/xyz")
    if unified_event_2: print("\n--- Unified Event 2 ---"); print(unified_event_2.model_dump_json(indent=2))
    else: print("\n--- Unified Event 2: Mapping Failed (as expected due to missing critical data) ---")
```
