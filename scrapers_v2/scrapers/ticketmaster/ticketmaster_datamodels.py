# scrapers_v2/scrapers/ticketmaster/ticketmaster_datamodels.py
from typing import Optional, List, Any, Dict
from datetime import datetime
from pydantic import BaseModel, HttpUrl, field_validator, ValidationInfo, Field
import hashlib

# --- Supporting Models ---

class PriceDetailModel(BaseModel):
    value: Optional[float] = Field(None, description="The numerical price value.")
    currency: Optional[str] = Field(None, description="The currency code (e.g., USD, EUR).", min_length=3, max_length=3)
    name: Optional[str] = Field(None, description="Name of the price tier, e.g., 'General Admission', 'VIP'.")
    # availability: Optional[str] = None # Example: "Available", "Sold Out"

class ArtistInfoModel(BaseModel):
    name: str = Field(..., description="Name of the artist or performer.")
    # url: Optional[HttpUrl] = None # URL to the artist's page

class VenueInfoModel(BaseModel):
    name: Optional[str] = Field(None, description="Name of the venue.")
    address_line_1: Optional[str] = Field(None, description="Street address.")
    city: Optional[str] = Field(None, description="City where the venue is located.")
    state_province: Optional[str] = Field(None, description="State, province, or region.")
    postal_code: Optional[str] = Field(None, description="Postal or ZIP code.")
    country: Optional[str] = Field(None, description="Country where the venue is located.")
    # latitude: Optional[float] = None
    # longitude: Optional[float] = None
    full_address_text: Optional[str] = Field(None, description="The full address as a single string if not broken down.")


# --- Main Event Model ---

class TicketmasterEventModel(BaseModel):
    event_id: str = Field(..., description="Unique identifier for the event, derived from event_url if not provided.")
    event_title: str = Field(..., min_length=1, description="The official title or name of the event.")
    event_url: HttpUrl = Field(..., description="Canonical URL of the event page on Ticketmaster.")

    event_start_datetime: Optional[datetime] = Field(None, description="Event start date and time in UTC (ISO 8601).")
    event_end_datetime: Optional[datetime] = Field(None, description="Event end date and time in UTC (ISO 8601), if specified.")
    raw_date_text: Optional[str] = Field(None, description="Original date string from the source if parsing failed or for reference.")

    venue_name: Optional[str] = Field(None, description="Name of the venue.")
    # venue_info: Optional[VenueInfoModel] = None # For more structured venue details

    lineup: Optional[List[ArtistInfoModel]] = Field(default_factory=list, description="List of artists or performers.")

    ticket_min_price: Optional[float] = Field(None, description="The minimum advertised price for a ticket.")
    ticket_currency: Optional[str] = Field(None, description="Currency of the ticket prices (e.g., USD).", min_length=3, max_length=3)
    raw_price_text: Optional[str] = Field(None, description="Original price string from the source for reference.")
    ticket_prices_detailed: Optional[List[PriceDetailModel]] = Field(default_factory=list, description="List of different ticket prices or tiers.")

    # main_image_url: Optional[HttpUrl] = None
    description: Optional[str] = Field(None, description="Full description of the event.")
    # category: Optional[List[str]] = Field(default_factory=list, description="Categories or genres of the event.")
    # on_sale_datetime: Optional[datetime] = None

    source_platform: str = Field("ticketmaster_mock", description="Platform from which the data was scraped.")
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp (UTC) when the data was scraped/model instance created.")

    # Internal flag, not part of the final schema output usually
    # _is_json_ld: Optional[bool] = PrivateAttr(default=False)

    @field_validator('event_id', mode='before')
    @classmethod
    def generate_event_id_if_missing(cls, v: Any, info: ValidationInfo) -> str:
        if v: # If event_id is already provided (e.g. during transformation)
            return str(v)

        # If not provided, generate from URL
        values = info.data # This contains the input data to the model
        url = values.get('event_url')

        if not url:
            # This case should ideally be caught by event_url being a required field
            # but as a fallback for id generation:
            raise ValueError("event_url is required to generate event_id")

        # Use a consistent hashing method
        return hashlib.md5(str(url).encode()).hexdigest()[:16]

    @field_validator('event_title', 'venue_name', 'description', 'raw_date_text', 'raw_price_text', 'ticket_currency')
    @classmethod
    def strip_string_fields(cls, v: Optional[str]) -> Optional[str]:
        if isinstance(v, str):
            return v.strip()
        return v

    # Example of a more complex validator if needed
    # @model_validator(mode='after')
    # def check_start_before_end(self) -> 'TicketmasterEventModel':
    #     if self.event_start_datetime and self.event_end_datetime:
    #         if self.event_start_datetime > self.event_end_datetime:
    #             raise ValueError("event_start_datetime cannot be after event_end_datetime")
    #     return self

# To make it easy to import all models:
# __all__ = ["TicketmasterEventModel", "PriceDetailModel", "ArtistInfoModel", "VenueInfoModel"]
