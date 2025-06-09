import json
from bs4 import BeautifulSoup, Tag
from typing import Optional, List, TypedDict

# --- Data Schema Definitions ---

class LocationSchema(TypedDict, total=False):
    venue: str
    address: str
    city: str
    country: str

class DateTimeSchema(TypedDict, total=False):
    startDate: str
    endDate: str
    doorTime: str
    timeZone: str
    displayText: str

class ArtistSchema(TypedDict, total=False):
    name: str
    headliner: bool

class TicketInfoSchema(TypedDict, total=False):
    url: str
    availability: str
    startingPrice: float # Keep as float, handle potential conversion issues if price is string
    currency: str

class EventSchema(TypedDict, total=False):
    url: str # Will not be set by this parser, but part of the schema
    scrapedAt: str # Will not be set by this parser
    extractionMethod: str # Will be set by this parser
    title: str
    location: LocationSchema
    dateTime: DateTimeSchema
    lineUp: List[ArtistSchema]
    ticketInfo: TicketInfoSchema
    description: str

# --- JSON-LD Parsing Function ---

def parse_json_ld_event_cs(soup: BeautifulSoup) -> Optional[EventSchema]:
    """
    Parses JSON-LD data from a BeautifulSoup object to extract event information.
    Adapted from classy_skkkrapey.TicketsIbizaScraper._parse_json_ld.
    """
    scripts = soup.find_all("script", type="application/ld+json")
    for script_tag in scripts:
        try:
            if script_tag.string:
                data = json.loads(script_tag.string)

                # Check if it's a single event or a list of events (e.g., on a listing page)
                if isinstance(data, list):
                    # If it's a list, take the first item if it's a MusicEvent
                    # This component is designed to parse a single event, so we pick the first.
                    # A more advanced parser might handle multiple events from one JSON-LD block.
                    if not data: continue # Empty list
                    event_data_item = None
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") == "MusicEvent":
                            event_data_item = item
                            break
                    if not event_data_item:
                        continue # No MusicEvent found in the list
                    data = event_data_item # Process the first MusicEvent found
                elif isinstance(data, dict):
                    if data.get("@type") != "MusicEvent":
                        # Handle cases where JSON-LD might be for a different type (e.g., BreadcrumbList then Event)
                        # Search for a graph containing the MusicEvent
                        if "@graph" in data and isinstance(data["@graph"], list):
                            event_data_item = None
                            for item in data["@graph"]:
                                if isinstance(item, dict) and item.get("@type") == "MusicEvent":
                                    event_data_item = item
                                    break
                            if not event_data_item:
                                continue # No MusicEvent found in @graph
                            data = event_data_item # Process the MusicEvent from @graph
                        else:
                            continue # Not a MusicEvent and no @graph to search
                else:
                    continue # Unexpected data type

                loc = data.get("location", {})
                if isinstance(loc, list): # Handle location being a list (take first)
                    loc = loc[0] if loc else {}

                # Ensure loc is a dictionary before proceeding
                if not isinstance(loc, dict):
                    loc = {}

                offers_data = data.get("offers", []) # offers can be a single dict or a list
                offer = {}
                if isinstance(offers_data, list):
                    if offers_data: # If list is not empty
                        offer = offers_data[0] # Take the first offer
                elif isinstance(offers_data, dict): # If it's a single offer object
                    offer = offers_data

                # Ensure offer is a dictionary
                if not isinstance(offer, dict):
                    offer = {}

                # Price conversion
                price_val = offer.get("price")
                starting_price = None
                if price_val is not None:
                    try:
                        starting_price = float(price_val)
                    except (ValueError, TypeError):
                        # print(f"Warning: Could not convert price '{price_val}' to float.")
                        starting_price = None

                # Performers
                performers_data = data.get("performer", [])
                lineup: List[ArtistSchema] = []
                if isinstance(performers_data, list):
                    for p in performers_data:
                        if isinstance(p, dict) and p.get("name"):
                            lineup.append(ArtistSchema(name=p.get("name"), headliner=True)) # Assuming all listed are headliners for simplicity
                elif isinstance(performers_data, dict) and performers_data.get("name"): # Single performer case
                     lineup.append(ArtistSchema(name=performers_data.get("name"), headliner=True))


                event_schema = EventSchema(
                    title=data.get("name"),
                    location=LocationSchema(
                        venue=loc.get("name"),
                        address=loc.get("address", {}).get("streetAddress") if isinstance(loc.get("address"), dict) else loc.get("address") if isinstance(loc.get("address"), str) else None
                    ),
                    dateTime=DateTimeSchema(startDate=data.get("startDate"), endDate=data.get("endDate")),
                    lineUp=lineup,
                    ticketInfo=TicketInfoSchema(
                        url=offer.get("url"),
                        startingPrice=starting_price,
                        currency=offer.get("priceCurrency"),
                        availability=offer.get("availability")
                    ),
                    description=data.get("description"),
                    extractionMethod="json-ld"
                )
                # Basic validation: title and startDate must exist for it to be a valid event
                if event_schema.get("title") and event_schema.get("dateTime", {}).get("startDate"):
                    return event_schema

        except (json.JSONDecodeError, AttributeError, TypeError, IndexError) as e:
            # print(f"[DEBUG] Error parsing JSON-LD in parse_json_ld_event_cs: {e}") # Keep for debugging if necessary
            continue
    return None
