from typing import TypedDict, List, Optional

# --- Data Schema Definitions (copied from classy_skkkrapey.py) ---

class LocationSchema(TypedDict, total=False):
    venue: str
    address: str
    city: str
    country: str

class DateTimeSchema(TypedDict, total=False):
    startDate: str # Assuming ISO date string or similar string representation
    endDate: str   # Assuming ISO date string or similar string representation
    doorTime: str
    timeZone: str
    displayText: str

class ArtistSchema(TypedDict, total=False):
    name: str
    headliner: bool

class TicketInfoSchema(TypedDict, total=False):
    url: str
    availability: str
    startingPrice: float
    currency: str

class EventSchema(TypedDict, total=False):
    url: str
    scrapedAt: str # Assuming ISO date string or similar string representation
    extractionMethod: str
    title: str
    location: LocationSchema
    dateTime: DateTimeSchema
    lineUp: List[ArtistSchema]
    ticketInfo: TicketInfoSchema
    description: str

# --- Markdown Formatting Function ---

def format_event_to_markdown_cs(event: EventSchema) -> str:
    """
    Converts a single EventSchema dictionary to a Markdown string.
    Adapted from classy_skkkrapey.format_event_to_markdown.
    """
    lines: List[str] = []

    # Title
    title = event.get('title', 'N/A Event Title')
    lines.append(f"### {title}")

    # URL
    event_url = event.get('url', 'N/A')
    lines.append(f"**URL**: {event_url}")

    # Extraction Method
    extraction_method = event.get('extractionMethod')
    if extraction_method:
        lines.append(f"**Extraction Method**: {extraction_method}")

    # Scraped At
    scraped_at = event.get('scrapedAt')
    if scraped_at:
        lines.append(f"**Scraped At**: {scraped_at}")

    lines.append("") # Add a blank line for spacing

    # Location
    loc = event.get("location")
    if loc:
        lines.append("**Location**:")
        if venue := loc.get('venue', 'N/A'):
            lines.append(f"- Venue: {venue}")
        if address := loc.get('address'):
            lines.append(f"- Address: {address}")
        if city := loc.get('city'):
            lines.append(f"- City: {city}")
        if country := loc.get('country'):
            lines.append(f"- Country: {country}")
        lines.append("") # Add a blank line for spacing


    # Date & Time
    dt = event.get("dateTime")
    if dt:
        lines.append("**Date & Time**:")
        if start_date := dt.get('startDate', 'N/A'):
            lines.append(f"- Start Date: {start_date}")
        if end_date := dt.get('endDate'):
            lines.append(f"- End Date: {end_date}")
        if door_time := dt.get('doorTime'):
            lines.append(f"- Door Time: {door_time}")
        if time_zone := dt.get('timeZone'):
            lines.append(f"- Time Zone: {time_zone}")
        if display_text := dt.get('displayText'):
            lines.append(f"- Display Text: {display_text}")
        lines.append("") # Add a blank line for spacing

    # Lineup
    lineup = event.get("lineUp")
    if lineup:
        lines.append("**Lineup**:")
        for artist in lineup:
            name = artist.get("name", "Unknown Artist")
            is_headliner = " (Headliner)" if artist.get("headliner", False) else ""
            lines.append(f"- {name}{is_headliner}")
        lines.append("") # Add a blank line for spacing

    # Ticket Information
    ticket_info = event.get("ticketInfo")
    if ticket_info:
        lines.append("**Ticket Information**:")
        if ticket_url := ticket_info.get('url'):
            lines.append(f"- URL: {ticket_url}")
        if availability := ticket_info.get('availability'):
            lines.append(f"- Availability: {availability}")
        # Ensure startingPrice is handled correctly (it's float)
        starting_price = ticket_info.get('startingPrice')
        if starting_price is not None: # Check for None explicitly for float
            currency = ticket_info.get('currency', '')
            lines.append(f"- Starting Price: {starting_price:.2f} {currency}".strip())
        lines.append("") # Add a blank line for spacing

    # Description
    description = event.get("description")
    if description:
        lines.append("**Description**:")
        lines.append(description)
        lines.append("") # Add a blank line for spacing

    return "\n".join(lines)
