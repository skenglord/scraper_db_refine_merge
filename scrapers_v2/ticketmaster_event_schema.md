## Ticketmaster Event Data Schema

This schema defines the structure for data scraped from Ticketmaster events.

### Top-Level Object

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `event_id`          | `string`             | Required  | Unique identifier for the event, ideally from the source or a hash. | Example: `vvG1zZp4vbS7Gk` (Ticketmaster ID)             |
| `event_title`       | `string`             | Required  | The official title or name of the event.                    | Example: "Billie Eilish: Hit Me Hard and Soft: The Tour" |
| `event_url`         | `string`             | Required  | Canonical URL of the event page on Ticketmaster.            | Must be a valid URL.                                     |
| `source_platform`   | `string`             | Required  | Platform from which the data was scraped.                   | Example: "ticketmaster.com"                              |
| `extraction_method` | `string`             | Optional  | Method used for extraction (e.g., "jsonld", "html_fallback"). |                                                          |
| `scraped_at`        | `datetime`           | Required  | Timestamp (UTC) when the data was scraped.                  | ISO 8601 format (e.g., `2024-07-15T10:30:00Z`).         |
| `last_checked_at`   | `datetime`           | Optional  | Timestamp (UTC) when the event was last checked for updates.| ISO 8601 format.                                         |
| `event_dates`       | `EventDateInfo`      | Required  | Object containing detailed date and time information.       | See `EventDateInfo` schema below.                        |
| `location`          | `LocationInfo`       | Required  | Object containing detailed location information.            | See `LocationInfo` schema below.                         |
| `lineup`            | `list[ArtistInfo]`   | Optional  | List of artists or performers.                              | See `ArtistInfo` schema below.                           |
| `ticket_info`       | `TicketInfo`         | Optional  | Object containing ticket pricing, status, and purchase links. | See `TicketInfo` schema below.                           |
| `event_images`      | `list[ImageInfo]`    | Optional  | List of URLs for event-related images.                      | See `ImageInfo` schema below.                            |
| `promoter`          | `PromoterInfo`       | Optional  | Information about the event promoter/organizer.             | See `PromoterInfo` schema below.                         |
| `event_category`    | `list[string]`       | Optional  | Categories or genres of the event.                          | Example: `["Concert", "Pop"]`                            |
| `on_sale_date`      | `datetime`           | Optional  | Timestamp (UTC) when tickets go/went on sale.             | ISO 8601 format.                                         |
| `age_restriction`   | `string`             | Optional  | Age limit or restriction for the event.                     | Example: "18+", "All Ages"                              |
| `accessibility`     | `AccessibilityInfo`  | Optional  | Information regarding accessibility features.               | See `AccessibilityInfo` schema below.                    |
| `additional_info`   | `dict`               | Optional  | Any other relevant information as key-value pairs.          | Example: `{"door_times": "6:00 PM"}`                     |
| `raw_description`   | `string`             | Optional  | Full raw HTML or text description of the event.             |                                                          |
| `is_free`           | `boolean`            | Optional  | True if the event is explicitly marked as free.             |                                                          |
| `is_sold_out`       | `boolean`            | Optional  | True if the event is explicitly marked as sold out.         |                                                          |

### `EventDateInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `start_datetime`    | `datetime`           | Required  | Event start date and time in UTC.                           | ISO 8601 format (e.g., `2024-12-25T20:00:00Z`).         |
| `end_datetime`      | `datetime`           | Optional  | Event end date and time in UTC (if specified).              | ISO 8601 format.                                         |
| `timezone`          | `string`             | Optional  | Original timezone of the event (e.g., "America/New_York").  | IANA timezone format.                                    |
| `date_display_text` | `string`             | Optional  | How the date/time is displayed on the source site.          | Example: "Sat, Dec 25 at 8:00 PM PST"                    |
| `doors_open_time`   | `string`             | Optional  | Time when doors open, if specified.                         | Example: "19:00" or "7:00 PM"                            |

### `LocationInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `venue_name`        | `string`             | Required  | Name of the venue.                                          | Example: "Madison Square Garden"                         |
| `address_line_1`    | `string`             | Optional  | Street address.                                             | Example: "4 Pennsylvania Plaza"                          |
| `address_line_2`    | `string`             | Optional  | Additional address details (suite, floor, etc.).            |                                                          |
| `city`              | `string`             | Required  | City where the venue is located.                            | Example: "New York"                                      |
| `state_province`    | `string`             | Optional  | State, province, or region.                                 | Example: "NY"                                            |
| `postal_code`       | `string`             | Optional  | Postal or ZIP code.                                         | Example: "10001"                                         |
| `country`           | `string`             | Required  | Country where the venue is located.                         | Example: "USA" (Consider using ISO 3166-1 alpha-3)       |
| `latitude`          | `float`              | Optional  | Latitude of the venue.                                      | Range: -90 to 90.                                        |
| `longitude`         | `float`              | Optional  | Longitude of the venue.                                     | Range: -180 to 180.                                      |
| `full_address_text` | `string`             | Optional  | The full address as a single string if not broken down.     | Example: "4 Pennsylvania Plaza, New York, NY 10001, USA" |

### `ArtistInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `name`              | `string`             | Required  | Name of the artist or performer.                            | Example: "Billie Eilish"                                 |
| `role`              | `string`             | Optional  | Role of the performer (e.g., "Headliner", "Support").       |                                                          |
| `artist_url`        | `string`             | Optional  | URL to the artist's page on Ticketmaster or official site.  | Must be a valid URL.                                     |

### `TicketInfo` Schema

| Field Name          | Data Type                 | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|---------------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `ticket_url`        | `string`                  | Optional  | Direct URL to purchase tickets or the event page for tickets. | Must be a valid URL.                                     |
| `status`            | `string`                  | Optional  | Current ticket availability status.                         | Example: "On Sale", "Sold Out", "Presale", "Unavailable" |
| `prices`            | `list[PriceDetail]`       | Optional  | List of different ticket prices or tiers.                   | See `PriceDetail` schema below.                          |
| `min_price`         | `float`                   | Optional  | The minimum advertised price for a ticket.                  |                                                          |
| `max_price`         | `float`                   | Optional  | The maximum advertised price for a ticket (e.g. VIP).       |                                                          |
| `currency`          | `string`                  | Optional  | Currency of the ticket prices.                              | ISO 4217 currency code (e.g., "USD", "EUR", "CAD").      |
| `price_display_text`| `string`                  | Optional  | How price information is displayed on the source site.      | Example: "$50 - $200 + Fees"                             |

### `PriceDetail` Schema (within `TicketInfo.prices`)

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `tier_name`         | `string`             | Optional  | Name of the price tier (e.g., "General Admission", "VIP").  |                                                          |
| `price`             | `float`              | Required  | The actual price for this tier.                             |                                                          |
| `availability`      | `string`             | Optional  | Availability specific to this tier.                         | Example: "Available", "Low Availability", "Sold Out"     |
| `includes_fees`     | `boolean`            | Optional  | Whether this price includes fees or not.                    |                                                          |

### `ImageInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `url`               | `string`             | Required  | URL of the image.                                           | Must be a valid URL.                                     |
| `caption`           | `string`             | Optional  | Caption or description of the image.                        |                                                          |
| `type`              | `string`             | Optional  | Type of image (e.g., "poster", "artist_photo", "venue_photo").|                                                          |

### `PromoterInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `name`              | `string`             | Required  | Name of the promoter or organizing company.                 | Example: "Live Nation"                                   |
| `promoter_url`      | `string`             | Optional  | URL to the promoter's website.                              | Must be a valid URL.                                     |
| `description`       | `string`             | Optional  | Brief description of the promoter.                          |                                                          |

### `AccessibilityInfo` Schema

| Field Name          | Data Type            | Required? | Description / Example                                       | Constraints / Format                                     |
|---------------------|----------------------|-----------|-------------------------------------------------------------|----------------------------------------------------------|
| `details_url`       | `string`             | Optional  | URL to a page with detailed accessibility information.      | Must be a valid URL.                                     |
| `summary`           | `string`             | Optional  | A summary of accessibility features available.              | Example: "Wheelchair accessible, Assistive listening"    |
| `has_accessible_seating` | `boolean`       | Optional  | Indicates if accessible seating options are mentioned.        |                                                          |

---
