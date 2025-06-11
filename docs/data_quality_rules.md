# Data Quality Rules for `unified_events` Collection

## 1. Introduction

This document defines a set of data quality rules and validation checks for the `unified_events` collection in MongoDB. The purpose of these rules is to ensure the accuracy, completeness, consistency, and reliability of the event data stored, which is crucial for any downstream usage, APIs, or user-facing applications.

These rules are intended to be applied during data ingestion (ETL, scraper saving process) and/or through periodic data quality audit jobs.

## 2. Categories of Data Quality Rules

The data quality rules are categorized as follows:

*   **Completeness**: Ensures that essential information is present.
*   **Validity & Format**: Verifies that data values conform to expected data types and structural formats.
*   **Consistency & Plausibility**: Checks for logical contradictions or highly improbable data.
*   **Data Freshness**: Assesses how up-to-date the information is.

## 3. Detailed Data Quality Rules

For each rule, we specify:
*   **Rule ID**: A unique identifier.
*   **Description**: What the rule checks.
*   **Field(s) Affected**: The relevant field(s) in the `unified_events` schema.
*   **Severity**: Critical, High, Medium, Low.
*   **Validation Logic**: How the rule can be checked.
*   **Action if Failed**: Suggested action when a record fails the rule.

---

### 3.1. Completeness Rules

| Rule ID | Description                                     | Field(s) Affected                                     | Severity | Validation Logic                                           | Action if Failed                                  |
|---------|-------------------------------------------------|-------------------------------------------------------|----------|------------------------------------------------------------|---------------------------------------------------|
| C001    | Mandatory unique event identifier.              | `event_id` (`_id`)                                    | Critical | Must exist, be non-empty, and unique within the collection. | Reject record, Investigate source of missing ID.  |
| C002    | Mandatory event title.                          | `event_details.title`                                 | Critical | Must exist and be a non-empty string.                     | Flag record, Investigate scraper parsing.         |
| C003    | Mandatory event start date.                     | `event_dates.start_date_utc`                          | Critical | Must exist and be a valid date.                           | Flag record, Investigate scraper parsing.         |
| C004    | Mandatory source URL.                           | `source_references.source_url`                        | Critical | Must exist and be a valid URL.                            | Flag record, Investigate data origin.             |
| C005    | Recommended venue information.                  | `location.venue_name` (or `location.full_address`)    | High     | At least one primary location identifier should exist.    | Flag record for review.                           |
| C006    | Recommended event description.                  | `event_details.description.text` or `event_details.description.html` | Medium   | Should exist and be non-empty if available from source. | Flag record for review.                           |
| C007    | Recommended main image URL.                     | `event_details.image_urls` (first element)            | Medium   | `image_urls` array should exist and not be empty.         | Flag record for review.                           |
| C008    | Recommended ticket information if not free.     | `ticketing.ticket_info` (or specific price fields)    | Medium   | If `ticketing.is_free` is false, price info is expected.  | Flag record for review.                           |
| C009    | Mandatory data source platform.                 | `source_references.source_platform`                   | Critical | Must exist and be non-empty.                              | Reject record, Fix ingestion logic.               |
| C010    | Mandatory scraped timestamp.                    | `timestamps.scraped_at_utc`                           | Critical | Must exist and be a valid date.                           | Reject record, Fix ingestion logic.               |
| C011    | Recommended performer/artist information.       | `performers` array                                    | Medium   | Array should exist; ideally not empty for music events.   | Flag record for review (especially for concerts). |

---

### 3.2. Validity & Format Rules

| Rule ID | Description                                     | Field(s) Affected                                     | Severity | Validation Logic                                           | Action if Failed                                  |
|---------|-------------------------------------------------|-------------------------------------------------------|----------|------------------------------------------------------------|---------------------------------------------------|
| VF001   | Valid URL format for source URL.                | `source_references.source_url`                        | Critical | Must conform to URL syntax (e.g., regex, URL parsing library). | Reject record, Investigate data origin.             |
| VF002   | Valid URL format for image URLs.                | `event_details.image_urls[]`                          | High     | Each URL in the list must be a valid URL.                 | Flag image URL, Remove invalid URL from list.     |
| VF003   | Valid URL format for ticket URLs.               | `ticketing.ticket_info[].ticket_url`, `ticketing.direct_ticket_purchase_url` | High     | Must be a valid URL if present.                           | Flag ticket URL, Remove invalid URL.              |
| VF004   | Valid ISO 8601 date format for UTC dates.       | `event_dates.start_date_utc`, `event_dates.end_date_utc`, `timestamps.scraped_at_utc`, `timestamps.first_seen_at_utc`, `timestamps.last_updated_at_utc` | Critical | Must be a valid ISO 8601 datetime string (e.g., "YYYY-MM-DDTHH:MM:SSZ"). | Reject/Flag record, Fix date parsing/formatting.  |
| VF005   | Valid timezone string for local dates.          | `event_dates.timezone`                                | High     | Must be a valid IANA timezone (e.g., "Europe/Madrid") if present. | Flag record, Correct timezone.                    |
| VF006   | Numeric and non-negative price values.          | `ticketing.ticket_info[].price`, `ticketing.ticket_info[].min_price`, `ticketing.ticket_info[].max_price` | High     | Must be a non-negative number if present.                 | Flag price, Investigate parsing.                  |
| VF007   | Valid currency code.                            | `ticketing.ticket_info[].currency`                    | High     | Must be a valid ISO 4217 currency code (e.g., "EUR", "USD") if present. | Flag currency, Correct or default.                |
| VF008   | Valid geo-coordinates.                          | `location.coordinates.latitude`, `location.coordinates.longitude` | High     | Latitude: -90 to 90. Longitude: -180 to 180.            | Flag coordinates, Remove/correct invalid values.  |
| VF009   | Boolean type for boolean fields.                | `ticketing.is_free`, `event_details.is_cancelled`, etc. | High     | Must be a true boolean type.                              | Flag field, Fix data type during transformation.  |
| VF010   | Performer list structure.                       | `performers[]`                                        | Medium   | Each item in list should be a dict with at least a 'name' string. | Flag record, Investigate performer parsing.       |
| VF011   | String length limits for text fields.           | `event_details.title`, `event_details.description.text`, `location.venue_name` | Medium   | Check for unusually short or excessively long strings (e.g., title > 5 chars, description < 10000 chars). | Flag record for review.                           |

---

### 3.3. Consistency & Plausibility Rules

| Rule ID | Description                                     | Field(s) Affected                                     | Severity | Validation Logic                                           | Action if Failed                                  |
|---------|-------------------------------------------------|-------------------------------------------------------|----------|------------------------------------------------------------|---------------------------------------------------|
| CP001   | Event end date after or same as start date.     | `event_dates.start_date_utc`, `event_dates.end_date_utc` | High     | `end_date_utc` must be >= `start_date_utc` if both exist. | Flag record, Investigate date parsing/logic.      |
| CP002   | Plausible event duration.                       | `event_dates.start_date_utc`, `event_dates.end_date_utc` | Medium   | Event duration should be within a reasonable range (e.g., < 7 days for typical events, unless it's a festival). | Flag record for review.                           |
| CP003   | Start date should not be too far in the past/future. | `event_dates.start_date_utc`                          | Medium   | E.g., not older than 1 year or more than 3 years in future (configurable). | Flag record for review.                           |
| CP004   | Location consistency (City/Country).            | `location.city`, `location.country_code`              | Medium   | If city is "New York", country should be "US". Use a lookup if needed. | Flag record for review.                           |
| CP005   | Price plausibility.                             | `ticketing.ticket_info[].price` (and min/max)        | Medium   | Price should be within a reasonable range (e.g., not $0.01 for a major concert, not $1,000,000). Exclude if `is_free`. | Flag record for review.                           |
| CP006   | Consistent `is_free` flag and price info.       | `ticketing.is_free`, `ticketing.ticket_info[].price`  | High     | If `is_free` is true, prices should generally be 0 or absent. If false, price info expected. | Flag record, Investigate pricing data.            |
| CP007   | Title and description content.                  | `event_details.title`, `event_details.description.text` | Low      | Check for placeholder text (e.g., "Untitled Event", "Lorem Ipsum"), excessive special characters, or overly promotional language. | Flag for review, Potential spam/low quality.    |
| CP008   | Venue name plausibility.                        | `location.venue_name`                                 | Low      | Check for generic names like "TBC", "Venue to be announced" if event date is very soon. | Flag for review.                                  |

---

### 3.4. Data Freshness Rules

| Rule ID | Description                                     | Field(s) Affected                                     | Severity | Validation Logic                                           | Action if Failed                                  |
|---------|-------------------------------------------------|-------------------------------------------------------|----------|------------------------------------------------------------|---------------------------------------------------|
| DF001   | Event data recently scraped/updated.            | `timestamps.scraped_at_utc` or `timestamps.last_updated_at_utc` | High     | `scraped_at_utc` (or `last_updated_at_utc`) should not be older than X days (e.g., 7 days for active events). | Flag record for re-scraping, Investigate scraper schedule. |
| DF002   | Stale future event.                             | `event_dates.start_date_utc`, `timestamps.scraped_at_utc` | Medium   | If `start_date_utc` is in the future, `scraped_at_utc` should be relatively recent (e.g., within 30-90 days depending on how far in future). | Flag record for re-scraping.                      |

## 4. Implementing Data Quality Checks

These rules can be implemented at various stages:

*   **During Scraping/Transformation**:
    *   Individual scrapers or the `schema_adapter` can perform some initial validation (e.g., basic type checks, URL format).
    *   The ETL process (`etl_sqlite_to_mongo.py`) or the new `save_unified_events_to_mongodb` utility can incorporate validation logic before writing to MongoDB, potentially using libraries like Pydantic for schema validation of the unified event model.
*   **Post-Load Batch Validation**:
    *   A separate script/Prefect flow can run periodically to query the `unified_events` collection and apply these rules.
    *   This script can generate reports, flag records with a "quality_issue" field, or move them to a quarantine collection.
*   **Database-Level Validation (MongoDB Atlas)**:
    *   MongoDB Atlas offers JSON Schema validation at the collection level. This can enforce structural rules, data types, and mandatory fields directly in the database.
*   **Monitoring & Alerting**:
    *   Metrics derived from these DQ checks (e.g., percentage of records failing C001) can be sent to Prometheus and visualized in Grafana.
    *   Alerts can be set up for significant drops in data quality or high failure rates for critical rules.

A combination of these approaches is often most effective. For instance, critical structural and format validations can happen pre-load or at the database level, while more complex consistency and plausibility checks might be done in a batch process.
