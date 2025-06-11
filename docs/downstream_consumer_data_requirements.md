# Downstream Consumer Data Requirements

## 1. Introduction

This document outlines the typical key data requirements for the hypothesized downstream consumer types identified in `docs/downstream_consumer_identification.md`. Understanding these requirements is crucial for designing appropriate data delivery mechanisms, APIs, and prioritizing data quality efforts for the `unified_events` collection.

The requirements are based on common fields expected in a unified event schema, such as: `event_id`, `title`, `description_text`, `description_html`, `start_date_utc`, `end_date_utc`, `timezone`, `venue_name`, `full_address`, `city`, `country_code`, `coordinates (lat, lon)`, `performers (name, role)`, `image_urls`, `ticket_info (price, currency, availability, ticket_url)`, `source_url`, `source_platform`, `tags/categories`, `is_free`, `is_cancelled`, `organizer_name`, `last_updated_utc`, `data_quality_summary`.

## 2. Data Requirements by Consumer Type

### A. Public Event API
*   **Description**: Exposes event data to external developers or partners.
*   **Key Fields**:
    *   `event_id`, `title`, `description_text` (or a snippet)
    *   `start_date_utc`, `end_date_utc`, `timezone`
    *   `venue_name`, `full_address`, `city`, `country_code`, `coordinates`
    *   `performers` (names primarily)
    *   `image_urls` (main image, possibly thumbnails)
    *   `ticket_info` (condensed: e.g., starting price, currency, main ticket URL, availability status)
    *   `source_url` (for attribution/details)
    *   `tags/categories`
    *   `is_free`, `is_cancelled`
    *   `organizer_name` (optional)
    *   `last_updated_utc` (for consumers to sync)
*   **Expected Data Freshness/Latency**:
    *   High to Medium. Updates within minutes to a few hours of changes are desirable. Latency < 1-6 hours.
*   **Common Data Formats/Access Methods**:
    *   REST API (JSON).
    *   GraphQL API.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: Event times, locations, and URLs must be accurate.
    *   **Completeness**: Core details (title, date, venue) are essential.
    *   **Validity**: URLs must be valid and lead to correct pages. Dates must be correctly formatted.
    *   **Consistency**: Date formats, timezone handling.

### B. Internal Analytics Dashboards
*   **Description**: Used by internal teams for BI, tracking, and insights.
*   **Key Fields**:
    *   All fields, potentially including metadata like `scraped_at_utc`, `extraction_method`.
    *   `event_id`, `title`
    *   `start_date_utc`, `event_dates.timezone`
    *   `location.city`, `location.country_code`, `location.venue_name`
    *   `source_references.source_platform`
    *   `ticketing.ticket_info` (prices, currency for financial analysis)
    *   `tags/categories`, `event_details.type` (if available)
    *   `data_quality_summary` (for tracking DQ itself)
    *   `performers` count, `image_urls` count
*   **Expected Data Freshness/Latency**:
    *   Medium. Daily updates are often sufficient. Latency < 24 hours.
*   **Common Data Formats/Access Methods**:
    *   Direct read access to MongoDB (potentially a replica or data warehouse).
    *   SQL database if data is further ETL'd.
    *   Data dumps (CSV, Parquet) for BI tool ingestion.
*   **Critical Data Quality Aspects**:
    *   **Completeness**: Completeness of fields used in aggregations (e.g., city, country, source).
    *   **Accuracy**: Dates, prices, and categorical data (like source platform, city) must be accurate for correct reporting.
    *   **Consistency**: Consistent naming for categories, platforms, etc.

### C. Recommendation Engine
*   **Description**: Personalizes event suggestions for users.
*   **Key Fields**:
    *   `event_id`, `title`
    *   `event_details.description_text` (for NLP/keyword extraction)
    *   `event_dates.start_date_utc` (to recommend upcoming events)
    *   `location.venue_name`, `location.city`, `location.coordinates`
    *   `performers` (names, roles, potentially genres associated with them)
    *   `tags/categories`, `event_details.type`
    *   `ticketing.ticket_info` (price range, `is_free`)
    *   `event_details.image_urls` (for display)
*   **Expected Data Freshness/Latency**:
    *   High. New events and cancellations should be reflected quickly. Latency < 1-4 hours.
*   **Common Data Formats/Access Methods**:
    *   Direct DB access (NoSQL or a search index like Elasticsearch).
    *   Internal API providing access to an event feature store.
    *   Data dumps for model training.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: Genres, performers, location, and dates are critical for relevant recommendations.
    *   **Completeness**: Rich descriptions, tags, and performer data improve recommendation quality. Missing key features can degrade performance.
    *   **Freshness**: Stale data (e.g., cancelled events still shown) leads to poor user experience.

### D. Marketing Systems & Operations
*   **Description**: Used for targeted campaigns, newsletters, social media.
*   **Key Fields**:
    *   `event_id`, `title`
    *   `event_details.description_text` (short snippet for promotions)
    *   `event_dates.start_date_utc`, `event_dates.timezone`
    *   `location.venue_name`, `location.city`
    *   `event_details.image_urls` (high-quality promotional images)
    *   `ticketing.ticket_info` (especially `ticket_url`, price, special offers)
    *   `tags/categories`
    *   `source_url` (for "more info" links)
*   **Expected Data Freshness/Latency**:
    *   Medium to High. Data for upcoming campaigns needs to be timely. Latency < 12-24 hours.
*   **Common Data Formats/Access Methods**:
    *   API access.
    *   Curated data dumps (CSV, JSON) for import into marketing platforms.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: Event details (date, time, venue), URLs, and prices must be correct to avoid misleading customers.
    *   **Completeness**: High-quality images, compelling short descriptions, and call-to-action URLs are essential.
    *   **Validity**: All URLs (event, ticket) must be working.

### E. Mobile Application Backend
*   **Description**: Powers a user-facing mobile app for event discovery.
*   **Key Fields**: (Similar to Public API, but tuned for mobile performance)
    *   `event_id`, `title`
    *   Concise `event_details.description_text`
    *   `event_dates.start_date_utc`, `event_dates.end_date_utc` (if applicable), `event_dates.timezone`
    *   `location.venue_name`, `location.city`, `location.coordinates` (for map views/nearby)
    *   `performers` (primary names)
    *   `event_details.image_urls` (optimized for mobile)
    *   `ticketing.ticket_info` (simplified: starting price, main ticket URL, availability)
    *   `tags/categories`
    *   `is_free`, `is_cancelled`
*   **Expected Data Freshness/Latency**:
    *   High. Users expect up-to-date information. Latency < 1-4 hours.
*   **Common Data Formats/Access Methods**:
    *   Optimized REST or GraphQL API.
    *   Backend might use a search index (Elasticsearch, Solr) populated from MongoDB for fast querying.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: Dates, times, locations, cancellation status are paramount.
    *   **Performance**: Geo-coordinates must be accurate for "nearby" features. Image URLs should be valid and load quickly.
    *   **Completeness**: Core information must be present for a good user experience.
    *   **Freshness**: Cancellations or major changes must be reflected quickly.

### F. Third-Party Event Aggregators / Syndication Partners
*   **Description**: External platforms that list events from multiple sources.
*   **Key Fields**:
    *   Often require a comprehensive set of fields, similar to the Public API, but may have their own specific schema mapping requirements.
    *   `event_id`, `title`, `description_text`, `start_date_utc`, `end_date_utc`, `timezone`, `venue_name`, `full_address`, `city`, `country_code`, `coordinates`, `performers`, `image_urls`, `ticket_info`, `source_url`, `organizer_name`, `tags/categories`.
*   **Expected Data Freshness/Latency**:
    *   Medium. Daily or twice-daily updates are common. Latency < 12-24 hours.
*   **Common Data Formats/Access Methods**:
    *   Structured data feeds (XML, JSON, CSV) via FTP, S3, or a dedicated API endpoint.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: High accuracy is needed to maintain partner trust.
    *   **Completeness**: Partners often expect rich data to populate their listings.
    *   **Validity**: URLs (event, ticket, image) must be correct and operational.
    *   **Consistency**: Adherence to the agreed-upon schema and data formats.

### G. Research / Data Science Teams
*   **Description**: Internal teams performing market analysis, trend identification, etc.
*   **Key Fields**:
    *   Potentially all available fields, including raw text (`description_html`, `description_text`), metadata (`scraped_at_utc`, `extraction_method`, `source_platform`), and historical data.
    *   `event_id`, `title`, dates, location details, `performers`, `tags/categories`, `ticket_info` (all tiers, prices), `organizer_name`.
*   **Expected Data Freshness/Latency**:
    *   Low to Medium. Often work with historical snapshots or weekly/monthly data dumps. Real-time data less critical than data completeness and historical depth.
*   **Common Data Formats/Access Methods**:
    *   Bulk data dumps (CSV, Parquet, JSON lines) from MongoDB or a data lake.
    *   Direct query access to a data warehouse or analytical database (could be MongoDB itself or a replica).
*   **Critical Data Quality Aspects**:
    *   **Completeness**: Access to as much raw and structured detail as possible is often valued.
    *   **Accuracy**: Historical accuracy is important for trend analysis.
    *   **Consistency**: Consistent data representation over time is key for longitudinal studies.

### H. Content Generation Systems
*   **Description**: Automated systems creating content (e.g., blog posts, social media updates).
*   **Key Fields**:
    *   `event_id`, `title` (catchy, well-formatted)
    *   `event_details.description_text` (concise, engaging summary)
    *   `event_dates.start_date_utc`, `event_dates.timezone` (formatted for readability)
    *   `location.venue_name`, `location.city`
    *   `performers` (key headliners)
    *   `event_details.image_urls` (high-quality, relevant images)
    *   `ticketing.ticket_info.ticket_url` (call to action)
    *   `tags/categories` (for theming content)
*   **Expected Data Freshness/Latency**:
    *   Medium to High. Content often needs to be timely (e.g., "events this weekend"). Latency < 12-24 hours.
*   **Common Data Formats/Access Methods**:
    *   API access.
    *   Direct database queries.
*   **Critical Data Quality Aspects**:
    *   **Accuracy**: All presented facts (names, dates, venue) must be correct.
    *   **Completeness**: Key promotional elements (title, short description, image, primary artist) are important.
    *   **Validity**: URLs must work. Text fields should be free of HTML tags or excessive special characters if plain text is expected.

By considering these varied requirements, the data pipeline, storage, and delivery mechanisms can be better tailored to serve its consumers effectively.
