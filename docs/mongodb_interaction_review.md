# MongoDB Interaction Patterns Review

This document reviews how different scraper and ETL scripts in the project interact with MongoDB, focusing on connection methods, saving mechanisms, and specific database operations.

## 1. `database/etl_sqlite_to_mongo.py`

*   **MongoDB Connection**:
    *   Connection is established in the `connect_mongo` function using `pymongo.MongoClient(uri)`.
    *   The MongoDB URI (`MONGO_URI`), database name (`MONGO_DATABASE_NAME`), and collection name (`MONGO_COLLECTION_NAME`) are sourced from environment variables via `os.getenv()`, with local defaults provided (e.g., "mongodb://localhost:27017/", "ventura_crawler_db", "unified_events").
*   **Saving Mechanism**:
    *   Uses direct `pymongo` calls.
    *   It does **not** use the `scraper_utils.save_to_mongodb` utility.
*   **Database Operations**:
    *   The `load_data_to_mongo` function performs data loading.
    *   It uses `collection.bulk_write()` with a list of `UpdateOne` operations.
    *   Each `UpdateOne` operation is configured as an **upsert**: `UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)`. The `_id` is derived from the `event_id` in the unified schema.
*   **Error Handling**:
    *   Catches `pymongo.errors.ConnectionFailure` during connection.
    *   Catches `pymongo.errors.OperationFailure` during bulk write operations.

## 2. `my_scrapers/classy_clubtickets_nav_scraper.py`

*   **MongoDB Connection**:
    *   MongoDB URI, database name, and collection name are defined in the `DEFAULT_CONFIG` dictionary (e.g., `"mongodb_uri": "mongodb://localhost:27017/"`, `"db_name": "clubtickets_test_db"`). These can be overridden by `config_overrides` passed to the constructor.
    *   The actual connection is handled by the `save_to_mongodb` utility.
*   **Saving Mechanism**:
    *   Uses the `save_to_mongodb` function imported from `my_scrapers.utils.scraper_utils`.
*   **Database Operations**:
    *   The specific operations (insert, upsert) are encapsulated within the `save_to_mongodb` utility. (Review of `scraper_utils.py` would be needed to determine the exact operation, but it typically performs upserts based on an `event_id` or a combination of unique fields).
*   **Error Handling**:
    *   Error handling related to MongoDB operations is managed within the `save_to_mongodb` utility.

## 3. `my_scrapers/mono_ticketmaster.py`

*   **MongoDB Connection**:
    *   The `MultiLayerEventScraper` class initializes `self.mongo_client`, `self.db`, and `self.events_collection` in its constructor using `pymongo.MongoClient()`.
    *   Connection parameters (`mongodb_uri`, `db_name`, `collection_name`) are taken from its local `ScraperConfig` dataclass. This dataclass attempts to load `settings.MONGODB_URI` (from `classy_skkkrapey.config`) or falls back to `os.getenv("MONGODB_URI_FALLBACK", ...)` and then hardcoded defaults.
*   **Saving Mechanism**:
    *   The `save_event_pw` method calls the `save_to_mongodb` utility function imported from `my_scrapers.utils.scraper_utils`.
*   **Database Operations**:
    *   Similar to `classy_clubtickets_nav_scraper.py`, the database operations are encapsulated within the `save_to_mongodb` utility.
*   **Error Handling**:
    *   Catches `ConnectionFailure` during initial `MongoClient` setup.
    *   Further error handling is within the `save_to_mongodb` utility.

## 4. `my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`

*   **MongoDB Connection**:
    *   In the `main()` function, `MongoClient(settings.MONGODB_URI)` is used to connect.
    *   Database and collection objects are obtained: `db = mongo_client[settings.DB_NAME]`, `events_collection = db.events`.
    *   It attempts to import `settings` from `classy_skkkrapey.config` and has a `DummySettings` fallback if the import fails, which includes a default MongoDB URI. The collection name `events` is hardcoded in this context.
*   **Saving Mechanism**:
    *   Uses direct `pymongo` calls within the `save_fast()` function.
*   **Database Operations**:
    *   Performs an **upsert** operation: `events_collection.update_one(update_key, {"$set": unified_event_doc}, upsert=True)`, where `update_key` is `{"event_id": unified_event_doc["event_id"]}`.
*   **Error Handling**:
    *   Catches `ConnectionFailure` during initial `MongoClient` setup.
    *   The `save_fast` function logs errors during individual `update_one` operations but continues processing other events.

## 5. `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`

*   **MongoDB Connection**:
    *   Uses a helper function `get_mongodb_connection` which takes `mongodb_uri` and `db_name` (from `self.config: ScraperConfig`) and returns a `pymongo.database.Database` object. `ScraperConfig` gets these values from `os.getenv` or defaults.
    *   The `IbizaSpotlightScraper` class initializes `self.db` using this function.
*   **Saving Mechanism**:
    *   The `save_event_pw` method uses direct `pymongo` calls.
*   **Database Operations**:
    *   Performs an **upsert** operation: `self.db.events.update_one(update_key, {"$set": unified_event_doc}, upsert=True)`.
    *   The `update_key` is `{"event_id": unified_event_doc["event_id"]}`.
    *   The collection name `events` is directly used (e.g., `self.db.events`).
*   **Error Handling**:
    *   `get_mongodb_connection` catches `pymongo.errors.ConnectionFailure` and general exceptions during connection, with retries.
    *   `save_event_pw` has a general try-except block for errors during saving.

## 6. `my_scrapers/unified_scraper.py`

*   **MongoDB Connection**:
    *   The `IbizaSpotlightUnifiedScraper` class initializes `self.mongo_client`, `self.db`, and `self.events_collection` in its constructor using `pymongo.MongoClient()`.
    *   It attempts to use `settings.MONGODB_URI` and `settings.DB_NAME` from `classy_skkkrapey.config`.
    *   The collection name `events` is hardcoded when getting the collection: `self.events_collection = self.db.events`.
*   **Saving Mechanism**:
    *   The `save_event_to_db()` method uses direct `pymongo` calls.
*   **Database Operations**:
    *   Performs an **upsert** operation: `self.events_collection.update_one(update_key, {"$set": unified_event_doc}, upsert=True)`.
    *   The `update_key` is `{"event_id": unified_event_doc["event_id"]}`.
*   **Error Handling**:
    *   Catches `ConnectionFailure` during initial `MongoClient` setup.
    *   `save_event_to_db` has a general try-except block.

## Summary of Patterns

*   **Connection Source**:
    *   Most scrapers attempt to use a central `settings` object (either `config.settings` or `classy_skkkrapey.config.settings`) for MongoDB URI and database name. Fallbacks to `os.getenv` or hardcoded defaults exist in some older/transitional versions.
    *   `etl_sqlite_to_mongo.py` uses `os.getenv` directly with defaults.
    *   `classy_clubtickets_nav_scraper.py` uses its internal `DEFAULT_CONFIG`.
*   **Saving Mechanism**:
    *   A mix of direct `pymongo` calls and the `scraper_utils.save_to_mongodb` utility.
    *   Scrapers using `save_to_mongodb`: `classy_clubtickets_nav_scraper.py`, `mono_ticketmaster.py`.
    *   Scrapers/scripts using direct `pymongo`: `etl_sqlite_to_mongo.py`, `scraper_ibizaspotlight_playwright_calendar.py`, `scraper_ibizaspotlight_revised_0506_final.py`, `unified_scraper.py`.
*   **Database Operations**:
    *   The predominant operation for saving event data is **upsert** (`update_one` with `upsert=True` or `bulk_write` with `UpdateOne` upserts). This is good for idempotency, using `event_id` from the unified schema as the primary key for the upsert.
*   **Collection Names**:
    *   Often hardcoded as `"events"` when direct `pymongo` is used (e.g., `self.db.events` or `db.events`).
    *   Configurable in `etl_sqlite_to_mongo.py` (via `MONGO_COLLECTION_NAME` env var, defaults to "unified_events").
    *   Configurable via `DEFAULT_CONFIG` in `classy_clubtickets_nav_scraper.py`.
    *   Configurable via `ScraperConfig` in `mono_ticketmaster.py`.
*   **Error Handling**: Basic `ConnectionFailure` and `OperationFailure` (or general exceptions) are usually handled around connection and write operations.

**Recommendations based on review**:
*   Standardize connection handling to use the central `config.settings` object for all MongoDB parameters (URI, DB name, collection names).
*   Decide whether to universally adopt the `scraper_utils.save_to_mongodb` utility or have each scraper manage its own `pymongo` calls. Using the utility could provide a consistent way to handle operations and error logging for DB writes. If direct calls are preferred, ensure they are robust.
*   Ensure all MongoDB write operations continue to use upserts based on `event_id` to maintain idempotency and prevent duplicate data.
*   Make collection names consistently configurable via the central `config.settings`, rather than being hardcoded in some scripts.
