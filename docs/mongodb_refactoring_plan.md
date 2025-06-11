# MongoDB Interaction Refactoring Plan

This document proposes specific refactoring changes to standardize and centralize MongoDB interactions across various scrapers and the ETL script. The core of this proposal is the enhancement of `my_scrapers/utils/scraper_utils.py` with a robust, centralized function for saving unified event data.

## 1. Introduction

The goal of this refactoring is to:
*   Ensure all components use a consistent method for saving unified event data to MongoDB.
*   Centralize MongoDB connection configuration by leveraging the global `config.settings` object.
*   Improve maintainability and reliability of database interactions.
*   Standardize on `upsert` operations based on `event_id` for idempotency.

## 2. Proposed `save_unified_events_to_mongodb` Utility

It is proposed to add a new function to `my_scrapers/utils/scraper_utils.py`:

**Function Name**: `save_unified_events_to_mongodb`

**Purpose**: To provide a standardized way for all scrapers to save lists of unified event dictionaries to MongoDB, handling connection, bulk operations, and basic error logging.

### Design and Features:

*   **Parameters**:
    *   `unified_events_list: List[Dict[str, Any]]`: A list of dictionaries, where each dictionary represents a unified event and is expected to have an `event_id` field.
    *   `collection_name_override: Optional[str] = None`: Optional. If provided, this collection name will be used instead of the default from settings.
    *   `logger_obj: Optional[logging.Logger] = None`: An optional logger instance. If not provided, a default logger for the utility will be used.
*   **Configuration**:
    *   Imports the global `settings` object from `config.py`.
    *   Uses `settings.db.mongodb_uri` and `settings.db.mongodb_db_name`.
    *   Uses a default collection name from `settings.db.default_unified_events_collection` (this would need to be added to `DatabaseSettings` in `config.py`), unless overridden by `collection_name_override`.
*   **Connection Handling**:
    *   Establishes a `MongoClient` connection using the URI from settings.
    *   Includes error handling for connection failures.
*   **Database Operations**:
    *   Prepares a list of `UpdateOne` operations for `collection.bulk_write()`.
    *   Each operation will be an **upsert** based on `event_id`. The `_id` field of the MongoDB document will be set to the value of `event_id` from the input dictionary.
        ```python
        # Inside the utility function
        operations = []
        for event_doc in unified_events_list:
            if not event_doc.get("event_id"):
                if logger:
                    logger.warning(f"Event missing 'event_id', cannot prepare for MongoDB: {event_doc.get('title', 'N/A Title')}")
                continue

            # Ensure _id is set to event_id for the document being written
            event_doc_to_save = event_doc.copy() # Avoid modifying original dict in list
            event_doc_to_save["_id"] = event_doc_to_save["event_id"]

            operations.append(
                UpdateOne(
                    {"_id": event_doc_to_save["_id"]},
                    {"$set": event_doc_to_save},
                    upsert=True
                )
            )
        ```
    *   Uses `ordered=False` for `bulk_write` to allow valid operations to succeed even if others in the batch fail.
*   **Return Value**:
    *   A tuple `(upserted_count: int, modified_count: int)` representing the outcome of the `bulk_write` operation.
*   **Logging**:
    *   Logs connection attempts, success, failures, and summary of `bulk_write` results.
*   **Error Handling**:
    *   Includes `try-except` blocks for `ConnectionFailure`, `OperationFailure`, and other potential `pymongo` errors.

### Conceptual Code Snippet for `my_scrapers/utils/scraper_utils.py`:

```python
# At the top of scraper_utils.py
import logging
from typing import List, Dict, Any, Optional, Tuple
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError

# Assuming 'settings' is imported from your central config.py
# from config import settings # This line would be in scraper_utils.py

# Placeholder for settings if config.py is not directly modifiable by this tool
# In real implementation, ensure 'settings' is properly imported.
class FallbackDBSettings:
    mongodb_uri = "mongodb://localhost:27017/"
    mongodb_db_name = "scraper_default_db"
    default_unified_events_collection = "unified_events"
settings_db = getattr(__import__("config", fromlist=["settings"]), "settings", {}).db if "config" in sys.modules else FallbackDBSettings()


def get_default_logger():
    # Basic logger if none provided
    logger = logging.getLogger("scraper_utils_mongodb")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def save_unified_events_to_mongodb(
    unified_events_list: List[Dict[str, Any]],
    collection_name_override: Optional[str] = None,
    logger_obj: Optional[logging.Logger] = None
) -> Tuple[int, int]:

    logger = logger_obj if logger_obj else get_default_logger()

    if not unified_events_list:
        logger.info("No events provided to save.")
        return 0, 0

    db_uri = settings_db.mongodb_uri
    db_name = settings_db.mongodb_db_name
    collection_name = collection_name_override if collection_name_override else settings_db.default_unified_events_collection

    client: Optional[MongoClient] = None
    upserted_count = 0
    modified_count = 0

    try:
        logger.info(f"Connecting to MongoDB: {db_uri} -> DB: {db_name} -> Collection: {collection_name}")
        client = MongoClient(db_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping') # Verify connection
        db = client[db_name]
        collection = db[collection_name]

        operations = []
        for event_doc in unified_events_list:
            if not isinstance(event_doc, dict):
                logger.warning(f"Skipping non-dictionary item: {type(event_doc)}")
                continue

            event_id = event_doc.get("event_id")
            if not event_id:
                logger.warning(f"Event missing 'event_id', cannot prepare for MongoDB: {event_doc.get('title', 'N/A Title')}")
                continue

            doc_to_save = event_doc.copy()
            doc_to_save["_id"] = event_id # Ensure _id is the event_id

            operations.append(
                UpdateOne({"_id": doc_to_save["_id"]}, {"$set": doc_to_save}, upsert=True)
            )

        if not operations:
            logger.info("No valid operations to perform after filtering events.")
            return 0, 0

        logger.info(f"Attempting to bulk_write {len(operations)} operations to collection '{collection_name}'.")
        result = collection.bulk_write(operations, ordered=False)
        upserted_count = result.upserted_count
        modified_count = result.modified_count
        logger.info(
            f"MongoDB bulk_write completed. Upserted: {upserted_count}, Modified: {modified_count}. "
            f"Matched: {result.matched_count}."
        )
        if result.bulk_api_result.get('writeErrors'):
            logger.error(f"MongoDB bulk_write encountered errors: {result.bulk_api_result['writeErrors']}")

    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed for URI {db_uri}: {e}")
    except BulkWriteError as bwe:
        logger.error(f"MongoDB bulk write error: {bwe.details}", exc_info=True)
    except OperationFailure as e:
        logger.error(f"MongoDB operation failure: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"An unexpected error occurred during MongoDB operation: {e}", exc_info=True)
    finally:
        if client:
            client.close()
            logger.debug("MongoDB connection closed.")

    return upserted_count, modified_count
```
*(Note: The `settings_db` placeholder in the snippet above would be replaced by a direct import of `settings` from `config.py` in the actual `scraper_utils.py` file.)*

## 3. Refactoring Plan for Scrapers

**General Changes for each scraper:**
*   Remove any local `MongoClient` instantiation and direct database/collection object management related to saving events.
*   Remove any local implementations of `update_one` or `bulk_write` for saving events.
*   Import `save_unified_events_to_mongodb` from `my_scrapers.utils.scraper_utils`.

### a. `my_scrapers/scraper_ibizaspotlight_playwright_calendar.py`
*   **Current State**: Uses direct `MongoClient` in `main()`, and `events_collection.update_one(..., upsert=True)` in `save_fast()`.
*   **Refactoring Steps**:
    1.  Remove `MongoClient`, `db`, and `events_collection` setup from its `main()` function.
    2.  Remove the `save_fast()` function.
    3.  In `main()`, after `unified_events_list = await scrape_fast(page)` is obtained:
        ```python
        # from my_scrapers.utils.scraper_utils import save_unified_events_to_mongodb
        # logger is already defined in this file

        if unified_events_list:
            save_unified_events_to_mongodb(unified_events_list, logger_obj=logger) # Collection name will use default from settings
        else:
            logger.warning("No events collected or mapped to save.")
        ```

### b. `my_scrapers/scraper_ibizaspotlight_revised_0506_final.py`
*   **Current State**: Uses `get_mongodb_connection()` and `self.db.events.update_one(..., upsert=True)` in `save_event_pw()`. It collects events in `self.all_scraped_events_for_run`.
*   **Refactoring Steps**:
    1.  Remove the `get_mongodb_connection` helper function and `self.db` initialization related to event saving. The `QualityScorer` might still need its own DB access if it's separate.
    2.  Modify `save_event_pw()`: Instead of directly saving to DB, this method should primarily ensure the `unified_event_doc` is correctly formed and appended to `self.all_scraped_events_for_run`.
    3.  At the end of the `run()` method, after the main scraping loop and before file-based saving:
        ```python
        # from my_scrapers.utils.scraper_utils import save_unified_events_to_mongodb
        # self.logger is available in this class

        if self.all_scraped_events_for_run:
            self.logger.info(f"Calling save_unified_events_to_mongodb for {len(self.all_scraped_events_for_run)} events.")
            # The default collection from settings will be used, or pass collection_name_override if needed.
            # e.g. collection_name_override=self.config.collection_name
            save_unified_events_to_mongodb(
                self.all_scraped_events_for_run,
                collection_name_override=self.config.collection_name, # Assuming self.config.collection_name exists
                logger_obj=self.logger
            )
        else:
            self.logger.info("No events were collected in all_scraped_events_for_run to save to MongoDB.")
        ```

### c. `my_scrapers/unified_scraper.py`
*   **Current State**: Initializes `self.mongo_client`, `self.db`, `self.events_collection`. Saves events in `save_event_to_db()` using `self.events_collection.update_one(..., upsert=True)`.
*   **Refactoring Steps**:
    1.  Remove `self.mongo_client`, `self.db`, `self.events_collection` initialization from `__init__`.
    2.  Modify `save_event_to_db()`:
        *   This method is called for single events. The utility expects a list.
        *   It could either:
            *   Call `save_unified_events_to_mongodb([unified_event_doc], logger_obj=logger)`.
            *   Or, similar to `scraper_ibizaspotlight_revised_0506_final.py`, accumulate events in a list and save them in batches or at the end of the `scrape_single_event` or `crawl_calendar` methods. Batching is generally preferred.
        *   Assuming batching at the end of `scrape_single_event` (if it's the main entry) or `crawl_calendar`:
            ```python
            # In crawl_calendar, after collecting all unified_event_docs:
            # all_unified_docs_from_crawl = [...]
            # save_unified_events_to_mongodb(all_unified_docs_from_crawl, logger_obj=logger)

            # If scrape_single_event is the main point:
            # event_id = scraper.scrape_single_event(args.url) -> this should return the doc
            # if unified_doc:
            #    save_unified_events_to_mongodb([unified_doc], logger_obj=logger)
            ```
            The `save_event_to_db` method itself might be removed if events are collected and saved at a higher level in the `main()` or `run()` equivalent. If it's kept for saving single events as they come:
            ```python
            # In IbizaSpotlightUnifiedScraper class, replacing existing save_event_to_db
            # from my_scrapers.utils.scraper_utils import save_unified_events_to_mongodb
            # logger is already defined as self.logger or module logger

            def save_single_unified_event(self, unified_event_doc: Dict[str, Any]):
                if not unified_event_doc or not unified_event_doc.get("event_id"):
                    logger.error("Attempted to save an event with missing data or event_id.")
                    return

                # Default collection from settings will be used.
                # Pass specific collection name if this scraper uses a different one.
                save_unified_events_to_mongodb([unified_event_doc], logger_obj=logger)
            ```

## 4. Recommendation for `database/etl_sqlite_to_mongo.py`

The `etl_sqlite_to_mongo.py` script currently uses its own direct `pymongo.bulk_write()` with `UpdateOne` and `upsert=True`, which is efficient for its purpose of migrating a potentially large dataset.

**Recommendation**:
**Option A: Retain direct `bulk_write` logic but ensure it uses the central `config.settings` for DB parameters.**

*   **Justification**:
    1.  **Specialized Process**: ETL is a distinct, bulk operation. Its current implementation is already optimized for this.
    2.  **Configuration Centralization**: The primary goal is to centralize DB *configuration*. The ETL script should be modified to import `settings` from `config.py` and use `settings.db.mongodb_uri`, `settings.db.mongodb_db_name`, and a specific ETL target collection name (e.g., `settings.db.etl_target_collection` or pass it as a parameter sourced from settings).
    3.  **Clarity**: Keeping the `bulk_write` logic within the ETL script can make the script's specific purpose and operation clearer, as it involves a direct data dump and transformation from one schema to another before fitting the unified schema.
    4.  **`_id` Handling**: The ETL script currently sets `_id` directly from `event_id` *before* creating `UpdateOne` operations. The proposed utility also does this. So, this is not a major differentiator.

*   **Refactoring Steps for `etl_sqlite_to_mongo.py`**:
    1.  Import `settings` from `config.py`.
    2.  Modify `connect_mongo` to use `settings.db.mongodb_uri` and `settings.db.mongodb_db_name` by default, or accept them as parameters derived from `settings`.
    3.  The `MONGO_COLLECTION_NAME` should also be sourced from `settings` (e.g., `settings.db.unified_events_collection` or a specific ETL collection name defined in `config.py`).
    4.  The core `bulk_write` logic can remain largely unchanged as it's already efficient.

While using the new utility is possible, the benefit might be marginal for the ETL script if it's already well-structured for bulk operations and the main goal of configuration centralization is met.

## 5. Benefits of this Refactoring

*   **Consistency**: All scrapers will use a single, well-tested utility for saving data to MongoDB.
*   **Centralized Configuration**: Database connection details (URI, DB name, default collection) are managed in one place (`config.py`).
*   **Improved Maintainability**: Changes to MongoDB interaction logic (e.g., error handling, logging, new `pymongo` features) only need to be updated in `scraper_utils.py`.
*   **Reduced Code Duplication**: Eliminates redundant MongoDB connection and saving logic in multiple scraper files.
*   **Robustness**: The utility function can implement more comprehensive error handling and retry logic for DB operations over time.
