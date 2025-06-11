import sqlite3
import json
from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure
from datetime import datetime, timezone
import logging
import os
import sys

# Adjust sys.path to import from the parent directory (project root)
# Assuming schema_adapter.py is in the project root
# The current script is in database/, so ../ moves to the project root
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from schema_adapter import map_to_unified_schema
except ImportError as e:
    logging.error(f"Failed to import map_to_unified_schema: {e}. Ensure schema_adapter.py is in the project root: {project_root}")
    sys.exit(1)

# --- Configuration ---
# SQLite DB path - assuming the default path used by ventura_crawler.py
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "serpentscale_scraper_data.db")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "ventura_crawler_db")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "unified_events") # Changed collection name

# Logging Setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

def connect_sqlite(db_path: str) -> sqlite3.Connection:
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Access columns by name
        logger.info(f"Successfully connected to SQLite database: {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Error connecting to SQLite database {db_path}: {e}")
        raise

def connect_mongo(uri: str, db_name: str) -> MongoClient:
    """Establishes a connection to MongoDB."""
    try:
        client = MongoClient(uri)
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB: {uri}, database: {db_name}")
        return client
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failed for URI {uri}: {e}")
        raise

def fetch_sqlite_data(conn: sqlite3.Connection) -> list:
    """Fetches all successful scrapes from the scraped_events table."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM scraped_events WHERE success = 1 AND event_data IS NOT NULL")
        rows = cursor.fetchall()
        logger.info(f"Fetched {len(rows)} rows from SQLite table scraped_events.")
        return rows
    except sqlite3.Error as e:
        logger.error(f"Error fetching data from SQLite: {e}")
        raise

def transform_data_for_mongo(sqlite_rows: list) -> list:
    """Transforms SQLite rows into unified MongoDB documents."""
    unified_mongo_documents = []
    etl_time = datetime.now(timezone.utc)

    for row in sqlite_rows:
        url_hash = row['url_hash']
        source_url = row['url']
        try:
            event_data_json = row['event_data']
            if not event_data_json:
                logger.warning(f"Skipping row for url_hash {url_hash} due to empty event_data.")
                continue

            scraped_event_data = json.loads(event_data_json)

            logger.debug(f"Attempting to unify data for url_hash {url_hash} from source {source_url}")
            unified_event_doc = map_to_unified_schema(
                raw_data=scraped_event_data,
                source_platform="ventura_crawler_event", # As specified
                source_url=source_url
            )

            if not unified_event_doc:
                logger.warning(f"Schema unification returned None for url_hash {url_hash}. Skipping.")
                continue

            if 'event_id' not in unified_event_doc or not unified_event_doc['event_id']:
                logger.warning(f"Unified document for url_hash {url_hash} is missing 'event_id'. Skipping.")
                continue

            # Set MongoDB _id to the event_id from the unified schema
            unified_event_doc["_id"] = unified_event_doc["event_id"]

            # Add ETL timestamp
            unified_event_doc["etl_timestamp_utc"] = etl_time

            # Optionally, we can add original source identifiers if not already in unified schema
            # unified_event_doc["source_details"] = {
            #     "original_url_hash": url_hash,
            #     "original_sqlite_title": row['title'],
            #     "extraction_method": row['extraction_method'],
            #     "last_scraped_utc": row['last_scraped_utc'], # Keep as string or parse as needed
            # }

            unified_mongo_documents.append(unified_event_doc)
            logger.debug(f"Successfully unified event data for event_id {unified_event_doc['_id']} (url_hash: {url_hash}).")

        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse event_data JSON for url_hash {url_hash}: {e}. Skipping this record.")
        except Exception as e: # Catch errors from map_to_unified_schema or other issues
            logger.error(f"Error transforming/unifying row for url_hash {url_hash}: {e}", exc_info=True)
            # Optionally, store errors or problematic raw_data for later analysis

    logger.info(f"Transformed and unified {len(unified_mongo_documents)} documents for MongoDB.")
    return unified_mongo_documents

def load_data_to_mongo(mongo_client: MongoClient, db_name: str, collection_name: str, documents: list, batch_size: int = 500):
    """Loads documents into MongoDB using bulk upserts."""
    if not documents:
        logger.info("No documents to load into MongoDB.")
        return 0

    db = mongo_client[db_name]
    collection = db[collection_name]

    operations = []
    for doc in documents:
        # Using doc['_id'] which is now unified_event_doc['event_id']
        operations.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True)
        )

    total_upserted = 0
    if not operations: # Double check if any valid operations were created
        logger.info("No valid operations to perform for MongoDB.")
        return 0

    try:
        for i in range(0, len(operations), batch_size):
            batch = operations[i:i + batch_size]
            result = collection.bulk_write(batch)
            upserted_count = result.upserted_count + result.modified_count
            total_upserted += upserted_count
            logger.info(f"Bulk write successful: Upserted/Modified {upserted_count} documents in this batch.")
        logger.info(f"Successfully loaded/updated {total_upserted} documents into MongoDB collection '{collection_name}'.")
        return total_upserted
    except OperationFailure as e:
        logger.error(f"MongoDB bulk write operation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during MongoDB bulk write: {e}")
        raise

def main_etl_process():
    logger.info("Starting ETL process from SQLite to MongoDB with Schema Unification...")

    sqlite_conn = None
    mongo_client = None

    try:
        # Extract
        sqlite_conn = connect_sqlite(SQLITE_DB_PATH)
        sqlite_data = fetch_sqlite_data(sqlite_conn)

        if not sqlite_data:
            logger.info("No data found in SQLite to process. Exiting.")
            return

        # Transform and Unify
        mongo_documents = transform_data_for_mongo(sqlite_data)

        if not mongo_documents:
            logger.info("No data transformed/unified for MongoDB. Exiting.")
            return

        # Load
        mongo_client = connect_mongo(MONGO_URI, MONGO_DATABASE_NAME)
        # Changed collection name to reflect unified data
        load_data_to_mongo(mongo_client, MONGO_DATABASE_NAME, MONGO_COLLECTION_NAME, mongo_documents)

        logger.info("ETL process with schema unification completed successfully.")

    except sqlite3.Error as e:
        logger.error(f"ETL process failed due to SQLite error: {e}")
    except ConnectionFailure as e:
        logger.error(f"ETL process failed due to MongoDB connection error: {e}")
    except OperationFailure as e:
        logger.error(f"ETL process failed due to MongoDB operation error: {e}")
    except ImportError as e: # Catch import error if it wasn't caught at the top
        logger.error(f"ETL process failed due to import error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during the ETL process: {e}", exc_info=True)
    finally:
        if sqlite_conn:
            sqlite_conn.close()
            logger.info("SQLite connection closed.")
        if mongo_client:
            mongo_client.close()
            logger.info("MongoDB connection closed.")

if __name__ == "__main__":
    if not os.path.exists(SQLITE_DB_PATH):
        logger.error(f"SQLite database file not found at {SQLITE_DB_PATH}. Ensure `ventura_crawler.py` has run and created the database.")
        logger.error("If the database is in a different location, set the SQLITE_DB_PATH environment variable.")
    else:
        # Check if map_to_unified_schema is available before running
        if 'map_to_unified_schema' not in globals():
            logger.error("map_to_unified_schema function not available. Cannot proceed with ETL.")
        else:
            main_etl_process()

```
