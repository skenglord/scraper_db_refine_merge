import logging
import json
import csv
import datetime
import pymongo
from pathlib import Path
from typing import List, Dict

def setup_logger(logger_name: str, log_file_name_prefix: str, log_dir: str = "scraper_logs"):
    """
    Configures and returns a logger that outputs to console and a file.
    Ensures the specified configuration is applied.
    """
    # Create log directory if it doesn't exist at the project root
    project_root_log_dir = Path(".") / log_dir
    project_root_log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels of messages

    # Clear existing handlers to ensure new configuration is applied
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
    )

    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)  # Console logs INFO and above
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File Handler
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_name = f"{log_file_name_prefix}_{timestamp}.log"
    fh = logging.FileHandler(project_root_log_dir / log_file_name)
    fh.setLevel(logging.DEBUG)  # File logs DEBUG and above
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # logger.info(f"Logger '{logger_name}' re-initialized. Logging to: {project_root_log_dir / log_file_name}")
    return logger

def save_to_mongodb(data_list: List[Dict], mongodb_uri: str, db_name: str, collection_name: str, logger_obj: logging.Logger):
    """
    Saves a list of event dictionaries to MongoDB using upsert.
    Assumes data_list contains dictionaries already mapped to the unified schema
    and each dictionary has a unique 'event_id' for upserting.
    """
    if not data_list:
        logger_obj.info("No data provided to save_to_mongodb.")
        return

    try:
        logger_obj.info(f"Connecting to MongoDB: DB '{db_name}', Collection '{collection_name}'.") # URI logged separately or omitted for brevity/security
        client = pymongo.MongoClient(mongodb_uri)
        db = client[db_name]
        collection = db[collection_name]

        upserted_count = 0
        modified_count = 0

        processed_records = 0
        for record in data_list:
            if not isinstance(record, dict):
                logger_obj.warning(f"Skipping non-dictionary record: {type(record)}")
                continue

            if 'event_id' not in record or not record['event_id']: # Ensure event_id exists and is not empty
                logger_obj.warning(f"Skipping record without valid 'event_id': {record.get('title', 'N/A')}")
                continue

            query = {"event_id": record["event_id"]}
            update = {"$set": record}
            result = collection.update_one(query, update, upsert=True)

            if result.upserted_id:
                upserted_count += 1
            elif result.modified_count > 0:
                modified_count += 1
            processed_records +=1

        logger_obj.info(f"MongoDB: Processed {processed_records} records for collection '{collection_name}'. "
                        f"Upserted {upserted_count} new records, modified {modified_count} existing records.")

    except pymongo.errors.ConnectionFailure as e:
        logger_obj.error(f"MongoDB connection failed (URI: {mongodb_uri}): {e}", exc_info=True)
    except pymongo.errors.ConfigurationError as e: # Specific error for config issues
        logger_obj.error(f"MongoDB configuration error (URI: {mongodb_uri}): {e}", exc_info=True)
    except pymongo.errors.PyMongoError as e: # Catch other pymongo specific errors
        logger_obj.error(f"A PyMongo error occurred with MongoDB (Collection: {collection_name}): {e}", exc_info=True)
    except Exception as e: # Catch any other exception
        logger_obj.error(f"An unexpected error occurred in save_to_mongodb (Collection: {collection_name}): {e}", exc_info=True)
    finally:
        if 'client' in locals() and client:
            try:
                client.close()
                logger_obj.debug("MongoDB connection closed.")
            except Exception as e:
                logger_obj.error(f"Error closing MongoDB connection: {e}", exc_info=True)


def _ensure_output_dir_exists(output_dir: str, logger_obj: logging.Logger = None):
    """Helper to create output directory at project root if it doesn't exist."""
    # Ensures output_dir is relative to project root e.g. "output" or "data/json_files"
    project_root_output_dir = Path(".") / output_dir
    try:
        project_root_output_dir.mkdir(parents=True, exist_ok=True)
        return project_root_output_dir
    except Exception as e:
        msg = f"Could not create output directory {project_root_output_dir}: {e}"
        if logger_obj:
            logger_obj.error(msg, exc_info=True)
        else:
            print(f"Error: {msg}")
        raise # Re-raise the exception if directory creation fails

def save_to_json_file(data_list: List[Dict], filename_prefix: str, output_dir: str = "output", logger_obj: logging.Logger = None):
    """Saves a list of dictionaries to a JSON file in the specified project output directory."""
    if not data_list:
        if logger_obj: logger_obj.info(f"No data provided to save_to_json_file for prefix '{filename_prefix}'.")
        else: print(f"No data to save to JSON for '{filename_prefix}'.")
        return

    try:
        output_path = _ensure_output_dir_exists(output_dir, logger_obj)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_path / f"{filename_prefix}_{timestamp}.json"

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=4, ensure_ascii=False, default=str) # default=str for datetime etc.
        msg = f"Data saved to JSON file: {filename}"
        if logger_obj: logger_obj.info(msg)
        else: print(msg)
    except IOError as e: # More specific for file I/O issues
        err_msg = f"IOError saving data to JSON file {filename}: {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)
    except Exception as e: # Catch-all for other unexpected errors
        err_msg = f"An unexpected error occurred in save_to_json_file ({filename}): {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)


def save_to_csv_file(data_list: List[Dict], filename_prefix: str, output_dir: str = "output", logger_obj: logging.Logger = None):
    """Saves a list of dictionaries to a CSV file in the specified project output directory."""
    if not data_list:
        if logger_obj: logger_obj.info(f"No data provided to save_to_csv_file for prefix '{filename_prefix}'.")
        else: print(f"No data to save to CSV for '{filename_prefix}'.")
        return

    # Filter for actual dictionaries and ensure all items for CSV are simple types or serialized
    dict_list = []
    for item in data_list:
        if isinstance(item, dict):
            processed_item = {}
            for k, v in item.items():
                if isinstance(v, (list, dict)): # Serialize complex types
                    try:
                        processed_item[k] = json.dumps(v, default=str)
                    except TypeError:
                        processed_item[k] = str(v) # Fallback for un-serializable complex types
                elif isinstance(v, datetime.datetime) or isinstance(v, datetime.date):
                    processed_item[k] = v.isoformat()
                else:
                    processed_item[k] = v
            dict_list.append(processed_item)
        else:
            if logger_obj: logger_obj.warning(f"Non-dictionary item skipped for CSV conversion: {type(item)}")

    if not dict_list:
        if logger_obj: logger_obj.info(f"No dictionary data available to save to CSV for prefix '{filename_prefix}' after processing.")
        else: print(f"No dictionary data for CSV for '{filename_prefix}' after processing.")
        return

    try:
        output_path = _ensure_output_dir_exists(output_dir, logger_obj)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_path / f"{filename_prefix}_{timestamp}.csv"

        headers = list(dict_list[0].keys()) # Get headers from the first processed dictionary
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers, extrasaction='ignore') # extrasaction='ignore' is safer
            writer.writeheader()
            writer.writerows(dict_list)
        msg = f"Data saved to CSV file: {filename}"
        if logger_obj: logger_obj.info(msg)
        else: print(msg)
    except IOError as e:
        err_msg = f"IOError saving data to CSV file {filename}: {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)
    except Exception as e:
        err_msg = f"An unexpected error occurred in save_to_csv_file ({filename}): {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)


def save_to_markdown_file(data_list: List[Dict], filename_prefix: str, output_dir: str = "output", logger_obj: logging.Logger = None):
    """Converts event data to a Markdown report and saves it to the specified project output directory."""
    if not data_list:
        if logger_obj: logger_obj.info(f"No data provided to save_to_markdown_file for prefix '{filename_prefix}'.")
        else: print(f"No data to save to Markdown for '{filename_prefix}'.")
        return

    try:
        output_path = _ensure_output_dir_exists(output_dir, logger_obj)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = output_path / f"{filename_prefix}_{timestamp}.md"

        md_content = f"# Event Report - {filename_prefix}\n\n"
        md_content += f"*Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n"
        md_content += f"*Total events processed: {len(data_list)}*\n\n"

        actual_events_count = 0
        for i, event in enumerate(data_list):
            if not isinstance(event, dict):
                md_content += f"## Entry {i+1} (Skipped)\n\n"
                md_content += f"- Item was not a dictionary (type: {type(event)}).\n\n---\n\n"
                if logger_obj: logger_obj.warning(f"Skipping non-dictionary item in save_to_markdown_file: {type(event)}")
                continue

            actual_events_count +=1
            md_content += f"## Event {actual_events_count}: {event.get('title', event.get('name', 'N/A'))}\n\n"
            for key, value in event.items():
                # Sanitize key for Markdown, make it title case
                md_key = key.replace('_', ' ').title()
                if isinstance(value, (list, dict)):
                    try:
                        # Pretty print JSON for complex types
                        val_str = json.dumps(value, indent=2, ensure_ascii=False, default=str)
                        md_content += f"- **{md_key}:**\n  ```json\n{val_str}\n  ```\n"
                    except TypeError: # Fallback for un-serializable complex types
                        md_content += f"- **{md_key}:** `{str(value)}`\n"
                elif isinstance(value, str) and ('\n' in value or len(value) > 100) : # Handle multiline strings or long strings
                     md_content += f"- **{md_key}:**\n  ```text\n{value}\n  ```\n"
                else:
                    md_content += f"- **{md_key}:** {value}\n"
            md_content += "\n---\n\n"

        if actual_events_count == 0 and data_list: # If all items were skipped
             md_content += "No valid event data found to report after filtering.\n"
        elif actual_events_count > 0 :
            md_content = md_content.replace(f"*Total events processed: {len(data_list)}*", f"*Total valid events reported: {actual_events_count}* (out of {len(data_list)} processed)")


        with open(filename, 'w', encoding='utf-8') as f:
            f.write(md_content)
        msg = f"Markdown report saved to: {filename}"
        if logger_obj: logger_obj.info(msg)
        else: print(msg)
    except IOError as e:
        err_msg = f"IOError saving Markdown report to {filename}: {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)
    except Exception as e:
        err_msg = f"An unexpected error occurred in save_to_markdown_file ({filename}): {e}"
        if logger_obj: logger_obj.error(err_msg, exc_info=True)
        else: print(err_msg)


# Example usage block from the original file (good for direct testing)
if __name__ == '__main__':
    # Test logger setup (will create logs in ./scraper_logs/)
    test_logger = setup_logger("ScraperUtilsLiveTest", "utils_direct_test")
    test_logger.info("Logger setup for direct testing of scraper_utils.")

    sample_events = [
        {"event_id": "evt001", "title": "Summer Fest 2024", "date": "2024-07-20", "location": "Central Park", "artists": ["DJ Alpha", "Band Beta"], "price_range": {"min": 25, "max": 75}, "description": "A great summer music festival.\nLineup subject to change."},
        {"event_id": "evt002", "title": "Art Expo", "date": "2024-09-15", "location": "City Gallery", "details": "Featuring modern and contemporary art.", "entry_fee": 15},
        {"event_id": "evt003", "title": "Tech Conference", "date": "2024-10-05", "location": "Convention Center", "speakers": ["Dr. Info", "Mr. Byte"], "topics": ["AI", "Blockchain", "IoT"], "website": "http://techconf.example.com"},
        "this is not a dict and should be skipped by some savers", # Test invalid data type
        {"event_id": "", "title": "Event with empty ID", "date": "2024-11-01"}, # Test empty event_id for MongoDB
        {"no_event_id": "event_no_id_key", "title": "Event without event_id key", "date": "2024-11-05"} # Test missing event_id for MongoDB
    ]

    # Test output directory (will create ./output/ relative to where script is run)
    test_output_dir = "output_utils_test" # Keep test outputs separate

    # Test JSON output
    save_to_json_file(sample_events, "sample_event_data_json", output_dir=test_output_dir, logger_obj=test_logger)
    save_to_json_file([], "empty_data_json", output_dir=test_output_dir, logger_obj=test_logger)


    # Test CSV output
    save_to_csv_file(sample_events, "sample_event_data_csv", output_dir=test_output_dir, logger_obj=test_logger)
    save_to_csv_file([], "empty_data_csv", output_dir=test_output_dir, logger_obj=test_logger)

    # Test Markdown output
    save_to_markdown_file(sample_events, "sample_event_report_md", output_dir=test_output_dir, logger_obj=test_logger)
    save_to_markdown_file([], "empty_report_md", output_dir=test_output_dir, logger_obj=test_logger)

    # Test MongoDB output (Requires a MongoDB instance)
    # IMPORTANT: Replace with your actual test MongoDB URI if you want to run this part
    MONGODB_URI_TEST = "mongodb://localhost:27017/" # Placeholder, use a real one for testing
    DB_NAME_TEST = "scraper_utils_test_db"
    COLLECTION_NAME_TEST = "events_output_test_collection"

    test_logger.info(f"Attempting to save to MongoDB. Ensure MongoDB is running at {MONGODB_URI_TEST} and accessible.")
    # Note: For this test to run, `sample_events` should ideally be mapped via a schema_adapter first if your `event_id` logic depends on it.
    # The current save_to_mongodb handles records missing 'event_id' by logging a warning and skipping.
    save_to_mongodb(sample_events, MONGODB_URI_TEST, DB_NAME_TEST, COLLECTION_NAME_TEST, test_logger)
    save_to_mongodb([], MONGODB_URI_TEST, DB_NAME_TEST, COLLECTION_NAME_TEST, test_logger) # Test empty list

    test_logger.info(f"Scraper utils direct testing finished. Check '{test_output_dir}/' and 'scraper_logs/' directories for outputs.")
    print(f"Finished running utility tests. Check '{Path(test_output_dir).resolve()}/' and '{Path('scraper_logs').resolve()}/' directories.")
    print(f"Note: For MongoDB tests to fully pass, ensure MongoDB is running at {MONGODB_URI_TEST} and accessible, and the test DB has permissions.")
    print("If you ran this from a different directory, the output paths will be relative to that CWD.")

    # To clean up test files (optional):
    # import shutil
    # if Path(test_output_dir).exists():
    #     shutil.rmtree(test_output_dir)
    #     test_logger.info(f"Cleaned up test output directory: {test_output_dir}")
    # # Log files are timestamped, so they won't be overwritten. Manual cleanup for scraper_logs if needed.
