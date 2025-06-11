import logging
import json
import csv
from datetime import datetime, date, time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Union

from pymongo import MongoClient, UpdateOne
from pymongo.errors import ConnectionFailure, OperationFailure, BulkWriteError

# Import settings from the new config location
try:
    from scrapers_v2.config import settings
    from scrapers_v2.schema_adapter import UnifiedEvent # Assuming UnifiedEvent is Pydantic model
except ImportError as e:
    # Fallback for environments where scrapers_v2 might not be in PYTHONPATH immediately
    # This is more for agent's direct execution context.
    # In a structured project, direct import should work.
    logging.basicConfig(level=logging.DEBUG) # Ensure logging is configured for this fallback
    logger_fallback = logging.getLogger("utils_fallback_import")
    logger_fallback.error(f"Critical Import Error in utils.py: {e}. Using placeholder settings/schemas. This may affect functionality.")

    # Define minimal placeholder settings if import fails
    class FallbackMongoSettings:
        uri = "mongodb://localhost:27017/"
        database = "fallback_scraper_db"
        default_unified_collection = "fallback_unified_events"
    class FallbackFileOutputSettings:
        base_output_directory = Path("fallback_output_data")
        log_output_directory = Path("fallback_scraper_logs")
        enable_json_output = False
        enable_csv_output = False
        enable_markdown_output = False
    class FallbackSettings:
        mongodb = FallbackMongoSettings()
        file_outputs = FallbackFileOutputSettings()
    settings = FallbackSettings() # type: ignore

    class UnifiedEvent: # Dummy class if import fails
        def __init__(self, **kwargs): self.__dict__.update(kwargs)
        def model_dump(self, **kwargs) -> Dict[str, Any]: return self.__dict__


# --- Logger Setup ---
_loggers: Dict[str, logging.Logger] = {}

def setup_logger(logger_name: str, log_file_prefix: str, level: int = logging.INFO) -> logging.Logger:
    """Configures and returns a logger that outputs to console and a timestamped file."""
    if logger_name in _loggers:
        return _loggers[logger_name]

    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    logger.propagate = False # Prevent duplicate logs if root logger is also configured

    # Clear existing handlers for this specific logger to avoid duplication on re-call
    if logger.hasHandlers():
        logger.handlers.clear()

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s"
    )

    # Console Handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File Handler
    log_dir = settings.file_outputs.log_output_directory
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = log_dir / f"{log_file_prefix}_{timestamp}.log"
        fh = logging.FileHandler(log_file_path)
        fh.setLevel(level) # Or logging.DEBUG for more verbose file logs
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except Exception as e:
        logger.error(f"Failed to create file handler for logger {logger_name} at {log_dir}: {e}", exc_info=True)


    _loggers[logger_name] = logger
    logger.info(f"Logger '{logger_name}' initialized. Logging to console and file (if path valid).")
    return logger

# Initialize a default logger for utilities if no specific logger is passed
default_utils_logger = setup_logger("scrapers_v2.utils", "utils_default_log")


# --- MongoDB Utility ---
def save_unified_events_to_mongodb(
    events: List[UnifiedEvent],
    collection_name_override: Optional[str] = None,
    logger_obj: Optional[logging.Logger] = None
) -> Tuple[int, int]:
    """
    Saves a list of UnifiedEvent Pydantic objects to MongoDB using bulk upserts.
    Uses settings from scrapers_v2.config.settings.
    """
    current_logger = logger_obj or default_utils_logger

    if not events:
        current_logger.info("No events provided to save_unified_events_to_mongodb.")
        return 0, 0

    db_uri = settings.mongodb.uri
    db_name = settings.mongodb.database
    collection_name = collection_name_override or settings.mongodb.default_unified_collection

    client: Optional[MongoClient] = None
    upserted_count = 0
    modified_count = 0

    try:
        current_logger.info(f"Connecting to MongoDB: URI='{db_uri}', DB='{db_name}', Collection='{collection_name}'")
        client = MongoClient(db_uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=20000)
        client.admin.command('ping') # Verify connection
        db = client[db_name]
        collection = db[collection_name]

        operations = []
        for event_obj in events:
            if not isinstance(event_obj, UnifiedEvent): # Check type
                current_logger.warning(f"Skipping non-UnifiedEvent item: {type(event_obj)}")
                continue

            # Convert Pydantic model to dict. by_alias=True can be useful if your Pydantic
            # field names differ from desired DB field names and you use aliases.
            # For _id handling, ensure event_id is used.
            event_dict = event_obj.model_dump(exclude_none=True) # exclude_none is often good for DBs

            if "event_id" not in event_dict or not event_dict["event_id"]:
                current_logger.warning(f"UnifiedEvent missing 'event_id': {event_dict.get('event_details',{}).get('title', 'N/A Title')}. Skipping.")
                continue

            # Use event_id as MongoDB's _id
            event_dict["_id"] = event_dict["event_id"]

            operations.append(
                UpdateOne({"_id": event_dict["_id"]}, {"$set": event_dict}, upsert=True)
            )

        if not operations:
            current_logger.info("No valid operations to perform after filtering events.")
            return 0, 0

        current_logger.info(f"Attempting to bulk_write {len(operations)} operations to collection '{collection_name}'.")
        result = collection.bulk_write(operations, ordered=False)

        upserted_count = result.upserted_count if result else 0
        modified_count = result.modified_count if result else 0
        matched_count = result.matched_count if result else 0

        current_logger.info(
            f"MongoDB bulk_write completed for collection '{collection_name}'. "
            f"Upserted: {upserted_count}, Modified: {modified_count}, Matched: {matched_count}."
        )
        if result and result.bulk_api_result.get('writeErrors'):
            current_logger.error(f"MongoDB bulk_write encountered errors: {result.bulk_api_result['writeErrors']}")

    except ConnectionFailure as e:
        current_logger.error(f"MongoDB connection failed for URI {db_uri}: {e}", exc_info=True)
    except BulkWriteError as bwe:
        current_logger.error(f"MongoDB bulk_write error to collection '{collection_name}': {bwe.details}", exc_info=True)
    except OperationFailure as e:
        current_logger.error(f"MongoDB operation failure on collection '{collection_name}': {e}", exc_info=True)
    except Exception as e:
        current_logger.error(f"An unexpected error occurred during MongoDB operation on collection '{collection_name}': {e}", exc_info=True)
    finally:
        if client:
            try:
                client.close()
                current_logger.debug("MongoDB connection closed.")
            except Exception as e_close:
                 current_logger.error(f"Error closing MongoDB connection: {e_close}", exc_info=True)

    return upserted_count, modified_count


# --- File Output Utilities ---

def _ensure_output_dir_exists(base_dir_path: Path, sub_folder_name: Optional[str], logger_obj: logging.Logger) -> Optional[Path]:
    """Helper to create output directory: base_dir_path / sub_folder_name."""
    try:
        target_dir = base_dir_path
        if sub_folder_name:
            target_dir = base_dir_path / sub_folder_name

        target_dir.mkdir(parents=True, exist_ok=True)
        logger_obj.debug(f"Ensured output directory exists: {target_dir}")
        return target_dir
    except Exception as e:
        logger_obj.error(f"Could not create output directory {base_dir_path / (sub_folder_name or '')}: {e}", exc_info=True)
        return None

def _serialize_item(item: Any) -> Any:
    """Helper to serialize complex types within data for file outputs."""
    if isinstance(item, (datetime, date, dt_time)):
        return item.isoformat()
    if isinstance(item, Path):
        return str(item)
    if isinstance(item, (list, dict, tuple)):
        try: # Try to json dump complex types, useful for CSV cells
            return json.dumps(item, default=_serialize_item)
        except TypeError:
            return str(item) # Fallback to string if still not serializable
    return item


def save_to_json_file(
    data_to_save: Union[List[Dict[str, Any]], Dict[str, Any]],
    filename_prefix: str,
    sub_folder: Optional[str] = None,
    logger_obj: Optional[logging.Logger] = None
):
    current_logger = logger_obj or default_utils_logger
    if not settings.file_outputs.enable_json_output:
        current_logger.debug(f"JSON output disabled globally. Skipping save for '{filename_prefix}'.")
        return

    if not data_to_save:
        current_logger.info(f"No data provided to save_to_json_file for prefix '{filename_prefix}'.")
        return

    actual_sub_folder = sub_folder if sub_folder else filename_prefix # Default sub_folder to prefix
    output_path = _ensure_output_dir_exists(settings.file_outputs.base_output_directory, actual_sub_folder, current_logger)
    if not output_path:
        return # Error already logged by _ensure_output_dir_exists

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_path / f"{filename_prefix}_{timestamp}.json"

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False, default=_serialize_item)
        current_logger.info(f"Data successfully saved to JSON file: {filepath}")
    except IOError as e:
        current_logger.error(f"IOError saving data to JSON file {filepath}: {e}", exc_info=True)
    except Exception as e:
        current_logger.error(f"An unexpected error occurred in save_to_json_file ({filepath}): {e}", exc_info=True)


def save_to_csv_file(
    data_to_save: List[Dict[str, Any]],
    filename_prefix: str,
    sub_folder: Optional[str] = None,
    logger_obj: Optional[logging.Logger] = None
):
    current_logger = logger_obj or default_utils_logger
    if not settings.file_outputs.enable_csv_output:
        current_logger.debug(f"CSV output disabled globally. Skipping save for '{filename_prefix}'.")
        return

    if not data_to_save:
        current_logger.info(f"No data provided to save_to_csv_file for prefix '{filename_prefix}'.")
        return

    processed_list = []
    all_headers = set()
    for item in data_to_save:
        if isinstance(item, dict):
            processed_item = {k: _serialize_item(v) for k, v in item.items()}
            all_headers.update(processed_item.keys())
            processed_list.append(processed_item)
        else:
            current_logger.warning(f"Non-dictionary item skipped for CSV conversion: {type(item)}")

    if not processed_list:
        current_logger.info(f"No dictionary data available to save to CSV for '{filename_prefix}'.")
        return

    sorted_headers = sorted(list(all_headers)) # Consistent column order

    actual_sub_folder = sub_folder if sub_folder else filename_prefix
    output_path = _ensure_output_dir_exists(settings.file_outputs.base_output_directory, actual_sub_folder, current_logger)
    if not output_path:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_path / f"{filename_prefix}_{timestamp}.csv"

    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted_headers, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(processed_list)
        current_logger.info(f"Data successfully saved to CSV file: {filepath}")
    except IOError as e:
        current_logger.error(f"IOError saving data to CSV file {filepath}: {e}", exc_info=True)
    except Exception as e:
        current_logger.error(f"An unexpected error occurred in save_to_csv_file ({filepath}): {e}", exc_info=True)


def save_to_markdown_file(
    data_to_save: Union[List[Dict[str, Any]], Dict[str, Any]],
    filename_prefix: str,
    sub_folder: Optional[str] = None,
    logger_obj: Optional[logging.Logger] = None
):
    current_logger = logger_obj or default_utils_logger
    if not settings.file_outputs.enable_markdown_output:
        current_logger.debug(f"Markdown output disabled globally. Skipping save for '{filename_prefix}'.")
        return

    if not data_to_save:
        current_logger.info(f"No data provided to save_to_markdown_file for prefix '{filename_prefix}'.")
        return

    actual_sub_folder = sub_folder if sub_folder else filename_prefix
    output_path = _ensure_output_dir_exists(settings.file_outputs.base_output_directory, actual_sub_folder, current_logger)
    if not output_path:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = output_path / f"{filename_prefix}_{timestamp}.md"

    md_content = f"# Report: {filename_prefix.replace('_', ' ').title()}\n\n"
    md_content += f"*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n\n"

    data_list = data_to_save if isinstance(data_to_save, list) else [data_to_save]

    if not data_list: # Should be caught by earlier check, but as safeguard
        md_content += "No data items to report.\n"
    else:
        md_content += f"**Total Items: {len(data_list)}**\n\n---\n\n"

    for i, item in enumerate(data_list):
        if not isinstance(item, dict):
            md_content += f"## Item {i+1} (Skipped - Not a Dictionary)\n\nType: `{type(item)}`\n\n---\n\n"
            current_logger.warning(f"Skipping non-dictionary item in save_to_markdown_file: {type(item)}")
            continue

        # Try to find a title for the item
        item_title = item.get('title', item.get('name', item.get('event_id', f"Item {i+1}")))
        md_content += f"## {item_title}\n\n"

        for key, value in item.items():
            md_key = key.replace('_', ' ').title()
            serialized_value = _serialize_item(value) # Use helper for consistent serialization

            if isinstance(serialized_value, str) and ('\n' in serialized_value or len(serialized_value) > 80) and not key.endswith("_html"):
                # Multi-line strings or long strings as text blocks
                md_content += f"- **{md_key}:**\n  ```text\n{serialized_value}\n  ```\n"
            elif isinstance(serialized_value, str) and key.endswith("_html"): # Render HTML as HTML block
                 md_content += f"- **{md_key}:**\n  ```html\n{serialized_value}\n  ```\n"
            elif isinstance(value, (list, dict)): # Original complex types (already JSON stringified by _serialize_item)
                md_content += f"- **{md_key}:**\n  ```json\n{serialized_value}\n  ```\n"
            else:
                md_content += f"- **{md_key}:** `{serialized_value}`\n"
        md_content += "\n---\n\n"

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(md_content)
        current_logger.info(f"Markdown report successfully saved to: {filepath}")
    except IOError as e:
        current_logger.error(f"IOError saving Markdown report to {filepath}: {e}", exc_info=True)
    except Exception as e:
        current_logger.error(f"An unexpected error occurred in save_to_markdown_file ({filepath}): {e}", exc_info=True)

```
