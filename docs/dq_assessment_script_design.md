# Data Quality Assessment Script Design

This document outlines the design for Python scripts and processes to perform periodic data quality assessments on the `unified_events` MongoDB collection. It covers the main validation script, the structure of individual rule functions, scheduling, and options for reporting results.

## 1. Introduction

To ensure the ongoing reliability and accuracy of our data, a systematic data quality (DQ) assessment process is necessary. This process will periodically scan the `unified_events` collection, apply a predefined set of DQ rules (based on `docs/data_quality_rules.md`), and report on any identified issues.

## 2. Main Validation Script (`dq_validator.py`)

**Purpose**:
The `dq_validator.py` script will be the central component for executing data quality checks. It will connect to MongoDB, load and apply DQ rules, and generate reports or log issues.

**Location**: `data_quality/dq_validator.py` (suggested new directory)

**Core Logic & Features**:

1.  **Configuration**:
    *   Imports the global `settings` object from `config.py` to obtain MongoDB connection details (`settings.db.mongodb_uri`, `settings.db.mongodb_db_name`, `settings.db.unified_events_collection`).
    *   Accepts command-line arguments (or Prefect parameters if run as a flow) for:
        *   `--limit`: Number of documents to process (for testing or partial runs).
        *   `--date-field`: Field to use for date-range filtering (e.g., `timestamps.scraped_at_utc`).
        *   `--start-date`, `--end-date`: For processing documents within a specific date range.
        *   `--rules-path`: Path to the directory containing rule definitions (e.g., `data_quality/rules`).
        *   `--output-mode`: How to output results (e.g., `log`, `json_report`, `mongodb`).
        *   `--collection`: Specify a collection to validate (defaults to unified_events from settings).

2.  **MongoDB Connection**:
    *   Establishes a connection to MongoDB using details from `config.settings`.

3.  **Rule Loading and Discovery**:
    *   Dynamically loads rule functions from Python files within a specified directory (e.g., `data_quality/rules/`).
    *   Rule functions could be identified by a naming convention (e.g., `check_dq_rulename()`) or by being decorated with a custom `@dq_rule` decorator that registers them with metadata (Rule ID, Severity, Description). (See Section 3 for rule function structure).

4.  **Document Processing**:
    *   Fetches documents from the `unified_events` collection. Can fetch all, a limited number, or based on a query (e.g., documents updated since last DQ run, or within a date range).
    *   Processes documents in batches to manage memory usage for large collections.
    *   For each document, it iterates through all loaded DQ rule functions.
    *   Applies each rule function to the document.

5.  **Result Aggregation**:
    *   Collects the results from each rule application (pass/fail, failure message, severity).
    *   Aggregates statistics:
        *   Total documents checked.
        *   Total documents with at least one DQ issue.
        *   Number of failures per specific rule.
        *   Breakdown of issues by severity.

6.  **Reporting and Output**: (See Section 5 for details)
    *   Generates a summary report.
    *   Logs detailed information about failures.
    *   Optionally saves detailed issue reports to a separate MongoDB collection or updates existing documents.
    *   Optionally exposes summary metrics for Prometheus.

7.  **Logging**:
    *   Uses the standard Python `logging` module.
    *   Logs the start and end of the DQ assessment process, rules loaded, batches processed, summary of findings, and any errors during the script's execution.

**Conceptual Structure of `dq_validator.py`**:
```python
# data_quality/dq_validator.py
import argparse
import logging
from pymongo import MongoClient
# from config import settings # Import your actual settings
# from .rule_loader import load_rules # Hypothetical rule loader

# Placeholder for settings
class SettingsDB:
    mongodb_uri = "mongodb://localhost:27017/"
    mongodb_db_name = "scraper_db"
    unified_events_collection = "unified_events"
settings = type('Settings', (object,), {'db': SettingsDB()})


logger = logging.getLogger(__name__)

def apply_rules_to_document(document, rules):
    issues = []
    for rule_func, rule_meta in rules.items(): # Assuming rules is a dict {func: meta}
        is_valid, message = rule_func(document)
        if not is_valid:
            issues.append({
                "event_id": document.get("_id"),
                "rule_id": rule_meta.get("id", rule_func.__name__),
                "severity": rule_meta.get("severity", "Unknown"),
                "message": message,
                "field": rule_meta.get("field", "N/A")
            })
    return issues

def main():
    # parser = argparse.ArgumentParser(...)
    # args = parser.parse_args()

    # Configure logging (basic example)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    logger.info("Starting Data Quality Assessment...")

    # 1. Load Rules (example placeholder)
    # rules = load_rules(args.rules_path or "data_quality/rules")
    # logger.info(f"Loaded {len(rules)} DQ rules.")
    rules = {} # Placeholder

    # 2. Connect to MongoDB
    try:
        client = MongoClient(settings.db.mongodb_uri)
        db = client[settings.db.mongodb_db_name]
        collection = db[settings.db.unified_events_collection]
        logger.info(f"Connected to MongoDB: {settings.db.mongodb_db_name}/{settings.db.unified_events_collection}")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}", exc_info=True)
        return

    # 3. Fetch Documents (example: fetch first 100, or use args for filtering)
    # query = build_query_from_args(args)
    # documents_cursor = collection.find(query).limit(args.limit or 1000)
    documents_cursor = collection.find({}).limit(100) # Placeholder

    total_docs_checked = 0
    total_docs_with_issues = 0
    all_issues_summary = {} # Rule_ID: count

    # 4. Process Documents
    for doc in documents_cursor:
        total_docs_checked += 1
        doc_issues = apply_rules_to_document(doc, rules)
        if doc_issues:
            total_docs_with_issues += 1
            logger.debug(f"Document ID {doc.get('_id')} failed DQ checks: {doc_issues}")
            for issue in doc_issues:
                all_issues_summary[issue["rule_id"]] = all_issues_summary.get(issue["rule_id"], 0) + 1
            # Handle reporting/storing of doc_issues (see Section 5)
            # store_issues_to_mongodb(doc_issues)
            # update_event_document_with_flags(doc.get('_id'), doc_issues)

    # 5. Generate Summary Report
    logger.info("--- Data Quality Assessment Summary ---")
    logger.info(f"Total Documents Checked: {total_docs_checked}")
    logger.info(f"Total Documents with Issues: {total_docs_with_issues}")
    if total_docs_checked > 0:
        overall_pass_rate = ((total_docs_checked - total_docs_with_issues) / total_docs_checked) * 100
        logger.info(f"Overall Pass Rate: {overall_pass_rate:.2f}%")
    logger.info("Failure Count per Rule:")
    for rule_id, count in all_issues_summary.items():
        logger.info(f"  - {rule_id}: {count} failures")

    # generate_summary_report_file(summary_stats)
    # expose_metrics_to_prometheus(summary_stats)

    if client:
        client.close()
    logger.info("Data Quality Assessment Finished.")

if __name__ == "__main__":
    main()
```

## 3. Structure of Individual Data Quality Rule Functions

*   **Location**: Create a new directory, e.g., `data_quality/rules/`. Inside this, files can group related rules (e.g., `completeness.py`, `validity_format.py`, `consistency.py`, `freshness.py`). Each file will contain multiple rule functions.
*   **Decorator for Metadata (Recommended)**: A custom decorator can associate metadata (Rule ID, description, severity, affected fields from `data_quality_rules.md`) with each rule function.
    ```python
    # data_quality/rule_utils.py (or similar)
    import functools

    RULE_REGISTRY = {}

    def dq_rule(rule_id: str, severity: str, description: str, field: str = "N/A"):
        def decorator(func):
            RULE_REGISTRY[func.__name__] = {
                "id": rule_id,
                "severity": severity,
                "description": description,
                "field": field,
                "function": func
            }
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    ```
*   **Rule Function Signature and Return**:
    *   Each function should accept one argument: `event_doc: Dict[str, Any]`.
    *   It should return a tuple: `(is_valid: bool, message: str)`.
        *   `is_valid`: `True` if the document passes the rule, `False` otherwise.
        *   `message`: A descriptive message, especially useful on failure (e.g., "event_id is missing", "start_date_utc is not a valid ISO date").

*   **Example Rule Function (`data_quality/rules/completeness.py`)**:
    ```python
    # from ..rule_utils import dq_rule # Assuming rule_utils.py is in parent dir or accessible
    # For simplicity in this example, decorator usage is conceptual.

    # @dq_rule(rule_id="C001", severity="Critical", description="Mandatory unique event identifier.", field="_id")
    def check_c001_event_id_present(event_doc: dict) -> tuple[bool, str]:
        event_id = event_doc.get("_id") # In MongoDB, event_id is stored as _id
        if event_id and isinstance(event_id, str) and event_id.strip():
            return True, "Valid _id (event_id)."
        return False, "Missing, empty, or invalid _id (event_id)."

    # @dq_rule(rule_id="C002", severity="Critical", description="Mandatory event title.", field="event_details.title")
    def check_c002_title_present(event_doc: dict) -> tuple[bool, str]:
        title = event_doc.get("event_details", {}).get("title")
        if title and isinstance(title, str) and title.strip() and len(title) > 3:
            return True, "Title is present and valid."
        return False, "Title is missing, empty, or too short."

    # ... other rule functions ...
    ```
*   **Rule Loader (`data_quality/rule_loader.py`)**:
    *   A utility function in `dq_validator.py` or a separate `rule_loader.py` would dynamically import modules from the `rules` directory and collect functions decorated with `@dq_rule` (or by naming convention) into a dictionary or list that `dq_validator.py` can iterate over.

## 4. Scheduling with Prefect

The `dq_validator.py` script is well-suited to be run as a Prefect flow.

*   **Flow Definition**:
    ```python
    # flows/data_quality_flow.py
    from prefect import flow, task
    # from data_quality.dq_validator import main as run_dq_validation # If main is importable
    # from config import settings

    @task(name="Run Data Quality Assessment")
    def run_dq_assessment_task(date_str: Optional[str] = None, limit: Optional[int] = None):
        # This task would call the main logic of dq_validator.py.
        # It might involve subprocess.run(['python', 'data_quality/dq_validator.py', ...])
        # or preferably, refactor dq_validator.py's main() to be callable.
        logger.info(f"Running DQ assessment for date: {date_str}, limit: {limit}")
        # run_dq_validation(args_for_validator) # Pass parameters
        # For now, simulate:
        print(f"Simulating DQ Assessment for date {date_str} with limit {limit}")
        return {"status": "completed", "docs_checked": limit or 1000, "issues_found": (limit or 1000) // 10}

    @flow(name="Periodic Data Quality Check")
    def data_quality_assessment_flow(run_date_iso: Optional[str] = None, processing_limit: Optional[int] = None):
        # run_date_iso could be today's date, or a specific date for backfills
        # This allows parameterization from Prefect UI or schedules
        summary = run_dq_assessment_task.submit(date_str=run_date_iso, limit=processing_limit)
        # Handle summary, e.g., save as artifact, send notification if issues exceed threshold
        print(f"Data quality flow completed. Summary future: {summary}")

    # To schedule, e.g., daily at 3 AM:
    # from prefect.deployments import Deployment
    # from prefect.server.schemas.schedules import CronSchedule
    # Deployment.build_from_flow(
    #     flow=data_quality_assessment_flow,
    #     name="daily-dq-check",
    #     schedule=(CronSchedule(cron="0 3 * * *", timezone="UTC")),
    #     parameters={"processing_limit": 10000} # Example parameter
    # )
    ```
*   **Scheduling**: Use Prefect's scheduling features (e.g., `CronSchedule`) to run the `data_quality_assessment_flow` periodically (e.g., daily or weekly).
*   **Benefits**: Leverages Prefect for logging, retries (if the entire DQ script fails), notifications, and UI visibility of DQ runs.

## 5. Reporting and Storing DQ Results

`dq_validator.py` (or the Prefect flow wrapping it) should provide various output options:

1.  **Console Logs**: Detailed logs during execution, with a summary at the end (as shown in the `dq_validator.py` conceptual structure).
2.  **JSON/Markdown Summary File**: A file containing aggregated statistics (total checked, total issues, breakdown by rule/severity). This can be stored locally or as a Prefect artifact.
3.  **Dedicated MongoDB Collection (`data_quality_issues`)**:
    *   **Structure**: Each document in this collection could represent a single data quality rule failure for a specific event.
        ```json
        {
            "issue_id": "<UUID>", // Unique ID for this issue entry
            "event_id": "<ID of the event in unified_events>",
            "rule_id": "C001",
            "rule_description": "Mandatory unique event identifier.",
            "severity": "Critical",
            "field_affected": "_id",
            "failure_message": "Missing or empty _id (event_id).",
            "checked_at_utc": "<ISO_DATETIME_OF_DQ_CHECK>",
            "event_scraped_at_utc": "<ISO_DATETIME_EVENT_WAS_SCRAPED>", // For context
            "status": "open" // or "resolved", "acknowledged"
        }
        ```
    *   **Benefits**: Allows querying and tracking of DQ issues over time, building dashboards on DQ trends.
4.  **Updating `unified_events` Documents**:
    *   Add a sub-document or array to each event in `unified_events` like `data_quality_summary: {"last_checked_utc": "...", "pass_status": "failed", "failed_rules": ["C001", "VF003"]}`.
    *   **Benefits**: Easy to see DQ status directly on the event; can be used for filtering.
    *   **Cons**: Can make event documents larger; frequent updates to event documents.
5.  **Prometheus Metrics**:
    *   The `dq_validator.py` script (if run as a long-running service, or via Pushgateway for batch jobs) can expose summary metrics:
        *   `dq_documents_checked_total`
        *   `dq_documents_with_issues_total`
        *   `dq_rule_failures_total{rule_id="C001", severity="Critical"}`
    *   These can be scraped by Prometheus and visualized in Grafana to track DQ trends.
6.  **Prefect Artifacts**:
    *   The Prefect flow can generate a Markdown report with the summary statistics and save it as a run artifact, making it easily accessible from the Prefect UI.

**Recommendation for Reporting**:
*   Start with detailed **console logs** and a **JSON/Markdown summary file** (saved as a Prefect artifact).
*   Implement saving detailed issues to a dedicated **`data_quality_issues` MongoDB collection** for historical tracking and analysis.
*   Expose key summary statistics as **Prometheus metrics** for dashboarding DQ trends in Grafana.
*   Consider updating `unified_events` with a DQ summary flag as a secondary step if direct querying based on DQ status is frequently needed.

This design provides a framework for a comprehensive data quality assessment process that is automated, reportable, and integrated into the orchestration system.
