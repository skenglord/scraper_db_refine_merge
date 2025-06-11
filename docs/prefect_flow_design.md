# Prefect Flow Definitions for Key Scrapers

This document outlines basic Prefect flow definitions for orchestrating key scrapers in the project: `mono_ticketmaster.py`, `classy_clubtickets_nav_scraper.py`, and `ventura_crawler.py` (including its ETL process). It also provides a generic example of Prefect's task and flow structure.

These definitions assume that the scrapers will be refactored to expose their main functionality as callable Python functions, and that configuration is primarily handled by the unified configuration system (e.g., `config.settings` object), with Prefect parameters providing runtime overrides.

## 1. Generic Prefect Task and Flow Structure Example

This example demonstrates the basic usage of Prefect's `@task` and `@flow` decorators.

```python
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from typing import List, Dict, Any

# Assumes 'settings' object is imported from a central config.py
# from config import settings

# --- Example Task Definitions ---

@task(
    name="Example Scraper Task",
    description="A generic task that simulates scraping a single URL.",
    retries=2,  # Number of retries on failure
    retry_delay_seconds=30, # Delay between retries
    cache_key_fn=task_input_hash, # Cache task results based on input hash
    cache_expiration=timedelta(hours=12) # Cache results for 12 hours
)
def example_run_scraper_script(target_url: str, scraper_param: str = "default_value") -> Dict[str, Any]:
    """
    Simulates running a scraper script for a given URL.
    In a real scenario, this would call the main function of a refactored scraper.
    """
    print(f"Prefect Task: Pretending to scrape URL: {target_url} with param: {scraper_param}")
    # Actual scraper logic would be here, e.g.:
    # result = some_scraper_main_function(url=target_url, custom_param=scraper_param)

    # Simulate a result
    if "error" in target_url:
        raise ValueError("Simulated error for URL containing 'error'")

    result_data = {"url": target_url, "data": f"scraped_content_for_{target_url.split('/')[-1]}", "param_used": scraper_param}
    print(f"Prefect Task: Finished scraping {target_url}. Result: {result_data}")
    return result_data

@task(
    name="Example Post-Processing Task",
    description="A generic task that simulates post-processing of scraped data."
)
def example_process_data(scraped_data: List[Dict[str, Any]]) -> str:
    """
    Simulates processing a list of scraped data items.
    """
    print(f"Prefect Task: Processing {len(scraped_data)} items.")
    if not scraped_data:
        summary = "No data processed."
    else:
        summary = f"Processed {len(scraped_data)} items. First item URL: {scraped_data[0].get('url', 'N/A')}."
    print(f"Prefect Task: {summary}")
    return summary

# --- Example Flow Definition ---

@flow(
    name="Generic Scraper Workflow",
    description="An example flow that orchestrates scraping and data processing."
)
def example_scraper_workflow(
    urls_to_scrape: List[str] = ["http://example.com/page1", "http://example.com/page2"],
    processing_param: str = "alpha"
):
    """
    Defines a workflow that scrapes multiple URLs and then processes the results.
    """
    print(f"Prefect Flow: Starting 'Generic Scraper Workflow' for {len(urls_to_scrape)} URLs.")

    scraped_results = []
    for url in urls_to_scrape:
        # .submit() is used to run tasks concurrently if the executor allows
        # For sequential execution, just call the task: result = example_run_scraper_script(url, scraper_param="test")
        # Here, we use .map to apply the task to each URL
        # For simplicity in this example, let's assume sequential execution for clarity of result handling
        # If using .submit(), you'd get Futures and would need to gather their results.
        try:
            # Pass parameters to the task
            # Scraper-specific parameters can be hardcoded, from config, or flow parameters
            result = example_run_scraper_script.fn(target_url=url, scraper_param=f"param_for_{url.split('/')[-1]}")
            scraped_results.append(result)
        except Exception as e:
            print(f"Prefect Flow: Error scraping {url}: {e}. Continuing with other URLs.")
            # Optionally, append error information or handle differently

    # This task depends on the completion of all scraping tasks implicitly
    # if results are passed directly.
    # For explicit dependency with .submit(), use wait_for=[future1, future2, ...]
    final_summary = example_process_data.fn(scraped_data=scraped_results)

    print(f"Prefect Flow: Workflow finished. Summary: {final_summary}")
    return final_summary

if __name__ == "__main__":
    # Example of running the flow (typically done via Prefect CLI or Agent)
    # To run locally for testing:
    example_scraper_workflow(
        urls_to_scrape=["http://example.com/data1", "http://example.com/error_trigger", "http://example.com/data3"],
        processing_param="beta"
    )
```

## 2. Prefect Flow Definitions for Key Scrapers

### a. `mono_ticketmaster.py`

*   **Refactoring Note**: The `MultiLayerEventScraper.run()` method or the `if __name__ == "__main__":` block in `mono_ticketmaster.py` would need to be refactored into a callable function (e.g., `execute_ticketmaster_scrape`) that accepts parameters and can be decorated with `@task`.
*   **Flow Name**: `ticketmaster_event_scraper_flow`
*   **Parameters**:
    *   `target_url: str`: The specific event URL or listing URL to start crawling. Default could be sourced from `settings.scrapers["monoticketmaster"].default_target_url`.
    *   `crawl_listing: bool`: Whether to enable crawling of a listing page. Default: `False`.
    *   `max_crawl_events: int`: Maximum events to scrape when crawling a listing. Default: `settings.scrapers["monoticketmaster"].default_max_crawl_events` or a sensible value like 10.
    *   `headless_mode: bool`: Override for headless browser operation. Default: `settings.scraper_globals.default_headless_browser`.
*   **Tasks**:
    *   `run_ticketmaster_scraper_task(target_url: str, crawl_listing: bool, max_crawl_events: int, headless_mode: bool)`:
        *   **Description**: Executes the main scraping logic of `mono_ticketmaster.py`.
        *   **Action**: Initializes `MultiLayerEventScraper` with appropriate config (derived from flow parameters and global settings) and calls its main execution method.
        *   **Returns**: Summary statistics (e.g., number of events scraped).
*   **Dependencies**: No explicit inter-task dependencies within this simple flow, unless pre/post-processing tasks are added.

```python
# Example structure for mono_ticketmaster_flow.py (conceptual)

# from prefect import task, flow
# from config import settings # Assuming unified config
# # from my_scrapers.mono_ticketmaster import execute_ticketmaster_scrape # Refactored main logic

# @task(name="Run Ticketmaster Scraper")
# def run_ticketmaster_scraper_task(target_url: str, crawl_listing: bool, max_crawl_events: int, headless_mode: bool) -> dict:
#     # effective_config = build_config_for_ticketmaster(target_url, crawl_listing, ...)
#     # return execute_ticketmaster_scrape(config=effective_config)
#     print(f"Simulating Ticketmaster scrape for {target_url}, crawl: {crawl_listing}")
#     return {"url": target_url, "events_found": 5 if crawl_listing else 1}


# @flow(name="Ticketmaster Event Scraper Flow")
# def ticketmaster_event_scraper_flow(
#     target_url: str = "default_ticketmaster_url_from_config", # settings.scrapers["monoticketmaster"].default_target_url
#     crawl_listing: bool = False,
#     max_crawl_events: int = 10, # settings.scrapers["monoticketmaster"].default_max_crawl_events
#     headless_mode: bool = True # settings.scraper_globals.default_headless_browser
# ):
#     summary = run_ticketmaster_scraper_task.submit(
#         target_url=target_url,
#         crawl_listing=crawl_listing,
#         max_crawl_events=max_crawl_events,
#         headless_mode=headless_mode
#     )
#     # Wait for summary if needed, or let flow complete
#     print(f"Ticketmaster flow submitted. Access result from summary future if needed.")
```

### b. `classy_clubtickets_nav_scraper.py`

*   **Refactoring Note**: The `ClubTicketsScraper.crawl_events()` method or the `if __name__ == "__main__":` block needs to be callable from a Prefect task.
*   **Flow Name**: `clubtickets_event_scraper_flow`
*   **Parameters**:
    *   `target_url: str`: The starting URL for scraping ClubTickets. Default: `settings.scrapers["clubtickets"].default_target_url`.
    *   `max_pages_to_process: int`: Max number of date tabs/pages to process. Default: `settings.scrapers["clubtickets"].max_pages_to_process`.
    *   `headless_mode: bool`: Override for headless browser operation. Default: `settings.scraper_globals.default_headless_browser`.
*   **Tasks**:
    *   `run_clubtickets_scraper_task(target_url: str, max_pages_to_process: int, headless_mode: bool)`:
        *   **Description**: Executes the main scraping logic of `classy_clubtickets_nav_scraper.py`.
        *   **Action**: Initializes `ClubTicketsScraper` and calls its main method.
        *   **Returns**: Summary of scraped events or statistics.
*   **Dependencies**: None within this simple flow.

```python
# Example structure for clubtickets_flow.py (conceptual)

# from prefect import task, flow
# from config import settings
# # from my_scrapers.classy_clubtickets_nav_scraper import execute_clubtickets_scrape

# @task(name="Run ClubTickets Scraper")
# def run_clubtickets_scraper_task(target_url: str, max_pages: int, headless: bool) -> dict:
#     # return execute_clubtickets_scrape(url=target_url, max_pages=max_pages, headless=headless)
#     print(f"Simulating ClubTickets scrape for {target_url}, max_pages: {max_pages}")
#     return {"url": target_url, "events_found": 20}

# @flow(name="ClubTickets Event Scraper Flow")
# def clubtickets_event_scraper_flow(
#     target_url: str = "default_clubtickets_url_from_config", # settings.scrapers["clubtickets"].default_target_url
#     max_pages_to_process: int = 2, # settings.scrapers["clubtickets"].max_pages_to_process
#     headless_mode: bool = True # settings.scraper_globals.default_headless_browser
# ):
#     run_clubtickets_scraper_task.submit(
#         target_url=target_url,
#         max_pages=max_pages_to_process,
#         headless=headless_mode
#     )
#     print("ClubTickets flow submitted.")
```

### c. `ventura_crawler.py` and ETL (`database/etl_sqlite_to_mongo.py`)

*   **Refactoring Note**:
    *   `ventura_crawler.py`: Its `SerpentScaleScraper.scrape_urls()` or `main()` needs to be callable by a task.
    *   `etl_sqlite_to_mongo.py`: Its `main_etl_process()` needs to be callable by a task.
*   **Flow Name**: `ventura_pipeline_flow`
*   **Parameters**:
    *   `urls_to_scrape: Optional[List[str]]`: List of URLs to scrape. If `None`, the scraper might use its default list or another source. Default: `None`.
    *   `ventura_config_overrides: Optional[Dict[str, Any]]`: Dictionary for specific overrides to Ventura's internal config. Default: `None`.
    *   `run_etl: bool`: Whether to run the ETL process after scraping. Default: `True`.
    *   `headless_mode: bool`: Override for headless browser. Default: `settings.scraper_globals.default_headless_browser`.
*   **Tasks**:
    *   `run_ventura_crawler_task(urls: Optional[List[str]], config_overrides: Optional[Dict], headless: bool)`:
        *   **Description**: Executes the `ventura_crawler.py` scraping logic.
        *   **Action**: Initializes `SerpentScaleScraper` and runs `scrape_urls()`.
        *   **Returns**: Path to the SQLite DB or scraping statistics.
    *   `run_sqlite_to_mongo_etl_task()`:
        *   **Description**: Executes the SQLite to MongoDB ETL process.
        *   **Action**: Calls the main function of `etl_sqlite_to_mongo.py`.
        *   **Returns**: Summary of ETL process (e.g., number of records migrated).
*   **Dependencies**:
    *   `run_sqlite_to_mongo_etl_task` should only run if `run_ventura_crawler_task` completes successfully and `run_etl` parameter is true.

```python
# Example structure for ventura_pipeline_flow.py (conceptual)

# from prefect import task, flow, case
# from typing import List, Dict, Optional, Any
# from config import settings
# # from my_scrapers.ventura_crawler import execute_ventura_scrape # Refactored
# # from database.etl_sqlite_to_mongo import main_etl_process # Refactored

# @task(name="Run Ventura Crawler")
# def run_ventura_crawler_task(urls: Optional[List[str]], config_overrides: Optional[Dict], headless: bool) -> Dict[str, Any]:
#     # result = execute_ventura_scrape(urls_to_scrape=urls, config_overrides=config_overrides, headless=headless)
#     # return {"status": "completed", "db_path": settings.scrapers["venturacrawler"].db_path, "events_scraped": result.get("successful_scrapes")}
#     print(f"Simulating Ventura Crawler scrape. URLs: {urls}, Headless: {headless}")
#     return {"status": "completed", "events_scraped": 50, "db_path": "serpentscale_scraper_data.db"}

# @task(name="Run SQLite to MongoDB ETL")
# def run_sqlite_to_mongo_etl_task(upstream_result: Dict[str, Any]) -> Dict[str, Any]:
#     # if upstream_result.get("status") == "completed":
#     #     etl_summary = main_etl_process() # Call refactored ETL main
#     #     return {"status": "completed", "records_migrated": etl_summary.get("migrated_count")}
#     # else:
#     #     return {"status": "skipped", "reason": "Upstream scraping failed or produced no data"}
#     print(f"Simulating ETL process. Upstream result: {upstream_result}")
#     return {"status": "completed", "records_migrated": upstream_result.get("events_scraped", 0)}


# @flow(name="Ventura Scraping and ETL Pipeline")
# def ventura_pipeline_flow(
#     urls_to_scrape: Optional[List[str]] = None, # Could be dynamically generated by another task/flow
#     ventura_config_overrides: Optional[Dict[str, Any]] = None,
#     run_etl: bool = True,
#     headless_mode: bool = True # settings.scraper_globals.default_headless_browser
# ):
#     # Use a specific config for Ventura if defined, e.g. settings.scrapers["venturacrawler"]
#     # For example, headless_mode could be:
#     # effective_headless = headless_mode if headless_mode is not None else settings.scrapers["venturacrawler"].headless

#     scraping_result_future = run_ventura_crawler_task.submit(
#         urls=urls_to_scrape,
#         config_overrides=ventura_config_overrides,
#         headless=headless_mode
#     )

#     # Conditional ETL run using Prefect's 'case' or simply passing data
#     # For simplicity here, we'll use a direct dependency and check within the task (as simulated above)
#     # or use Prefect's conditional logic if more complex.
#     if run_etl: # This is a flow-level conditional execution
#         # The .result() here would block until scraping_result_future is done
#         # In Prefect 2.x, you can pass futures directly or results.
#         # For explicit dependency, use wait_for=[scraping_result_future] in the ETL task submit call.
#         etl_summary_future = run_sqlite_to_mongo_etl_task.submit(
#             upstream_result=scraping_result_future, # Pass future directly
#             wait_for=[scraping_result_future] # Explicit dependency
#         )
#         print("Ventura scraping and ETL flow tasks submitted.")
#         # Access results if needed:
#         # final_etl_summary = etl_summary_future.result()
#         # print(f"ETL Summary: {final_etl_summary}")
#     else:
#         print("Ventura scraping flow submitted. ETL was skipped by parameter.")
#         # final_scraping_summary = scraping_result_future.result()
#         # print(f"Scraping Summary: {final_scraping_summary}")

```

## 3. Key Considerations

*   **Refactoring Scrapers**: The most significant part of implementing these flows is refactoring the `if __name__ == "__main__":` blocks and main methods of the existing scrapers into functions that can be called by Prefect tasks. These functions should accept configuration parameters or rely on the global `settings` object.
*   **Configuration**: The flow parameters should ideally source their default values from the unified `config.settings` object. This makes flows runnable with sensible defaults directly from the Prefect UI, while still allowing overrides for specific runs.
*   **Error Handling**: Prefect tasks have built-in retry mechanisms. The scraper logic should still handle expected errors gracefully (e.g., page not found, parsing errors for a single item) but can let unexpected errors propagate to be handled by Prefect's retry system.
*   **State and Artifacts**: Tasks can return values (e.g., paths to saved files, summary statistics, database record counts). These can be passed to downstream tasks or logged as Prefect artifacts.
*   **Concurrency**: Prefect allows for concurrent task execution using different executors (LocalDaskExecutor, RayExecutor, etc.). Tasks submitted with `.submit()` (Prefect 2.x) or by not explicitly waiting for results can run in parallel if the executor supports it. The examples above use `.submit()` conceptually for tasks that could run independently or where the main flow logic doesn't immediately need their result.

This design provides a basic structure. More complex dependencies, conditional logic, and detailed error handling can be built upon these foundational flow definitions.
