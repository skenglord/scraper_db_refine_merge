# Universal Scraper Testing Tool

This tool is designed to test the functionality and reliability of various Python web scraper scripts within this repository. It provides a common framework for executing scrapers, running a suite of universal test cases, and reporting results.

## Features

- **Universal Test Suite**: A core set of test cases applicable to different types of scrapers, covering:
    - Basic script health and dependency checks.
    - Connectivity to live URLs and local test pages.
    - Core workflow testing (single URL scraping, crawling).
    - Parsing ability against known data.
    - Output generation and format validation.
- **Scraper Adapters**: A modular system to interface with diverse scraper scripts, allowing the tool to execute them with consistent parameters.
- **Controlled Test Environment**: Utilizes a local HTTP server to serve test HTML files, ensuring reproducible test conditions.
- **Configuration Driven**: Test runs can be configured (e.g., selecting scrapers, defining test data paths) via a central configuration file.
- **Clear Reporting**: Generates reports summarizing test outcomes for each scraper.

## Project Structure

```
scraper_testing_tool/
├── scraper_tester.py         # Main test runner script
├── adapters/                 # Scraper-specific adapters
│   ├── __init__.py
│   ├── base_adapter.py       # Abstract base class for adapters
│   └── ...                   # Concrete adapter implementations
├── test_cases/               # Universal test case implementations
│   ├── __init__.py
│   ├── base_test_case.py     # Base class for test cases
│   └── ...                   # Concrete test case implementations
├── test_data/                # Test files (HTML, expected JSON outputs)
│   ├── common/               # Common test files (e.g., simple_page.html)
│   └── <scraper_name>/       # Scraper-specific test data
│       ├── parse_test.html
│       └── parse_expected.json (or .txt)
├── test_configs/             # Configurations for test runs
│   └── default_test_config.yaml # Defines scrapers, test URLs, data mappings
├── local_http_server.py      # Simple HTTP server for serving test_data
└── results/                  # Directory for storing test reports
    └── test_report_YYYYMMDD_HHMMSS.txt
```

## Prerequisites

- Python 3.8+
- `requests` library
- `beautifulsoup4` library
- `lxml` (optional, for some scrapers using XPath)
- `PyYAML` (for parsing `default_test_config.yaml`)
- **For Playwright-based scrapers**:
    - `playwright` library (`pip install playwright`)
    - Playwright browsers installed (`playwright install` or `playwright install chromium`)

It's recommended to install these dependencies in a virtual environment.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install requests beautifulsoup4 PyYAML playwright
    playwright install --with-deps chromium # Or other browsers if needed
    ```
    (Install `lxml` if scrapers using XPath are to be tested: `pip install lxml`)

2.  **Configure Scrapers and Test Data**:
    *   Ensure all target scraper scripts are present in the repository (e.g., at the root level or in a known location).
    *   Update adapter paths in `scraper_tester.py` or the configuration if scrapers are not in the root directory.
    *   Populate the `test_data/<scraper_name>/` directories with:
        *   `parse_test.html`: A representative HTML file for testing the scraper's parsing logic.
        *   `parse_expected.json` (or `parse_expected.txt`): The expected output when the scraper processes `parse_test.html`.
        *   Other HTML files needed for workflow or crawl tests.
    *   Review and update `test_configs/default_test_config.yaml`:
        *   List the `scraper_name` (matching adapter filenames without `_adapter.py`) under `scrapers_to_test`.
        *   Set the `reliable_url_for_connectivity`.
        *   Define paths to scraper-specific test data files if they deviate from the convention `test_data/<scraper_name>/<test_type>_test.html`.

## Running Tests

The main test runner script is `scraper_tester.py`. (Implementation of this script is pending as part of the overall tool development).

**Expected Usage (Conceptual):**

```bash
python scraper_testing_tool/scraper_tester.py --config scraper_testing_tool/test_configs/default_test_config.yaml
```

Or, to run tests for a specific scraper:

```bash
python scraper_testing_tool/scraper_tester.py --scraper mono_basic_html
```

Or, to run a specific test case:

```bash
python scraper_testing_tool/scraper_tester.py --test_case test_parsing_ability_known_data
```

Test results will be printed to the console and saved in the `results/` directory.

## Adding a New Scraper

1.  **Place the Scraper Script**: Add the new scraper's Python script to the repository.
2.  **Create an Adapter**:
    *   In the `adapters/` directory, create `new_scraper_adapter.py`.
    *   Implement a class `NewScraperAdapter(ScraperAdapter)` that inherits from `base_adapter.ScraperAdapter`.
    *   Implement the `run_scraper()` method to execute the new scraper script, translating generic parameters into specific command-line arguments.
    *   Implement the `get_capabilities()` method to declare what the scraper can do (e.g., `"scrape_single_url"`, `"outputs_json"`).
3.  **Add Test Data**:
    *   Create a directory `test_data/new_scraper/`.
    *   Add relevant test HTML files (e.g., `parse_test.html`) and expected output files (e.g., `parse_expected.json`).
4.  **Update Configuration**:
    *   Add `"new_scraper"` to the `scrapers_to_test` list in `test_configs/default_test_config.yaml`.
    *   Add any scraper-specific data file mappings if needed.
5.  **Run Tests**: Execute `scraper_tester.py` to include the new scraper in the test run.

## Adding a New Test Case

1.  **Create a Test Case Module**:
    *   In the `test_cases/` directory, create `test_new_feature.py`.
    *   Implement a class `TestNewFeature(TestCase)` that inherits from `base_test_case.TestCase`.
    *   Implement the `applies_to()` method if the test is only for scrapers with specific capabilities.
    *   Implement the `run()` method, which contains the logic for executing the test and determining pass/fail. It should return a `TestResult` object.
2.  **Test Runner Discovery**: The `scraper_tester.py` should automatically discover new `TestCase` subclasses (details of discovery mechanism TBD, could be based on naming conventions or explicit registration).

## Troubleshooting

- **`FileNotFoundError` for scraper script**: Ensure the `script_path` in the scraper's adapter is correct relative to where `scraper_tester.py` is run, or use absolute paths. Consider making paths relative to the repository root.
- **Tests SKIPPED**:
    - A test case might be skipped if its `applies_to()` method returns `False` for a scraper (due to missing capabilities).
    - A test case might be skipped if its required test data files (e.g., `parse_test.html`, `parse_expected.json`) are not found in the `test_data/` directory. Check paths and naming conventions.
- **Parsing Test Failures**:
    - Use the diff output in the test report to pinpoint discrepancies between actual and expected output.
    - Verify that the scraper's selectors/logic are still valid for its `parse_test.html`.
    - Ensure the `parse_expected.json` (or `.txt`) accurately reflects the correct output for the given test HTML.

```
