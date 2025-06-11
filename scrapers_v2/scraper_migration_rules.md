# Scraper Migration Rules

This document outlines the rules and best practices for migrating scrapers to the `scrapers_v2` system. Its goal is to serve as a comprehensive guide for developers.

## 1. General Principles

- **Modularity:** Design scrapers to be modular and reusable.
    - **Best Practice:** Break down scraper logic into smaller, independent functions or classes, each responsible for a specific task (e.g., fetching data, parsing HTML, extracting specific data points).
    - **`scrapers_v2` Framework:** Leverage `scrapers_v2` components for common tasks like HTTP requests, User-Agent rotation, and proxy management. This promotes consistency and reduces boilerplate code.
    - **Example:** Instead of a single monolithic function, create separate functions for `fetch_product_page()`, `parse_product_details()`, and `extract_reviews()`.

- **Configuration-driven:** Utilize configuration files for scraper-specific settings.
    - **Best Practice:** Store all settings that might change (e.g., target URLs, CSS selectors, API keys, retry attempts, timeouts) in a dedicated configuration file (e.g., YAML, JSON).
    - **`scrapers_v2` Framework:** `scrapers_v2` should provide a mechanism for loading and accessing these configurations easily.
    - **Example:**
      ```yaml
      target_url: "http://example.com/products"
      selectors:
        product_name: ".product-title"
        price: "#price"
      api_key: "your_api_key_here"
      ```

- **Data Quality:** Implement data validation and cleaning steps.
    - **Best Practice:** Validate extracted data against expected formats (e.g., data types, regex patterns). Clean data by removing whitespace, special characters, or inconsistencies.
    - **`scrapers_v2` Framework:** Consider integrating or suggesting data validation libraries (e.g., Pydantic, Cerberus).
    - **Example:** Ensure a price is always a float, a product ID matches a specific pattern, or a date is in ISO format.

- **Error Handling:** Implement robust error handling and logging.
    - **Best Practice:** Use try-except blocks to catch potential errors (e.g., network issues, changes in website structure, missing data). Log errors with sufficient detail (e.g., timestamp, scraper name, error message, URL).
    - **`scrapers_v2` Framework:** `scrapers_v2` should have a centralized logging system. Implement mechanisms for retries with exponential backoff for transient errors.
    - **Example:** If a selector isn't found, log a warning and return `None` or a default value, rather than crashing the scraper.

- **Performance:** Optimize scrapers for performance and efficiency.
    - **Best Practice:** Use efficient selectors (CSS selectors are generally faster than XPath if not overly complex). Avoid unnecessary requests. Use caching where appropriate (e.g., for session cookies or unchanging data).
    - **`scrapers_v2` Framework:** Explore options for asynchronous operations (e.g., using `asyncio` with `aiohttp`) if I/O bound operations are a bottleneck.
    - **Example:** Fetch a list page once, then iterate through its items, rather than re-fetching the list for each item.

- **Maintainability:** Write clean, well-documented code.
    - **Best Practice:** Follow consistent coding style (e.g., PEP 8 for Python). Use meaningful variable and function names. Add comments to explain complex logic.
    - **`scrapers_v2` Framework:** Encourage adherence to its established coding conventions.
    - **Example:** `product_price = extract_price(html_content)` is more readable than `p = ep(h)`.

## 2. Pre-Migration Analysis

- **Understand Existing Scraper:**
    - **Checklist:**
        - [ ] What is the primary goal of the scraper? What data does it collect?
        - [ ] What is the source URL(s)?
        - [ ] How does it handle pagination?
        - [ ] What data extraction methods are used (e.g., regex, string manipulation, specific libraries like BeautifulSoup, Scrapy)?
        - [ ] How are CSS selectors or XPath expressions currently defined and used?
        - [ ] Are there any anti-scraping measures in place on the target site (e.g., CAPTCHAs, IP blocking, dynamic content loading)? How does the current scraper handle them?
        - [ ] How is data currently stored or outputted?
        - [ ] What are the known issues or limitations of the current scraper?
        - [ ] Are there any specific headers, cookies, or session management techniques used?
        - [ ] How frequently does this scraper run?
        - [ ] What is its current performance (e.g., time to complete, resources consumed)?

- **Identify Data Schema:**
    - **Action:** Define the exact fields to be extracted for each item.
    - **Best Practice:** Specify data types, whether a field is optional or required, and any known constraints or formats.
    - **`scrapers_v2` Compatibility:** Ensure the schema aligns with how `scrapers_v2` expects data to be structured for storage or further processing.
    - **Example:**
        - `product_name`: string, required
        - `price`: float, required, (e.g., 19.99)
        - `product_url`: string, required, valid URL format
        - `image_url`: string, optional, valid URL format
        - `rating`: float, optional, 0.0 to 5.0

- **Assess Dependencies:**
    - **Action:** List all external libraries or services the current scraper relies on.
    - **Best Practice:** Determine if these dependencies are still needed or if `scrapers_v2` provides equivalent functionality.
    - **Example:** If the old scraper uses a custom library for HTTP requests, check if `scrapers_v2`'s built-in HTTP client is sufficient.

## 3. Migration Process

- **Setup Environment:**
    - **Task:** Ensure you have the `scrapers_v2` framework installed and configured.
    - **Task:** Set up any necessary virtual environments.
    - **Task:** Obtain any required API keys or credentials for `scrapers_v2` or target sites.

- **Implement Core Logic using `scrapers_v2` components:**
    - **Task:** Initialize the `scrapers_v2` client or relevant components.
    - **Task:** Implement functions for making HTTP requests using `scrapers_v2`'s request handling, including error handling, retries, proxy usage, and User-Agent rotation as configured.
    - **Example (`scrapers_v2` hypothetical):**
      ```python
      # from scrapers_v2.client import ScraperClient
      # client = ScraperClient(config_path='config/my_scraper_config.yaml')
      # html_content = await client.fetch_url(target_url)
      ```

- **Data Extraction:**
    - **Task:** Translate existing selectors (CSS, XPath) or regex patterns. Prioritize robust selectors that are less likely to break with minor site changes.
    - **Task:** Use `scrapers_v2`'s recommended parsing libraries (e.g., BeautifulSoup, lxml).
    - **Best Practice:** Group related extraction logic (e.g., a function to extract all details from a product page).
    - **Example (using BeautifulSoup):**
      ```python
      # from bs4 import BeautifulSoup
      # soup = BeautifulSoup(html_content, 'html.parser')
      # product_name = soup.select_one(config['selectors']['product_name']).text.strip()
      ```

- **Data Transformation:**
    - **Task:** Implement logic to clean extracted data (e.g., remove currency symbols from prices, convert relative URLs to absolute).
    - **Task:** Format data according to the defined schema.
    - **Example:**
      ```python
      # price_str = "$19.99"
      # price = float(price_str.replace("$", "").strip()) # 19.99
      ```

- **Configuration:**
    - **Task:** Create the scraper-specific configuration file (e.g., `my_scraper_config.yaml`).
    - **Task:** Populate it with target URLs, selectors, API keys, rate limits, etc.
    - **Best Practice:** Keep sensitive information like API keys out of version control (use environment variables or a secrets management system, which `scrapers_v2` should support).

- **Testing (Initial):**
    - **Task:** Perform basic tests by running the scraper against a few target pages.
    - **Task:** Verify that data is being extracted as expected.

## 4. Code Refactoring and Optimization

- **Refactor for Clarity:**
    - **Technique:** Apply the "Single Responsibility Principle" to functions and classes.
    - **Technique:** Replace magic numbers or strings with named constants or configuration values.
    - **Technique:** Simplify complex conditional logic.
    - **Example:** Instead of a long function doing fetching, parsing, and saving, break it into `fetch_data()`, `parse_data()`, and `save_data()`.

- **Optimize Performance:**
    - **Strategy:** If dealing with many pages, implement or leverage `scrapers_v2`'s support for concurrency (async/await, threading, or multiprocessing).
    - **Strategy:** Cache frequently accessed, rarely changing data (e.g., category lists).
    - **Strategy:** Analyze selector performance; very complex XPath expressions can be slow.
    - **Strategy:** Ensure appropriate timeouts and retry mechanisms are in place to avoid getting stuck on unresponsive pages.

- **Code Review:**
    - **Process:** Have another developer review the migrated scraper code.
    - **Focus Areas:** Adherence to `scrapers_v2` best practices, correctness of logic, efficiency, maintainability, and documentation.

## 5. Data Validation and Testing

- **Unit Tests:**
    - **Focus:** Test individual functions or methods in isolation.
    - **Examples:**
        - Test a function that cleans price strings: `assert clean_price("$19.99") == 19.99`.
        - Test a selector function with sample HTML: `assert extract_title(sample_html) == "Expected Title"`.
        - Test data transformation logic.
    - **`scrapers_v2` Integration:** Use mock objects for `scrapers_v2` components if necessary.

- **Integration Tests:**
    - **Focus:** Test the scraper's end-to-end process, from fetching a page to extracting and transforming data.
    - **Examples:**
        - Run the scraper against a locally saved HTML page (a "fixture") to ensure it extracts all fields correctly.
        - Test pagination logic by running the scraper on a few pages of a multi-page listing.
    - **Best Practice:** Avoid hitting live sites during automated integration tests where possible; use static fixtures or a test server.

- **Data Validation Rules:**
    - **Implementation:** Use libraries like Pydantic for schema validation or implement custom validation functions.
    - **Examples:**
        - `price` must be a positive float.
        - `product_url` must be a valid URL.
        - `rating` must be between 0 and 5.
        - `name` field should not be empty.
    - **Action:** Log validation errors or flag items that fail validation.

- **Manual Testing:**
    - **Process:** Run the scraper against the live website.
    - **Verification:** Spot-check the output data for accuracy, completeness, and consistency.
    - **Focus:** Pay attention to edge cases or pages with unusual layouts.

## 6. Documentation and Maintenance

- **Scraper Documentation (Standard Format):**
    - **`README.md` per scraper (e.g., `scrapers_v2/scrapers/my_target_site/README.md`):**
        - **Overview:** Brief description of the target website and the data being scraped.
        - **Configuration:** Details on the specific configuration file (`config_name.yaml`) and its parameters (selectors, special settings, etc.). How to obtain API keys if needed.
        - **Setup:** Any specific setup steps beyond standard `scrapers_v2` environment setup.
        - **Running the Scraper:** Command-line instructions or how to trigger it via `scrapers_v2` system.
        - **Output Schema:** Description of the data fields being extracted (as defined in Pre-Migration Analysis).
        - **Known Issues/Limitations:** Any current problems or things to watch out for.
        - **Maintainer(s):** Who is responsible for this scraper.

- **Code Comments:**
    - **Best Practice:** Add comments to explain:
        - Complex or non-obvious logic.
        - The purpose of functions and classes.
        - The meaning of important variables or constants.
        - Any workarounds for website quirks.

- **Maintenance Plan:**
    - **Regular Checks:** Define how often the scraper's output and performance should be checked (e.g., daily, weekly).
    - **Monitoring:** Set up alerts for critical errors or significant drops in data quality/quantity (ideally integrated with `scrapers_v2`'s capabilities).
    - **Update Process:** Procedure for updating selectors or logic when the target website changes. This includes testing and deployment.
    - **Contact Person:** Who to contact if the scraper fails or needs updates.
```
