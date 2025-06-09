import time
import json
from pathlib import Path
from typing import List, Dict, Any
import difflib # For showing differences in complex structures

from .base_test_case import TestCase, TestResult
from adapters.base_adapter import ScraperAdapter

class TestParsingAbilityKnownData(TestCase):
    """
    Tests if the scraper correctly extracts predefined data items from a controlled HTML input.
    This test requires scraper-specific test HTML and expected JSON output files,
    which should be defined in the main test configuration.
    """
    def __init__(self):
        super().__init__(
            case_name="test_parsing_ability_known_data",
            description="Verify correct data extraction from a controlled HTML input against expected values."
        )

    def applies_to(self, adapter_capabilities: List[str]) -> bool:
        # Applies to scrapers that can scrape a single URL and output structured data (JSON implied for comparison)
        # or text (for simpler scrapers like mono_basic_html).
        return "scrape_single_url" in adapter_capabilities and \
               ("outputs_json" in adapter_capabilities or "outputs_text_to_file" in adapter_capabilities or "outputs_text_to_stdout" in adapter_capabilities)

    def run(self, adapter: ScraperAdapter, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        start_time = time.time()

        # These paths would typically come from a global test configuration object
        # passed to the run method, specific to the adapter.scraper_name.
        # For now, we'll construct them based on convention.
        # Example: test_data/scraper_name/parse_test.html and parse_expected.json
        
        scraper_test_data_dir = test_data_root / adapter.scraper_name
        html_test_file_path_on_disk = scraper_test_data_dir / "parse_test.html"
        expected_output_file_path = scraper_test_data_dir / "parse_expected.json" # For JSON scrapers
        expected_text_output_file_path = scraper_test_data_dir / "parse_expected.txt" # For text scrapers

        if not html_test_file_path_on_disk.exists():
            return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="SKIP",
                message=f"Test data not found for {adapter.scraper_name}: {html_test_file_path_on_disk} missing."
            )

        is_json_scraper = "outputs_json" in adapter.get_capabilities()
        is_text_scraper = "outputs_text_to_file" in adapter.get_capabilities() or \
                          "outputs_text_to_stdout" in adapter.get_capabilities()

        if is_json_scraper and not expected_output_file_path.exists():
            return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="SKIP",
                message=f"Expected JSON output file not found for {adapter.scraper_name}: {expected_output_file_path} missing."
            )
        if is_text_scraper and not is_json_scraper and not expected_text_output_file_path.exists():
             return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="SKIP",
                message=f"Expected text output file not found for {adapter.scraper_name}: {expected_text_output_file_path} missing."
            )

        # The URL for the scraper will point to the local HTTP server
        # The file is at scraper_name/parse_test.html relative to test_data_root
        test_local_url = f"{local_http_server_url.rstrip('/')}/{adapter.scraper_name}/parse_test.html"
        
        output_filename = f"{adapter.scraper_name}_parsing_test_output" # Base name, adapter might add extension
        if is_json_scraper:
            output_filename += ".json"
        elif is_text_scraper:
            output_filename += ".txt"


        params = {
            "url": test_local_url,
            "target_url_is_local_file": True,
            # For mono_basic_adapter, it needs the relative path for URL construction if target_url_is_local_file is true
            "actual_file_path_for_adapter": str(html_test_file_path_on_disk),
            "output_filename": output_filename
        }
        if adapter.scraper_name == "mono_basic_html":
             params["url"] = f"{adapter.scraper_name}/parse_test.html" # Relative path for mono_basic_adapter
             # mono_basic_html also needs selectors, these should be part of the test config for it.
             # For this generic test, we assume it's configured to extract relevant parts or all text.
             # To make it truly work for mono_basic_html, the test config would need to provide
             # the selectors/xpaths that correspond to the content of its parse_test.html and parse_expected.txt.
             # For now, this test might be limited for mono_basic_html unless we add that config.
             # Let's assume for now it just dumps all text.
             pass


        test_case_output_dir = temp_output_dir / self.case_name
        test_case_output_dir.mkdir(parents=True, exist_ok=True)

        execution_result = adapter.run_scraper(
            action="scrape",
            params=params,
            temp_output_dir=test_case_output_dir,
            local_http_server_url=local_http_server_url
        )
        duration = time.time() - start_time

        if not execution_result["success"]:
            return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="FAIL",
                message=f"Scraper execution failed. Stderr: {execution_result['stderr']}",
                duration=duration,
                details=execution_result
            )

        actual_output_path = test_case_output_dir / output_filename
        
        # If scraper outputs to stdout and no output_filename was used by adapter:
        # This needs robust handling in adapter. For now, assume output_filename is created.
        if not actual_output_path.exists() and not execution_result["stdout"]:
             return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="FAIL",
                message=f"Scraper did not produce the expected output file: {actual_output_path} and stdout was empty.",
                duration=duration,
                details=execution_result
            )

        status = "FAIL" # Default to fail
        message = ""
        
        try:
            if is_json_scraper:
                with open(expected_output_file_path, "r", encoding="utf-8") as f:
                    expected_data = json.load(f)
                
                # Check if output file was created
                if not actual_output_path.exists():
                    return TestResult(scraper_name=adapter.scraper_name,test_case_name=self.case_name,status="FAIL",
                                      message=f"Expected JSON output file {actual_output_path} not created.", duration=duration)

                with open(actual_output_path, "r", encoding="utf-8") as f:
                    actual_data = json.load(f)

                # Basic comparison: are they equal?
                # For more complex objects, a deep diff might be needed.
                # The structure of `actual_data` might be a list of events, or a single event object.
                # The `expected_data` should match this structure.
                if actual_data == expected_data:
                    status = "PASS"
                    message = "Extracted JSON data matches expected data."
                else:
                    status = "FAIL"
                    message = "Extracted JSON data does not match expected data."
                    # Provide a diff for easier debugging
                    diff = difflib.unified_diff(
                        json.dumps(expected_data, indent=2).splitlines(keepends=True),
                        json.dumps(actual_data, indent=2).splitlines(keepends=True),
                        fromfile='expected.json',
                        tofile='actual.json',
                    )
                    message += "\nDiff:\n" + "".join(diff)

            elif is_text_scraper: # Handle plain text output (e.g., mono_basic_html)
                with open(expected_text_output_file_path, "r", encoding="utf-8") as f:
                    expected_text = f.read().strip()
                
                actual_text = ""
                if actual_output_path.exists():
                    with open(actual_output_path, "r", encoding="utf-8") as f:
                        actual_text = f.read().strip()
                elif execution_result["stdout"]: # Check stdout if file not created
                    actual_text = execution_result["stdout"].strip()
                else:
                    return TestResult(scraper_name=adapter.scraper_name,test_case_name=self.case_name,status="FAIL",
                                      message="No output file and no stdout content for text comparison.", duration=duration)

                if actual_text == expected_text:
                    status = "PASS"
                    message = "Extracted text data matches expected text data."
                else:
                    status = "FAIL"
                    message = "Extracted text data does not match expected text data."
                    diff = difflib.unified_diff(
                        expected_text.splitlines(keepends=True),
                        actual_text.splitlines(keepends=True),
                        fromfile='expected.txt',
                        tofile='actual.txt',
                    )
                    message += "\nDiff:\n" + "".join(diff)
            else:
                status = "SKIP"
                message = "Scraper is not designated as JSON or Text output for this test."

        except json.JSONDecodeError as e:
            status = "FAIL"
            message = f"Failed to decode JSON output: {e}. File content: {actual_output_path.read_text()[:200]}"
        except Exception as e:
            status = "FAIL"
            message = f"An error occurred during output comparison: {e}"

        return TestResult(
            scraper_name=adapter.scraper_name,
            test_case_name=self.case_name,
            status=status,
            message=message,
            duration=duration,
            details=execution_result
        )
