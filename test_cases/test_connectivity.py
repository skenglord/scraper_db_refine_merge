import time
from pathlib import Path
from typing import List, Dict, Any

from .base_test_case import TestCase, TestResult
from adapters.base_adapter import ScraperAdapter

class TestConnectivityBasicFetch(TestCase):
    """
    Tests if the scraper can fetch a simple, reliable live URL.
    """
    def __init__(self):
        super().__init__(
            case_name="test_connectivity_basic_fetch",
            description="Verify successful fetching of a known simple live URL."
        )

    def applies_to(self, adapter_capabilities: List[str]) -> bool:
        # This test applies to any scraper that can scrape a single URL.
        return "scrape_single_url" in adapter_capabilities

    def run(self, adapter: ScraperAdapter, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        start_time = time.time()
        
        # URL for testing basic connectivity. httpbin.org is good for this.
        # This should be configurable in the main test_config.yaml
        # For now, hardcoding it here for simplicity of the test case itself.
        # The TestRunner will eventually pass this via a global_config object.
        test_live_url = "http://httpbin.org/html" # A simple HTML page

        params = {
            "url": test_live_url,
            # For mono_basic_adapter, we might not need selectors/xpaths if we just check the fetch.
            # Let's assume the scraper is supposed to output *something* or exit cleanly.
            # Some scrapers might require a specific output instruction or will write to stdout.
            "output_filename": f"{adapter.scraper_name}_connectivity_test_output.html" # Save the fetched content
        }
        
        test_case_output_dir = temp_output_dir / self.case_name
        test_case_output_dir.mkdir(parents=True, exist_ok=True)

        execution_result = adapter.run_scraper(
            action="scrape", 
            params=params,
            temp_output_dir=test_case_output_dir,
            local_http_server_url=local_http_server_url # Not used for live URL, but part of signature
        )
        
        duration = time.time() - start_time
        
        # Check for successful execution and if an output file was potentially created (though content isn't checked here)
        if execution_result["success"]:
            status = "PASS"
            message = f"Successfully fetched live URL {test_live_url}. Exit code {execution_result['exit_code']}."
            # Optionally, check if the output file exists if one was specified
            if params.get("output_filename"):
                output_file_path = test_case_output_dir / params["output_filename"]
                if output_file_path.exists() and output_file_path.stat().st_size > 0:
                    message += f" Output file {params['output_filename']} created."
                elif not output_file_path.exists():
                     # This might be a FAIL for some scrapers, or acceptable for others if they output to stdout
                     # For now, let's consider it a PASS if exit code is 0, but add a note.
                    message += f" Note: Specified output file {params['output_filename']} was not created (may use stdout)."
        else:
            status = "FAIL"
            message = f"Failed to fetch live URL {test_live_url}. Exit code {execution_result['exit_code']}. Stderr: {execution_result['stderr']}"

        return TestResult(
            scraper_name=adapter.scraper_name,
            test_case_name=self.case_name,
            status=status,
            message=message,
            duration=duration,
            details=execution_result
        )

class TestLocalHtmlProcessing(TestCase):
    """
    Tests if the scraper can process a local HTML file (served via local HTTP server).
    """
    def __init__(self):
        super().__init__(
            case_name="test_local_html_processing",
            description="Verify scraper can process a local HTML file served by the local HTTP server."
        )

    def applies_to(self, adapter_capabilities: List[str]) -> bool:
        return "scrape_single_url" in adapter_capabilities

    def run(self, adapter: ScraperAdapter, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        start_time = time.time()

        # Define a simple local HTML file for this test
        local_html_filename = "common/simple_page.html" # Relative to test_data_root
        local_html_file_path_on_disk = test_data_root / local_html_filename
        
        # Ensure the test HTML file exists
        if not local_html_file_path_on_disk.exists():
            return TestResult(
                scraper_name=adapter.scraper_name,
                test_case_name=self.case_name,
                status="FAIL", # Should be SKIP or ERROR if file setup is wrong
                message=f"Test setup error: Local HTML file {local_html_file_path_on_disk} not found.",
                duration=time.time() - start_time
            )
        
        # The URL for the scraper will point to the local HTTP server
        test_local_url = f"{local_http_server_url.rstrip('/')}/{local_html_filename}"

        params = {
            "url": test_local_url, # This is the URL on the local server
            # We're telling the adapter that the original source for this URL is a local file
            # so it can construct the command correctly if it needs to differentiate.
            # However, the adapter for mono_basic_html already takes the full URL.
            # This param might be more for the test case's clarity or future adapters.
            # Let's use the target_url_is_local_file param for adapters that might need the original file path.
            "target_url_is_local_file": True, # True because it's served from local disk via HTTP server
            "actual_file_path_for_adapter": str(local_html_file_path_on_disk), # For adapters that might need direct file path.
                                                                            # MonoBasicAdapter uses this to form the URL.
            "output_filename": f"{adapter.scraper_name}_local_processing_test.txt"
        }

        # Adjust params for mono_basic_adapter to use the actual file path for URL construction
        if adapter.scraper_name == "mono_basic_html":
             params["url"] = str(local_html_filename) # Pass the relative path for mono_basic_adapter

        test_case_output_dir = temp_output_dir / self.case_name
        test_case_output_dir.mkdir(parents=True, exist_ok=True)
        
        execution_result = adapter.run_scraper(
            action="scrape",
            params=params,
            temp_output_dir=test_case_output_dir,
            local_http_server_url=local_http_server_url
        )
        duration = time.time() - start_time

        if execution_result["success"]:
            status = "PASS"
            message = f"Successfully processed local HTML file via {test_local_url}. Exit code {execution_result['exit_code']}."
        else:
            status = "FAIL"
            message = f"Failed to process local HTML file via {test_local_url}. Stderr: {execution_result['stderr']}"
            
        return TestResult(
            scraper_name=adapter.scraper_name,
            test_case_name=self.case_name,
            status=status,
            message=message,
            duration=duration,
            details=execution_result
        )
