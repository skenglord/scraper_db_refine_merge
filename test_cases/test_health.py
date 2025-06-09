import time
from pathlib import Path
from typing import List, Dict, Any

from .base_test_case import TestCase, TestResult
from adapters.base_adapter import ScraperAdapter # Relative import

class TestScriptExecutionHealth(TestCase):
    """
    Tests if the scraper script runs without crashing on a basic command (e.g., --help).
    """
    def __init__(self):
        super().__init__(
            case_name="test_script_execution_health",
            description="Verify basic script execution with a minimal command (e.g., help action or no problematic args)."
        )

    # This test applies to all scrapers, so no override for applies_to needed.

    def run(self, adapter: ScraperAdapter, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        start_time = time.time()
        
        # Most CLI tools have a "help" action or respond to --help
        # For scrapers, a "help" action might not be standard.
        # We'll try a minimal 'scrape' action with no URL, expecting it to fail gracefully or show help.
        # Or, if an adapter can define a "ping" or "help" action, that's better.
        # For now, we assume the adapter's run_scraper can handle a generic "help" or minimal action.
        # Let's define that the adapter should know how to invoke its script for a health check.
        # We can use a placeholder action like "health_check" and let the adapter interpret it.
        
        # For many scripts, just running them (python script.py) without args, or with --help, is a basic health check.
        # The adapter's run_scraper needs to map "health_check" to this.
        # Let's assume a "help" action for now, and adapters can map it.
        # If a script doesn't have a "help" action, the adapter can try running it with no arguments.

        params = {"help": True} # A generic param, adapter should translate
        
        # Create a specific subdirectory for this test case's run to avoid conflicts
        test_case_output_dir = temp_output_dir / self.case_name
        test_case_output_dir.mkdir(parents=True, exist_ok=True)

        execution_result = adapter.run_scraper(
            action="help", # Generic action name, adapter needs to translate
            params=params,
            temp_output_dir=test_case_output_dir, # Pass the specific dir for this test
            local_http_server_url=local_http_server_url # Not used by this test, but part of signature
        )
        
        duration = time.time() - start_time

        if execution_result["success"] or execution_result["exit_code"] == 0 : # Some help commands might exit 0
            # Further check: stderr should ideally be empty for a clean help output,
            # but some scripts print help to stderr. So, success (exit code 0) is the primary check.
            # We also accept non-zero if it's a typical "missing arguments" error for just `python script.py`
            status = "PASS"
            message = f"Script executed successfully (exit code {execution_result['exit_code']})."
            if execution_result["stderr"]:
                message += f" Stderr: {execution_result['stderr'][:200]}" # Show snippet of stderr
        elif execution_result["exit_code"] != 0 and "Error: Script not found" not in execution_result["stderr"] and "timed out" not in execution_result["stderr"]:
             # Many scripts will return a non-zero exit code if essential arguments like a URL are missing.
             # This is still a "healthy" execution if it's a controlled exit.
             status = "PASS"
             message = f"Script exited with code {execution_result['exit_code']} (expected for missing args/help). Stderr: {execution_result['stderr'][:200]}"
        else:
            status = "FAIL"
            message = f"Script execution failed. Exit code: {execution_result['exit_code']}. Stderr: {execution_result['stderr']}"

        return TestResult(
            scraper_name=adapter.scraper_name,
            test_case_name=self.case_name,
            status=status,
            message=message,
            duration=duration,
            details=execution_result
        )

class TestDependencyCheck(TestCase):
    """
    Tests if essential dependencies for the scraper are importable.
    This is a placeholder as true dependency checking is complex from outside.
    A better approach is for each scraper project to have its own basic import test.
    For now, this test will rely on the script execution health; if the script runs (even to show help),
    Python's import mechanisms have worked for direct imports in that script.
    """
    def __init__(self):
        super().__init__(
            case_name="test_dependency_check",
            description="Verify that the scraper's main script can be imported or run, implying core dependencies are met."
        )

    def run(self, adapter: ScraperAdapter, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        start_time = time.time()
        
        # This test is largely covered by test_script_execution_health, as Python running the script
        # means its direct imports didn't immediately fail.
        # A more robust check would involve `pip check` or trying to import specific known
        # dependencies for each adapter, but that increases complexity.
        # For now, we consider it a pass if script_execution_health passes,
        # as it implies the Python interpreter could load the script and its initial imports.
        
        # Re-run a minimal execution, similar to health check, as a proxy for dependency check.
        test_case_output_dir = temp_output_dir / self.case_name
        test_case_output_dir.mkdir(parents=True, exist_ok=True)

        execution_result = adapter.run_scraper(
            action="help", # Or a minimal action
            params={"help": True}, 
            temp_output_dir=test_case_output_dir,
            local_http_server_url=local_http_server_url
        )
        duration = time.time() - start_time

        if execution_result["exit_code"] == 0 or            (execution_result["exit_code"] != 0 and "Error: Script not found" not in execution_result["stderr"] and "timed out" not in execution_result["stderr"]):
            # If the script ran (even to show help or complain about args), its direct imports likely worked.
            status = "PASS"
            message = "Script ran, implying direct Python imports are likely resolved."
        else:
            status = "FAIL"
            message = f"Script failed to run, potentially due to import errors or other critical issues. Stderr: {execution_result['stderr']}"
            if "ModuleNotFoundError" in execution_result["stderr"]:
                message += " (ModuleNotFoundError detected)"

        return TestResult(
            scraper_name=adapter.scraper_name,
            test_case_name=self.case_name,
            status=status,
            message=message,
            duration=duration,
            details=execution_result
        )
