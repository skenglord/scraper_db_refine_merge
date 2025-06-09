from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict, Any, NamedTuple

# Using NamedTuple for a simple, immutable TestResult structure
class TestResult(NamedTuple):
    scraper_name: str
    test_case_name: str
    status: str  # "PASS", "FAIL", "SKIP"
    message: str = ""
    duration: float = 0.0
    details: Dict[str, Any] = {} # For any extra context

class TestCase(ABC):
    """
    Abstract Base Class for individual test cases.
    """

    def __init__(self, case_name: str, description: str):
        self.case_name = case_name
        self.description = description

    def applies_to(self, adapter_capabilities: List[str]) -> bool:
        """
        Determines if this test case is applicable to a scraper
        based on its declared capabilities.
        Default implementation assumes the test applies to all scrapers.
        Subclasses should override this if the test is capability-specific.

        Args:
            adapter_capabilities (List[str]): Capabilities of the scraper adapter.

        Returns:
            bool: True if the test case applies, False otherwise.
        """
        return True

    @abstractmethod
    def run(self, adapter: Any, local_http_server_url: str, test_data_root: Path, temp_output_dir: Path) -> TestResult:
        """
        Executes the test case against the given scraper adapter.

        Args:
            adapter (ScraperAdapter): The adapter for the scraper to be tested.
            local_http_server_url (str): Base URL of the local HTTP server for test files.
            test_data_root (Path): Path to the root of the test_data directory.
            temp_output_dir (Path): Temporary directory for this test run's outputs.
                                    The test case should create a sub-directory within this
                                    for its own specific files if needed, to avoid conflicts.

        Returns:
            TestResult: The outcome of the test.
        """
        pass
