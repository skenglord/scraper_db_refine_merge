# playwright_stealth_integration_fs.py

# Note: To use this module, you need to install playwright_stealth:
# pip install playwright-stealth

try:
    from playwright.sync_api import sync_playwright, Browser, Page, Playwright
    # It's good practice to also import specific exceptions if you plan to handle them.
    # from playwright.sync_api import Error as PlaywrightError
    # from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError: # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False
    # Define dummy types for when Playwright is not available, to allow type hinting
    # and prevent runtime errors if this module is imported when Playwright is missing.
    class Browser: pass
    class Page: pass
    class Playwright: # Dummy for the main Playwright context manager object
        def __enter__(self): return self
        def __exit__(self, exc_type, exc_val, exc_tb): pass
        def chromium(self): return DummyBrowserLauncher()
        def firefox(self): return DummyBrowserLauncher()
        def webkit(self): return DummyBrowserLauncher()
        def stop(self): pass # For playwright.stop()

    class DummyBrowserLauncher: # Dummy for playwright.chromium etc.
        def launch(self, *args, **kwargs) -> Browser: return Browser()
        def connect(self, *args, **kwargs) -> Browser: return Browser()
        # Add other methods like launch_persistent_context if needed by your examples

    def sync_playwright() -> Playwright:
        """Returns a dummy Playwright context manager if Playwright is not installed."""
        return Playwright()

try:
    from playwright_stealth import stealth_sync as stealth
except ImportError: # pragma: no cover
    stealth = None # type: ignore
    if PLAYWRIGHT_AVAILABLE: # Only print warning if Playwright itself is there but stealth is missing
        print("Warning: playwright_stealth is not installed. Stealth features will not be applied.")
        print("You can install it with: pip install playwright-stealth")


def launch_stealth_browser(
    headless: bool = True,
    browser_type: str = "chromium", # Allow choosing browser type
    channel: str | None = None, # For specific browser channels like 'chrome', 'msedge'
    slow_mo: float | None = None # Optional slow_mo for debugging
) -> tuple[Playwright | None, Browser | None, Page | None]:
    """
    Launches a browser (Chromium by default) with Playwright, creates a new page,
    and applies playwright_stealth if available.

    Args:
        headless: Whether to run the browser in headless mode.
        browser_type: "chromium", "firefox", or "webkit".
        channel: Specific browser channel (e.g., "chrome", "msedge"). Only for chromium.
        slow_mo: Slow down Playwright operations by the given ms.

    Returns:
        A tuple containing the Playwright instance, Browser instance, and Page instance.
        Returns (None, None, None) if Playwright is not available.

    Note: The caller is responsible for closing the browser and stopping Playwright
    (e.g., using a try/finally block or context managers).
    Example:
        pw, browser, page = launch_stealth_browser()
        if page:
            try:
                # ... use page ...
            finally:
                if browser: browser.close()
                if pw: pw.stop() # Important for proper cleanup if sync_playwright().start() was used implicitly
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright is not installed. Cannot launch browser.")
        return None, None, None

    if stealth is None and PLAYWRIGHT_AVAILABLE: # Check again in case it was imported later by another module
        print("Warning: playwright_stealth not found. Proceeding without stealth.")

    pw_instance: Playwright | None = None
    browser_instance: Browser | None = None
    page_instance: Page | None = None

    try:
        pw_instance = sync_playwright().start() # Start Playwright

        launch_options = {"headless": headless}
        if slow_mo is not None:
            launch_options["slow_mo"] = slow_mo
        if channel and browser_type == "chromium":
            launch_options["channel"] = channel

        if browser_type == "chromium":
            browser_instance = pw_instance.chromium.launch(**launch_options)
        elif browser_type == "firefox":
            browser_instance = pw_instance.firefox.launch(**launch_options)
        elif browser_type == "webkit":
            browser_instance = pw_instance.webkit.launch(**launch_options)
        else:
            print(f"Unsupported browser type: {browser_type}. Defaulting to Chromium.")
            browser_instance = pw_instance.chromium.launch(**launch_options)

        if not browser_instance: # Should not happen if Playwright is working
            raise RuntimeError("Failed to launch browser instance.")

        page_instance = browser_instance.new_page()

        if stealth and page_instance:
            try:
                stealth(page_instance)
                # print("playwright_stealth applied successfully.") # Optional logging
            except Exception as e: # pragma: no cover
                print(f"Error applying playwright_stealth: {e}")

        return pw_instance, browser_instance, page_instance

    except Exception as e: # Catch any error during setup
        print(f"Error launching stealth browser: {e}")
        if browser_instance:
            try:
                browser_instance.close()
            except Exception as close_e: # pragma: no cover
                print(f"Error closing browser instance during cleanup: {close_e}")
        if pw_instance:
            try:
                pw_instance.stop()
            except Exception as stop_e: # pragma: no cover
                print(f"Error stopping Playwright instance during cleanup: {stop_e}")
        return None, None, None


if __name__ == "__main__": # pragma: no cover
    print("Attempting to launch a stealth browser (Chromium)...")
    # Note: playwright.stop() is crucial for cleaning up the Playwright child process.
    # If sync_playwright() is used as a context manager, .stop() is called automatically.
    # If sync_playwright().start() is used, .stop() must be called manually.

    pw, browser, page = launch_stealth_browser(headless=True)

    if page and browser and pw:
        print(f"Browser launched. Page URL: {page.url} (should be about:blank)")
        try:
            # Example: Navigate and get title
            page.goto("https://httpbin.org/user-agent", wait_until="networkidle")
            print(f"Navigated to: {page.url}")
            content = page.content()
            print(f"Page content (User-Agent check): {content[:200]}...") # Print first 200 chars

            # Test if stealth is working by checking for common stealth indicators
            # (This is a basic check; more sophisticated checks might be needed)
            is_webdriver = page.evaluate("navigator.webdriver")
            print(f"navigator.webdriver: {is_webdriver}") # Should be false after stealth

        except Exception as e:
            print(f"Error during page interaction: {e}")
        finally:
            print("Closing browser and Playwright...")
            try:
                browser.close()
            except Exception as e:
                print(f"Error closing browser: {e}")
            try:
                pw.stop() # Essential for cleanup
            except Exception as e:
                print(f"Error stopping Playwright: {e}")
            print("Cleanup finished.")
    else:
        print("Failed to launch browser or page.")

    # Example for Firefox (if installed)
    # print("\nAttempting to launch a stealth browser (Firefox)...")
    # pw_ff, browser_ff, page_ff = launch_stealth_browser(headless=True, browser_type="firefox")
    # if page_ff and browser_ff and pw_ff:
    #     print(f"Firefox browser launched. Page URL: {page_ff.url}")
    #     try:
    #         page_ff.goto("https://httpbin.org/headers", wait_until="networkidle")
    #         print(f"Firefox headers: {page_ff.evaluate('() => JSON.stringify(performance.getEntriesByType(\"navigation\")[0].serverTiming, null, 2)')}")
    #     finally:
    #         browser_ff.close()
    #         pw_ff.stop()
    # else:
    #     print("Failed to launch Firefox browser or page.")
