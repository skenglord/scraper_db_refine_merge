import random
import re # For route interception patterns

try:
    from playwright.sync_api import Page, Route
    PLAYWRIGHT_AVAILABLE = True
except ImportError: # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False
    class Page: pass
    class Route: # Dummy for type hinting if playwright is not available
        def abort(self, error_code: str | None = None): pass
        def continue_(self, **kwargs): pass
        def fulfill(self, **kwargs): pass


# A selection of modern user agents. More can be added.
# This list can be shared or imported from a central constants module in a larger project.
MODERN_USER_AGENTS_SUBSET = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
]

# Resource types to potentially block for faster, less detectable scraping
COMMON_RESOURCE_TYPES_TO_BLOCK = ["image", "stylesheet", "font", "media", "other"] # "script" can also be blocked but may break sites

# Regex patterns for common tracking/analytics domains or resource types to block
# These are examples; a real list would be more extensive and maintained.
DEFAULT_BLOCK_PATTERNS = [
    re.compile(r"(google-analytics\.com|googletagmanager\.com|api\.segment\.io)"), # Analytics & Tag Managers
    re.compile(r"(\.png$|\.jpg$|\.jpeg$|\.gif$|\.webp$)", re.IGNORECASE), # Common image formats
    re.compile(r"(\.css$)", re.IGNORECASE), # Stylesheets (can be useful for speed, but alters appearance)
    re.compile(r"(\.woff$|\.woff2$|\.ttf$|\.eot$)", re.IGNORECASE), # Fonts
    # re.compile(r"(\.mp4$|\.webm$|\.ogg$)", re.IGNORECASE), # Media files
]


def _should_block_resource(route: Route, block_patterns: list[re.Pattern], resource_types_to_block: list[str] | None) -> bool:
    """
    Determines if a resource should be blocked based on its URL and type.
    """
    if resource_types_to_block and route.request.resource_type in resource_types_to_block:
        # print(f"Blocking resource type: {route.request.resource_type} for URL: {route.request.url}") # Optional logging
        return True
    for pattern in block_patterns:
        if pattern.search(route.request.url):
            # print(f"Blocking URL pattern: {pattern.pattern} for URL: {route.request.url}") # Optional logging
            return True
    return False

def setup_enhanced_playwright_page(
    page: Page,
    user_agents_list: list[str] | None = None,
    block_patterns: list[re.Pattern] | None = None,
    resource_types_to_block: list[str] | None = None,
    extra_headers: dict | None = None,
    set_viewport: dict | None = None, # e.g., {"width": 1920, "height": 1080}
    emulate_device: str | None = None, # e.g., "iPhone 13 Pro" - uses page.emulate_device
    set_locale: str | None = None, # e.g., "en-US"
    set_timezone: str | None = None, # e.g., "America/New_York"
    bypass_csp: bool = False # Caution: use only if necessary and understand implications
    ):
    """
    Applies various enhancements to a Playwright Page object for stealth and efficiency.
    This function serves as a collection of common setup steps.

    Args:
        page: The Playwright Page object to enhance.
        user_agents_list: A list of User-Agent strings. If None, uses MODERN_USER_AGENTS_SUBSET.
        block_patterns: A list of compiled regex patterns. URLs matching these patterns will be blocked.
                        If None, uses DEFAULT_BLOCK_PATTERNS.
        resource_types_to_block: A list of resource types (e.g., "image", "stylesheet") to block.
                                 If None, uses COMMON_RESOURCE_TYPES_TO_BLOCK.
        extra_headers: A dictionary of extra HTTP headers to set for all requests from this page.
        set_viewport: A dictionary specifying viewport size (e.g., {"width": 1920, "height": 1080}).
        emulate_device: Name of a device to emulate (see Playwright device descriptors). Overrides viewport, UA.
        set_locale: Locale to set (e.g., "en-US").
        set_timezone: Timezone ID to set (e.g., "America/New_York").
        bypass_csp: Whether to bypass Content Security Policy. Use with caution.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return

    final_user_agents = user_agents_list if user_agents_list is not None else MODERN_USER_AGENTS_SUBSET
    final_block_patterns = block_patterns if block_patterns is not None else DEFAULT_BLOCK_PATTERNS
    final_resource_types_to_block = resource_types_to_block if resource_types_to_block is not None else COMMON_RESOURCE_TYPES_TO_BLOCK

    # 1. Set User-Agent (if not emulating a device which sets its own UA)
    if not emulate_device and final_user_agents:
        user_agent = random.choice(final_user_agents)
        page.set_extra_http_headers({"User-Agent": user_agent})
        # print(f"Set User-Agent to: {user_agent}") # Optional logging

    # 2. Set Viewport, Locale, Timezone (if not emulating)
    if not emulate_device:
        if set_viewport:
            page.set_viewport_size(set_viewport)
            # print(f"Set viewport to: {set_viewport}") # Optional logging
        if set_locale:
            # Note: page.set_locale() is not a direct method. Emulation or context options are typical.
            # This can be achieved via context.new_page(locale=set_locale)
            # For an existing page, it's more complex. We can set headers like Accept-Language.
            accept_language_header = {"Accept-Language": set_locale}
            if extra_headers:
                extra_headers.update(accept_language_header)
            else:
                extra_headers = accept_language_header
            # print(f"Set Accept-Language header for locale: {set_locale}") # Optional logging
        if set_timezone:
            # page.set_timezone_id(set_timezone) # Not a direct method on page
            # Achieved via context.new_page(timezone_id=set_timezone)
            # print(f"Timezone ({set_timezone}) would need to be set at context level.") # Optional logging
            pass # Placeholder, as this needs context-level setting

    # 3. Emulate Device (this will override UA, viewport, etc.)
    if emulate_device:
        try:
            # Ensure playwright context has device descriptors available.
            # This might require having playwright.devices available.
            # For simplicity, this example assumes device name is valid.
            page.emulate_device(emulate_device) # This method is on BrowserContext or Page
            # print(f"Emulated device: {emulate_device}") # Optional logging
        except Exception as e: # pragma: no cover
            print(f"Could not emulate device '{emulate_device}': {e}. Make sure Playwright's device descriptors are available.")


    # 4. Set Extra HTTP Headers
    if extra_headers:
        page.set_extra_http_headers(extra_headers)
        # print(f"Set extra HTTP headers: {extra_headers}") # Optional logging

    # 5. Route Interception to Block Resources
    if final_block_patterns or final_resource_types_to_block:
        # print(f"Setting up route interception to block resources...") # Optional logging
        # Ensure patterns are compiled if they are strings
        compiled_patterns = []
        for p in final_block_patterns:
            if isinstance(p, str):
                try:
                    compiled_patterns.append(re.compile(p, re.IGNORECASE))
                except re.error as re_err: # pragma: no cover
                    print(f"Warning: Invalid regex pattern provided and skipped: {p} ({re_err})")
            else: # Assuming it's already a compiled regex
                compiled_patterns.append(p)

        def handle_route(route: Route):
            if _should_block_resource(route, compiled_patterns, final_resource_types_to_block):
                route.abort()
            else:
                route.continue_()

        try:
            page.route("**/*", handle_route)
            # print("Route interception active.") # Optional logging
        except Exception as e: # pragma: no cover
            print(f"Error setting up route interception: {e}")

    # 6. Bypass Content Security Policy (CSP)
    if bypass_csp:
        try:
            page.set_bypass_csp(True)
            # print("Bypassing Content Security Policy.") # Optional logging
        except Exception as e: # pragma: no cover
            print(f"Error bypassing CSP: {e}")

    # Add other enhancements like:
    # - Setting geolocation: page.context.set_geolocation({"longitude": ..., "latitude": ...}) (context level)
    # - Setting permissions: page.context.grant_permissions(["geolocation"]) (context level)
    # - Injecting scripts: page.add_init_script("() => { Object.defineProperty(navigator, 'webdriver', { get: () => undefined }) }")

    # print("Page enhancements applied.") # Optional logging


if __name__ == '__main__': # pragma: no cover
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright is not installed. Skipping setup_enhanced_playwright_page example.")
    else:
        from playwright.sync_api import sync_playwright

        pw_manager = sync_playwright().start()
        browser = pw_manager.chromium.launch(headless=True) # Keep headless for CI/testing
        context = browser.new_context() # Create a context to potentially set locale/timezone
        page = context.new_page()

        print("Applying page enhancements...")
        setup_enhanced_playwright_page(
            page,
            set_viewport={"width": 1280, "height": 720},
            extra_headers={"X-Custom-Header": "TestValue"},
            # To test resource blocking, navigate to a resource-heavy page.
            # block_patterns=[re.compile(r"\.jpg$")], # Example: block only jpg images
            resource_types_to_block=["image", "font"] # Block all images and fonts
        )

        print("\nEnhanced page setup complete. Navigating to test page (httpbin.org)...")
        try:
            # Test User-Agent and Headers
            print("\n--- Testing Headers and User Agent ---")
            page.goto("https://httpbin.org/headers", wait_until="networkidle")
            headers_content = page.content()
            print(f"Headers from httpbin: {headers_content[:500]}...") # Show first 500 chars
            if "TestValue" in headers_content:
                print("SUCCESS: Custom header 'X-Custom-Header' found.")
            else:
                print("FAILURE: Custom header 'X-Custom-Header' NOT found.")

            # Test resource blocking (manual check by observing network tab in headed mode, or by checking for missing elements)
            print("\n--- Testing Resource Blocking (Images and Fonts) ---")
            print("Navigating to example.com (images/fonts should be blocked if setup correctly)...")
            # For this test, we'd ideally load a page and check if images are missing.
            # example.com doesn't have many images. A more complex page would be better.
            # Let's try to load an image directly and see if it's blocked.

            image_load_failed = False
            try:
                # Try to navigate to an image URL; if blocked, it should fail.
                # Using a known small image URL for testing.
                # This specific test might be flaky depending on how httpbin handles direct image access
                # or if other network rules interfere.
                print("Attempting to navigate to a JPG image (should be blocked by type 'image')...")
                page.goto("https://httpbin.org/image/jpeg", timeout=5000)
                # If it loads, blocking might not be working as expected for direct navigation to resource
                print("WARNING: Direct navigation to image succeeded, resource blocking for 'image' type might not be fully effective for direct nav.")
            except PlaywrightTimeoutError:
                image_load_failed = True
                print("SUCCESS: Navigation to JPG image timed out, likely blocked as expected.")
            except Exception as e:
                if "net::ERR_FAILED" in str(e) or "Navigation failed because page was closed" in str(e): # net::ERR_BLOCKED_BY_CLIENT is ideal but hard to catch
                    image_load_failed = True
                    print(f"SUCCESS: Navigation to JPG image failed as expected (likely blocked): {str(e)[:100]}")
                else:
                    print(f"WARNING: Navigation to JPG image failed with an unexpected error: {e}")

            if not image_load_failed and "image" in (resource_types_to_block or []):
                 print("NOTE: Image blocking test was inconclusive via direct navigation. Check a page with <img> tags.")


            print("\n--- Emulation Test (Illustrative - not fully run here) ---")
            # To test emulation, you'd typically create a new page or context
            # For example:
            # context_emulated = browser.new_context(device_scale_factor=2, user_agent="SpecificEmulatedUA/1.0")
            # page_emulated = context_emulated.new_page()
            # setup_enhanced_playwright_page(page_emulated, emulate_device="iPhone 13 Pro")
            # page_emulated.goto("https://httpbin.org/headers")
            # ... check headers ...
            # page_emulated.close()
            # context_emulated.close()
            print("Emulation setup would typically occur on a new context or page with device parameters.")


        except Exception as main_ex:
            print(f"An error occurred during the example: {main_ex}")
            import traceback
            traceback.print_exc()
        finally:
            print("\nClosing browser...")
            page.close()
            context.close()
            browser.close()
            pw_manager.stop()
            print("Playwright stopped.")
