import time
import random

try:
    from playwright.sync_api import Page, PlaywrightTimeoutError, Error as PlaywrightError
    # We will attempt to import human_click from the sibling module
    # If this agent cannot guarantee PYTHONPATH or relative import resolution,
    # a fallback to basic click will be used.
    from .playwright_human_click_us import human_click
    # If the above import fails in the execution environment, the except block below will catch it.
    # For local testing, ensure stealth_components is a package or on PYTHONPATH.
    HUMAN_CLICK_AVAILABLE = True
except ImportError: # pragma: no cover
    HUMAN_CLICK_AVAILABLE = False
    # Dummy types if Playwright itself is missing or if human_click cannot be imported
    class Page: pass
    class PlaywrightTimeoutError(Exception): pass
    class PlaywrightError(Exception): pass
    # Define a fallback click if human_click is not available
    def human_click(page: Page, locator, timeout: int = 5000, **kwargs) -> bool:
        print("[WARNING] human_click not available/imported. Using direct locator.click() for overlay.")
        try:
            locator.click(timeout=timeout)
            time.sleep(0.5 + random.uniform(0.1, 0.4)) # Small delay after direct click
            return True
        except Exception as e:
            # print(f"Direct click for overlay failed: {e}") # Optional logging
            return False

# List of common selectors for overlays, cookie banners, etc.
DEFAULT_OVERLAY_SELECTORS = [
    'a.cb-seen-accept',  # Common cookie banner accept button
    'button#onetrust-accept-btn-handler',  # OneTrust cookie consent
    'button[data-testid="accept-all-cookies"]',
    'button:has-text("Accept all")',
    'button:has-text("Accept All")',
    'button:has-text("Accept Cookies")',
    'button:has-text("I AGREE")',
    'button:has-text("I Accept")',
    'button:has-text("Agree")',
    'button:has-text("No problem")',
    'button:has-text("Got it")',
    'button:has-text("Understood")',
    '[aria-label*="close" i]',  # Case-insensitive close button by aria-label
    '[aria-label*="accept" i]', # Case-insensitive accept button by aria-label
    '[role="button"][aria-label*="close" i]',
    '.modal-close',
    'button.close',
    '.cookie-banner-accept-button',
    '#cookieChoiceDismiss', # Google's older cookie consent
    'div[id*="cookie"] button[data-action*="accept"]', # Generic pattern
    'button[class*="consent" i][class*="accept" i]', # Buttons with "consent" and "accept" in class
    'button[id*="cookie" i][id*="accept" i]',
    # Add more selectors as commonly encountered
]

def handle_overlays(
    page: Page,
    custom_selectors: list[str] | None = None,
    click_timeout: int = 5000, # Timeout for individual click attempts
    quick_visibility_check_timeout: int = 2000, # Quick check before committing to a full click attempt
    check_iframes: bool = True
    ) -> bool:
    """
    Attempts to find and click common overlay elements like cookie banners,
    pop-ups, etc., to clear the view for further interaction.
    Adapted from my_scrapers/unified_scraper.py/_handle_overlays.

    Args:
        page: The Playwright Page object.
        custom_selectors: A list of custom CSS selectors for overlays,
                          which will be checked before the default ones.
        click_timeout: Timeout for the click action on an overlay element (in ms).
        quick_visibility_check_timeout: Timeout for the initial is_visible check (in ms).
        check_iframes: Whether to attempt to find and close overlays within iframes.

    Returns:
        True if an overlay was successfully handled, False otherwise.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return False

    all_selectors = (custom_selectors or []) + DEFAULT_OVERLAY_SELECTORS
    overlay_handled_in_main_page = False

    # print("[INFO] Checking for overlays and cookie banners on main page...") # Optional logging
    # Short initial delay to allow some overlays to appear
    time.sleep(random.uniform(0.3, 0.7))

    for i, selector in enumerate(all_selectors):
        try:
            # Use page.query_selector to check existence without strict timeout initially
            # Then locate the first one if multiple matches exist for a selector.
            # Some sites have multiple identical close buttons, only one needs to be clicked.
            element_locator = page.locator(selector).first

            # Quick check for visibility before attempting a more complex click
            if element_locator.is_visible(timeout=quick_visibility_check_timeout):
                # print(f"[INFO] Found potential overlay with selector: '{selector}'. Attempting click {i+1}/{len(all_selectors)}.") # Optional logging

                # Use the human_click function (imported or fallback)
                if human_click(page, element_locator, timeout=click_timeout):
                    overlay_handled_in_main_page = True
                    # print(f"[INFO] Overlay handled with selector: {selector}.") # Optional logging
                    # Wait a bit for overlay to potentially disappear or animate out
                    time.sleep(random.uniform(0.8, 1.5))

                    # After handling one, re-check if it's truly gone or if another appears.
                    # For simplicity here, we break after the first successfully handled overlay.
                    # A more robust solution might loop until no more known overlays are found.
                    break
                # else:
                    # print(f"[DEBUG] human_click returned False for selector '{selector}'.") # Optional logging

        except PlaywrightTimeoutError:
            # print(f"[DEBUG] Selector '{selector}' not visible within quick check timeout.") # Optional logging
            continue # Element not visible within the quick check, try next selector
        except PlaywrightError as pe: # Catch other Playwright errors like "strict mode violation" if .first wasn't used.
             # print(f"[DEBUG] PlaywrightError for selector '{selector}': {pe}. This might be okay if element is not unique or gone.")
             continue
        except Exception as e:
            # print(f"[DEBUG] Error trying overlay selector '{selector}': {e}") # Optional logging
            continue

    if overlay_handled_in_main_page:
        # print("[INFO] Overlay handling on main page complete.") # Optional logging
        return True

    # Optional: Check iframes if no overlay was handled on the main page
    if check_iframes:
        # print("[INFO] No overlays handled on main page. Checking iframes (if any).") # Optional logging
        iframe_overlay_handled = False
        try:
            frames = page.frames
            if len(frames) > 1: # More than just the main frame
                for frame_idx, frame in enumerate(frames[1:]): # Skip main frame (index 0)
                    # print(f"[DEBUG] Checking iframe {frame_idx+1}/{len(frames)-1} (URL: {frame.url})") # Optional logging
                    # It's good to ensure frame is loaded if possible, though playwright usually handles this
                    try:
                        frame.wait_for_load_state('domcontentloaded', timeout=1000) # Quick check
                    except PlaywrightTimeoutError:
                        # print(f"[DEBUG] Frame {frame_idx+1} did not reach domcontentloaded quickly. Proceeding cautiously.")
                        pass
                    except Exception as fe: # Catch other errors like frame detached
                        # print(f"[DEBUG] Error waiting for frame {frame_idx+1} load state: {fe}")
                        continue


                    for selector in all_selectors: # Check all selectors in each frame
                        try:
                            element_locator_frame = frame.locator(selector).first
                            if element_locator_frame.is_visible(timeout=quick_visibility_check_timeout):
                                # print(f"[INFO] Found overlay in iframe {frame_idx+1} with selector: '{selector}'. Attempting click.") # Optional logging
                                # Note: human_click expects the page object of the frame's page, which is still `page`
                                # However, the locator is correctly scoped to the frame.
                                if human_click(page, element_locator_frame, timeout=click_timeout): # Pass main page, but locator is frame-specific
                                    iframe_overlay_handled = True
                                    # print(f"[INFO] Overlay in iframe {frame_idx+1} handled with selector: {selector}.") # Optional logging
                                    time.sleep(random.uniform(0.8, 1.5))
                                    break # Break from selectors loop for this frame
                        except PlaywrightTimeoutError:
                            continue
                        except Exception as e_frame_sel:
                            # print(f"[DEBUG] Error with selector '{selector}' in iframe {frame_idx+1}: {e_frame_sel}")
                            continue
                    if iframe_overlay_handled:
                        break # Break from frames loop if handled in one frame
        except Exception as e_frames: # Catch errors related to accessing frames list or properties
            # print(f"[WARNING] Could not complete iframe check for overlays: {e_frames}")
            pass # Continue, main page check was done.

        if iframe_overlay_handled:
            # print("[INFO] Overlay handling in iframe complete.") # Optional logging
            return True

    # if not overlay_handled_in_main_page and not (check_iframes and iframe_overlay_handled):
        # print("[INFO] No overlays actively handled on main page or in iframes.") # Optional logging
    return False


if __name__ == '__main__': # pragma: no cover
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright is not installed. Skipping handle_overlays example.")
    else:
        from playwright.sync_api import sync_playwright

        # Example HTML content with a cookie banner
        html_content_with_banner = """
        <html>
            <head><title>Test Page</title></head>
            <body>
                <h1>Main Content</h1>
                <div id="cookie-banner" style="position: fixed; bottom: 0; width: 100%; background: lightgray; padding: 20px; text-align: center;">
                    This is a cookie banner.
                    <button id="accept-cookies-btn" onclick="document.getElementById('cookie-banner').style.display='none'; console.log('Cookie banner accepted.');">Accept Cookies</button>
                    <button id="close-banner-btn" aria-label="close cookie banner" onclick="document.getElementById('cookie-banner').style.display='none';">Close Banner</button>
                </div>
                <script>console.log('Page loaded');</script>
            </body>
        </html>
        """

        html_content_no_banner = """
        <html><head><title>Test Page No Banner</title></head><body><h1>Main Content</h1></body></html>
        """

        pw_manager = sync_playwright().start()
        browser = pw_manager.chromium.launch(headless=False, slow_mo=100)
        page = browser.new_page()

        try:
            print("--- Test 1: Page with a cookie banner ---")
            page.set_content(html_content_with_banner)
            time.sleep(0.5) # Allow content to render

            handled = handle_overlays(page, custom_selectors=["button#accept-cookies-btn"])
            print(f"Overlay handled (Test 1): {handled}")
            if handled:
                banner_visible = page.locator("#cookie-banner").is_visible()
                print(f"Banner visible after handling (Test 1): {banner_visible} (should be False or hidden)")

            print("\n--- Test 2: Page with a banner (using default selectors) ---")
            page.set_content(html_content_with_banner) # Reset content
            page.locator("#cookie-banner").evaluate("el => el.style.display = 'block'") # Make sure it's visible
            time.sleep(0.5)

            # Test with a selector that should match the aria-label close
            handled_default = handle_overlays(page, custom_selectors=['[aria-label*="close cookie banner" i]'])
            print(f"Overlay handled by default (Test 2): {handled_default}")
            if handled_default:
                banner_visible_default = page.locator("#cookie-banner").is_visible()
                print(f"Banner visible after handling (Test 2): {banner_visible_default} (should be False or hidden)")

            print("\n--- Test 3: Page with no banner ---")
            page.set_content(html_content_no_banner)
            time.sleep(0.5)
            handled_none = handle_overlays(page)
            print(f"Overlay handled (Test 3 - no banner): {handled_none} (should be False)")

        except Exception as main_ex:
            print(f"An error occurred during the example: {main_ex}")
            import traceback
            traceback.print_exc()
        finally:
            print("Closing browser...")
            browser.close()
            pw_manager.stop()
            print("Playwright stopped.")
