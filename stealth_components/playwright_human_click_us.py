import random
import time

# Playwright imports
try:
    from playwright.sync_api import Page, Locator, PlaywrightTimeoutError, Error as PlaywrightError
    PLAYWRIGHT_AVAILABLE = True
except ImportError: # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False
    # Dummy types for when Playwright is not available
    class Page: pass
    class Locator: pass
    class PlaywrightTimeoutError(Exception): pass # Inherit from Exception for basic error handling
    class PlaywrightError(Exception): pass


def _get_simple_random_delay(min_seconds: float = 0.2, max_seconds: float = 0.8, multiplier: float = 1.0) -> None:
    """Simple random delay. A more configurable version is planned for random_delay_util_us.py"""
    if not PLAYWRIGHT_AVAILABLE: # Should not be called if playwright isn't there, but as a safe guard
        return
    time.sleep(random.uniform(min_seconds * multiplier, max_seconds * multiplier))


def human_click(
    page: Page,
    locator: Locator,
    timeout: int = 10000,
    min_move_delay: float = 0.1, # Min delay for mouse move sequence
    max_move_delay: float = 0.4, # Max delay for mouse move sequence
    pre_click_delay_min: float = 0.05, # Min delay after mouse move, before click
    pre_click_delay_max: float = 0.25, # Max delay after mouse move, before click
    post_click_delay_min: float = 0.3, # Min delay after click
    post_click_delay_max: float = 0.7   # Max delay after click
    ) -> bool:
    """
    Attempts a human-like click on a Playwright Locator.
    Includes random mouse movements and delays.
    Adapted from my_scrapers/unified_scraper.py/_human_click.

    Args:
        page: The Playwright Page object.
        locator: The Playwright Locator for the element to click.
        timeout: Maximum time (ms) to wait for the element to be visible.
        min_move_delay, max_move_delay: Range for total mouse movement duration.
        pre_click_delay_min, pre_click_delay_max: Range for delay just before clicking.
        post_click_delay_min, post_click_delay_max: Range for delay after clicking.

    Returns:
        True if the click was attempted (either human-like or direct fallback),
        False if the locator was not found or an error prevented even a fallback click.
    """
    if not PLAYWRIGHT_AVAILABLE:
        # print("Playwright not available, cannot perform human_click.") # Or raise error
        return False

    try:
        # print(f"[DEBUG] Attempting human_click on locator...") # Optional logging
        locator.wait_for(state="visible", timeout=timeout)
        bounding_box = locator.bounding_box()

        if not bounding_box:
            # print(f"[WARNING] Could not get bounding box for locator. Using direct click.") # Optional logging
            locator.click(timeout=timeout) # Use Playwright's click which waits
            _get_simple_random_delay(post_click_delay_min, post_click_delay_max)
            return True

        # Calculate a random point within the bounding box
        # Aim for roughly the center 50% of the element to avoid edges if possible
        width_margin = bounding_box['width'] * 0.25
        height_margin = bounding_box['height'] * 0.25

        target_x = bounding_box['x'] + width_margin + (random.uniform(0, 1) * (bounding_box['width'] - 2 * width_margin))
        target_y = bounding_box['y'] + height_margin + (random.uniform(0, 1) * (bounding_box['height'] - 2 * height_margin))

        # Ensure target is still within bounds if margins were too large for small elements
        target_x = max(bounding_box['x'] + 1, min(target_x, bounding_box['x'] + bounding_box['width'] - 1))
        target_y = max(bounding_box['y'] + 1, min(target_y, bounding_box['y'] + bounding_box['height'] - 1))

        # print(f"[DEBUG] Moving mouse to element area: ({target_x:.0f}, {target_y:.0f})") # Optional logging
        # Simulate mouse movement with random steps and duration
        # Playwright's page.mouse.move takes target x, y and options like 'steps'
        # The 'steps' parameter controls the number of intermediate points for the mouse move.
        page.mouse.move(target_x, target_y, steps=random.randint(5, 15))
        _get_simple_random_delay(min_move_delay, max_move_delay, multiplier=0.2) # Small delay during overall move action

        _get_simple_random_delay(pre_click_delay_min, pre_click_delay_max) # Pause before click

        # print(f"[DEBUG] Performing mouse click at ({target_x:.0f}, {target_y:.0f})") # Optional logging
        page.mouse.down()
        _get_simple_random_delay(0.05, 0.15) # Very short delay between mouse down and up
        page.mouse.up()

        # print(f"[INFO] Human-like click potentially successful.") # Optional logging
        _get_simple_random_delay(post_click_delay_min, post_click_delay_max) # Pause after click
        return True

    except PlaywrightTimeoutError:
        # print(f"[WARNING] Locator not visible for human_click within {timeout/1000}s. Trying direct click.") # Optional logging
        try:
            locator.click(timeout=timeout)
            _get_simple_random_delay(post_click_delay_min, post_click_delay_max)
            return True
        except Exception as direct_click_err:
            # print(f"[ERROR] Direct click also failed: {direct_click_err}") # Optional logging
            return False
    except Exception as e:
        # print(f"[WARNING] Human-like click failed: {e}. Falling back to direct click.") # Optional logging
        try:
            locator.click(timeout=timeout)
            _get_simple_random_delay(post_click_delay_min, post_click_delay_max)
            return True
        except Exception as click_err:
            # print(f"[ERROR] Direct click also failed: {click_err}") # Optional logging
            return False

if __name__ == '__main__': # pragma: no cover
    if not PLAYWRIGHT_AVAILABLE:
        print("Playwright is not installed. Skipping human_click example.")
    else:
        from playwright.sync_api import sync_playwright

        pw_manager = sync_playwright().start()
        browser = pw_manager.chromium.launch(headless=False, slow_mo=50)
        page = browser.new_page()

        try:
            # Example usage: Go to a page with a button
            page.goto("https://www.example.com") # A simple page

            # Create a dummy button to click for testing if example.com doesn't have one
            page.evaluate("""() => {
                const button = document.createElement('button');
                button.innerText = 'Click Me for Human Click Test';
                button.id = 'testButtonHumanClick';
                button.style.position = 'absolute';
                button.style.top = '100px';
                button.style.left = '100px';
                button.style.padding = '20px';
                button.style.border = '1px solid blue';
                button.onclick = () => {
                    console.log('Test button clicked!');
                    button.innerText = 'Clicked!';
                };
                document.body.appendChild(button);
            }""")

            time.sleep(1) # wait for button to be on page

            button_locator = page.locator("#testButtonHumanClick")

            if button_locator.is_visible():
                print("Attempting human_click on the test button...")
                clicked_successfully = human_click(page, button_locator)
                if clicked_successfully:
                    print("human_click reported success.")
                    # Check if button text changed or console message appeared
                    time.sleep(2) # Give time for JS to execute
                    button_text = button_locator.text_content()
                    print(f"Button text after click: '{button_text}'")
                    if button_text == "Clicked!":
                        print("SUCCESS: Button click confirmed by text change.")
                    else:
                        print("NOTE: Button text did not change as expected. Click might have partially failed or page JS is slow.")
                else:
                    print("human_click reported failure.")
            else:
                print("Test button not visible on example.com. Cannot run click test.")

            # Example with a non-existent element to test timeout/fallback
            print("\nAttempting human_click on a non-existent element (should fallback or fail)...")
            non_existent_locator = page.locator("#nonExistentButton")
            clicked_non_existent = human_click(page, non_existent_locator, timeout=2000)
            print(f"human_click on non-existent element reported: {clicked_non_existent} (expected False or True if it somehow clicks body)")

        except Exception as main_ex:
            print(f"An error occurred during the example: {main_ex}")
        finally:
            print("Closing browser...")
            browser.close()
            pw_manager.stop()
            print("Playwright stopped.")
