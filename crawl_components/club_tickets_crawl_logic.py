import asyncio
from playwright.async_api import Playwright, async_playwright, expect, TimeoutError as PlaywrightTimeoutError
import random
import logging

# Configure logging for clear output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Parameters ---
config = {
    "headless": False,  # Set to True for production to run browser in background
    "slow_mo": 0,       # Slows down Playwright operations by this amount (milliseconds). Useful for debugging.
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "viewport_width": 1366, # Common laptop screen width
    "viewport_height": 768, # Common laptop screen height
    "max_retries": 3,      # Max attempts for an action before failing
    "retry_delay_sec": 0.5, # Delay between retries (seconds)

    # Randomized delays for human-like interaction (seconds)
    "random_short_delay_sec_min": 0.5,
    "random_short_delay_sec_max": 1.5,
    "random_long_delay_sec_min": 2,
    "random_long_delay_sec_max": 4,

    # Target URL - consider making this dynamic
    "target_url": "https://www.clubtickets.com/events/ibiza/pacha/2025/06/01"
}

# --- Helper function for retries with delay ---
async def retry_action(action_coro, description, is_critical=True):
    """
    Attempts to perform an asynchronous action with retries and delays.
    :param action_coro: An awaitable (e.g., lambda: page.click(...))
    :param description: A string describing the action for logging
    :param is_critical: If True, logs a critical error and returns False if all retries fail.
                        If False, logs a warning and returns False, allowing the script to continue.
    :return: True if action succeeded, False otherwise.
    """
    for attempt in range(1, config["max_retries"] + 1):
        try:
            logging.info(f"Attempt {attempt}/{config['max_retries']}: {description}...")
            await action_coro()
            logging.info(f"Successfully performed: {description}")
            return True # Action succeeded
        except PlaywrightTimeoutError as e:
            logging.warning(f"Timeout on attempt {attempt} for {description}: {e}")
            if attempt < config["max_retries"]:
                await asyncio.sleep(config["retry_delay_sec"])
        except Exception as e:
            logging.warning(f"Error on attempt {attempt} for {description}: {e}")
            if attempt < config["max_retries"]:
                await asyncio.sleep(config["retry_delay_sec"])

    if is_critical:
        logging.critical(f"Failed to perform {description} after {config['max_retries']} attempts. Ending run.")
    else:
        logging.error(f"Failed to perform {description} after {config['max_retries']} attempts. Skipping this step.")
    return False # Action failed after all retries

async def automate_clubtickets():
    async with async_playwright() as p:
        # Launch browser with configured options
        browser = await p.chromium.launch(
            headless=config["headless"],
            slow_mo=config["slow_mo"],
            args=["--no-sandbox", "--disable-setuid-sandbox"],
            user_agent=config["user_agent"]
        )
        page = await browser.new_page(viewport={"width": config["viewport_width"], "height": config["viewport_height"]})

        # --- Define XPaths using your provided values ---
        # 1. "Show more events" button
        show_more_xpath = "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"more-events\", \" \" ))]"
        # 2. "Events text cards"
        event_card_xpath = "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"content-text-card\", \" \" ))]"
        # 3. "Next Date" Buttons
        date_tab_xpath = "//*[contains(concat( \" \", @class, \" \" ), concat( \" \", \"btn-custom-day-tab\", \" \" ))]"

        # --- Helper function to process events for a given page/date ---
        async def process_events(page_obj, date_context_name="current day"):
            # Wait for event cards to be visible
            if not await retry_action(
                lambda: page_obj.wait_for_selector(event_card_xpath, state='visible', timeout=20000),
                f"Wait for event cards for {date_context_name}",
                is_critical=False # Not critical to stop if events don't load after date click
            ):
                return True # If events don't load, consider it 'processed' for this date, but log warning

            event_elements = await page_obj.locator(event_card_xpath).all()
            if not event_elements:
                logging.info(f"No events found for {date_context_name}.")
                return True

            logging.info(f"Found {len(event_elements)} events for {date_context_name}.")

            for i, event_element in enumerate(event_elements):
                event_link_locator = event_element.locator("a")
                try:
                    href = await event_link_locator.get_attribute("href")
                    if href:
                        logging.info(f"  Processing event {i+1} for {date_context_name}: {href}")

                        # Add a short, random delay before processing each event link
                        await asyncio.sleep(random.uniform(config["random_short_delay_sec_min"], config["random_short_delay_sec_max"]))

                        # --- YOUR DATA EXTRACTION LOGIC GOES HERE ---
                        # If you need to navigate to the event's detail page:
                        # if not await retry_action(
                        #     lambda: page_obj.goto(href, wait_until="domcontentloaded", timeout=30000),
                        #     f"Navigate to event detail page {href}",
                        #     is_critical=False
                        # ):
                        #     logging.error(f"  Skipping event {i+1} due to navigation failure.")
                        #     continue # Skip to next event

                        # # Add your scraping logic for the event page here
                        # logging.info(f"    Successfully scraped data from {href}")

                        # # IMPORTANT: Go back to the events list page
                        # if not await retry_action(
                        #     lambda: page_obj.go_back(timeout=30000),
                        #     f"Go back from {href}",
                        #     is_critical=True # Critical if we can't go back
                        # ):
                        #     return False # Propagate critical failure

                        # # Wait for the event list to be visible again after going back
                        # if not await retry_action(
                        #     lambda: page_obj.wait_for_selector(event_card_xpath, state='visible', timeout=15000),
                        #     f"Wait for event list to reload after returning from {href}",
                        #     is_critical=True # Critical if list doesn't reload
                        # ):
                        #     return False # Propagate critical failure

                    else:
                        logging.warning(f"  Event {i+1} for {date_context_name} has no 'href' attribute. Skipping.")
                except Exception as e:
                    logging.error(f"  Unhandled error processing event {i+1} for {date_context_name}: {e}. Skipping.")
            return True # All events processed for this context (or handled errors)

        # --- Main Automation Flow ---

        # 1. Navigate to the target URL
        if not await retry_action(
            lambda: page.goto(config["target_url"], wait_until="domcontentloaded", timeout=60000),
            f"Navigate to {config['target_url']}"
        ):
            await browser.close()
            return # Exit if initial navigation fails

        # Add a longer, random delay after initial page load for human-like behavior
        await asyncio.sleep(random.uniform(config["random_long_delay_sec_min"], config["random_long_delay_sec_max"]))

        # 2. Click "Show more events" on the initial day (if visible)
        show_more_button_locator = page.locator(show_more_xpath)
        try:
            # Check if button is visible within a short timeout
            if await show_more_button_locator.is_visible(timeout=5000):
                await asyncio.sleep(random.uniform(config["random_short_delay_sec_min"], config["random_short_delay_sec_max"])) # Delay before click
                if not await retry_action(
                    lambda: show_more_button_locator.click(timeout=10000),
                    "Click 'Show more events'"
                ):
                    await browser.close()
                    return # Exit if clicking "Show more events" fails
                logging.info("Clicked 'Show more events'.")
            else:
                logging.info("'Show more events' button not visible or already clicked. Assuming all events loaded.")
        except PlaywrightTimeoutError: # Catch timeout if is_visible() takes too long
            logging.info("'Show more events' button not visible after initial check. Assuming all events loaded.")
        except Exception as e:
            logging.error(f"Error checking or clicking 'Show more events': {e}. Proceeding assuming events are loaded.")


        # 3. Process events for the initially loaded day (current date)
        if not await process_events(page, "initial loaded day"):
            await browser.close()
            return # Exit if initial event processing fails critically

        # Add a longer, random delay after processing the first day's events
        await asyncio.sleep(random.uniform(config["random_long_delay_sec_min"], config["random_long_delay_sec_max"]))

        # 4. Loop through date tabs from the next day until November
        # Retrieve all date tab locators. This assumes they are all present in the DOM initially.
        date_tabs_locators = await page.locator(date_tab_xpath).all()
        logging.info(f"Found {len(date_tabs_locators)} total date tabs.")

        # Iterate starting from the second date tab (index 1), as the first day's events are already processed.
        for i in range(1, len(date_tabs_locators)):
            current_date_tab_locator = date_tabs_locators[i]
            date_text = "unknown date" # Default for logging in case of error
            try:
                date_text = await current_date_tab_locator.text_content()
                logging.info(f"Attempting to process date: {date_text}")

                # Randomized horizontal scroll to expose next date tabs (if they are in a scrollable container)
                scroll_amount = random.randint(90, 120)
                await page.evaluate(f"window.scrollBy({scroll_amount}, 0)")
                logging.info(f"Scrolled {scroll_amount} pixels right (randomized) to reveal {date_text}.")
                await asyncio.sleep(random.uniform(config["random_short_delay_sec_min"], config["random_short_delay_sec_max"])) # Short delay after scroll

                # Ensure the date tab is in view before clicking for reliability
                if not await retry_action(
                    lambda: current_date_tab_locator.scroll_into_view_if_needed(timeout=10000),
                    f"Scroll date tab {date_text} into view",
                    is_critical=False
                ):
                    logging.error(f"Could not scroll date tab {date_text} into view. Skipping this date.")
                    continue # Skip to the next date if scrolling fails

                # Click the date tab with retry logic
                if not await retry_action(
                    lambda: current_date_tab_locator.click(timeout=10000),
                    f"Click date tab {date_text}"
                ):
                    logging.error(f"Failed to click date tab {date_text}. Skipping this date.")
                    continue # Skip to the next date if click fails

                logging.info(f"Clicked date tab: {date_text}.")
                # Event loading is handled by process_events' wait_for_selector

                # Process events for the newly selected date
                if not await process_events(page, date_text):
                    logging.critical(f"Critical error during event processing for {date_text}. Ending run.")
                    await browser.close()
                    return # Exit the script if event processing fails critically

                # Add a longer, random delay after processing each date's events
                await asyncio.sleep(random.uniform(config["random_long_delay_sec_min"], config["random_long_delay_sec_max"]))

            except Exception as e:
                logging.error(f"Unhandled error during processing of date tab {i} ({date_text}): {e}. Skipping this date.")
                continue # Continue to the next date even if this one encounters an unhandled error

        await browser.close()
        logging.info("Automation finished. Browser closed.")

# Run the script
asyncio.run(automate_clubtickets())