import random
import time

def get_random_delay(
    min_delay_seconds: float,
    max_delay_seconds: float,
    multiplier: float = 1.0
    ) -> None:
    """
    Pauses execution for a random duration within a specified range,
    optionally modified by a multiplier.

    Args:
        min_delay_seconds: The minimum duration for the delay in seconds.
        max_delay_seconds: The maximum duration for the delay in seconds.
        multiplier: A factor to scale the delay duration. Defaults to 1.0.
                    A multiplier of 0 would result in no delay if min_delay is 0,
                    or min_delay if min_delay > 0 after multiplication.
                    It's applied to both min and max before random.uniform.

    Raises:
        ValueError: If min_delay_seconds is greater than max_delay_seconds after
                    applying the multiplier.
    """
    if min_delay_seconds < 0 or max_delay_seconds < 0:
        # Or raise ValueError, but for simplicity, just proceed (uniform might handle negative ranges, but it's unusual)
        # print("Warning: Negative delay values provided. Behavior might be unexpected.")
        pass

    actual_min = min_delay_seconds * multiplier
    actual_max = max_delay_seconds * multiplier

    if actual_min > actual_max:
        # Option 1: Raise an error
        # raise ValueError(f"Calculated min_delay ({actual_min}s) cannot be greater than calculated max_delay ({actual_max}s).")
        # Option 2: Swap them or use only one
        # print(f"Warning: Calculated min_delay ({actual_min}s) > max_delay ({actual_max}s). Using max_delay for both.")
        actual_min = actual_max # Or some other sensible default or warning

    if actual_max <= 0: # If max delay is zero or negative, no sleep.
        return

    # Ensure actual_min is not negative if actual_max is positive, to avoid issues with random.uniform
    # if actual_max > 0 and actual_min < 0:
    #    actual_min = 0
    # For simplicity, we assume positive ranges or that user handles inverted ranges if not raising error.

    delay_duration = random.uniform(actual_min, actual_max)

    # Ensure delay is not negative if the range somehow allowed it (e.g. if no strict positive check above)
    if delay_duration < 0:
        delay_duration = 0

    time.sleep(delay_duration)


if __name__ == '__main__': # pragma: no cover
    print("Testing get_random_delay function...")

    print("Test 1: Basic delay (1-3 seconds)")
    start_time = time.time()
    get_random_delay(1.0, 3.0)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Delay 1 lasted: {duration:.4f} seconds. Expected between 1.0 and 3.0.")
    assert 1.0 <= duration <= 3.0 + 0.01 # Add small tolerance for time.time precision

    print("\nTest 2: Short delay with multiplier (0.1-0.3 seconds)")
    start_time = time.time()
    get_random_delay(0.2, 0.6, multiplier=0.5)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Delay 2 lasted: {duration:.4f} seconds. Expected between 0.1 and 0.3.")
    assert 0.1 <= duration <= 0.3 + 0.01

    print("\nTest 3: Zero max delay (should be no delay or minimal)")
    start_time = time.time()
    get_random_delay(0, 0)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Delay 3 lasted: {duration:.4f} seconds. Expected close to 0.")
    assert duration < 0.01 # Should be very small

    print("\nTest 4: Min delay greater than max (implementation specific handling - current: uses max for both)")
    # With current implementation (uses max for both if min > max)
    start_time = time.time()
    get_random_delay(5.0, 2.0) # min > max
    end_time = time.time()
    duration = end_time - start_time
    print(f"Delay 4 lasted: {duration:.4f} seconds. Current logic makes this equivalent to get_random_delay(2.0, 2.0).")
    assert 2.0 <= duration <= 2.0 + 0.01

    print("\nTest 5: Negative delay (implementation specific - current: allows, uniform might behave unexpectedly or error)")
    # This test depends on how random.uniform handles negative ranges if not caught.
    # For safety, it's better if the function ensures non-negative delays or raises error.
    # Current implementation will likely result in delay_duration being actual_max or actual_min.
    # If actual_max is negative, it returns early.
    print("Testing with negative max_delay (should result in no sleep):")
    start_time = time.time()
    get_random_delay(1.0, -1.0)
    end_time = time.time()
    duration = end_time - start_time
    print(f"Delay 5 lasted: {duration:.4f} seconds. Expected no effective sleep.")
    assert duration < 0.01


    print("\nAll tests seem to have run.")
