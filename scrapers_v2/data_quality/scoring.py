import logging
from typing import Dict, List, Optional, Any
from datetime import datetime

# Assuming UnifiedEvent will be importable. For standalone testing, define a mock.
try:
    from scrapers_v2.schema_adapter import UnifiedEvent, EventDetails, EventDateDetails, EventLocation
except ImportError:
    # Mock classes for standalone testing if schema_adapter is not in path
    logging.basicConfig(level=logging.DEBUG)
    logger_mock = logging.getLogger("scoring_mock")
    logger_mock.warning("Using mock UnifiedEvent for scoring.py. Ensure schema_adapter is in PYTHONPATH for full functionality.")

    class MockBaseModel:
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)
        def model_dump(self, **kwargs) -> Dict[str, Any]: return self.__dict__

    class EventDetails(MockBaseModel): pass
    class EventDateDetails(MockBaseModel): pass
    class EventLocation(MockBaseModel): pass
    class UnifiedEvent(MockBaseModel): pass


logger = logging.getLogger(__name__)
if not logger.handlers: # Ensure logger is configured if module is run directly
    logging.basicConfig(level=logging.INFO)


def is_valid_iso_date_string(date_string: Optional[str]) -> bool:
    """Rudimentary check if a string might be an ISO date string (ends with Z or has timezone)."""
    if not date_string or not isinstance(date_string, str):
        return False
    try:
        # A more robust check would be dateutil_parser.isoparse, but that's a heavier dependency.
        # For a basic check, ensure it can be parsed and has timezone info or is UTC (ends with Z).
        datetime.fromisoformat(date_string.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False

def calculate_basic_quality_score(unified_event: UnifiedEvent) -> Dict[str, Any]:
    """
    Calculates a basic data quality score for a UnifiedEvent object.
    Checks for presence and basic validity of key fields.
    """
    if not isinstance(unified_event, UnifiedEvent): # Check if it's the Pydantic model or mock
        logger.warning(f"Invalid input type for quality scoring: {type(unified_event)}. Expected UnifiedEvent.")
        # In a real scenario with strict typing, this might not be needed, but good for robustness if mocks are used.
        # Attempt to work with it if it's dict-like (duck typing for mock)
        if not hasattr(unified_event, 'event_details') or not hasattr(unified_event, 'event_dates') or not hasattr(unified_event, 'location'):
             return {"overall_score": 0, "issues": [{"field": "root_event_object", "issue": "Invalid event object structure"}]}


    score = 100.0
    issues_found: List[Dict[str, str]] = []

    # Define weights for key fields
    field_checks = {
        "event_details.title": {"weight": 30, "message": "Title is missing or empty."},
        "event_dates.start_date_utc": {"weight": 30, "message": "Start date UTC is missing or invalid format."},
        "location.venue_name": {"weight": 20, "message": "Venue name is missing or empty."},
        "event_details.description_text": {"weight": 20, "message": "Text description is missing or empty."}
    }

    # Check Title
    title = getattr(getattr(unified_event, 'event_details', None), 'title', None)
    if not title or not str(title).strip():
        score -= field_checks["event_details.title"]["weight"]
        issues_found.append({"field": "event_details.title", "issue": field_checks["event_details.title"]["message"]})

    # Check Start Date UTC
    start_date_utc = getattr(getattr(unified_event, 'event_dates', None), 'start_date_utc', None)
    if not start_date_utc or not is_valid_iso_date_string(str(start_date_utc)): # Ensure it's a string for validation
        score -= field_checks["event_dates.start_date_utc"]["weight"]
        issues_found.append({"field": "event_dates.start_date_utc", "issue": field_checks["event_dates.start_date_utc"]["message"]})

    # Check Venue Name
    venue_name = getattr(getattr(unified_event, 'location', None), 'venue_name', None)
    if not venue_name or not str(venue_name).strip():
        score -= field_checks["location.venue_name"]["weight"]
        issues_found.append({"field": "location.venue_name", "issue": field_checks["location.venue_name"]["message"]})

    # Check Description Text
    description_text = getattr(getattr(unified_event, 'event_details', None), 'description_text', None)
    if not description_text or not str(description_text).strip():
        score -= field_checks["event_details.description_text"]["weight"]
        issues_found.append({"field": "event_details.description_text", "issue": field_checks["event_details.description_text"]["message"]})

    # Ensure score is not negative
    final_score = max(0.0, score)

    logger.debug(f"Quality score for event_id '{getattr(unified_event, 'event_id', 'N/A')}': {final_score}, Issues: {issues_found}")

    return {
        "overall_score": final_score,
        "issues": issues_found, # Renamed from missing_or_invalid_fields for clarity
        "last_assessed_utc": datetime.now(dt_timezone.utc).isoformat()
    }


if __name__ == "__main__":
    # Example Usage with Mocked UnifiedEvent structure for standalone testing
    # This assumes the structure of UnifiedEvent and its nested models.

    # Setup basic logger for __main__ test
    logging.basicConfig(level=logging.DEBUG)
    test_logger = logging.getLogger(__name__) # Use module logger

    # Test case 1: Good event
    good_event_data = {
        "event_id": "evt_good",
        "event_details": EventDetails(title="Awesome Concert", description_text="A really great concert experience."),
        "event_dates": EventDateDetails(start_date_utc="2024-09-15T20:00:00Z"),
        "location": EventLocation(venue_name="The Big Arena")
    }
    good_event = UnifiedEvent(**good_event_data)
    quality_1 = calculate_basic_quality_score(good_event)
    test_logger.info(f"Good Event Quality: {quality_1}")
    assert quality_1["overall_score"] == 100.0
    assert not quality_1["issues"]

    # Test case 2: Event missing title
    missing_title_data = {
        "event_id": "evt_no_title",
        "event_details": EventDetails(description_text="A really great concert experience."),
        "event_dates": EventDateDetails(start_date_utc="2024-09-15T20:00:00Z"),
        "location": EventLocation(venue_name="The Big Arena")
    }
    missing_title_event = UnifiedEvent(**missing_title_data)
    quality_2 = calculate_basic_quality_score(missing_title_event)
    test_logger.info(f"Missing Title Event Quality: {quality_2}")
    assert quality_2["overall_score"] == 70.0 # 100 - 30
    assert len(quality_2["issues"]) == 1
    assert quality_2["issues"][0]["field"] == "event_details.title"

    # Test case 3: Event with invalid date and missing venue
    invalid_date_missing_venue_data = {
        "event_id": "evt_bad_date_no_venue",
        "event_details": EventDetails(title="Party Time", description_text="A party."),
        "event_dates": EventDateDetails(start_date_utc="not-a-date"), # Invalid date
        "location": EventLocation() # Missing venue_name
    }
    bad_event = UnifiedEvent(**invalid_date_missing_venue_data)
    quality_3 = calculate_basic_quality_score(bad_event)
    test_logger.info(f"Invalid Date & No Venue Event Quality: {quality_3}")
    assert quality_3["overall_score"] == 50.0 # 100 - 30 (date) - 20 (venue)
    assert len(quality_3["issues"]) == 2

    # Test case 4: All key fields missing
    all_missing_data = {
        "event_id": "evt_all_missing",
        "event_details": EventDetails(), # Empty details
        "event_dates": EventDateDetails(), # Empty dates
        "location": EventLocation() # Empty location
    }
    empty_event = UnifiedEvent(**all_missing_data)
    quality_4 = calculate_basic_quality_score(empty_event)
    test_logger.info(f"All Missing Event Quality: {quality_4}")
    assert quality_4["overall_score"] == 0.0 # 100 - 30 - 30 - 20 - 20
    assert len(quality_4["issues"]) == 4

    test_logger.info("Basic quality scoring tests completed.")

```
