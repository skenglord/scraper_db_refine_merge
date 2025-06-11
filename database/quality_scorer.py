"""
Quality Scoring Engine for Event Data
Implements field-specific validation and confidence scoring
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class QualityScorer:
    """Calculate quality scores for event data fields"""
    
    def __init__(self):
        """Initialize quality scorer with validation rules"""
        # Updated field weights to align with unifiedEventsSchema_v2 terminology
        self.field_weights = {
            "title": 0.25,
            "venue": 0.20,       # Changed from "location"
            "datetime": 0.25,    # Changed from "dateTime"
            "acts": 0.15,        # Changed from "lineUp"
            "ticketing": 0.15    # Changed from "ticketInfo"
        }
        
    def calculate_event_quality(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate quality scores for an entire event based on unifiedEventsSchema_v2.
        
        Args:
            event_data: Event data dictionary conforming to unifiedEventsSchema_v2
            
        Returns:
            Dictionary with quality scores and metadata, structured for data_quality field.
        """
        field_scores = {} # Renamed from scores for clarity
        validation_flags_by_field = {} # Renamed from validation_details

        # Calculate individual field scores using updated method names and V2 field paths
        field_scores["title"], validation_flags_by_field["title"] = self._score_title_info(
            event_data.get("title", "")
        )
        
        field_scores["venue"], validation_flags_by_field["venue"] = self._score_venue_info(
            event_data.get("venue", {})
        )
        
        field_scores["datetime"], validation_flags_by_field["datetime"] = self._score_datetime_info(
            event_data.get("datetime", {})
        )
        
        field_scores["acts"], validation_flags_by_field["acts"] = self._score_acts_info(
            event_data.get("acts", []) # Pass the top-level 'acts' array
        )
        
        field_scores["ticketing"], validation_flags_by_field["ticketing"] = self._score_ticketing_info(
            event_data.get("ticketing", {})
        )
        
        # Calculate overall score
        overall_score = self._calculate_overall_score(field_scores)

        # Construct the data_quality field content
        # Note: The adapter is expected to create the initial data_quality structure.
        # This function can be used to *recalculate* or *enrich* it if needed,
        # or its logic can be integrated into the adapter directly.
        # For now, it returns a structure that *could* be part of data_quality.

        # Flatten validation_flags from all fields for the main validation_flags array
        all_validation_flags = []
        for field_name, details in validation_flags_by_field.items():
            if details and isinstance(details.get("flags"), list):
                for flag_desc in details["flags"]:
                    all_validation_flags.append({"field": field_name, "issue": flag_desc})

        return {
            # This structure matches the `data_quality` field in unifiedEventsSchema_v2
            "overall": overall_score,
            "overall_score": overall_score,
            "field_quality_scores": field_scores, # field_scores now directly maps to this
            "validation_flags": all_validation_flags,
            "manual_verification": { # Default manual_verification state
                "is_verified": False,
                "verified_by": None,
                "verified_at": None
            }
            # lastCalculated can be added by the caller if this function is used for updates
        }
    
    def _score_title_info(self, title: str) -> Tuple[float, Dict]: # Renamed from _score_title
        """Score title field (logic largely unchanged, name updated for consistency)"""
        score = 0.0
        flags = []

        if not title:
            return 0.0, { # This dict is for internal details, not directly part of schema's validation_flags
                "score_component": 0.0,
                "flags": ["missing_title"]
            }

        # Ensure title is a string before checking length
        if not isinstance(title, str):
            return 0.0, {
                "score_component": 0.0,
                "flags": ["invalid_title_type"]
            }

        # Length check
        if len(title) >= 5:
            score += 0.3
        else:
            flags.append("title_too_short")
        
        # Contains date pattern
        if re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}', title):
            score += 0.2
        
        # Contains venue/artist name
        if len(title.split()) >= 2:
            score += 0.2
        
        # No excessive special characters
        special_char_ratio = len(re.findall(r'[^a-zA-Z0-9\s\-&]', title)) / len(title)
        if special_char_ratio < 0.2:
            score += 0.2
        else:
            flags.append("excessive_special_chars")
        
        # Proper capitalization
        if title[0].isupper() and not title.isupper():
            score += 0.1
        
        return min(score, 1.0), { # Return score (0-1) and details dict
            "score_component": min(score, 1.0),
            "flags": flags
        }
    
    def _score_venue_info(self, venue_data: Dict) -> Tuple[float, Dict]: # Renamed from _score_location
        """Score venue information based on unifiedEventsSchema_v2"""
        score = 0.0
        flags = []
        
        if not venue_data:
            return 0.0, {"score_component": 0.0, "flags": ["missing_venue_data"]}
        
        # Venue name
        if venue_data.get("name"):
            score += 0.3
            known_venues = ["Hï Ibiza", "Ushuaïa", "Pacha", "Amnesia", "DC10", "Privilege"]
            if isinstance(venue_data["name"], str) and any(venue in venue_data["name"] for venue in known_venues):
                score += 0.1
        else:
            flags.append("missing_venue_name")
        
        # Address (check for full_address or city)
        address_info = venue_data.get("address", {})
        if address_info.get("full_address"):
            score += 0.2
        elif address_info.get("street") and address_info.get("city"): # Or at least street and city
             score += 0.15
        else:
            flags.append("missing_address_details")

        # City (should be Ibiza ideally for context, but presence is key)
        if address_info.get("city"):
            score += 0.2
            if isinstance(address_info["city"], str) and "ibiza" in address_info["city"].lower():
                score += 0.1
        else:
            flags.append("missing_city")
        
        # Coordinates
        coordinates_data = venue_data.get("coordinates", {})
        if coordinates_data.get("type") == "Point" and coordinates_data.get("coordinates"):
            coords_array = coordinates_data.get("coordinates", [])
            if len(coords_array) == 2:
                lon, lat = coords_array[0], coords_array[1]
                # Check if coordinates are in Ibiza area (longitude, latitude order)
                if 1.2 <= lon <= 1.6 and 38.8 <= lat <= 39.1 :
                    score += 0.2
                else:
                    flags.append("coordinates_outside_ibiza")
            else:
                flags.append("invalid_coordinates_format")
        else:
            flags.append("missing_coordinates")

        return min(score, 1.0), {"score_component": min(score, 1.0), "flags": flags}
    
    def _score_datetime_info(self, datetime_data: Dict) -> Tuple[float, Dict]: # Renamed from _score_datetime
        """Score datetime information based on unifiedEventsSchema_v2"""
        score = 0.0
        flags = []
        
        if not datetime_data:
            return 0.0, {"score_component": 0.0, "flags": ["missing_datetime_data"]}
        
        # Start date (required in V2)
        start_date_str = datetime_data.get("start_date")
        if start_date_str:
            score += 0.4
            try:
                # Ensure Z is handled for UTC, otherwise assume local or needs timezone
                if 'Z' in start_date_str.upper() and not start_date_str.endswith('+00:00'):
                     start_date_dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                else:
                     start_date_dt = datetime.fromisoformat(start_date_str)

                # Make timezone-aware if naive, assuming UTC if not specified by offset
                if start_date_dt.tzinfo is None:
                    start_date_dt = start_date_dt.replace(tzinfo=timezone.utc)

                now = datetime.now(timezone.utc)
                if start_date_dt < now - timedelta(days=30): # More than 30 days in the past
                    flags.append("date_too_far_past")
                elif start_date_dt > now + timedelta(days=2*365): # More than 2 years in future
                    flags.append("date_too_far_future")
                else:
                    score += 0.1 # Reasonable date
            except ValueError:
                flags.append("invalid_start_date_format")
        else:
            flags.append("missing_start_date") # Critical as per V2

        # End date (optional)
        if datetime_data.get("end_date"):
            score += 0.1 # Bonus for having an end date
            # Could add validation for end_date > start_date if both present
            end_date_str = datetime_data.get("end_date")
            if start_date_str and end_date_str:
                try:
                    if 'Z' in end_date_str.upper() and not end_date_str.endswith('+00:00'):
                        end_date_dt = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                    else:
                        end_date_dt = datetime.fromisoformat(end_date_str)

                    start_dt_for_compare = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')) if 'Z' in start_date_str else datetime.fromisoformat(start_date_str)

                    if end_date_dt < start_dt_for_compare:
                        flags.append("end_date_before_start_date")
                    else:
                        score += 0.05 # Valid end date relative to start
                except ValueError:
                     flags.append("invalid_end_date_format")

        # Timezone (required in V2)
        if datetime_data.get("timezone"):
            score += 0.2 # Increased importance as it's required
            if datetime_data["timezone"] in ["Europe/Madrid", "CET", "CEST"]:
                score += 0.05 # Bonus for common/expected timezones
        else:
            flags.append("missing_timezone") # Critical as per V2

        # Recurring info - presence of pattern_description if is_recurring is true
        recurring_info = datetime_data.get("recurring", {})
        if recurring_info.get("is_recurring"):
            score += 0.05 # Base for being recurring
            if recurring_info.get("pattern_description"):
                score += 0.1 # Good to have a description
            else:
                flags.append("missing_recurring_pattern_description")
        
        return min(score, 1.0), {"score_component": min(score, 1.0), "flags": flags}

    def _score_acts_info(self, acts_data: List[Dict]) -> Tuple[float, Dict]: # Renamed from _score_lineup
        """Score acts (lineup) information based on unifiedEventsSchema_v2's top-level acts array."""
        score = 0.0
        flags = []
        # item_validation = {} # V2 schema doesn't have a direct place for this in data_quality.field_quality_scores

        if not acts_data: # acts_data is the list of act objects
            return 0.0, {"score_component": 0.0, "flags": ["missing_acts_data"]}
        
        # Has at least one act
        if len(acts_data) > 0:
            score += 0.4
        
        valid_acts = 0
        for act_item in acts_data:
            act_score_component = 0.0
            
            if act_item.get("act_name"):
                act_score_component += 0.6
                if len(act_item["act_name"]) >= 2:
                    act_score_component += 0.2
                else:
                    # Not adding to main flags, this is per-act
                    pass # flags.append(f"act_name_too_short:{act_item['act_name']}")
            else:
                flags.append("missing_act_name_in_list") # Flag if an act in the list has no name

            if act_item.get("act_type"): # V2 has act_type
                 act_score_component += 0.1

            if act_item.get("genres") and isinstance(act_item.get("genres"), list) and len(act_item.get("genres")) > 0 :
                act_score_component += 0.1

            if act_score_component >= 0.6: # If has name and good length
                valid_acts +=1
        
        if valid_acts > 0:
            score += 0.4 * (valid_acts / len(acts_data)) # Weighted by proportion of well-defined acts
        
        # Bonus for multiple artists
        if len(acts_data) >= 3:
            score += 0.2
        elif len(acts_data) >= 2:
            score += 0.1
        
        # Headliner check is complex with V2: it's in venue.stages.acts.is_headliner
        # For this simplified version, we are only checking the top-level acts array.
        # A more advanced check would need the full event_data to cross-reference.
        # For now, this aspect is omitted from this specific method's direct scoring.

        return min(score, 1.0), {"score_component": min(score, 1.0), "flags": flags}

    def _score_ticketing_info(self, ticketing_data: Dict) -> Tuple[float, Dict]: # Renamed from _score_ticket_info
        """Score ticketing information based on unifiedEventsSchema_v2"""
        score = 0.0
        flags = []
        
        if not ticketing_data:
            return 0.0, {"score_component": 0.0, "flags": ["missing_ticketing_data"]}

        # is_free status
        if ticketing_data.get("is_free") is True: # Explicitly true
            score += 0.5 # High score if explicitly free, less other checks needed
        elif ticketing_data.get("is_free") is False: # Explicitly not free, expect tiers or URL
             score += 0.1 # Base for knowing it's not free
        else: # is_free is None or missing
            flags.append("missing_is_free_status")


        # Ticket URL (important if not free)
        tickets_url = ticketing_data.get("tickets_url")
        if tickets_url:
            score += 0.2
            if tickets_url.startswith(("http://", "https://")):
                score += 0.05
            else:
                flags.append("invalid_tickets_url")
        elif ticketing_data.get("is_free") is False: # If not free, URL is more important
            flags.append("missing_tickets_url_for_paid_event")


        # Tiers information (if not free)
        tiers = ticketing_data.get("tiers", [])
        if isinstance(tiers, list) and len(tiers) > 0:
            score += 0.2 # Has some tier information

            valid_tiers = 0
            cheapest_price = float('inf')
            currency_found = None

            for tier in tiers:
                if isinstance(tier, dict) and tier.get("tier_name") and tier.get("tier_price") is not None and tier.get("currency"):
                    valid_tiers +=1
                    if tier.get("tier_price") < cheapest_price:
                        cheapest_price = tier.get("tier_price")
                    if not currency_found:
                         currency_found = tier.get("currency")

            if valid_tiers > 0:
                score += 0.1 * (valid_tiers / len(tiers)) # Proportion of valid tiers
                if currency_found == "EUR":
                     score += 0.05
                if 5 <= cheapest_price <= 500: # More reasonable price range for Ibiza
                    score += 0.05
                elif cheapest_price > 500 : # Very high price
                    flags.append("very_high_ticket_price")
                elif cheapest_price < 5 and cheapest_price > 0: # Very low price (but not free)
                    flags.append("very_low_ticket_price")

            if ticketing_data.get("is_free") is False and not tiers:
                 flags.append("missing_tiers_for_paid_event")
        elif ticketing_data.get("is_free") is False : # Not free, but tiers is empty or not a list
            flags.append("missing_tiers_for_paid_event_or_invalid_format")


        # Age restriction
        age_restriction = ticketing_data.get("age_restriction", {})
        if age_restriction.get("minimum_age") is not None or age_restriction.get("restriction_type"):
            score += 0.05

        return min(score, 1.0), {"score_component": min(score, 1.0), "flags": flags}
    
    def _calculate_overall_score(self, field_quality_scores: Dict[str, float]) -> float: # Renamed field_scores
        """Calculate weighted overall score. Input dict keys should match self.field_weights."""
        """Calculate weighted overall score"""
        total_score = 0.0
        total_weight = 0.0
        
        for field, score_component in field_quality_scores.items(): # Iterate through the passed scores
            if field in self.field_weights:
                total_score += score_component * self.field_weights[field]
                total_weight += self.field_weights[field]
        
        if total_weight > 0:
            # Score is already 0.0 to 1.0 from individual scorers
            return round(total_score / total_weight, 3)
        return 0.0
    
    # get_quality_summary and _get_recommendation might be less relevant if the output
    # of calculate_event_quality directly populates the schema's data_quality field.
    # However, they can be kept for internal reporting or debugging.
    # For this exercise, their internal logic doesn't need to change drastically,
    # but their input would be the new structure from calculate_event_quality.

    def get_quality_summary(self, data_quality_field: Dict) -> Dict[str, Any]:
        """Generate a human-readable quality summary from a data_quality field content."""
        overall = data_quality_field.get("overall_score", 0.0)
        field_scores = data_quality_field.get("field_quality_scores", {})
        
        # Determine quality level (0.0 to 1.0 scale)
        if overall >= 0.9:
            quality_level = "Excellent"
        elif overall >= 0.8:
            quality_level = "Good"
        elif overall >= 0.7:
            quality_level = "Fair"
        elif overall >= 0.6: # Adjusted threshold for "Poor"
            quality_level = "Poor"
        else:
            quality_level = "Very Poor"
        
        # Find weakest fields
        weak_fields = [field for field, score in field_scores.items() if score < 0.7]
        
        total_flags = len(data_quality_field.get("validation_flags", []))
        
        return {
            "qualityLevel": quality_level,
            "overallScore": overall,
            "weakFields": weak_fields,
            "totalFlags": total_flags,
            "recommendation": self._get_recommendation(overall, weak_fields) # Uses the same overall score
        }
    
    def _get_recommendation(self, overall_score: float, weak_fields: List[str]) -> str: # No change needed here
        """Generate improvement recommendation"""
        if overall_score >= 0.9:
            return "Data quality is excellent. No immediate action needed."
        elif overall_score >= 0.8:
            return f"Good data quality. Consider improving: {', '.join(weak_fields)}"
        elif overall_score >= 0.7:
            return f"Fair data quality. Priority improvements needed for: {', '.join(weak_fields)}"
        else: # overall_score < 0.7
            return f"Poor data quality ({overall_score:.2f}). Focus on: {', '.join(weak_fields)}. Consider re-scraping or manual review."


# Example usage
if __name__ == "__main__":
    scorer = QualityScorer()
    
    # Test with sample event data (conforming to unifiedEventsSchema_v2)
    sample_v2_event = {
        "title": "Techno Night at Amnesia",
        "type": "club_night",
        "status": "scheduled",
        "datetime": {
            "start_date": "2025-07-15T23:00:00Z",
            "timezone": "Europe/Madrid",
            "recurring": {"is_recurring": False}
        },
        "venue": {
            "venue_id": "venue_amnesia",
            "name": "Amnesia",
            "address": {"city": "Ibiza", "country": "Spain"},
            "coordinates": {"type": "Point", "coordinates": [1.405, 38.955]} # lon, lat
        },
        "acts": [
            {"act_id": "artist_charlotte", "act_name": "Charlotte de Witte", "act_type": "dj", "genres": ["Techno"]},
            {"act_id": "artist_amelie", "act_name": "Amelie Lens", "act_type": "dj", "genres": ["Techno"]}
        ],
        "ticketing": {
            "is_free": False,
            "tickets_url": "https://amnesia.es/tickets",
            "tiers": [{"tier_name": "Standard", "tier_price": 50.0, "currency": "EUR", "is_sold_out": False, "is_nearly_sold_out": False}]
        },
        "scraping_metadata": { # Required by schema, not directly scored but good to have
            "source_platform": "example_platform",
            "source_url": "http://example.com/event/123",
            "first_scraped": datetime.now(timezone.utc).isoformat(),
            "last_scraped": datetime.now(timezone.utc).isoformat()
        },
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat()
        # data_quality field will be populated by calculate_event_quality
    }
    
    # The calculate_event_quality now returns the content for the 'data_quality' field
    data_quality_content = scorer.calculate_event_quality(sample_v2_event)

    # To use get_quality_summary, pass this content:
    summary = scorer.get_quality_summary(data_quality_content)
    
    print(f"Overall Score: {data_quality_content['overall_score']:.3f}") # Direct access
    print(f"Field Scores: {data_quality_content['field_quality_scores']}")
    print(f"Validation Flags: {data_quality_content['validation_flags']}")
    print(f"Quality Level: {summary['qualityLevel']}")
    print(f"Recommendation: {summary['recommendation']}")