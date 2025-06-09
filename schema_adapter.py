# schema_adapter.py
# This module acts as the bridge between raw scraped data and the
# structured Unified Events Schema V2.

import uuid
from datetime import datetime, timezone

def generate_unique_id(prefix: str, identifier: str) -> str:
    """Generates a unique, deterministic ID for entities."""
    return f"{prefix}_{identifier.lower().replace(' ', '_').replace('.', '')}"

def map_to_unified_schema(raw_data: dict, source_platform: str, source_url: str) -> dict:
    """
    Maps a raw scraped data dictionary to the unifiedEventsSchema V2.

    Args:
        raw_data: The dictionary of data directly from a scraper.
        source_platform: The name of the platform being scraped (e.g., 'ibiza-spotlight').
        source_url: The URL the data was scraped from.

    Returns:
        A dictionary conforming to the unifiedEventsSchema V2.
    """
    # --- I. CORE EVENT IDENTIFICATION & STATUS ---
    event_id = generate_unique_id(source_platform, f"{raw_data.get('title', 'untitled')}_{raw_data.get('raw_date_string', '')}")

    # --- IV. NORMALIZED ACTS & LINEUP ---
    # This is the most complex mapping. We create the main artist entries
    # and the stage performance references simultaneously.
    top_level_acts = []
    stage_acts_references = []
    scraped_artists = raw_data.get('artists', [])
    if isinstance(scraped_artists, list):
        for artist in scraped_artists:
            act_id = generate_unique_id("artist", artist.get('name', 'unknown'))
            top_level_acts.append({
                "act_id": act_id,
                "act_name": artist.get('name'),
                "act_type": artist.get('role', 'dj'), # Default, consider standardizing
                "genres": [], # To be filled by enrichment pipeline
                "styles": [], # To be filled by enrichment pipeline
                "social_media": artist.get('social_media', {}),
                "popularity_metrics": {} # To be filled by enrichment pipeline
            })
            stage_acts_references.append({
                "act_id": act_id, # Reference to the top-level act
                "set_time": {
                    "start": artist.get('performance_time'), # Ensure this is ISO format or datetime
                    "end": None,
                    "duration_minutes": None
                },
                "billing_order": 1 if artist.get('role') == 'headliner' else 2,
                "is_headliner": artist.get('role') == 'headliner'
            })


    # --- FINAL ASSEMBLY OF THE UNIFIED EVENT DOCUMENT ---
    unified_event = {
        # I. CORE EVENT IDENTIFICATION & STATUS
        "event_id": event_id,
        "canonical_id": event_id, # Initially, every event is its own canonical master
        "title": raw_data.get('title'),
        "type": "club_night", # Default, can be improved with NLP later
        "status": "scheduled",

        # II. COMPREHENSIVE DATETIME
        "datetime": {
            "start_date": raw_data.get('datetime_obj'), # Must be ISO format or datetime object
            "end_date": raw_data.get('datetime_info', {}).get('end_datetime'), # Must be ISO format or datetime object
            "timezone": raw_data.get('datetime_info', {}).get('timezone', 'Europe/Madrid'),
            "doors_open": None,
            "last_entry": None,
            "is_all_day": False,
            "duration_hours": None,
            "recurring": {
                "is_recurring": raw_data.get('datetime_info', {}).get('is_recurring', False),
                "frequency": raw_data.get('datetime_info', {}).get('recurrence_pattern'),
                "pattern_description": raw_data.get('datetime_info', {}).get('original_string'),
                "end_recurrence": None
            }
        },

        # III. ENHANCED VENUE & STAGE STRUCTURE
        "venue": {
            "venue_id": generate_unique_id("venue", raw_data.get('venue') or 'unknown'),
            "name": raw_data.get('venue'),
            "address": {
                "street": None,
                "city": "Ibiza", # Default context
                "state": None,
                "country": "Spain",
                "postal_code": None,
                "full_address": raw_data.get('location', {}).get('address')
            },
            "coordinates": {"type": "Point", "coordinates": []}, # To be filled by enrichment
            "venue_type": 'club', # Default
            "total_capacity": raw_data.get('key_info', {}).get('capacity'),
            "has_disabled_access": None,
            "website": None,
            "social_links": {},
            "stage_count": 1,
            "stages": [{
                "stage_id": "stage_1", # Consider making this dynamic if multiple stages possible from one source
                "stage_name": "Main Stage",
                "capacity": None,
                "stage_type": "main_stage",
                "host": {"host_name": raw_data.get('promoter')},
                "stage_genres": raw_data.get('genres', []),
                "acts": stage_acts_references # Use the references created earlier
            }]
        },

        # IV. NORMALIZED ACTS & LINEUP (SINGLE SOURCE OF TRUTH)
        "acts": top_level_acts,

        # V. RICH CONTENT & MUSIC ANALYSIS
        "content": {
            "short_description": raw_data.get('json_ld_description'),
            "full_description": raw_data.get('full_description'),
            "keywords": [],
            "hashtags": []
        },
        "music": {
            "primary_genre": raw_data.get('genres', [None])[0],
            "sub_genres": raw_data.get('genres', []), # Ensure this is a list
            "styles": [],
            "mood_tags": [],
            "energy_level": None,
            "genre_confidence": None
        },

        # VI. DETAILED TICKETING & ACCESSIBILITY
        "ticketing": {
            "tickets_url": raw_data.get('tickets_url'),
            "is_free": False,
            "age_restriction": {
                "minimum_age": 18 if raw_data.get('key_info', {}).get('age_restriction') else None,
                "restriction_type": raw_data.get('key_info', {}).get('age_restriction')
            },
            "promos": [],
            "tiers": [
                tier for tier in [
                    raw_data.get('tier_1'),
                    raw_data.get('tier_2'),
                    raw_data.get('tier_3')
                ] if tier # Make sure tiers are actual dicts and not None
            ],
            "external_platforms": []
        },

        # VII. SCRAPING METADATA
        "scraping_metadata": {
            "source_platform": source_platform,
            "source_url": source_url,
            "source_event_id": None, # Consider if raw_data has a unique ID from the source
            "first_scraped": datetime.now(timezone.utc).isoformat(),
            "last_scraped": datetime.now(timezone.utc).isoformat(),
            "scraper_version": "2.0", # Or make this dynamic
            "raw_data": raw_data # Store the original scraped data for debugging
        },

        # VIII. DATA QUALITY & VALIDATION (Default empty structure)
        "data_quality": {
            "overall_score": 0,
            "field_quality_scores": {},
            "validation_flags": [],
            "manual_verification": {"is_verified": False}
        },

        # IX. DEDUPLICATION & MERGING (Default empty structure)
        "deduplication": {
            "is_canonical": True, # Schema doc says default is false, but adapter sets true. Clarify.
                                 # For now, following adapter code.
            "merged_from_ids": [],
            "merge_log": []
        },

        # X. KNOWLEDGE GRAPH & ANALYTICS (Default empty structure)
        "knowledge_graph": {
            "related_events": [],
            "audience_profile_tags": [],
            "influence_score": 0
        },
        "analytics": {"views": 0, "saves": 0, "clicks_to_tickets": 0},

        # XI. TIMESTAMPS & SYSTEM FLAGS
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "system_flags": {"is_featured": False, "is_hidden": False}
    }

    # Convert datetime objects to be JSON serializable for MongoDB
    # The datetime_obj from raw_data needs to be handled carefully.
    # If it's already a datetime object, .isoformat() is fine.
    # If it's a string, it needs to be parsed first, then re-formatted or stored as is if already ISO.
    # For now, assuming it's a datetime object or None.

    start_date = unified_event["datetime"]["start_date"]
    if isinstance(start_date, datetime):
        unified_event["datetime"]["start_date"] = start_date.isoformat()

    end_date = unified_event["datetime"]["end_date"]
    if isinstance(end_date, datetime):
        unified_event["datetime"]["end_date"] = end_date.isoformat()

    # Ensure artist set_time.start is also ISO format if it's a datetime object
    for stage in unified_event["venue"]["stages"]:
        for act_performance in stage.get("acts", []):
            set_start_time = act_performance.get("set_time", {}).get("start")
            if isinstance(set_start_time, datetime):
                act_performance["set_time"]["start"] = set_start_time.isoformat()
            set_end_time = act_performance.get("set_time", {}).get("end")
            if isinstance(set_end_time, datetime):
                act_performance["set_time"]["end"] = set_end_time.isoformat()


    return unified_event
