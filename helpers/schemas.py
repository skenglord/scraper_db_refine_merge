"""
MongoDB Event Schema for Classy Skkkrapey (unifiedEventsSchema_v2)

This module defines the unified MongoDB schema for event data storage
based on unifiedEventsSchema_v2.
"""

from typing import Dict, List, Optional, Any

# Python type hints for unifiedEventsSchema_v2 for documentation and internal reference.
# Note: MongoDB bsonTypes like 'date' for ISO strings are represented as 'str' here.
EVENT_SCHEMA: Dict[str, Any] = {
    # I. CORE EVENT IDENTIFICATION & STATUS
    "event_id": str,  # Primary unique identifier for an event document.
    "canonical_id": str, # Identifier of the canonical version if this is a duplicate.
    "title": str,     # Main title of the event.
    "type": str,      # Type of event (e.g., club_night, festival, concert, day_party). Enum.
    "status": str,    # Current status (e.g., scheduled, cancelled, postponed, sold_out). Enum.

    # II. COMPREHENSIVE DATETIME
    "datetime": {
        "start_date": str,  # ISO 8601 string (YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD). Required.
        "end_date": Optional[str],    # ISO 8601 string or null.
        "timezone": str,              # E.g., "Europe/Madrid". Required.
        "doors_open": Optional[str],  # ISO 8601 string or null.
        "last_entry": Optional[str],  # ISO 8601 string or null.
        "is_all_day": bool,
        "duration_hours": Optional[float],
        "recurring": {
            "is_recurring": bool,
            "frequency": Optional[str], # e.g., daily, weekly, monthly. Enum.
            "pattern_description": Optional[str], # e.g., "Every Friday"
            "end_recurrence": Optional[str] # ISO 8601 string or null
        }
    },

    # III. ENHANCED VENUE & STAGE STRUCTURE
    "venue": {
        "venue_id": str, # Unique ID for the venue.
        "name": str,     # Name of the venue. Required.
        "address": {
            "street": Optional[str],
            "city": Optional[str],
            "state": Optional[str],
            "country": Optional[str],
            "postal_code": Optional[str],
            "full_address": Optional[str]
        },
        "coordinates": { # GeoJSON Point
            "type": str, # "Point"
            "coordinates": List[float] # [longitude, latitude]
        },
        "venue_type": Optional[str], # e.g., club, outdoor_space, arena. Enum.
        "total_capacity": Optional[int],
        "has_disabled_access": Optional[bool],
        "website": Optional[str],
        "social_links": Dict[str, str], # e.g., {"facebook": "url", "instagram": "url"}
        "stage_count": int,
        "stages": List[{
            "stage_id": str,
            "stage_name": str,
            "capacity": Optional[int],
            "stage_type": Optional[str], # e.g., main_stage, side_room, outdoor_stage. Enum.
            "host": { # Promoter or entity hosting this specific stage/room
                "host_name": Optional[str],
                "host_id": Optional[str] # Link to a promoter/organization entity
            },
            "stage_genres": List[str],
            "acts": List[{ # Acts performing on this stage
                "act_id": str, # Reference to an act in the top-level "acts" array
                "set_time": {
                    "start": Optional[str], # ISO 8601 string or null
                    "end": Optional[str],   # ISO 8601 string or null
                    "duration_minutes": Optional[int]
                },
                "billing_order": Optional[int], # 1 for headliner, 2 for main support, etc.
                "is_headliner": bool
            }]
        }]
    },

    # IV. NORMALIZED ACTS & LINEUP (SINGLE SOURCE OF TRUTH)
    "acts": List[{ # Array of unique artists/acts involved in the event
        "act_id": str, # Unique ID for the artist/act.
        "act_name": str,
        "act_type": str, # e.g., dj, live_band, vocalist, mc. Enum.
        "genres": List[str],
        "styles": List[str],
        "social_media": Dict[str, str], # Platform name -> URL
        "popularity_metrics": Dict[str, Any] # e.g., {"spotify_followers": 10000}
    }],

    # V. RICH CONTENT & MUSIC ANALYSIS
    "content": {
        "short_description": Optional[str],
        "full_description": Optional[str],
        "keywords": List[str],
        "hashtags": List[str]
    },
    "music": {
        "primary_genre": Optional[str],
        "sub_genres": List[str],
        "styles": List[str], # More specific music styles
        "mood_tags": List[str], # e.g., energetic, chill, underground
        "energy_level": Optional[int], # Scale of 1-10
        "genre_confidence": Optional[float] # 0.0 to 1.0
    },

    # VI. DETAILED TICKETING & ACCESSIBILITY
    "ticketing": {
        "tickets_url": Optional[str], # Direct link to purchase tickets
        "is_free": bool,
        "age_restriction": {
            "minimum_age": Optional[int],
            "restriction_type": Optional[str] # e.g., "18+", "all_ages"
        },
        "promos": List[Dict[str, Any]], # e.g., {"promo_code": "EARLYBIRD", "discount_percentage": 10}
        "tiers": List[{
            "tier_id": Optional[str],
            "tier_name": str,
            "tier_price": float,
            "currency": str, # ISO 4217 currency code
            "sale_start": Optional[str], # ISO 8601 string
            "sale_end": Optional[str],   # ISO 8601 string
            "is_sold_out": bool,
            "is_nearly_sold_out": bool
        }],
        "external_platforms": List[Dict[str, Any]] # For third-party ticket sellers
    },

    # VII. SCRAPING METADATA
    "scraping_metadata": {
        "source_platform": str, # Name of the platform data was scraped from. Required.
        "source_url": str,      # URL of the original event page. Required.
        "source_event_id": Optional[str], # Event ID from the source platform
        "first_scraped": str,   # ISO 8601 datetime string.
        "last_scraped": str,    # ISO 8601 datetime string.
        "scraper_version": Optional[str],
        "raw_data": Optional[Dict[str, Any]] # Store the original scraped data for debugging
    },

    # VIII. DATA QUALITY & VALIDATION
    "data_quality": {
        "overall_score": float, # 0.0 to 1.0
        "field_quality_scores": Dict[str, float], # e.g., {"title": 0.9, "datetime": 0.7}
        "validation_flags": List[Dict[str, Any]], # e.g., [{"field": "end_date", "issue": "missing"}]
        "manual_verification": {
            "is_verified": bool,
            "verified_by": Optional[str],
            "verified_at": Optional[str] # ISO 8601 datetime string
        }
    },

    # IX. DEDUPLICATION & MERGING
    "deduplication": {
        "is_canonical": bool, # True if this is the master document for this event
        "merged_from_ids": List[str], # List of event_ids that were merged into this one
        "merge_log": List[Dict[str, Any]] # Log of merge operations
    },

    # X. KNOWLEDGE GRAPH & ANALYTICS
    "knowledge_graph": {
        "related_events": List[str], # List of event_ids for related events
        "audience_profile_tags": List[str], # e.g., "students", "techno_lovers"
        "influence_score": Optional[float]
    },
    "analytics": {
        "views": Optional[int],
        "saves": Optional[int],
        "clicks_to_tickets": Optional[int]
    },

    # XI. TIMESTAMPS & SYSTEM FLAGS
    "created_at": str,  # ISO 8601 datetime string when the document was first created.
    "updated_at": str,  # ISO 8601 datetime string for the last update.
    "system_flags": {
        "is_featured": bool,
        "is_hidden": bool
    }
}


def get_mongodb_validation_schema() -> Dict[str, Any]:
    """
    Returns the MongoDB JSON Schema validation rules for the events collection,
    aligned with unifiedEventsSchema_v2.
    """
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "event_id", "title", "type", "status",
                "datetime", "venue", "acts", "scraping_metadata",
                "created_at", "updated_at"
            ],
            "properties": {
                # I. CORE EVENT IDENTIFICATION & STATUS
                "event_id": {"bsonType": "string", "description": "Primary unique identifier. Req."},
                "canonical_id": {"bsonType": "string", "description": "Identifier of the canonical version."},
                "title": {"bsonType": "string", "description": "Main title of the event. Req."},
                "type": {"bsonType": "string", "description": "Type of event (e.g., club_night, festival). Req. Enum."},
                "status": {"bsonType": "string", "description": "Current status (e.g., scheduled, cancelled). Req. Enum."},

                # II. COMPREHENSIVE DATETIME
                "datetime": {
                    "bsonType": "object",
                    "required": ["start_date", "timezone"],
                    "properties": {
                        "start_date": {"bsonType": "string", "description": "ISO 8601 string (YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD). Req."},
                        "end_date": {"bsonType": ["string", "null"], "description": "ISO 8601 string or null."},
                        "timezone": {"bsonType": "string", "description": "E.g., Europe/Madrid. Req."},
                        "doors_open": {"bsonType": ["string", "null"], "description": "ISO 8601 string or null."},
                        "last_entry": {"bsonType": ["string", "null"], "description": "ISO 8601 string or null."},
                        "is_all_day": {"bsonType": "bool", "description": "True if the event runs for the entire day."},
                        "duration_hours": {"bsonType": ["double", "null"], "description": "Duration of the event in hours."},
                        "recurring": {
                            "bsonType": "object",
                            "properties": {
                                "is_recurring": {"bsonType": "bool"},
                                "frequency": {"bsonType": ["string", "null"], "description": "e.g., daily, weekly, monthly. Enum."},
                                "pattern_description": {"bsonType": ["string", "null"], "description": "e.g., Every Friday"},
                                "end_recurrence": {"bsonType": ["string", "null"], "description": "ISO 8601 string or null"}
                            }
                        }
                    }
                },

                # III. ENHANCED VENUE & STAGE STRUCTURE
                "venue": {
                    "bsonType": "object",
                    "required": ["venue_id", "name"],
                    "properties": {
                        "venue_id": {"bsonType": "string", "description": "Unique ID for the venue. Req."},
                        "name": {"bsonType": "string", "description": "Name of the venue. Req."},
                        "address": {
                            "bsonType": "object",
                            "properties": {
                                "street": {"bsonType": ["string", "null"]},
                                "city": {"bsonType": ["string", "null"]},
                                "state": {"bsonType": ["string", "null"]},
                                "country": {"bsonType": ["string", "null"]},
                                "postal_code": {"bsonType": ["string", "null"]},
                                "full_address": {"bsonType": ["string", "null"]}
                            }
                        },
                        "coordinates": {
                            "bsonType": "object",
                            "required": ["type", "coordinates"],
                            "properties": {
                                "type": {"bsonType": "string", "enum": ["Point"]},
                                "coordinates": {
                                    "bsonType": "array",
                                    "minItems": 2,
                                    "maxItems": 2,
                                    "items": {"bsonType": "double"} # [longitude, latitude]
                                }
                            }
                        },
                        "venue_type": {"bsonType": ["string", "null"], "description": "e.g., club, outdoor_space. Enum."},
                        "total_capacity": {"bsonType": ["int", "null"]},
                        "has_disabled_access": {"bsonType": ["bool", "null"]},
                        "website": {"bsonType": ["string", "null"]},
                        "social_links": {"bsonType": "object", "description": "Platform name -> URL"},
                        "stage_count": {"bsonType": "int", "minimum": 0},
                        "stages": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "object",
                                "required": ["stage_id", "stage_name"],
                                "properties": {
                                    "stage_id": {"bsonType": "string"},
                                    "stage_name": {"bsonType": "string"},
                                    "capacity": {"bsonType": ["int", "null"]},
                                    "stage_type": {"bsonType": ["string", "null"], "description": "e.g., main_stage. Enum."},
                                    "host": {
                                        "bsonType": "object",
                                        "properties": {
                                            "host_name": {"bsonType": ["string", "null"]},
                                            "host_id": {"bsonType": ["string", "null"]}
                                        }
                                    },
                                    "stage_genres": {"bsonType": "array", "items": {"bsonType": "string"}},
                                    "acts": {
                                        "bsonType": "array",
                                        "items": {
                                            "bsonType": "object",
                                            "required": ["act_id", "is_headliner"],
                                            "properties": {
                                                "act_id": {"bsonType": "string", "description": "Ref to top-level acts array"},
                                                "set_time": {
                                                    "bsonType": "object",
                                                    "properties": {
                                                        "start": {"bsonType": ["string", "null"], "description": "ISO 8601 string"},
                                                        "end": {"bsonType": ["string", "null"], "description": "ISO 8601 string"},
                                                        "duration_minutes": {"bsonType": ["int", "null"]}
                                                    }
                                                },
                                                "billing_order": {"bsonType": ["int", "null"]},
                                                "is_headliner": {"bsonType": "bool"}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },

                # IV. NORMALIZED ACTS & LINEUP
                "acts": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "required": ["act_id", "act_name", "act_type"],
                        "properties": {
                            "act_id": {"bsonType": "string"},
                            "act_name": {"bsonType": "string"},
                            "act_type": {"bsonType": "string", "description": "e.g., dj, live_band. Enum."},
                            "genres": {"bsonType": "array", "items": {"bsonType": "string"}},
                            "styles": {"bsonType": "array", "items": {"bsonType": "string"}},
                            "social_media": {"bsonType": "object", "description": "Platform name -> URL"},
                            "popularity_metrics": {"bsonType": "object", "description": "e.g., spotify_followers"}
                        }
                    }
                },

                # V. RICH CONTENT & MUSIC ANALYSIS
                "content": {
                    "bsonType": "object",
                    "properties": {
                        "short_description": {"bsonType": ["string", "null"]},
                        "full_description": {"bsonType": ["string", "null"]},
                        "keywords": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "hashtags": {"bsonType": "array", "items": {"bsonType": "string"}}
                    }
                },
                "music": {
                    "bsonType": "object",
                    "properties": {
                        "primary_genre": {"bsonType": ["string", "null"]},
                        "sub_genres": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "styles": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "mood_tags": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "energy_level": {"bsonType": ["int", "null"], "minimum": 1, "maximum": 10},
                        "genre_confidence": {"bsonType": ["double", "null"], "minimum": 0.0, "maximum": 1.0}
                    }
                },

                # VI. DETAILED TICKETING & ACCESSIBILITY
                "ticketing": {
                    "bsonType": "object",
                    "properties": {
                        "tickets_url": {"bsonType": ["string", "null"]},
                        "is_free": {"bsonType": "bool"},
                        "age_restriction": {
                            "bsonType": "object",
                            "properties": {
                                "minimum_age": {"bsonType": ["int", "null"]},
                                "restriction_type": {"bsonType": ["string", "null"]}
                            }
                        },
                        "promos": {"bsonType": "array", "items": {"bsonType": "object"}},
                        "tiers": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "object",
                                "required": ["tier_name", "tier_price", "currency", "is_sold_out", "is_nearly_sold_out"],
                                "properties": {
                                    "tier_id": {"bsonType": ["string", "null"]},
                                    "tier_name": {"bsonType": "string"},
                                    "tier_price": {"bsonType": "double"},
                                    "currency": {"bsonType": "string"},
                                    "sale_start": {"bsonType": ["string", "null"], "description": "ISO 8601 string"},
                                    "sale_end": {"bsonType": ["string", "null"], "description": "ISO 8601 string"},
                                    "is_sold_out": {"bsonType": "bool"},
                                    "is_nearly_sold_out": {"bsonType": "bool"}
                                }
                            }
                        },
                        "external_platforms": {"bsonType": "array", "items": {"bsonType": "object"}}
                    }
                },

                # VII. SCRAPING METADATA
                "scraping_metadata": {
                    "bsonType": "object",
                    "required": ["source_platform", "source_url", "first_scraped", "last_scraped"],
                    "properties": {
                        "source_platform": {"bsonType": "string", "description": "Scraping source platform. Req."},
                        "source_url": {"bsonType": "string", "description": "Original event page URL. Req."},
                        "source_event_id": {"bsonType": ["string", "null"]},
                        "first_scraped": {"bsonType": "string", "description": "ISO 8601 datetime string. Req."},
                        "last_scraped": {"bsonType": "string", "description": "ISO 8601 datetime string. Req."},
                        "scraper_version": {"bsonType": ["string", "null"]},
                        "raw_data": {"bsonType": ["object", "null"], "description": "Original scraped data."}
                    }
                },

                # VIII. DATA QUALITY & VALIDATION
                "data_quality": {
                    "bsonType": "object",
                    "properties": {
                        "overall_score": {"bsonType": "double", "minimum": 0.0, "maximum": 1.0},
                        "field_quality_scores": {"bsonType": "object"},
                        "validation_flags": {"bsonType": "array", "items": {"bsonType": "object"}},
                        "manual_verification": {
                            "bsonType": "object",
                            "properties": {
                                "is_verified": {"bsonType": "bool"},
                                "verified_by": {"bsonType": ["string", "null"]},
                                "verified_at": {"bsonType": ["string", "null"], "description": "ISO 8601 datetime"}
                            }
                        }
                    }
                },

                # IX. DEDUPLICATION & MERGING
                "deduplication": {
                    "bsonType": "object",
                    "properties": {
                        "is_canonical": {"bsonType": "bool"},
                        "merged_from_ids": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "merge_log": {"bsonType": "array", "items": {"bsonType": "object"}}
                    }
                },

                # X. KNOWLEDGE GRAPH & ANALYTICS
                "knowledge_graph": {
                    "bsonType": "object",
                    "properties": {
                        "related_events": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "audience_profile_tags": {"bsonType": "array", "items": {"bsonType": "string"}},
                        "influence_score": {"bsonType": ["double", "null"]}
                    }
                },
                "analytics": {
                    "bsonType": "object",
                    "properties": {
                        "views": {"bsonType": ["int", "null"]},
                        "saves": {"bsonType": ["int", "null"]},
                        "clicks_to_tickets": {"bsonType": ["int", "null"]}
                    }
                },

                # XI. TIMESTAMPS & SYSTEM FLAGS
                "created_at": {"bsonType": "string", "description": "ISO 8601 datetime string. Req."},
                "updated_at": {"bsonType": "string", "description": "ISO 8601 datetime string. Req."},
                "system_flags": {
                    "bsonType": "object",
                    "properties": {
                        "is_featured": {"bsonType": "bool"},
                        "is_hidden": {"bsonType": "bool"}
                    }
                }
            }
        }
    }
