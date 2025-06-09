"""
MongoDB Event Schema for Classy Skkkrapey

This module defines the unified MongoDB schema for event data storage.
The schema provides comprehensive event information with quality scoring
and data validation capabilities.
"""

from typing import Dict, List, Optional, Union
from datetime import datetime

# Unified MongoDB Event Schema
EVENT_SCHEMA = {
    # Core Event Information
    "event_id": str,  # Unique identifier
    "title": str,
    "event_type": str,  # concert, festival, club_night, etc.
    "status": str,  # upcoming, ongoing, completed, cancelled
    
    # Temporal Information
    "datetime": {
        "start_datetime": datetime,  # ISO 8601 datetime
        "end_datetime": Optional[datetime],  # ISO 8601 datetime (optional)
        "raw_date_string": str,  # Original scraped date
        "timezone": str,
        "doors_open": Optional[datetime],  # ISO 8601 datetime (optional)
        "last_entry": Optional[datetime]  # ISO 8601 datetime (optional)
    },
    
    # Location Information
    "venue": {
        "name": str,
        "slug": str,
        "address": str,
        "city": str,
        "country": str,
        "coordinates": {
            "latitude": float,
            "longitude": float
        }
    },
    
    # Content & Description
    "content": {
        "short_description": str,
        "full_description": str,
        "event_highlights": List[str],
        "special_notes": str,
        "content_language": str
    },
    
    # Artists & Lineup
    "artists": {
        "headliners": List[Dict],  # array of artist objects
        "supporting_acts": List[Dict],  # array of artist objects
        "all_performers": List[Dict],  # array of all artist objects
        "lineup_order": List[{
            "act_number": int,
            "artist_name": str,
            "artist_slug": str,
            "performance_time": Optional[datetime],  # ISO 8601 datetime (optional)
            "stage": Optional[str],
            "set_duration": Optional[int]  # minutes
        }]
    },
    
    # Genre & Music Information
    "music_info": {
        "primary_genres": List[str],
        "secondary_genres": List[str],
        "music_style_tags": List[str],
        "bpm_range": Optional[str],
        "music_description": str
    },
    
    # Ticketing Information
    "ticketing": {
        "ticket_tiers": List[{
            "tier_name": str,
            "price": float,
            "currency": str,
            "description": str,
            "availability": str,
            "conditions": str,
            "includes": List[str]
        }],
        "price_range": {
            "min_price": float,
            "max_price": float,
            "currency": str
        },
        "ticket_url": str,
        "booking_platforms": List[Dict],  # array of platform objects
        "presale_info": Optional[Dict]
    },
    
    # Organizer Information
    "organization": {
        "promoter": {
            "name": str,
            "slug": str,
            "social_media": {
                "instagram": str,
                "facebook": str,
                "twitter": str,
                "website": str
            }
        },
        "event_organizer": Dict,  # similar structure to promoter
        "booking_agency": Optional[str]
    },
    
    # Media & Assets
    "media": {
        "featured_image": str,  # URL
        "gallery_images": List[str],  # array of URLs
        "promotional_video": Optional[str],  # URL
        "social_media_assets": List[Dict]
    },
    
    # Scraping Metadata
    "scraping_metadata": {
        "scraped_at": datetime,  # ISO 8601 datetime
        "source_url": str,
        "source_site": str,
        "extraction_method": str,
        "last_updated": datetime,  # ISO 8601 datetime
        "scraper_version": str
    },
    
    # Quality & Validation
    "data_quality": {
        "overall_score": float,  # 0-100
        "completeness_score": float,  # 0-100
        "accuracy_score": float,  # 0-100
        "freshness_score": float,  # 0-100
        "missing_fields": List[str],
        "quality_flags": List[str],
        "validation_status": str,
        "fallback_methods_used": List[str]
    }
}


def get_mongodb_validation_schema():
    """
    Returns the MongoDB JSON Schema validation rules for the events collection.
    This ensures data consistency and quality at the database level.
    """
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["event_id", "title", "scraping_metadata"],
            "properties": {
                # Core Event Information
                "event_id": {
                    "bsonType": "string",
                    "description": "Unique event identifier - required"
                },
                "title": {
                    "bsonType": "string",
                    "description": "Event title - required"
                },
                "event_type": {
                    "bsonType": "string",
                    "enum": ["concert", "festival", "club_night", "dj_set", "live_performance", "special_event", "other"],
                    "description": "Type of event"
                },
                "status": {
                    "bsonType": "string",
                    "enum": ["upcoming", "ongoing", "completed", "cancelled", "postponed"],
                    "description": "Current event status"
                },
                
                # Temporal Information
                "datetime": {
                    "bsonType": "object",
                    "properties": {
                        "start_datetime": {"bsonType": "date"},
                        "end_datetime": {"bsonType": ["date", "null"]},
                        "raw_date_string": {"bsonType": "string"},
                        "timezone": {"bsonType": "string"},
                        "doors_open": {"bsonType": ["date", "null"]},
                        "last_entry": {"bsonType": ["date", "null"]}
                    }
                },
                
                # Location Information
                "venue": {
                    "bsonType": "object",
                    "properties": {
                        "name": {"bsonType": "string"},
                        "slug": {"bsonType": "string"},
                        "address": {"bsonType": "string"},
                        "city": {"bsonType": "string"},
                        "country": {"bsonType": "string"},
                        "coordinates": {
                            "bsonType": "object",
                            "properties": {
                                "latitude": {"bsonType": "double"},
                                "longitude": {"bsonType": "double"}
                            }
                        }
                    }
                },
                
                # Content & Description
                "content": {
                    "bsonType": "object",
                    "properties": {
                        "short_description": {"bsonType": "string"},
                        "full_description": {"bsonType": "string"},
                        "event_highlights": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "special_notes": {"bsonType": "string"},
                        "content_language": {"bsonType": "string"}
                    }
                },
                
                # Artists & Lineup
                "artists": {
                    "bsonType": "object",
                    "properties": {
                        "headliners": {
                            "bsonType": "array",
                            "items": {"bsonType": "object"}
                        },
                        "supporting_acts": {
                            "bsonType": "array",
                            "items": {"bsonType": "object"}
                        },
                        "all_performers": {
                            "bsonType": "array",
                            "items": {"bsonType": "object"}
                        },
                        "lineup_order": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "object",
                                "properties": {
                                    "act_number": {"bsonType": "int"},
                                    "artist_name": {"bsonType": "string"},
                                    "artist_slug": {"bsonType": "string"},
                                    "performance_time": {"bsonType": ["date", "null"]},
                                    "stage": {"bsonType": ["string", "null"]},
                                    "set_duration": {"bsonType": ["int", "null"]}
                                }
                            }
                        }
                    }
                },
                
                # Genre & Music Information
                "music_info": {
                    "bsonType": "object",
                    "properties": {
                        "primary_genres": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "secondary_genres": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "music_style_tags": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "bpm_range": {"bsonType": ["string", "null"]},
                        "music_description": {"bsonType": "string"}
                    }
                },
                
                # Ticketing Information
                "ticketing": {
                    "bsonType": "object",
                    "properties": {
                        "ticket_tiers": {
                            "bsonType": "array",
                            "items": {
                                "bsonType": "object",
                                "properties": {
                                    "tier_name": {"bsonType": "string"},
                                    "price": {"bsonType": "double"},
                                    "currency": {"bsonType": "string"},
                                    "description": {"bsonType": "string"},
                                    "availability": {"bsonType": "string"},
                                    "conditions": {"bsonType": "string"},
                                    "includes": {
                                        "bsonType": "array",
                                        "items": {"bsonType": "string"}
                                    }
                                }
                            }
                        },
                        "price_range": {
                            "bsonType": "object",
                            "properties": {
                                "min_price": {"bsonType": "double"},
                                "max_price": {"bsonType": "double"},
                                "currency": {"bsonType": "string"}
                            }
                        },
                        "ticket_url": {"bsonType": "string"},
                        "booking_platforms": {
                            "bsonType": "array",
                            "items": {"bsonType": "object"}
                        },
                        "presale_info": {"bsonType": ["object", "null"]}
                    }
                },
                
                # Organizer Information
                "organization": {
                    "bsonType": "object",
                    "properties": {
                        "promoter": {
                            "bsonType": "object",
                            "properties": {
                                "name": {"bsonType": "string"},
                                "slug": {"bsonType": "string"},
                                "social_media": {
                                    "bsonType": "object",
                                    "properties": {
                                        "instagram": {"bsonType": "string"},
                                        "facebook": {"bsonType": "string"},
                                        "twitter": {"bsonType": "string"},
                                        "website": {"bsonType": "string"}
                                    }
                                }
                            }
                        },
                        "event_organizer": {"bsonType": "object"},
                        "booking_agency": {"bsonType": ["string", "null"]}
                    }
                },
                
                # Media & Assets
                "media": {
                    "bsonType": "object",
                    "properties": {
                        "featured_image": {"bsonType": "string"},
                        "gallery_images": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "promotional_video": {"bsonType": ["string", "null"]},
                        "social_media_assets": {
                            "bsonType": "array",
                            "items": {"bsonType": "object"}
                        }
                    }
                },
                
                # Scraping Metadata (required)
                "scraping_metadata": {
                    "bsonType": "object",
                    "required": ["scraped_at", "source_url"],
                    "properties": {
                        "scraped_at": {"bsonType": "date"},
                        "source_url": {"bsonType": "string"},
                        "source_site": {"bsonType": "string"},
                        "extraction_method": {"bsonType": "string"},
                        "last_updated": {"bsonType": "date"},
                        "scraper_version": {"bsonType": "string"}
                    }
                },
                
                # Quality & Validation
                "data_quality": {
                    "bsonType": "object",
                    "properties": {
                        "overall_score": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 100
                        },
                        "completeness_score": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 100
                        },
                        "accuracy_score": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 100
                        },
                        "freshness_score": {
                            "bsonType": "double",
                            "minimum": 0,
                            "maximum": 100
                        },
                        "missing_fields": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "quality_flags": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        },
                        "validation_status": {"bsonType": "string"},
                        "fallback_methods_used": {
                            "bsonType": "array",
                            "items": {"bsonType": "string"}
                        }
                    }
                }
            }
        }
    }


def create_event_document(data: Dict[str, any]) -> Dict[str, any]:
    """
    Helper function to create a properly structured event document
    from scraped data, ensuring all required fields are present.
    """
    from datetime import datetime
    import uuid
    
    # Generate event_id if not provided
    event_id = data.get('event_id') or str(uuid.uuid4())
    
    # Build the document with defaults
    event_doc = {
        "event_id": event_id,
        "title": data.get('title', ''),
        "event_type": data.get('event_type', 'other'),
        "status": data.get('status', 'upcoming'),
        
        # Temporal Information
        "datetime": data.get('datetime', {}),
        
        # Location Information
        "venue": data.get('venue', {}),
        
        # Content & Description
        "content": data.get('content', {}),
        
        # Artists & Lineup
        "artists": data.get('artists', {
            "headliners": [],
            "supporting_acts": [],
            "all_performers": [],
            "lineup_order": []
        }),
        
        # Genre & Music Information
        "music_info": data.get('music_info', {
            "primary_genres": [],
            "secondary_genres": [],
            "music_style_tags": []
        }),
        
        # Ticketing Information
        "ticketing": data.get('ticketing', {}),
        
        # Organizer Information
        "organization": data.get('organization', {}),
        
        # Media & Assets
        "media": data.get('media', {
            "gallery_images": [],
            "social_media_assets": []
        }),
        
        # Scraping Metadata (always required)
        "scraping_metadata": {
            "scraped_at": data.get('scraping_metadata', {}).get('scraped_at', datetime.utcnow()),
            "source_url": data.get('scraping_metadata', {}).get('source_url', ''),
            "source_site": data.get('scraping_metadata', {}).get('source_site', ''),
            "extraction_method": data.get('scraping_metadata', {}).get('extraction_method', 'unknown'),
            "last_updated": datetime.utcnow(),
            "scraper_version": data.get('scraping_metadata', {}).get('scraper_version', '0.1.0')
        },
        
        # Quality & Validation
        "data_quality": data.get('data_quality', {
            "overall_score": 0,
            "completeness_score": 0,
            "accuracy_score": 0,
            "freshness_score": 100,
            "missing_fields": [],
            "quality_flags": [],
            "validation_status": "pending",
            "fallback_methods_used": []
        })
    }
    
    return event_doc
