"""
Enhanced Schema Adapter with Comprehensive Data Validation
This module provides scalable validation for data from various sources worldwide.
"""

import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from urllib.parse import urlparse
import logging
from abc import ABC, abstractmethod
from collections import defaultdict

# Import the existing quality scorer
try:
    from database.quality_scorer import QualityScorer
except ImportError:
    # Fallback if running from different directory
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).parent))
    from database.quality_scorer import QualityScorer

logger = logging.getLogger(__name__)


class ValidationLevel:
    """Validation severity levels"""
    CRITICAL = "critical"  # Data cannot be processed
    ERROR = "error"       # Major issues that need fixing
    WARNING = "warning"   # Data quality issues
    INFO = "info"        # Suggestions for improvement


class ValidationResult:
    """Container for validation results"""
    def __init__(self):
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.transformations: List[Dict[str, Any]] = []
        self.confidence_score: float = 1.0
    
    def add_issue(self, level: str, field: str, message: str, details: Optional[Dict] = None):
        issue = {
            "field": field,
            "message": message,
            "level": level,
            "details": details or {}
        }
        
        if level == ValidationLevel.CRITICAL or level == ValidationLevel.ERROR:
            self.errors.append(issue)
            self.confidence_score *= 0.7
        elif level == ValidationLevel.WARNING:
            self.warnings.append(issue)
            self.confidence_score *= 0.9
        else:
            self.info.append(issue)
            self.confidence_score *= 0.95
    
    def add_transformation(self, field: str, original: Any, transformed: Any, reason: str):
        self.transformations.append({
            "field": field,
            "original": original,
            "transformed": transformed,
            "reason": reason
        })


class BaseValidator(ABC):
    """Abstract base class for platform-specific validators"""
    
    @abstractmethod
    def validate_and_clean(self, raw_data: Dict[str, Any]) -> Tuple[Dict[str, Any], ValidationResult]:
        """Validate and clean raw data from specific platform"""
        pass
    
    @abstractmethod
    def get_field_mapping(self) -> Dict[str, List[str]]:
        """Return field mapping for this platform"""
        pass


class UniversalValidator:
    """Universal validation methods applicable to all platforms"""
    
    @staticmethod
    def validate_url(url: str) -> Tuple[bool, Optional[str]]:
        """Validate and normalize URL"""
        if not url:
            return False, None
        
        try:
            # Add protocol if missing
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
            
            parsed = urlparse(url)
            if parsed.scheme and parsed.netloc:
                return True, url
            return False, None
        except Exception:
            return False, None
    
    @staticmethod
    def validate_datetime(date_str: str, timezone_str: Optional[str] = None) -> Tuple[bool, Optional[str]]:
        """Validate and normalize datetime strings"""
        if not date_str:
            return False, None
        
        # Common date formats to try
        date_formats = [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M",
            "%d-%m-%Y %H:%M",
            "%B %d, %Y at %I:%M %p",
            "%d %B %Y",
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                
                # Add timezone if missing
                if dt.tzinfo is None and timezone_str:
                    # Convert timezone string to offset (simplified)
                    if timezone_str == "Europe/Madrid":
                        dt = dt.replace(tzinfo=timezone.utc)
                    else:
                        dt = dt.replace(tzinfo=timezone.utc)
                
                return True, dt.isoformat()
            except ValueError:
                continue
        
        return False, None
    
    @staticmethod
    def validate_coordinates(lon: float, lat: float) -> Tuple[bool, str]:
        """Validate geographical coordinates"""
        try:
            lon = float(lon)
            lat = float(lat)
            
            if -180 <= lon <= 180 and -90 <= lat <= 90:
                return True, ""
            else:
                return False, "Coordinates out of valid range"
        except (TypeError, ValueError):
            return False, "Invalid coordinate format"
    
    @staticmethod
    def normalize_currency(amount: Any, currency: str) -> Tuple[Optional[float], str]:
        """Normalize currency amounts"""
        try:
            # Handle various formats
            if isinstance(amount, str):
                # Remove currency symbols and spaces
                amount = re.sub(r'[€$£¥₹\s,]', '', amount)
            
            amount_float = float(amount)
            
            # Validate currency code (ISO 4217)
            valid_currencies = ['EUR', 'USD', 'GBP', 'JPY', 'INR', 'AUD', 'CAD', 'CHF']
            if currency.upper() not in valid_currencies:
                currency = 'EUR'  # Default for unknown currencies
            
            return amount_float, currency.upper()
        except (TypeError, ValueError):
            return None, currency


class IbizaSpotlightValidator(BaseValidator):
    """Validator specific to Ibiza Spotlight data"""
    
    def get_field_mapping(self) -> Dict[str, List[str]]:
        return {
            'title': ['title', 'event_name', 'name'],
            'datetime': ['time', 'datetime', 'date', 'start_time'],
            'venue': ['venue', 'location', 'place'],
            'artists': ['lineup', 'artists', 'acts', 'djs'],
            'url': ['url', 'link', 'event_url'],
            'price': ['price', 'ticket_price', 'cost']
        }
    
    def validate_and_clean(self, raw_data: Dict[str, Any]) -> Tuple[Dict[str, Any], ValidationResult]:
        result = ValidationResult()
        cleaned_data = {}
        
        # Validate title
        title = raw_data.get('title', '').strip()
        if not title:
            result.add_issue(ValidationLevel.ERROR, 'title', 'Missing event title')
        else:
            if len(title) < 3:
                result.add_issue(ValidationLevel.WARNING, 'title', 'Title too short')
            cleaned_data['title'] = title
        
        # Validate and clean datetime
        time_str = raw_data.get('time', '')
        if time_str:
            # Ibiza Spotlight specific date parsing
            cleaned_time = self._parse_ibiza_datetime(time_str)
            if cleaned_time:
                cleaned_data['datetime_obj'] = cleaned_time
                cleaned_data['raw_date_string'] = time_str
            else:
                result.add_issue(ValidationLevel.ERROR, 'datetime', f'Invalid date format: {time_str}')
        
        # Validate venue
        venue = raw_data.get('venue', '')
        if venue:
            cleaned_data['venue'] = self._normalize_venue_name(venue)
        else:
            result.add_issue(ValidationLevel.WARNING, 'venue', 'Missing venue information')
        
        # Validate URL
        url = raw_data.get('url', '')
        if url:
            is_valid, normalized_url = UniversalValidator.validate_url(url)
            if is_valid:
                cleaned_data['url'] = normalized_url
            else:
                result.add_issue(ValidationLevel.WARNING, 'url', 'Invalid URL format')
        
        # Process lineup/artists
        lineup = raw_data.get('lineup', [])
        if lineup:
            cleaned_artists = self._process_lineup(lineup, result)
            cleaned_data['artists'] = cleaned_artists
        
        # Keep original data for reference
        cleaned_data['_original'] = raw_data
        
        return cleaned_data, result
    
    def _parse_ibiza_datetime(self, time_str: str) -> Optional[datetime]:
        """Parse Ibiza-specific datetime formats"""
        try:
            # Handle various Ibiza date formats
            # Example: "Friday 15 July 2025"
            if 'day' in time_str.lower():
                # Remove day name
                time_str = re.sub(r'^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\s+', '', time_str, flags=re.IGNORECASE)
            
            # Try parsing
            formats = [
                "%d %B %Y",
                "%d %b %Y",
                "%d/%m/%Y",
                "%Y-%m-%d"
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(time_str.strip(), fmt)
                    # Set to evening time for club events
                    dt = dt.replace(hour=23, minute=0, tzinfo=timezone.utc)
                    return dt
                except ValueError:
                    continue
            
            return None
        except Exception:
            return None
    
    def _normalize_venue_name(self, venue: str) -> str:
        """Normalize venue names for consistency"""
        venue_mappings = {
            'hi ibiza': 'Hï Ibiza',
            'hi': 'Hï Ibiza',
            'ushuaia': 'Ushuaïa',
            'pacha': 'Pacha',
            'amnesia': 'Amnesia',
            'dc10': 'DC10',
            'dc-10': 'DC10',
            'privilege': 'Privilege',
            'eden': 'Eden',
            'es paradis': 'Es Paradis'
        }
        
        venue_lower = venue.lower().strip()
        return venue_mappings.get(venue_lower, venue)
    
    def _process_lineup(self, lineup: List[Dict], result: ValidationResult) -> List[Dict]:
        """Process and validate artist lineup"""
        processed_artists = []
        
        for artist in lineup:
            if isinstance(artist, dict) and artist.get('name'):
                processed = {
                    'name': artist['name'].strip(),
                    'role': artist.get('role', 'dj').lower()
                }
                
                # Validate artist name
                if len(processed['name']) < 2:
                    result.add_issue(
                        ValidationLevel.INFO, 
                        'artists', 
                        f"Very short artist name: {processed['name']}"
                    )
                
                processed_artists.append(processed)
        
        return processed_artists


class FacebookEventValidator(BaseValidator):
    """Validator for Facebook event data"""
    
    def get_field_mapping(self) -> Dict[str, List[str]]:
        return {
            'title': ['name'],
            'datetime': ['start_time'],
            'venue': ['place.name', 'location'],
            'description': ['description'],
            'url': ['id'],  # Will be converted to Facebook URL
        }
    
    def validate_and_clean(self, raw_data: Dict[str, Any]) -> Tuple[Dict[str, Any], ValidationResult]:
        result = ValidationResult()
        cleaned_data = {}
        
        # Facebook-specific validation logic
        # Implementation would follow similar pattern to IbizaSpotlightValidator
        
        return cleaned_data, result


class FieldNormalizer:
    """Normalize fields across different platforms"""
    
    def __init__(self):
        self.genre_mappings = self._load_genre_mappings()
        self.timezone_mappings = self._load_timezone_mappings()
    
    def _load_genre_mappings(self) -> Dict[str, str]:
        """Load genre normalization mappings"""
        return {
            'techno': 'Techno',
            'tech-house': 'Tech House',
            'tech house': 'Tech House',
            'house': 'House',
            'deep house': 'Deep House',
            'deep-house': 'Deep House',
            'progressive': 'Progressive House',
            'progressive house': 'Progressive House',
            'trance': 'Trance',
            'melodic techno': 'Melodic Techno',
            'melodic-techno': 'Melodic Techno',
            'minimal': 'Minimal',
            'drum & bass': 'Drum & Bass',
            'dnb': 'Drum & Bass',
            'd&b': 'Drum & Bass',
        }
    
    def _load_timezone_mappings(self) -> Dict[str, str]:
        """Load timezone mappings for different regions"""
        return {
            'ibiza': 'Europe/Madrid',
            'london': 'Europe/London',
            'berlin': 'Europe/Berlin',
            'new york': 'America/New_York',
            'los angeles': 'America/Los_Angeles',
            'tokyo': 'Asia/Tokyo',
            'sydney': 'Australia/Sydney',
        }
    
    def normalize_genre(self, genre: str) -> str:
        """Normalize genre names"""
        if not genre:
            return ''
        
        genre_lower = genre.lower().strip()
        return self.genre_mappings.get(genre_lower, genre.title())
    
    def detect_timezone_from_location(self, location: str) -> str:
        """Detect timezone based on location"""
        if not location:
            return 'UTC'
        
        location_lower = location.lower()
        
        for city, tz in self.timezone_mappings.items():
            if city in location_lower:
                return tz
        
        # Default to UTC if unknown
        return 'UTC'


class ValidationRegistry:
    """Registry for platform-specific validators"""
    
    def __init__(self):
        self.validators = {
            'ibiza-spotlight': IbizaSpotlightValidator(),
            'ibiza-spotlight-calendar': IbizaSpotlightValidator(),
            'facebook': FacebookEventValidator(),
            # Add more validators as needed
        }
        self.default_validator = IbizaSpotlightValidator()
    
    def get_validator(self, platform: str) -> BaseValidator:
        """Get validator for specific platform"""
        return self.validators.get(platform.lower(), self.default_validator)


class EnhancedSchemaAdapter:
    """Enhanced schema adapter with comprehensive validation"""
    
    def __init__(self):
        self.validation_registry = ValidationRegistry()
        self.quality_scorer = QualityScorer()
        self.field_normalizer = FieldNormalizer()
        self.universal_validator = UniversalValidator()
        
    def generate_unique_id(self, prefix: str, identifier: str) -> str:
        """Generate a unique, deterministic ID for entities"""
        clean_id = re.sub(r'[^a-zA-Z0-9_-]', '_', identifier.lower())
        return f"{prefix}_{clean_id}_{uuid.uuid5(uuid.NAMESPACE_DNS, f'{prefix}:{identifier}').hex[:8]}"
    
    def map_to_unified_schema(self, raw_data: Dict[str, Any], source_platform: str, source_url: str) -> Dict[str, Any]:
        """
        Map raw scraped data to unified schema with comprehensive validation
        
        Args:
            raw_data: Raw data from scraper
            source_platform: Platform identifier
            source_url: Source URL
            
        Returns:
            Validated and normalized event data
        """
        # 1. Platform-specific validation and cleaning
        validator = self.validation_registry.get_validator(source_platform)
        cleaned_data, validation_result = validator.validate_and_clean(raw_data)
        
        # 2. Map to unified schema structure
        unified_event = self._create_base_structure(cleaned_data, source_platform, source_url)
        
        # 3. Apply field-level validation and normalization
        self._validate_and_normalize_fields(unified_event, validation_result)
        
        # 4. Calculate quality score
        quality_data = self.quality_scorer.calculate_event_quality(unified_event)
        unified_event['data_quality'] = quality_data
        
        # 5. Add validation metadata
        unified_event['validation_metadata'] = {
            'validation_timestamp': datetime.now(timezone.utc).isoformat(),
            'validation_errors': [e for e in validation_result.errors],
            'validation_warnings': [w for w in validation_result.warnings],
            'validation_info': [i for i in validation_result.info],
            'data_transformations': validation_result.transformations,
            'platform_validator': validator.__class__.__name__,
            'confidence_score': validation_result.confidence_score
        }
        
        # 6. Schema compliance check
        compliance_issues = self._check_schema_compliance(unified_event)
        if compliance_issues:
            unified_event['validation_metadata']['schema_compliance_issues'] = compliance_issues
        
        return unified_event
    
    def _create_base_structure(self, cleaned_data: Dict[str, Any], source_platform: str, source_url: str) -> Dict[str, Any]:
        """Create base unified event structure"""
        # Extract basic fields
        title = cleaned_data.get('title', 'Untitled Event')
        venue_name = cleaned_data.get('venue', 'Unknown Venue')
        
        # Generate IDs
        event_id = self.generate_unique_id(
            source_platform, 
            f"{title}_{cleaned_data.get('raw_date_string', '')}"
        )
        
        # Process artists/acts
        artists_data = cleaned_data.get('artists', [])
        acts, stage_acts = self._process_acts(artists_data)
        
        # Build unified structure
        unified_event = {
            # Core identification
            "event_id": event_id,
            "canonical_id": event_id,
            "title": title,
            "type": self._determine_event_type(cleaned_data),
            "status": "scheduled",
            
            # DateTime
            "datetime": self._build_datetime_structure(cleaned_data),
            
            # Venue
            "venue": self._build_venue_structure(venue_name, stage_acts),
            
            # Acts
            "acts": acts,
            
            # Content
            "content": {
                "short_description": cleaned_data.get('short_description'),
                "full_description": cleaned_data.get('full_description'),
                "keywords": [],
                "hashtags": []
            },
            
            # Music
            "music": self._build_music_structure(cleaned_data),
            
            # Ticketing
            "ticketing": self._build_ticketing_structure(cleaned_data),
            
            # Scraping metadata
            "scraping_metadata": {
                "source_platform": source_platform,
                "source_url": source_url,
                "source_event_id": cleaned_data.get('source_id'),
                "first_scraped": datetime.now(timezone.utc).isoformat(),
                "last_scraped": datetime.now(timezone.utc).isoformat(),
                "scraper_version": "2.0",
                "raw_data": cleaned_data.get('_original', {})
            },
            
            # Default structures
            "data_quality": {
                "overall_score": 0,
                "field_quality_scores": {},
                "validation_flags": [],
                "manual_verification": {"is_verified": False}
            },
            
            "deduplication": {
                "is_canonical": True,
                "merged_from_ids": [],
                "merge_log": []
            },
            
            "knowledge_graph": {
                "related_events": [],
                "audience_profile_tags": [],
                "influence_score": 0
            },
            
            "analytics": {
                "views": 0,
                "saves": 0,
                "clicks_to_tickets": 0
            },
            
            # Timestamps
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            
            "system_flags": {
                "is_featured": False,
                "is_hidden": False
            }
        }
        
        return unified_event
    
    def _determine_event_type(self, data: Dict[str, Any]) -> str:
        """Determine event type from data"""
        title = data.get('title', '').lower()
        venue = data.get('venue', '').lower()
        
        # Simple heuristics - can be enhanced with ML
        if any(word in title for word in ['festival', 'fest']):
            return 'festival'
        elif any(word in venue for word in ['beach', 'pool', 'outdoor']):
            return 'day_party'
        elif any(word in title for word in ['live', 'concert', 'band']):
            return 'concert'
        else:
            return 'club_night'
    
    def _build_datetime_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build datetime structure with validation"""
        dt_obj = data.get('datetime_obj')
        
        if isinstance(dt_obj, datetime):
            start_date = dt_obj.isoformat()
        else:
            start_date = data.get('start_date', datetime.now(timezone.utc).isoformat())
        
        # Detect timezone from venue if not specified
        timezone_str = data.get('timezone', 'Europe/Madrid')
        
        return {
            "start_date": start_date,
            "end_date": data.get('end_date'),
            "timezone": timezone_str,
            "doors_open": None,
            "last_entry": None,
            "is_all_day": False,
            "duration_hours": None,
            "recurring": {
                "is_recurring": False,
                "frequency": None,
                "pattern_description": data.get('raw_date_string'),
                "end_recurrence": None
            }
        }
    
    def _build_venue_structure(self, venue_name: str, stage_acts: List[Dict]) -> Dict[str, Any]:
        """Build venue structure"""
        venue_id = self.generate_unique_id("venue", venue_name)
        
        return {
            "venue_id": venue_id,
            "name": venue_name,
            "address": {
                "street": None,
                "city": "Ibiza",  # Default for Ibiza events
                "state": None,
                "country": "Spain",
                "postal_code": None,
                "full_address": None
            },
            "coordinates": {"type": "Point", "coordinates": []},
            "venue_type": "club",
            "total_capacity": None,
            "has_disabled_access": None,
            "website": None,
            "social_links": {},
            "stage_count": 1,
            "stages": [{
                "stage_id": "main_stage",
                "stage_name": "Main Stage",
                "capacity": None,
                "stage_type": "main_stage",
                "host": {"host_name": None, "host_id": None},
                "stage_genres": [],
                "acts": stage_acts
            }]
        }
    
    def _process_acts(self, artists_data: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """Process artists into acts and stage references"""
        acts = []
        stage_refs = []
        
        for idx, artist in enumerate(artists_data):
            if not isinstance(artist, dict):
                continue
                
            act_id = self.generate_unique_id("artist", artist.get('name', 'unknown'))
            
            # Top-level act
            act = {
                "act_id": act_id,
                "act_name": artist.get('name', 'Unknown Artist'),
                "act_type": artist.get('role', 'dj').lower(),
                "genres": [],
                "styles": [],
                "social_media": {},
                "popularity_metrics": {}
            }
            acts.append(act)
            
            # Stage reference
            stage_ref = {
                "act_id": act_id,
                "set_time": {
                    "start": None,
                    "end": None,
                    "duration_minutes": None
                },
                "billing_order": idx + 1,
                "is_headliner": idx == 0  # First artist is headliner
            }
            stage_refs.append(stage_ref)
        
        return acts, stage_refs
    
    def _build_music_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build music structure"""
        genres = data.get('genres', [])
        primary_genre = genres[0] if genres else None
        
        if primary_genre:
            primary_genre = self.field_normalizer.normalize_genre(primary_genre)
        
        return {
            "primary_genre": primary_genre,
            "sub_genres": [self.field_normalizer.normalize_genre(g) for g in genres],
            "styles": [],
            "mood_tags": [],
            "energy_level": None,
            "genre_confidence": None
        }
    
    def _build_ticketing_structure(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Build ticketing structure"""
        return {
            "tickets_url": data.get('tickets_url'),
            "is_free": False,
            "age_restriction": {
                "minimum_age": 18,  # Default for club events
                "restriction_type": "18+"
            },
            "promos": [],
            "tiers": [],
            "external_platforms": []
        }
    
    def _validate_and_normalize_fields(self, event: Dict[str, Any], result: ValidationResult):
        """Apply additional field-level validation"""
        # Validate required fields
        required_fields = ['event_id', 'title', 'datetime', 'venue', 'scraping_metadata']
        for field in required_fields:
            if not event.get(field):
                result.add_issue(
                    ValidationLevel.CRITICAL,
                    field,
                    f"Required field '{field}' is missing"
                )
        
        # Validate datetime
        dt_data = event.get('datetime', {})
        if not dt_data.get('start_date'):
            result.add_issue(
                ValidationLevel.ERROR,
                'datetime.start_date',
                'Start date is required'
            )
        
        # Validate venue
        venue_data = event.get('venue', {})
        if not venue_data.get('name'):
            result.add_issue(
                ValidationLevel.ERROR,
                'venue.name',
                'Venue name is required'
            )
    
    def _check_schema_compliance(self, event: Dict[str, Any]) -> List[Dict[str, str]]:
        """Check compliance with schema requirements"""
        issues = []
        
        # Check data types
        if not isinstance(event.get('acts'), list):
            issues.append({
                'field': 'acts',
                'issue': 'Must be an array'
            })
        
        if not isinstance(event.get('venue'), dict):
            issues.append({
                'field': 'venue',
                'issue': 'Must be an object'
            })
        
        # Check nested requirements
        venue = event.get('venue', {})
        if venue and not venue.get('stages'):
            issues.append({
                'field': 'venue.stages',
                'issue': 'Stages array is required'
            })
        
        return issues


# Backward compatibility function
def map_to_unified_schema(raw_data: dict, source_platform: str, source_url: str) -> dict:
    """
    Backward compatible function that uses the enhanced adapter
    """
    adapter = EnhancedSchemaAdapter()
    return adapter.map_to_unified_schema(raw_data, source_platform, source_url)


# Example usage
if __name__ == "__main__":
    # Test with sample data
    sample_raw_data = {
        "title": "Techno Night at Amnesia",
        "time": "Friday 15 July 2025",
        "venue": "amnesia",
        "lineup": [
            {"name": "Charlotte de Witte", "role": "headliner"},
            {"name": "Amelie Lens", "role": "support"}
        ],
        "url": "www.ibiza-spotlight.com/event/12345",
        "genres": ["techno", "melodic-techno"]
    }
    
    adapter = EnhancedSchemaAdapter()
    result = adapter.map_to_unified_schema(
        sample_raw_data,
        "ibiza-spotlight",
        "https://www.ibiza-spotlight.com/event/12345"
    )
    
    print("Validation Metadata:")
    print(f"Errors: {len(result['validation_metadata']['validation_errors'])}")
    print(f"Warnings: {len(result['validation_metadata']['validation_warnings'])}")
    print(f"Confidence Score: {result['validation_metadata']['confidence_score']}")
    print(f"\nData Quality Score: {result['data_quality']['overall_score']}")
