"""
Fix MongoDB schema to implement the unified event schema for comprehensive data storage.

This script updates the MongoDB validation schema to support the new unified structure
with enhanced fields for artists, ticketing, organization, and data quality tracking.
"""

import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pymongo import MongoClient
import logging
from helpers.schemas import get_mongodb_validation_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fix_schema():
    """Update the MongoDB schema to use the unified event schema"""
    
    # Get MongoDB connection details from environment or use defaults
    mongodb_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DB_NAME", "classy_skkkrapey")
    
    client = MongoClient(mongodb_uri)
    db = client[db_name]
    
    # Get the unified validation schema
    validation_schema = get_mongodb_validation_schema()
    
    try:
        # First, check if collection exists
        if "events" not in db.list_collection_names():
            logger.info("Creating 'events' collection...")
            db.create_collection("events")
        
        # Remove existing validation to avoid conflicts
        logger.info("Removing existing validation schema...")
        db.command("collMod", "events", validator={})
        
        # Apply the new unified validation schema
        logger.info("Applying unified validation schema to 'events' collection...")
        db.command("collMod", "events", validator=validation_schema)
        
        logger.info("✅ Schema updated successfully!")
        
        # Create indexes for better query performance
        logger.info("Creating indexes...")
        create_indexes(db)
        
        print("\n✅ MongoDB schema update completed successfully!")
        print(f"Database: {db_name}")
        print("\nThe events collection now supports the unified schema with:")
        print("  - Core event information (event_id, title, type, status)")
        print("  - Comprehensive datetime fields with timezone support")
        print("  - Enhanced venue information with coordinates")
        print("  - Rich content and description fields")
        print("  - Detailed artist and lineup tracking")
        print("  - Music genre and style categorization")
        print("  - Multi-tier ticketing information")
        print("  - Organization and promoter details")
        print("  - Media asset management")
        print("  - Scraping metadata tracking")
        print("  - Data quality scoring and validation")
        
    except Exception as e:
        logger.error(f"Error updating schema: {e}")
        raise
    finally:
        client.close()


def create_indexes(db):
    """Create indexes for optimal query performance"""
    
    indexes_created = []
    
    try:
        # Unique index on event_id (Primary Key)
        db.events.create_index("event_id", unique=True, name="idx_event_id_unique")
        indexes_created.append("event_id (unique)")

        # Core V2 Fields marked for indexing
        db.events.create_index("type", name="idx_type")
        indexes_created.append("type")

        db.events.create_index("status", name="idx_status")
        indexes_created.append("status")

        db.events.create_index("datetime.start_date", name="idx_datetime_start_date")
        indexes_created.append("datetime.start_date")
        
        db.events.create_index("venue.name", name="idx_venue_name")
        indexes_created.append("venue.name")

        db.events.create_index("venue.address.city", name="idx_venue_address_city")
        indexes_created.append("venue.address.city")

        db.events.create_index("acts.act_id", name="idx_acts_act_id") # Indexing array of strings
        indexes_created.append("acts.act_id")
        
        db.events.create_index("acts.act_name", name="idx_acts_act_name") # Indexing array of strings
        indexes_created.append("acts.act_name")

        db.events.create_index("music.primary_genre", name="idx_music_primary_genre")
        indexes_created.append("music.primary_genre")

        db.events.create_index("scraping_metadata.source_platform", name="idx_scraping_metadata_source_platform")
        indexes_created.append("scraping_metadata.source_platform")
        
        # Index on source_url as it's frequently used for deduplication/identification
        db.events.create_index("scraping_metadata.source_url", name="idx_scraping_metadata_source_url")
        indexes_created.append("scraping_metadata.source_url")

        db.events.create_index("scraping_metadata.last_scraped", name="idx_scraping_metadata_last_scraped")
        indexes_created.append("scraping_metadata.last_scraped")

        db.events.create_index("data_quality.overall_score", name="idx_data_quality_overall_score")
        indexes_created.append("data_quality.overall_score")
        
        db.events.create_index("deduplication.is_canonical", name="idx_deduplication_is_canonical")
        indexes_created.append("deduplication.is_canonical")

        db.events.create_index("created_at", name="idx_created_at")
        indexes_created.append("created_at")

        # Geospatial index for venue.coordinates
        db.events.create_index([("venue.coordinates", "2dsphere")], name="idx_venue_coordinates_2dsphere")
        indexes_created.append("venue.coordinates (2dsphere)")

        # Additional useful indexes for querying nested arrays/objects
        db.events.create_index("venue.stages.host.host_name", name="idx_venue_stages_host_name")
        indexes_created.append("venue.stages.host.host_name")
        
        # Text index for searching (ensure fields exist in V2)
        # V2 fields: title, content.short_description, content.full_description
        db.events.create_index([
            ("title", "text"),
            ("content.short_description", "text"),
            ("content.full_description", "text"),
            ("venue.name", "text"),
            ("acts.act_name", "text") # Including act names in text search
        ], name="idx_text_search_all")
        indexes_created.append("Text Search (title, descriptions, venue, acts)")

        logger.info(f"Attempted to create/ensure {len(indexes_created)} indexes.")
        # Listing actual indexes to confirm (MongoDB handles if they already exist)
        final_indexes = list(db.events.list_indexes())
        logger.info(f"Current indexes on 'events' collection ({len(final_indexes)}):")
        for idx_info in final_indexes:
            logger.info(f"  - Name: {idx_info['name']}, Key: {idx_info['key']}")
            
    except Exception as e:
        logger.error(f"Error creating indexes: {e}", exc_info=True)


def verify_schema():
    """Verify the schema was applied correctly"""
    
    mongodb_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    db_name = os.getenv("MONGO_DB_NAME", "classy_skkkrapey")
    
    client = MongoClient(mongodb_uri)
    db = client[db_name]
    
    try:
        # Get collection info
        coll_info = db.command("listCollections", filter={"name": "events"})
        
        if coll_info.get("cursor", {}).get("firstBatch"):
            validator = coll_info["cursor"]["firstBatch"][0].get("options", {}).get("validator")
            if validator:
                print("\n✅ Validation schema is active")
                # Count required fields
                required_fields = validator.get("$jsonSchema", {}).get("required", [])
                print(f"Required fields: {', '.join(required_fields)}")
            else:
                print("\n⚠️  No validation schema found")
        
        # Show index information
        indexes = list(db.events.list_indexes())
        print(f"\nIndexes ({len(indexes)}):")
        for idx in indexes:
            print(f"  - {idx.get('name')}: {idx.get('key')}")
            
    finally:
        client.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fix MongoDB schema for unified event structure")
    parser.add_argument("--verify", action="store_true", help="Verify schema after update")
    args = parser.parse_args()
    
    fix_schema()
    
    if args.verify:
        print("\n" + "="*50)
        print("SCHEMA VERIFICATION")
        print("="*50)
        verify_schema()