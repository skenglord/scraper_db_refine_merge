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
        # Unique index on event_id
        db.events.create_index("event_id", unique=True)
        indexes_created.append("event_id (unique)")
        
        # Index on scraping metadata
        db.events.create_index("scraping_metadata.source_url")
        indexes_created.append("scraping_metadata.source_url")
        
        # Index on datetime for date-based queries
        db.events.create_index("datetime.start_datetime")
        indexes_created.append("datetime.start_datetime")
        
        # Index on venue for location-based queries
        db.events.create_index("venue.name")
        indexes_created.append("venue.name")
        
        # Index on venue city
        db.events.create_index("venue.city")
        indexes_created.append("venue.city")
        
        # Compound index on venue coordinates for geo queries
        db.events.create_index([("venue.coordinates.latitude", 1), ("venue.coordinates.longitude", 1)])
        indexes_created.append("venue.coordinates (compound)")
        
        # Index on event type
        db.events.create_index("event_type")
        indexes_created.append("event_type")
        
        # Index on status
        db.events.create_index("status")
        indexes_created.append("status")
        
        # Index on data quality score for quality-based filtering
        db.events.create_index("data_quality.overall_score")
        indexes_created.append("data_quality.overall_score")
        
        # Index on scraping timestamp
        db.events.create_index("scraping_metadata.scraped_at")
        indexes_created.append("scraping_metadata.scraped_at")
        
        # Text index for full-text search on title and descriptions
        db.events.create_index([
            ("title", "text"),
            ("content.short_description", "text"),
            ("content.full_description", "text")
        ])
        indexes_created.append("text search (title, descriptions)")
        
        logger.info(f"Created {len(indexes_created)} indexes")
        for idx in indexes_created:
            logger.info(f"  - {idx}")
            
    except Exception as e:
        logger.warning(f"Some indexes may already exist: {e}")


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