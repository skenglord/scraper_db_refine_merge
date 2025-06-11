"""
Data Migration Script for Tickets Ibiza Event Data
Consolidates data from JSON files into MongoDB with quality scoring
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError
import logging
import re
from pathlib import Path

# Assuming schema_adapter.py is in the parent directory (project root)
# Import moved inside function to avoid circular import
# from mongodb_setup import MongoDBSetup
from quality_scorer import QualityScorer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataMigration:
    """Handles migration of event data from JSON files to MongoDB"""
    
    def __init__(self, db_connection_string: str = "mongodb://localhost:27017/",
                 database_name: str = "tickets_ibiza_events"):
        """Initialize migration with database connection"""
        self.client = MongoClient(db_connection_string)
        self.db = self.client[database_name]
        self.scorer = QualityScorer()
        self.stats = {
            "total_processed": 0,
            "successfully_migrated": 0,
            "duplicates_found": 0,
            "errors": 0,
            "quality_scores": []
        }
    
    def load_json_file(self, filepath: str) -> Optional[Dict]:
        """Load and parse JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading {filepath}: {e}")
            return None
    
    def parse_event_from_scraped_data(self, event_data: Dict) -> Dict:
        """
        Transforms event data from the source JSON file to unifiedEventsSchema_v2
        using the schema_adapter.
        """
        source_url = event_data.get("url", "")
        # Determine source_platform; if the JSON files are all from one platform,
        # it can be hardcoded. Otherwise, it needs to be inferred or passed.
        # Assuming 'ticketsibiza_scraped_data.json' implies platform.
        source_platform_name = "ticketsibiza_json_import"
        
        # The original `event_data` is treated as the `raw_data` input
        # for the adapter.
        try:
            # Assuming map_to_unified_schema is globally available or imported elsewhere.
            # If it's part of a class or needs specific import, this must be adjusted.
            # For now, direct call:
            from schema_adapter import map_to_unified_schema # Ensure this import is valid in your project structure
            unified_event = map_to_unified_schema(
                raw_data=event_data,
                source_platform=source_platform_name,
                source_url=source_url
            )
            return unified_event
        except Exception as e:
            logger.error(f"Error mapping event data for URL {source_url} using schema_adapter: {e}")
            # Return a minimal structure or re-raise, depending on desired error handling
            return {} # Or None, and handle upstream
    
    def deduplicate_events(self, events: List[Dict]) -> List[Dict]:
        """Remove duplicate events based on source_url and start_date from unified schema"""
        seen = set()
        unique_events = []
        
        for event in events:
            if not event: # Handle cases where parse_event_from_scraped_data might return None/empty
                continue
            # Create unique key from source_url and start_date
            key = event.get("scraping_metadata", {}).get("source_url", "")
            if event.get("datetime", {}).get("start_date"):
                key += str(event["datetime"]["start_date"])
            
            if key not in seen:
                seen.add(key)
                unique_events.append(event)
            else:
                self.stats["duplicates_found"] += 1
                logger.info(f"Duplicate found: {event.get('title', 'Unknown Title')} for key {key}")
        
        return unique_events
    
    def migrate_events(self, events: List[Dict], batch_size: int = 100):
        """Migrate events to MongoDB in batches using event_id as the primary key"""
        logger.info(f"Starting migration of {len(events)} events")
        
        for i in range(0, len(events), batch_size):
            batch = events[i:i + batch_size]
            operations = []
            
            for event in batch:
                if not event or not event.get("event_id"): # Skip if event is empty or has no event_id
                    logger.warning(f"Skipping event due to missing data or event_id: {event.get('title', 'N/A')}")
                    self.stats["errors"] += 1
                    continue

                self.stats["total_processed"] += 1
                
                # QualityScorer now operates on the unified schema.
                # The QualityScorer itself might need updates if its internal field access
                # is not compatible with unifiedEventsSchema_v2.
                # For this task, we assume QualityScorer is adapted or will be.
                # The quality score is expected to be part of the `data_quality` field
                # within the `event` dict returned by `map_to_unified_schema`.
                # So, no explicit call to self.scorer here if adapter handles it.
                # If adapter does not set data_quality, it would be:
                # event["data_quality"] = self.scorer.calculate_event_quality_unified(event)
                # For now, assume map_to_unified_schema populates event["data_quality"]
                
                # Track quality scores (assuming overall_score is populated by adapter or scorer)
                if event.get("data_quality") and "overall_score" in event["data_quality"]:
                    self.stats["quality_scores"].append(event["data_quality"]["overall_score"])
                
                # Create upsert operation using event_id
                operations.append(
                    UpdateOne(
                        {"event_id": event["event_id"]}, # Use event_id as the unique key
                        {"$set": event},
                        upsert=True
                    )
                )
            
            if not operations: # Skip if batch is empty
                continue

            # Execute batch operation
            try:
                result = self.db.events.bulk_write(operations)
                self.stats["successfully_migrated"] += result.modified_count + result.upserted_count
                logger.info(f"Batch {i//batch_size + 1}: Migrated {result.modified_count + result.upserted_count} events using event_id")
            except BulkWriteError as e:
                logger.error(f"Batch write error: {e.details}")
                # Count errors more accurately based on writeErrors if possible
                error_count = sum(1 for err in e.details.get('writeErrors', []) if err.get('code') != 11000) # Exclude duplicate key errors if upserting
                self.stats["errors"] += error_count
                # Log duplicate errors separately if needed
                duplicate_errors = sum(1 for err in e.details.get('writeErrors', []) if err.get('code') == 11000)
                if duplicate_errors > 0:
                     logger.warning(f"{duplicate_errors} duplicate key errors encountered during bulk write (event_id).")


    
    def migrate_from_json_files(self, json_file_path: str, parsed_md_path: Optional[str] = None):
        """Main migration function"""
        logger.info("Starting data migration process")
        self.stats = { "total_processed": 0, "successfully_migrated": 0, "duplicates_found": 0, "errors": 0, "quality_scores": [] } # Reset stats

        # Load main JSON file
        raw_event_list = self.load_json_file(json_file_path) # Expecting a list of events directly
        if not raw_event_list or not isinstance(raw_event_list, list):
            logger.error(f"Failed to load or parse event list from JSON file: {json_file_path}")
            if raw_event_list: logger.error(f"Loaded data type: {type(raw_event_list)}")
            return
        
        # Parse events using schema_adapter
        mapped_events = []
        for event_data in raw_event_list:
            if not isinstance(event_data, dict):
                logger.warning(f"Skipping non-dictionary item in JSON list: {type(event_data)}")
                self.stats["errors"] +=1
                continue
            parsed_event = self.parse_event_from_scraped_data(event_data)
            if parsed_event: # Only add if mapping was successful
                mapped_events.append(parsed_event)
            else:
                self.stats["errors"] +=1 # Count as error if mapping fails and returns empty/None
        
        logger.info(f"Mapped {len(mapped_events)} events from JSON file using schema_adapter")
        
        # Deduplicate based on new schema fields
        unique_events = self.deduplicate_events(mapped_events)
        logger.info(f"Found {len(unique_events)} unique events after deduplication (duplicates found: {self.stats['duplicates_found']})")
        
        # Migrate to MongoDB
        self.migrate_events(unique_events)
        
        # Print summary
        self.print_migration_summary()
    
    def print_migration_summary(self):
        """Print migration statistics"""
        print("\n" + "="*50)
        print("MIGRATION SUMMARY")
        print("="*50)
        print(f"Total events processed: {self.stats['total_processed']}")
        print(f"Successfully migrated: {self.stats['successfully_migrated']}")
        print(f"Duplicates found: {self.stats['duplicates_found']}")
        print(f"Errors: {self.stats['errors']}")
        
        if self.stats['quality_scores']:
            avg_quality = sum(self.stats['quality_scores']) / len(self.stats['quality_scores'])
            print(f"\nAverage quality score: {avg_quality:.3f}")
            print(f"Highest quality score: {max(self.stats['quality_scores']):.3f}")
        print(f"Lowest quality score: {min(self.stats['quality_scores'] if self.stats['quality_scores'] else [0]):.3f}")
        
        print("="*50)
    
    def create_quality_report(self) -> Dict[str, Any]:
        """Generate detailed quality report for migrated data based on data_quality.overall_score"""
        pipeline = [
            {
                "$match": {"data_quality.overall_score": {"$exists": True}} # Ensure score exists
            },
            {
                "$group": {
                    "_id": None,
                    "totalEvents": {"$sum": 1},
                    "avgQuality": {"$avg": "$data_quality.overall_score"},
                    "excellentQuality": { # Assuming score 0.0 to 1.0
                        "$sum": {"$cond": [{"$gte": ["$data_quality.overall_score", 0.9]}, 1, 0]}
                    },
                    "goodQuality": {
                        "$sum": {"$cond": [
                            {"$and": [
                                {"$gte": ["$data_quality.overall_score", 0.8]},
                                {"$lt": ["$data_quality.overall_score", 0.9]}
                            ]}, 1, 0
                        ]}
                    },
                    "fairQuality": {
                        "$sum": {"$cond": [
                            {"$and": [
                                {"$gte": ["$data_quality.overall_score", 0.7]},
                                {"$lt": ["$data_quality.overall_score", 0.8]}
                            ]}, 1, 0
                        ]}
                    },
                    "poorQuality": { # Corrected to less than 0.7
                        "$sum": {"$cond": [{"$lt": ["$data_quality.overall_score", 0.7]}, 1, 0]}
                    }
                }
            }
        ]
        
        result = list(self.db.events.aggregate(pipeline))
        
        if result:
            report = result[0]
            report["qualityDistribution"] = {
                "excellent": report.pop("excellentQuality", 0),
                "good": report.pop("goodQuality", 0),
                "fair": report.pop("fairQuality", 0),
                "poor": report.pop("poorQuality", 0)
            }
            return report
        
        return {"error": "No data found"}
    
    def close(self):
        """Close database connection"""
        self.client.close()


def main():
    """Main migration function"""
    # File paths - adjust these to your actual file locations
    json_file_path = "../ticketsibiza_scraped_data.json"
    
    # Check if files exist
    if not os.path.exists(json_file_path):
        logger.error(f"JSON file not found: {json_file_path}")
        logger.info("Please ensure the file path is correct")
        return
    
    # Initialize migration
    migration = DataMigration()
    
    try:
        # Run migration
        migration.migrate_from_json_files(json_file_path)
        
        # Generate quality report
        print("\nGenerating quality report...")
        report = migration.create_quality_report()
        
        if "error" not in report:
            print("\nQUALITY REPORT")
            print("="*50)
            print(f"Total events in database: {report.get('totalEvents', 0)}")
            print(f"Average quality score: {report.get('avgQuality', 0):.3f}")
            print("\nQuality Distribution:")
            dist = report.get('qualityDistribution', {})
            for level, count in dist.items():
                print(f"  {level.capitalize()}: {count} events")
        
    finally:
        migration.close()


if __name__ == "__main__":
    main()