#!/usr/bin/env python3
"""
Export MongoDB events to Markdown format
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from pymongo import MongoClient

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Assuming classy_skkkrapey is one level up from database directory
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from classy_skkkrapey.config import settings
    MONGODB_URI_FROM_SETTINGS = settings.MONGODB_URI
    DB_NAME_FROM_SETTINGS = settings.DB_NAME
except ImportError:
    print("Warning: Could not import settings from classy_skkkrapey.config. Using hardcoded MongoDB details.")
    MONGODB_URI_FROM_SETTINGS = "mongodb://localhost:27017/classy_skkkrapey"
    DB_NAME_FROM_SETTINGS = "classy_skkkrapey"


# MongoDB configuration
MONGODB_URI = os.getenv("MONGODB_URI", MONGODB_URI_FROM_SETTINGS) # Prioritize env var
DB_NAME = os.getenv("DB_NAME", DB_NAME_FROM_SETTINGS) # Prioritize env var
COLLECTION_NAME = "events" # This is standard

def export_to_markdown():
    """Export all events to markdown file"""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Query all events, projecting only necessary fields
        events = list(collection.find({}, {
            "title": 1,
            "datetime.start_date": 1,
            "datetime.recurring.pattern_description": 1, # For better date display
            "venue.name": 1,
            "venue.website": 1,
            "venue.stages.host.host_name": 1,
            "acts.act_name": 1,
            "scraping_metadata.source_url": 1,
            "scraping_metadata.extraction_method": 1,
            "data_quality.overall_score": 1,
            "data_quality.field_quality_scores": 1,
            "content.short_description": 1, # Use short_description for brevity
        }))
        
        if not events:
            print("No events found in database")
            return
        
        # Generate markdown content
        md_content = f"# Events Export ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})\n\n"
        md_content += f"Total events: {len(events)}\n\n"
        
        for event in events:
            md_content += f"## {event.get('title', 'Untitled Event')}\n"
            
            date_str = event.get('datetime', {}).get('recurring', {}).get('pattern_description') or \
                       event.get('datetime', {}).get('start_date', 'N/A')
            md_content += f"- **Date**: {date_str}\n"
            md_content += f"- **Venue**: {event.get('venue', {}).get('name', 'N/A')}\n"

            # Display host name from the first stage as a proxy for promoter
            host_name = "N/A"
            stages = event.get('venue', {}).get('stages', [])
            if stages and isinstance(stages, list) and len(stages) > 0:
                first_stage_host = stages[0].get('host', {})
                if first_stage_host and first_stage_host.get('host_name'):
                    host_name = first_stage_host['host_name']
            md_content += f"- **Host/Promoter**: {host_name}\n"

            venue_website = event.get('venue', {}).get('website', '')
            if venue_website:
                md_content += f"- **Venue Website**: [{venue_website}]({venue_website})\n"
            else:
                # Fallback to source_url if no venue website
                source_url = event.get('scraping_metadata', {}).get('source_url', '')
                if source_url:
                    md_content += f"- **Source URL**: [{source_url}]({source_url})\n"

            acts_list = event.get('acts', [])
            if acts_list:
                md_content += "- **Lineup**:\n"
                for act in acts_list[:5]: # Show max 5 artists for brevity
                    md_content += f"  - {act.get('act_name', 'Unknown Artist')}\n"
                if len(acts_list) > 5:
                    md_content += f"  - ...and {len(acts_list) - 5} more.\n"
            
            md_content += f"- **Extraction Method**: {event.get('scraping_metadata', {}).get('extraction_method', 'N/A')}\n"
            
            # Add quality scores
            dq = event.get('data_quality', {})
            if dq:
                md_content += "- **Quality Scores**:\n"
                md_content += f"  - Overall: {dq.get('overall_score', 0.0):.2f}\n" # V2 scores are 0.0-1.0
                fqs = dq.get('field_quality_scores', {})
                if fqs: # Display specific field scores if available
                    md_content += f"  - Title: {fqs.get('title', 0.0):.2f}\n"
                    md_content += f"  - DateTime: {fqs.get('datetime', 0.0):.2f}\n"
                    md_content += f"  - Venue: {fqs.get('venue', 0.0):.2f}\n"
                    md_content += f"  - Acts: {fqs.get('acts', 0.0):.2f}\n"
                    md_content += f"  - Ticketing: {fqs.get('ticketing', 0.0):.2f}\n"

            description = event.get('content', {}).get('short_description', '')
            if not description: # Fallback to a part of full_description if short is empty
                 full_desc = event.get('content', {}).get('full_description', '')
                 if full_desc: description = full_desc[:200] + "..." if len(full_desc) > 200 else full_desc
            
            md_content += f"\n{description or 'No description available.'}\n\n"
            md_content += "---\n\n"
        
        # Create output directory if needed
        # Using PROJECT_ROOT which is now parent of 'database'
        output_dir = PROJECT_ROOT / "output" / "markdown_reports"
        output_dir.mkdir(exist_ok=True)
        
        # Save to timestamped file
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_path = output_dir / f"events_export_{timestamp}.md"
        
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(md_content)
        
        print(f"Exported {len(events)} events to {output_path}")
        
    except Exception as e:
        print(f"Export failed: {e}")

if __name__ == "__main__":
    export_to_markdown()