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
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# MongoDB configuration
MONGODB_URI = "mongodb://localhost:27017/classy_skkkrapey"
DB_NAME = "classy_skkkrapey"
COLLECTION_NAME = "events"

def export_to_markdown():
    """Export all events to markdown file"""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Query all events
        events = list(collection.find({}))
        
        if not events:
            print("No events found in database")
            return
        
        # Generate markdown content
        md_content = f"# Events Export ({datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')})\n\n"
        md_content += f"Total events: {len(events)}\n\n"
        
        for event in events:
            md_content += f"## {event.get('title', 'Untitled Event')}\n"
            md_content += f"- **Date**: {event.get('date', 'N/A')}\n"
            md_content += f"- **Location**: {event.get('location', 'N/A')}\n"
            md_content += f"- **Promoter URL**: [{event.get('promoterUrl', '')}]({event.get('promoterUrl', '')})\n"
            
            if event.get('lineup'):
                md_content += "- **Lineup**:\n"
                for artist in event['lineup']:
                    md_content += f"  - {artist}\n"
            
            md_content += f"- **Extraction Method**: {event.get('extractionMethod', 'N/A')}\n"
            
            # Add quality scores
            if '_quality' in event:
                q = event['_quality']
                md_content += "- **Quality Scores**:\n"
                md_content += f"  - Overall: {q.get('overall', 0)}%\n"
                md_content += f"  - Title: {q.get('title', 0)}%\n"
                md_content += f"  - DateTime: {q.get('dateTime', 0)}%\n"
                md_content += f"  - Lineup: {q.get('lineUp', 0)}%\n"
            
            md_content += f"\n{event.get('description', '')}\n\n"
            md_content += "---\n\n"
        
        # Create output directory if needed
        output_dir = PROJECT_ROOT / "output"
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