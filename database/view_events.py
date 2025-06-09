"""
MongoDB Event Data Viewer
Connects to MongoDB and displays event data in a readable format
"""

import sys
import os
# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from classy_skkkrapey.database.mongodb_setup import MongoDBSetup
import json
from datetime import datetime

def get_all_events():
    """Retrieve all events from MongoDB"""
    setup = MongoDBSetup()
    if not setup.connect():
        print("Failed to connect to MongoDB")
        return []
    
    try:
        events = list(setup.db.events.find())
        print(f"Found {len(events)} events")
        return events
    finally:
        setup.close()

def format_event(event):
    """Format a single event for readable display"""
    # Convert MongoDB ObjectId to string and remove MongoDB-specific fields
    event.pop('_id', None)
    event.pop('_quality', None)
    event.pop('_validation', None)
    
    # Format datetime objects
    for field in ['scrapedAt', 'dateTime.start', 'dateTime.end']:
        parts = field.split('.')
        obj = event
        for part in parts[:-1]:
            obj = obj.get(part, {})
        if parts[-1] in obj and isinstance(obj[parts[-1]], datetime):
            obj[parts[-1]] = obj[parts[-1]].strftime('%Y-%m-%d %H:%M:%S')
    
    return event

def save_events_to_markdown(events, filename):
    """Save events to a Markdown file with formatted output"""
    with open(filename, 'w') as f:
        f.write("# MongoDB Event Data\n\n")
        f.write(f"## Total Events: {len(events)}\n\n")
        
        for i, event in enumerate(events, 1):
            f.write(f"### Event {i}\n")
            f.write("```json\n")
            f.write(json.dumps(format_event(event), indent=2))
            f.write("\n```\n\n")

if __name__ == "__main__":
    events = get_all_events()
    if events:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"output/events_export_{timestamp}.md"
        save_events_to_markdown(events, filename)
        print(f"Event data saved to {filename}")
        print("Open this file in VSCode to view the formatted event data")