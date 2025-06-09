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
    event.pop('_id', None) # _id is still standard
    event.pop('data_quality', None) # Pop the whole data_quality object
    # No direct _validation equivalent at top level; validation_flags is inside data_quality

    # Format datetime objects if they are not already strings
    # V2 stores dates as ISO strings, so this loop might not do much
    # if data is already compliant. It's kept for path correction demonstration
    # and if any datetime objects somehow exist.
    date_fields_to_check = [
        'scraping_metadata.first_scraped',
        'scraping_metadata.last_scraped',
        'datetime.start_date',
        'datetime.end_date',
        'datetime.doors_open',
        'datetime.last_entry',
        'datetime.recurring.end_recurrence',
        'ticketing.tiers.sale_start', # Note: this is nested in an array, loop won't handle directly
        'ticketing.tiers.sale_end',   # Loop won't handle directly
        'created_at',
        'updated_at'
    ]

    for field_path in date_fields_to_check:
        parts = field_path.split('.')
        obj = event
        # Traverse the path
        # This simple loop won't handle arrays like ticketing.tiers.sale_start.
        # A more complex recursive function would be needed for full datetime conversion
        # if they weren't already strings. For this task, focusing on direct paths.
        try:
            for part in parts[:-1]:
                obj = obj.get(part, {})

            last_part = parts[-1]
            if last_part in obj and isinstance(obj[last_part], datetime):
                obj[last_part] = obj[last_part].isoformat() # Convert to ISO string
            # If it's already a string (as expected for V2), no change needed by this logic.
        except AttributeError: # Raised if a part of the path doesn't exist / is not a dict
            continue
    
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