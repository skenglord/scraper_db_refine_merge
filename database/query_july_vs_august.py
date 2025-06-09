"""
MongoDB Query: July vs August Events Comparison
"""
import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "classy_skkkrapey")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    """Establish connection to MongoDB"""
    try:
        is_atlas = "mongodb+srv" in MONGO_URI
        tls_options = {'tls': True, 'tlsCAFile': certifi.where()} if is_atlas else {}
        
        client = MongoClient(MONGO_URI, **tls_options)
        client.admin.command('ping')
        db = client[MONGO_DB_NAME]
        logger.info(f"Connected to MongoDB: {MONGO_DB_NAME}")
        return db
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return None

def query_events_by_month(db, year, month):
    """Query events for a specific month"""
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year+1, 1, 1)
    else:
        end_date = datetime(year, month+1, 1)
    
    query = {
        "dateTime.start": {
            "$gte": start_date,
            "$lt": end_date
        }
    }
    
    return list(db.events.find(query))

def main():
    db = connect_to_mongodb()
    if db is None:
        return
    
    # Debugging: show database stats
    print(f"Database name: {db.name}")
    print(f"Collections: {db.list_collection_names()}")
    
    # Check if events collection exists
    if 'events' not in db.list_collection_names():
        print("Error: 'events' collection not found")
        return
    
    # Get event count
    total_events = db.events.count_documents({})
    print(f"Total events in database: {total_events}")
    
    # Query July 2025 events
    july_events = query_events_by_month(db, 2025, 7)
    august_events = query_events_by_month(db, 2025, 8)
    
    print(f"\nJuly 2025 Events: {len(july_events)}")
    print(f"August 2025 Events: {len(august_events)}")

if __name__ == "__main__":
    main()