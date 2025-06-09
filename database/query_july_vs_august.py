"""
MongoDB Query: July vs August Events Comparison
"""
import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import certifi # Added for tlsCAFile

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/") # Changed from MONGO_DB_CONNECTION_STRING
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "classy_skkkrapey")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    """Establish connection to MongoDB"""
    try:
        is_atlas = "mongodb+srv" in MONGO_URI
        # Ensure certifi.where() is called correctly for tlsCAFile
        tls_options = {'tls': True, 'tlsCAFile': certifi.where()} if is_atlas else {}
        
        client = MongoClient(MONGO_URI, **tls_options)
        client.admin.command('ping') # Verify connection
        db = client[MONGO_DB_NAME]
        logger.info(f"Connected to MongoDB: {MONGO_DB_NAME}")
        return db
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}", exc_info=True)
        return None

def query_events_by_month(db, year, month):
    """Query events for a specific month using ISO string dates."""
    # Create datetime objects for the start and end of the month
    start_date_dt = datetime(year, month, 1, 0, 0, 0)
    if month == 12:
        # For December, the end date is the start of the next year
        end_date_dt = datetime(year + 1, 1, 1, 0, 0, 0)
    else:
        # For other months, the end date is the start of the next month
        end_date_dt = datetime(year, month + 1, 1, 0, 0, 0)
    
    # Convert to ISO string format (YYYY-MM-DDTHH:MM:SS or just YYYY-MM-DD if time is not relevant for query)
    # Assuming datetime.start_date is stored as an ISO string that starts with YYYY-MM-DD
    start_date_iso = start_date_dt.isoformat()
    end_date_iso = end_date_dt.isoformat()

    # If only date part is needed for comparison (e.g., start_date is 'YYYY-MM-DD')
    # start_date_iso_short = start_date_dt.strftime('%Y-%m-%d')
    # end_date_iso_short = end_date_dt.strftime('%Y-%m-%d')

    query = {
        "datetime.start_date": {  # Updated field path
            "$gte": start_date_iso, # Using full ISO string for comparison
            "$lt": end_date_iso     # Using full ISO string for comparison
        }
        # If start_date is just YYYY-MM-DD, use start_date_iso_short and end_date_iso_short
    }
    logger.info(f"Querying events for {year}-{month:02d} with query: {query}")
    
    try:
        return list(db.events.find(query))
    except Exception as e:
        logger.error(f"Error during query_events_by_month: {e}", exc_info=True)
        return []

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