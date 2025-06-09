import pymongo
from pymongo import MongoClient
import os

# Load environment variables directly
MONGODB_URI = os.getenv("MONGODB_URI")
DB_NAME = os.getenv("DB_NAME")

if not MONGODB_URI or not DB_NAME:
    raise ValueError("MongoDB connection details not found in environment variables")

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]

def get_events(limit=10):
    return list(db.events.find().limit(limit))

def get_venue_events(venue_name):
    return list(db.events.find({"venue.name": venue_name}))

def get_upcoming_events(days=7):
    from datetime import datetime, timedelta
    end_date = datetime.now() + timedelta(days=days)
    # Query against 'datetime.start_date' which is an ISO string
    # Sort by 'datetime.start_date'
    return list(db.events.find({"datetime.start_date": {"$lt": end_date.isoformat()}}).sort("datetime.start_date", 1))

def get_events_by_promoter(promoter_name):
    # Query against 'venue.stages.host.host_name'
    return list(db.events.find({"venue.stages.host.host_name": promoter_name}))

def get_events_by_artist(artist_name):
    # Query against 'acts.act_name'
    return list(db.events.find({"acts.act_name": artist_name}))

# NEW: Sitemap URL retrieval function
def get_sitemap_urls(quality_threshold=0.8): # Assuming quality_threshold is now 0.0 to 1.0
    """Fetch URLs for sitemap generation meeting quality threshold"""
    # Query against 'data_quality.overall_score'
    # Project 'scraping_metadata.source_url'
    return [doc['scraping_metadata']['source_url'] for doc in db.events.find(
        {"data_quality.overall_score": {"$gte": quality_threshold}},
        {"scraping_metadata.source_url": 1, "_id": 0}
    ) if doc.get('scraping_metadata') and doc['scraping_metadata'].get('source_url')]

def get_total_events_count():
    """Return total number of events in database"""
    return db.events.count_documents({})

def get_distinct_promoters_count():
    """Return count of distinct promoters"""
    # Get distinct values from 'venue.stages.host.host_name'
    return len(db.events.distinct("venue.stages.host.host_name"))

def get_events_by_date_distribution():
    """Return event count distribution by date"""
    from datetime import datetime # Ensure datetime is imported for $toDate conversion if needed by context
    pipeline = [
        {"$group": {
            # Group by 'datetime.start_date' after converting it from ISO string to date
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$toDate": "$datetime.start_date"}}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db.events.aggregate(pipeline))