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
    return list(db.events.find({"dateTime": {"$lt": end_date}}).sort("dateTime", 1))

def get_events_by_promoter(promoter_name):
    return list(db.events.find({"promoter": promoter_name}))

def get_events_by_artist(artist_name):
    return list(db.events.find({"lineUp": artist_name}))

# NEW: Sitemap URL retrieval function
def get_sitemap_urls(quality_threshold=80):
    """Fetch URLs for sitemap generation meeting quality threshold"""
    return [doc['url'] for doc in db.events.find(
        {"_quality": {"$gte": quality_threshold}},
        {"url": 1, "_id": 0}
    )]

def get_total_events_count():
    """Return total number of events in database"""
    return db.events.count_documents({})

def get_distinct_promoters_count():
    """Return count of distinct promoters"""
    return len(set(db.events.distinct("promoter")))

def get_events_by_date_distribution():
    """Return event count distribution by date"""
    pipeline = [
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$dateTime"}},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]
    return list(db.events.aggregate(pipeline))