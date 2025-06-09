"""
FastAPI server for accessing MongoDB event data with quality filtering
"""
from config import settings
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import motor.motor_asyncio # Replaced pymongo
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
import uvicorn

# Initialize FastAPI app
app = FastAPI(
    title="Tickets Ibiza Event API",
    description="API for accessing quality-filtered event data",
    version="1.0.0"
)

# Add CORS middleware for your app
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure this for your app's domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# MongoDB connection
# client = MongoClient("mongodb://localhost:27017/") # Old
client = motor.motor_asyncio.AsyncIOMotorClient(settings.MONGODB_URI) # New
db = client.tickets_ibiza_events # Ensure this uses settings.DB_NAME if that's how other scripts do it.
# For now, assuming 'tickets_ibiza_events' is the correct DB name from settings or direct use.
# If settings.DB_NAME is preferred, this line should be: db = client[settings.DB_NAME]


# --- Pydantic Models aligned with unifiedEventsSchema_v2 ---

class Coordinates(BaseModel):
    type: str = Field("Point", Literal="Point")
    coordinates: List[float] # [longitude, latitude]

class Address(BaseModel):
    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    postal_code: Optional[str] = None
    full_address: Optional[str] = None

class SetTime(BaseModel):
    start: Optional[str] = None # ISO datetime string
    end: Optional[str] = None   # ISO datetime string
    duration_minutes: Optional[int] = None

class VenueStageAct(BaseModel):
    act_id: str
    set_time: Optional[SetTime] = None
    billing_order: Optional[int] = None
    is_headliner: Optional[bool] = False

class StageHost(BaseModel):
    host_name: Optional[str] = None
    host_id: Optional[str] = None

class VenueStage(BaseModel):
    stage_id: str
    stage_name: str = "Main Stage"
    capacity: Optional[int] = None
    stage_type: Optional[str] = None
    host: Optional[StageHost] = None
    stage_genres: Optional[List[str]] = None
    acts: Optional[List[VenueStageAct]] = None

class Venue(BaseModel): # Was Location
    venue_id: str # Changed to non-optional based on schema
    name: str
    address: Optional[Address] = None
    coordinates: Optional[Coordinates] = None
    venue_type: Optional[str] = None
    total_capacity: Optional[int] = None
    has_disabled_access: Optional[bool] = None
    website: Optional[str] = None
    social_links: Optional[Dict[str, str]] = None
    stage_count: Optional[int] = 1
    stages: Optional[List[VenueStage]] = None

class SocialMedia(BaseModel): # Example, expand as needed
    # Using generic dict for flexibility as per schema (object type)
    # For specific known platforms:
    # spotify_url: Optional[str] = None
    # soundcloud_url: Optional[str] = None
    # instagram_url: Optional[str] = None
    # facebook_url: Optional[str] = None
    # general: Optional[Dict[str, str]] = None # Catch-all for other platforms
    pass # Representing as a generic object, will be Dict[str, str] in Event

class PopularityMetrics(BaseModel): # Example, expand as needed
    # spotify_followers: Optional[int] = None
    # soundcloud_plays: Optional[int] = None
    pass # Representing as a generic object, will be Dict[str, Any] in Event

class Act(BaseModel): # Was Artist
    act_id: str
    act_name: str
    act_type: Optional[str] = None
    genres: Optional[List[str]] = None
    styles: Optional[List[str]] = None
    social_media: Optional[Dict[str, str]] = None # Changed to simple Dict
    popularity_metrics: Optional[Dict[str, Any]] = None # Changed to simple Dict

class TicketTier(BaseModel):
    tier_id: Optional[str] = None
    tier_name: Optional[str] = None # Made optional as schema doesn't mandate it
    tier_price: Optional[float] = None # Made optional
    currency: Optional[str] = "EUR"
    sale_start: Optional[str] = None # ISO datetime string
    sale_end: Optional[str] = None   # ISO datetime string
    is_sold_out: Optional[bool] = False
    is_nearly_sold_out: Optional[bool] = False

class AgeRestriction(BaseModel):
    minimum_age: Optional[int] = None
    restriction_type: Optional[str] = None

class Ticketing(BaseModel): # Was TicketInfo
    tickets_url: Optional[str] = None
    is_free: Optional[bool] = False
    age_restriction: Optional[AgeRestriction] = None
    promos: Optional[List[Dict[str, Any]]] = None # Changed from List[str]
    tiers: Optional[List[TicketTier]] = None
    external_platforms: Optional[List[Dict[str, Any]]] = None


class RecurringInfo(BaseModel):
    is_recurring: bool = False
    frequency: Optional[str] = None
    pattern_description: Optional[str] = None
    end_recurrence: Optional[str] = None # ISO datetime string

class DateTimeInfo(BaseModel):
    start_date: str # ISO datetime string
    end_date: Optional[str] = None # ISO datetime string
    timezone: str
    doors_open: Optional[str] = None # ISO datetime string
    last_entry: Optional[str] = None # ISO datetime string
    is_all_day: Optional[bool] = False
    duration_hours: Optional[float] = None
    recurring: Optional[RecurringInfo] = None

class FieldQualityScores(BaseModel): # Reflects schema's "object" type for field_quality_scores
    # Actual field names here depend on what QualityScorer produces.
    # These are placeholders based on common patterns.
    title: Optional[float] = Field(None, ge=0, le=1) # Assuming 0-1 scale from QualityScorer
    venue: Optional[float] = Field(None, ge=0, le=1)
    datetime: Optional[float] = Field(None, ge=0, le=1)
    acts: Optional[float] = Field(None, ge=0, le=1)
    ticketing: Optional[float] = Field(None, ge=0, le=1)
    # Add other specific field scores if present in data_quality.field_quality_scores

class ValidationFlag(BaseModel):
    field: str
    issue: str
    # severity: Optional[str] = None # Severity not in V2 example, make optional or remove

class ManualVerification(BaseModel):
    is_verified: bool = False
    verified_by: Optional[str] = None
    verified_at: Optional[str] = None # Changed from verified_date to match schema

class DataQuality(BaseModel): # Was QualityScore
    overall_score: Optional[float] = Field(None, ge=0, le=1) # Schema implies 0-1 for overall_score
    field_quality_scores: Optional[FieldQualityScores] = None # Or Dict[str, float] if more generic
    validation_flags: Optional[List[ValidationFlag]] = None
    manual_verification: Optional[ManualVerification] = None

class ScrapingMetadata(BaseModel):
    source_platform: str
    source_url: str
    source_event_id: Optional[str] = None
    first_scraped: str # ISO datetime string
    last_scraped: str  # ISO datetime string
    scraper_version: Optional[str] = None
    # raw_data is excluded as it's too large for API responses

class ContentInfo(BaseModel):
    short_description: Optional[str] = None
    full_description: Optional[str] = None
    keywords: Optional[List[str]] = None
    hashtags: Optional[List[str]] = None

class MusicInfo(BaseModel):
    primary_genre: Optional[str] = None
    sub_genres: Optional[List[str]] = None
    styles: Optional[List[str]] = None
    mood_tags: Optional[List[str]] = None
    energy_level: Optional[int] = Field(None, ge=1, le=10)
    genre_confidence: Optional[float] = Field(None, ge=0, le=1)

class Event(BaseModel):
    # id: str = Field(..., alias="_id") # Using event_id as primary identifier now
    event_id: str
    canonical_id: Optional[str] = None
    title: str
    type: str
    status: Optional[str] = None
    datetime: DateTimeInfo
    venue: Venue
    acts: Optional[List[Act]] = None
    content: Optional[ContentInfo] = None
    music: Optional[MusicInfo] = None
    ticketing: Optional[Ticketing] = None
    scraping_metadata: ScrapingMetadata
    data_quality: Optional[DataQuality] = None
    # Other top-level V2 fields like deduplication, knowledge_graph, analytics, system_flags
    # can be added if they are intended to be part of the API response.
    # For now, focusing on the core event details.

    class Config:
        populate_by_name = True
        # alias_generator = to_camel # If converting snake_case from DB to camelCase for API

class EventSummary(BaseModel):
    event_id: str # Changed from id
    title: str
    # Using direct field names from Event model for summary, access via dot notation in projection
    venue_name: Optional[str] = None
    start_date: Optional[str] = None
    overall_score: Optional[float] = None
    # status: Optional[str] = None # This was ticketInfo.status, now more complex to summarize

    class Config:
        populate_by_name = True # Allows population using field names directly

class QualityStats(BaseModel): # To be updated based on V2 data_quality structure
    totalEvents: int
    averageQuality: Optional[float] = None # From data_quality.overall_score
    distribution: Dict[str, int] # e.g. "excellent": count, "good": count
    topVenues: List[Dict[str, Any]] # Based on venue.name and avg data_quality.overall_score


@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "Tickets Ibiza Event API",
        "database": "connected" if client else "disconnected"
    }


@app.get("/api/events", response_model=List[EventSummary], tags=["Events"])
async def get_events(
    min_quality: float = Query(0.7, ge=0, le=1, description="Minimum overall quality score (0.0-1.0)"),
    venue: Optional[str] = Query(None, description="Filter by venue name (regex, case-insensitive)"),
    future_only: bool = Query(True, description="Only show future events based on datetime.start_date"),
    limit: int = Query(50, ge=1, le=200, description="Number of results"),
    skip: int = Query(0, ge=0, description="Skip results for pagination")
):
    """
    Get events filtered by quality and other criteria, aligned with unifiedEventsSchema_v2.
    """
    query: Dict[str, Any] = {"data_quality.overall_score": {"$gte": min_quality}}
    
    if venue:
        query["venue.name"] = {"$regex": venue, "$options": "i"}
    
    if future_only:
        query["datetime.start_date"] = {"$gte": datetime.utcnow().isoformat()}
    
    projection = {
        "event_id": 1,
        "title": 1,
        "venue.name": 1,
        "datetime.start_date": 1,
        "data_quality.overall_score": 1,
        "_id": 0 # Exclude MongoDB default _id
    }
    
    cursor = db.events.find(query, projection).sort("datetime.start_date", 1).skip(skip).limit(limit)
    events_from_db = await cursor.to_list(length=limit)

    # Adapt data for EventSummary model if needed (Pydantic will also try to map using aliases)
    summaries = []
    for event_db in events_from_db:
        summary_data = {
            "event_id": event_db.get("event_id"),
            "title": event_db.get("title"),
            "venue_name": event_db.get("venue", {}).get("name"), # Handles nested field
            "start_date": event_db.get("datetime", {}).get("start_date"), # Handles nested field
            "overall_score": event_db.get("data_quality", {}).get("overall_score") # Handles nested field
        }
        summaries.append(EventSummary(**summary_data))

    return summaries


@app.get("/api/events/{event_id}", response_model=Event, tags=["Events"])
async def get_event(event_id: str): # event_id is now the string ID from V2 schema
    """
    Get detailed information for a specific event using its event_id.
    """
    event = await db.events.find_one({"event_id": event_id}) # Query by event_id
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    
    # Pydantic model 'Event' will validate and map the data.
    # MongoDB's _id is not part of the V2 Event model if not explicitly added with alias.
    # If _id needs to be returned, the Event model should include it with an alias.
    # For now, assuming event_id is the primary identifier for API interaction.
    return event


@app.get("/api/events/search/{search_term}", response_model=List[EventSummary], tags=["Events"])
async def search_events(
    search_term: str,
    min_quality: float = Query(0.6, ge=0, le=1), # Quality from data_quality.overall_score
    limit: int = Query(20, ge=1, le=50)
):
    """
    Search events by text (assuming text index on V2 fields) with quality filtering.
    """
    query = {
        "$text": {"$search": search_term},
        "data_quality.overall_score": {"$gte": min_quality}
    }
    projection = {
        "event_id": 1,
        "title": 1,
        "venue.name": 1,
        "datetime.start_date": 1,
        "data_quality.overall_score": 1,
        "score": {"$meta": "textScore"}, # For sorting by relevance
        "_id": 0
    }
    
    cursor = db.events.find(query, projection).sort([("score", {"$meta": "textScore"})]).limit(limit)
    events_from_db = await cursor.to_list(length=limit)

    summaries = []
    for event_db in events_from_db:
        summary_data = {
            "event_id": event_db.get("event_id"),
            "title": event_db.get("title"),
            "venue_name": event_db.get("venue", {}).get("name"),
            "start_date": event_db.get("datetime", {}).get("start_date"),
            "overall_score": event_db.get("data_quality", {}).get("overall_score")
        }
        summaries.append(EventSummary(**summary_data))
    return summaries


@app.get("/api/venues", tags=["Venues"])
async def get_venues():
    """
    Get list of all venues with event counts, using V2 schema fields.
    """
    pipeline = [
        {"$match": {"venue.name": {"$ne": None, "$exists": True}}}, # Ensure venue name exists
        {"$group": {
            "_id": "$venue.name", # Group by venue.name
            "eventCount": {"$sum": 1},
            "avgQuality": {"$avg": "$data_quality.overall_score"}, # Use data_quality.overall_score
            "upcomingEvents": {
                "$sum": {
                    "$cond": [ # Compare datetime.start_date (string) with current ISO string
                        {"$gte": ["$datetime.start_date", datetime.utcnow().isoformat()]},
                        1, 0
                    ]
                }
            }
        }},
        {"$sort": {"eventCount": -1}},
        {"$project": {
            "venueName": "$_id", # Rename _id to venueName for clarity in response
            "eventCount": 1,
            "avgQuality": {"$round": ["$avgQuality", 3]},
            "upcomingEvents": 1,
            "_id": 0
        }}
    ]
    
    cursor = db.events.aggregate(pipeline)
    venues = await cursor.to_list(length=None)
    return venues


@app.get("/api/venues/{venue_name}/events", response_model=List[EventSummary], tags=["Venues"])
async def get_venue_events(
    venue_name: str,
    future_only: bool = Query(True),
    limit: int = Query(50, ge=1, le=200)
):
    """
    Get all events for a specific venue, using V2 schema fields.
    """
    query: Dict[str, Any] = {"venue.name": {"$regex": venue_name, "$options": "i"}} # Query venue.name
    
    if future_only:
        query["datetime.start_date"] = {"$gte": datetime.utcnow().isoformat()} # Compare ISO strings
    
    projection = {
        "event_id": 1,
        "title": 1,
        "venue.name": 1,
        "datetime.start_date": 1,
        "data_quality.overall_score": 1,
        "_id": 0
    }
    
    cursor = db.events.find(query, projection).sort("datetime.start_date", 1).limit(limit)
    events_from_db = await cursor.to_list(length=limit)

    summaries = []
    for event_db in events_from_db:
        summary_data = {
            "event_id": event_db.get("event_id"),
            "title": event_db.get("title"),
            "venue_name": event_db.get("venue", {}).get("name"),
            "start_date": event_db.get("datetime", {}).get("start_date"),
            "overall_score": event_db.get("data_quality", {}).get("overall_score")
        }
        summaries.append(EventSummary(**summary_data))
    return summaries


@app.get("/api/stats/quality", response_model=QualityStats, tags=["Statistics"])
async def get_quality_stats():
    """
    Get overall quality statistics based on data_quality.overall_score.
    """
    pipeline_stats = [
        {
            "$group": {
                "_id": None,
                "totalEvents": {"$sum": 1},
                "avgQuality": {"$avg": "$data_quality.overall_score"}, # Use data_quality.overall_score
                "excellent": {"$sum": {"$cond": [{"$gte": ["$data_quality.overall_score", 0.9]}, 1, 0]}},
                "good": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$data_quality.overall_score", 0.8]},
                        {"$lt": ["$data_quality.overall_score", 0.9]}
                    ]}, 1, 0
                ]}},
                "fair": {"$sum": {"$cond": [
                    {"$and": [
                        {"$gte": ["$data_quality.overall_score", 0.7]},
                        {"$lt": ["$data_quality.overall_score", 0.8]}
                    ]}, 1, 0
                ]}},
                "poor": {"$sum": {"$cond": [{"$lt": ["$data_quality.overall_score", 0.7]}, 1, 0]}}
            }
        }
    ]
    
    stats_cursor = db.events.aggregate(pipeline_stats)
    stats_result_list = await stats_cursor.to_list(length=1)
    
    if not stats_result_list:
        # Return default/empty stats if no events are found or no quality scores yet
        return QualityStats(totalEvents=0, averageQuality=0.0, distribution={}, topVenues=[])
    
    stats = stats_result_list[0]
    
    venue_pipeline = [
        {"$match": {"venue.name": {"$ne": None, "$exists": True}}},
        {"$group": {
            "_id": "$venue.name", # Group by venue.name
            "avgQuality": {"$avg": "$data_quality.overall_score"}, # Use data_quality.overall_score
            "eventCount": {"$sum": 1}
        }},
        {"$sort": {"avgQuality": -1, "eventCount": -1}}, # Sort by quality then count
        {"$limit": 10},
        {"$project": {
            "venueName": "$_id", # Map _id to venueName for response model
            "averageQuality": {"$round": ["$avgQuality", 3]},
            "eventCount": 1,
            "_id": 0
        }}
    ]
    
    top_venues_cursor = db.events.aggregate(venue_pipeline)
    top_venues_list = await top_venues_cursor.to_list(length=10)
    
    return QualityStats( # Constructing the response model directly
        totalEvents=stats.get("totalEvents", 0),
        averageQuality=round(stats.get("avgQuality", 0.0), 3) if stats.get("avgQuality") is not None else 0.0,
        distribution={
            "excellent": stats.get("excellent", 0),
            "good": stats.get("good", 0),
            "fair": stats.get("fair", 0),
            "poor": stats.get("poor", 0)
        },
        topVenues=top_venues_list # Already in correct format from projection
    )


@app.get("/api/upcoming", response_model=List[EventSummary], tags=["Events"])
async def get_upcoming_events(
    days: int = Query(7, ge=1, le=30, description="Number of days ahead"),
    min_quality: float = Query(0.75, ge=0, le=1), # data_quality.overall_score
    limit: int = Query(20, ge=1, le=100)
):
    """
    Get upcoming events within specified days, using V2 fields.
    """
    start_date_iso = datetime.utcnow().isoformat()
    end_date_iso = (datetime.utcnow() + timedelta(days=days)).isoformat()
    
    query = {
        "datetime.start_date": { # Query datetime.start_date (ISO string)
            "$gte": start_date_iso,
            "$lte": end_date_iso
        },
        "data_quality.overall_score": {"$gte": min_quality} # Query data_quality.overall_score
    }
    projection = {
        "event_id": 1,
        "title": 1,
        "venue.name": 1,
        "datetime.start_date": 1,
        "data_quality.overall_score": 1,
        "_id": 0
    }
    
    cursor = db.events.find(query, projection).sort("datetime.start_date", 1).limit(limit)
    events_from_db = await cursor.to_list(length=limit)

    summaries = []
    for event_db in events_from_db:
        summary_data = {
            "event_id": event_db.get("event_id"),
            "title": event_db.get("title"),
            "venue_name": event_db.get("venue", {}).get("name"),
            "start_date": event_db.get("datetime", {}).get("start_date"),
            "overall_score": event_db.get("data_quality", {}).get("overall_score")
        }
        summaries.append(EventSummary(**summary_data))
    return summaries


@app.post("/api/events/{event_id}/refresh", tags=["Events"])
async def refresh_event(event_id: str): # event_id is now the string ID
    """
    Mark an event for re-scraping (operational endpoint, fields might be outside V2).
    """
    # This endpoint modifies operational flags not strictly part of V2 content schema.
    # Assuming 'needsRefresh' and 'refreshRequestedAt' are still desired operational fields.
    # If they need to be part of V2, schema_adapter and helpers/schemas.py would need them.
    
    result = await db.events.update_one(
        {"event_id": event_id}, # Query by event_id
        {
            "$set": {
                "system_flags.needs_refresh": True, # Example: store under system_flags
                "system_flags.refresh_requested_at": datetime.utcnow().isoformat()
            }
        }
    )

    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Event not found with given event_id")

    return {"message": "Event marked for refresh", "event_id": event_id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    # Run the server
    print("Starting Tickets Ibiza Event API...")
    print("API Documentation: http://localhost:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)