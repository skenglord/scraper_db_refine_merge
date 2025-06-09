"""
Example queries for the MongoDB event database with quality scoring
"""

from pymongo import MongoClient
from datetime import datetime, timedelta
import json


def get_high_quality_events(min_score=0.8):
    """Get events with quality score above threshold"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    events = list(db.events.find(
        {"data_quality.overall_score": {"$gte": min_score}}, # Updated path
        {
            "title": 1,
            "datetime.start_date": 1, # Updated path
            "venue.name": 1,          # Updated path
            "data_quality.overall_score": 1, # Updated path
            "ticketing.is_free": 1, # Example: check if free
            "ticketing.tiers": 1    # Example: get tiers to check prices/status
        }
    ).sort("datetime.start_date", 1).limit(10)) # Updated sort path
    
    print(f"\n🌟 HIGH QUALITY EVENTS (Score >= {min_score}):")
    print("=" * 60)
    for event in events:
        # Using pattern_description for a more human-readable date if available
        date_display = event.get('datetime', {}).get('recurring', {}).get('pattern_description') or \
                       event.get('datetime', {}).get('start_date', 'N/A Date')
        print(f"📅 {date_display}")
        print(f"   📍 {event.get('venue', {}).get('name', 'Unknown venue')}")
        print(f"   🎵 {event.get('title', 'N/A Title')}") # Added fallback for title
        print(f"   ⭐ Quality Score: {event.get('data_quality', {}).get('overall_score', 0.0):.3f}") # Updated path

        # Determine ticket status (example logic)
        ticket_status = "Unknown"
        ticketing_info = event.get('ticketing', {})
        if ticketing_info.get('is_free'):
            ticket_status = "Free"
        elif ticketing_info.get('tiers'):
            # Check if any tier is explicitly not sold out
            available_tiers = [t for t in ticketing_info['tiers'] if not t.get('is_sold_out')]
            if available_tiers:
                ticket_status = f"Available (from {min(t.get('tier_price', 0) for t in available_tiers if t.get('tier_price') is not None) if any(t.get('tier_price') is not None for t in available_tiers) else 'N/A'} {available_tiers[0].get('currency', '')})"
            else:
                ticket_status = "Sold Out / Check URL"
        elif ticketing_info.get('tickets_url'):
            ticket_status = "Check URL"

        print(f"   🎟️  Status: {ticket_status}")
        print()
    
    client.close()
    return events


def get_events_by_venue(venue_name):
    """Get all events for a specific venue"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Case-insensitive search
    events = list(db.events.find(
        {"venue.name": {"$regex": venue_name, "$options": "i"}}, # Updated path
        {
            "title": 1,
            "datetime.start_date": 1, # Updated path
            "venue.name": 1,
            "data_quality.overall_score": 1, # Updated path
            "acts": 1 # Fetch top-level acts
        }
    ).sort("datetime.start_date", 1).limit(20)) # Updated sort path
    
    print(f"\n🏛️  EVENTS AT {venue_name.upper()}:")
    print("=" * 60)
    for event in events:
        date_display = event.get('datetime', {}).get('start_date', 'N/A Date')
        print(f"📅 {date_display}")
        print(f"   🎵 {event.get('title', 'N/A Title')}")
        print(f"   ⭐ Quality: {event.get('data_quality', {}).get('overall_score', 0.0):.3f}")
        
        # Show artist names from the main 'acts' array
        acts_list = event.get('acts', [])
        artist_names = [act.get('act_name', 'Unknown Artist') for act in acts_list if isinstance(act, dict)]
        if artist_names:
            print(f"   🎤 Artists: {', '.join(artist_names[:3])}{'...' if len(artist_names) > 3 else ''}") # Show first 3
        print()
    
    client.close()
    return events


def get_events_with_issues():
    """Get events that have quality issues"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Find events with validation flags
    pipeline = [
        {
            "$match": {
                "data_quality.overall_score": {"$lt": 0.7}, # Example: low overall score
                "data_quality.validation_flags": {"$exists": True, "$ne": []} # Has validation flags
            }
        },
        {
            "$project": {
                "title": 1,
                "scraping_metadata.source_url": 1, # Updated path
                "data_quality.overall_score": 1,    # Updated path
                "validation_flags": "$data_quality.validation_flags" # Direct access
            }
        },
        {"$limit": 10}
    ]
    
    events = list(db.events.aggregate(pipeline))
    
    print("\n⚠️  EVENTS WITH QUALITY ISSUES:")
    print("=" * 60)
    for event in events:
        print(f"🎵 {event.get('title', 'N/A Title')}")
        source_url = event.get('scraping_metadata', {}).get('source_url', 'N/A URL')
        print(f"   🔗 {source_url}")
        print(f"   ⭐ Quality Score: {event.get('data_quality', {}).get('overall_score', 0.0):.3f}")
        
        flags = event.get('validation_flags', [])
        if flags:
            # Flags are now dicts like {"field": "name", "issue": "missing_title"}
            issues_summary = [f"{f.get('field')}: {f.get('issue')}" for f in flags if isinstance(f, dict)]
            print(f"   🚩 Issues: {', '.join(issues_summary)}")
        print()
    
    client.close()
    return events


def get_quality_statistics():
    """Get overall quality statistics"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Aggregate statistics
    pipeline = [
        {
            "$group": {
                "_id": None,
                "totalEvents": {"$sum": 1},
                "avgOverallScore": {"$avg": "$data_quality.overall_score"}, # Updated path
                "minOverallScore": {"$min": "$data_quality.overall_score"}, # Updated path
                "maxOverallScore": {"$max": "$data_quality.overall_score"}, # Updated path
                "avgTitleScore": {"$avg": "$data_quality.field_quality_scores.title"}, # Updated path
                "avgVenueScore": {"$avg": "$data_quality.field_quality_scores.venue"}, # Updated path (key "venue")
                "avgDateTimeScore": {"$avg": "$data_quality.field_quality_scores.datetime"},# Updated path (key "datetime")
                "avgActsScore": {"$avg": "$data_quality.field_quality_scores.acts"},    # Updated path (key "acts")
                "avgTicketingScore": {"$avg": "$data_quality.field_quality_scores.ticketing"} # Updated path (key "ticketing")
            }
        }
    ]
    
    stats_list = list(db.events.aggregate(pipeline))
    if not stats_list:
        print("\n📊 No data for QUALITY STATISTICS.")
        client.close()
        return {} # Return empty if no stats

    stats = stats_list[0]
    
    print("\n📊 QUALITY STATISTICS:")
    print("=" * 60)
    print(f"Total Events: {stats.get('totalEvents', 0)}")
    print(f"\nOverall Quality Score:")
    print(f"  Average: {stats.get('avgOverallScore', 0.0):.3f}")
    print(f"  Minimum: {stats.get('minOverallScore', 0.0):.3f}")
    print(f"  Maximum: {stats.get('maxOverallScore', 0.0):.3f}")
    print(f"\nAverage Field Scores (based on V2 keys):")
    print(f"  Title:      {stats.get('avgTitleScore', 0.0):.3f}")
    print(f"  Venue:      {stats.get('avgVenueScore', 0.0):.3f}")
    print(f"  DateTime:   {stats.get('avgDateTimeScore', 0.0):.3f}")
    print(f"  Acts:       {stats.get('avgActsScore', 0.0):.3f}")
    print(f"  Ticketing:  {stats.get('avgTicketingScore', 0.0):.3f}")
    
    # Get quality distribution
    distribution_pipeline = [
        {
            "$bucket": {
                "groupBy": "$data_quality.overall_score", # Updated path
                "boundaries": [0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01], # Adjusted upper bound for 1.0
                "default": "Other", # Scores outside 0-1 range, or nulls
                "output": {
                    "count": {"$sum": 1}
                    # "$push": "$title" might make results too large, removed for summary
                }
            }
        }
    ]
    
    distribution = list(db.events.aggregate(distribution_pipeline))
    
    print(f"\nQuality Distribution (Overall Score):")
    # Boundaries: [0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
    # Labels should correspond to these buckets.
    # Bucket _id values will be the lower bound of each bucket e.g. 0, 0.5, ... 0.9
    dist_map = {b["_id"]: b["count"] for b in distribution if b.get("_id") != "Other"}

    labels_bounds = [
        ("Very Poor (<0.5)", 0),
        ("Poor (0.5-0.6)", 0.5),
        ("Fair (0.6-0.7)", 0.6),
        ("Good (0.7-0.8)", 0.7),
        ("Very Good (0.8-0.9)", 0.8),
        ("Excellent (0.9-1.0)", 0.9)
    ]
    for label, bound in labels_bounds:
        print(f"  {label}: {dist_map.get(bound, 0)} events")
    if any(b["_id"] == "Other" for b in distribution):
        other_count = next((b["count"] for b in distribution if b["_id"] == "Other"), 0)
        print(f"  Other (e.g. null scores): {other_count} events")
    
    client.close()
    return stats


def search_events(search_term):
    """Search events by text"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Text search (assuming text index is updated for V2 fields like title, content.full_description, venue.name, acts.act_name)
    events = list(db.events.find(
        {"$text": {"$search": search_term}},
        {
            "title": 1,
            "content.full_description": 1, # Updated path
            "datetime.start_date": 1,    # Updated path
            "venue.name": 1,             # Updated path
            "acts.act_name": 1,          # Example: show artist names
            "data_quality.overall_score": 1, # Updated path
            "score": {"$meta": "textScore"}
        }
    ).sort([("score", {"$meta": "textScore"})]).limit(10))
    
    print(f"\n🔍 SEARCH RESULTS FOR '{search_term}':")
    print("=" * 60)
    for event in events:
        print(f"🎵 {event.get('title', 'N/A Title')}")
        print(f"   📍 {event.get('venue', {}).get('name', 'Unknown venue')}")
        date_display = event.get('datetime', {}).get('start_date', 'N/A Date')
        print(f"   📅 {date_display}")
        print(f"   ⭐ Quality: {event.get('data_quality', {}).get('overall_score', 0.0):.3f}")
        
        desc = event.get('content', {}).get('full_description', '') # Updated path
        if desc:
            snippet = desc[:150] + "..." if len(desc) > 150 else desc
            print(f"   📝 Description Snippet: {snippet}")

        artists = [act.get('act_name') for act in event.get('acts', []) if isinstance(act, dict) and act.get('act_name')]
        if artists:
            print(f"   🎤 Artists: {', '.join(artists[:3])}{'...' if len(artists) > 3 else ''}")
        print()
    
    client.close()
    return events


def export_for_app(min_quality=0.7):
    """Export high-quality events in a format suitable for your app"""
    client = MongoClient()
    db = client.tickets_ibiza_events
    
    # Get events with good quality for app consumption
    now_iso_string = datetime.utcnow().isoformat()
    events = list(db.events.find(
        {
            "data_quality.overall_score": {"$gte": min_quality}, # Updated path
            "datetime.start_date": {"$gte": now_iso_string}  # Compare ISO strings for future events
        },
        { # Projection: Exclude large or internal fields
            "_id": 0,
            "scraping_metadata.raw_data": 0, # Exclude bulky raw_data
            "data_quality": 0, # Exclude detailed quality scores if only overall was used for filter
            "deduplication": 0 # Exclude deduplication info
            # Keep other fields like title, datetime, venue, acts, ticketing, content, music etc.
        }
    ).sort("datetime.start_date", 1).limit(100)) # Updated sort path
    
    # Save to JSON file
    export_filename = "app_export_high_quality_events.json"
    with open(export_filename, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2, default=str) # default=str for any remaining datetime objects (should be strings now)
    
    print(f"\n✅ Exported {len(events)} high-quality events to '{export_filename}'")
    print(f"   Minimum quality score: {min_quality}")
    print(f"   Future events only: Yes")
    
    client.close()
    return events


def main():
    """Run example queries"""
    print("🎉 MongoDB Event Database Query Examples")
    print("=" * 80)
    
    # 1. Quality statistics
    get_quality_statistics()
    
    # 2. High quality events
    get_high_quality_events(0.8)
    
    # 3. Events by venue
    get_events_by_venue("Hï Ibiza")
    
    # 4. Events with issues
    get_events_with_issues()
    
    # 5. Search events
    search_events("techno")
    
    # 6. Export for app
    export_for_app(0.75)
    
    print("\n✅ All queries completed successfully!")


if __name__ == "__main__":
    main()