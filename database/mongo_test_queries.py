"""
MongoDB Query Test Script
"""

from mongo_queries import MongoQueries
import sys

def run_tests():
    queries = MongoQueries()
    
    print("=== MongoDB Query Tests ===")
    
    # 1. Test source URL = ibizaspotlight.com
    # Assuming queries.find_events_by_source now queries scraping_metadata.source_platform
    print("\n1. Events from 'ibiza-spotlight' platform:") # Example platform name
    ibiza_events = queries.find_events_by_source("ibiza-spotlight") # Adjust platform name as needed
    print(f"Found {len(ibiza_events)} events")
    for i, event in enumerate(ibiza_events[:3], 1):
        print(f"  {i}. {event.get('title', 'No title')} - {event.get('scraping_metadata', {}).get('source_url', 'No URL')}")
    
    # 2. Test venue = unvrs
    # Assuming queries.find_events_by_venue now queries venue.name
    print("\n2. Events at UNVRS venue:")
    unvrs_events = queries.find_events_by_venue("UNVRS") # Venue name might need case adjustment depending on data
    print(f"Found {len(unvrs_events)} events")
    for i, event in enumerate(unvrs_events[:3], 1):
        print(f"  {i}. {event.get('title', 'No title')} - {event.get('datetime', {}).get('start_date', 'No date')}")
    
    # 3-5. Test text searches
    # Assuming queries.search_events_by_text uses a text index updated for V2 fields
    search_terms = ["pacha", "carl cox", "eric prydz"]
    for term in search_terms:
        print(f"\n3-5. Search results for '{term}':")
        results = queries.search_events_by_text(term)
        print(f"Found {len(results)} events")
        for i, event in enumerate(results[:3], 1):
            print(f"  {i}. {event.get('title', 'No title')} - Score: {event.get('data_quality', {}).get('overall_score', 'N/A')}")
    
    # 6. Quality statistics
    # Assuming queries.get_quality_statistics is updated for data_quality.overall_score
    print("\n6. Quality statistics:")
    stats = queries.get_quality_statistics()
    print(f"Average score: {stats.get('avg_overall_score', 0):.2f}") # Key might change in stats result
    print(f"High quality events (>0.8): {stats.get('high_quality_count', 0)}") # Key might change
    print(f"Low quality events (<0.5): {stats.get('low_quality_count', 0)}") # Key might change
    
    # 7. Validation flag: poor quality (score < 0.5)
    print("\n7. Events with poor quality (score < 0.5):")
    # Direct query updated to use data_quality.overall_score
    poor_quality = list(queries.events.find({"data_quality.overall_score": {"$lt": 0.5}}))
    print(f"Found {len(poor_quality)} events")
    for i, event in enumerate(poor_quality[:5], 1):
        title = event.get('title', 'No title')
        score = event.get('data_quality', {}).get('overall_score', 'N/A')
        print(f"  {i}. {title} - Score: {score}")

if __name__ == "__main__":
    try:
        run_tests()
    except Exception as e:
        print(f"Error during tests: {e}")
        sys.exit(1)