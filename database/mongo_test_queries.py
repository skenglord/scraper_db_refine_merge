"""
MongoDB Query Test Script
"""

from mongo_queries import MongoQueries
import sys

def run_tests():
    queries = MongoQueries()
    
    print("=== MongoDB Query Tests ===")
    
    # 1. Test source URL = ibizaspotlight.com
    print("\n1. Events from ibizaspotlight.com:")
    ibiza_events = queries.find_events_by_source("ibizaspotlight.com")
    print(f"Found {len(ibiza_events)} events")
    for i, event in enumerate(ibiza_events[:3], 1):
        print(f"  {i}. {event.get('title', 'No title')} - {event.get('url', 'No URL')}")
    
    # 2. Test venue = unvrs
    print("\n2. Events at UNVRS venue:")
    unvrs_events = queries.find_events_by_venue("UNVRS")
    print(f"Found {len(unvrs_events)} events")
    for i, event in enumerate(unvrs_events[:3], 1):
        print(f"  {i}. {event.get('title', 'No title')} - {event.get('date', 'No date')}")
    
    # 3-5. Test text searches
    search_terms = ["pacha", "carl cox", "eric prydz"]
    for term in search_terms:
        print(f"\n3-5. Search results for '{term}':")
        results = queries.search_events_by_text(term)
        print(f"Found {len(results)} events")
        for i, event in enumerate(results[:3], 1):
            print(f"  {i}. {event.get('title', 'No title')} - Score: {event.get('_quality', {}).get('overall', 'N/A')}")
    
    # 6. Quality statistics
    print("\n6. Quality statistics:")
    stats = queries.get_quality_statistics()
    print(f"Average score: {stats.get('avg_score', 0):.2f}")
    print(f"High quality events (>0.8): {stats.get('high_quality', 0)}")
    print(f"Low quality events (<0.5): {stats.get('low_quality', 0)}")
    
    # 7. Validation flag: poor quality (score < 0.5)
    print("\n7. Events with poor quality (score < 0.5):")
    poor_quality = list(queries.events.find({"_quality.overall": {"$lt": 0.5}}))
    print(f"Found {len(poor_quality)} events")
    for i, event in enumerate(poor_quality[:5], 1):
        title = event.get('title', 'No title')
        score = event.get('_quality', {}).get('overall', 'N/A')
        print(f"  {i}. {title} - Score: {score}")

if __name__ == "__main__":
    try:
        run_tests()
    except Exception as e:
        print(f"Error during tests: {e}")
        sys.exit(1)