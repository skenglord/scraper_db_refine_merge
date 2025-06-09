from mongodb_setup import get_db
from mongo_queries import get_total_events_count, get_distinct_promoters_count, get_events_by_date_distribution
import sys

def main():
    # Get database connection
    db = get_db()
    
    # Get statistics
    total_events = get_total_events_count(db)
    distinct_promoters = get_distinct_promoters_count(db)
    date_distribution = get_events_by_date_distribution(db)
    
    # Format results
    date_distribution_str = ", ".join(
        [f"{item['_id']}: {item['count']}" for item in date_distribution]
    )
    
    # Print results in required format
    print(f"Total number of events: {total_events}")
    print(f"Number of distinct promoters: {distinct_promoters}")
    print(f"Date distribution: {date_distribution_str}")

if __name__ == "__main__":
    main()