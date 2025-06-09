from mongodb_setup import MongoDBSetup
from datetime import datetime
import os

def main():
    # Initialize MongoDB setup
    setup = MongoDBSetup()
    if not setup.connect():
        print("Failed to connect to MongoDB")
        return

    try:
        db = setup.db

        # 1. Total number of events
        total_events = db.events.count_documents({})
        print(f"Total number of events: {total_events}")

        # 2. Number of distinct promoters
        # V2: Promoter is venue.stages.host.host_name
        # Note: This will count each host_name once, even if they appear at multiple venues/stages.
        # If a promoter can have multiple host_names, this might overcount distinct "promoter entities".
        # For now, this directly translates the old logic to the new path.
        distinct_promoters = len(db.events.distinct("venue.stages.host.host_name"))
        print(f"Number of distinct host names (promoters): {distinct_promoters}")

        # 3. Distribution of events by date
        # V2: Date field is datetime.start_date and is an ISOString
        # First, sample some date values for debugging
        sample_dates = list(db.events.aggregate([
            {"$match": {"datetime.start_date": {"$exists": True}}}, # Updated path
            {"$project": {"dateType": {"$type": "$datetime.start_date"}, "dateValue": "$datetime.start_date"}}, # Updated path
            {"$limit": 5}
        ]))
        print("Sample date values and types (from datetime.start_date):")
        for doc in sample_dates:
            print(f"Type: {doc['dateType']}, Value: {doc['dateValue']}")
        
        # Now run the date distribution pipeline
        pipeline = [
            # Filter out documents without datetime.start_date
            {"$match": {"datetime.start_date": {"$exists": True}}}, # Updated path
            
            # Convert ISO string dates to Date objects for grouping
            {"$addFields": {
                "convertedDate": {
                    # V2 stores start_date as string, so $toDate is appropriate
                    "$toDate": "$datetime.start_date" # Updated path
                }
            }},
            
            # Group by converted date
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$convertedDate"}},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"_id": 1}}
        ]
        date_distribution = list(db.events.aggregate(pipeline))
        date_distribution_str = ", ".join(
            [f"{item['_id']}: {item['count']}" for item in date_distribution]
        )
        print(f"Date distribution: {date_distribution_str}")

    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        setup.close()

if __name__ == "__main__":
    main()