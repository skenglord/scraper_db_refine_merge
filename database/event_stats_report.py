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
        distinct_promoters = len(set(db.events.distinct("promoter")))
        print(f"Number of distinct promoters: {distinct_promoters}")

        # 3. Distribution of events by date
        # First, sample some date values for debugging
        sample_dates = list(db.events.aggregate([
            {"$match": {"dateTime.start": {"$exists": True}}},
            {"$project": {"dateType": {"$type": "$dateTime.start"}, "dateValue": "$dateTime.start"}},
            {"$limit": 5}
        ]))
        print("Sample date values and types:")
        for doc in sample_dates:
            print(f"Type: {doc['dateType']}, Value: {doc['dateValue']}")
        
        # Now run the date distribution pipeline
        pipeline = [
            # Filter out documents without dateTime.start
            {"$match": {"dateTime.start": {"$exists": True}}},
            
            # Convert string dates to Date objects
            {"$addFields": {
                "convertedDate": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$dateTime.start"}, "string"]},
                        "then": {"$dateFromString": {"dateString": "$dateTime.start"}},
                        "else": "$dateTime.start"
                    }
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