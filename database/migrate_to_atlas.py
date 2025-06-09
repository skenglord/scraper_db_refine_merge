import time
from pymongo import MongoClient
import sys
from pathlib import Path

# Add parent directory to path to import config
sys.path.append(str(Path(__file__).parent.parent))
from config import settings
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_collection(local_collection, atlas_collection, batch_size=50):
    """Migrate collection data in batches with error handling"""
    total_docs = local_collection.count_documents({})
    logger.info(f"Migrating {total_docs} documents from {local_collection.name}")
    
    migrated = 0
    for i in range(0, total_docs, batch_size):
        try:
            docs = list(local_collection.find().skip(i).limit(batch_size))
            if not docs:
                break
                
            result = atlas_collection.insert_many(docs, ordered=False)
            migrated += len(result.inserted_ids)
            logger.info(f"Migrated batch: {i}-{i+len(docs)} ({migrated}/{total_docs})")
            
        except Exception as e:
            logger.error(f"Batch {i} failed: {str(e)}")
            # Try individual document insertion
            for doc in docs:
                try:
                    atlas_collection.insert_one(doc)
                    migrated += 1
                except:
                    logger.warning(f"Failed to migrate document: {doc.get('_id')}")
        
        time.sleep(1)  # Be nice to free tier
    
    return migrated

def main():
    """Main migration function"""
    # Get connection URIs with fallbacks
    local_uri = getattr(settings, "MONGODB_LOCAL_URI", "mongodb://localhost:27017/")
    atlas_uri = settings.MONGODB_URI
    
    # Connect to local database
    local_client = MongoClient(local_uri)
    
    # Configure TLS/SSL for Atlas connections
    tls_options = {}
    if "mongodb+srv" in atlas_uri:
        tls_options = {
            'tls': True,
            'tlsCAFile': certifi.where(),
            'tlsAllowInvalidCertificates': False
        }
    
    # Connect to Atlas
    atlas_client = MongoClient(
        atlas_uri,
        maxPoolSize=10,  # Free tier connection limit
        **tls_options
    )
    local_db = local_client.tickets_ibiza_events
    atlas_db = atlas_client.tickets_ibiza_events
    
    # Collections to migrate
    collections = {
        "events": {"batch_size": 30},  # Smaller batches for large documents
        "quality_scores": {"batch_size": 50}
    }
    
    # Migrate each collection
    for name, config in collections.items():
        logger.info(f"\n{'='*40}")
        logger.info(f"Starting migration: {name}")
        migrated = migrate_collection(
            local_db[name],
            atlas_db[name],
            batch_size=config["batch_size"]
        )
        logger.info(f"Finished {name}: {migrated} documents migrated")

if __name__ == "__main__":
    main()