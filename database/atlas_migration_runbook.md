# MongoDB Atlas Migration Runbook

## 1. Pre-Migration Checklist
### Prerequisites
- [ ] Atlas cluster created with M10 tier or higher
- [ ] Network access configured (IP whitelisting/VPC peering)
- [ ] Database user credentials created
- [ ] Local MongoDB version 4.2+ (for `mongodump` compatibility)

### Environment Preparation
```bash
# Install updated MongoDB tools
sudo apt-get install -y mongodb-database-tools

# Create backup directory
mkdir -p database/backups
```

## 2. Migration Procedure

### Free Tier Limitations
⚠️ MongoDB Atlas Free Tier (M0) has important limitations:
- No `mongodump`/`mongorestore` support
- 512MB storage limit
- Max 10 connections
- Shared cluster resources

### Free Tier Migration Procedure

#### Step 1: Configure Environment Variables
Create `.env` file with Atlas credentials:
```ini
# .env
MONGODB_URI="mongodb+srv://<username>:<password>@cluster0.example.mongodb.net/tickets_ibiza_events?retryWrites=true&w=majority&tls=true"
MONGODB_LOCAL_URI="mongodb://localhost:27017/tickets_ibiza_events"
```

#### Step 2: Run Migration Script
```bash
python classy_skkkrapey/database/migrate_to_atlas.py
```

#### Step 3: Verify Migration
```python
# In Python shell
from pymongo import MongoClient
from config import settings

atlas_client = MongoClient(settings.MONGODB_URI)
print("Event count:", atlas_client.tickets_ibiza_events.events.count_documents({}))
```

### Standard Tier Migration
For paid tiers (M10+), use the original procedure:

#### Step 1: Create Local Backup
```bash
mongodump --uri=$MONGODB_LOCAL_URI --out=database/backups/pre-atlas-$(date +%Y%m%d)
```

#### Step 2: Data Migration
```bash
mongorestore --uri=$MONGODB_URI database/backups/pre-atlas-*/tickets_ibiza_events
```

## 3. Free Tier Optimization
### Storage Management
- Only migrate essential collections (`events` and `quality_scores`)
- Add data cleanup before migration:
```python
# In migrate_to_atlas.py before migration
six_months_ago = datetime.now() - timedelta(days=180)
local_db.events.delete_many({"dateTime.start": {"$lt": six_months_ago}})
```

### Connection Management
```python
# In mongodb_setup.py
client = MongoClient(
    settings.MONGODB_URI,
    maxPoolSize=10,  # Free tier connection limit
    # ... other params ...
)
```
### TLS/SSL Configuration
```python
# In mongodb_setup.py
client = MongoClient(
    settings.MONGODB_URI,
    tls=True,
    tlsAllowInvalidCertificates=False,
    tlsCAFile=certifi.where(),
    maxPoolSize=100,
    socketTimeoutMS=30000
)
```

## 4. Validation Tests
### Connection Test
```python
# test_setup.py
def test_atlas_connection():
    setup = MongoDBSetup(connection_string=os.getenv("MONGODB_URI"))
    assert setup.connect() is True
```

### Data Integrity Check
```python
# Compare document counts
local_count = local_db.events.count_documents({})
atlas_count = atlas_db.events.count_documents({})
assert local_count == atlas_count
```

## 5. Rollback Procedure
```bash
# Restore from local backup
mongorestore --uri=$MONGODB_LOCAL_URI database/backups/pre-atlas-*/tickets_ibiza_events

# Revert config changes
git checkout -- config.py
```

## 6. Post-Migration Optimization
### Atlas Configuration
- Enable auto-scaling
- Configure query profiling
- Set up cloud backups
- Enable performance advisor

### Monitoring Setup
```python
# Add performance monitoring
client = MongoClient(settings.MONGODB_URI, eventListeners=[CommandLogger()])
```

## Timeline Estimates
| Phase | Duration | Owner |
|-------|----------|-------|
| Preparation | 1 hour | DevOps |
| Data Migration | 30 mins | DBA |
| Validation | 45 mins | QA |
| Optimization | 2 hours | DevOps |

## Support Contacts
- Atlas Support: support@mongodb.com
- Emergency Rollback: ops-team@example.com