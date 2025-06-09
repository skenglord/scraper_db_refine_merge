# MongoDB Atlas Setup Guide

## 1. Create Atlas Cluster
1. Go to [MongoDB Atlas](https://www.mongodb.com/cloud/atlas)
2. Create new project: `Tickets Ibiza Events`
3. Build a cluster:
   - Select M0 (Free Tier)
   - Provider: AWS
   - Region: Choose closest to you
   - Cluster Name: `tickets-cluster`
4. Click "Create Cluster"

## 2. Configure Network Access
1. In Security → Network Access
2. Add IP Address:
   - For development: Add your current IP
   - For production: Add 0.0.0.0/0 (allow all)
3. Click "Confirm"

## 3. Create Database User
1. In Security → Database Access
2. Add New User:
   - Authentication: Password
   - Username: `tickets_admin`
   - Password: [Generate strong password]
   - Privileges: Atlas admin
3. Click "Add User"

## 4. Get Connection String
1. In Atlas Dashboard, click "Connect"
2. Select "Connect your application"
3. Driver: Python
4. Version: 3.6 or later
5. Copy connection string:
```ini
mongodb+srv://tickets_admin:<password>@tickets-cluster.example.mongodb.net/
```

## 5. Configure .env File
Create `.env` file in project root:
```ini
# .env
MONGODB_URI="mongodb+srv://tickets_admin:your_password@tickets-cluster.example.mongodb.net/tickets_ibiza_events?retryWrites=true&w=majority"
MONGODB_LOCAL_URI="mongodb://localhost:27017/tickets_ibiza_events"
```

## 6. Run Migration
```bash
# Install dependencies
pip install -r classy_skkkrapey/database/requirements.txt

# Execute migration
python classy_skkkrapey/database/migrate_to_atlas.py
```

## 7. Verify Migration
1. In Atlas UI, go to Collections
2. Verify `events` and `quality_scores` collections
3. Check document counts match local database

## 8. Update Application
Modify your application to use the Atlas URI:
```python
from config import settings

# Use Atlas connection
client = MongoClient(settings.MONGODB_URI)
```

## Troubleshooting
- Connection issues: Verify network access and credentials
- Migration errors: Check `migrate_to_atlas.py` logs
- Performance: Upgrade to paid tier for production