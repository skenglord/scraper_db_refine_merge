# MongoDB Connection Setup Guide (for Atlas)

## 1. Introduction

This guide explains how developers should set up their local environment to connect the `scrapers_v2` application to a MongoDB Atlas cluster. It involves creating a `.env` file from the provided example and populating it with your specific Atlas connection details.

The application uses a centralized configuration system (`scrapers_v2/config.py`) that loads database connection details (and other settings) from environment variables, which can be conveniently managed using a `.env` file for local development.

## 2. Creating Your `.env` File

1.  **Locate the Example File**: In the `scrapers_v2/` directory, you will find a file named `.env.example`. This file contains templates for all environment variables the application can use.
2.  **Copy the Example**: Create a copy of `.env.example` in the *same* `scrapers_v2/` directory and name it `.env`.
    ```bash
    # In your terminal, navigate to the scrapers_v2 directory
    cp .env.example .env
    ```
3.  **IMPORTANT**: The `.env` file contains sensitive credentials. It is already listed in the project's `.gitignore` file and **must not be committed to version control (Git)**.

## 3. Finding Your MongoDB Atlas Connection String (SRV URI)

To connect to your MongoDB Atlas cluster, you need an SRV connection string.

1.  **Log in to MongoDB Atlas**: Go to [cloud.mongodb.com](https://cloud.mongodb.com/) and log in.
2.  **Navigate to Your Cluster**: Select the organization and project, then click on your cluster name.
3.  **Click the "Connect" Button**: This will open the connection dialog.
4.  **Choose "Connect your application"** (or similar wording, often "Drivers").
5.  **Select Driver and Version**: Choose "Python" as the driver and select a recent version (e.g., "3.6 or later").
6.  **Copy the SRV Connection String**: Atlas will display an SRV connection string. It will look something like this:
    `mongodb+srv://<username>:<password>@yourclustername.xxxx.mongodb.net/?retryWrites=true&w=majority`
    Copy this entire string.

    *Note: Ensure the database user (`<username>`) you intend to use has the necessary permissions for the database(s) your application will access.*
    *Also, ensure your current IP address is whitelisted in Atlas under "Network Access". You might need to add your IP or allow access from anywhere (0.0.0.0/0 - less secure, use with caution and only if necessary for dynamic IPs during development).*

## 4. Populating `.env` Variables for MongoDB

Open your newly created `scrapers_v2/.env` file in a text editor and fill in the MongoDB Atlas details:

1.  **`MONGODB_URI` (or `MONGO_URI`)**:
    *   Paste the SRV connection string you copied from Atlas.
    *   **Crucially, replace `<password>` in the string with the actual password for the database user specified in `<username>`**.
    *   You can also optionally specify a default database name directly in the URI by adding it after the `.net/` part and before the `?`, for example:
        `mongodb+srv://myuser:mypassword@mycluster.xxxx.mongodb.net/myDefaultDbName?retryWrites=true&w=majority`
        If you do this, this database will be used unless overridden by `MONGODB_DATABASE`.

    Example:
    ```env
    MONGODB_URI="mongodb+srv://myDbUser:myActualSecurePassword@mycluster.abcdef.mongodb.net/events_dev_db?retryWrites=true&w=majority"
    ```
    (The `.env.example` shows both `MONGODB_URI` and `MONGO_URI` as options because `scrapers_v2/config.py` uses `AliasChoices`. You only need to set one of them.)

2.  **`MONGODB_DATABASE` (or `MONGO_DATABASE`)**:
    *   Specify the name of the database you want the application to use primarily.
    *   If you already specified a database in the `MONGODB_URI` connection string, this variable can override it or confirm it. If no database is in the URI, this variable *must* be set for the application to target a specific database.
    *   Example:
        ```env
        MONGODB_DATABASE="scrapers_v2_data"
        ```

3.  **`MONGODB_DEFAULT_UNIFIED_COLLECTION` (or `MONGO_DEFAULT_UNIFIED_COLLECTION`)**:
    *   Specify the default name for the collection where unified event data will be stored.
    *   Example:
        ```env
        MONGODB_DEFAULT_UNIFIED_COLLECTION="unified_events"
        ```

**Example completed section in `.env`**:
```env
# ... other settings ...

# --- MongoDB Atlas Connection Settings ---
MONGODB_URI="mongodb+srv://scraper_user:Th1sIsMyS3cr3tP4sswOrd@mycluster.xxxx.mongodb.net/dev_event_db?retryWrites=true&w=majority"
MONGODB_DATABASE="dev_event_db" # Can be same as in URI or a different one if URI has no specific DB
MONGODB_DEFAULT_UNIFIED_COLLECTION="unified_events_alpha"

# ... other settings ...
```

**Remember**: Save the `.env` file. **Do not commit it to Git.**

## 5. Confirmation of `scrapers_v2/config.py` Setup

The `scrapers_v2/config.py` file is already configured to load these MongoDB-related environment variables. It uses the `MongoDBSettings` Pydantic model with an `env_prefix='MONGODB_'` and `AliasChoices` (e.g., `MONGO_URI` also works).

When the application starts, the `settings` object instantiated from `config.py` will automatically read the values from your `.env` file (if present) and then from actual environment variables (which can override `.env` values). This makes your MongoDB connection details available to any part of the application that imports and uses the `settings` object (e.g., `settings.mongodb.uri`).
