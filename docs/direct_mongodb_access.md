# Direct MongoDB Atlas Access Guide

## 1. Introduction

This guide provides instructions for developers on how to connect directly to the shared MongoDB Atlas database instance used by the `scrapers_v2` project. Direct access is useful for data verification, debugging, ad-hoc querying, and understanding the database schema.

This guide assumes that the MongoDB Atlas cluster is already set up and you have followed the instructions in `docs/mongodb_connection_setup.md` to configure your environment variables (primarily by creating a `.env` file).

## 2. Prerequisites

Before you can connect, ensure the following:

1.  **MongoDB Atlas Cluster Access**: You have the SRV connection string for the Atlas cluster.
2.  **Database User Credentials**: You have a username and password for a database user that has appropriate permissions (e.g., read-only or read-write for specific collections/databases).
3.  **IP Whitelisting**: Your current public IP address must be whitelisted in MongoDB Atlas under "Network Access". If you have a dynamic IP, you may need to update this periodically or use a VPN with a static IP that is whitelisted.
4.  **MongoDB Tools**:
    *   For GUI access: [MongoDB Compass](https://www.mongodb.com/try/download/compass) (download and install).
    *   For CLI access: [`mongosh`](https://www.mongodb.com/try/download/shell) (download and install, or it might be included with recent MongoDB Server installations).

## 3. Connecting with MongoDB Compass (GUI)

MongoDB Compass provides a graphical interface for interacting with your database.

1.  **Open MongoDB Compass**.
2.  On the initial connection screen, you have two main options:
    *   **Paste your connection string (Recommended for SRV)**:
        *   Click "Connect" and then "Fill in connection fields individually" if it doesn't immediately show the URI input, or look for an input field labeled "URI" or "Paste your connection string".
        *   Get your full SRV connection string (e.g., `mongodb+srv://<username>:<password>@yourcluster.xxxx.mongodb.net/`).
        *   **Important**: Replace `<username>` and `<password>` placeholders in the string with your actual database user credentials *before* pasting.
        *   Paste the complete, modified SRV string into the URI field in Compass. Compass should parse it and populate other fields automatically when using an SRV string.
    *   **Fill in connection fields individually** (Less common for SRV, but possible):
        *   Hostname: Extract from your SRV string (e.g., `yourcluster.xxxx.mongodb.net`).
        *   Port: Usually `27017` (default for MongoDB).
        *   Authentication: Select "Username / Password".
        *   Username: Your database username.
        *   Password: Your database password.
        *   Replica Set Name: Often auto-detected with SRV or found in Atlas connection options.
        *   Read Preference, SSL, etc.: Usually defaults are fine with SRV. Ensure SSL is set to "Server Validation" or similar (default for Atlas SRV).
        *   More Options Tab: You might need to specify `authSource=admin` or the specific database where the user is defined if it's not `admin`. The SRV string usually handles this.
3.  **Click "Connect"**.
4.  If the connection is successful, you will see a list of databases on the left panel. You can click on a database to see its collections, and then click on a collection to view, query, and manage its documents.

## 4. Connecting with `mongosh` (CLI)

`mongosh` is the modern command-line interface for MongoDB.

1.  **Open your terminal or command prompt**.
2.  **Construct the Connection Command**:
    You will use your SRV connection string directly with the `mongosh` command.
    ```bash
    mongosh "mongodb+srv://<username>:<password>@yourcluster.xxxx.mongodb.net/<database_name>?retryWrites=true&w=majority"
    ```
    *   **Replace Placeholders**:
        *   `<username>`: Your database username.
        *   `<password>`: Your database user's password.
        *   `yourcluster.xxxx.mongodb.net`: The hostname part of your Atlas SRV string.
        *   `<database_name>`: Optional. You can specify the database you want to connect to directly. If omitted, you'll connect to a default database (often `test` or `admin`), and you can switch later using `use <database_name>`.
    *   **Quoting**: It's crucial to enclose the entire connection string in double quotes (`"`) to prevent your shell from misinterpreting special characters (like `&` or `?`).

3.  **Execute the command**. If successful, you will be connected to your Atlas cluster, and the prompt will change to indicate the current database context (e.g., `your_database_name>`).

**Basic `mongosh` Orientation Commands**:

*   `show dbs` or `show databases`: Lists all databases you have access to.
*   `use <database_name>`: Switches to the specified database context. For example, `use my_scraper_data_v2`.
*   `show collections`: Lists all collections in the current database.
*   `db.getCollectionNames()`: Another way to list collections.
*   `db.<collection_name>.find().limit(5)`: Retrieves up to 5 documents from the specified collection. For example, `db.unified_events.find().limit(5)`. If your collection name has special characters (like hyphens), use `db.getCollection("collection-name").find()`.
*   `db.<collection_name>.find({ "event_details.title": /Music Festival/i }).pretty()`: Example of a query with a filter (case-insensitive regex search for "Music Festival" in the title) and pretty printing.
*   `db.<collection_name>.countDocuments()`: Shows the total number of documents in the collection.
*   `db.<collection_name>.stats()`: Shows statistics for the collection.
*   `help`: Shows help for `mongosh` commands.
*   `exit` or `quit()`: Disconnects from the database and exits `mongosh`.

## 5. Target Database and Collection Names

As configured in `scrapers_v2/config.py` (and potentially overridden by your `.env` file), the typical targets are:

*   **Database Name**: Loaded from `settings.mongodb.database` (e.g., environment variable `MONGODB_DATABASE` or `MONGO_DATABASE`, defaulting to `scraper_data_v2`).
*   **Default Unified Events Collection**: Loaded from `settings.mongodb.default_unified_collection` (e.g., environment variable `MONGODB_DEFAULT_UNIFIED_COLLECTION` or `MONGO_DEFAULT_UNIFIED_COLLECTION`, defaulting to `unified_events`).

When using `mongosh` or Compass, make sure you are interacting with the correct database and collection as defined in your project's configuration.

## 6. Security Reminders

*   **Credential Management**:
    *   **Never** commit your actual `.env` file containing passwords or sensitive connection strings to version control (Git).
    *   Use strong, unique passwords for your database users.
    *   Avoid sharing credentials insecurely.
*   **IP Whitelisting**: Regularly review the IP access list in MongoDB Atlas. Remove IPs that are no longer needed. Prefer specific IPs or ranges over `0.0.0.0/0` (allow all) whenever possible, especially for production environments.
*   **Principle of Least Privilege**: Ensure database users have only the permissions necessary for their tasks (e.g., read-only access if they only need to view data).
