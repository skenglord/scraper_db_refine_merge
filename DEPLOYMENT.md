# Deployment Guide: Ibiza Event Scraper & API

## Introduction

This document provides general guidance and recommendations for deploying the two main components of this project: the **API Server** and the **Web Scraper**. These guidelines are intended for users who wish to run these components in a development, staging, or production-like server environment.

## General Prerequisites

Before deploying, ensure the server environment meets these basic requirements:

*   **Python 3.9+:** The project is built with Python 3.9 or higher.
*   **MongoDB:** A MongoDB instance must be running and accessible from the server(s) where the API and potentially the scraper will run.
*   **Testing Dependencies:** For development and testing, install additional dependencies from `requirements-dev.txt` which includes tools like mongomock for mocking MongoDB in tests.
*   **Git:** For cloning the repository onto the server.
*   **Pip:** For installing Python packages.

## Deployment Considerations

*   **Configuration Management:**
    *   Sensitive information such as the MongoDB URI, API keys (if introduced in the future), or other credentials should **never** be hardcoded directly into the source code for production deployments.
    *   **Recommendation:** Use environment variables to manage configuration. The API server (FastAPI) can leverage Pydantic's `BaseSettings` for easy loading of environment variables. For the scraper, custom scripts or environment variables can be used.

*   **Python Virtual Environments:**
    *   It is highly recommended to use Python virtual environments (e.g., `venv`) to isolate project dependencies and avoid conflicts with system-wide Python packages.
    *   Activate the virtual environment before installing dependencies and running the applications.

## Deploying the API Server (`database/api_server.py`)

The API server provides access to the scraped event data.

1.  **Clone the Repository:**
    ```bash
    git clone <repository-url>
    cd <repository-directory>
    ```

2.  **Setup Virtual Environment and Install Dependencies:**
    ```bash
    python -m venv venv
    # On macOS/Linux
    source venv/bin/activate
    # On Windows
    # venv\Scripts\activate
    pip install -r requirements.txt
    # For development and testing, install additional dependencies:
    pip install -r requirements-dev.txt
    ```

3.  **MongoDB Connection:**
    *   Ensure the API server can connect to your MongoDB instance.
    *   Set the `MONGODB_URI` environment variable with your MongoDB connection string. For example:
        ```bash
        export MONGODB_URI="mongodb://user:password@host:port/database_name"
        ```
    *   The `api_server.py` script might need adjustments to read this environment variable if not already implemented (e.g., using Pydantic's `BaseSettings`).

4.  **Running with a Production ASGI Server:**
    *   The `uvicorn` server started directly via `python database/api_server.py` is suitable for development but **not recommended for production**.
    *   For production, use a production-ready ASGI server like Gunicorn managing Uvicorn workers.
    *   **Example using Gunicorn:**
        ```bash
        gunicorn -k uvicorn.workers.UvicornWorker database.api_server:app -w 4 -b 0.0.0.0:8000
        ```
        *   `-k uvicorn.workers.UvicornWorker`: Specifies the Uvicorn worker class.
        *   `database.api_server:app`: Points to the FastAPI application instance (`app`) in `database/api_server.py`.
        *   `-w 4`: Number of worker processes (adjust based on your server's CPU cores).
        *   `-b 0.0.0.0:8000`: Binds to all available network interfaces on port 8000.

5.  **Process Management:**
    *   To ensure the API server runs continuously and restarts automatically if it crashes, use a process manager like:
        *   `systemd` (common on modern Linux distributions)
        *   `supervisor`
    *   Configure a service file for `systemd` or a program definition for `supervisor` to manage the Gunicorn process.

6.  **CORS Configuration:**
    *   In `database/api_server.py`, the `CORSMiddleware` settings (specifically `allow_origins`) should be updated from `["*"]` to a list of specific domains that are allowed to access your API in a production environment.

## Development Testing Setup

When running tests locally or in CI/CD pipelines:

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Use mongomock to simulate MongoDB behavior in tests without requiring a running database instance.

3. Run tests with pytest:
```bash
pytest tests/
```

## Deploying/Running the Scraper (`my_scrapers/classy_skkkrapey.py`)

The scraper fetches data from websites and stores it in MongoDB.

1.  **Clone Repository & Setup (if not already done for API):**
    Follow steps 1 & 2 from the API server deployment if deploying on a separate machine or if not already done.

2.  **Install Playwright Browser Binaries:**
    Playwright requires browser binaries. Install them on the server:
    ```bash
    playwright install
    ```
    *   **Note:** If your server is a headless environment without a graphical interface, you might need to ensure all necessary system dependencies for browsers are installed. Sometimes, running `playwright install` might require a non-headless environment initially, or specific flags/environment variables for headless installation. Check Playwright documentation for CI/headless environments.

3.  **Scheduling Scrapes:**
    *   Web scrapers are typically run on a schedule (e.g., daily, hourly).
    *   **Linux/macOS:** Use `cron` jobs.
    *   **Windows:** Use Task Scheduler.

    *   **Conceptual `cron` Job Example:**
        To run the scraper daily at 2:00 AM and log output:
        ```cron
        0 2 * * * /path/to/your/project/venv/bin/python /path/to/your/project/my_scrapers/classy_skkkrapey.py https://www.ibiza-spotlight.com/nightlife/club_dates_i.htm crawl --headless True >> /path/to/your/project/logs/scraper.log 2>&1
        ```
        *   Replace `/path/to/your/project/venv/bin/python` and `/path/to/your/project/my_scrapers/classy_skkkrapey.py` with the absolute paths relevant to your server setup.
        *   Ensure the virtual environment's Python interpreter is used.
        *   `>> /path/to/your/project/logs/scraper.log 2>&1`: Appends both standard output and standard error to a log file. Create the `logs` directory if it doesn't exist.

4.  **Logging:**
    *   The scraper uses Python's `logging` module. For production, ensure this is configured to write to persistent log files. The `cron` example above shows a basic way to redirect output. You might enhance the scraper's logging configuration for more structured logging.

5.  **Headless Mode:**
    *   Always run Playwright in headless mode on a server to avoid issues with graphical interfaces. The scraper script should default to headless or allow it to be set via CLI argument (e.g., `--headless True` or ensure the default is `True`).

## Database (MongoDB)

*   **Security:** Ensure your MongoDB instance is secured for production:
    *   Enable authentication (username/password).
    *   Configure network access rules (firewall, bind IP) so that MongoDB is only accessible from trusted sources (e.g., your API server, scraper server).
*   **Performance:** Ensure appropriate indexes are created for common query patterns. Refer to `database/README.md` and `database/mongodb_setup.py` for schema details.
*   **Backups:** Implement a regular backup strategy for your MongoDB database.
*   Refer to the official MongoDB documentation for comprehensive guidance on production deployment and security.

## Further Steps / Production Hardening (High-Level)

Consider these additional steps for a robust production environment:

*   **HTTPS for the API:** Secure your API endpoints with HTTPS. Use a reverse proxy like Nginx or Caddy to handle SSL/TLS termination.
*   **Centralized Logging:** For larger deployments, consider sending logs from both the API server and scrapers to a centralized logging system (e.g., ELK Stack, Graylog, Papertrail).
*   **Monitoring and Alerting:**
    *   Set up monitoring for your API server (e.g., response times, error rates, resource usage) and scraper jobs (e.g., success/failure, duration).
    *   Implement alerting for critical issues (e.g., API down, scraper failures).
*   **Scalability:** If traffic or scraping load increases, you may need to consider:
    *   Scaling the API server (e.g., more Gunicorn workers, multiple server instances behind a load balancer).
    *   Distributing scraper tasks (e.g., using task queues like Celery).

This guide provides a starting point. Specific deployment details will vary based on your infrastructure and requirements.
