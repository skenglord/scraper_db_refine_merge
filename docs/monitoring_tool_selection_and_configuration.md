# Monitoring Tool Selection and Initial Configuration

This document outlines a proposed stack of monitoring and alerting tools for the web scraping pipeline. It also details initial configuration steps for these tools, considering their integration with Prefect and the Python-based components.

## 1. Introduction

A robust monitoring and alerting system is essential for maintaining the health, performance, and reliability of the scraping pipeline. This setup will provide insights into scraper execution, data processing, orchestration status, and system-level performance, enabling proactive issue resolution.

## 2. Proposed Monitoring Stack

The recommended stack combines the strengths of several open-source and SaaS tools:

*   **Prefect UI (Orion/Cloud)**: For orchestrator-level monitoring, flow/task status, logs, and basic notifications. This is the first point of call for workflow health.
*   **Prometheus**: For collecting and storing time-series metrics from all components of the pipeline (scrapers, ETL, agents, potentially Prefect itself).
*   **Grafana**: For creating dashboards to visualize metrics collected by Prometheus and for setting up metric-based alerts.
*   **Sentry**: For application-level error tracking and exception reporting, providing detailed context for debugging code issues.

**Rationale for this stack:**

*   **Comprehensive Coverage**: Addresses metrics, logging (via Prefect and potentially Sentry for errors), error tracking, visualization, and alerting.
*   **Widely Adopted & Open Source Focus**: Prometheus, Grafana, and Sentry (core) are popular, well-documented, and have strong communities.
*   **Python-Friendly**: These tools have good support and client libraries for Python applications.
*   **Integration Capabilities**: They can work together effectively (e.g., Grafana using Prometheus as a data source, Sentry linking errors to specific flow runs via tags).

## 3. Configuration and Integration Steps

### A. Prefect (Orchestration Monitoring & Basic Alerting)

*   **Monitoring**:
    *   The **Prefect UI** (local Orion server or Prefect Cloud) is the primary interface for monitoring flow and task states (running, completed, failed, retrying), viewing logs per task run, and inspecting flow run history and upcoming schedules.
    *   Regularly check the dashboard for failed runs or tasks stuck in a 'Running' state for too long.
*   **Alerting (Notifications)**:
    *   **Source**: Prefect UI (Cloud or self-hosted server).
    *   **Configuration**:
        1.  Navigate to the "Notifications" or "Automations" section in the Prefect UI.
        2.  Create new notifications based on flow run state changes (e.g., "Failed", "Crashed").
        3.  Configure notification channels: Email, Slack, Microsoft Teams, PagerDuty, Webhooks, etc.
        4.  Apply notifications to specific flows (using tags) or all flows.
    *   **Use**: Provides immediate alerts for orchestration failures, which are often the first sign of a problem.

### B. Prometheus (Metrics Collection)

*   **Role**: Collects time-series metrics from various sources.
*   **Setup**:
    1.  **Installation**: Deploy Prometheus (e.g., via Docker, Kubernetes, or native binary).
        ```bash
        # Example using Docker
        docker run -d --name prometheus -p 9090:9090 \
          -v /path/to/your/prometheus.yml:/etc/prometheus/prometheus.yml \
          prom/prometheus
        ```
    2.  **Python Client Library**: In your Python scrapers and ETL scripts, use the `prometheus_client` library.
        ```bash
        pip install prometheus_client
        ```
*   **Configuration (`prometheus.yml`)**:
    ```yaml
    global:
      scrape_interval: 15s # How frequently to scrape targets.

    scrape_configs:
      - job_name: 'prefect_metrics' # If Prefect exposes a Prometheus endpoint
        # static_configs:
        #   - targets: ['prefect-orion-host:prefect-metrics-port'] # Replace with actual endpoint if available

      - job_name: 'scraper_custom_metrics'
        # Example: If scrapers expose metrics on an HTTP endpoint
        # This requires scrapers to run an HTTP server (e.g., Flask, FastAPI, or prometheus_client's start_http_server)
        # Consider a push-based approach using Prometheus Pushgateway for short-lived scraper jobs if exposing an endpoint is hard.
        static_configs:
          - targets: ['scraper_host_1:8000', 'scraper_host_2:8000'] # Replace with actual scraper metric endpoints

      - job_name: 'node_exporter' # For system metrics of agent machines
        static_configs:
          - targets: ['agent_host_1:9100', 'agent_host_2:9100'] # Assuming Node Exporter is running on agents
    ```
*   **Exposing Metrics from Python Scripts**:
    *   Use `prometheus_client` to define Gauges, Counters, Histograms for metrics listed in `pipeline_metrics_definition.md`.
    *   For long-running services (like an API or a continuously running scraper manager), use `start_http_server(port)` from `prometheus_client`.
    *   For batch jobs/scripts (typical for individual scraper runs orchestrated by Prefect), metrics might need to be pushed to Prometheus Pushgateway, or the Prefect task itself could expose metrics if it's long-lived enough for a scrape. Prefect 2.x is moving away from direct Prometheus metrics exposure from Orion; custom metrics from tasks are preferred.
    *   **Example (Conceptual - in a scraper task)**:
        ```python
        from prefect import task
        from prometheus_client import Counter, Gauge, start_http_server, CollectorRegistry
        import time

        # In a real setup, manage the registry and server more carefully if tasks run in separate processes/threads.
        # For tasks run by Prefect agents, exposing an HTTP endpoint per task might be complex.
        # A common pattern is for the application/agent itself to expose metrics,
        # or use Pushgateway for ephemeral tasks.

        # Simplified example for a persistent service or for Pushgateway:
        ITEMS_SCRAPED = Counter('items_scraped_total', 'Total number of items scraped', ['scraper_name'])
        PAGE_LOAD_TIME = Gauge('page_load_time_seconds', 'Page load time in seconds', ['scraper_name'])

        # @task
        # def my_scraper_task(scraper_name, target_url):
        #     # ... scraping logic ...
        #     ITEMS_SCRAPED.labels(scraper_name=scraper_name).inc()
        #     PAGE_LOAD_TIME.labels(scraper_name=scraper_name).set(load_time_seconds)
        #     # If using Pushgateway, push metrics here.
        ```
        Given Prefect's model, a more common approach for task-specific metrics is to log them or send them to a metrics system via custom code within the task, rather than each task exposing a Prometheus endpoint. Prefect itself can be a source for operational metrics (flow states, durations).

### C. Grafana (Dashboards and Visualization)

*   **Role**: Visualize metrics collected by Prometheus; can also display logs and set up alerts.
*   **Setup**:
    1.  **Installation**: Deploy Grafana (e.g., via Docker).
        ```bash
        docker run -d --name grafana -p 3000:3000 grafana/grafana
        ```
    2.  **Access Grafana**: Open `http://localhost:3000` (default admin/admin).
    3.  **Add Prometheus Data Source**:
        *   Go to Configuration (gear icon) > Data Sources > Add data source.
        *   Select Prometheus.
        *   Set the HTTP URL to your Prometheus server (e.g., `http://prometheus:9090` if Docker networked, or `http://<host_ip>:9090` if Prometheus is on host).
        *   Save & Test.
*   **Initial Dashboards**:
    1.  **Prefect Overview Dashboard**:
        *   If Prefect Orion/Cloud exposes Prometheus metrics directly or via an exporter, use those.
        *   Alternatively, use the Prefect API to pull data and feed it to Prometheus (custom exporter) or directly to Grafana via a JSON API data source.
        *   Metrics: Flow run counts (by status), Task run counts (by status), Agent status.
    2.  **Scraper Performance Dashboard**:
        *   Data Source: Prometheus (scraping custom metrics from your Python scripts/services).
        *   Panels:
            *   Success/Failure rates per scraper.
            *   Items extracted per scraper (time series).
            *   Average page load times per scraper.
            *   HTTP error counts.
            *   CAPTCHA detection rates.
    3.  **ETL Performance Dashboard**:
        *   Data Source: Prometheus.
        *   Panels: Records processed, loaded, errors; ETL duration.
    4.  **Agent System Metrics Dashboard**:
        *   Data Source: Prometheus (scraping Node Exporter from agent hosts).
        *   Panels: CPU usage, memory usage, disk I/O, network traffic for agent machines.
*   **Alerting**: Grafana has its own alerting engine. Define alert rules on dashboard panels for critical thresholds (e.g., high scraper failure rate, long ETL duration, low disk space on agent).

### D. Sentry (Error Tracking and Exception Reporting)

*   **Role**: Captures and aggregates Python exceptions, providing detailed context for debugging.
*   **Setup**:
    1.  **Sentry Account**: Sign up at [sentry.io](https://sentry.io) (offers a free tier) or deploy a self-hosted instance.
    2.  **Project Setup**: Create a new project in Sentry for your Python application. Get the DSN.
    3.  **Python SDK Integration**:
        ```bash
        pip install sentry-sdk
        ```
        In your Python scripts (scrapers, ETL, and potentially within Prefect task definitions):
        ```python
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
        from sentry_sdk.integrations.prefect import PrefectIntegration # If available and suitable

        # It's good practice to get DSN from environment variables/config
        # from config import settings
        # SENTRY_DSN = settings.sentry_dsn

        SENTRY_DSN = "YOUR_SENTRY_DSN_HERE" # Replace with actual DSN

        if SENTRY_DSN != "YOUR_SENTRY_DSN_HERE": # Check if DSN is configured
            sentry_logging = LoggingIntegration(
                level=logging.INFO,        # Capture info and above as breadcrumbs
                event_level=logging.ERROR  # Send errors as Sentry events
            )
            sentry_sdk.init(
                dsn=SENTRY_DSN,
                integrations=[sentry_logging, PrefectIntegration()], # PrefectIntegration helps link Sentry errors to Prefect flow/task runs
                traces_sample_rate=0.2, # Capture 20% of transactions for performance monitoring (optional)
                environment="development" # Set dynamically (dev, staging, prod) via config.settings.environment
            )
        ```
*   **Usage**:
    *   Sentry will automatically capture unhandled exceptions.
    *   You can manually capture exceptions: `sentry_sdk.capture_exception(e)`
    *   Add context (tags, extra data): `sentry_sdk.set_tag("scraper_name", "ventura_crawler")`, `sentry_sdk.set_extra("url_being_scraped", url)`
    *   Errors in Sentry UI will provide stack traces, request data (if web app), and custom tags/extra.

## 4. How They Work Together

1.  **Prefect** orchestrates the execution of scraper and ETL flows. Its UI provides the first layer of monitoring for flow status and logs. For critical flow failures, Prefect sends notifications.
2.  **Python Scrapers/ETL Tasks**, when run by Prefect agents:
    *   Send unhandled exceptions and specific errors to **Sentry** for detailed debugging.
    *   (Optionally) Expose custom metrics via `prometheus_client` (e.g., items scraped, page load times) or push to Prometheus Pushgateway.
3.  **Prometheus** scrapes metrics from:
    *   The custom metrics endpoints/Pushgateway from scrapers/ETL.
    *   Node Exporters on agent machines for system health.
    *   (Potentially) A Prefect metrics endpoint or custom exporter for deeper Prefect operational metrics.
4.  **Grafana** connects to Prometheus as a data source to:
    *   Display dashboards visualizing all these metrics.
    *   Define alert rules based on metric thresholds, sending notifications via various channels (complementing Prefect's flow-level alerts).
5.  **Sentry** alerts can also be configured for new or frequent error types.

## 5. Next Steps

*   **Implement PoC**: Start by instrumenting one key scraper with Sentry and basic `prometheus_client` metrics.
*   **Set up Local Stack**: Run Prometheus and Grafana locally using Docker to build initial dashboards.
*   **Refine Metrics**: Based on initial data, refine which metrics are most valuable.
*   **Build Out Dashboards**: Develop comprehensive dashboards in Grafana.
*   **Configure Alerts**: Set up alert rules in Grafana and/or Prometheus Alertmanager for critical conditions.
*   **Integrate Sentry with Prefect**: Ensure the `PrefectIntegration` for Sentry is working to link Sentry issues back to Prefect flow/task runs.

This layered approach provides both high-level orchestration monitoring via Prefect and deep dives into application errors (Sentry) and performance metrics (Prometheus/Grafana).
