# Basic Alerting Setup for Critical Pipeline Failures

This document outlines the initial setup for basic alerting on critical failures within the web scraping pipeline, using the selected monitoring tools: Prefect, Sentry, and Grafana (with Prometheus).

## 1. Introduction

Proactive alerting is essential for quickly identifying and addressing issues that can impact data collection, processing, and overall pipeline reliability. This plan focuses on setting up alerts for critical failures that require attention.

## 2. Alerting with Prefect

Prefect is the first line of defense for issues related to workflow execution itself.

*   **Source**: Prefect UI (Cloud or self-hosted Server) - Notifications / Automations feature.

*   **Alert Trigger Conditions**:
    1.  **Flow Run Failure/Crash**:
        *   **Condition**: A flow run enters a `Failed` or `Crashed` state.
        *   **Importance**: Critical. Indicates a significant problem with a scraper or ETL process.
    2.  **Critical Task Failure**:
        *   **Condition**: A specific, critical task within a flow enters a `Failed` or `Crashed` state (can be configured per task or for all tasks in a flow).
        *   **Importance**: High to Critical, depending on the task.
    3.  **Flow Run SLA Breach (Not Running as Scheduled)**:
        *   **Condition**: A scheduled flow run does not start within a predefined time window (e.g., a daily scraper not starting for X hours past its schedule). Prefect Cloud calls this "Lateness".
        *   **Importance**: High. Indicates issues with the scheduler, agent availability, or work queue backlog.
    4.  **Flow Run Taking Too Long (Exceeding Expected Duration)**:
        *   **Condition**: A flow run exceeds its typical or maximum expected duration.
        *   **Importance**: High. May indicate performance issues, hangs, or infinite loops.

*   **Setup**:
    1.  In the Prefect UI, navigate to the "Notifications" or "Automations" section.
    2.  Create new notification rules.
    3.  Select the trigger states (e.g., "Failed", "Crashed", "Late").
    4.  Specify which flows (by name or tags) or tasks the notification applies to.
    5.  Configure the notification block (e.g., Email, Slack message, PagerDuty).

*   **Notification Channels**:
    *   **Email**: For less urgent but important failure notifications.
    *   **Slack/Microsoft Teams**: For team visibility and quick response.
    *   **PagerDuty/Opsgenie**: For critical alerts requiring immediate attention, potentially waking someone up.
    *   **Webhooks**: For custom integrations.

## 3. Alerting with Sentry

Sentry is focused on application-level code errors and exceptions.

*   **Source**: Sentry UI ([sentry.io](sentry.io) or self-hosted instance).

*   **Alert Trigger Conditions**:
    1.  **New Critical Error**:
        *   **Condition**: A new, previously unseen type of error (issue) is captured in a specific project (e.g., for a particular scraper).
        *   **Importance**: High. Indicates a new bug or an unhandled edge case.
    2.  **High Frequency of Errors**:
        *   **Condition**: An existing error issue occurs more than X times in Y minutes/hours.
        *   **Importance**: High to Critical. Can indicate a widespread or rapidly escalating problem.
    3.  **Regression (Resolved Issue Reappears)**:
        *   **Condition**: An error that was previously marked as "resolved" in Sentry starts occurring again.
        *   **Importance**: High. Indicates a fix was incomplete or a similar issue has been reintroduced.

*   **Setup**:
    1.  In Sentry, navigate to your Project > Alerts > Create Alert Rule.
    2.  Define conditions based on:
        *   "An event is seen for the first time" (for new errors).
        *   "An issue occurs X times in Y interval" (for frequency).
        *   "An issue changes state from resolved to unresolved" (for regressions).
    3.  Filter by environment (e.g., `production`, `staging`).
    4.  Filter by error level (e.g., `error`, `fatal`).
    5.  Configure actions (send a notification).

*   **Notification Channels**:
    *   Email, Slack, PagerDuty, Microsoft Teams, Jira, GitHub Issues, etc. Sentry offers many integrations.

## 4. Alerting with Grafana (from Prometheus Metrics)

Grafana uses data queried from Prometheus to trigger alerts based on metric thresholds.

*   **Source**: Grafana UI, with Prometheus as the data source.

*   **Alert Trigger Conditions & Setup**:
    *   **Prerequisite**: Ensure Prometheus is scraping metrics from:
        *   Custom Python applications (scrapers, ETL) using `prometheus_client`.
        *   Node Exporters on agent/scraper hosts for system metrics.
        *   Database exporters (e.g., MongoDB exporter) for database health.
        *   (Optionally) A Prefect Prometheus exporter if available and needed for deeper Prefect metrics not covered by its native notifications.

    1.  **Low Scraper Success Rate**:
        *   **Condition**: `avg_over_time(scraper_job_success_rate{scraper_name="X"}[5m]) < 0.8` (Success rate for scraper "X" drops below 80% over 5 mins).
        *   **Importance**: Critical.
        *   **Metric Source**: Custom metric from scraper (e.g., a Gauge for success status per run, aggregated).
    2.  **Zero Data Extraction**:
        *   **Condition**: `sum_over_time(items_extracted_total{scraper_name="X"}[1h]) == 0` (Scraper "X" extracts zero items in the last hour, for a scraper expected to run and find data).
        *   **Importance**: Critical.
    3.  **High HTTP Error Rate from Target Site**:
        *   **Condition**: `rate(http_client_requests_total{scraper_name="X", code=~"4xx|5xx"}[5m]) > 5` (More than 5 client/server errors per minute from scraper "X").
        *   **Importance**: High.
        *   **Metric Source**: Custom metric from scraper (Counter for HTTP responses by code).
    4.  **ETL Failures or No Output**:
        *   **Condition**: `etl_records_loaded_total{etl_name="Y"}` shows no increase after an expected run, or a specific `etl_process_failed_total{etl_name="Y"}` counter increments.
        *   **Importance**: Critical.
        *   **Metric Source**: Custom metrics from ETL script.
    5.  **High Prefect Work Queue Length**:
        *   **Condition**: `prefect_work_queue_length{queue_name="scraping"} > 50` (More than 50 items pending in the "scraping" work queue for an extended period).
        *   **Importance**: High. Indicates agent capacity issues or stuck jobs.
        *   **Metric Source**: Prefect metrics exposed to Prometheus (may require a Prefect Prometheus exporter or custom instrumentation if not available by default).
    6.  **Agent Down**:
        *   **Condition**: `up{job="my_prefect_agents"} == 0` for a specific agent instance.
        *   **Importance**: Critical.
        *   **Metric Source**: Prometheus `up` metric for agent targets.
    7.  **Database Connectivity Issues**:
        *   **Condition**: `mongodb_up == 0` (or similar metric from a MongoDB exporter).
        *   **Importance**: Critical.
        *   **Metric Source**: Database exporter metrics.
    8.  **Low Disk Space on Agent/DB Host**:
        *   **Condition**: `node_filesystem_avail_bytes{mountpoint="/"} / node_filesystem_size_bytes{mountpoint="/"} < 0.1` (Less than 10% disk space available).
        *   **Importance**: Critical.
        *   **Metric Source**: Node Exporter metrics.

*   **Setup in Grafana**:
    1.  Create a dashboard panel visualizing the metric you want to alert on.
    2.  Go to the "Alert" tab of the panel.
    3.  Create an alert rule, define the condition (query and threshold), evaluation interval, and "for" duration (how long the condition must be true to trigger).
    4.  Configure "Notification channels" in Grafana (under Alerting > Notification channels) and link them to your alert rules.

*   **Notification Channels**:
    *   Email, Slack, PagerDuty, Webhooks, Microsoft Teams, etc.

## 5. General Alerting Considerations

*   **Start Simple, Iterate**: Begin with the most critical alerts (flow failures, core scraper failures, DB down).
*   **Define Alert Severity**: Use prefixes like `[CRITICAL]`, `[WARNING]` in alert messages.
*   **Avoid Alert Fatigue**: Fine-tune alert thresholds and conditions to ensure they are meaningful. Suppress noisy or flapping alerts.
*   **Actionable Alerts**: Ensure alerts provide enough context (e.g., which scraper, what error, link to logs/dashboard) for someone to start investigating.
*   **Documentation/Runbooks**: For common critical alerts, have basic troubleshooting steps or runbooks documented.
*   **On-Call Rotation**: If the pipeline is business-critical, establish an on-call rotation for handling critical alerts outside of business hours.
*   **Regular Review**: Periodically review alert configurations, thresholds, and notification channels to ensure they remain relevant and effective.

This multi-layered alerting strategy ensures that failures at different levels of the pipeline (orchestration, application code, metrics thresholds) are caught and communicated appropriately.
