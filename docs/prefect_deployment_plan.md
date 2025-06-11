# Prefect Deployment Plan

This document outlines a plan for deploying Prefect to orchestrate the web scraping workflows for this project. It covers setting up the Prefect Orion server, developing and registering flows, configuring agents, managing configurations for flows, handling code deployment, and initial monitoring strategies.

## 1. Introduction

Prefect is a modern workflow orchestration tool that will enable robust scheduling, monitoring, and management of our scrapers and ETL processes. This plan details the steps and considerations for a successful Prefect deployment.

## 2. Choosing a Prefect Orion Server Setup

The Prefect Orion server is the backend that orchestrates and tracks flow runs. There are several options:

*   **A. Local Development Server (`prefect orion start`)**:
    *   **Description**: Runs a lightweight Orion server directly on a developer's machine.
    *   **Pros**: Excellent for local development, testing flows, and familiarizing with the UI. No external dependencies.
    *   **Cons**: Not suitable for production; tied to the local machine's uptime.
    *   **Use Case**: Initial development and testing of flows.

*   **B. Self-Hosted Orion Server**:
    *   **Description**: Deploying the Orion server (e.g., via Docker, Kubernetes, or directly on a VM) within your own infrastructure.
    *   **Pros**: Full control over the infrastructure and data. Potentially lower cost for compute if existing infrastructure is leveraged.
    *   **Cons**: Requires setup, maintenance, backups, and scaling of the Orion server components (database, API server).
    *   **Use Case**: Production deployments where data privacy and control are paramount, and the team has infrastructure management capabilities.

*   **C. Prefect Cloud**:
    *   **Description**: A SaaS offering by Prefect Technologies. Provides a managed Orion backend, UI, collaboration features, and more.
    *   **Pros**: No server maintenance overhead. Managed service with high availability. User-friendly UI, user management, workspaces, automations. Generous free tier often sufficient for small to medium projects.
    *   **Cons**: Flow run data and metadata are stored in the Prefect Cloud. Potential cost at higher usage tiers.
    *   **Use Case**: Recommended for most teams to get started quickly and for production deployments where a managed service is preferred.

**Initial Recommendation**:
1.  Start with the **Local Development Server** for all initial flow development and testing.
2.  For shared development or initial production, **Prefect Cloud** (free tier) is highly recommended due to its ease of setup and rich feature set.
3.  Consider a **Self-Hosted Orion Server** later if Prefect Cloud becomes unsuitable due to cost at massive scale or strict data residency requirements.

## 3. Developing and Registering Flows

Flows are the core Python scripts that define your workflows (e.g., `my_scraper_flow.py`).

*   **Flow Code**: Refactor existing scraper scripts into Python functions decorated with `@task` and `@flow` as outlined in `docs/prefect_flow_design.md`.
*   **Deployment Definition (`prefect.yaml`)**:
    *   This YAML file, typically at the root of your project, defines how your flows should be deployed. It specifies entry points, parameters, schedules, infrastructure to run on, etc.
    *   **Example `prefect.yaml` snippet**:
        ```yaml
        # prefect.yaml
        name: scraper-project
        prefect-version: 2.x # Specify your Prefect version

        build: null # Set to null if not building custom images initially for local/simple deployments

        deployments:
          - name: ventura-pipeline-deployment
            flow_path: flows/ventura_pipeline_flow.py # Path to the flow script
            entrypoint: ventura_pipeline_flow.py:ventura_pipeline_flow # File:flow_function_name
            work_queue_name: scraping
            schedule: # Optional schedule
              cron: "0 5 * * *" # Run daily at 5 AM UTC
            parameters:
              run_etl: true
              headless_mode: true
            tags: ["scraping", "ventura", "production"]

          - name: ticketmaster-scraper-deployment
            flow_path: flows/ticketmaster_flow.py
            entrypoint: ticketmaster_flow.py:ticketmaster_event_scraper_flow
            work_queue_name: scraping
            # No schedule = ad-hoc runs
            parameters:
              crawl_listing: false
            tags: ["scraping", "ticketmaster"]
        ```
    *   **Deployment Command**: Use the Prefect CLI to deploy flows based on `prefect.yaml`:
        ```bash
        prefect deploy
        ```
        This command sends the deployment metadata to the Orion API (local, self-hosted, or Cloud).

*   **Python API for Deployments**: For more dynamic or programmatic deployment definitions, Prefect's Python API can be used (e.g., `Deployment.build_from_flow()`). This is useful for CI/CD pipelines.

## 4. Setting Up Prefect Agents

Agents are lightweight processes that poll a work queue for flow runs and execute them in a specified environment.

*   **Role**: An agent acts as the bridge between the Prefect Orion API and your execution environment.
*   **Deployment Environments for Agents**:
    *   **Local Machine**: For testing, run `prefect agent start -q <work_queue_name>`.
    *   **Dedicated Server/VM**: Run the agent as a service (e.g., using systemd).
    *   **Docker Container**: Package the agent with project dependencies into a Docker image. This is a common production setup.
    *   **Kubernetes**: Deploy agents as Kubernetes Deployments.
*   **Key Agent Environment Considerations**:
    *   **Python Dependencies**: The agent's environment *must* have access to Python and all libraries required by your flows (e.g., specified in `requirements.txt`). Use virtual environments (venv, Conda) or ensure the Docker image contains them.
    *   **Playwright Dependencies**: For scrapers using Playwright, the agent's environment needs the Playwright library *and* the browser binaries (`playwright install`). If using Docker, include this step in your Dockerfile.
    *   **Configuration Access**: The agent's execution environment needs access to any required configurations for your flows, typically through environment variables that the unified `config.py` will pick up.
    *   **Network Access**: Ensure the agent can reach the Prefect Orion API and any external services your scrapers need.
*   **Starting an Agent**:
    ```bash
    prefect agent start -q scraping # '-q' specifies the work queue to poll
    ```
    Multiple agents can poll the same work queue for scalability.

## 5. Configuration Management for Flows

Flows will use the unified `config.py` (Pydantic `Settings`) as their primary source of configuration.

*   **Environment Variables**: The execution environment where the agent runs a flow *must* have the necessary environment variables set (e.g., `DB__MONGODB_URI`, `MYSCRAPER__SOME_SETTING`, `API_KEYS__CAPTCHA_SOLVER_API_KEY`). These are picked up by `config.settings`.
*   **Prefect Flow Run Parameters**:
    *   Parameters defined in the `@flow` function (e.g., `target_url: str`) can be provided when a flow run is created (via UI, CLI, or schedule).
    *   These runtime parameters take precedence over defaults defined in the flow signature or potentially even values from `config.settings` if the flow logic is written to prioritize them.
    *   Defaults for flow parameters in `prefect.yaml` or in the flow function signature can be sourced from `config.settings` for consistency.
*   **Secrets Management**:
    *   **Environment Variables**: Securely inject secrets as environment variables into the agent's execution environment.
    *   **Prefect Blocks (Recommended for Prefect Cloud/Server)**: Use Prefect's "Blocks" feature (e.g., Secret Block, JSON Block) to store secrets securely via the Prefect UI. Flows can then load these blocks at runtime. This avoids exposing secrets directly as environment variables in some contexts.
        ```python
        # Example: Accessing a secret block in a flow
        # from prefect.blocks.system import Secret
        # my_api_key = Secret.load("my-api-key-secret-name").get()
        ```

## 6. Code Deployment and Versioning

*   **Git Repository**: All flow code, scraper logic, `prefect.yaml`, and Dockerfiles should be versioned in Git.
*   **Deployment Strategies for Flow Code**:
    *   **A. Image-Based (Recommended for Production)**:
        1.  Package your project (scrapers, flows, `config.py`, `requirements.txt`, etc.) into a Docker image.
        2.  Include Playwright browser binary installation in the Dockerfile.
        3.  Push the image to a container registry (e.g., Docker Hub, AWS ECR, GCP Artifact Registry).
        4.  In `prefect.yaml`, specify an `infrastructure` block of type `DockerContainer` (or `KubernetesJob` using a Docker image) pointing to your image.
        5.  When an agent picks up a flow run, it will pull the specified Docker image and execute the flow run within a container created from that image.
        *   **Pros**: Ensures consistency between development, testing, and production environments. Simplifies dependency management for agents.
    *   **B. Repository-Based / Local Filesystem (Simpler for local/dev)**:
        1.  The agent pulls the latest code from a Git repository before running a flow (e.g., using a `Git` storage block or by configuring the agent's startup script to do so).
        2.  Alternatively, ensure the project code is present on the machine where the agent is running (e.g., via volume mounts for Docker agents).
        *   **Pros**: Simpler setup for local development.
        *   **Cons**: Requires careful management of Python environments and dependencies directly on the agent machines/base images. Can lead to inconsistencies if not managed well.
*   **Versioning**:
    *   Use Git tags for code versions.
    *   Use Docker image tags for image versions.
    *   Prefect deployment names can also incorporate versions (e.g., `my-flow-v1.2`).

## 7. Initial Monitoring Strategies

The Prefect UI (Orion local server or Prefect Cloud) is the primary tool for monitoring.

*   **Dashboard**: View overall statistics, upcoming runs, and recent activity.
*   **Flow Runs Page**: Track the status of individual flow runs (Scheduled, Pending, Running, Completed, Failed, Crashed, Cancelled).
*   **Task Runs View**: Drill down into individual tasks within a flow run to see their status, logs, and retry attempts.
*   **Logs**: Access logs generated by your flows and tasks directly in the UI. Ensure your Python logging is configured to be captured by Prefect (Prefect generally integrates well with standard Python logging).
*   **Automations/Notifications (Prefect Cloud/Server)**:
    *   Set up basic notifications for flow run state changes, especially for failures (e.g., email, Slack).
*   **Regular Checks**: Initially, make it a practice to regularly check the Prefect UI for the status of important flows.
*   **Parameter and Configuration Review**: Use the UI to see which parameters and configurations were used for specific flow runs, aiding in debugging.

By following this plan, the project can effectively deploy Prefect for robust workflow orchestration, leading to more reliable and manageable scraper execution.
