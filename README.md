# Airflow 3.x Docker Compose Setup

This project runs Apache Airflow 3.x using Docker Compose with the CeleryExecutor, Redis, and PostgreSQL.

## Prerequisites

- Docker Desktop
- Docker Compose

## Quick Start

1. Setup the .env file

    ```bash
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

2.  **Prepare Directories and Environment:**
    Create the necessary directories and set the Airflow user ID in the `.env` file.
    ```bash
    mkdir -p ./dags ./logs ./plugins ./config
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Initialize the Environment:**
    Run the initialization service to create necessary directories and set permissions.
    ```bash
    docker compose up airflow-init
    ```

5.  **Start Airflow:**
    Start all services in detached mode.
    ```bash
    docker compose up -d
    ```

6.  **Access the UI:**
    Open your browser to [http://localhost:8080](http://localhost:8080).
    - **Username:** `airflow`
    - **Password:** `airflow`

7.  **Stop Airflow:**
    ```bash
    docker compose down
    ```

## Configuration Details

This setup is based on the official [Airflow Docker Compose](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html) documentation.

### Services

- **airflow-apiserver:** The Airflow API server (replaces webserver in 3.x for some functions, but serves UI here).
- **airflow-scheduler:** The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
- **airflow-worker:** The worker that executes the tasks given by the scheduler.
- **airflow-triggerer:** The triggerer runs an event loop for deferrable tasks.
- **airflow-dag-processor:** Parses DAG files and synchronizes them with the database.
- **postgres:** The database.
- **redis:** The broker that forwards messages from scheduler to worker.

### Environment Variables

Key environment variables configured in `compose.yml`:

- `AIRFLOW__CORE__EXECUTOR`: `CeleryExecutor`
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: `postgresql+psycopg2://airflow:airflow@postgres/airflow`
- `AIRFLOW__CELERY__RESULT_BACKEND`: `db+postgresql://airflow:airflow@postgres/airflow`
- `AIRFLOW__CELERY__BROKER_URL`: `redis://:@redis:6379/0`
- `AIRFLOW__CORE__EXECUTION_API_SERVER_URL`: `http://airflow-apiserver:8080/execution/`
- `AIRFLOW__CORE__LOAD_EXAMPLES`: `true`

### Ports

- **8080:** Airflow Web UI / API
- **5555:** Flower (Celery monitoring) - *Optional, enable with `--profile flower`*
- **5432:** PostgreSQL (internal)
- **6379:** Redis (internal)

## Directories

- `./dags`: Put your DAG files here.
- `./logs`: Contains logs from task execution and scheduler.
- `./plugins`: Put your custom plugins here.
- `./config`: Configuration files.

## Useful Commands

- **List DAGs:**
  ```bash
  docker compose run airflow-cli dags list
  ```

- **Trigger a DAG:**
  ```bash
  docker compose run airflow-cli dags trigger <dag_id>
  ```

- **Check Worker Logs:**
  ```bash
  docker compose logs -f airflow-worker
  ```


# After the docker compose up below is the state of the services
![Airflow UI Screenshot](./assets/image.png)
