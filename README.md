# Excel to Postgres Data Pipeline with Airflow

This project implements a data pipeline that extracts data from an Excel file and loads it into a PostgreSQL database using Apache Airflow.

## Prerequisites

- Python 3.8+
- Docker and Docker Compose
- `virtualenv` (optional but recommended)

## Setup

1.  **Clone the repository** (if applicable) or navigate to the project directory.

2.  **Create and activate a virtual environment**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Start PostgreSQL**:
    Use Docker Compose to start the PostgreSQL database.
    ```bash
    docker-compose up -d
    ```

## Configuration

1.  **Set Airflow Home**:
    Set the `AIRFLOW_HOME` environment variable to the current directory so Airflow uses the local `airflow.cfg` and `dags/` folder.
    ```bash
    export AIRFLOW_HOME=$(pwd)
    ```

2.  **Initialize Airflow (First time only)**:
    If you haven't run Airflow before in this directory, initialize the database and create a user.
    ```bash
    airflow db init
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    ```
    *Note: If you use `airflow standalone`, this is done automatically.*

3.  **Add PostgreSQL Connection**:
    Add the connection string for the PostgreSQL database to Airflow.
    ```bash
    airflow connections add 'postgres_default' --conn-uri 'postgresql://user:password@localhost:5432/mydatabase'
    ```

## Running the Pipeline

1.  **Start Airflow**:
    Run the standalone command to start the Webserver, Scheduler, and Triggerer.
    ```bash
    export AIRFLOW_HOME=$(pwd)
    airflow standalone
    ```

2.  **Access the Dashboard**:
    -   Open your browser and go to [http://localhost:8080](http://localhost:8080).
    -   Login with the credentials provided in the terminal output (or `admin`/`admin` if you created it manually).

3.  **Trigger the DAG**:
    -   Locate the `excel_to_postgres` DAG.
    -   Unpause it (toggle the switch to blue).
    -   The DAG is scheduled to run every 10 seconds. You can also manually trigger it by clicking the "Play" button.

## Project Structure

-   `dags/excel_to_postgres_dag.py`: The Airflow DAG definition.
-   `data/sample_data.xlsx`: The source Excel file.
-   `compose.yml`: Docker Compose file for PostgreSQL.
-   `airflow.cfg`: Airflow configuration file.
