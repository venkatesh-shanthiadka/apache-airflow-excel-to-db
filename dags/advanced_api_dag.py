import json
import urllib.request
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    "owner": "airflow",
    "retries": 1,
}

@dag(
    dag_id="advanced_api_dag",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example"],
)
def advanced_api_dag():
    @task()
    def fetch_data():
        print("-->Fetching data...")
        url = "https://jsonplaceholder.typicode.com/todos/1"
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
        print("-->Data fetched successfully.")
        return data

    @task()
    def store_data(data: dict):
        print("-->Storing data...")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"/opt/airflow/dags/data_{timestamp}.json"
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print("-->Data stored successfully.")
        return f"Stored data to {filename}"

    data = fetch_data()
    store_data(data)

advanced_api_dag()
