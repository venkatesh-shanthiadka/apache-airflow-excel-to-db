import json
import urllib.request
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.timetables.trigger import DeltaTriggerTimetable
import os
import time
default_args = {
    "owner": "airflow",
    "retries": 1,
}

@dag(
    dag_id="advanced_api_dag",
    schedule=None,
    # schedule=DeltaTriggerTimetable(timedelta(seconds=10)),
    # start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["coverity"],
)
def advanced_api_dag():
    @task()
    def fetch_data():
        print("-->Fetching data...")
        url = "https://jsonplaceholder.typicode.com/todos"
        # Set timeout to 30 seconds to prevent hanging
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode())
        print(f"-->Fetched {len(data)} records.")
        print("-->Data fetched successfully.")
        return data

    @task()
    def store_data(data: dict):
        print("-->Storing data...")
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"data_{timestamp}.json"
        print(f"-->Current directory: {os.getcwd()}")
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print("-->Data stored successfully.")
        return f"Stored data to {filename}"

    @task()
    def process_items_sequentially(data: list):
        """Process items sequentially, one after another"""
        processed_items = []
        for item in data:
            print(f"-->Processing item ID: {item.get('id')}")
            print(f"-->Title: {item.get('title')}")
            print(f"-->User ID: {item.get('userId')}")
            print(f"-->Completed: {item.get('completed')}")
            
            # # Simulate failure for item with id 5
            # if item.get('id') == 5:
            #     print(f"-->ERROR: Failed to process item {item.get('id')}")
            #     raise Exception(f"Processing failed for item ID {item.get('id')}: Simulated failure")
            
            time.sleep(2)
            processed_result = {
                "id": item.get('id'),
                "processed": True,
                "title": item.get('title')
            }
            processed_items.append(processed_result)
            print(f"-->Item {item.get('id')} processed successfully.")
        print(f"-->Total items processed: {len(processed_items)}")
        return processed_items

    data = fetch_data()
    store_data(data)
    # Process all items sequentially
    process_items_sequentially(data)

advanced_api_dag()
