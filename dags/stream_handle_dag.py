from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
from typing import List, Dict

# Configuration for the API endpoint
STREAMS_API_URL = "https://api.example.com/streams"  # Replace with your actual API endpoint
API_TIMEOUT = 30  # seconds

def fetch_streams_from_api(**context) -> List[Dict]:
    """
    Fetch streams configuration from an API endpoint.
    This function runs as an Airflow task to dynamically fetch stream configurations.
    
    Returns:
        List of stream configurations with added metadata
    """
    try:
        response = requests.get(STREAMS_API_URL, timeout=API_TIMEOUT)
        response.raise_for_status()
        streams = response.json()
        
        logging.info(f"Successfully fetched {len(streams)} streams from API")
        
        # Add Airflow context to each stream configuration
        execution_date = context['ts']
        run_id = context['run_id']
        
        enriched_streams = [
            {
                "stream_id": stream["stream_id"],
                "source": stream["source"],
                "partition_count": stream["partition_count"],
                "priority": stream["priority"],
                "triggered_at": execution_date,
                "dag_run_id": run_id
            }
            for stream in streams
        ]
        
        return enriched_streams
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch streams from API: {e}")
        # Fallback to default streams if API is unavailable
        logging.warning("Using fallback streams configuration")
        
        execution_date = context['ts']
        run_id = context['run_id']
        
        # Generate 100 fallback streams with varied configurations
        sources = ["kafka", "kinesis", "pubsub", "rabbitmq", "sqs"]
        priorities = ["high", "medium", "low"]
        
        fallback_streams = []
        for i in range(100):
            fallback_streams.append({
                "stream_id": f"stream_{i:03d}",
                "source": sources[i % len(sources)],
                "partition_count": (i % 10) + 1,
                "priority": priorities[i % len(priorities)],
                "triggered_at": execution_date,
                "dag_run_id": run_id
            })
        
        return fallback_streams

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dynamic_stream_processor',
    default_args=default_args,
    description='Process multiple streams using dynamic task mapping',
    schedule=None,  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['streams', 'dynamic-mapping', 'processing'],
) as dag:

    # Step 1: Fetch streams from API as a task
    get_streams_task = PythonOperator(
        task_id='fetch_streams_from_api',
        python_callable=fetch_streams_from_api,
    )
    
    # Step 2: Dynamic Task Mapping based on API response
    # .partial() - defines static parameters (same for all instances)
    # .expand() - defines dynamic parameters (different for each instance)
    # The .output property gets the return value from the upstream task
    
    trigger_stream_processing = TriggerDagRunOperator.partial(
        task_id='trigger_stream_handling',
        trigger_dag_id='advanced_api_dag',
        wait_for_completion=True,   # Wait for triggered DAGs to complete
        depends_on_past=True,       # Only run if previous task instance succeeded
        poke_interval=30,           # Check every 30 seconds if waiting
        reset_dag_run=True,         # Reset if DAG run already exists
        allowed_states=['success'], # Only consider success state as valid
        failed_states=['failed'],   # Consider failed state as failure
    ).expand(
        # Dynamically expand based on the output from get_streams_task
        # Each stream configuration becomes a separate task instance
        conf=get_streams_task.output
    )
    
    # Define task dependencies
    get_streams_task >> trigger_stream_processing