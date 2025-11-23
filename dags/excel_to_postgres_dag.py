from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def create_table():
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INTEGER,
        city VARCHAR(100)
    );
    """
    hook.run(sql)

def process_data():
    # Path to the Excel file
    file_path = os.path.join(os.environ.get('AIRFLOW_HOME', '.'), 'data', 'sample_data.xlsx')
    
    # Read Excel file
    df = pd.read_excel(file_path)
    
    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id='postgres_default')
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    # Insert data
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO users (name, age, city) VALUES (%s, %s, %s)",
            (row['name'], row['age'], row['city'])
        )
    
    connection.commit()
    cursor.close()
    connection.close()

with DAG('excel_to_postgres', default_args=default_args, schedule='@daily', catchup=False) as dag:
    
    create_table_task = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )
    
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data
    )
    
    create_table_task >> process_data_task
