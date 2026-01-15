"""
Sample SubDAG Example for Apache Airflow 3 using Decorators

Note: SubDAGs are deprecated. Use TaskGroups with @task_group decorator instead.
This example demonstrates the modern Airflow 3 decorator syntax.
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task, task_group


# =============================================================================
# Example 1: Using @task_group decorator (Recommended approach)
# =============================================================================

@dag(
    dag_id='taskgroup_decorator_example',
    description='Modern Airflow 3 DAG using @task_group decorator',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example', 'taskgroup', 'airflow3', 'decorators'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def taskgroup_decorator_example():
    """
    Example DAG using Airflow 3 decorator syntax with task groups
    """
    
    @task()
    def start():
        """Start the pipeline"""
        print("Starting the pipeline...")
        return "Pipeline started"
    
    @task_group(group_id='processing_group')
    def processing_group():
        """
        Group of processing tasks using @task_group decorator
        """
        
        @task()
        def process_step_1():
            """First processing step"""
            print("Processing step 1...")
            return "Step 1 complete"
        
        @task()
        def process_step_2(prev_result: str):
            """Second processing step"""
            print(f"Previous result: {prev_result}")
            print("Processing step 2...")
            return "Step 2 complete"
        
        @task()
        def process_step_3(prev_result: str):
            """Third processing step"""
            print(f"Previous result: {prev_result}")
            print("Processing step 3...")
            return "Step 3 complete"
        
        # Define dependencies within the task group
        step1 = process_step_1()
        step2 = process_step_2(step1)
        step3 = process_step_3(step2)
        
        return step3
    
    @task()
    def end(processing_result: str):
        """End the pipeline"""
        print(f"Processing result: {processing_result}")
        print("Pipeline completed!")
        return "Pipeline finished"
    
    # Define DAG dependencies
    start_result = start()
    processing_result = processing_group()
    end(processing_result)


# Instantiate the DAG
taskgroup_decorator_example()


# # =============================================================================
# # Example 2: Nested TaskGroups with Data Passing
# # =============================================================================

# @dag(
#     dag_id='nested_taskgroup_example',
#     description='Advanced example with nested task groups and data passing',
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=['example', 'taskgroup', 'nested', 'airflow3'],
#     default_args={
#         'owner': 'airflow',
#         'retries': 1,
#     }
# )
# def nested_taskgroup_example():
#     """
#     Example showing nested task groups with data passing between tasks
#     """
    
#     @task()
#     def extract_data():
#         """Extract data from source"""
#         print("Extracting data...")
#         data = {
#             'records': [
#                 {'id': 1, 'name': 'Item 1', 'value': 100},
#                 {'id': 2, 'name': 'Item 2', 'value': 200},
#                 {'id': 3, 'name': 'Item 3', 'value': 300},
#             ]
#         }
#         print(f"Extracted {len(data['records'])} records")
#         return data
    
#     @task_group(group_id='transform_group')
#     def transform_group():
#         """Transform data with multiple steps"""
        
#         @task()
#         def validate_data(data: dict):
#             """Validate extracted data"""
#             print("Validating data...")
#             valid_records = [r for r in data['records'] if r['value'] > 0]
#             print(f"Valid records: {len(valid_records)}")
#             return {'records': valid_records, 'validated': True}
        
#         @task()
#         def enrich_data(data: dict):
#             """Enrich data with additional fields"""
#             print("Enriching data...")
#             for record in data['records']:
#                 record['enriched'] = True
#                 record['category'] = 'A' if record['value'] > 150 else 'B'
#             print(f"Enriched {len(data['records'])} records")
#             return data
        
#         @task()
#         def aggregate_data(data: dict):
#             """Aggregate data"""
#             print("Aggregating data...")
#             total_value = sum(r['value'] for r in data['records'])
#             data['total_value'] = total_value
#             data['count'] = len(data['records'])
#             print(f"Total value: {total_value}, Count: {data['count']}")
#             return data
        
#         # Get data from parent scope
#         extracted = extract_data()
#         validated = validate_data(extracted)
#         enriched = enrich_data(validated)
#         aggregated = aggregate_data(enriched)
        
#         return aggregated
    
#     @task_group(group_id='load_group')
#     def load_group():
#         """Load data to multiple destinations"""
        
#         @task()
#         def load_to_database(data: dict):
#             """Load data to database"""
#             print("Loading to database...")
#             print(f"Loaded {data['count']} records with total value {data['total_value']}")
#             return "Database load complete"
        
#         @task()
#         def load_to_cache(data: dict):
#             """Load data to cache"""
#             print("Loading to cache...")
#             print(f"Cached {data['count']} records")
#             return "Cache load complete"
        
#         @task()
#         def verify_load(db_result: str, cache_result: str):
#             """Verify all loads completed"""
#             print(f"DB: {db_result}, Cache: {cache_result}")
#             return "All loads verified"
        
#         # Get transformed data
#         transformed = transform_group()
#         db = load_to_database(transformed)
#         cache = load_to_cache(transformed)
#         verified = verify_load(db, cache)
        
#         return verified
    
#     @task()
#     def send_notification(load_result: str):
#         """Send completion notification"""
#         print(f"Load result: {load_result}")
#         print("Sending notification...")
#         return "Notification sent"
    
#     # Define DAG flow
#     notification = send_notification(load_group())


# # Instantiate the DAG
# nested_taskgroup_example()


# # =============================================================================
# # Example 3: Dynamic Task Groups
# # =============================================================================

# @dag(
#     dag_id='dynamic_taskgroup_example',
#     description='Example with dynamic task groups using expand',
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=['example', 'dynamic', 'taskgroup', 'airflow3'],
#     default_args={'owner': 'airflow', 'retries': 1}
# )
# def dynamic_taskgroup_example():
#     """
#     Example showing dynamic task creation with task groups
#     """
    
#     @task()
#     def get_categories():
#         """Get list of categories to process"""
#         return ['category_A', 'category_B', 'category_C']
    
#     @task()
#     def process_category(category: str):
#         """Process a single category"""
#         print(f"Processing {category}...")
#         result = {
#             'category': category,
#             'processed': True,
#             'items_count': len(category) * 10
#         }
#         print(f"Processed {result['items_count']} items for {category}")
#         return result
    
#     @task()
#     def aggregate_results(results: list):
#         """Aggregate all category results"""
#         print("Aggregating results from all categories...")
#         total_items = sum(r['items_count'] for r in results)
#         summary = {
#             'total_categories': len(results),
#             'total_items': total_items
#         }
#         print(f"Summary: {summary}")
#         return summary
    
#     # Dynamic task mapping
#     categories = get_categories()
#     results = process_category.expand(category=categories)
#     summary = aggregate_results(results)


# # Instantiate the DAG
# dynamic_taskgroup_example()
