"""
Airflow DAG for orchestrating dbt transformations with OpenLineage integration.

This DAG demonstrates how to:
1. Run dbt transformations with automatic lineage capture
2. Execute Python jobs with custom lineage emission
3. Monitor data quality and lineage completeness
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

# DAG definition
dag = DAG(
    'dbt_daily_lineage',
    default_args=default_args,
    description='Daily dbt transformations with lineage tracking',
    schedule_interval='@daily',
    max_active_runs=1,
    tags=['dbt', 'lineage', 'data-quality'],
)

# Task definitions
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# dbt seed task
dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd /opt/airflow/dbt_project && dbt seed',
    dag=dag,
)

# dbt run task
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt_project && dbt run',
    dag=dag,
)

# dbt test task
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt_project && dbt test',
    dag=dag,
)

# Python lineage job
def run_python_lineage_job():
    """Run Python lineage job."""
    import sys
    import os
    
    # Add the python jobs directory to Python path
    sys.path.append('/opt/airflow/python_jobs')
    
    # Change to the python jobs directory
    os.chdir('/opt/airflow/python_jobs')
    
    # Import and run the job
    from job_transform_orders import OrderTransformJob
    
    job = OrderTransformJob()
    job.run()

python_lineage_job = PythonOperator(
    task_id='python_lineage_job',
    python_callable=run_python_lineage_job,
    dag=dag,
)

# Data quality check task
def check_lineage_completeness():
    """Check that lineage was properly captured."""
    import requests
    import json
    
    marquez_url = "http://marquez:5000"
    
    try:
        # Check if datasets exist
        datasets_response = requests.get(f"{marquez_url}/api/v1/namespaces/data-lineage-audit/datasets")
        datasets = datasets_response.json()
        
        expected_datasets = [
            'raw_customers',
            'raw_orders', 
            'stg_orders',
            'dim_customers',
            'fct_orders',
            'enriched_orders',
            'order_summary'
        ]
        
        existing_datasets = [dataset['name'] for dataset in datasets.get('datasets', [])]
        
        missing_datasets = set(expected_datasets) - set(existing_datasets)
        
        if missing_datasets:
            raise Exception(f"Missing datasets in lineage: {missing_datasets}")
        
        print(f"Lineage check passed. Found {len(existing_datasets)} datasets.")
        
        # Check if jobs exist
        jobs_response = requests.get(f"{marquez_url}/api/v1/namespaces/data-lineage-audit/jobs")
        jobs = jobs_response.json()
        
        expected_jobs = [
            'dbt_seed',
            'dbt_run', 
            'dbt_test',
            'customer_data_processing',
            'order_data_transformation'
        ]
        
        existing_jobs = [job['name'] for job in jobs.get('jobs', [])]
        
        missing_jobs = set(expected_jobs) - set(existing_jobs)
        
        if missing_jobs:
            raise Exception(f"Missing jobs in lineage: {missing_jobs}")
        
        print(f"Lineage check passed. Found {len(existing_jobs)} jobs.")
        
    except Exception as e:
        print(f"Lineage completeness check failed: {e}")
        raise

lineage_check = PythonOperator(
    task_id='lineage_completeness_check',
    python_callable=check_lineage_completeness,
    dag=dag,
)

# End task
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Task dependencies
start_task >> dbt_seed >> dbt_run >> dbt_test >> python_lineage_job >> lineage_check >> end_task
