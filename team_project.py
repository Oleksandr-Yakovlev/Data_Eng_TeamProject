from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract_module import extract_data

default_args = {
    'owner': 'Team-Project_ETL',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'Historical weather data',
    default_args=default_args,
    description='ETL pipeline for Historical weather data with validation and trigger rules',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)


extract_task