from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from extract_module import extract_data
from transform_module import transform_data
from validate_module import validate_data
from load_module import load_data

default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'Historical_weather_data',
    default_args=default_args,
    description='ETL pipeline for Historical weather data',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    provide_context=True,
    trigger_rule='all_success',
    dag=dag,
    )

extract_task >> transform_task >> validate_task >> load_task
