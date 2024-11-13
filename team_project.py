import pandas as pd
import sqlite3
import os
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi

# Default arguments for the DAG
default_args = {
    'owner': 'Team_work',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'world_happiness_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for World Happiness Report data with validation and trigger rules',
    schedule_interval='@daily',
)

# File paths
csv_file_path = '/home/de2023a/airflow/datasets/2019.csv'
db_path = '/home/de2023a/airflow/databases/happiness_data.db'