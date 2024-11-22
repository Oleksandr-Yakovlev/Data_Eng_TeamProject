import pandas as pd
import os
import zipfile
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi


# File paths
csv_file_path = '/home/tuuli/airflow/datasets/weatherHistory.csv'

# Task 1: Extract data
def extract_data(**kwargs):

    
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Define file paths
    downloaded_file_path = '/home/tuuli/airflow/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'

    if not os.path.exists(downloaded_file_path):
        api.dataset_download_file("muthuj7/weather-dataset", file_name='weatherHistory.csv', path='/home/tuuli/airflow/datasets')
    else:
        print("File already exists, skipping download.")# Download the dataset file

    # Check if the downloaded file is a ZIP file
    if os.path.exists(zip_file_path) and zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/home/tuuli/airflow/datasets')
        # Optionally delete the ZIP file after extraction
        os.remove(zip_file_path)
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)
