import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

# File paths
csv_file_path = '/Users/home/airflow/datasets/weatherHistory.csv'

# Task 1: Extract data
def extract_data(**kwargs):
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file("muthuj7/weather-dataset", file_name='weatherHistory.csv', path='/Users/home/airflow/datasets')

    # Define file paths
    downloaded_file_path = '/Users/home/airflow/datasets/weatherHistory.csv'
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if os.path.exists(zip_file_path) and zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall('/Users/home/airflow/datasets')
        # Optionally delete the ZIP file after extraction
        os.remove(zip_file_path)
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    # kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

# Execute the function
extract_data()
