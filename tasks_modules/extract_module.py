import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi

kaggle_dataset = os.getenv('KAGGLE_DATASET')
kaggle_file_name = os.getenv('KAGGLE_FILE_NAME')
download_path = os.getenv('DOWNLOAD_PATH')


# Task 1: Extract data
def extract_data(**kwargs):
    # Set up Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Download the dataset file
    api.dataset_download_file(kaggle_dataset, file_name=kaggle_file_name,
                              path=download_path)

    # Define file paths
    downloaded_file_path = os.path.join(download_path, kaggle_file_name)
    zip_file_path = downloaded_file_path + '.zip'

    # Check if the downloaded file is a ZIP file
    if os.path.exists(zip_file_path) and zipfile.is_zipfile(zip_file_path):
        # If it's a ZIP file, unzip it
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(download_path)
        # Optionally delete the ZIP file after extraction
        os.remove(zip_file_path)
    else:
        print("Downloaded file is not a ZIP archive, skipping extraction.")

    # Push the CSV file path to XCom for use in the next steps
    kwargs['ti'].xcom_push(key='csv_file_path', value=downloaded_file_path)

