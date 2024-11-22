import pandas as pd
import sqlite3

def load_data(**kwargs):
    # Get the validated file path from XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='daily_validated_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='monthly_validated_file_path')

    # Read the transformed CSV
    daily_data = pd.read_csv(daily_file_path)
    monthly_data = pd.read_csv(monthly_file_path)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Insert data into SQLite
    daily_data.to_sql('daily_weather', conn, if_exists='append', index=False)
    monthly_data.to_sql('daily_weather', conn, if_exists='append', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()
