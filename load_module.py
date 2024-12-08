import pandas as pd
import sqlite3
import os

# Path for the database
db_path = os.getenv('DATABASE_PATH')
    
def load_data(**kwargs):
    # Get the validated file path from XCom
    daily_file_path = kwargs['ti'].xcom_pull(key='validated_daily_weather_file_path')
    monthly_file_path = kwargs['ti'].xcom_pull(key='validated_monthly_weather_file_path')

    # Read the transformed CSV
    daily_data = pd.read_csv(daily_file_path)
    monthly_data = pd.read_csv(monthly_file_path)

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Insert data into SQLite
    daily_data.to_sql('daily_weather', conn, if_exists='replace', index=False)
    monthly_data.to_sql('monthly_weather', conn, if_exists='replace', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()
