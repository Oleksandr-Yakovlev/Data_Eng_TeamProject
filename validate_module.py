#Validate Module
import pandas as pd
from airflow.operators.python_operator import PythonOperator
import logging


# Task 3: Validate data
def validate_data(**kwargs):
    # Retrieve transformed file path from XCom
    transformed_file_path = kwargs['ti'].xcom_pull(key='transformed_file_path')
    df = pd.read_csv(transformed_file_path)

    # Ensure the 'Formatted Date' column is parsed as datetime
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'])

    # Missing values check: no critical columns have missing values after transformation
    def check_missing_values(df):
        # List the critical columns
        critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Visibility (km)',
                            'Pressure (millibars)', 'Wind Strength', 'Mode']
        if df[critical_columns].isnull().any().any():
            raise ValueError("Validation failed: Missing values in critical columns after transformation.")

    # Range check: Verify that values fall within expected ranges
    def check_ranges(df):
        # Temperature (-50 to 50°C)
        if not ((df['Temperature (C)'] >= -50) & (df['Temperature (C)'] <= 50)).all():
            raise ValueError("Validation failed: Temperature values out of range (-50 to 50°C).")
        # Humidity (0 to 1)
        if not ((df['Humidity'] >= 0) & (df['Humidity'] <= 1)).all():
            raise ValueError("Validation failed: Humidity values out of range (0 to 1).")
        # Wind Speed (0 and above)
        if not ((df['Wind Speed (km/h)'] >= 0)).all():
            raise ValueError("Validation failed: Wind speed values out of range (0 and above).")

    # Outlier Detection: Identify and log any extreme outliers in data.
    # Find and log temperatures that are outside expected seasonal ranges.

    # Seasonal ranges for temperature
    def detect_outliers_temperature(df):
        seasonal_ranges = {
            "Winter": (-50, 15),
            "Spring": (-15, 30),
            "Summer": (5, 50),
            "Autumn": (-15, 30),
        }

        # Assign season based on 'Formatted Date'
        def determine_season(row):
            month = row.month
            if month in [12, 1, 2]:
                return "Winter"
            elif month in [3, 4, 5]:
                return "Spring"
            elif month in [6, 7, 8]:
                return "Summer"
            else:
                return "Autumn"

        outliers_temperature = []

        for season, (temp_min, temp_max) in seasonal_ranges.items():
            # Identify rows that fall in the season
            season_rows = df[df['Formatted Date'].apply(lambda x: determine_season(x)) == season]

            # Check for temperature outliers within that season
            season_outliers = season_rows[
                (season_rows['Temperature (C)'] < temp_min) |
                (season_rows['Temperature (C)'] > temp_max)
                ]
            outliers_temperature.append(season_outliers)

        # Log the season temperature outliers
        outliers_temperature = pd.concat(outliers_temperature,
                                         ignore_index=True) if outliers_temperature else pd.DataFrame()
        if not outliers_temperature.empty:
            logging.warning(f"Outliers detected in temperature data. Total count: {len(outliers_temperature)}")
            logging.warning(f"Sample outliers:\n{outliers_temperature[['Formatted Date', 'Temperature (C)']].head()}")
        else:
            logging.info("No temperature outliers detected.")

    # Detect Temperature vs Apparent Temperature Outliers: if the difference more than 10 degrees
    def detect_outliers_temp_vs_apptemp(df):
        temp_diff_threshold = 10  # Define acceptable difference threshold
        outliers_temp_vs_apptemp = df[abs(df['Temperature (C)'] - df['Apparent Temperature (C)']) > temp_diff_threshold]
        # Log the Apparent Temperature outliers
        if not outliers_temp_vs_apptemp.empty:
            logging.warning(
                f"Temperature vs. Apparent Temperature outliers detected. Count: {len(outliers_temp_vs_apptemp)}")
            logging.warning(
                f"Sample outliers:\n{outliers_temp_vs_apptemp[['Formatted Date', 'Temperature (C)', 'Apparent Temperature (C)']].head()}")
        else:
            logging.info("No Temperature vs. Apparent Temperature outliers detected.")

    # Detect Wind Speed outliers, higher than 150 km/h
    def detect_outliners_wind_speed(df):
        wind_speed_max = 150  # Define acceptable wind speed
        outliners_wind_speed = df[df['Wind Speed (km/h)'] > wind_speed_max]
        # Log the high wind speeds
        if not outliners_wind_speed.empty:
            logging.warning(f"Wind speed outliers detected. Count: {len(outliners_wind_speed)}")
            logging.warning(f"Sample outliers:\n{outliners_wind_speed[['Formatted Date', 'Wind Speed (km/h)']].head()}")
        else:
            logging.info("No wind speed outliers detected.")

    check_missing_values(df)
    check_ranges(df)
    detect_outliers_temperature(df)
    detect_outliers_temp_vs_apptemp(df)
    detect_outliners_wind_speed(df)

    # Pass the file path to XCom for the next task
    kwargs['ti'].xcom_push(key='validated_file_path', value=transformed_file_path)


validate_task = PythonOperator(
    task_id='validate_task',
    python_callable=validate_data,
    provide_context=True,
    trigger_rule='all_success',  # Proceed only if all previous tasks succeed
    dag=dag,
)

# REMEMBER TO ADD TRIGGER RULE-LINE TO THE LOAD PART ALSO!
