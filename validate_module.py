import pandas as pd
import logging

#Validate the transformed daily and monthly weather data.
def validate_data(**kwargs):

    # Retrieve file paths
    daily_weather_file_path = kwargs['ti'].xcom_pull(key='daily_weather_csv_file_path')
    monthly_weather_file_path = kwargs['ti'].xcom_pull(key='monthly_weather_csv_file_path')

    # Load data
    df_daily = pd.read_csv(daily_weather_file_path)
    df_monthly = pd.read_csv(monthly_weather_file_path)


    # Critical columns for validation
    daily_critical_columns = [
        'formatted_date', 'avg_temperature_c', 'avg_apparent_temperature_c',
        'avg_humidity', 'avg_wind_speed_kmh', 'avg_visibility_km', 'avg_pressure_millibars'
    ]
    monthly_critical_columns = [
        'month', 'avg_temperature_c', 'avg_apparent_temperature_c',
        'avg_humidity', 'avg_visibility_km', 'avg_pressure_millibars'
    ]

    # Set the expected ranges for temperature, humidity and wind speed
    daily_ranges = {
        'avg_temperature_c': (-50, 50),
        'avg_humidity': (0, 1),
        'avg_wind_speed_kmh': (0, 150),
    }

    monthly_ranges = {
        'avg_temperature_c': (-50, 50),
        'avg_humidity': (0, 1),
    }
    
    # Seasonal ranges for temperature
    seasonal_ranges = {
            "Winter": (-50, 15),
            "Spring": (-15, 30),
            "Summer": (5, 50),
            "Autumn": (-15, 30),
    }

    # Check missing values in critical columns
    def check_missing_values(df, critical_columns):
        """Ensure critical columns have no missing values."""
        missing_values = df[critical_columns].isnull().sum()
        if missing_values.any():
            logging.error(f"Missing values found:\n{missing_values}")
            raise ValueError("Validation failed: Missing values in critical columns.")
        logging.info("No missing values in critical columns.")

    # Verify that values fall within expected ranges
    def check_ranges(df, ranges):
        """Verify that numerical columns fall within defined ranges."""
        for column, (min_val, max_val) in ranges.items():
            if not df[column].between(min_val, max_val).all():
                logging.error(f"{column} values out of range ({min_val}, {max_val}).")
                raise ValueError(f"Validation failed: {column} values out of range.")
        logging.info("All values within expected ranges.")

    # Outlier Detection: Identify and log any extreme outliers in data.
    def log_outliers(df, column, threshold, description):
        """Log outliers that exceed a given threshold."""
        outliers = df[df[column] > threshold]
        if not outliers.empty:
            logging.warning(f"{len(outliers)} {description} outliers detected in {column}.")
            logging.warning(f"Sample outliers:\n{outliers[[column]].head()}")
        else:
            logging.info(f"No {description} outliers detected in {column}.")

    # Find and log temperatures that are outside expected seasonal ranges.
    def detect_outliers_temperature_daily(df):

        df['formatted_date'] = pd.to_datetime(df['formatted_date'], utc=True)
                                              
        # Assign season based on 'formatted_date'
        def determine_season(row):
            row = pd.to_datetime(row)
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
            season_rows = df[df['formatted_date'].apply(lambda x: determine_season(x)) == season]

            # Check for temperature outliers within that season
            season_outliers = season_rows[
                (season_rows['avg_temperature_c'] < temp_min) |
                (season_rows['avg_temperature_c'] > temp_max)
                ]
            outliers_temperature.append(season_outliers)

        # Log the season temperature outliers
        outliers_temperature = pd.concat(outliers_temperature,
                                         ignore_index=True) if outliers_temperature else pd.DataFrame()
        if not outliers_temperature.empty:
            logging.warning(f"Outliers detected in temperature data. Total count: {len(outliers_temperature)}")
            logging.warning(f"Sample outliers:\n{outliers_temperature[['formatted_date', 'avg_temperature_c']].head()}")
        else:
            logging.info("No temperature outliers detected.")
    
    def detect_outliers_temperature_monthly(df):
                                              
        # Assign season based on 'month'
        def determine_season(month):
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
            season_rows = df[df['month'].apply(lambda x: determine_season(x)) == season]

            # Check for temperature outliers within that season
            season_outliers = season_rows[
                (season_rows['avg_temperature_c'] < temp_min) |
                (season_rows['avg_temperature_c'] > temp_max)
                ]
            outliers_temperature.append(season_outliers)

        # Log the season temperature outliers
        outliers_temperature = pd.concat(outliers_temperature,
                                         ignore_index=True) if outliers_temperature else pd.DataFrame()
        if not outliers_temperature.empty:
            logging.warning(f"Outliers detected in temperature data. Total count: {len(outliers_temperature)}")
            logging.warning(f"Sample outliers:\n{outliers_temperature[['month', 'avg_temperature_c']].head()}")
        else:
            logging.info("No temperature outliers detected.")

    # Perform validations
    logging.info("Validating daily weather data.")
    check_missing_values(df_daily, daily_critical_columns)
    check_ranges(df_daily, daily_ranges)
    log_outliers(df_daily, 'avg_wind_speed_kmh', 150, "high wind speed")
    detect_outliers_temperature_daily(df_daily)

    logging.info("Validating monthly weather data.")
    check_missing_values(df_monthly, monthly_critical_columns)
    check_ranges(df_monthly, monthly_ranges)
    detect_outliers_temperature_monthly(df_monthly)

    # Push validated file paths to XCom
    kwargs['ti'].xcom_push(key='validated_daily_weather_file_path', value=daily_weather_file_path)
    kwargs['ti'].xcom_push(key='validated_monthly_weather_file_path', value=monthly_weather_file_path)

    logging.info("Data validation completed successfully.")
