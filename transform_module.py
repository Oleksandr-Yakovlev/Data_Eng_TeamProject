#Transform Module
import pandas as pd
import numpy as np
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    # Set id
    if 'id' not in df.columns:
        df['id'] = df.index

        # Formatted date column to date format
    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)

    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    for column in critical_columns:
        df[column].fillna(df[column].median(), inplace=True)

    df.drop_duplicates(inplace=True)

    # Create 'Date' and 'Month' for calculating averages
    df['Date'] = df['Formatted Date'].dt.date
    df['Month'] = df['Formatted Date'].dt.month

    # Calculating daily averages
    daily_avg_temperature = df.groupby('Date')['Temperature (C)'].mean()
    daily_avg_humidity = df.groupby('Date')['Humidity'].mean()
    daily_avg_wind_speed = df.groupby('Date')['Wind Speed (km/h)'].mean()

    df['Avg Temperature (C)'] = df['Date'].map(daily_avg_temperature)
    df['Avg Humidity'] = df['Date'].map(daily_avg_humidity)
    df['Avg Wind Speed (km/h)'] = df['Date'].map(daily_avg_wind_speed)

    # Define a function to calculate monthly mode value
    def calculate_monthly_mode(series):
        mode = series.mode()
        return mode[0] if len(mode) == 1 else np.nan

    # Use 'calculate_monthly_mode' function
    monthly_precip_type = df.groupby('Month')['Precip Type'].agg(calculate_monthly_mode)

    # Mapping Mode Values to the DataFrame
    df['Mode'] = df['Month'].map(monthly_precip_type)

    # Define function to categorize wind speed
    def categorize_wind_speed(speed_kmh):
        speed = speed_kmh / 3.6
        if 0 <= speed <= 1.5:
            return 'Calm'
        elif 1.6 <= speed <= 3.3:
            return 'Light Air'
        elif 3.4 <= speed <= 5.4:
            return 'Light Breeze'
        elif 5.5 <= speed <= 7.9:
            return 'Gentle Breeze'
        elif 8.0 <= speed <= 10.7:
            return 'Moderate Breeze'
        elif 10.8 <= speed <= 13.8:
            return 'Fresh Breeze'
        elif 13.9 <= speed <= 17.1:
            return 'Strong Breeze'
        elif 17.2 <= speed <= 20.7:
            return 'Near Gale'
        elif 20.8 <= speed <= 24.4:
            return 'Gale'
        elif 24.5 <= speed <= 28.4:
            return 'Strong Gale'
        elif 28.5 <= speed <= 32.6:
            return 'Storm'
        elif speed >= 32.7:
            return 'Violent Storm'

    # Using 'categorize_wind_speed' function and write the value to new Wind Strength column.
    df['Wind Strength'] = df['Wind Speed (km/h)'].apply(categorize_wind_speed)

    # Calculating Monthly averages
    monthly_avg_temperature = df.groupby('Month')['Temperature (C)'].mean()
    monthly_avg_apparent_temperature = df.groupby('Month')['Apparent Temperature (C)'].mean()
    monthly_avg_humidity = df.groupby('Month')['Humidity'].mean()
    monthly_avg_visibility = df.groupby('Month')['Visibility (km)'].mean()
    monthly_avg_pressure = df.groupby('Month')['Pressure (millibars)'].mean()

    daily_weather = df[['id', 'Formatted Date', 'Temperature (C)', 'Apparent Temperature (C)', 'Humidity',
                        'Wind Speed (km/h)', 'Visibility (km)', 'Pressure (millibars)', 'Wind Strength',
                        'Avg Temperature (C)', 'Avg Humidity', 'Avg Wind Speed (km/h)']]

    monthly_weather = pd.DataFrame({
        'id': range(1, len(monthly_avg_temperature) + 1),
        'Month': monthly_avg_temperature.index,
        'Avg Temperature (C)': monthly_avg_temperature.values,
        'Avg Apparent Temperature (C)': monthly_avg_apparent_temperature.values,
        'Avg Humidity': monthly_avg_humidity.values,
        'Avg Visibility (km)': monthly_avg_visibility.values,
        'Avg Pressure (millibars)': monthly_avg_pressure.values,
        'Mode Precip Type': monthly_precip_type.values
    })


    daily_weather.to_csv('daily_weather.csv', index=False)
    monthly_weather.to_csv('monthly_weather.csv', index=False)

    daily_weather_file_path = 'tmp/daily_weather.csv'
    monthly_weather_file_path = 'tmp/monthly_weather.csv'

    kwargs['ti'].xcom_push(key='daily_weather_csv_file_path', value=daily_weather_file_path)
    kwargs['ti'].xcom_push(key='monthly_weather_csv_file_path', value=monthly_weather_file_path)


