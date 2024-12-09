# Transform Module
import pandas as pd
import numpy as np


def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

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

    # Define function to categorize wind speed
    def categorize_wind_speed(speed_kmh):
        speed = round(speed_kmh / 3.6, 1)
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

    # Create daily weather table grouped by days with mean values
    daily_weather = df.groupby('Date').agg({
        'Temperature (C)': 'mean',
        'Apparent Temperature (C)': 'mean',
        'Humidity': 'mean',
        'Wind Speed (km/h)': 'mean',
        'Visibility (km)': 'mean',
        'Pressure (millibars)': 'mean',
    }).reset_index()

    # Rename columns
    daily_weather.rename(columns={
        'Date': 'formatted_date',
        'Temperature (C)': 'avg_temperature_c',
        'Apparent Temperature (C)': 'avg_apparent_temperature_c',
        'Humidity': 'avg_humidity',
        'Wind Speed (km/h)': 'avg_wind_speed_kmh',
        'Visibility (km)': 'avg_visibility_km',
        'Pressure (millibars)': 'avg_pressure_millibars',

    }, inplace=True)

    # Create new column 'wind_strength' and us 'categorize_wind_speed 'function to categorizing wind speed
    daily_weather['wind_strength'] = daily_weather['avg_wind_speed_kmh'].apply(categorize_wind_speed)

    # Create id column put it first position with values corresponding to the row index
    daily_weather.insert(loc=0, column='id', value=daily_weather.index)

    # Define a function to calculate monthly mode value
    def calculate_monthly_mode(series):
        mode = series.mode()
        return mode[0] if len(mode) == 1 else np.nan

    # Use 'calculate_monthly_mode' function
    monthly_precip_type = df.groupby('Month')['Precip Type'].agg(calculate_monthly_mode)

    # Calculating Monthly averages
    monthly_avg_temperature = df.groupby('Month')['Temperature (C)'].mean()
    monthly_avg_apparent_temperature = df.groupby('Month')['Apparent Temperature (C)'].mean()
    monthly_avg_humidity = df.groupby('Month')['Humidity'].mean()
    monthly_avg_visibility = df.groupby('Month')['Visibility (km)'].mean()
    monthly_avg_pressure = df.groupby('Month')['Pressure (millibars)'].mean()

    # Creating monthly_weather dataframe and apply monthly averages values to it
    monthly_weather = pd.DataFrame({
        'month': monthly_avg_temperature.index,
        'avg_temperature_c': monthly_avg_temperature.values,
        'avg_apparent_temperature_c': monthly_avg_apparent_temperature.values,
        'avg_humidity': monthly_avg_humidity.values,
        'avg_visibility_km': monthly_avg_visibility.values,
        'avg_pressure_millibars': monthly_avg_pressure.values,
        'mode_precip_type': monthly_precip_type.values
    })

    # Create id column put it first position with values corresponding to the row index
    monthly_weather.insert(loc=0, column='id', value=monthly_weather.index)

    # Set files paths
    daily_weather_file_path = '/tmp/daily_weather.csv'
    monthly_weather_file_path = '/tmp/monthly_weather.csv'

    # Save files in csv format without indexes
    daily_weather.to_csv(daily_weather_file_path, index=False)
    monthly_weather.to_csv(monthly_weather_file_path, index=False)

    # Pass files paths to xcom
    kwargs['ti'].xcom_push(key='daily_weather_csv_file_path', value=daily_weather_file_path)
    kwargs['ti'].xcom_push(key='monthly_weather_csv_file_path', value=monthly_weather_file_path)
