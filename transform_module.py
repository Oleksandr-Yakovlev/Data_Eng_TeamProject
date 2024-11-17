#Transform Module
import pandas as pd
def transform_data(**kwargs):
    # Retrieve file path from XCom
    file_path = kwargs['ti'].xcom_pull(key='csv_file_path')
    df = pd.read_csv(file_path)

    df['Formatted Date'] = pd.to_datetime(df['Formatted Date'], utc=True)
    df['Formatted Date'] = df['Formatted Date'].dt.strftime('%d-%m-%Y')

    # Handle missing values in critical columns
    critical_columns = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)']
    for column in critical_columns:
        df[column].fillna(df[column].median(), inplace=True)

    df['Date'] = df['Formatted Date'].dt.date
    df['Month'] = df['Formatted Date'].dt.month

    daily_avg_temperature = df.groupby('Date')['Temperature (C)'].mean()
    daily_avg_humidity = df.groupby('Date')['Humidity'].mean()
    daily_avg_wind_speed = df.groupby('Date')['Wind Speed (km/h)'].mean()

    monthly_precip_type = df.groupby(['Month'])['Precip Type'].agg(pd.Series.mode)

    df['Mode'] = monthly_precip_type