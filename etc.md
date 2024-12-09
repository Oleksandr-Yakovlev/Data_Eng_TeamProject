# Historical Weather Data ETL

## Overview

This project is a data engineering pipeline designed to automated processing historical weather data using Apache Airflow. The project includes modular Python scripts for data extraction, transformation, validation, and loading (ETL) into a structured database. 

The pipeline includes:

-   **Extraction**: Fetching data from Kaggle using Kaggle API.
-   **Transformation**: Cleaning and reshaping datasets for analysis.
-   **Validation**: Ensuring data quality and integrity.
-   **Loading**: Inserting processed data into a target database.

## Example input

| **Formatted Date**            | **Summary**   | **Precip Type** | **Temperature**   | **Apparent Temperature** | **Humidity** | **Wind Speed (km/h)** | **Wind Bearing (degrees)** | **Visibility (km)** | **Cloud Cover** | **Pressure (millibars)** | **Daily Summary**                 |
|-------------------------------|---------------|-----------------|-------------------|--------------------------|--------------|-----------------------|----------------------------|---------------------|-----------------|--------------------------|-----------------------------------|
| 2006-04-01 00:00:00.000 +0200 | Partly Cloudy | rain            | 9.472222222222221 | 7.3888888888888875       | 0.89         | 14.1197               | 251.0                      | 15.826300000000002  | 0.0             | 1015.13                  | Partly cloudy throughout the day. |
| 2006-04-01 01:00:00.000 +0200 | Partly Cloudy | rain            | 9.355555555555558 | 7.227777777777776        | 0.86         | 14.2646               | 259.0                      | 15.26300000000002   | 0.0             | 1015.63                  | Partly cloudy throughout the day. |

## Example output

Daily Weather

| id | formatted_date | avg_temperature_c | avg_apparent_temperature_c | avg_humidity      | avg_wing_speed_kmh | avg_visibility_km | avg_pressure_millibars | wind_strength |
|----|----------------|-------------------|----------------------------|-------------------|--------------------|-------------------|------------------------|---------------|
| 0  | 2005-12-31     | 0.577777777777777 | -4.05                      | 0.89              | 17.1143            | 9.982             | 1016.66                | Light Breeze  |
| 1  | 2006-01-01     | 4.075             | -0.174537037037037         | 0.817083333333333 | 21.2291916666667   | 11.3484875        | 1011.985               | Gentle Breeze |

Monthly Weather

| id | month | avg_temperature_c | avg_apparent_temperature_c | avg_humidity      | avg_visibility_km | avg_pressure_millibars | mode_precip_type |
|----|-------|-------------------|----------------------------|-------------------|-------------------|------------------------|------------------|
| 0  | 1     | 0.815677609427609 | -1.93876058976865          | 0.8506977028348   | 7.83582514662757  | 1006.24522238514       | rain             |
| 1  | 2     | 2.16687954031202  | -0.555908360128617         | 0.813407020364416 | 8.73053561093248  | 1003.92512593783       | rain             |

## Features

-   **Modular design for reusability**: Each step of the pipeline is separated into reusable modules.
-    **Data Validation**: Ensures the data's accuracy and integrity by checking for anomalies, missing values, and consistency.
-   **Scalable Architecture**: Supports processing large datasets efficiently.
-   **Ease of Configuration**: Flexible source and target database connections.

## Architecture

  
```mermaid
graph LR
extract_task --> transform_task --> validate_task --> load_task
```

## Technologies Used

-   **Programming Language**: Python
-  **Orchestration Tool**: Apache Airflow
-   **Libraries**: Pandas, NumPy
-   **Database**: SQLite
-   **Version Control**: Git

## Setup

### Prerequisites

-   Python 3.9+
-   Virtual environment tool (e.g., venv, conda)
-   Database connection setup (sqlite3)
-   Kaggle's account and API key
