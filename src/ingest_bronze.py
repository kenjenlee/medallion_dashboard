import os
import pandas as pd
from config.settings import DATA_DIR, DATA_FILENAME, DOTENV_PATH, FORECAST_NDAYS
from dotenv import load_dotenv
from databricks import sql

# yfinance is buggy as of writing
def fetch_yfinance_data(stock_ticker='NVDA'):
    import yfinance as yf
    from datetime import datetime, timedelta

    df = yf.download(
        stock_ticker
        , start=datetime.now() - timedelta(days=1)
        , end=datetime.now()
        , interval='1h'
    )
    df.columns = df.columns.get_level_values(0)
    df.to_csv(os.path.join(
        DATA_DIR
        , DATA_FILENAME
        ), index=True)
    return df


def fetch_meteo_data(save_local_copy=False):
    import openmeteo_requests
    import requests_cache
    import pandas as pd
    from retry_requests import retry

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.52,
        "longitude": 13.41,
        "hourly": ["temperature_2m", "precipitation", "relative_humidity_2m", "apparent_temperature", "visibility", "cloud_cover"],
        "timezone": "Asia/Singapore",
        "forecast_days": FORECAST_NDAYS,
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation: {response.Elevation()} m asl")
    print(f"Timezone: {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_visibility = hourly.Variables(4).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()

    # type is datetime64[ns, UTC]
    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end =  pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["visibility"] = hourly_visibility
    hourly_data["cloud_cover"] = hourly_cloud_cover

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    if save_local_copy:
        hourly_dataframe.to_csv(os.path.join(
            DATA_DIR
            , DATA_FILENAME
        ), index=False)
        print(f'Data downloaded to {DATA_FILENAME}')
    
    print('Data ingestion done')
    return hourly_dataframe

# write to databricks
def write_bronze(df):
    load_dotenv(dotenv_path=DOTENV_PATH)
    
    db_con = sql.connect(
        server_hostname=os.getenv('DB_SERVER_HOSTNAME')
        , http_path=os.getenv('DB_HTTP_PATH')
        , access_token=os.getenv('DB_ACCESS_TOKEN')
    )
    cursor = db_con.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meteo_db.weather_bronze (
            date DATE
            ,temperature_2m DOUBLE
            ,precipitation DOUBLE
            ,relative_humidity_2m DOUBLE
            ,apparent_temperature DOUBLE
            ,visibility DOUBLE
            ,cloud_cover DOUBLE
        ) USING DELTA
    """)
    cursor.execute("""DELETE FROM meteo_db.weather_bronze""")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO meteo_db.weather_bronze
            (date,temperature_2m,precipitation
            ,relative_humidity_2m,apparent_temperature
            ,visibility,cloud_cover)
            VALUES (?,?,?,?,?,?,?);
        """, (
            row['date']
            , row['temperature_2m']
            , row['precipitation']
            , row['relative_humidity_2m']
            , row['apparent_temperature']
            , row['visibility']
            , row['cloud_cover']
        ))

    db_con.commit()
    cursor.close()
    db_con.close()
    print('Bronze data writing done')
    return df


def bronze_pipeline(local_csv=False):
    df = fetch_meteo_data(local_csv)
    if not local_csv:
        write_bronze(df)

if __name__ == "__main__":
    bronze_pipeline()
