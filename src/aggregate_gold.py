import pandas as pd
import os
from dotenv import load_dotenv
from databricks import sql
from config.settings import DOTENV_PATH, DATA_DIR, DATA_FILENAME

def load_silver():
    load_dotenv(dotenv_path=DOTENV_PATH)
    
    db_con = sql.connect(
        server_hostname=os.getenv('DB_SERVER_HOSTNAME')
        , http_path=os.getenv('DB_HTTP_PATH')
        , access_token=os.getenv('DB_ACCESS_TOKEN')
    )
    cursor = db_con.cursor()
    cursor.execute("""
        select * from meteo_db.weather_silver
    """)
    df = pd.DataFrame(
        cursor.fetchall()
        , columns=[
            "date"
            ,"temperature_2m"
            ,"precipitation"
            ,"relative_humidity_2m"
            ,"apparent_temperature"
            ,"visibility"
            ,"cloud_cover"
        ]
    )
    cursor.close()
    db_con.close()
    print('Silver data loading done')
    return df

def write_gold(df):
    load_dotenv(dotenv_path=DOTENV_PATH)
    
    db_con = sql.connect(
        server_hostname=os.getenv('DB_SERVER_HOSTNAME')
        , http_path=os.getenv('DB_HTTP_PATH')
        , access_token=os.getenv('DB_ACCESS_TOKEN')
    )
    cursor = db_con.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meteo_db.weather_summary_gold (
            date DATE
            ,avg_temperature DOUBLE
            ,std_temperature DOUBLE
            ,avg_apparent_temp_diff DOUBLE
            ,avg_precipitation DOUBLE
            ,avg_humidity DOUBLE
            ,avg_visibility DOUBLE
            ,avg_cloud_cover DOUBLE
        ) USING DELTA
    """)
    cursor.execute("""DELETE FROM meteo_db.weather_summary_gold""")

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO meteo_db.weather_summary_gold
            (date
            ,avg_temperature
            ,std_temperature
            ,avg_apparent_temp_diff
            ,avg_precipitation 
            ,avg_humidity 
            ,avg_visibility 
            ,avg_cloud_cover)
            VALUES (?,?,?,?,?,?,?,?);
        """, (
            row['date_only']
            , row['avg_temperature']
            , row['std_temperature']
            , row['avg_apparent_temp_diff']
            , row['avg_precipitation']
            , row['avg_humidity']
            , row['avg_visibility']
            , row['avg_cloud_cover']
        ))

    db_con.commit()
    cursor.close()
    db_con.close()
    print('Gold data writing done')
    return df

def aggregate(df:pd.DataFrame):
    df['date_only'] = pd.to_datetime(df['date']).dt.date
    df['apparent_temp_diff'] = df['apparent_temperature'] - df['temperature_2m']

    df_summary= df.groupby('date_only').agg(
        avg_temperature=("temperature_2m", "mean")
        , std_temperature=("temperature_2m", "std")
        , avg_apparent_temp_diff=("apparent_temp_diff", "mean")
        , avg_precipitation=("precipitation", "mean")
        , avg_humidity=("relative_humidity_2m", "mean")
        , avg_visibility=("visibility", "mean")
        , avg_cloud_cover=("cloud_cover", "mean")
    ).reset_index()
    return df_summary

def gold_pipeline(local_csv=False):
    if local_csv:
        df = pd.read_csv(os.path.join(
            DATA_DIR
            , DATA_FILENAME))
    else:
        df=load_silver()
    
    write_gold(
        aggregate(df))


if __name__=="__main__":
    gold_pipeline()