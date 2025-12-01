import pandas as pd
import os
from dotenv import load_dotenv
from databricks import sql
from config.settings import DOTENV_PATH, DATA_DIR, DATA_FILENAME

def load_data():
    load_dotenv(dotenv_path=DOTENV_PATH)
    
    df = pd.read_csv(os.path.join(DATA_DIR, DATA_FILENAME))

    db_con = sql.connect(
        server_hostname=os.getenv('DB_SERVER_HOSTNAME')
        , http_path=os.getenv('DB_HTTP_PATH')
        , access_token=os.getenv('DB_ACCESS_TOKEN')
    )
    cursor = db_con.cursor()
    cursor.execute("""
            DELETE FROM meteo_db.meteo_weather;""")
    
    for _, row in df.iterrows():
        
        cursor.execute("""
            INSERT INTO meteo_db.meteo_weather
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
    print('Data loaded to databricks')

if __name__=="__main__":
    load_data()