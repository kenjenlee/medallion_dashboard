from prefect import flow, task
from ingest_data import fetch_meteo_data
from validate_gx import validate_data
from load_databricks import load_data

@task
def ingest():
    return fetch_meteo_data()

@task
def validate():
    return validate_data()

@task
def load():
    return load_data()

@flow(name='meteo_gx_databricks_pipeline')
def pipeline():
    ingest()
    if validate():
        load()
    else:
        print('Validation failed. Data not loaded to databricks.')

if __name__=="__main__":
    pipeline()