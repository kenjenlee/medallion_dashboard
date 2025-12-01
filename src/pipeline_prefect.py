from prefect import flow, task
from ingest_bronze import bronze_pipeline
from validate_silver import silver_pipeline
from aggregate_gold import gold_pipeline

@task
def bronze(local_csv):
    return bronze_pipeline(local_csv)

@task
def silver(local_csv):
    return silver_pipeline(local_csv)

@task
def gold(local_csv):
    return gold_pipeline(local_csv)

@flow(name='meteo_gx_databricks_pipeline')
# local_csv: don't write to bronze and silver, just gold
def pipeline(local_csv=False):
    bronze(local_csv)
    if silver(local_csv):
        gold(local_csv)
    else:
        print('Validation failed. Gold not processed.')

if __name__=="__main__":
    pipeline()