from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from airflow.hooks.base import BaseHook

PROJECT_ID = "de-zoomcamp25-448205"
BIGQUERY_DATASET = "ny_taxi"

@dlt.resource(name="rides")
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page


def run_dlt_pipeline():
    conn = BaseHook.get_connection("google_cloud_default")
    
    # Set credentials from Airflow connection
    os.environ["DESTINATION__BIGQUERY__CREDENTIALS"] = conn.extra
    
    pipeline = dlt.pipeline(
        pipeline_name='taxi_data_pipeline',
        destination='bigquery',
        dataset_name=BIGQUERY_DATASET
    )
    pipeline.run(ny_taxi)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dlt_taxi_data_load',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=2 
) as dag:

    load_task = PythonOperator(
        task_id='load_taxi_data',
        python_callable=run_dlt_pipeline
    )
    
    load_task