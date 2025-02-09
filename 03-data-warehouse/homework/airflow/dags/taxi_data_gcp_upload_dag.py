from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator
)
import requests

PROJECT_ID = "de-zoomcamp25-448205"
BUCKET = "terraform-demo-bucket-de-zoomcamp25-448205"
BIGQUERY_DATASET = "ny_taxi"

taxi_type = 'yellow'

SCHEMA_MAP = {
    'yellow': [
        {'name': 'unique_row_id', 'type': 'BYTES', 'description': 'Unique identifier for deduplication'},
        {'name': 'filename', 'type': 'STRING', 'description': 'Source filename'},
        {'name': 'VendorID', 'type': 'STRING', 'description': 'A code indicating the provider. 1=Creative Mobile Technologies, 2=VeriFone'},
        {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP', 'description': 'Pickup date and time'},
        {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP', 'description': 'Dropoff date and time'},
        {'name': 'passenger_count', 'type': 'INTEGER', 'description': 'Number of passengers'},
        {'name': 'trip_distance', 'type': 'NUMERIC', 'description': 'Trip distance in miles'},
        {'name': 'RatecodeID', 'type': 'STRING', 'description': 'Rate code: 1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group'},
        {'name': 'store_and_fwd_flag', 'type': 'STRING', 'description': 'Store and forward flag'},
        {'name': 'PULocationID', 'type': 'STRING', 'description': 'Pickup location ID'},
        {'name': 'DOLocationID', 'type': 'STRING', 'description': 'Dropoff location ID'},
        {'name': 'payment_type', 'type': 'INTEGER', 'description': 'Payment type: 1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided'},
        {'name': 'fare_amount', 'type': 'NUMERIC', 'description': 'Time and distance fare'},
        {'name': 'extra', 'type': 'NUMERIC', 'description': 'Extra charges'},
        {'name': 'mta_tax', 'type': 'NUMERIC', 'description': 'MTA tax'},
        {'name': 'tip_amount', 'type': 'NUMERIC', 'description': 'Tip amount'},
        {'name': 'tolls_amount', 'type': 'NUMERIC', 'description': 'Tolls amount'},
        {'name': 'improvement_surcharge', 'type': 'NUMERIC', 'description': 'Improvement surcharge'},
        {'name': 'total_amount', 'type': 'NUMERIC', 'description': 'Total amount'},
        {'name': 'congestion_surcharge', 'type': 'NUMERIC', 'description': 'Congestion surcharge'}
    ]
}

def stream_to_gcs(taxi_type, execution_date, bucket):
    """Stream data directly from source to GCS"""
    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{execution_date}.parquet'
    blob_name = f'raw/{taxi_type}/{taxi_type}_tripdata_{execution_date}.parquet'
    
    gcs_hook = GCSHook()
    response = requests.get(url, stream=True)
    response.raise_for_status()

    try:
        gcs_hook.upload(
            bucket_name=bucket,
            object_name=blob_name,
            data=response.raw.read(),
            mime_type='application/octet-stream'
        )
    except Exception as e:
        raise Exception(f"Upload to GCS failed: {str(e)}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 6, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'taxi_data_gcp_upload',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=2 
) as dag:

    upload_to_gcs = PythonOperator(
        task_id=f'upload_{taxi_type}_to_gcs',
        python_callable=stream_to_gcs,
        op_kwargs={
            'taxi_type': taxi_type,
            'execution_date': '{{ execution_date.strftime("%Y-%m") }}',
            'bucket': BUCKET
        }
    )
    
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id=f'create_external_{taxi_type}',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': f'{taxi_type}_tripdata_external_{{{{ execution_date.strftime("%Y_%m") }}}}'
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/raw/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.parquet'],
                'autodetect': True, #'schema': {'fields': [field for field in SCHEMA_MAP[taxi_type] if field['name'] not in ['unique_row_id', 'filename']]},
                'location': 'US'
            }
        }
    )

    upload_to_gcs >> create_external_table