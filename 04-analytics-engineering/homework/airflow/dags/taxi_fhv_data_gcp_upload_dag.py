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

SCHEMA_MAP = {
    'fhv': [
        {'name': 'unique_row_id', 'type': 'BYTES', 'description': 'Unique identifier for deduplication'},
        {'name': 'filename', 'type': 'STRING', 'description': 'Source filename'},
        {'name': 'dispatching_base_num', 'type': 'STRING', 'description': 'The TLC Base License Number of the base that dispatched the trip'},
        {'name': 'pickup_datetime', 'type': 'TIMESTAMP', 'description': 'The date and time of the pickup'},
        {'name': 'dropoff_datetime', 'type': 'TIMESTAMP', 'description': 'The date and time of the drop-off'},
        {'name': 'PULocationID', 'type': 'INTEGER', 'description': 'TLC Taxi Zone of the pickup'},
        {'name': 'DOLocationID', 'type': 'INTEGER', 'description': 'TLC Taxi Zone of the drop-off'},
        {'name': 'SR_Flag', 'type': 'INTEGER', 'description': 'Indicates if the trip was a shared ride'},
        {'name': 'Affiliated_base_number', 'type': 'STRING', 'description': 'The TLC Base License Number of the base affiliated with the trip'}
    ]
}

def stream_to_gcs(taxi_type, execution_date, bucket):
    """Stream data directly from source to GCS"""
    url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{execution_date}.csv.gz'
    blob_name = f'raw/{taxi_type}/{taxi_type}_tripdata_{execution_date}.csv.gz'
    
    gcs_hook = GCSHook()
    response = requests.get(url, stream=True)
    response.raise_for_status()

    try:
        gcs_hook.upload(
            bucket_name=bucket,
            object_name=blob_name,
            data=response.raw.read(),
            mime_type='application/gzip'
        )
    except Exception as e:
        raise Exception(f"Upload to GCS failed: {str(e)}")

def get_fhv_merge_query():
    return f"""
    MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.fhv_tripdata` T
    USING (
        SELECT 
            MD5(CONCAT(
                COALESCE(CAST(dispatching_base_num AS STRING), ''),
                COALESCE(CAST(pickup_datetime AS STRING), ''),
                COALESCE(CAST(dropoff_datetime AS STRING), ''),
                COALESCE(CAST(PULocationID AS STRING), ''),
                COALESCE(CAST(DOLocationID AS STRING), '')
            )) as unique_row_id,
            'fhv_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.csv.gz' as filename,
            *
        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.fhv_tripdata_external_{{{{ execution_date.strftime("%Y_%m") }}}}`
    ) S
    ON T.unique_row_id = S.unique_row_id
    WHEN NOT MATCHED THEN
    INSERT (unique_row_id, filename, dispatching_base_num, pickup_datetime, dropoff_datetime,
            PULocationID, DOLocationID, SR_Flag, Affiliated_base_number)
    VALUES (S.unique_row_id, S.filename, S.dispatching_base_num, S.pickup_datetime, S.dropoff_datetime,
            S.PULocationID, S.DOLocationID, S.SR_Flag, S.Affiliated_base_number)
    """

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2020, 12, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'taxi_fhv_data_gcp_upload',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    concurrency=2 
) as dag:

     for taxi_type in ['fhv']:
        upload_to_gcs = PythonOperator(
            task_id=f'upload_{taxi_type}_to_gcs',
            python_callable=stream_to_gcs,
            op_kwargs={
                'taxi_type': taxi_type,
                'execution_date': '{{ execution_date.strftime("%Y-%m") }}',
                'bucket': BUCKET
            }
        )

        create_partitioned_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_partitioned_{taxi_type}',
            project_id=PROJECT_ID,
            dataset_id=BIGQUERY_DATASET,
            table_id=f'{taxi_type}_tripdata',
            schema_fields=SCHEMA_MAP[taxi_type],
            time_partitioning={
                'type': 'DAY',
                'field': 'pickup_datetime'
            },
            exists_ok=True
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
                    'sourceFormat': 'CSV',
                    'compression': 'GZIP',
                    'csvOptions': {'skipLeadingRows': 1, 'allowJaggedRows': True},
                    'sourceUris': [f'gs://{BUCKET}/raw/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.csv.gz'],
                    'schema': {'fields': [field for field in SCHEMA_MAP[taxi_type] if field['name'] not in ['unique_row_id', 'filename']]},
                    'maxBadRecords': 0,
                    'location': 'US'
                }
            }
        )

        merge_data = BigQueryExecuteQueryOperator(
            task_id=f'merge_{taxi_type}_data',
            sql=get_fhv_merge_query(),
            use_legacy_sql=False,
            location='US'
        )

        upload_to_gcs >> create_partitioned_table >> create_external_table >> merge_data