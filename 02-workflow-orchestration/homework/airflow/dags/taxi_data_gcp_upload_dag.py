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
    ],
    'green': [
        {'name': 'unique_row_id', 'type': 'BYTES', 'description': 'Unique identifier for deduplication'},
        {'name': 'filename', 'type': 'STRING', 'description': 'Source filename'},
        {'name': 'VendorID', 'type': 'STRING', 'description': 'A code indicating the provider'},
        {'name': 'lpep_pickup_datetime', 'type': 'TIMESTAMP', 'description': 'Pickup date and time'},
        {'name': 'lpep_dropoff_datetime', 'type': 'TIMESTAMP', 'description': 'Dropoff date and time'},
        {'name': 'store_and_fwd_flag', 'type': 'STRING', 'description': 'Store and forward flag'},
        {'name': 'RatecodeID', 'type': 'STRING', 'description': 'Rate code'},
        {'name': 'PULocationID', 'type': 'STRING', 'description': 'Pickup location ID'},
        {'name': 'DOLocationID', 'type': 'STRING', 'description': 'Dropoff location ID'},
        {'name': 'passenger_count', 'type': 'INTEGER', 'description': 'Number of passengers'},
        {'name': 'trip_distance', 'type': 'NUMERIC', 'description': 'Trip distance in miles'},
        {'name': 'fare_amount', 'type': 'NUMERIC', 'description': 'Time and distance fare'},
        {'name': 'extra', 'type': 'NUMERIC', 'description': 'Extra charges'},
        {'name': 'mta_tax', 'type': 'NUMERIC', 'description': 'MTA tax'},
        {'name': 'tip_amount', 'type': 'NUMERIC', 'description': 'Tip amount'},
        {'name': 'tolls_amount', 'type': 'NUMERIC', 'description': 'Tolls amount'},
        {'name': 'ehail_fee', 'type': 'NUMERIC', 'description': 'E-hail fee'},
        {'name': 'improvement_surcharge', 'type': 'NUMERIC', 'description': 'Improvement surcharge'},
        {'name': 'total_amount', 'type': 'NUMERIC', 'description': 'Total amount'},
        {'name': 'payment_type', 'type': 'INTEGER', 'description': 'Payment type'},
        {'name': 'trip_type', 'type': 'STRING', 'description': '1=Street-hail, 2=Dispatch'},
        {'name': 'congestion_surcharge', 'type': 'NUMERIC', 'description': 'Congestion surcharge'}
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

def get_yellow_merge_query():
    return f"""
    MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.yellow_tripdata` T
    USING (
        SELECT 
            MD5(CONCAT(
                COALESCE(CAST(VendorID AS STRING), ''),
                COALESCE(CAST(tpep_pickup_datetime AS STRING), ''),
                COALESCE(CAST(tpep_dropoff_datetime AS STRING), ''),
                COALESCE(CAST(PULocationID AS STRING), ''),
                COALESCE(CAST(DOLocationID AS STRING), '')
            )) as unique_row_id,
            'yellow_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.csv.gz' as filename,
            *
        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.yellow_tripdata_external_{{{{ execution_date.strftime("%Y_%m") }}}}`
    ) S
    ON T.unique_row_id = S.unique_row_id
    WHEN NOT MATCHED THEN
    INSERT (unique_row_id, filename, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, 
            passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, 
            DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, 
            improvement_surcharge, total_amount, congestion_surcharge)
    VALUES (S.unique_row_id, S.filename, S.VendorID, S.tpep_pickup_datetime, S.tpep_dropoff_datetime, 
            S.passenger_count, S.trip_distance, S.RatecodeID, S.store_and_fwd_flag, S.PULocationID, 
            S.DOLocationID, S.payment_type, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, 
            S.improvement_surcharge, S.total_amount, S.congestion_surcharge)
    """

def get_green_merge_query():
    return f"""
    MERGE INTO `{PROJECT_ID}.{BIGQUERY_DATASET}.green_tripdata` T
    USING (
        SELECT 
            MD5(CONCAT(
                COALESCE(CAST(VendorID AS STRING), ''),
                COALESCE(CAST(lpep_pickup_datetime AS STRING), ''),
                COALESCE(CAST(lpep_dropoff_datetime AS STRING), ''),
                COALESCE(CAST(PULocationID AS STRING), ''),
                COALESCE(CAST(DOLocationID AS STRING), '')
            )) as unique_row_id,
            'green_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.csv.gz' as filename,
            *
        FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.green_tripdata_external_{{{{ execution_date.strftime("%Y_%m") }}}}`
    ) S
    ON T.unique_row_id = S.unique_row_id
    WHEN NOT MATCHED THEN
    INSERT (unique_row_id, filename, VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, 
            store_and_fwd_flag, RatecodeID, PULocationID, DOLocationID, passenger_count, 
            trip_distance, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, 
            improvement_surcharge, total_amount, payment_type, trip_type, congestion_surcharge)
    VALUES (S.unique_row_id, S.filename, S.VendorID, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, 
            S.store_and_fwd_flag, S.RatecodeID, S.PULocationID, S.DOLocationID, S.passenger_count, 
            S.trip_distance, S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee, 
            S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type, S.congestion_surcharge)
    """

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 1),
    'end_date': datetime(2021, 8, 1),
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

     for taxi_type in ['yellow', 'green']:
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
                'field': 'tpep_pickup_datetime' if taxi_type == 'yellow' else 'lpep_pickup_datetime'
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
                    'csvOptions': {'skipLeadingRows': 1},
                    'sourceUris': [f'gs://{BUCKET}/raw/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime("%Y-%m") }}}}.csv.gz'],
                    'schema': {'fields': [field for field in SCHEMA_MAP[taxi_type] if field['name'] not in ['unique_row_id', 'filename']]},
                    'location': 'US'
                }
            }
        )

        merge_data = BigQueryExecuteQueryOperator(
            task_id=f'merge_{taxi_type}_data',
            sql=get_yellow_merge_query() if taxi_type == 'yellow' else get_green_merge_query(),
            use_legacy_sql=False,
            location='US'
        )

        upload_to_gcs >> create_partitioned_table >> create_external_table >> merge_data