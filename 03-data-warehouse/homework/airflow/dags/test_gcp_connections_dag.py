from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT_ID = "de-zoomcamp25-448205"
BUCKET = "terraform-demo-bucket-de-zoomcamp25-448205"
DATASET = "ny_taxi"

def test_gcs_connection():
    """Test GCS connection and bucket access"""
    hook = GCSHook()
    try:
        hook.list(BUCKET)
        return "GCS Connection: OK"
    except Exception as e:
        raise Exception(f"GCS Connection Failed: {str(e)}")

def test_bigquery_connection():
    """Test BigQuery connection and dataset access"""
    hook = BigQueryHook()
    try:
        hook.get_client().get_dataset(DATASET)
        return "BigQuery Connection: OK"
    except Exception as e:
        raise Exception(f"BigQuery Connection Failed: {str(e)}")

with DAG(
    'test_gcp_connections',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    test_gcs = PythonOperator(
        task_id='test_gcs_connection',
        python_callable=test_gcs_connection
    )

    test_bq = PythonOperator(
        task_id='test_bigquery_connection',
        python_callable=test_bigquery_connection
    )

    test_gcs >> test_bq