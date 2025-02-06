from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

TAXI_TYPES = ['yellow', 'green']

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Ensures sequential execution
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='taxi_data_local_upload',
    schedule_interval='@monthly',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 7, 31),
    catchup=True,
    max_active_runs=1,  # Only one DAG run at a time
    concurrency=1,  # Allow only one task of yellow or green to run in parallel
    default_args=default_args
) as dag:
    
    for taxi_type in TAXI_TYPES:
        download_task = BashOperator(
            task_id=f'download_{taxi_type}_taxi_data',
            bash_command=f"""
                wget --tries=3 --timeout=60 -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime('%Y-%m') }}}}.csv.gz | \
                gunzip > /opt/airflow/data/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime('%Y-%m') }}}}.csv
            """,
            retries=3
        )