from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

PG_HOST = "pgdatabase"  # Match container name
PG_USER = "root"
PG_PASSWORD = "root"
PG_PORT = "5432" # Container internal port
PG_DATABASE = "ny_taxi"

TAXI_TYPES = ['yellow', 'green']

SCHEMA_MAP = {
    'yellow': """
        CREATE TABLE IF NOT EXISTS yellow_taxi_data (
            vendor_id TEXT,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            ratecode_id TEXT,
            store_and_fwd_flag TEXT,
            pu_location_id TEXT,
            do_location_id TEXT,
            payment_type INTEGER,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION
        );
    """,
    'green': """
        CREATE TABLE IF NOT EXISTS green_taxi_data (
            vendor_id TEXT,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag TEXT,
            ratecode_id TEXT,
            pu_location_id TEXT,
            do_location_id TEXT,
            passenger_count INTEGER,
            trip_distance DOUBLE PRECISION,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            ehail_fee DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            payment_type INTEGER,
            trip_type INTEGER,
            congestion_surcharge DOUBLE PRECISION
        );
    """
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,  # Ensures sequential execution
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='taxi_data_postgres_upload',
    schedule_interval='@monthly',
    start_date=datetime(2018, 12, 1),
    end_date=datetime(2021, 7, 31),
    catchup=True,
    max_active_runs=1,  # Only one DAG run at a time
    concurrency=1,  # Allow only one task of yellow or green to run in parallel
    default_args=default_args
) as dag:
    
    for taxi_type in TAXI_TYPES:
        create_table_task = PostgresOperator(
        task_id=f'create_{taxi_type}_taxi_table',
        postgres_conn_id='postgres_default',
        sql=SCHEMA_MAP[taxi_type]
        )

        load_task = BashOperator(
            task_id=f'load_{taxi_type}_taxi_data',
            bash_command=f"""
                wget --tries=3 --timeout=60 -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{taxi_type}/{taxi_type}_tripdata_{{{{ execution_date.strftime('%Y-%m') }}}}.csv.gz | \
                gunzip | \
            psql postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE} \
            -c "\copy {taxi_type}_taxi_data FROM STDIN CSV HEADER;" && \
            touch /opt/airflow/data/{taxi_type}_{{{{ execution_date.strftime('%Y-%m') }}}}.txt
        """,
        retries=3
        )

        create_table_task >> load_task