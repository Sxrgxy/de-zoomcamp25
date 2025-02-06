from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

PG_HOST = "pgdatabase"
PG_USER = "root"
PG_PASSWORD = "root"
PG_PORT = "5432"
PG_DATABASE = "ny_taxi"

with DAG(
    'load_jan_2019_taxi_data',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None  # one-time run
) as dag:

    copy_files = BashOperator(
    task_id='copy_csv_files',
    bash_command="""
        cp /home/vologzhaninsergey23/data-engineering-zoomcamp/02-workflow-orchestration/homework/airflow/yellow_2019-01.csv /opt/airflow/data/ &&
        cp /home/vologzhaninsergey23/data-engineering-zoomcamp/02-workflow-orchestration/homework/airflow/green_2019-01.csv /opt/airflow/data/
        """
    )

    load_yellow = BashOperator(
        task_id='load_yellow_jan_2019',
        bash_command=f"""
            psql postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE} \
            -c "\copy yellow_taxi_data FROM '/opt/airflow/data/yellow_2019-01.csv' CSV HEADER;"
        """
    )

    load_green = BashOperator(
        task_id='load_green_jan_2019',
        bash_command=f"""
            psql postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE} \
            -c "\copy green_taxi_data FROM '/opt/airflow/data/green_2019-01.csv' CSV HEADER;"
        """
    )

    # Task dependencies
    copy_files >> [load_yellow, load_green]