FROM apache/airflow:2.8.1

#USER airflow
#RUN pip install apache-airflow-providers-google==10.3.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install dbt-core dbt-bigquery
    
USER airflow