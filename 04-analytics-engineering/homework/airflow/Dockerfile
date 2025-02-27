FROM apache/airflow:2.8.1-python3.9

USER root

RUN apt-get update \
    # && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
      wget \
      git \
      ssh-client \
      software-properties-common \
      make \
      build-essential \
      ca-certificates \
    && apt-get clean \
    && rm -rf \
      /var/lib/apt/lists/* \
      /tmp/* \
      /var/tmp/*

USER airflow

# Environment setup
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Update python and install dbt-bigquery
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir
RUN pip install --no-cache-dir \
    jinja2==3.1.3 \
    markupsafe==2.1.3 \
    dbt-core==1.9.2 \
    dbt-bigquery==1.9.1
