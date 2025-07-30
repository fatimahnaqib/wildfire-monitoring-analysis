FROM apache/airflow:2.8.1-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        build-essential \
        default-libmysqlclient-dev \
        libpq-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        libkrb5-dev \
        curl \
        netcat-openbsd \
        vim \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
