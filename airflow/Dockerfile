FROM apache/airflow:2.8.1-python3.11

USER root

# Install only essential build dependencies (removed unused packages)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        libffi-dev \
        wget \
        netcat-openbsd \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements first for better layer caching
COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
