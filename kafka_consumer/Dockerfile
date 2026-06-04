FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install dependencies first for better layer caching
COPY kafka_consumer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir -r /app/requirements.txt

# Copy application code (build context is repo root; see docker-compose.yml)
COPY kafka_consumer/create_topic.py /app/
COPY kafka_consumer/consumer.py /app/
COPY kafka_consumer/kafka_dlq.py /app/
COPY airflow/etl/transport_config.py /app/transport_config.py

CMD ["sh", "-c", "python create_topic.py && python consumer.py"]
