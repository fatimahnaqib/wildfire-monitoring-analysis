# kafka-consumer/Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY create_topic.py .
COPY consumer.py .

# Create topic and then run consumer
CMD ["sh", "-c", "python create_topic.py && python consumer.py"]
