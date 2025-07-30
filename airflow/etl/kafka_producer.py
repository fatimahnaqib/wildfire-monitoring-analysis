import csv
import json
import logging
from confluent_kafka import Producer
from .config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, OUTPUT_FILE

def delivery_report(err, msg):
    if err:
        logging.warning(f"Kafka delivery failed: {err}")
    else:
        logging.info(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def produce_to_kafka():
    producer = Producer({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})
    logging.info("Starting Kafka producer...")

    try:
        with open(OUTPUT_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                producer.produce(KAFKA_TOPIC, value=json.dumps(row), callback=delivery_report)
                producer.poll(0)

        producer.flush()
        logging.info("All records sent to Kafka topic")
    except Exception as e:
        logging.error(f"Kafka production error: {str(e)}")
        raise
