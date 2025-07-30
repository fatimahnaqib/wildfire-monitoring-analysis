from confluent_kafka import Consumer, KafkaException
import json
import psycopg2
import logging
import sys

# Kafka Configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC_NAME = 'wildfire_data'
GROUP_ID = 'wildfire_group'

# PostgreSQL Configuration
PG_CONFIG = {
    'dbname': 'wildfire_db',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}

def connect_to_postgres():
    try:
        return psycopg2.connect(**PG_CONFIG)
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")
        return None

def normalize_acq_time(acq_time_raw):
    """Convert raw acquisition time like '939' to '09:39:00'."""
    acq_time_str = str(acq_time_raw).zfill(4)  # Pad to 4 digits
    hour = acq_time_str[:2]
    minute = acq_time_str[2:]
    return f"{hour}:{minute}:00"

def insert_into_db(data):
    conn = connect_to_postgres()
    if not conn:
        return

    try:
        cursor = conn.cursor()

        # Normalize acquisition time to HH:MM:SS
        data['acq_time'] = normalize_acq_time(data['acq_time'])

        insert_query = """
        INSERT INTO wildfire_events (
            latitude, longitude, bright_ti4, bright_ti5, scan, track,
            acq_date, acq_time, satellite, confidence, version, frp, daynight
        ) VALUES (
            %(latitude)s, %(longitude)s, %(bright_ti4)s, %(bright_ti5)s, %(scan)s, %(track)s,
            %(acq_date)s, %(acq_time)s, %(satellite)s, %(confidence)s, %(version)s, %(frp)s, %(daynight)s
        )
        ON CONFLICT (latitude, longitude, acq_date, acq_time, satellite) DO NOTHING
        """
        cursor.execute(insert_query, data)
        conn.commit()

        if cursor.rowcount == 0:
            logging.info("Duplicate record skipped (already exists)")
        else:
            logging.info("Inserted record into PostgreSQL")

    except Exception as e:
        logging.error(f"Error inserting into database: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def consume_messages():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    try:
        consumer.subscribe([TOPIC_NAME])
        logging.info(f"Kafka consumer started. Listening to '{TOPIC_NAME}' topic...")

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            try:
                data = json.loads(msg.value().decode('utf-8'))
                insert_into_db(data)
            except json.JSONDecodeError as e:
                logging.error(f"Failed to decode JSON: {e}")
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user")
    finally:
        consumer.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consume_messages()
