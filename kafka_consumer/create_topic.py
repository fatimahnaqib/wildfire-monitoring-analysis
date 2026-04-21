# create_topic.py
import os

from confluent_kafka.admin import AdminClient, NewTopic

from transport_config import kafka_common_client_config

bootstrap = os.getenv("KAFKA_BROKER", "localhost:9092")
admin_conf = {"bootstrap.servers": bootstrap, **kafka_common_client_config()}
admin_client = AdminClient(admin_conf)

topic_name = "wildfire_data"
replication_factor = int(os.getenv("KAFKA_TOPIC_REPLICATION_FACTOR", "1"))
num_partitions = int(os.getenv("KAFKA_LEGACY_TOPIC_PARTITIONS", "3"))

existing_topics = admin_client.list_topics(timeout=5).topics
if topic_name not in existing_topics:
    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
    )
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
else:
    print(f"Topic '{topic_name}' already exists")
