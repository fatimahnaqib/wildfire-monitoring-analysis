# create_topic.py
from confluent_kafka.admin import AdminClient, NewTopic

admin_conf = {"bootstrap.servers": "kafka:9092"}
admin_client = AdminClient(admin_conf)

topic_name = "wildfire_data"

existing_topics = admin_client.list_topics(timeout=5).topics
if topic_name not in existing_topics:
    new_topic = NewTopic(topic=topic_name, num_partitions=1, replication_factor=1)
    fs = admin_client.create_topics([new_topic])
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
else:
    print(f"Topic '{topic_name}' already exists")
