import os
from kafka import KafkaConsumer

TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
consumer.subscribe([TOPIC])
for msg in consumer:
    print (msg)
