import os
from kafka import KafkaConsumer

SENSOR_TOPIC = os.environ.get('SENSOR_TOPIC', "sensor")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 
                                         "localhost:9092")

consumer = KafkaConsumer(SENSOR_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

for msg in consumer:
    print (msg)
