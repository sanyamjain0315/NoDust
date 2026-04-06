import os
import logging
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

consumer = KafkaConsumer(TOPIC)


def main():
  for msg in consumer:
      print(msg)

if __name__=="__main__":
   main()
