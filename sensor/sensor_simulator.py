#!/usr/bin/env python3
"""
A tiny sensor that emits JSON measurements to a Kafka topic.
Each container runs one instance, but we simulate many sensors
by randomising the `sensor_id` and `site_id` per message.
"""

import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone

from kafka import KafkaProducer

# ------------------------------------------------------------------
# Configuration (environment variables with sensible defaults)
# ------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")
SITE_IDS = os.getenv("SITE_IDS", "SITE_01").split(",")
SLEEP_MS = int(os.getenv("SLEEP_MS", "800"))          # mean ms between msgs
STDDEV_MS = int(os.getenv("SLEEP_STDDEV_MS", "200"))  # jitter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
    linger_ms=10,
)

POLLUTANTS = {
    "PM2.5": {"min": 5, "max": 120},
    "NO2":   {"min": 10, "max": 300},
    "SO2":   {"min": 8, "max": 200},
}


def random_conc(pollutant):
    cfg = POLLUTANTS[pollutant]
    return round(random.uniform(cfg["min"], cfg["max"]), 2)


def make_message():
    site = random.choice(SITE_IDS).strip()
    pollutant = random.choice(list(POLLUTANTS.keys()))
    return {
        "sensor_id": str(uuid.uuid4()),
        "site_id": site,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pollutant_type": pollutant,
        "concentration": random_conc(pollutant),
        "unit": "µg/m³",
    }


def jitter_sleep():
    # Gaussian jitter, never negative
    wait = max(0.1, random.gauss(SLEEP_MS, STDDEV_MS) / 1000.0)
    time.sleep(wait)


def main():
    logging.info(f"Starting sensor → {BOOTSTRAP_SERVERS}, topic={TOPIC}")
    while True:
        msg = make_message()
        # Use `site_id` as key so all msgs from a site are ordered in the same partition
        key_bytes = msg["site_id"].encode("utf-8")
        try:
            producer.send(TOPIC, key=key_bytes, value=msg)
            logging.debug(f"sent {msg}")
        except Exception as exc:
            logging.error(f"Kafka send failed: {exc}")
        jitter_sleep()
        # print(msg)


if __name__ == "__main__":
    main()
