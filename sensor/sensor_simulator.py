#!/usr/bin/env python3
"""
A sensor simulation that emits JSON measurements to a Kafka topic.
Simulates realistic pollution events (spikes) that start, sustain,
and decay over time for specific sites.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")

NUM_SITES = int(os.getenv("NUM_SITES", "5"))
SITE_IDS = [f"SITE_{i:02d}" for i in range(1, NUM_SITES + 1)]

SLEEP_MS = int(os.getenv("SLEEP_MS", "800"))  # mean ms between msgs
STDDEV_MS = int(os.getenv("SLEEP_STDDEV_MS", "200"))  # jitter
EVENT_TIMESTAMP_RATE_MS = int(
    os.getenv("EVENT_TIMESTAMP_RATE_MS", "900000")
)  # In ms

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

# Unit value by which any sensor metric will go up or down
METRIC_MEAN = int(os.getenv("METRIC_MEAN", 0))
METRIC_STD_DEV = int(os.getenv("METRIC_STD_DEV", 1))

POLLUTANTS = {
    "PM2.5": {
        "min": 0,
        "max": 500,
        "unit": "µg/m³",
    },
    "PM10": {
        "min": 0,
        "max": 500,
        "unit": "µg/m³",
    },
    # "TEMPERATURE": {
    #     "min": 20,
    #     "max": 40,
    #     "unit": "ºC",
    # },
    # "RELATIVE_HUMIDITY": {
    #     "min": 0,
    #     "max": 100,
    #     "unit": "%",
    # },
}


SENSOR_STATES = {}


def initialize_states():
    for site_id in SITE_IDS:
        SENSOR_STATES[site_id] = {}
        for metric, limits in POLLUTANTS.items():
            SENSOR_STATES[site_id][metric] = {
                "value": random.uniform(limits["min"], limits["max"]),
            }


def update_site_concentration(site_id, pollutant):
    state = SENSOR_STATES[site_id][pollutant]

    # Changing the value by a few unit measurements
    new_value = state["value"] + random.randint(
        -METRIC_STD_DEV, METRIC_STD_DEV
    )

    # Clipping to min and max values
    if new_value < POLLUTANTS[pollutant]["min"]:
        new_value = POLLUTANTS[pollutant]["min"]
    elif new_value > POLLUTANTS[pollutant]["max"]:
        new_value = POLLUTANTS[pollutant]["max"]

    # Updating value
    SENSOR_STATES[site_id][pollutant] = {"value": new_value}
    return new_value


def make_message(timestamp: datetime):
    site = random.choice(SITE_IDS)
    pollutant = random.choice(list(POLLUTANTS.keys()))

    # Calculate next concentration based on state history
    concentration = update_site_concentration(site, pollutant)

    return {
        "sensor_id": str(uuid.uuid4()),
        "site_id": site,
        "timestamp": timestamp.isoformat(),
        "pollutant_type": pollutant,
        "concentration": concentration,
        "unit": POLLUTANTS[pollutant]["unit"],
    }


def jitter_sleep():
    wait = abs(random.gauss(SLEEP_MS, STDDEV_MS) / 1000.0)
    time.sleep(wait)


def main():
    logging.info(f"Starting sensor → {BOOTSTRAP_SERVERS}, topic={TOPIC}")
    logging.info(f"Simulating {NUM_SITES} sites: {SITE_IDS}")

    # Initialize state tracker before starting the loop
    initialize_states()

    start = datetime.now(timezone.utc)
    current = start
    while True:
        msg = make_message(current)
        current += timedelta(milliseconds=EVENT_TIMESTAMP_RATE_MS)
        key_bytes = msg["site_id"].encode("utf-8")
        try:
            producer.send(TOPIC, key=key_bytes, value=msg)
        except Exception as exc:
            logging.error(f"Kafka send failed: {exc}")
        if SLEEP_MS != 0 and STDDEV_MS != 0:
            jitter_sleep()


if __name__ == "__main__":
    main()
