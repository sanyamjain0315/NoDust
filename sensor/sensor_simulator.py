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

# ------------------------------------------------------------------
# Configuration (environment variables with sensible defaults)
# ------------------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "site-sensor-raw")

NUM_SITES = int(os.getenv("NUM_SITES", "5"))
SITE_IDS = [f"SITE_{i:02d}" for i in range(1, NUM_SITES + 1)]

SLEEP_MS = int(os.getenv("SLEEP_MS", "800"))  # mean ms between msgs
STDDEV_MS = int(os.getenv("SLEEP_STDDEV_MS", "200"))  # jitter
EVENT_TIMESTAMP_RATE_MS = int(
    os.getenv("EVENT_TIMESTAMP_RATE_MS", "1000")
)  # In seconds

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

# Define baseline vs elevated ranges for realism
POLLUTANTS = {
    "PM2.5": {
        "base_min": 5,
        "base_max": 25,
        "spike_min": 80,
        "spike_max": 150,
    },
    "NO2": {
        "base_min": 10,
        "base_max": 40,
        "spike_min": 180,
        "spike_max": 350,
    },
    "SO2": {"base_min": 5, "base_max": 20, "spike_min": 100, "spike_max": 250},
}

# ------------------------------------------------------------------
# State Management for Realistic Spikes
# ------------------------------------------------------------------
# Track the current state of each site to simulate continuity
# Structure: { "SITE_01": { "PM2.5": { "state": "NORMAL", "current_val": 12.5, "steps_left": 0 } } }
SITE_STATES = {}


def initialize_states():
    for site in SITE_IDS:
        SITE_STATES[site] = {}
        for pollutant, config in POLLUTANTS.items():
            SITE_STATES[site][pollutant] = {
                "state": "NORMAL",
                "current_val": random.uniform(
                    config["base_min"], config["base_max"]
                ),
                "steps_left": 0,
                "target_val": 0.0,
            }


def update_site_concentration(site, pollutant):
    state_info = SITE_STATES[site][pollutant]
    config = POLLUTANTS[pollutant]

    current = state_info["current_val"]
    state = state_info["state"]

    if state == "NORMAL":
        # Small random walk around the baseline
        current += random.uniform(-2, 2)
        current = max(config["base_min"], min(current, config["base_max"]))

        # 1% chance a site triggers a pollution event (spike)
        if random.random() < 0.01:
            state_info["state"] = "CLIMBING"
            state_info["target_val"] = random.uniform(
                config["spike_min"], config["spike_max"]
            )
            # It takes 5 to 15 steps (messages) to reach the peak
            state_info["steps_left"] = random.randint(5, 15)

    elif state == "CLIMBING":
        # Gradually step up to the target high value
        steps = state_info["steps_left"]
        target = state_info["target_val"]
        current += (target - current) / steps

        state_info["steps_left"] -= 1
        if state_info["steps_left"] <= 0:
            state_info["state"] = "SUSTAINING"
            # Sustain the high level for 20 to 50 readings
            state_info["steps_left"] = random.randint(20, 50)

    elif state == "SUSTAINING":
        # Flucluate slightly while remaining high
        current += random.uniform(-5, 5)
        current = max(config["spike_min"], min(current, config["spike_max"]))

        state_info["steps_left"] -= 1
        if state_info["steps_left"] <= 0:
            state_info["state"] = "FALLING"
            state_info["target_val"] = random.uniform(
                config["base_min"], config["base_max"]
            )
            # It takes 10 to 25 steps to cool down back to normal
            state_info["steps_left"] = random.randint(10, 25)

    elif state == "FALLING":
        # Gradually step down to the baseline
        steps = state_info["steps_left"]
        target = state_info["target_val"]
        current -= (current - target) / steps

        state_info["steps_left"] -= 1
        if state_info["steps_left"] <= 0:
            state_info["state"] = "NORMAL"

    # Save the updated value
    state_info["current_val"] = round(current, 2)
    return state_info["current_val"]


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
        "unit": "µg/m³",
    }


def jitter_sleep():
    wait = max(0.001, random.gauss(SLEEP_MS, STDDEV_MS) / 1000.0)
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
