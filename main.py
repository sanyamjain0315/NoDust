import os

from system.dataflow import run_dataflow


def main():
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "site-sensor-raw")
    OUTPUT_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
    ALGORITHM = os.getenv("ALGORITHM", "Thresholding")
    run_dataflow(
        "SensorAnomalyDetection",
        BOOTSTRAP_SERVERS,
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        ALGORITHM,
    )


if __name__ == "__main__":
    main()
