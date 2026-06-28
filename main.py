import argparse
import os

from system import anomaly_dataflow, metrics_dataflow


def main():
    parser = argparse.ArgumentParser(description="Running pyspark dataflows")

    parser.add_argument(
        "--mode",
        help="Either 'anomaly' or 'metrics' mode",
        choices=["anomaly", "metrics"],
        default="anomaly",
    )
    args = parser.parse_args()

    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "site-sensor-raw")

    if args.mode == "anomaly":
        OUTPUT_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
        ALGORITHM = os.getenv("ALGORITHM", "Thresholding")

        print(f"Starting Anomaly Dataflow using {ALGORITHM} algorithm...")
        anomaly_dataflow.run_dataflow(
            appname="SensorAnomalyDetection",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
            output_topic=OUTPUT_TOPIC,
            algorithm_str=ALGORITHM,
        )

    elif args.mode == "metrics":
        print("Starting Metrics Dataflow...")
        metrics_dataflow.run_dataflow(
            appname="SensorMetricsAggregation",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
        )


if __name__ == "__main__":
    main()
