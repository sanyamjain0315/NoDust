import argparse
import os
import sys

import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from system import anomaly_dataflow, metrics_dataflow

load_dotenv()

KAFKA_SECURITY_OPTIONS = {
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "AWS_MSK_IAM",
    "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
}


def build_spark(appname: str) -> SparkSession:
    """Create the single shared SparkSession used by both streaming queries."""
    spark = (
        SparkSession.builder.appName(appname)
        .config(
            "spark.jars.packages",
            "com.amazonaws:aws-java-sdk:1.7.4, org.apache.hadoop:hadoop-aws:2.7.3",
        )
        .getOrCreate()
    )

    # Suppressing logging
    spark.sparkContext.setLogLevel("WARN")
    log4jLogger = spark._jvm.org.apache.log4j
    log_manager = log4jLogger.LogManager
    logger = log_manager.getRootLogger()
    logger.setLevel(log4jLogger.Level.WARN)

    spark._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider",
    )

    return spark


def get_bootstrap_servers():
    ssm = boto3.client(
        "ssm", region_name=os.getenv("AWS_REGION", "ap-south-1")
    )
    try:
        parameter = ssm.get_parameter(Name="/kafka/bootstrap_servers")
        return parameter["Parameter"]["Value"]
    except ClientError as e:
        # Fallback to default if SSM fails (useful for local dev)
        print(f"Error fetching from SSM: {e}")
        return "localhost:9092"


def main():
    parser = argparse.ArgumentParser(description="Running pyspark dataflows")

    parser.add_argument(
        "--mode",
        help=(
            "Which dataflow(s) to run. 'anomaly' or 'metrics' runs a single "
            "pipeline; 'all' runs both queries concurrently on a single "
            "SparkSession."
        ),
        choices=["anomaly", "metrics", "all"],
        default="all",
    )
    args = parser.parse_args()

    BOOTSTRAP_SERVERS = get_bootstrap_servers()
    INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "site-sensor-raw")
    OUTPUT_TOPIC_INTERNAL = os.getenv("INTERNAL_ALERTS_TOPIC", "alerts_internal")
    OUTPUT_TOPIC_SEVERE = os.getenv("SEVERE_ALERTS_TOPIC", "alerts_severe")
    OUTPUT_TOPIC_FORECASTS = os.getenv("FORECASTS_ALERTS_TOPIC", "alerts_forecasts")
    ALGORITHM = os.getenv("ALGORITHM", "Thresholding")

    mode = args.mode

    if mode == "all":
        # One SparkSession, two streaming queries sharing the same cluster.
        spark = build_spark("NoDustAllDataflows")

        print("Starting Metrics Dataflow...")
        metrics_query = metrics_dataflow.run_dataflow(
            appname="SensorMetricsAggregation",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
            # Unique checkpoint per query is mandatory when running
            # multiple streaming queries against the same SparkSession.
            checkpoint_location="/tmp/checkpoints/metrics",
            spark=spark,
            kafka_security_options=KAFKA_SECURITY_OPTIONS,
        )

        print(f"Starting Anomaly Dataflow using {ALGORITHM} algorithm...")
        (
            anomaly_query_internal,
            anomaly_query_severe,
            anomaly_query_forecast,
        ) = anomaly_dataflow.run_dataflow(
            appname="SensorAnomalyDetection",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
            output_topic_internal=OUTPUT_TOPIC_INTERNAL,
            output_topic_severe=OUTPUT_TOPIC_SEVERE,
            output_topic_forecasts=OUTPUT_TOPIC_FORECASTS,
            algorithm_str=ALGORITHM,
            checkpoint_location="/tmp/checkpoints/anomaly",
            spark=spark,
            kafka_security_options=KAFKA_SECURITY_OPTIONS,
        )

        try:
            spark.streams.awaitAnyTermination()
        finally:
            for q in (
                metrics_query,
                anomaly_query_internal,
                anomaly_query_severe,
                anomaly_query_forecast,
            ):
                try:
                    if q and q.isActive:
                        q.stop()
                except Exception:
                    pass
            spark.stop()
        return

    # --- Single-dataflow modes ---
    spark = build_spark(
        "SensorMetricsAggregation" if mode == "metrics" else "SensorAnomalyDetection"
    )

    if mode == "metrics":
        print("Starting Metrics Dataflow...")
        query = metrics_dataflow.run_dataflow(
            appname="SensorMetricsAggregation",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
            checkpoint_location="/tmp/checkpoints/metrics",
            spark=spark,
            kafka_security_options=KAFKA_SECURITY_OPTIONS,
        )
        try:
            query.awaitTermination()
        finally:
            spark.stop()
        return

    if mode == "anomaly":
        print(f"Starting Anomaly Dataflow using {ALGORITHM} algorithm...")
        q_int, q_sev, q_fc = anomaly_dataflow.run_dataflow(
            appname="SensorAnomalyDetection",
            kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
            input_topic=INPUT_TOPIC,
            output_topic_internal=OUTPUT_TOPIC_INTERNAL,
            output_topic_severe=OUTPUT_TOPIC_SEVERE,
            output_topic_forecasts=OUTPUT_TOPIC_FORECASTS,
            algorithm_str=ALGORITHM,
            checkpoint_location="/tmp/checkpoints/anomaly",
            spark=spark,
            kafka_security_options=KAFKA_SECURITY_OPTIONS,
        )
        try:
            spark.streams.awaitAnyTermination()
        finally:
            for q in (q_int, q_sev, q_fc):
                try:
                    if q and q.isActive:
                        q.stop()
                except Exception:
                    pass
            spark.stop()
        return

    # Should be unreachable because of argparse `choices`
    sys.exit(f"Unknown mode: {mode}")


if __name__ == "__main__":
    main()
