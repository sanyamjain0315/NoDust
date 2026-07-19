import os
from typing import Literal, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from system.algorithms import ALGORITHM_REGISTERY
from system.schemas import SENSOR_SCHEMA


def run_dataflow(
    appname: str,
    kafka_bootstrap_servers: str,
    input_topic: str,
    output_topic_internal: str,
    output_topic_severe: str,
    output_topic_forecasts: Optional[str] = None,
    algorithm_str: Literal[
        "Thresholding",
        "EMAThresholding",
        "CUSUMThresholding",
        "XGBoostForecasting",
    ] = "Thresholding",
    output_mode: Literal["append", "complete", "update"] = "append",
    checkpoint_location: os.PathLike | str = "/tmp/checkpoints",
    spark: Optional[SparkSession] = None,
) -> Tuple[StreamingQuery, StreamingQuery, Optional[StreamingQuery]]:
    """
    Build and start the anomaly streaming query.

    If `spark` is provided it will be reused; otherwise a new SparkSession
    is created. The function returns the started `StreamingQuery` *without*
    awaiting termination so that multiple queries can run concurrently on
    the same SparkSession.
    """
    # Connecting to spark session
    if spark is None:
        spark = SparkSession.builder.appName(appname).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    Algorithm = ALGORITHM_REGISTERY[algorithm_str]
    algorithm = Algorithm(config_path="system/thresholds.yaml")

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        df
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
    )

    # Resolve algorithm patterns
    query_forecast = None
    if algorithm_str == "XGBoostForecasting":
        alerts_internal, alerts_severe, forecasts = algorithm.get_anomalies(
            parsed,
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            input_topic=input_topic,
        )
    else:
        alerts_internal, alerts_severe = algorithm.get_anomalies(parsed)
        forecasts = None

    # Write internal alerts
    query_internal = (
        alerts_internal
        .selectExpr(
            "CAST(site_id AS STRING) AS key", "to_json(struct(*)) AS value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", output_topic_internal)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_location, "internal_alerts"),
        )
        .outputMode(output_mode)
        .start()
    )

    # Write severe alerts
    query_severe = (
        alerts_severe
        .selectExpr(
            "CAST(site_id AS STRING) AS key", "to_json(struct(*)) AS value"
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("topic", output_topic_severe)
        .option(
            "checkpointLocation",
            os.path.join(checkpoint_location, "severe_alerts"),
        )
        .outputMode(output_mode)
        .start()
    )

    # Write machine learning forecasts if generated
    if forecasts is not None and output_topic_forecasts is not None:
        query_forecast = (
            forecasts
            .selectExpr(
                "CAST(site_id AS STRING) AS key", "to_json(struct(*)) AS value"
            )
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
            .option("topic", output_topic_forecasts)
            .option(
                "checkpointLocation",
                os.path.join(checkpoint_location, "forecast_alerts"),
            )
            .outputMode(output_mode)
            .start()
        )

    return query_internal, query_severe, query_forecast
