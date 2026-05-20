import os

from algorithms import ALGORITHM_REGISTERY
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from schemas import SENSOR_SCHEMA

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "site-sensor-raw")
OUTPUT_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")
ALGORITHM = os.getenv("ALGORITHM", "Thresholding")

spark = SparkSession.builder.appName(
    "SensorAnomalyDetection"
).getOrCreate()

Algorithm = ALGORITHM_REGISTERY[ALGORITHM]
algorithm = Algorithm()

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), SENSOR_SCHEMA).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

# Apply anomaly detection algorithm
anomalies = algorithm.get_anomalies(parsed)

# Write Alerts to Kafka
query = (
    anomalies.selectExpr(
        "CAST(site_id AS STRING) AS key", "to_json(struct(*)) AS value"
    )
    .writeStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints")
    .outputMode("append")
    .start()
)

query.awaitTermination()
