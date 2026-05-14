import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# Load config from Docker environment variables
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC = os.getenv("SENSOR_TOPIC", "site-sensor-raw")
OUTPUT_TOPIC = os.getenv("ALERTS_TOPIC", "alerts")

spark = SparkSession.builder.appName("SensorAnomalyDetection").getOrCreate()

schema = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pollutant_type", StringType(), True),
        StructField("concentration", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", col("timestamp").cast("timestamp"))
)

# 2. Stateful Aggregation (Average)
windowed_aggs = (
    parsed.withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"), col("pollutant_type"), col("site_id")
    )
    .avg("concentration")  # Changed from max to avg
    .withColumnRenamed("avg(concentration)", "avg_concentration")
)

# 3. Filter the Averages
anomalies = windowed_aggs.filter(col("avg_concentration") > 50)

# 4. Write Alerts to Kafka
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
