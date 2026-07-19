from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SENSOR_SCHEMA = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("pollutant_type", StringType(), False),
    StructField("concentration", DoubleType(), False),
    StructField("unit", StringType(), True),
])

ANOMALY_OUTPUT_SCHEMA = StructType([
    StructField("site_id", StringType(), False),
    StructField("pollutant_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("concentration", DoubleType(), False),
    StructField("metric_value", DoubleType(), False),
    StructField("metric_name", StringType(), False),
    StructField("alert_category", StringType(), True),
])

FORECAST_OUTPUT_SCHEMA = StructType([
    StructField("site_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("pollutant_type", StringType(), False),
    StructField("timescale", StringType(), False),  # "15min", "1hr", "3hr"
    StructField(
        "predicted_category", StringType(), False
    ),  # "green_yellow", "orange", "red"
])

EMA_STATE_SCHEMA = "ema double"
CUSUM_STATE_SCHEMA = "cusum double"
