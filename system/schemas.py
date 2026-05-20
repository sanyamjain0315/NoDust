from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pollutant_type", StringType(), True),
        StructField("concentration", DoubleType(), True),
        StructField("unit", StringType(), True),
    ]
)
