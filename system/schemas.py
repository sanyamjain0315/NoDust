from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

SENSOR_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), False),
        StructField("site_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("pollutant_type", StringType(), False),
        StructField("concentration", DoubleType(), False),
        StructField("unit", StringType(), True),
    ]
)
