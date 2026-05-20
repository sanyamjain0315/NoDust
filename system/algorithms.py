"""Algorithms for detecting anomalies. All of them will take the parsed \
dataframe and return a dataframe of anomalies (timestamp, site, pollutant \
type and possibly concentration).
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window


class Thresholding:
    def __init__(self, config_path: str = "thresholds.yaml"):
        import yaml

        with open(config_path, "r") as f:
            self.thresholds = yaml.safe_load(f)

    def get_anomalies(self, parsed_dataframe: DataFrame) -> DataFrame:
        windowed_aggs = (
            parsed_dataframe.withWatermark("event_time", "2 minutes")
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("pollutant_type"),
                col("site_id"),
            )
            .avg("concentration")
            .withColumnRenamed(
                "avg(concentration)", "avg_concentration"
            )
        )

        filter_expr = False
        for pollutant, threshold in self.thresholds.items():
            filter_expr |= (col("pollutant_type") == pollutant) & (
                col("avg_concentration") > threshold
            )

        anomalies = windowed_aggs.filter(filter_expr)
        return anomalies


ALGORITHM_REGISTERY = {"Thresholding": Thresholding}
