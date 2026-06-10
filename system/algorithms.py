"""Algorithms for detecting anomalies. All of them will take the parsed \
dataframe and return a dataframe of anomalies (timestamp, site, pollutant \
type and possibly concentration).
"""

from __future__ import annotations

from typing import Callable, Iterator

import pandas as pd
import yaml
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ANOMALY_OUTPUT_SCHEMA = StructType(
    [
        StructField("site_id", StringType(), False),
        StructField("pollutant_type", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("concentration", DoubleType(), False),
        StructField("metric_value", DoubleType(), False),
        StructField("metric_name", StringType(), False),
    ]
)

EMA_STATE_SCHEMA = "ema double"
CUSUM_STATE_SCHEMA = "cusum double"


def _load_thresholds(config_path: str) -> dict[str, float]:
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return {
        pollutant: float(threshold)
        for pollutant, threshold in raw.items()
        if isinstance(threshold, (int, float))
    }


def _pollutant_limits(thresholds: dict[str, float], pollutant_type: str) -> float | None:
    return thresholds.get(pollutant_type)


def _stateful_anomalies(
    parsed_dataframe: DataFrame,
    state_fn: Callable,
    state_struct_type: str,
) -> DataFrame:
    """Run per-(site, pollutant) stateful detection and emit alert rows."""
    watermarked = (
        parsed_dataframe.select(
            "site_id",
            "pollutant_type",
            "event_time",
            "concentration",
        )
        .withWatermark("event_time", "2 minutes")
    )

    return watermarked.groupBy("site_id", "pollutant_type").applyInPandasWithState(
        state_fn,
        outputStructType=ANOMALY_OUTPUT_SCHEMA,
        stateStructType=state_struct_type,
        outputMode="append",
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
    )


def _make_ema_fn(
    thresholds: dict[str, float], alpha: float, metric_name: str
) -> Callable:
    def ema_fn(
        key: tuple,
        pdf_iter: Iterator[pd.DataFrame],
        state: GroupState,
    ) -> Iterator[pd.DataFrame]:
        site_id = str(key[0])
        pollutant_type = str(key[1])
        limit = _pollutant_limits(thresholds, pollutant_type)
        if limit is None:
            return

        ema = state.get[0] if state.exists else None

        for pdf in pdf_iter:
            if pdf.empty:
                if state.hasTimedOut:
                    state.remove()
                continue

            pdf = pdf.sort_values("event_time")
            alerts: list[dict] = []
            for row in pdf.itertuples(index=False):
                conc = float(row.concentration)
                if ema is None:
                    ema = conc
                else:
                    ema = alpha * conc + (1.0 - alpha) * ema

                if ema > limit:
                    alerts.append(
                        {
                            "site_id": site_id,
                            "pollutant_type": pollutant_type,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "metric_value": ema,
                            "metric_name": metric_name,
                        }
                    )

                state.update((ema,))
                state.setTimeoutDuration(120000)

            if alerts:
                yield pd.DataFrame(alerts)

    return ema_fn


def _make_cusum_fn(
    thresholds: dict[str, float],
    slack: float,
    decision_interval: float,
    metric_name: str,
) -> Callable:
    def cusum_fn(
        key: tuple,
        pdf_iter: Iterator[pd.DataFrame],
        state: GroupState,
    ) -> Iterator[pd.DataFrame]:
        site_id = str(key[0])
        pollutant_type = str(key[1])
        limit = _pollutant_limits(thresholds, pollutant_type)
        if limit is None:
            return

        cusum = state.get[0] if state.exists else 0.0

        for pdf in pdf_iter:
            if pdf.empty:
                if state.hasTimedOut:
                    state.remove()
                continue

            pdf = pdf.sort_values("event_time")
            alerts: list[dict] = []
            for row in pdf.itertuples(index=False):
                conc = float(row.concentration)
                # One-sided upper CUSUM: accumulate excess above limit + slack.
                cusum = max(0.0, cusum + conc - limit - slack)

                if cusum > decision_interval:
                    alerts.append(
                        {
                            "site_id": site_id,
                            "pollutant_type": pollutant_type,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "metric_value": cusum,
                            "metric_name": metric_name,
                        }
                    )

                state.update((cusum,))
                state.setTimeoutDuration("2 minutes")

            if alerts:
                yield pd.DataFrame(alerts)

    return cusum_fn


class Thresholding:
    """Windowed mean thresholding over a 1-minute tumbling window."""

    def __init__(self, config_path: str = "thresholds.yaml"):
        self.thresholds = _load_thresholds(config_path)

    def get_anomalies(self, parsed_dataframe: DataFrame) -> DataFrame:
        windowed_aggs = (
            parsed_dataframe.withWatermark("event_time", "2 minutes")
            .groupBy(
                window(col("event_time"), "1 minute"),
                col("pollutant_type"),
                col("site_id"),
            )
            .avg("concentration")
            .withColumnRenamed("avg(concentration)", "avg_concentration")
        )

        filter_expr = False
        for pollutant, threshold in self.thresholds.items():
            filter_expr |= (col("pollutant_type") == pollutant) & (
                col("avg_concentration") > threshold
            )

        return windowed_aggs.filter(filter_expr)


class EMAThresholding:
    """Exponential moving average (EMA) thresholding per site and pollutant."""

    def __init__(
        self,
        config_path: str = "thresholds.yaml",
        alpha: float = 0.3,
    ):
        self.thresholds = _load_thresholds(config_path)
        self.alpha = alpha
        self._state_fn = _make_ema_fn(self.thresholds, self.alpha, "ema")

    def get_anomalies(self, parsed_dataframe: DataFrame) -> DataFrame:
        return _stateful_anomalies(
            parsed_dataframe, self._state_fn, EMA_STATE_SCHEMA
        )


class CUSUMThresholding:
    """Cumulative sum (CUSUM) control chart for sustained threshold exceedances."""

    def __init__(
        self,
        config_path: str = "thresholds.yaml",
        slack: float = 2.0,
        decision_interval: float = 50.0,
    ):
        self.thresholds = _load_thresholds(config_path)
        self.slack = slack
        self.decision_interval = decision_interval
        self._state_fn = _make_cusum_fn(
            self.thresholds,
            self.slack,
            self.decision_interval,
            "cusum",
        )

    def get_anomalies(self, parsed_dataframe: DataFrame) -> DataFrame:
        return _stateful_anomalies(
            parsed_dataframe, self._state_fn, CUSUM_STATE_SCHEMA
        )


ALGORITHM_REGISTERY = {
    "Thresholding": Thresholding,
    "EMAThresholding": EMAThresholding,
    "CUSUMThresholding": CUSUMThresholding,
}
