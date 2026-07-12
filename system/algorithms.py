"""Algorithms for detecting anomalies. All of them will take the parsed \
dataframe and return a dataframe of anomalies (timestamp, site, pollutant \
type and possibly concentration).
"""

import os
from typing import Callable, Dict, Iterator, List, Tuple

import pandas as pd
import yaml
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from system.schemas import (
    ANOMALY_OUTPUT_SCHEMA,
    CUSUM_STATE_SCHEMA,
    EMA_STATE_SCHEMA,
)


def _load_thresholds(config_path: str) -> Dict[str, float]:
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)
    return {pollutant: categories for pollutant, categories in raw.items()}


class BaseAlgorithm:
    def __init__(self, config_path: os.PathLike | str = "thresholds.yaml"):
        self.thresholds = _load_thresholds(config_path)


class Thresholding(BaseAlgorithm):
    """Simple Category based thresholding. If any one value fall in the problem
    categories, they are passed on as alerts"""

    def __init__(self, config_path: os.PathLike | str = "thresholds.yaml"):
        super().__init__(config_path)

    def get_anomalies(
        self, parsed_dataframe: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        filter_expr_internal_alerts = False
        for pollutant, categories in self.thresholds.items():
            filter_expr_internal_alerts |= (
                col("pollutant_type") == pollutant
            ) & (
                (col("concentration") >= categories["orange"]["min"])
                & (col("concentration") < categories["orange"]["max"])
            )

        filter_expr_severe_alerts = False
        for pollutant, categories in self.thresholds.items():
            filter_expr_severe_alerts |= (
                col("pollutant_type") == pollutant
            ) & (col("concentration") >= categories["red"]["min"])

        alerts_internal = parsed_dataframe.filter(filter_expr_internal_alerts)
        alerts_severe = parsed_dataframe.filter(filter_expr_severe_alerts)
        return alerts_internal, alerts_severe


class EMAThresholding(BaseAlgorithm):
    """Exponential moving average (EMA) thresholding per site and pollutant."""

    def __init__(
        self,
        config_path: os.PathLike | str = "thresholds.yaml",
        alpha: float = 0.3,
        watermark_duration: str = "1 hours",
        output_mode: str = "append",
    ):
        """Initilizing algorithm params for EWMA.

        Args:
            config_path (os.PathLike | str, optional): Path thresholds yaml file. \
                Should be of the form \
                    ```{\
                            pollutant_type:{\
                                category[green,yellow,orange,red]:{\
                                    thresholds[min,max]:value\
                                }\
                            }\
                        }```\
                Thresholds are min inclusive, max exclusive. Defaults to "thresholds.yaml".
            alpha (float, optional): Alpha value for exponential average. \
                Should be between 0 and 1. Decides how much the current value \
                should be important. \
                Take higher values if you want more weight on latest values \
                than the ewma Defaults to 0.3.
            watermark_duration (str, optional): Duration over which EWMA should\
                be calculated. Defaults to "1 hours".
            output_mode (str, optional): Mode in which windows are triggered. \
                Options are ["append", "complete", "update"]. Doc similar to \
                    pyspark output modes for windows. Defaults to "append".
        """
        super().__init__(config_path)
        self.alpha = alpha
        self.watermark_duration = watermark_duration
        self.output_mode = output_mode

    def _make_state_fn(
        self,
        thresholds: Dict[str, float],
        alpha: float,
        metric_name: str,
        timeout: int = 120000,
    ) -> Callable:
        def _state_fn(
            key: Tuple, pdf_iter: Iterator[pd.DataFrame], state: GroupState
        ) -> Iterator[pd.DataFrame]:
            site_id = str(key[0])
            pollutant_type = str(key[1])
            limit_categories = thresholds[pollutant_type]
            if limit_categories is None:
                return

            ema = state.get[0] if state.exists else None

            for pdf in pdf_iter:
                if pdf.empty:
                    if state.hasTimedOut:
                        state.remove()
                    continue

                pdf = pdf.sort_values("event_time")
                alerts: List[Dict] = []
                
                for row in pdf.itertuples(index=False):
                    conc = float(row.concentration)
                    if ema is None:
                        ema = conc
                    else:
                        ema = alpha * conc + (1.0 - alpha) * ema

                    if ema >= limit_categories['orange']['min'] and ema < limit_categories['orange']['max']:
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": ema,
                            "metric_name": metric_name,
                            "alert_category": "orange",
                        })
                    elif ema >= limit_categories['red']['min']:
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": ema,
                            "metric_name": metric_name,
                            "alert_category": "red",
                        })

                    state.update((ema,))

                state.setTimeoutDuration(timeout)

                if alerts:
                    yield pd.DataFrame(alerts)

        return _state_fn

    def get_anomalies(self, parsed_dataframe: DataFrame) -> Tuple[DataFrame, DataFrame]:
        watermarked = parsed_dataframe.select(
            "site_id",
            "pollutant_type",
            "event_time",
            "concentration",
        ).withWatermark("event_time", self.watermark_duration)

        all_alerts = watermarked.groupBy(
            "site_id", "pollutant_type"
        ).applyInPandasWithState(
            self._make_state_fn(self.thresholds, self.alpha, "ema"),
            outputStructType=ANOMALY_OUTPUT_SCHEMA,
            stateStructType=EMA_STATE_SCHEMA,
            outputMode=self.output_mode,
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )
        
        alerts_internal = all_alerts.filter(col("alert_category") == "orange")
        alerts_severe = all_alerts.filter(col("alert_category") == "red")
        return alerts_internal, alerts_severe

class CUSUMThresholding(BaseAlgorithm):
    """Cumulative sum (CUSUM) control chart for sustained threshold exceedances."""

    def __init__(
        self,
        config_path: os.PathLike | str = "thresholds.yaml",
        slack: float = 2.0,
        decision_interval: float = 50.0,
        watermark_duration: str = "2 minutes",
        output_mode: str = "append",
    ):
        super().__init__(config_path)
        self.slack = slack
        self.decision_interval = decision_interval
        self.watermark_duration = watermark_duration
        self.output_mode = output_mode

    def _make_state_fn(
        self,
        thresholds: Dict[str, float],
        slack: float,
        decision_interval: float,
        metric_name: str,
        timeout: int = 120000,
    ) -> Callable:
        def state_fn(
            key: Tuple,
            pdf_iter: Iterator[pd.DataFrame],
            state: GroupState,
        ) -> Iterator[pd.DataFrame]:
            site_id = str(key[0])
            pollutant_type = str(key[1])
            limit = thresholds[pollutant_type]
            if limit is None:
                return

            cusum = state.get[0] if state.exists else 0.0

            for pdf in pdf_iter:
                if pdf.empty:
                    if state.hasTimedOut:
                        state.remove()
                    continue

                pdf = pdf.sort_values("event_time")
                alerts: List[Dict] = []
                for row in pdf.itertuples(index=False):
                    conc = float(row.concentration)
                    # One-sided upper CUSUM: accumulate excess above limit + slack.
                    cusum = max(0.0, cusum + conc - limit - slack)

                    if cusum > decision_interval:
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": cusum,
                            "metric_name": metric_name,
                        })

                    state.update((cusum,))

                state.setTimeoutDuration(timeout)

                if alerts:
                    yield pd.DataFrame(alerts)

        return state_fn

    def get_anomalies(self, parsed_dataframe: DataFrame) -> DataFrame:
        watermarked = parsed_dataframe.select(
            "site_id",
            "pollutant_type",
            "event_time",
            "concentration",
        ).withWatermark("event_time", self.watermark_duration)

        return watermarked.groupBy(
            "site_id", "pollutant_type"
        ).applyInPandasWithState(
            self._make_state_fn(
                self.thresholds, self.slack, self.decision_interval, "cusum"
            ),
            outputStructType=ANOMALY_OUTPUT_SCHEMA,
            stateStructType=CUSUM_STATE_SCHEMA,
            outputMode=self.output_mode,
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )


ALGORITHM_REGISTERY = {
    "Thresholding": Thresholding,
    "EMAThresholding": EMAThresholding,
    "CUSUMThresholding": CUSUMThresholding,
}
