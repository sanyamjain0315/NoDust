"""Algorithms for detecting anomalies. All of them will take the parsed \
dataframe and return a dataframe of anomalies (timestamp, site, pollutant \
type and possibly concentration).
"""

import os
from typing import Callable, Dict, Iterator, List, Literal, Tuple

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


class FuzzyThresholding(BaseAlgorithm):
    """Soft category thresholding with dual fuzzy zones for Orange and Red entry."""

    def __init__(
        self,
        config_path: os.PathLike | str = "thresholds.yaml",
        max_time: int = 14400,
        fuzzy_start_delta: float = 10,
        output_mode: Literal["append", "complete", "update"] = "append",
    ):
        """Fuzzy Thresholding init.

        Args:
            config_path (os.PathLike | str, optional): Path thresholds yaml file.
            max_time (int, optional): Maximum amount of time a datapoint can
                stay in either fuzzy zone. Time specified in seconds. Defaults to 14400 seconds.
            fuzzy_start_delta (float, optional): How many units below the limits
                the fuzzy zones start. Defaults to 10.
        """
        super().__init__(config_path)
        assert max_time <= 86400, (
            f"Max allowed time is 24 hours (86400 seconds), instead got {max_time} seconds"
        )
        assert fuzzy_start_delta > 0, (
            f"Fuzzy start delta should be >0, got {fuzzy_start_delta} instead"
        )
        self.max_time = max_time
        self.fuzzy_start_delta = fuzzy_start_delta
        self.output_mode = output_mode

    def _make_state_fn(
        self,
        thresholds: Dict[str, float],
        T_max: int,
        delta: float,
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

            orange_min = float(limit_categories["orange"]["min"])
            red_min = float(limit_categories["red"]["min"])

            # Zone 1 (Orange) Boundaries
            C_orange = orange_min - delta
            slope_orange = (0 - T_max) / (orange_min - C_orange)
            intercept_orange = T_max - (slope_orange * C_orange)

            # Zone 2 (Red) Boundaries
            C_red = red_min - delta
            slope_red = (0 - T_max) / (red_min - C_red)
            intercept_red = T_max - (slope_red * C_red)

            def _get_timelimit_orange(x: float) -> float:
                return slope_orange * x + intercept_orange

            def _get_timelimit_red(x: float) -> float:
                return slope_red * x + intercept_red

            # Unpack the dual-state tracking variables (initialized to -1.0 for clean/None state)
            if state.exists:
                current_state = state.get
                t_s_orange = float(current_state[0])
                t_s_red = float(current_state[1])
            else:
                t_s_orange, t_s_red = -1.0, -1.0

            for pdf in pdf_iter:
                if pdf.empty:
                    if state.hasTimedOut:
                        state.remove()
                    continue

                pdf = pdf.sort_values("event_time")
                alerts: List[Dict] = []

                for row in pdf.itertuples(index=False):
                    conc = float(row.concentration)
                    current_time_epoch = row.event_time.timestamp()

                    # CASE 1: Hard breach of Critical Red limit
                    if conc >= red_min:
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": conc,
                            "metric_name": "hard_threshold",
                            "alert_category": "red",
                        })
                        t_s_orange, t_s_red = -1.0, -1.0

                    # CASE 2: Inside the Red Fuzzy Zone
                    elif conc >= C_red:
                        # Since it's above orange_min, it's inherently a hard orange violation
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": conc,
                            "metric_name": "hard_threshold",
                            "alert_category": "orange",
                        })

                        # Now evaluate temporal progression toward a Red escalation flag
                        if t_s_red == -1.0:
                            t_s_red = current_time_epoch

                        T_red = current_time_epoch - t_s_red
                        if T_red >= _get_timelimit_red(conc):
                            alerts.append({
                                "site_id": site_id,
                                "event_time": row.event_time,
                                "concentration": conc,
                                "pollutant_type": pollutant_type,
                                "metric_value": T_red,
                                "metric_name": "fuzzy_zone_red_duration",
                                "alert_category": "red",
                            })
                            t_s_red = (
                                -1.0
                            )  # Reset red zone tracker upon alert fire

                        t_s_orange = (
                            -1.0
                        )  # Clear orange tracking since we are higher up

                    # CASE 3: Hard breach of Orange limit (but below Red Fuzzy Zone)
                    elif conc >= orange_min:
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": conc,
                            "metric_name": "hard_threshold",
                            "alert_category": "orange",
                        })
                        t_s_orange, t_s_red = -1.0, -1.0

                    # CASE 4: Inside the Orange Fuzzy Zone
                    elif conc >= C_orange:
                        if t_s_orange == -1.0:
                            t_s_orange = current_time_epoch

                        T_orange = current_time_epoch - t_s_orange
                        if T_orange >= _get_timelimit_orange(conc):
                            alerts.append({
                                "site_id": site_id,
                                "event_time": row.event_time,
                                "concentration": conc,
                                "pollutant_type": pollutant_type,
                                "metric_value": T_orange,
                                "metric_name": "fuzzy_zone_orange_duration",
                                "alert_category": "orange",
                            })
                            t_s_orange = (
                                -1.0
                            )  # Reset orange tracker upon alert fire

                        t_s_red = -1.0  # Clear red tracking

                    # CASE 5: Clean Zone (Below both fuzzy thresholds)
                    else:
                        t_s_orange, t_s_red = -1.0, -1.0

                # Persist state back to Spark state manager
                if t_s_orange != -1.0 or t_s_red != -1.0:
                    state.update((t_s_orange, t_s_red))
                    state.setTimeoutDuration(timeout)
                else:
                    if state.exists:
                        state.remove()

                if alerts:
                    yield pd.DataFrame(alerts)

        return _state_fn

    def get_anomalies(
        self, parsed_dataframe: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        watermarked = parsed_dataframe.select(
            "site_id",
            "pollutant_type",
            "event_time",
            "concentration",
        ).withWatermark("event_time", "24 hours")

        all_alerts = watermarked.groupBy(
            "site_id", "pollutant_type"
        ).applyInPandasWithState(
            self._make_state_fn(
                self.thresholds, self.max_time, self.fuzzy_start_delta
            ),
            outputStructType=ANOMALY_OUTPUT_SCHEMA,
            stateStructType="t_s_orange double, t_s_red double",
            outputMode=self.output_mode,
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
        )

        alerts_internal = all_alerts.filter(col("alert_category") == "orange")
        alerts_severe = all_alerts.filter(col("alert_category") == "red")
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

                    if (
                        ema >= limit_categories["orange"]["min"]
                        and ema < limit_categories["orange"]["max"]
                    ):
                        alerts.append({
                            "site_id": site_id,
                            "event_time": row.event_time,
                            "concentration": conc,
                            "pollutant_type": pollutant_type,
                            "metric_value": ema,
                            "metric_name": metric_name,
                            "alert_category": "orange",
                        })
                    elif ema >= limit_categories["red"]["min"]:
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

    def get_anomalies(
        self, parsed_dataframe: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
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


ALGORITHM_REGISTERY = {
    "Thresholding": Thresholding,
    "EMAThresholding": EMAThresholding,
    "FuzzyThresholding": FuzzyThresholding,
}
