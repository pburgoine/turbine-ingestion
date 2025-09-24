from typing import Literal

import pyspark.sql.functions as sf
from pyspark.sql import DataFrame

AggregationLevel = Literal[
    "HOUR",
    "DAY",
]


def flag_anomalies(df: DataFrame, lookup_df: DataFrame) -> DataFrame:
    """Static join to hourly aggregate table to determine outliers.

    Any power output more than 2 standard deviations from the mean at that hour is flagged.
    If the look up is null for that given hour and turbine, row will not be flagged.
    """
    df = df.alias("l").join(
        lookup_df.alias("r").select(
            "turbine_id", "period", "avg_power_output", "stddev_power_output"
        ),
        on=[
            df.turbine_id == lookup_df.turbine_id,
            sf.date_format(sf.col("timestamp"), "hh:mm") == lookup_df.period,
        ],
        how="left_outer",
    )
    return df.withColumn(
        "is_anomaly",
        sf.when(
            (
                sf.col("power_output")
                > (sf.col("avg_power_output") + 2 * sf.col("stddev_power_output"))
            )
            | (
                sf.col("power_output")
                < (sf.col("avg_power_output") - 2 * sf.col("stddev_power_output"))
            ),
            sf.lit(True),
        ).otherwise(sf.lit(False)),
    ).select("l.*")


def compute_aggregates(df: DataFrame, level: AggregationLevel) -> DataFrame:
    """Compute summary statistics table at turbine level over given time period."""
    format = "yyyy-MM-dd" if level == "DAY" else "hh:mm"

    return (
        df.withColumn("period", sf.date_format(sf.col("timestamp"), format))
        .groupBy("turbine_id", "period")
        .agg(
            sf.min(sf.col("power_output")).alias("min_power_output"),
            sf.avg(sf.col("power_output")).alias("avg_power_output"),
            sf.max(sf.col("power_output")).alias("max_power_output"),
            sf.stddev(sf.col("power_output")).alias("stddev_power_output"),
        )
    )
