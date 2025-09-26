from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual

import pytest

from turbine_ingestion import apply_latest_filter, compute_aggregates, flag_anomalies
from turbine_ingestion.transforms.enrich import AggregationLevel, AggregationLevelError


def test_flag_anomalies(
    input_df: DataFrame, lookup_df: DataFrame, expected_anomaly_df: DataFrame
):
    df = input_df.transform(lambda df: flag_anomalies(df, lookup_df))

    assertDataFrameEqual(
        df,
        expected_anomaly_df,
    )


@pytest.mark.parametrize(
    ("lvl", "expected_agg_df"), [("HOUR", "expected_hour_df"), ("DAY", "expected_day_df")]
)
def test_compute_aggregates(
    request, input_df: DataFrame, lvl: AggregationLevel, expected_agg_df: str
):
    df = input_df.transform(lambda df: compute_aggregates(df, lvl))
    assertDataFrameEqual(df, request.getfixturevalue(expected_agg_df))


def test_compute_aggregates_raises_error_for_wrong_level(input_df: DataFrame):
    with pytest.raises(AggregationLevelError):
        input_df.transform(lambda df: compute_aggregates(df, "WRONG_LEVEL"))  # pyright: ignore[reportArgumentType]


def test_apply_latest_filter(
    duplicate_df: DataFrame, expected_deduplicated_df: DataFrame
):
    df = duplicate_df.transform(apply_latest_filter)
    assertDataFrameEqual(df, expected_deduplicated_df)
