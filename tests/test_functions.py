from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual
from turbine_ingestion import flag_anomalies, compute_aggregates
from turbine_ingestion.transforms.enrich import AggregationLevel, AggregationLevelError
import pytest

def test_flag_anomalies(
    input_df: DataFrame, lookup_df: DataFrame, expected_anomaly_df: DataFrame
):
    df = input_df.transform(lambda df: flag_anomalies(df, lookup_df))

    assertDataFrameEqual(df, expected_anomaly_df)

@pytest.mark.parametrize(
    ("lvl", "expected_agg_df"),
    [("HOUR","expected_hour_df"),
     ("DAY","expected_day_df")])
def test_compute_aggregates( request,
    input_df: DataFrame, lvl: AggregationLevel, expected_agg_df: str
):
    df = input_df.transform(lambda df: compute_aggregates(df, lvl))
    assertDataFrameEqual(df, request.getfixturevalue(expected_agg_df))

    
def test_compute_aggregates_raises_error_for_wrong_level(
    input_df: DataFrame
):
    with pytest.raises(AggregationLevelError):
        df = input_df.transform(lambda df: compute_aggregates(df, "WRONG_LEVEL")) # pyright: ignore[reportArgumentType]
    
