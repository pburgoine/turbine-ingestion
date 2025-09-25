from datetime import UTC, datetime

import pyspark.sql.types as st
from pyspark.sql import DataFrame, SparkSession

import pytest

AGGREGATE_DF_SCHEMA = st.StructType(
    [
        st.StructField("turbine_id", st.IntegerType(), False),
        st.StructField("period", st.StringType(), False),
        st.StructField("min_power_output", st.DoubleType(), True),
        st.StructField("avg_power_output", st.DoubleType(), True),
        st.StructField("max_power_output", st.DoubleType(), True),
        st.StructField("stddev_power_output", st.DoubleType(), True),
    ]
)


def pytest_addoption(parser):
    parser.addoption("--connect", action="store_true", default=None)


@pytest.fixture(scope="session")
def use_connect(pytestconfig):
    return pytestconfig.getoption("connect")


@pytest.fixture(scope="session")
def spark(use_connect) -> SparkSession:
    if use_connect:
        spark_session = (
            SparkSession.builder.remote("sc://localhost:15002")  # type: ignore
            .appName("Spark Connect Pytest Fixture")
            .getOrCreate()
        )
    else:
        spark_session = (
            SparkSession.builder.master("local")  # type: ignore
            .appName("Spark Connect Pytest Fixture")
            .getOrCreate()
        )
    return spark_session


AGGREGATE_DF_SCHEMA = st.StructType(
    [
        st.StructField("turbine_id", st.IntegerType(), False),
        st.StructField("period", st.StringType(), False),
        st.StructField("min_power_output", st.DoubleType(), True),
        st.StructField("avg_power_output", st.DoubleType(), True),
        st.StructField("max_power_output", st.DoubleType(), True),
        st.StructField("stddev_power_output", st.DoubleType(), True),
    ]
)


@pytest.fixture(scope="session")
def input_df(spark: SparkSession) -> DataFrame:
    schema = st.StructType(
        [
            st.StructField("turbine_id", st.IntegerType(), False),
            st.StructField("timestamp", st.TimestampType(), False),
            st.StructField("power_output", st.DoubleType(), False),
        ]
    )
    rows = [
        (1, datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC), 150.0),
        (1, datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC), 130.0),
        (2, datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC), 500.0),
        (1, datetime(2025, 1, 1, 3, 0, 0, tzinfo=UTC), 140.0),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture(scope="session")
def lookup_df(spark: SparkSession) -> DataFrame:
    schema = st.StructType(
        [
            st.StructField("turbine_id", st.IntegerType(), False),
            st.StructField("period", st.StringType(), False),
            st.StructField("avg_power_output", st.DoubleType(), True),
            st.StructField("stddev_power_output", st.DoubleType(), True),
        ]
    )
    rows = [
        (1, "01:00", 100.0, 20.0),
        (1, "03:00", 100.0, 20.0),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture(scope="session")
def expected_anomaly_df(spark: SparkSession) -> DataFrame:
    schema = st.StructType(
        [
            st.StructField("turbine_id", st.IntegerType(), False),
            st.StructField("timestamp", st.TimestampType(), False),
            st.StructField("power_output", st.DoubleType(), False),
            st.StructField("is_anomaly", st.BooleanType(), False),
        ]
    )
    rows = [
        (1, datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC), 150.0, True),
        (1, datetime(2025, 1, 1, 1, 0, 0, tzinfo=UTC), 130.0, False),
        (2, datetime(2025, 1, 1, 2, 0, 0, tzinfo=UTC), 500.0, False),
        (1, datetime(2025, 1, 1, 3, 0, 0, tzinfo=UTC), 140.0, False),
    ]
    return spark.createDataFrame(rows, schema)


@pytest.fixture(scope="session")
def expected_hour_df(spark: SparkSession) -> DataFrame:
    rows = [
        (1, "01:00", 130.0, 140.0, 150.0, 14.142135623730951),
        (1, "03:00", 140.0, 140.0, 140.0, None),
        (2, "02:00", 500.0, 500.0, 500.0, None),
    ]
    return spark.createDataFrame(rows, AGGREGATE_DF_SCHEMA)


@pytest.fixture(scope="session")
def expected_day_df(spark: SparkSession) -> DataFrame:
    rows = [
        (1, "2025-01-01", 130.0, 140.0, 150.0, 10.0),
        (2, "2025-01-01", 500.0, 500.0, 500.0, None),
    ]
    return spark.createDataFrame(rows, AGGREGATE_DF_SCHEMA)
