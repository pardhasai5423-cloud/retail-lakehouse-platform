"""Real Spark tests for Silver MERGE input validation."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

from src.glue.silver_merge import validate_merge_source


SCHEMA = StructType([
    StructField("customer_key", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), True),
])


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("SilverValidationIntegration")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def make_df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


@pytest.mark.parametrize(
    "rows,error",
    [
        (
            [(None, datetime(2026, 10, 20), "batch-001")],
            "null or blank business keys",
        ),
        (
            [("  ", datetime(2026, 10, 20), "batch-001")],
            "null or blank business keys",
        ),
        (
            [("retailmax:C001", None, "batch-001")],
            "null ingestion timestamps",
        ),
        (
            [("retailmax:C001", datetime(2026, 10, 20), "")],
            "null or blank batch IDs",
        ),
        (
            [
                ("retailmax:C001", datetime(2026, 10, 20), "batch-001"),
                ("retailmax:C002", datetime(2026, 10, 20), "batch-002"),
            ],
            "multiple batch IDs",
        ),
        (
            [
                ("retailmax:C001", datetime(2026, 10, 20), "batch-001"),
                ("retailmax:C001", datetime(2026, 10, 20), "batch-001"),
            ],
            "duplicate business keys",
        ),
    ],
)
def test_invalid_merge_input(spark, rows, error):
    df = make_df(spark, rows)

    with pytest.raises(ValueError, match=error):
        validate_merge_source(df, "customers")


def test_valid_merge_input(spark):
    df = make_df(
        spark,
        [
            ("retailmax:C001", datetime(2026, 10, 20), "batch-001"),
            ("retailmax:C002", datetime(2026, 10, 20), "batch-001"),
        ],
    )

    assert validate_merge_source(df, "customers") == "customer_key"
