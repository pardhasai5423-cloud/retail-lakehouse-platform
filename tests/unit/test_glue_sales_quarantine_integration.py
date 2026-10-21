"""Real Spark/Delta integration tests for sales quarantine."""

from datetime import datetime

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, ArrayType,
)

from src.glue.sales_quarantine import merge_sales_quarantine


SCHEMA = StructType([
    StructField("transaction_key", StringType(), True),
    StructField("batch_id", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("validation_status", StringType(), True),
    StructField("quality_issues", ArrayType(StringType()), True),
])


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("SalesQuarantineIntegration")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def make_batch(spark, rows, batch_id, timestamp):
    return spark.createDataFrame(
        [
            (
                key,
                batch_id,
                datetime.fromisoformat(timestamp),
                "QUARANTINE",
                issues,
            )
            for key, issues in rows
        ],
        SCHEMA,
    )


def read_quarantine(spark, target):
    return {
        row.transaction_key: row.quality_issues
        for row in spark.read.format("delta").load(target).collect()
    }


def test_quarantine_merge_and_retries(spark, tmp_path):
    target = str(tmp_path / "sales_quarantine")

    first = make_batch(
        spark,
        [
            ("retailmax:T001", ["INVALID_CUSTOMER_REFERENCE"]),
            ("retailmax:T002", ["NEGATIVE_QUANTITY"]),
        ],
        "batch-001",
        "2026-10-20T10:00:00",
    )

    assert merge_sales_quarantine(
        spark, first, target
    )["status"] == "created"

    expected = {
        "retailmax:T001": ["INVALID_CUSTOMER_REFERENCE"],
        "retailmax:T002": ["NEGATIVE_QUANTITY"],
    }

    assert read_quarantine(spark, target) == expected

    newer = make_batch(
        spark,
        [
            ("retailmax:T001", ["TOTAL_AMOUNT_MISMATCH"]),
            ("retailmax:T003", ["MISSING_PRODUCT_ID"]),
        ],
        "batch-002",
        "2026-10-20T11:00:00",
    )

    merge_sales_quarantine(spark, newer, target)

    expected["retailmax:T001"] = ["TOTAL_AMOUNT_MISMATCH"]
    expected["retailmax:T003"] = ["MISSING_PRODUCT_ID"]

    assert read_quarantine(spark, target) == expected

    # Reprocessing the same batch must not duplicate records.
    merge_sales_quarantine(spark, newer, target)

    assert read_quarantine(spark, target) == expected

    older = make_batch(
        spark,
        [("retailmax:T001", ["OLD_ERROR"])],
        "batch-old",
        "2026-10-20T09:00:00",
    )

    merge_sales_quarantine(spark, older, target)

    assert read_quarantine(spark, target) == expected

    final_df = spark.read.format("delta").load(target)

    assert final_df.count() == 3
    assert final_df.select("transaction_key").distinct().count() == 3


def test_empty_quarantine_batch(spark, tmp_path):
    target = str(tmp_path / "empty_quarantine")

    empty = spark.createDataFrame([], SCHEMA)

    result = merge_sales_quarantine(spark, empty, target)

    assert result["status"] == "created"
    assert spark.read.format("delta").load(target).count() == 0
