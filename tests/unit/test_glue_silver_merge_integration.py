"""Real local Spark/Delta integration tests for incremental Silver MERGE."""

from datetime import datetime

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

from src.glue.silver_merge import merge_silver_batch


SCHEMA = StructType([
    StructField("customer_key", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), False),
])


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("SilverMergeIntegration")
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
                customer_key,
                name,
                datetime.fromisoformat(timestamp),
                batch_id,
            )
            for customer_key, name in rows
        ],
        SCHEMA,
    )


def read_customers(spark, target):
    return {
        row.customer_key: row.first_name
        for row in (
            spark.read.format("delta")
            .load(target)
            .select("customer_key", "first_name")
            .collect()
        )
    }


def test_incremental_merge_insert_update_retry_and_older_batch(
    spark, tmp_path
):
    target = str(tmp_path / "silver_customers")

    # Batch 1: Initial load
    batch1 = make_batch(
        spark,
        [
            ("retailmax:C001", "Alice"),
            ("retailmax:C002", "Bob"),
        ],
        "batch-001",
        "2026-10-20T10:00:00",
    )

    result = merge_silver_batch(
        spark, batch1, "customers", target
    )

    assert result["status"] == "created"
    assert read_customers(spark, target) == {
        "retailmax:C001": "Alice",
        "retailmax:C002": "Bob",
    }

    # Batch 2: Update existing customer and insert new customer
    batch2 = make_batch(
        spark,
        [
            ("retailmax:C001", "Alicia"),
            ("retailmax:C003", "Charlie"),
        ],
        "batch-002",
        "2026-10-20T11:00:00",
    )

    result = merge_silver_batch(
        spark, batch2, "customers", target
    )

    assert result["status"] == "merged"

    expected = {
        "retailmax:C001": "Alicia",
        "retailmax:C002": "Bob",
        "retailmax:C003": "Charlie",
    }

    assert read_customers(spark, target) == expected

    # Retry the same batch: must not duplicate or change records
    merge_silver_batch(spark, batch2, "customers", target)
    assert read_customers(spark, target) == expected

    # Older batch: must not overwrite a newer record
    older = make_batch(
        spark,
        [("retailmax:C001", "Old Alice")],
        "batch-old",
        "2026-10-20T09:00:00",
    )

    merge_silver_batch(spark, older, "customers", target)

    assert read_customers(spark, target) == expected

    # Verify business key uniqueness
    df = spark.read.format("delta").load(target)

    assert df.count() == 3
    assert df.select("customer_key").distinct().count() == 3
