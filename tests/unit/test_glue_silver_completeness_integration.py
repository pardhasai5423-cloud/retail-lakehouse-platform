"""Real Spark/Delta test for completeness-aware customer MERGE."""

from datetime import datetime

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    TimestampType, DateType,
)

from src.glue.silver_merge import merge_silver_batch


SCHEMA = StructType([
    StructField("customer_key", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), False),
])


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("SilverCompletenessIntegration")
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

    session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def customer_batch(spark, name, email, city, timestamp, batch):
    return spark.createDataFrame(
        [(
            "retailmax:C001",
            name,
            "Smith",
            email,
            None,
            city,
            "VA",
            None,
            datetime.fromisoformat(timestamp),
            batch,
        )],
        SCHEMA,
    )


def get_customer(spark, target):
    return (
        spark.read.format("delta")
        .load(target)
        .select("first_name", "email", "city")
        .first()
    )


def test_customer_completeness_precedence(spark, tmp_path):
    target = str(tmp_path / "customers")

    initial = customer_batch(
        spark, "Alice", "alice@example.com",
        "Fairfax", "2026-10-20T10:00:00", "batch-001"
    )

    merge_silver_batch(spark, initial, "customers", target)

    # Newer record has less information: must not replace Alice.
    incomplete = customer_batch(
        spark, "Alicia", None,
        None, "2026-10-20T11:00:00", "batch-002"
    )

    merge_silver_batch(spark, incomplete, "customers", target)

    row = get_customer(spark, target)
    assert row.first_name == "Alice"
    assert row.email == "alice@example.com"
    assert row.city == "Fairfax"

    # Newer record has more information: should replace Alice.
    improved = customer_batch(
        spark, "Alicia", "alicia@example.com",
        "Richmond", "2026-10-20T12:00:00", "batch-003"
    ).withColumn("phone", __import__(
        "pyspark.sql.functions", fromlist=["lit"]
    ).lit("555-0101"))

    merge_silver_batch(spark, improved, "customers", target)

    row = get_customer(spark, target)
    assert row.first_name == "Alicia"
    assert row.email == "alicia@example.com"
    assert row.city == "Richmond"

    # Retrying the same batch must preserve one customer.
    merge_silver_batch(spark, improved, "customers", target)

    result = spark.read.format("delta").load(target)

    assert result.count() == 1
    assert result.first().batch_id == "batch-003"
