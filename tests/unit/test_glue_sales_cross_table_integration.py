"""Real Spark/Delta tests for sales freshness and reconciliation."""

from datetime import datetime

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
)

from src.glue.sales_freshness import filter_stale_cross_table_records
from src.glue.sales_reconciliation import remove_stale_sales_records


SCHEMA = StructType([
    StructField("transaction_key", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
    StructField("validation_status", StringType(), False),
])


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("SalesCrossTableIntegration")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


def make_df(spark, key, timestamp, status):
    return spark.createDataFrame(
        [(
            key,
            datetime.fromisoformat(timestamp),
            status,
        )],
        SCHEMA,
    )


def keys_at(spark, path):
    return {
        row.transaction_key
        for row in spark.read.format("delta").load(path).collect()
    }


def test_older_quarantine_cannot_override_newer_valid(
    spark, tmp_path
):
    valid_path = str(tmp_path / "valid")
    quarantine_path = str(tmp_path / "quarantine")

    newer_valid = make_df(
        spark, "T001", "2026-10-20T11:00:00", "VALID"
    )
    older_quarantine = make_df(
        spark, "T001", "2026-10-20T10:00:00", "QUARANTINE"
    )

    newer_valid.write.format("delta").save(valid_path)

    filtered = filter_stale_cross_table_records(
        spark, older_quarantine, valid_path
    )

    assert filtered.count() == 0

    # An older record must not trigger deletion.
    remove_stale_sales_records(
        spark, older_quarantine, valid_path
    )

    assert keys_at(spark, valid_path) == {"T001"}


def test_older_valid_cannot_override_newer_quarantine(
    spark, tmp_path
):
    valid_path = str(tmp_path / "valid")
    quarantine_path = str(tmp_path / "quarantine")

    newer_quarantine = make_df(
        spark, "T002", "2026-10-20T11:00:00", "QUARANTINE"
    )
    older_valid = make_df(
        spark, "T002", "2026-10-20T10:00:00", "VALID"
    )

    newer_quarantine.write.format("delta").save(quarantine_path)

    filtered = filter_stale_cross_table_records(
        spark, older_valid, quarantine_path
    )

    assert filtered.count() == 0

    remove_stale_sales_records(
        spark, older_valid, quarantine_path
    )

    assert keys_at(spark, quarantine_path) == {"T002"}


def test_newer_valid_removes_old_quarantine(spark, tmp_path):
    quarantine_path = str(tmp_path / "quarantine")

    old_quarantine = make_df(
        spark, "T003", "2026-10-20T10:00:00", "QUARANTINE"
    )
    new_valid = make_df(
        spark, "T003", "2026-10-20T11:00:00", "VALID"
    )

    old_quarantine.write.format("delta").save(quarantine_path)

    filtered = filter_stale_cross_table_records(
        spark, new_valid, quarantine_path
    )

    assert filtered.count() == 1

    remove_stale_sales_records(
        spark, filtered, quarantine_path
    )

    assert keys_at(spark, quarantine_path) == set()


def test_newer_quarantine_removes_old_valid(spark, tmp_path):
    valid_path = str(tmp_path / "valid")

    old_valid = make_df(
        spark, "T004", "2026-10-20T10:00:00", "VALID"
    )
    new_quarantine = make_df(
        spark, "T004", "2026-10-20T11:00:00", "QUARANTINE"
    )

    old_valid.write.format("delta").save(valid_path)

    filtered = filter_stale_cross_table_records(
        spark, new_quarantine, valid_path
    )

    assert filtered.count() == 1

    remove_stale_sales_records(
        spark, filtered, valid_path
    )

    assert keys_at(spark, valid_path) == set()
