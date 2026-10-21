"""Real local Delta integration tests for the processing ledger."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.glue.delta_processing_ledger import (
    read_batch,
    register_batch,
    update_batch_status,
)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("delta-processing-ledger-integration")
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

    session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_successful_batch_persists(spark, tmp_path):
    path = str(tmp_path / "ledger")

    record = register_batch(
        spark,
        path,
        "batch-success",
        ["sales", "customers"],
        {"sales": "version-001"},
        allow_writes=True,
    )

    assert record["status"] == "PENDING"

    record = update_batch_status(
        spark,
        path,
        "batch-success",
        "RUNNING",
        allow_writes=True,
    )

    assert record["attempt"] == 1

    record = update_batch_status(
        spark,
        path,
        "batch-success",
        "COMPLETED",
        allow_writes=True,
    )

    assert record["status"] == "COMPLETED"

    persisted = read_batch(spark, path, "batch-success")
    assert persisted["status"] == "COMPLETED"
    assert persisted["datasets"] == ("customers", "sales")

    duplicate = register_batch(
        spark,
        path,
        "batch-success",
        ["customers", "sales"],
        {"sales": "version-001"},
        allow_writes=True,
    )

    assert duplicate["status"] == "COMPLETED"
    assert duplicate["attempt"] == 1

    with pytest.raises(ValueError, match="different source identity"):
        register_batch(
            spark,
            path,
            "batch-success",
            ["sales", "customers"],
            {"sales": "version-002"},
            allow_writes=True,
        )


def test_failed_batch_retry_persists(spark, tmp_path):
    path = str(tmp_path / "ledger")

    register_batch(
        spark,
        path,
        "batch-retry",
        ["inventory"],
        {"inventory": "version-100"},
        allow_writes=True,
    )

    update_batch_status(
        spark,
        path,
        "batch-retry",
        "RUNNING",
        allow_writes=True,
    )

    failed = update_batch_status(
        spark,
        path,
        "batch-retry",
        "FAILED",
        error="Temporary Spark failure",
        allow_writes=True,
    )

    assert failed["status"] == "FAILED"
    assert failed["error"] == "Temporary Spark failure"

    retried = update_batch_status(
        spark,
        path,
        "batch-retry",
        "RUNNING",
        allow_writes=True,
    )

    assert retried["attempt"] == 2
    assert retried["error"] is None

    saved = read_batch(spark, path, "batch-retry")
    assert saved["status"] == "RUNNING"
    assert saved["attempt"] == 2


def test_dry_run_does_not_create_delta_table(spark, tmp_path):
    from delta.tables import DeltaTable

    path = str(tmp_path / "ledger")

    result = register_batch(
        spark,
        path,
        "batch-dry-run",
        ["products"],
    )

    assert result["dry_run"] is True
    assert not DeltaTable.isDeltaTable(spark, path)
