"""Tests for the safe Bronze Spark execution preview."""

import pytest
from pyspark.sql import SparkSession

from src.glue.jobs.bronze_executor import preview_bronze_ingestion


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-glue-bronze-executor")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture(autouse=True)
def approved_environment(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "retail-lakehouse-parsam-2026",
    )
    monkeypatch.setenv(
        "LAKEHOUSE_REGION",
        "us-east-2",
    )
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/glue-incremental-v1",
    )


@pytest.fixture
def sample_csv(tmp_path):
    path = tmp_path / "sales.csv"

    path.write_text(
        "transaction_id,customer_id,product_id,store_id,"
        "transaction_date,quantity,unit_price,"
        "discount_amount,total_amount\n"
        "T001,C001,P001,S001,2026-10-19,2,12.50,0,25.00\n"
        "T002,C002,P002,S002,2026-10-19,1,10.00,0,10.00\n",
        encoding="utf-8",
    )

    return str(path)


def test_preview_processes_csv(spark, sample_csv):
    result = preview_bronze_ingestion(
        spark,
        sample_csv,
        SOURCE_KEY,
        version_id="version-001",
    )

    assert result["row_count"] == 2
    assert result["company"] == "retailmax"
    assert result["dataset"] == "sales"

    assert result["target"].endswith(
        "/bronze/retailmax/sales"
    )

    rows = result["dataframe"].collect()

    assert len(rows) == 2

    for row in rows:
        assert row["_source_system"] == "retailmax"
        assert row["_source_dataset"] == "sales"
        assert row["_batch_id"] == result["batch_id"]
        assert row["_ingestion_timestamp"] is not None


def test_preview_batch_id_is_stable(spark, sample_csv):
    first = preview_bronze_ingestion(
        spark,
        sample_csv,
        SOURCE_KEY,
        version_id="version-001",
    )

    second = preview_bronze_ingestion(
        spark,
        sample_csv,
        SOURCE_KEY,
        version_id="version-001",
    )

    assert first["batch_id"] == second["batch_id"]


def test_preview_rejects_missing_file(spark, tmp_path):
    missing = tmp_path / "missing.csv"

    with pytest.raises(FileNotFoundError):
        preview_bronze_ingestion(
            spark,
            str(missing),
            SOURCE_KEY,
            version_id="version-001",
        )


def test_preview_rejects_missing_identity(spark, sample_csv):
    with pytest.raises(ValueError):
        preview_bronze_ingestion(
            spark,
            sample_csv,
            SOURCE_KEY,
        )


def test_preview_rejects_unapproved_prefix(
    spark, sample_csv, monkeypatch
):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )

    with pytest.raises(RuntimeError):
        preview_bronze_ingestion(
            spark,
            sample_csv,
            SOURCE_KEY,
            version_id="version-001",
        )


def test_preview_rejects_malformed_csv(spark, tmp_path):
    """A CSV row with extra fields must fail validation."""

    csv_file = tmp_path / "malformed_sales.csv"

    csv_file.write_text(
        "transaction_id,customer_id,product_id,store_id,"
        "transaction_date,quantity,unit_price,"
        "discount_amount,total_amount\n"
        "T001,C001,P001,S001,2026-10-19,2,12.50,0,25.00\n"
        "T002,C002,P002,S002,2026-10-19,1,10.00,0,10.00,"
        "UNEXPECTED_EXTRA_FIELD\n",
        encoding="utf-8",
    )

    with pytest.raises(Exception):
        preview_bronze_ingestion(
            spark,
            str(csv_file),
            SOURCE_KEY,
            version_id="version-001",
        )
