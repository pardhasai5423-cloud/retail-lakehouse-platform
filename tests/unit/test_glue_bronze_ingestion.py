"""Local tests for Glue-compatible Bronze ingestion."""

import pytest

from pyspark.sql.types import StringType

from src.glue.jobs.bronze_ingestion import (
    read_bronze_batch,
    prepare_bronze_batch,
)


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("test-glue-bronze-ingestion")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def sample_csv(tmp_path):
    csv_file = tmp_path / "sales.csv"

    csv_file.write_text(
        "transaction_id,customer_id,product_id,store_id,"
        "transaction_date,quantity,unit_price,"
        "discount_amount,total_amount\n"
        "T001,C001,P001,S001,2026-10-19,2,12.50,0,25.00\n"
        "T002,C002,P002,S002,2026-10-19,"
        "invalid,10.00,0,20.00\n",
        encoding="utf-8",
    )

    return str(csv_file)


def test_read_bronze_batch(spark, sample_csv):
    df, plan = read_bronze_batch(
        spark, sample_csv, SOURCE_KEY
    )

    assert df.count() == 2
    assert plan.company == "retailmax"
    assert plan.dataset == "sales"

    assert all(
        isinstance(field.dataType, StringType)
        for field in df.schema.fields
    )


def test_raw_invalid_numeric_value_is_preserved(
    spark, sample_csv
):
    df, _ = read_bronze_batch(
        spark, sample_csv, SOURCE_KEY
    )

    rows = {
        row["transaction_id"]: row
        for row in df.collect()
    }

    assert rows["T002"]["quantity"] == "invalid"


def test_ingestion_metadata(spark, sample_csv):
    df, plan = read_bronze_batch(
        spark, sample_csv, SOURCE_KEY
    )

    result = prepare_bronze_batch(
        df, plan, "batch-test-001", SOURCE_KEY)

    rows = result.collect()

    assert len(rows) == 2

    for row in rows:
        assert row["_source_system"] == "retailmax"
        assert row["_source_dataset"] == "sales"
        assert row["_batch_id"] == "batch-test-001"
        assert row["_source_file"] == SOURCE_KEY
        assert row["_ingestion_timestamp"] is not None


@pytest.mark.parametrize("batch_id", ["", "   ", None])
def test_invalid_batch_id_rejected(
    spark, sample_csv, batch_id
):
    df, plan = read_bronze_batch(
        spark, sample_csv, SOURCE_KEY
    )

    with pytest.raises(ValueError):
        prepare_bronze_batch(df, plan, batch_id, SOURCE_KEY)


def test_swapped_csv_headers_are_rejected(spark, tmp_path):
    """Reject files whose header order differs from the schema."""

    csv_file = tmp_path / "swapped_sales.csv"

    csv_file.write_text(
        "transaction_id,product_id,customer_id,store_id,"
        "transaction_date,quantity,unit_price,"
        "discount_amount,total_amount\n"
        "T001,P001,C001,S001,2026-10-19,2,12.50,0,25.00\n",
        encoding="utf-8",
    )

    df, _ = read_bronze_batch(
        spark, str(csv_file), SOURCE_KEY
    )

    with pytest.raises(Exception) as exc_info:
        df.collect()

    assert "header" in str(exc_info.value).lower()
