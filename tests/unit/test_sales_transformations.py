from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.silver.clean_sales import (
    clean_identifiers,
    deduplicate_sales,
    add_reference_status,
    add_quality_flags,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("RetailLakehouseSalesUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_clean_identifiers_trims_and_builds_keys(spark):
    data = [
        (
            "retailmax",
            " TX001 ",
            " C001 ",
            " P001 ",
            " S001 ",
        )
    ]

    columns = [
        "source_system",
        "source_transaction_id",
        "source_customer_id",
        "source_product_id",
        "source_store_id",
    ]

    result = clean_identifiers(
        spark.createDataFrame(data, columns)
    ).first()

    assert result.source_transaction_id == "TX001"
    assert result.source_customer_id == "C001"
    assert result.transaction_key == "retailmax:TX001"
    assert result.customer_key == "retailmax:C001"
    assert result.product_key == "retailmax:P001"
    assert result.store_key == "retailmax:S001"


def test_clean_identifiers_converts_blanks_to_null(spark):
    data = [
        ("freshmart", " TX002 ", "   ", "P002", "")
    ]

    columns = [
        "source_system",
        "source_transaction_id",
        "source_customer_id",
        "source_product_id",
        "source_store_id",
    ]

    result = clean_identifiers(
        spark.createDataFrame(data, columns)
    ).first()

    assert result.source_customer_id is None
    assert result.customer_key is None
    assert result.source_store_id is None
    assert result.store_key is None


def test_deduplication_prefers_complete_record(spark):
    columns = [
        "source_system",
        "source_transaction_id",
        "source_customer_id",
        "source_product_id",
        "source_store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_amount",
        "total_amount",
        "ingestion_timestamp",
        "source_file",
    ]

    data = [
        (
            "retailmax", "TX100", "C1", "P1", "S1",
            "2026-01-01", 2, 10.0, 0.0, 20.0,
            datetime(2026, 1, 1), "complete.csv",
        ),
        (
            "retailmax", "TX100", None, "P1", "S1",
            "2026-01-01", 2, 10.0, 0.0, 20.0,
            datetime(2026, 1, 2), "incomplete.csv",
        ),
    ]

    df = spark.createDataFrame(data, columns)

    result = deduplicate_sales(df).collect()

    assert len(result) == 1
    assert result[0].source_customer_id == "C1"
    assert result[0].source_file == "complete.csv"


def test_deduplication_preserves_different_sources(spark):
    columns = [
        "source_system",
        "source_transaction_id",
        "source_customer_id",
        "source_product_id",
        "source_store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_amount",
        "total_amount",
        "ingestion_timestamp",
        "source_file",
    ]

    data = [
        (
            "retailmax", "TX200", "C1", "P1", "S1",
            "2026-01-01", 1, 10.0, 0.0, 10.0,
            datetime(2026, 1, 1), "retailmax.csv",
        ),
        (
            "freshmart", "TX200", "C2", "P2", "S2",
            "2026-01-01", 1, 20.0, 0.0, 20.0,
            datetime(2026, 1, 1), "freshmart.csv",
        ),
    ]

    df = spark.createDataFrame(data, columns)

    result = deduplicate_sales(df).collect()

    assert len(result) == 2
    assert {row.source_system for row in result} == {
        "retailmax",
        "freshmart",
    }



def test_reference_status_marks_matches_and_missing(spark):
    sales = spark.createDataFrame(
        [
            ("TX001", "retailmax:P001"),
            ("TX002", "retailmax:P999"),
        ],
        ["transaction_key", "product_key"],
    )

    products = spark.createDataFrame(
        [("retailmax:P001",)],
        ["product_key"],
    )

    result = add_reference_status(
        sales,
        products,
        "product_key",
        "product_reference_status",
    )

    statuses = {
        row.transaction_key: row.product_reference_status
        for row in result.collect()
    }

    assert statuses == {
        "TX001": "MATCHED",
        "TX002": "MISSING_REFERENCE",
    }


def test_duplicate_reference_keys_do_not_duplicate_sales(spark):
    sales = spark.createDataFrame(
        [
            ("TX001", "retailmax:P001"),
            ("TX002", "retailmax:P002"),
        ],
        ["transaction_key", "product_key"],
    )

    products = spark.createDataFrame(
        [
            ("retailmax:P001",),
            ("retailmax:P001",),
            ("retailmax:P002",),
        ],
        ["product_key"],
    )

    result = add_reference_status(
        sales,
        products,
        "product_key",
        "product_reference_status",
    )

    assert result.count() == 2

    assert {
        row.product_reference_status
        for row in result.collect()
    } == {"MATCHED"}



def test_sales_quality_flags(spark):
    from datetime import date
    from decimal import Decimal
    from pyspark.sql.types import (
        StructType, StructField, StringType,
        DateType, IntegerType, DecimalType,
    )

    schema = StructType([
        StructField("source_transaction_id", StringType()),
        StructField("source_customer_id", StringType()),
        StructField("source_product_id", StringType()),
        StructField("source_store_id", StringType()),
        StructField("transaction_date", DateType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(12, 2)),
        StructField("discount_amount", DecimalType(12, 2)),
        StructField("total_amount", DecimalType(14, 2)),
        StructField("customer_reference_status", StringType()),
        StructField("product_reference_status", StringType()),
        StructField("store_reference_status", StringType()),
    ])

    def record(tx, quantity=2, total="19.00",
               product_status="MATCHED"):
        return (
            tx, "C001", "P001", "S001",
            date(2026, 1, 1),
            quantity,
            Decimal("10.00"),
            Decimal("1.00"),
            Decimal(total),
            "MATCHED",
            product_status,
            "MATCHED",
        )

    data = [
        record("VALID_TX"),
        record("NEGATIVE_TX", quantity=-2),
        record("MISSING_REF_TX", product_status="MISSING_REFERENCE"),
        record("WRONG_TOTAL_TX", total="25.00"),
    ]

    df = spark.createDataFrame(data, schema)
    result = add_quality_flags(df)

    rows = {
        row.source_transaction_id: row
        for row in result.collect()
    }

    assert rows["VALID_TX"].validation_status == "VALID"
    assert rows["VALID_TX"].quality_issues == []

    assert rows["NEGATIVE_TX"].validation_status == "QUARANTINE"
    assert "NEGATIVE_QUANTITY" in rows["NEGATIVE_TX"].quality_issues

    assert rows["MISSING_REF_TX"].validation_status == "QUARANTINE"
    assert (
        "INVALID_PRODUCT_REFERENCE"
        in rows["MISSING_REF_TX"].quality_issues
    )

    assert rows["WRONG_TOTAL_TX"].validation_status == "QUARANTINE"
    assert (
        "TOTAL_AMOUNT_MISMATCH"
        in rows["WRONG_TOTAL_TX"].quality_issues
    )

    assert len(rows) == 4
