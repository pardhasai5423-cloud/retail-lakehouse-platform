from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DateType,
)

from src.silver.clean_inventory import (
    clean_identifiers,
    add_validation_status,
    add_stock_status,
    add_inventory_freshness_status,
    deduplicate_inventory,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("RetailLakehouseInventoryUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


SCHEMA = StructType([
    StructField("source_system", StringType()),
    StructField("source_inventory_id", StringType()),
    StructField("source_product_id", StringType()),
    StructField("source_store_id", StringType()),
    StructField("stock_quantity", IntegerType()),
    StructField("reorder_level", IntegerType()),
    StructField("last_updated", DateType()),
])


def make_record(
    inventory_id="I001",
    product_id="P001",
    store_id="S001",
    quantity=20,
    reorder_level=10,
    updated=None,
):
    if updated is None:
        updated = date.today()

    return (
        "retailmax",
        inventory_id,
        product_id,
        store_id,
        quantity,
        reorder_level,
        updated,
    )


def test_inventory_identifier_cleaning(spark):
    df = spark.createDataFrame(
        [
            make_record(
                inventory_id=" I001 ",
                product_id=" P001 ",
                store_id="   ",
            )
        ],
        SCHEMA,
    )

    row = clean_identifiers(df).first()

    assert row.source_inventory_id == "I001"
    assert row.inventory_key == "retailmax:I001"
    assert row.product_key == "retailmax:P001"
    assert row.source_store_id is None
    assert row.store_key is None


@pytest.mark.parametrize(
    "record,expected",
    [
        (make_record(), "VALID"),
        (
            make_record(inventory_id=""),
            "MISSING_INVENTORY_ID",
        ),
        (
            make_record(product_id=""),
            "MISSING_PRODUCT_ID",
        ),
        (
            make_record(quantity=-1),
            "NEGATIVE_STOCK",
        ),
        (
            make_record(reorder_level=-1),
            "NEGATIVE_REORDER_LEVEL",
        ),
    ],
)
def test_inventory_validation(spark, record, expected):
    df = spark.createDataFrame([record], SCHEMA)

    result = add_validation_status(
        clean_identifiers(df)
    ).first()

    assert result.validation_status == expected


def test_inventory_stock_classification(spark):
    records = [
        make_record(inventory_id="I001", quantity=0),
        make_record(inventory_id="I002", quantity=5),
        make_record(inventory_id="I003", quantity=20),
        make_record(inventory_id="I004", quantity=-1),
    ]

    df = spark.createDataFrame(records, SCHEMA)

    result = add_stock_status(
        add_validation_status(clean_identifiers(df))
    )

    statuses = {
        row.source_inventory_id: row.stock_status
        for row in result.collect()
    }

    assert statuses == {
        "I001": "OUT_OF_STOCK",
        "I002": "REORDER_REQUIRED",
        "I003": "SUFFICIENT_STOCK",
        "I004": "UNKNOWN",
    }


def test_inventory_freshness_classification(spark):
    today = date.today()

    records = [
        make_record(
            inventory_id="CURRENT",
            updated=today - timedelta(days=30),
        ),
        make_record(
            inventory_id="STALE",
            updated=today - timedelta(days=400),
        ),
        make_record(
            inventory_id="FUTURE",
            updated=today + timedelta(days=10),
        ),
    ]

    df = spark.createDataFrame(records, SCHEMA)

    result = add_inventory_freshness_status(df)

    statuses = {
        row.source_inventory_id:
        row.inventory_freshness_status
        for row in result.collect()
    }

    assert statuses == {
        "CURRENT": "CURRENT",
        "STALE": "STALE",
        "FUTURE": "FUTURE_DATE_REVIEW",
    }



def test_inventory_deduplication_prefers_complete_record(spark):
    from datetime import datetime

    schema = StructType(list(SCHEMA.fields)).add(
        "ingestion_timestamp", "timestamp"
    ).add(
        "source_file", "string"
    )

    today = date.today()

    records = [
        make_record(
            inventory_id="I100",
            product_id="P100",
            updated=today - timedelta(days=2),
        ) + (datetime(2026, 1, 1), "complete.csv"),
        make_record(
            inventory_id="I100",
            product_id=None,
            updated=today,
        ) + (datetime(2026, 1, 2), "incomplete.csv"),
    ]

    df = spark.createDataFrame(records, schema)

    result = deduplicate_inventory(df).collect()

    assert len(result) == 1
    assert result[0].source_product_id == "P100"
    assert result[0].source_file == "complete.csv"


def test_inventory_deduplication_preserves_different_sources(spark):
    from datetime import datetime

    schema = StructType(list(SCHEMA.fields)).add(
        "ingestion_timestamp", "timestamp"
    ).add(
        "source_file", "string"
    )

    records = [
        make_record(inventory_id="I200")
        + (datetime(2026, 1, 1), "retailmax.csv"),
        (
            "freshmart", "I200", "P200", "S200",
            15, 5, date.today(),
            datetime(2026, 1, 1), "freshmart.csv",
        ),
    ]

    df = spark.createDataFrame(records, schema)

    result = deduplicate_inventory(df).collect()

    assert len(result) == 2
    assert {row.source_system for row in result} == {
        "retailmax",
        "freshmart",
    }
