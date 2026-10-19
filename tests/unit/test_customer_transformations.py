from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession

from src.silver.transform_customers import (
    transform_retailmax_customers,
    transform_freshmart_customers,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("RetailLakehouseCustomerUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def retailmax_data(spark):
    return spark.createDataFrame(
        [(
            "C001", " Alice ", " Smith ",
            " ALICE@EXAMPLE.COM ", " 5551234 ",
            " Phoenix ", " az ", "2026-01-15",
            datetime(2026, 1, 16), "batch-001",
            "retailmax.csv",
        )],
        [
            "customer_id", "first_name", "last_name",
            "email", "phone", "city", "state",
            "created_at", "_ingestion_timestamp",
            "_batch_id", "_source_file",
        ],
    )


def freshmart_data(spark):
    return spark.createDataFrame(
        [(
            "C001", " Bob ", " Jones ",
            " BOB@EXAMPLE.COM ", " 5555678 ",
            " Dallas ", " tx ", "01/20/2026",
            datetime(2026, 1, 21), "batch-002",
            "freshmart.csv",
        )],
        [
            "cust_no", "fname", "lname",
            "email_address", "mobile_no",
            "city_name", "state_code", "signup_date",
            "_ingestion_timestamp", "_batch_id",
            "_source_file",
        ],
    )


def test_retailmax_customer_standardization(spark):
    row = transform_retailmax_customers(
        retailmax_data(spark)
    ).first()

    assert row.source_system == "retailmax"
    assert row.source_customer_id == "C001"
    assert row.customer_key == "retailmax:C001"
    assert row.first_name == "Alice"
    assert row.last_name == "Smith"
    assert row.email == "alice@example.com"
    assert row.city == "Phoenix"
    assert row.state == "AZ"
    assert row.registration_date == date(2026, 1, 15)
    assert row.batch_id == "batch-001"
    assert row.source_file == "retailmax.csv"
    assert row.ingestion_timestamp == datetime(2026, 1, 16)


def test_freshmart_customer_standardization(spark):
    row = transform_freshmart_customers(
        freshmart_data(spark)
    ).first()

    assert row.source_system == "freshmart"
    assert row.source_customer_id == "C001"
    assert row.customer_key == "freshmart:C001"
    assert row.first_name == "Bob"
    assert row.last_name == "Jones"
    assert row.email == "bob@example.com"
    assert row.city == "Dallas"
    assert row.state == "TX"
    assert row.registration_date == date(2026, 1, 20)
    assert row.batch_id == "batch-002"
    assert row.source_file == "freshmart.csv"
    assert row.ingestion_timestamp == datetime(2026, 1, 21)


def test_customer_keys_remain_distinct_across_sources(spark):
    retailmax = transform_retailmax_customers(
        retailmax_data(spark)
    )

    freshmart = transform_freshmart_customers(
        freshmart_data(spark)
    )

    keys = {
        row.customer_key
        for row in retailmax.unionByName(
            freshmart
        ).select("customer_key").collect()
    }

    assert keys == {
        "retailmax:C001",
        "freshmart:C001",
    }
