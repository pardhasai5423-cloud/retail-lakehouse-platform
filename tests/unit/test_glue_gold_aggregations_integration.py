"""Real Spark tests for Gold aggregate business calculations."""

from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.glue.gold_aggregations import (
    build_daily_sales,
    build_store_performance,
    build_category_sales,
    build_inventory_status,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-aggregation-integration")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_gold_aggregations_with_real_spark(spark):
    sales_schema = T.StructType([
        T.StructField("date_key", T.IntegerType()),
        T.StructField("transaction_date", T.DateType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("store_key", T.StringType()),
        T.StructField("product_key", T.StringType()),
        T.StructField("customer_key", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DecimalType(18, 2)),
        T.StructField("discount_amount", T.DecimalType(18, 2)),
        T.StructField("total_amount", T.DecimalType(18, 2)),
    ])

    from datetime import date

    sales = spark.createDataFrame([
        (
            20261020, date(2026, 10, 20), "retailmax",
            "retailmax:S1", "retailmax:P1", "retailmax:C1",
            2, Decimal("10.00"), Decimal("2.00"), Decimal("18.00"),
        ),
        (
            20261020, date(2026, 10, 20), "retailmax",
            "retailmax:S1", "retailmax:P1", "retailmax:C2",
            1, Decimal("10.00"), Decimal("0.00"), Decimal("10.00"),
        ),
    ], sales_schema)

    stores = spark.createDataFrame([
        ("retailmax:S1", "retailmax", "S1", "Store One", "A", "VA"),
        ("retailmax:S2", "retailmax", "S2", "Store Two", "B", "VA"),
    ], [
        "store_key", "source_system", "source_store_id",
        "store_name", "city", "state",
    ])

    products = spark.createDataFrame([
        ("retailmax:P1", "retailmax", "Grocery", Decimal("10.00")),
    ], T.StructType([
        T.StructField("product_key", T.StringType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("unit_price", T.DecimalType(18, 2)),
    ]))

    inventory = spark.createDataFrame([
        (
            "retailmax", "retailmax:P1", "retailmax:S1",
            "LOW_STOCK", "CURRENT", 3, 5,
        ),
    ], [
        "source_system", "product_key", "store_key",
        "stock_status", "freshness_status",
        "stock_quantity", "reorder_level",
    ])

    daily = build_daily_sales(sales).collect()
    assert len(daily) == 1
    assert daily[0]["transaction_count"] == 2
    assert daily[0]["units_sold"] == 3
    assert daily[0]["net_revenue"] == Decimal("28.00")
    assert daily[0]["average_transaction_value"] == Decimal("14.00")

    performance = {
        row["store_key"]: row
        for row in build_store_performance(sales, stores).collect()
    }

    assert len(performance) == 2
    assert performance["retailmax:S1"]["transaction_count"] == 2
    assert performance["retailmax:S2"]["transaction_count"] == 0
    assert performance["retailmax:S2"]["net_revenue"] == Decimal("0.00")

    category = build_category_sales(sales, products).collect()
    assert len(category) == 1
    assert category[0]["category"] == "Grocery"
    assert category[0]["transaction_count"] == 2
    assert category[0]["distinct_customers"] == 2
    assert category[0]["net_revenue"] == Decimal("28.00")

    stock = build_inventory_status(inventory, products).collect()
    assert len(stock) == 1
    assert stock[0]["reorder_shortfall_units"] == 2
    assert stock[0]["estimated_inventory_value"] == Decimal("30.00")
    assert stock[0]["missing_price_record_count"] == 0


def test_missing_product_reference_rejected(spark):
    sales = spark.createDataFrame(
        [("retailmax:P999", "retailmax")],
        ["product_key", "source_system"],
    )

    products = spark.createDataFrame(
        [("retailmax:P1", "retailmax", "Grocery", Decimal("10.00"))],
        T.StructType([
            T.StructField("product_key", T.StringType()),
            T.StructField("source_system", T.StringType()),
            T.StructField("category", T.StringType()),
            T.StructField("unit_price", T.DecimalType(18, 2)),
        ]),
    )

    with pytest.raises(ValueError, match="missing products"):
        build_category_sales(sales, products)


def test_protected_direct_gold_target_rejected():
    from unittest.mock import MagicMock
    from src.glue.gold_writer import merge_gold_table

    with pytest.raises(ValueError, match="Protected"):
        merge_gold_table(
            MagicMock(),
            MagicMock(),
            "dim_customers",
            "s3://bucket/test-runs/run-001/gold/dim_customers",
        )
