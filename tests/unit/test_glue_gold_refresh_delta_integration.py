"""Real local Delta tests for Gold date and aggregate refreshes."""

from datetime import date
from decimal import Decimal

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.glue.gold_refresh import (
    merge_date_dimension,
    execute_aggregate_refresh,
)


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-refresh-delta-integration")
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


def test_date_dimension_merge_is_idempotent(spark, tmp_path):
    target = str(tmp_path / "gold" / "dim_date")

    schema = T.StructType([
        T.StructField("date_key", T.IntegerType(), False),
        T.StructField("calendar_date", T.DateType(), False),
    ])

    first = spark.createDataFrame([
        (20261019, date(2026, 10, 19)),
        (20261020, date(2026, 10, 20)),
    ], schema)

    created = merge_date_dimension(spark, first, target)
    assert created["status"] == "created"

    repeated = merge_date_dimension(spark, first, target)
    assert repeated["status"] == "merged"

    dates = spark.read.format("delta").load(target)
    assert dates.count() == 2

    additional = spark.createDataFrame([
        (20261020, date(2026, 10, 20)),
        (20261021, date(2026, 10, 21)),
    ], schema)

    merge_date_dimension(spark, additional, target)

    dates = spark.read.format("delta").load(target)
    assert dates.count() == 3
    assert {r.date_key for r in dates.collect()} == {
        20261019, 20261020, 20261021
    }


def test_aggregate_refresh_replaces_old_totals(spark, tmp_path):
    base = str(tmp_path / "gold")
    sales_path = f"{base}/fact_sales"

    schema = T.StructType([
        T.StructField("date_key", T.IntegerType()),
        T.StructField("transaction_date", T.DateType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DecimalType(18, 2)),
        T.StructField("discount_amount", T.DecimalType(18, 2)),
        T.StructField("total_amount", T.DecimalType(18, 2)),
    ])

    original = spark.createDataFrame([
        (
            20261020, date(2026, 10, 20),
            "retailmax", 2,
            Decimal("10.00"), Decimal("2.00"),
            Decimal("18.00"),
        )
    ], schema)

    original.write.format("delta").save(sales_path)

    first = execute_aggregate_refresh(
        spark, "agg_daily_sales", base, allow_writes=True
    )
    assert first["status"] == "refreshed"

    second = execute_aggregate_refresh(
        spark, "agg_daily_sales", base, allow_writes=True
    )
    assert second["status"] == "refreshed"

    agg_path = f"{base}/agg_daily_sales"
    rows = spark.read.format("delta").load(agg_path).collect()

    assert len(rows) == 1
    assert rows[0]["transaction_count"] == 1
    assert rows[0]["net_revenue"] == Decimal("18.00")

    corrected = spark.createDataFrame([
        (
            20261020, date(2026, 10, 20),
            "retailmax", 2,
            Decimal("10.00"), Decimal("0.00"),
            Decimal("20.00"),
        )
    ], schema)

    corrected.write.format("delta").mode("overwrite").save(
        sales_path
    )

    execute_aggregate_refresh(
        spark, "agg_daily_sales", base, allow_writes=True
    )

    updated = spark.read.format("delta").load(agg_path).collect()

    assert len(updated) == 1
    assert updated[0]["transaction_count"] == 1
    assert updated[0]["net_revenue"] == Decimal("20.00")
    assert updated[0]["discount_amount"] == Decimal("0.00")
