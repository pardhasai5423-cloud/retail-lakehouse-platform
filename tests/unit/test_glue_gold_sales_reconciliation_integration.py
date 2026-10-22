"""Real Delta integration test for Gold sales snapshot reconciliation."""

from datetime import datetime
from decimal import Decimal

import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from src.glue.gold_writer import merge_gold_table


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("gold-sales-reconciliation-integration")
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

    session = (
        builder
        .config(
            "spark.jars",
            ",".join([
                str(Path.home() / ".ivy2" / "jars" / "io.delta_delta-spark_2.12-3.3.2.jar"),
                str(Path.home() / ".ivy2" / "jars" / "io.delta_delta-storage-3.3.2.jar"),
            ]),
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_gold_sales_snapshot_reconciliation(spark, tmp_path):
    target = str(tmp_path / "gold" / "fact_sales")

    schema = T.StructType([
        T.StructField("transaction_key", T.StringType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("total_amount", T.DecimalType(18, 2)),
        T.StructField("ingestion_timestamp", T.TimestampType()),
    ])

    timestamp1 = datetime(2026, 10, 19, 10, 0, 0)
    timestamp2 = datetime(2026, 10, 20, 10, 0, 0)

    initial = spark.createDataFrame([
        ("retailmax:T1", "retailmax", Decimal("20.00"), timestamp1),
        ("retailmax:T2", "retailmax", Decimal("30.00"), timestamp1),
    ], schema)

    result = merge_gold_table(
        spark,
        initial,
        "fact_sales",
        target,
        full_snapshot=True,
    )
    assert result["status"] == "created"

    first = spark.read.format("delta").load(target)
    assert first.count() == 2

    # T1 is no longer present in the valid Silver snapshot.
    # T2 remains valid and receives a newer correction.
    corrected = spark.createDataFrame([
        ("retailmax:T2", "retailmax", Decimal("35.00"), timestamp2),
    ], schema)

    result = merge_gold_table(
        spark,
        corrected,
        "fact_sales",
        target,
        full_snapshot=True,
    )
    assert result["status"] == "merged"

    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 1
    assert rows[0]["transaction_key"] == "retailmax:T2"
    assert rows[0]["total_amount"] == Decimal("35.00")

    # Retry the same snapshot: no duplicates or resurrected sales.
    merge_gold_table(
        spark,
        corrected,
        "fact_sales",
        target,
        full_snapshot=True,
    )

    rows = spark.read.format("delta").load(target).collect()
    assert len(rows) == 1
    assert rows[0]["total_amount"] == Decimal("35.00")


def test_empty_sales_snapshot_cannot_delete_gold(spark, tmp_path):
    target = str(tmp_path / "gold" / "fact_sales")

    schema = T.StructType([
        T.StructField("transaction_key", T.StringType()),
        T.StructField("ingestion_timestamp", T.TimestampType()),
    ])

    empty = spark.createDataFrame([], schema)

    with pytest.raises(ValueError, match="Empty sales"):
        merge_gold_table(
            spark,
            empty,
            "fact_sales",
            target,
            full_snapshot=True,
        )
