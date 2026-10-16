
import os
import sys
from decimal import Decimal

from pyspark.sql import functions as F
from delta.tables import DeltaTable


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"
GOLD_BASE = f"s3a://{BUCKET}/gold"

INVENTORY_PATH = f"{GOLD_BASE}/fact_inventory"
PRODUCTS_PATH = f"{GOLD_BASE}/dim_products"
AGG_PATH = f"{GOLD_BASE}/agg_inventory_status"

EXPECTED_INVENTORY = 3600
EXPECTED_PRODUCTS = 1800
EXPECTED_AGG_ROWS = 59

EXPECTED_SOURCE_COUNTS = {
    "retailmax": 2000,
    "freshmart": 1600,
}

EXPECTED_STOCK_STATUS = {
    "OUT_OF_STOCK": 3,
    "REORDER_REQUIRED": 436,
    "SUFFICIENT_STOCK": 3161,
}

EXPECTED_FRESHNESS = {
    "CURRENT": 889,
    "STALE": 2711,
}

EXPECTED_SHORTFALL = 14768


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD INVENTORY "
            "STATUS VALIDATION ==="
        )

        # 1. Read persisted Gold tables

        inventory = (
            spark.read.format("delta")
            .load(INVENTORY_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        actual = (
            spark.read.format("delta")
            .load(AGG_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, AGG_PATH
        )

        print(
            "PASS: Inventory Status is a Delta table"
        )

        # 2. Verify record counts

        assert inventory.count() == EXPECTED_INVENTORY
        assert products.count() == EXPECTED_PRODUCTS
        assert actual.count() == EXPECTED_AGG_ROWS

        print(
            "PASS: Fact, Product dimension and "
            "aggregation counts verified"
        )

        # 3. Validate exact schema

        expected_types = {
            "source_system": "string",
            "category": "string",
            "stock_status": "string",
            "freshness_status": "string",
            "inventory_record_count": "bigint",
            "distinct_products": "bigint",
            "distinct_stores": "bigint",
            "total_stock_quantity": "bigint",
            "total_reorder_level": "bigint",
            "reorder_shortfall_units": "bigint",
            "estimated_inventory_value": "decimal(18,2)",
            "missing_price_record_count": "bigint",
        }

        assert dict(actual.dtypes) == expected_types

        print(
            "PASS: Inventory Status schema verified"
        )

        # 4. Verify unique aggregation grain

        grain = [
            "source_system",
            "category",
            "stock_status",
            "freshness_status",
        ]

        duplicates = (
            actual.groupBy(*grain)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        null_grain = actual.filter(
            F.col("source_system").isNull()
            | F.col("category").isNull()
            | F.col("stock_status").isNull()
            | F.col("freshness_status").isNull()
        ).count()

        assert duplicates == 0
        assert null_grain == 0

        print(
            "PASS: Inventory aggregation grain "
            "unique and complete"
        )

        # 5. Validate Product dimension keys

        duplicate_products = (
            products.groupBy("product_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        null_products = products.filter(
            F.col("product_key").isNull()
        ).count()

        assert duplicate_products == 0
        assert null_products == 0

        missing_product_refs = (
            inventory.select("product_key")
            .join(
                products.select("product_key"),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_product_refs == 0

        print(
            "PASS: Product keys and "
            "Inventory references verified"
        )

        # 6. Independently join Inventory
        # to Products.

        product_lookup = products.select(
            "product_key",
            F.col("source_system").alias(
                "product_source_system"
            ),
            "category",
            "unit_price"
        )

        joined = inventory.join(
            product_lookup,
            on="product_key",
            how="inner"
        )

        assert joined.count() == EXPECTED_INVENTORY

        source_mismatches = joined.filter(
            ~F.col("source_system").eqNullSafe(
                F.col("product_source_system")
            )
        ).count()

        assert source_mismatches == 0

        print(
            "PASS: Inventory and Product "
            "source systems match"
        )

        # 7. Independently calculate
        # record-level replenishment and
        # estimated retail value.

        enriched = (
            joined.withColumn(
                "expected_shortfall",
                F.greatest(
                    F.col("reorder_level")
                    - F.col("stock_quantity"),
                    F.lit(0)
                )
            )
            .withColumn(
                "expected_retail_value",
                (
                    F.col("stock_quantity")
                    * F.col("unit_price")
                ).cast("decimal(18,2)")
            )
            .withColumn(
                "missing_price_flag",
                F.when(
                    F.col("unit_price").isNull(),
                    1
                ).otherwise(0)
            )
        )

        # 8. Reconstruct all 59 rows.

        expected = (
            enriched.groupBy(*grain)
            .agg(
                F.count("*").alias(
                    "inventory_record_count"
                ),
                F.countDistinct(
                    "product_key"
                ).alias(
                    "distinct_products"
                ),
                F.countDistinct(
                    "store_key"
                ).alias(
                    "distinct_stores"
                ),
                F.sum(
                    "stock_quantity"
                ).alias(
                    "total_stock_quantity"
                ),
                F.sum(
                    "reorder_level"
                ).alias(
                    "total_reorder_level"
                ),
                F.sum(
                    "expected_shortfall"
                ).alias(
                    "reorder_shortfall_units"
                ),
                F.sum(
                    "expected_retail_value"
                ).cast("decimal(18,2)").alias(
                    "estimated_inventory_value"
                ),
                F.sum(
                    "missing_price_flag"
                ).alias(
                    "missing_price_record_count"
                )
            )
        )

        assert expected.count() == EXPECTED_AGG_ROWS

        # 9. Compare every reconstructed
        # row against the persisted Delta table.

        columns = list(expected.columns)

        missing_or_changed = (
            expected.select(*columns)
            .exceptAll(
                actual.select(*columns)
            )
            .count()
        )

        unexpected_or_changed = (
            actual.select(*columns)
            .exceptAll(
                expected.select(*columns)
            )
            .count()
        )

        assert missing_or_changed == 0, (
            f"{missing_or_changed} expected "
            "rows missing or changed"
        )

        assert unexpected_or_changed == 0, (
            f"{unexpected_or_changed} unexpected "
            "rows found"
        )

        print(
            "PASS: Every Inventory aggregation "
            "row matches independent calculations"
        )

        # 10. Verify source-level counts

        source_counts = {
            row["source_system"]: row["records"]
            for row in (
                actual.groupBy("source_system")
                .agg(
                    F.sum(
                        "inventory_record_count"
                    ).alias("records")
                )
                .collect()
            )
        }

        assert source_counts == EXPECTED_SOURCE_COUNTS

        print(
            "PASS: RetailMax and FreshMart "
            "inventory counts reconciled"
        )

        # 11. Verify stock status counts

        stock_counts = {
            row["stock_status"]: row["records"]
            for row in (
                actual.groupBy("stock_status")
                .agg(
                    F.sum(
                        "inventory_record_count"
                    ).alias("records")
                )
                .collect()
            )
        }

        assert stock_counts == EXPECTED_STOCK_STATUS

        print(
            "PASS: Stock classifications reconciled"
        )

        # 12. Verify freshness counts

        freshness_counts = {
            row["freshness_status"]: row["records"]
            for row in (
                actual.groupBy("freshness_status")
                .agg(
                    F.sum(
                        "inventory_record_count"
                    ).alias("records")
                )
                .collect()
            )
        }

        assert freshness_counts == EXPECTED_FRESHNESS

        print(
            "PASS: Freshness classifications reconciled"
        )

        # 13. Verify total stock quantities,
        # reorder thresholds and shortfalls.

        inventory_totals = inventory.agg(
            F.sum("stock_quantity").alias("stock"),
            F.sum("reorder_level").alias("reorder")
        ).first()

        aggregate_totals = actual.agg(
            F.sum("total_stock_quantity").alias(
                "stock"
            ),
            F.sum("total_reorder_level").alias(
                "reorder"
            ),
            F.sum("reorder_shortfall_units").alias(
                "shortfall"
            )
        ).first()

        assert (
            inventory_totals["stock"]
            == aggregate_totals["stock"]
        )

        assert (
            inventory_totals["reorder"]
            == aggregate_totals["reorder"]
        )

        assert (
            aggregate_totals["shortfall"]
            == EXPECTED_SHORTFALL
        )

        print(
            "PASS: Stock quantities, thresholds "
            "and reorder shortfalls reconciled"
        )

        # 14. Verify missing-price treatment

        missing_price_records = joined.filter(
            F.col("unit_price").isNull()
        ).count()

        actual_missing = actual.agg(
            F.sum(
                "missing_price_record_count"
            ).alias("missing")
        ).first()["missing"]

        assert missing_price_records == actual_missing

        # The estimated value must remain NULL
        # for a group where every product price
        # is missing, rather than falsely showing
        # a known zero value.

        incorrectly_zeroed = (
            actual.filter(
                (
                    F.col("missing_price_record_count")
                    == F.col("inventory_record_count")
                )
                & F.col(
                    "estimated_inventory_value"
                ).isNotNull()
            )
            .count()
        )

        assert incorrectly_zeroed == 0

        print(
            "PASS: Missing-price records "
            "and unknown valuations preserved"
        )

        # 15. Validate non-negative metrics

        invalid_metrics = actual.filter(
            (F.col("inventory_record_count") <= 0)
            | (F.col("distinct_products") <= 0)
            | (F.col("distinct_stores") <= 0)
            | (F.col("total_stock_quantity") < 0)
            | (F.col("total_reorder_level") < 0)
            | (F.col("reorder_shortfall_units") < 0)
            | (F.col("missing_price_record_count") < 0)
            | (
                F.col("missing_price_record_count")
                > F.col("inventory_record_count")
            )
        ).count()

        assert invalid_metrics == 0

        print(
            "PASS: Inventory metrics "
            "are within valid bounds"
        )

        # 16. Show Delta history

        print(
            "\n=== GOLD INVENTORY STATUS "
            "DELTA HISTORY ==="
        )

        DeltaTable.forPath(
            spark, AGG_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD INVENTORY "
            "STATUS VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
