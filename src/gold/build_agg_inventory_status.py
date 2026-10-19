from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark
from src.config.settings import require_safe_write_environment


GOLD_BASE = get_s3_path("gold")

INVENTORY_PATH = get_s3_path(
    "gold", "fact_inventory"
)

PRODUCTS_PATH = get_s3_path(
    "gold", "dim_products"
)

OUTPUT_PATH = get_s3_path(
    "gold", "agg_inventory_status"
)


EXPECTED_INVENTORY = 3600
EXPECTED_PRODUCTS = 1800

EXPECTED_SOURCE_COUNTS = {
    "retailmax": 2000,
    "freshmart": 1600,
}

EXPECTED_STOCK_STATUS = {
    "OUT_OF_STOCK": 3,
    "REORDER_REQUIRED": 436,
    "SUFFICIENT_STOCK": 3161,
}



def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD INVENTORY STATUS "
            "AGGREGATION PIPELINE ==="
        )

        # 1. Read validated Gold tables

        inventory = (
            spark.read.format("delta")
            .load(INVENTORY_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        assert inventory.count() == EXPECTED_INVENTORY
        assert products.count() == EXPECTED_PRODUCTS

        print(
            f"Gold Inventory records: "
            f"{EXPECTED_INVENTORY:,}"
        )

        print(
            f"Gold Products: "
            f"{EXPECTED_PRODUCTS:,}"
        )

        # 2. Validate product keys

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

        print(
            "PASS: Product dimension keys "
            "unique and valid"
        )

        # 3. Validate Inventory product references

        missing_products = (
            inventory.select("product_key")
            .join(
                products.select("product_key"),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_products == 0

        print(
            "PASS: All Inventory references "
            "valid Gold Products"
        )

        # 4. Join Inventory to Product categories
        # and unit selling prices.

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

        # 5. Validate grouping attributes

        invalid_attributes = joined.filter(
            F.col("category").isNull()
            | (F.trim("category") == "")
            | F.col("stock_status").isNull()
            | F.col("freshness_status").isNull()
        ).count()

        assert invalid_attributes == 0

        print(
            "PASS: Category, stock status "
            "and freshness attributes valid"
        )

        # 6. Calculate inventory metrics
        # at the individual inventory-record grain.

        enriched = (
            joined.withColumn(
                "reorder_shortfall",
                F.greatest(
                    F.col("reorder_level")
                    - F.col("stock_quantity"),
                    F.lit(0)
                )
            )
            .withColumn(
                "estimated_retail_value",
                (
                    F.col("stock_quantity")
                    * F.col("unit_price")
                ).cast("decimal(18,2)")
            )
            .withColumn(
                "missing_price_flag",
                F.when(
                    F.col("unit_price").isNull(),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
        )

        # 7. Aggregate by company, category,
        # stock status and freshness.

        aggregated = (
            enriched.groupBy(
                "source_system",
                "category",
                "stock_status",
                "freshness_status"
            )
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
                    "reorder_shortfall"
                ).alias(
                    "reorder_shortfall_units"
                ),
                F.sum(
                    "estimated_retail_value"
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

        # 8. Validate aggregation grain

        duplicates = (
            aggregated.groupBy(
                "source_system",
                "category",
                "stock_status",
                "freshness_status"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0

        print(
            "PASS: Inventory aggregation "
            "grain unique"
        )

        # 9. Reconcile source counts

        source_counts = {
            row["source_system"]: row["records"]
            for row in (
                aggregated.groupBy("source_system")
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
            "PASS: Source-level Inventory "
            "counts reconciled"
        )

        # 10. Reconcile stock classifications

        stock_counts = {
            row["stock_status"]: row["records"]
            for row in (
                aggregated.groupBy("stock_status")
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
            "PASS: Stock status counts reconciled"
        )

        # 11. Reconcile freshness classifications

        freshness_counts = {
            row["freshness_status"]: row["records"]
            for row in (
                aggregated.groupBy("freshness_status")
                .agg(
                    F.sum(
                        "inventory_record_count"
                    ).alias("records")
                )
                .collect()
            )
        }

        expected_freshness_counts = {
            row["freshness_status"]: row["count"]
            for row in (
                inventory.groupBy("freshness_status")
                .count()
                .collect()
            )
        }

        assert (
            set(freshness_counts)
            .issubset({"CURRENT", "STALE"})
        ), f"Unexpected freshness statuses: {freshness_counts}"

        assert sum(freshness_counts.values()) == EXPECTED_INVENTORY

        assert freshness_counts == expected_freshness_counts, (
            "Inventory freshness reconciliation failed: "
            f"aggregated={freshness_counts}, "
            f"fact_inventory={expected_freshness_counts}"
        )

        print(
            "PASS: Freshness status "
            "counts reconciled"
        )

        # 12. Validate stock quantities
        # and reorder shortfalls.

        source_inventory_totals = (
            inventory.agg(
                F.sum("stock_quantity").alias(
                    "stock"
                ),
                F.sum("reorder_level").alias(
                    "reorder"
                )
            )
            .first()
        )

        aggregated_totals = (
            aggregated.agg(
                F.sum(
                    "total_stock_quantity"
                ).alias("stock"),
                F.sum(
                    "total_reorder_level"
                ).alias("reorder")
            )
            .first()
        )

        assert (
            source_inventory_totals["stock"]
            == aggregated_totals["stock"]
        )

        assert (
            source_inventory_totals["reorder"]
            == aggregated_totals["reorder"]
        )

        invalid_shortfalls = aggregated.filter(
            F.col("reorder_shortfall_units") < 0
        ).count()

        assert invalid_shortfalls == 0

        print(
            "PASS: Stock quantities and "
            "reorder thresholds reconciled"
        )

        # 13. Verify missing-price records

        missing_price_records = joined.filter(
            F.col("unit_price").isNull()
        ).count()

        aggregated_missing_prices = (
            aggregated.agg(
                F.sum(
                    "missing_price_record_count"
                ).alias("missing")
            )
            .first()["missing"]
        )

        assert (
            missing_price_records
            == aggregated_missing_prices
        )

        print(
            "PASS: Missing-price records "
            "preserved and counted"
        )

        # 14. Write Gold aggregation

        print(
            "\n=== WRITING AGG_INVENTORY_STATUS ==="
        )

        require_new_s3_target(spark, OUTPUT_PATH)
        (
            aggregated.write
            .format("delta")
            .mode("errorifexists")
            .option(
                "overwriteSchema", "true"
            )
            .save(OUTPUT_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, OUTPUT_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(OUTPUT_PATH)
        )

        row_count = persisted.count()

        print(
            f"PASS: Persisted {row_count:,} "
            "inventory aggregation rows"
        )

        # 15. Display schema

        print(
            "\n=== INVENTORY STATUS SCHEMA ==="
        )

        persisted.printSchema()

        # 16. Show inventory status summary

        print(
            "\n=== INVENTORY STATUS SUMMARY ==="
        )

        persisted.groupBy(
            "source_system",
            "stock_status"
        ).agg(
            F.sum(
                "inventory_record_count"
            ).alias("records"),
            F.sum(
                "total_stock_quantity"
            ).alias("units_on_hand"),
            F.sum(
                "reorder_shortfall_units"
            ).alias("reorder_shortfall_units")
        ).orderBy(
            "source_system",
            "stock_status"
        ).show(truncate=False)

        # 17. Show freshness summary

        print(
            "\n=== INVENTORY FRESHNESS SUMMARY ==="
        )

        persisted.groupBy(
            "source_system",
            "freshness_status"
        ).agg(
            F.sum(
                "inventory_record_count"
            ).alias("records")
        ).orderBy(
            "source_system",
            "freshness_status"
        ).show(truncate=False)

        # 18. Show top categories by shortfall

        print(
            "\n=== TOP CATEGORIES BY "
            "REORDER SHORTFALL ==="
        )

        persisted.groupBy(
            "source_system",
            "category"
        ).agg(
            F.sum(
                "reorder_shortfall_units"
            ).alias("shortfall_units"),
            F.sum(
                "inventory_record_count"
            ).alias("inventory_records")
        ).orderBy(
            F.col("shortfall_units").desc()
        ).show(15, truncate=False)

        # 19. Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark, OUTPUT_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD "
            "AGG_INVENTORY_STATUS CREATED!"
        )

        print(
            f"Gold path: {OUTPUT_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
