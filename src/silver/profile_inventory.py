
import os
import sys

from pyspark.sql import functions as F
from src.config.settings import get_s3_path

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_inventory import (
    transform_retailmax_inventory,
    transform_freshmart_inventory,
)


SILVER_BASE = get_s3_path("silver")


def missing(column_name):
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == "")
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== INVENTORY DATA QUALITY PROFILING ===")

        # 1. Read Bronze inventory.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/inventory")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/inventory")
        )

        retailmax = transform_retailmax_inventory(
            retailmax_bronze
        )

        freshmart = transform_freshmart_inventory(
            freshmart_bronze
        )

        inventory = (
            retailmax.unionByName(freshmart).cache()
        )

        print("\n=== 1. INVENTORY COUNTS ===")

        inventory.groupBy(
            "source_system"
        ).count().show()

        # 2. Duplicate inventory IDs.
        print("\n=== 2. DUPLICATE INVENTORY IDS ===")

        duplicates = (
            inventory.groupBy(
                "source_system",
                "source_inventory_id"
            )
            .count()
            .filter(F.col("count") > 1)
        )

        print(
            "Duplicate inventory ID groups:",
            duplicates.count()
        )

        duplicates.show(15, truncate=False)

        # 3. Missing identifiers.
        print("\n=== 3. MISSING IDENTIFIERS ===")

        inventory.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing("source_inventory_id"), 1
                ).otherwise(0)
            ).alias("missing_inventory_ids"),

            F.sum(
                F.when(
                    missing("source_product_id"), 1
                ).otherwise(0)
            ).alias("missing_product_ids"),

            F.sum(
                F.when(
                    missing("source_store_id"), 1
                ).otherwise(0)
            ).alias("missing_store_ids")
        ).show()

        # 4. Stock and reorder quality.
        print("\n=== 4. STOCK QUANTITY QUALITY ===")

        inventory.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("stock_quantity").isNull(), 1
                ).otherwise(0)
            ).alias("null_stock_quantities"),

            F.sum(
                F.when(
                    F.col("stock_quantity") < 0, 1
                ).otherwise(0)
            ).alias("negative_stock_quantities"),

            F.sum(
                F.when(
                    F.col("stock_quantity") == 0, 1
                ).otherwise(0)
            ).alias("zero_stock_quantities"),

            F.sum(
                F.when(
                    F.col("reorder_level").isNull(), 1
                ).otherwise(0)
            ).alias("null_reorder_levels"),

            F.sum(
                F.when(
                    F.col("reorder_level") < 0, 1
                ).otherwise(0)
            ).alias("negative_reorder_levels")
        ).show(truncate=False)

        # 5. Low stock and out-of-stock.
        print("\n=== 5. INVENTORY BUSINESS STATUS ===")

        inventory.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("stock_quantity") == 0, 1
                ).otherwise(0)
            ).alias("out_of_stock"),

            F.sum(
                F.when(
                    (F.col("stock_quantity") >= 0)
                    & (F.col("reorder_level") >= 0)
                    & (
                        F.col("stock_quantity")
                        <= F.col("reorder_level")
                    ),
                    1
                ).otherwise(0)
            ).alias("at_or_below_reorder_level")
        ).show()

        # 6. Inventory dates and freshness.
        print("\n=== 6. INVENTORY DATE QUALITY ===")

        inventory.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("last_updated").isNull(), 1
                ).otherwise(0)
            ).alias("null_update_dates"),

            F.sum(
                F.when(
                    F.col("last_updated")
                    > F.current_date(), 1
                ).otherwise(0)
            ).alias("future_update_dates"),

            F.sum(
                F.when(
                    F.col("last_updated")
                    < F.date_sub(F.current_date(), 365),
                    1
                ).otherwise(0)
            ).alias("older_than_365_days"),

            F.min("last_updated").alias(
                "earliest_update"
            ),

            F.max("last_updated").alias(
                "latest_update"
            )
        ).show(truncate=False)

        # 7. Duplicate product/store combinations.
        print(
            "\n=== 7. DUPLICATE PRODUCT/STORE COMBINATIONS ==="
        )

        repeated_locations = (
            inventory.groupBy(
                "source_system",
                "source_product_id",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
        )

        print(
            "Repeated product/store groups:",
            repeated_locations.count()
        )

        repeated_locations.orderBy(
            F.desc("count")
        ).show(15, truncate=False)

        # 8. Read existing Silver master data.
        print(
            "\n=== 8. PRODUCT REFERENTIAL INTEGRITY ==="
        )

        products = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/products")
            .select("product_key")
            .distinct()
        )

        unmatched_products = inventory.join(
            products,
            on="product_key",
            how="left_anti"
        )

        unmatched_product_count = (
            unmatched_products.count()
        )

        print(
            "Inventory records with missing products:",
            unmatched_product_count
        )

        unmatched_products.select(
            "source_system",
            "source_inventory_id",
            "source_product_id",
            "product_key"
        ).show(15, truncate=False)

        # 9. Store referential integrity.
        print(
            "\n=== 9. STORE REFERENTIAL INTEGRITY ==="
        )

        stores = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/stores")
            .select("store_key")
            .distinct()
        )

        unmatched_stores = inventory.join(
            stores,
            on="store_key",
            how="left_anti"
        )

        unmatched_store_count = (
            unmatched_stores.count()
        )

        print(
            "Inventory records with missing stores:",
            unmatched_store_count
        )

        unmatched_stores.select(
            "source_system",
            "source_inventory_id",
            "source_store_id",
            "store_key"
        ).show(15, truncate=False)

        # 10. Sample problematic stock records.
        print("\n=== 10. SAMPLE INVALID STOCK RECORDS ===")

        inventory.filter(
            F.col("stock_quantity").isNull()
            | (F.col("stock_quantity") < 0)
            | F.col("reorder_level").isNull()
            | (F.col("reorder_level") < 0)
        ).select(
            "source_system",
            "source_inventory_id",
            "product_key",
            "store_key",
            "stock_quantity",
            "reorder_level"
        ).show(20, truncate=False)

        inventory.unpersist()

        print("\n=== PROFILING COMPLETED ===")
        print(
            "\nNOTE: No Silver Inventory table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
