
import os
import sys

from datetime import date, timedelta

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
SILVER_BASE = f"s3a://{BUCKET}/silver"

INVENTORY_PATH = f"{SILVER_BASE}/inventory"
PRODUCTS_PATH = f"{SILVER_BASE}/products"
STORES_PATH = f"{SILVER_BASE}/stores"


def check_missing_strings(df, columns):
    condition = F.lit(False)

    for name in columns:
        condition = (
            condition
            | F.col(name).isNull()
            | (F.trim(F.col(name)) == "")
        )

    return df.filter(condition).count()


def check_references(inventory, reference, key, name):
    """
    Verify that every inventory record has
    a corresponding reference/master record.
    """

    reference_keys = reference.select(key).distinct()

    unmatched = inventory.join(
        reference_keys,
        on=key,
        how="left_anti"
    )

    unmatched_count = unmatched.count()

    assert unmatched_count == 0, (
        f"{name}: {unmatched_count} unmatched references"
    )

    print(f"PASS: All {name} references valid")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER INVENTORY VALIDATION ===")

        # -----------------------------------
        # 1. Read persisted Delta tables
        # -----------------------------------

        inventory = (
            spark.read.format("delta")
            .load(INVENTORY_PATH)
            .cache()
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        stores = (
            spark.read.format("delta")
            .load(STORES_PATH)
        )

        # -----------------------------------
        # 2. Record counts
        # -----------------------------------

        total = inventory.count()

        assert total == 3600, (
            f"Expected 3600 records, found {total}"
        )

        print("PASS: Total Silver Inventory = 3,600")

        source_counts = {
            row["source_system"]: row["count"]
            for row in inventory.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 2000,
            "freshmart": 1600
        }

        print("PASS: RetailMax = 2,000")
        print("PASS: FreshMart = 1,600")

        # -----------------------------------
        # 3. Schema verification
        # -----------------------------------

        expected_types = {
            "source_system": "string",
            "source_inventory_id": "string",
            "inventory_key": "string",
            "source_product_id": "string",
            "product_key": "string",
            "source_store_id": "string",
            "store_key": "string",
            "stock_quantity": "int",
            "reorder_level": "int",
            "last_updated": "date",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
            "validation_status": "string",
            "stock_status": "string",
            "inventory_freshness_status": "string",
        }

        actual_types = dict(inventory.dtypes)

        for column, expected_type in expected_types.items():
            assert column in actual_types, (
                f"Missing column: {column}"
            )

            assert actual_types[column] == expected_type, (
                f"{column}: expected {expected_type}, "
                f"found {actual_types[column]}"
            )

        print("PASS: Silver Inventory schema verified")

        # -----------------------------------
        # 4. Mandatory identifiers
        # -----------------------------------

        required_strings = [
            "source_system",
            "source_inventory_id",
            "inventory_key",
            "source_product_id",
            "product_key",
            "source_store_id",
            "store_key",
        ]

        missing_count = check_missing_strings(
            inventory,
            required_strings
        )

        assert missing_count == 0

        print("PASS: No missing mandatory identifiers")

        # -----------------------------------
        # 5. Duplicate inventory IDs
        # -----------------------------------

        duplicates = (
            inventory.groupBy(
                "source_system",
                "source_inventory_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0

        print("PASS: No duplicate inventory IDs")

        # -----------------------------------
        # 6. Composite key consistency
        # -----------------------------------

        key_pairs = [
            ("source_inventory_id", "inventory_key"),
            ("source_product_id", "product_key"),
            ("source_store_id", "store_key"),
        ]

        for source_column, key_column in key_pairs:
            expected_key = F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col(source_column)
            )

            invalid_keys = inventory.filter(
                ~F.col(key_column).eqNullSafe(
                    expected_key
                )
            ).count()

            assert invalid_keys == 0, (
                f"Invalid composite keys: {key_column}"
            )

        print("PASS: Inventory, Product, Store keys valid")

        # -----------------------------------
        # 7. Numeric and date quality
        # -----------------------------------

        invalid_values = inventory.filter(
            F.col("stock_quantity").isNull()
            | (F.col("stock_quantity") < 0)
            | F.col("reorder_level").isNull()
            | (F.col("reorder_level") < 0)
            | F.col("last_updated").isNull()
            | (F.col("last_updated") > F.current_date())
        ).count()

        assert invalid_values == 0

        print("PASS: Quantities, reorder levels, dates valid")

        # -----------------------------------
        # 8. Validation status
        # -----------------------------------

        invalid_statuses = inventory.filter(
            F.col("validation_status").isNull()
            | (F.col("validation_status") != "VALID")
        ).count()

        assert invalid_statuses == 0

        print("PASS: All records have VALID status")

        # -----------------------------------
        # 9. Independently verify stock status
        # -----------------------------------

        expected_stock_status = (
            F.when(
                F.col("stock_quantity") == 0,
                "OUT_OF_STOCK"
            )
            .when(
                F.col("stock_quantity")
                <= F.col("reorder_level"),
                "REORDER_REQUIRED"
            )
            .otherwise("SUFFICIENT_STOCK")
        )

        wrong_stock_status = inventory.filter(
            ~F.col("stock_status").eqNullSafe(
                expected_stock_status
            )
        ).count()

        assert wrong_stock_status == 0

        print("PASS: Stock status logic verified")

        stock_counts = {
            row["stock_status"]: row["count"]
            for row in inventory.groupBy(
                "stock_status"
            ).count().collect()
        }

        assert stock_counts == {
            "OUT_OF_STOCK": 3,
            "REORDER_REQUIRED": 436,
            "SUFFICIENT_STOCK": 3161,
        }

        print("PASS: Stock status counts verified")

        # -----------------------------------
        # 10. Independently verify freshness
        # -----------------------------------

        # Use the same calendar-day threshold
        # as the cleansing pipeline.
        expected_freshness = (
            F.when(
                F.col("last_updated")
                < F.date_sub(F.current_date(), 365),
                "STALE"
            )
            .otherwise("CURRENT")
        )

        wrong_freshness = inventory.filter(
            ~F.col(
                "inventory_freshness_status"
            ).eqNullSafe(expected_freshness)
        ).count()

        assert wrong_freshness == 0, (
            "Freshness statuses do not match "
            "the current 365-day threshold"
        )

        print("PASS: Freshness classification verified")

        print("\n=== FRESHNESS COUNTS ===")

        inventory.groupBy(
            "inventory_freshness_status"
        ).count().show()

        # -----------------------------------
        # 11. Lineage metadata
        # -----------------------------------

        missing_lineage = inventory.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim(F.col("batch_id")) == "")
            | F.col("source_file").isNull()
            | (F.trim(F.col("source_file")) == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Ingestion lineage metadata preserved")

        # -----------------------------------
        # 12. Referential integrity
        # -----------------------------------

        print(
            "\n=== MASTER DATA REFERENTIAL INTEGRITY ==="
        )

        check_references(
            inventory,
            products,
            "product_key",
            "Product"
        )

        check_references(
            inventory,
            stores,
            "store_key",
            "Store"
        )

        # -----------------------------------
        # 13. Inventory grain verification
        # -----------------------------------

        repeated_product_stores = (
            inventory.groupBy(
                "source_system",
                "source_product_id",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            "Repeated product/store combinations:",
            repeated_product_stores
        )

        assert repeated_product_stores == 0

        print("PASS: Product/store combinations unique")

        # -----------------------------------
        # 14. Delta Lake verification
        # -----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            INVENTORY_PATH
        )

        print("PASS: Valid Inventory Delta table")

        print("\n=== DELTA TRANSACTION HISTORY ===")

        DeltaTable.forPath(
            spark,
            INVENTORY_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL SILVER INVENTORY "
            "VALIDATION CHECKS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
