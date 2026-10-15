
import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_inventory import (
    transform_retailmax_inventory,
    transform_freshmart_inventory,
)

from clean_inventory import (
    clean_identifiers,
    add_validation_status,
    add_stock_status,
    add_inventory_freshness_status,
    deduplicate_inventory,
)


BUCKET = "retail-lakehouse-parsam-2026"

SILVER_BASE = f"s3a://{BUCKET}/silver"
SILVER_INVENTORY_PATH = f"{SILVER_BASE}/inventory"


def assert_no_missing_references(
    inventory,
    reference_df,
    key_column,
    reference_name,
):
    """
    Fail the write if an inventory record
    references a missing master-data record.
    """

    reference_keys = (
        reference_df
        .select(key_column)
        .distinct()
    )

    unmatched = inventory.join(
        reference_keys,
        on=key_column,
        how="left_anti"
    )

    unmatched_count = unmatched.count()

    print(
        f"{reference_name} unmatched references: "
        f"{unmatched_count}"
    )

    if unmatched_count > 0:
        unmatched.select(
            "source_system",
            "source_inventory_id",
            key_column
        ).show(20, truncate=False)

        raise ValueError(
            f"Inventory contains {unmatched_count} "
            f"unmatched {reference_name} references"
        )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER INVENTORY PIPELINE ===")

        # ----------------------------------
        # 1. Read Bronze Delta tables
        # ----------------------------------

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/inventory")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/inventory")
        )

        # ----------------------------------
        # 2. Standardize source schemas
        # ----------------------------------

        retailmax = transform_retailmax_inventory(
            retailmax_bronze
        )

        freshmart = transform_freshmart_inventory(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        print(f"Input inventory records: {input_count:,}")

        assert input_count == 3600

        # ----------------------------------
        # 3. Clean and classify inventory
        # ----------------------------------

        cleaned = clean_identifiers(unified)

        validated = add_validation_status(cleaned)

        classified = add_inventory_freshness_status(
            add_stock_status(validated)
        )

        # ----------------------------------
        # 4. Separate valid and invalid
        # ----------------------------------

        invalid = classified.filter(
            F.col("validation_status") != "VALID"
        )

        invalid_count = invalid.count()

        print(f"Invalid records: {invalid_count}")

        if invalid_count > 0:
            invalid.groupBy(
                "validation_status"
            ).count().show()

            raise ValueError(
                "Invalid inventory records found. "
                "Quarantine handling is required "
                "before writing Silver Inventory."
            )

        valid = classified.filter(
            F.col("validation_status") == "VALID"
        )

        # ----------------------------------
        # 5. Deduplicate inventory IDs
        # ----------------------------------

        silver_inventory = deduplicate_inventory(
            valid
        ).cache()

        output_count = silver_inventory.count()

        print(
            f"Cleaned inventory records: "
            f"{output_count:,}"
        )

        assert output_count == 3600

        duplicate_count = (
            silver_inventory
            .groupBy(
                "source_system",
                "source_inventory_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_count == 0

        print("PASS: No duplicate inventory IDs")

        # ----------------------------------
        # 6. Validate source counts
        # ----------------------------------

        source_counts = {
            row["source_system"]: row["count"]
            for row in silver_inventory.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 2000,
            "freshmart": 1600,
        }

        print("PASS: RetailMax = 2,000")
        print("PASS: FreshMart = 1,600")

        # ----------------------------------
        # 7. Validate stock classification
        # ----------------------------------

        stock_counts = {
            row["stock_status"]: row["count"]
            for row in silver_inventory.groupBy(
                "stock_status"
            ).count().collect()
        }

        assert stock_counts == {
            "OUT_OF_STOCK": 3,
            "REORDER_REQUIRED": 436,
            "SUFFICIENT_STOCK": 3161,
        }

        print("PASS: Stock classifications verified")

        # ----------------------------------
        # 8. Validate freshness classification
        # ----------------------------------

        freshness_counts = {
            row["inventory_freshness_status"]: row["count"]
            for row in silver_inventory.groupBy(
                "inventory_freshness_status"
            ).count().collect()
        }

        assert sum(freshness_counts.values()) == 3600
        assert set(freshness_counts).issubset({
            "CURRENT",
            "STALE",
        })

        print("PASS: Freshness classifications verified")

        print("\n=== STOCK STATUS ===")

        silver_inventory.groupBy(
            "stock_status"
        ).count().orderBy(
            "stock_status"
        ).show()

        print("\n=== FRESHNESS STATUS ===")

        silver_inventory.groupBy(
            "inventory_freshness_status"
        ).count().orderBy(
            "inventory_freshness_status"
        ).show()

        # ----------------------------------
        # 9. Validate master-data references
        # ----------------------------------

        print(
            "\n=== MASTER DATA REFERENTIAL INTEGRITY ==="
        )

        products = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/products")
        )

        stores = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/stores")
        )

        assert_no_missing_references(
            silver_inventory,
            products,
            "product_key",
            "Product",
        )

        assert_no_missing_references(
            silver_inventory,
            stores,
            "store_key",
            "Store",
        )

        print(
            "PASS: All inventory product/store "
            "references are valid"
        )

        # ----------------------------------
        # 10. Write Silver Inventory to S3
        # ----------------------------------

        print(
            "\n=== WRITING SILVER INVENTORY TO S3 ==="
        )

        (
            silver_inventory.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_INVENTORY_PATH)
        )

        # ----------------------------------
        # 11. Read back persisted Delta table
        # ----------------------------------

        print(
            "\n=== VALIDATING PERSISTED S3 TABLE ==="
        )

        verified = (
            spark.read.format("delta")
            .load(SILVER_INVENTORY_PATH)
        )

        verified_count = verified.count()

        assert verified_count == 3600

        print(
            f"PASS: S3 Silver Inventory = "
            f"{verified_count:,}"
        )

        print("\n=== PERSISTED COUNTS BY SOURCE ===")

        verified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SILVER INVENTORY SCHEMA ===")

        verified.printSchema()

        # ----------------------------------
        # 12. Delta transaction verification
        # ----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            SILVER_INVENTORY_PATH
        )

        print(
            "\n=== DELTA TRANSACTION HISTORY ==="
        )

        DeltaTable.forPath(
            spark,
            SILVER_INVENTORY_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: SILVER INVENTORY "
            "DELTA TABLE CREATED!"
        )

        print(
            f"\nS3 Path: {SILVER_INVENTORY_PATH}"
        )

        silver_inventory.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
